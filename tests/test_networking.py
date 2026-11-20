"""Tests for raft/networking/.

These tests exercise the RPC plumbing, not Raft logic. The RaftNode is faked
(it records calls and returns canned replies), while the servers and clients run
for real over OS-assigned TCP ports. A passing test therefore means the RPC
actually travelled over a socket and came back decoded.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import pytest

from raft.networking import serialization
from raft.networking.chat_api import ChatApiServer
from raft.networking.peer_client import PeerClient
from raft.networking.peer_server import PeerServer
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    HistoryReply,
    HistoryRequest,
    LogEntry,
    NodeId,
    PeerAddress,
    ProposalResult,
    RequestVote,
    RequestVoteReply,
)

if TYPE_CHECKING:
    from raft.node import RaftNode


def free_port() -> int:
    """Return an OS-assigned free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return cast(int, sock.getsockname()[1])


class FakeNode:
    """A stand-in RaftNode that records calls and returns canned replies."""

    def __init__(self) -> None:
        self.request_vote_reply = RequestVoteReply(term=1, vote_granted=True)
        self.append_entries_reply = AppendEntriesReply(term=1, success=True, match_index=0)
        self.proposal_result = ProposalResult(accepted=True, index=1, term=1)
        self.history_entries: list[LogEntry] = []
        self.received_votes: list[RequestVote] = []
        self.received_appends: list[AppendEntries] = []
        self.received_forwarded: list[Command] = []
        self.received_proposals: list[Command] = []
        self.received_history: list[tuple[int, int | None]] = []

    async def handle_request_vote(self, request: RequestVote) -> RequestVoteReply:
        """Record the request and return the canned vote reply."""
        self.received_votes.append(request)
        return self.request_vote_reply

    async def handle_append_entries(self, request: AppendEntries) -> AppendEntriesReply:
        """Record the request and return the canned append reply."""
        self.received_appends.append(request)
        return self.append_entries_reply

    async def handle_forward_proposal(self, command: Command) -> ProposalResult:
        """Record the command and return the canned proposal result."""
        self.received_forwarded.append(command)
        return self.proposal_result

    async def propose(self, command: Command) -> ProposalResult:
        """Record the command and return the canned proposal result."""
        self.received_proposals.append(command)
        return self.proposal_result

    def committed_entries(self, from_index: int = 1, to_index: int | None = None) -> list[LogEntry]:
        """Record the range and return the canned committed entries."""
        self.received_history.append((from_index, to_index))
        return self.history_entries


async def exchange(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    message_type: serialization.PeerMessageType,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Send one peer message and return the decoded reply."""
    await serialization.write_message(writer, {"type": message_type.value, "payload": payload})
    reply = await serialization.read_message(reader)
    assert reply is not None
    return reply


async def wait_for_connections(server: ChatApiServer | PeerServer, count: int) -> None:
    """Wait until the server has registered `count` connections."""
    for _ in range(100):
        if len(server.connections) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"server never reached {count} connection(s)")


# ----------------------------------------------------------------------
# serialization.py
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "decoder"),
    [
        (
            RequestVote(term=3, candidate_id=NodeId("n1"), last_log_index=5, last_log_term=2),
            serialization.request_vote_from_dict,
        ),
        (
            RequestVoteReply(term=3, vote_granted=True),
            serialization.request_vote_reply_from_dict,
        ),
        (
            AppendEntries(
                term=3,
                leader_id=NodeId("n1"),
                prev_log_index=1,
                prev_log_term=1,
                entries=[
                    LogEntry(
                        index=2,
                        term=3,
                        command=Command(type="POST_MESSAGE", payload={"text": "hi"}),
                    )
                ],
                leader_commit=1,
            ),
            serialization.append_entries_from_dict,
        ),
        (
            AppendEntriesReply(term=3, success=True, match_index=2),
            serialization.append_entries_reply_from_dict,
        ),
        (
            Command(type="POST_MESSAGE", payload={"text": "hi"}),
            serialization.command_from_dict,
        ),
        (
            LogEntry(index=2, term=3, command=Command(type="POST_MESSAGE", payload={"text": "hi"})),
            serialization.entry_from_dict,
        ),
        (
            ProposalResult(accepted=True, index=2, term=3, leader_id=NodeId("n1"), error=None),
            serialization.proposal_result_from_dict,
        ),
        (
            HistoryRequest(from_index=2, to_index=5),
            serialization.history_request_from_dict,
        ),
        (
            HistoryReply(
                entries=[
                    LogEntry(
                        index=1,
                        term=1,
                        command=Command(type="POST_MESSAGE", payload={"text": "hi"}),
                    )
                ]
            ),
            serialization.history_reply_from_dict,
        ),
    ],
)
def test_round_trip_through_the_wire(
    value: Any, decoder: Callable[[dict[str, Any]], Any]
) -> None:
    """Every Raft type survives to_dict then its decoder unchanged."""
    assert decoder(serialization.to_dict(value)) == value


async def test_framing_over_a_real_socket() -> None:
    """Two messages arrive intact and a closed stream reads as None."""
    first = {"type": "A", "payload": {"n": 1}}
    second = {"type": "B", "payload": {"text": "line\nbreak"}}

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await serialization.write_message(writer, first)
        await serialization.write_message(writer, second)
        writer.close()

    port = free_port()
    server = await asyncio.start_server(handler, "127.0.0.1", port)
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        assert await serialization.read_message(reader) == first
        assert await serialization.read_message(reader) == second
        assert await serialization.read_message(reader) is None
    finally:
        writer.close()
        server.close()
        await server.wait_closed()


# ----------------------------------------------------------------------
# peer_server.py
# ----------------------------------------------------------------------


async def test_peer_server_dispatches_every_rpc_type() -> None:
    """Each inbound RPC reaches the matching node handler and returns its reply."""
    node = FakeNode()
    server = PeerServer("127.0.0.1", free_port(), cast("RaftNode", node))
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        vote = RequestVote(term=2, candidate_id=NodeId("n2"), last_log_index=0, last_log_term=0)
        reply = await exchange(
            reader, writer, serialization.PeerMessageType.REQUEST_VOTE, serialization.to_dict(vote)
        )
        assert reply["type"] == serialization.PeerMessageType.REQUEST_VOTE_REPLY.value
        assert (
            serialization.request_vote_reply_from_dict(reply["payload"]) == node.request_vote_reply
        )
        assert node.received_votes == [vote]

        append = AppendEntries(term=2, leader_id=NodeId("n1"), prev_log_index=0, prev_log_term=0)
        reply = await exchange(
            reader,
            writer,
            serialization.PeerMessageType.APPEND_ENTRIES,
            serialization.to_dict(append),
        )
        assert reply["type"] == serialization.PeerMessageType.APPEND_ENTRIES_REPLY.value
        assert (
            serialization.append_entries_reply_from_dict(reply["payload"])
            == node.append_entries_reply
        )
        assert node.received_appends == [append]

        command = Command(type="POST_MESSAGE", payload={"text": "hi"})
        reply = await exchange(
            reader,
            writer,
            serialization.PeerMessageType.FORWARD_PROPOSAL,
            {"command": serialization.to_dict(command)},
        )
        assert reply["type"] == serialization.PeerMessageType.FORWARD_PROPOSAL_REPLY.value
        assert serialization.proposal_result_from_dict(reply["payload"]) == node.proposal_result
        assert node.received_forwarded == [command]
    finally:
        writer.close()
        await server.stop()


# ----------------------------------------------------------------------
# peer_client.py
# ----------------------------------------------------------------------


async def test_peer_client_sends_every_rpc_type() -> None:
    """Each outbound RPC reaches a real server and returns the decoded reply."""
    node = FakeNode()
    server = PeerServer("127.0.0.1", free_port(), cast("RaftNode", node))
    await server.start()
    peer_id = NodeId("n2")
    peers = {peer_id: PeerAddress(node_id=peer_id, host="127.0.0.1", port=server.port)}
    client = PeerClient(peers)
    try:
        vote = RequestVote(term=2, candidate_id=NodeId("n1"), last_log_index=0, last_log_term=0)
        assert await client.send_request_vote(peer_id, vote) == node.request_vote_reply
        assert node.received_votes == [vote]

        append = AppendEntries(term=2, leader_id=NodeId("n1"), prev_log_index=0, prev_log_term=0)
        assert await client.send_append_entries(peer_id, append) == node.append_entries_reply
        assert node.received_appends == [append]

        command = Command(type="POST_MESSAGE", payload={"text": "hi"})
        assert await client.send_forward_proposal(peer_id, command) == node.proposal_result
        assert node.received_forwarded == [command]
    finally:
        await client.close()
        await server.stop()


# ----------------------------------------------------------------------
# chat_api.py
# ----------------------------------------------------------------------


async def test_chat_api_serves_propose() -> None:
    """A PROPOSE_REQUEST reaches node.propose and its result comes back as PROPOSAL_RESULT."""
    node = FakeNode()
    server = ChatApiServer("127.0.0.1", free_port(), cast("RaftNode", node))
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        command = Command(type="POST_MESSAGE", payload={"text": "hi"})
        await serialization.write_message(
            writer,
            {"type": "PROPOSE_REQUEST", "payload": {"command": serialization.to_dict(command)}},
        )
        reply = await serialization.read_message(reader)
        assert reply is not None
        assert reply["type"] == "PROPOSAL_RESULT"
        assert serialization.proposal_result_from_dict(reply["payload"]) == node.proposal_result
        assert node.received_proposals == [command]
    finally:
        writer.close()
        await server.stop()


async def test_chat_api_pushes_committed_entries() -> None:
    """notify_committed_entry reaches every connected chat server."""
    node = FakeNode()
    server = ChatApiServer("127.0.0.1", free_port(), cast("RaftNode", node))
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        await wait_for_connections(server, 1)
        entry = LogEntry(
            index=1, term=1, command=Command(type="POST_MESSAGE", payload={"text": "hi"})
        )
        await server.notify_committed_entry(entry)
        message = await serialization.read_message(reader)
        assert message is not None
        assert message["type"] == "COMMITTED_ENTRY"
        assert serialization.entry_from_dict(message["payload"]["entry"]) == entry
    finally:
        writer.close()
        await server.stop()


async def test_chat_api_serves_history_request() -> None:
    """A HISTORY_REQUEST reaches node.committed_entries and its entries come back."""
    node = FakeNode()
    node.history_entries = [
        LogEntry(index=1, term=1, command=Command(type="POST_MESSAGE", payload={"text": "a"})),
        LogEntry(index=2, term=1, command=Command(type="POST_MESSAGE", payload={"text": "b"})),
    ]
    server = ChatApiServer("127.0.0.1", free_port(), cast("RaftNode", node))
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        request = HistoryRequest(from_index=1, to_index=None)
        await serialization.write_message(
            writer,
            {"type": "HISTORY_REQUEST", "payload": serialization.to_dict(request)},
        )
        reply = await serialization.read_message(reader)
        assert reply is not None
        assert reply["type"] == "HISTORY_REPLY"
        assert serialization.history_reply_from_dict(reply["payload"]) == HistoryReply(
            entries=node.history_entries
        )
        assert node.received_history == [(1, None)]
    finally:
        writer.close()
        await server.stop()