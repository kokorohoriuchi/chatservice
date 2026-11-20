from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from raft.networking import serialization
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    NodeId,
    PeerAddress,
    ProposalResult,
    RequestVote,
    RequestVoteReply,
)

logger = logging.getLogger(__name__)


@dataclass
class _PeerConnection:
    """One open connection to a peer, holding both halves of the stream."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class PeerClient:
    """Outbound Raft peer client.

    Handles the sending of RPC messages to peers and receiving their replies.
    """

    def __init__(self, peers: dict[NodeId, PeerAddress], request_timeout: float = 1.0) -> None:
        self.peers = peers
        self.request_timeout = request_timeout
        self.connections: dict[NodeId, _PeerConnection] = {}
        self.locks: dict[NodeId, asyncio.Lock] = {peer_id: asyncio.Lock() for peer_id in peers}

    async def send_request_vote(
        self, peer_id: NodeId, request: RequestVote
    ) -> RequestVoteReply | None:
        """Ask one peer for its vote, or return None if it is unreachable."""

        payload = await self._exchange(
            peer_id,
            serialization.PeerMessageType.REQUEST_VOTE,
            {"term": request.term, "candidate_id": request.candidate_id,
             "last_log_index": request.last_log_index, "last_log_term": request.last_log_term},
        )
        if payload is None:
            return None
        return serialization.request_vote_reply_from_dict(payload)

    async def send_append_entries(
        self, peer_id: NodeId, request: AppendEntries
    ) -> AppendEntriesReply | None:
        """Replicate entries or send a heartbeat to one peer, or None if unreachable."""

        payload = await self._exchange(
            peer_id,
            serialization.PeerMessageType.APPEND_ENTRIES,
            {
                "term": request.term,
                "leader_id": request.leader_id,
                "prev_log_index": request.prev_log_index,
                "prev_log_term": request.prev_log_term,
                "entries": request.entries,
                "leader_commit": request.leader_commit,
            },
        )
        if payload is None:
            return None
        return serialization.append_entries_reply_from_dict(payload)

    async def send_forward_proposal(
        self, peer_id: NodeId, command: Command
    ) -> ProposalResult | None:
        """Ask the leader to append a command on this node's behalf."""

        payload = await self._exchange(
            peer_id, serialization.PeerMessageType.FORWARD_PROPOSAL, {"command": command}
        )
        if payload is None:
            return None
        return serialization.proposal_result_from_dict(payload)

    async def close(self) -> None:
        """Close every open peer connection."""
        logger.info("closing %d peer connection(s)", len(self.connections))
        for connection in list(self.connections.values()):
            connection.writer.close()
        self.connections.clear()

    async def _exchange(
        self, peer_id: NodeId, message_type: serialization.PeerMessageType, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Send one message to a peer and await its reply, or None on timeout."""

        peer_address = self.peers.get(peer_id)
        if peer_address is None:
            logger.warning("no address known for peer %s", peer_id)
            return None

        # Per peer allow only one request at a time, reply order guarantee
        async with self.locks[peer_id]:
            connection = self.connections.get(peer_id)

            if connection is None or connection.writer.is_closing():
                try:
                    reader, writer = await asyncio.open_connection(
                        peer_address.host, 
                        peer_address.port
                        )
                except Exception as exc:
                    logger.warning(
                        "could not connect to peer %s at %s:%d: %s",
                        peer_id,
                        peer_address.host,
                        peer_address.port,
                        exc,
                    )
                    return None
                connection = _PeerConnection(reader=reader, writer=writer)
                self.connections[peer_id] = connection
                logger.info(
                    "connected to peer %s at %s:%d",
                    peer_id,
                    peer_address.host,
                    peer_address.port,
                )

            logger.debug("sending %s to peer %s", message_type.value, peer_id)
            try:
                await serialization.write_message(
                    connection.writer, {"type": message_type.value, "payload": payload}
                )
                reply = await asyncio.wait_for(
                    serialization.read_message(connection.reader),
                    timeout=self.request_timeout,
                )
            except TimeoutError:
                logger.warning(
                    "timed out waiting for %s reply from peer %s after %.1fs",
                    message_type.value,
                    peer_id,
                    self.request_timeout,
                )
                self._drop(peer_id)
                return None
            except Exception as exc:
                logger.warning(
                    "error exchanging %s with peer %s: %s", message_type.value, peer_id, exc
                )
                self._drop(peer_id)
                return None

            if reply is None:
                logger.info("peer %s closed the connection", peer_id)
                self._drop(peer_id)
                return None

            reply_payload = reply.get("payload")
            if not isinstance(reply_payload, dict):
                logger.warning("peer %s sent a malformed reply %r", peer_id, reply)
                return None
            return reply_payload

    def _drop(self, peer_id: NodeId) -> None:
        """Forget a broken connection."""
        connection = self.connections.pop(peer_id, None)
        if connection is not None:
            logger.info("dropping connection to peer %s", peer_id)
            connection.writer.close()
