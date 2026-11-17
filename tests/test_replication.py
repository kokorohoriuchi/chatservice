"""Tests for raft/replication.py: log replication, commitment, and applying.

The nodes run in memory: a fake PeerClient delivers each RPC straight to the
target RaftNode's handler, and a fake chat API records every committed entry it
is told about. Election functions are replaced with minimal stand-ins, so these
tests exercise replication.py alone.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest

from raft import election, replication
from raft.node import RaftNode
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    LogEntry,
    NodeId,
    PeerAddress,
    ProposalResult,
    RaftConfig,
    Role,
)

if TYPE_CHECKING:
    from raft.networking.chat_api import ChatApiServer
    from raft.networking.peer_client import PeerClient


class FakeChatApi:
    """Records every entry pushed to the chat server."""

    def __init__(self) -> None:
        # Every entry pushed, in push order. Tests check order and duplicates.
        self.entries: list[LogEntry] = []

    async def notify_committed_entry(self, entry: LogEntry) -> None:
        """Record the entry instead of sending it over TCP."""
        self.entries.append(entry)


class FakePeerClient:
    """Delivers RPCs directly to in-memory nodes. Nodes in `down` are unreachable."""

    def __init__(self, cluster: dict[NodeId, RaftNode]) -> None:
        self.cluster = cluster
        # Nodes simulated as crashed or partitioned: RPCs to them return None,
        # exactly like the real PeerClient does for an unreachable peer.
        self.down: set[NodeId] = set()
        # Every AppendEntries sent, so tests can inspect what went over the wire.
        self.sent: list[tuple[NodeId, AppendEntries]] = []

    async def send_append_entries(
        self, peer_id: NodeId, request: AppendEntries
    ) -> AppendEntriesReply | None:
        """Deliver AppendEntries straight to the target node's handler."""
        self.sent.append((peer_id, request))
        if peer_id in self.down:
            return None
        return await self.cluster[peer_id].handle_append_entries(request)

    async def send_forward_proposal(
        self, peer_id: NodeId, command: Command
    ) -> ProposalResult | None:
        """Deliver a forwarded proposal straight to the target node."""
        if peer_id in self.down:
            return None
        return await self.cluster[peer_id].handle_forward_proposal(command)


@pytest.fixture(autouse=True)
def stub_election(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, NodeId]]:
    """Replace the election functions replication calls, and record the calls.

    election.py is another person's area and still unimplemented, so these
    stand-ins do only the minimum replication relies on.
    """
    calls: list[tuple[str, NodeId]] = []

    async def become_follower(node: RaftNode, term: int) -> None:
        # Adopt the newer term (clearing the vote, as Raft requires) and step down.
        calls.append(("become_follower", node.node_id))
        if term > node.current_term:
            node.current_term = term
            node.voted_for = None
        node.role = Role.FOLLOWER

    async def reset_election_timer(node: RaftNode) -> None:
        # No real timer in unit tests; just record that it was reset.
        calls.append(("reset_election_timer", node.node_id))

    monkeypatch.setattr(election, "become_follower", become_follower)
    monkeypatch.setattr(election, "reset_election_timer", reset_election_timer)
    return calls


def cmd(text: str) -> Command:
    """Build a chat command. Raft treats the payload as opaque."""
    return Command(type="POST_MESSAGE", payload={"text": text})


def entry(index: int, term: int, text: str = "") -> LogEntry:
    """Build a log entry. Different text makes otherwise equal entries distinct."""
    return LogEntry(index=index, term=term, command=cmd(text or f"m{index}"))


def make_node(node_id: str, peer_ids: list[str]) -> RaftNode:
    """Build a stopped RaftNode with a fake chat API. Addresses are never used."""
    peers = {
        NodeId(p): PeerAddress(node_id=NodeId(p), host="127.0.0.1", port=7000 + i)
        for i, p in enumerate(peer_ids)
    }
    config = RaftConfig(
        node_id=NodeId(node_id),
        peers=peers,
        peer_host="127.0.0.1",
        peer_port=7100,
        chat_api_host="127.0.0.1",
        chat_api_port=8100,
    )
    node = RaftNode(config)
    node.chat_api = cast("ChatApiServer", FakeChatApi())
    return node


def make_cluster(
    size: int = 3, term: int = 1
) -> tuple[dict[NodeId, RaftNode], RaftNode, FakePeerClient]:
    """Build a cluster with n1 as leader of `term` and the rest as its followers."""
    ids = [f"n{i}" for i in range(1, size + 1)]
    cluster = {NodeId(i): make_node(i, [p for p in ids if p != i]) for i in ids}
    client = FakePeerClient(cluster)
    for node in cluster.values():
        node.peer_client = cast("PeerClient", client)
        node.current_term = term
        node.leader_id = NodeId("n1")
    leader = cluster[NodeId("n1")]
    leader.role = Role.LEADER
    replication.init_leader_state(leader)
    return cluster, leader, client


def pushed(node: RaftNode) -> list[LogEntry]:
    """Return the entries a node has pushed to its (fake) chat server."""
    return cast(FakeChatApi, node.chat_api).entries


async def drain() -> None:
    """Wait for every background replication task started by propose."""
    # Loop because a finishing task could, in principle, start another one.
    while replication._background_tasks:
        await asyncio.gather(*list(replication._background_tasks))


# ----------------------------------------------------------------------
# Follower: handle_append_entries
# ----------------------------------------------------------------------


async def test_stale_term_is_rejected_without_resetting_timer(
    stub_election: list[tuple[str, NodeId]],
) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 5
    reply = await node.handle_append_entries(
        AppendEntries(term=4, leader_id=NodeId("n1"), prev_log_index=0, prev_log_term=0)
    )
    assert reply == AppendEntriesReply(term=5, success=False, match_index=0)
    assert stub_election == []


async def test_heartbeat_on_empty_log_succeeds_and_records_leader(
    stub_election: list[tuple[str, NodeId]],
) -> None:
    node = make_node("n2", ["n1", "n3"])
    reply = await node.handle_append_entries(
        AppendEntries(term=2, leader_id=NodeId("n1"), prev_log_index=0, prev_log_term=0)
    )
    assert reply == AppendEntriesReply(term=2, success=True, match_index=0)
    assert node.current_term == 2
    assert node.leader_id == "n1"
    assert ("reset_election_timer", "n2") in stub_election


async def test_candidate_steps_down_on_append_entries_in_same_term() -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 3
    node.role = Role.CANDIDATE
    await node.handle_append_entries(
        AppendEntries(term=3, leader_id=NodeId("n1"), prev_log_index=0, prev_log_term=0)
    )
    assert node.role == Role.FOLLOWER


async def test_mismatch_at_prev_log_index_is_rejected_with_hint() -> None:
    """A follower missing the leader's previous entry rejects, and says where to resume."""
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(1, 1), entry(2, 1)]
    reply = await node.handle_append_entries(
        AppendEntries(term=2, leader_id=NodeId("n1"), prev_log_index=5, prev_log_term=2)
    )
    assert not reply.success
    assert reply.match_index == 2  # last index the follower has
    reply = await node.handle_append_entries(
        AppendEntries(term=2, leader_id=NodeId("n1"), prev_log_index=2, prev_log_term=2)
    )
    assert not reply.success
    assert reply.match_index == 1  # entry 2 has the wrong term


async def test_conflicting_suffix_is_truncated_and_replaced() -> None:
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(1, 1), entry(2, 1, "old"), entry(3, 1, "old")]
    reply = await node.handle_append_entries(
        AppendEntries(
            term=2,
            leader_id=NodeId("n1"),
            prev_log_index=1,
            prev_log_term=1,
            entries=[entry(2, 2, "new")],
        )
    )
    assert reply == AppendEntriesReply(term=2, success=True, match_index=2)
    assert node.log == [entry(1, 1), entry(2, 2, "new")]


async def test_delayed_append_entries_does_not_truncate_newer_entries() -> None:
    """An old AppendEntries arriving late carries a prefix the follower already has."""
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(1, 1), entry(2, 1), entry(3, 1)]
    reply = await node.handle_append_entries(
        AppendEntries(
            term=1,
            leader_id=NodeId("n1"),
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry(1, 1)],
        )
    )
    assert reply.success
    assert reply.match_index == 1
    assert len(node.log) == 3


async def test_follower_commits_up_to_last_new_entry_and_applies_once() -> None:
    node = make_node("n2", ["n1", "n3"])
    request = AppendEntries(
        term=1,
        leader_id=NodeId("n1"),
        prev_log_index=0,
        prev_log_term=0,
        entries=[entry(1, 1), entry(2, 1)],
        leader_commit=5,
    )
    await node.handle_append_entries(request)
    assert node.commit_index == 2  # min(leader_commit, last new entry)
    assert [e.index for e in pushed(node)] == [1, 2]

    await node.handle_append_entries(request)  # duplicate delivery
    assert [e.index for e in pushed(node)] == [1, 2]


async def test_follower_commit_index_never_moves_back() -> None:
    """A heartbeat confirming only up to index 1 must not undo commits up to 3."""
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(1, 1), entry(2, 1), entry(3, 1)]
    node.commit_index = 3
    node.last_applied = 3
    await node.handle_append_entries(
        AppendEntries(
            term=1, leader_id=NodeId("n1"), prev_log_index=1, prev_log_term=1, leader_commit=4
        )
    )
    assert node.commit_index == 3


# ----------------------------------------------------------------------
# Leader: propose, replication, commitment
# ----------------------------------------------------------------------


async def test_propose_appends_and_returns_before_commit() -> None:
    cluster, leader, _ = make_cluster(term=4)
    leader.log = [entry(1, 3)]
    replication.init_leader_state(leader)

    result = await leader.propose(cmd("hello"))
    assert result == ProposalResult(accepted=True, index=2, term=4, leader_id=NodeId("n1"))
    assert leader.log[-1] == LogEntry(index=2, term=4, command=cmd("hello"))
    assert leader.commit_index == 0  # replication still in the background

    await drain()
    assert leader.commit_index == 2
    for node in cluster.values():
        assert node.log == leader.log


async def test_commit_needs_a_majority() -> None:
    """With both followers down nothing commits; one follower back makes 2 of 3."""
    cluster, leader, client = make_cluster()
    client.down = {NodeId("n2"), NodeId("n3")}
    await leader.propose(cmd("alone"))
    await drain()
    assert leader.commit_index == 0
    assert pushed(leader) == []

    client.down = {NodeId("n3")}  # one of two followers acks: 2 of 3 is a majority
    await replication.broadcast_append_entries(leader)
    assert leader.commit_index == 1
    assert [e.index for e in pushed(leader)] == [1]
    assert cluster[NodeId("n3")].log == []


async def test_followers_commit_on_next_append_entries() -> None:
    cluster, leader, _ = make_cluster()
    await leader.propose(cmd("hi"))
    await drain()
    follower = cluster[NodeId("n2")]
    assert follower.commit_index == 0  # learns the commit on the next message
    await replication.broadcast_append_entries(leader)  # heartbeat
    assert follower.commit_index == 1
    assert [e.index for e in pushed(follower)] == [1]


async def test_entry_from_earlier_term_is_not_committed_by_count() -> None:
    """Raft Figure 8: only a current-term entry is committed by counting replicas."""
    _, leader, _ = make_cluster(term=3)
    leader.log = [entry(1, 1), entry(2, 2)]
    replication.init_leader_state(leader)
    leader.match_index = {NodeId("n2"): 2, NodeId("n3"): 2}

    await replication.advance_commit_index(leader)
    assert leader.commit_index == 0

    leader.log.append(entry(3, 3))
    leader.match_index[NodeId("n2")] = 3
    await replication.advance_commit_index(leader)
    assert leader.commit_index == 3  # earlier entries commit with it
    assert [e.index for e in pushed(leader)] == [1, 2, 3]


async def test_lagging_follower_catches_up_after_restart() -> None:
    """Fault tolerance table: a follower that was down catches up from the leader."""
    cluster, leader, client = make_cluster()
    client.down = {NodeId("n3")}
    for i in range(5):
        await leader.propose(cmd(f"m{i}"))
    await drain()
    assert leader.commit_index == 5

    client.down = set()  # n3 comes back with an empty log
    await replication.broadcast_append_entries(leader)
    assert cluster[NodeId("n3")].log == leader.log
    assert leader.match_index[NodeId("n3")] == 5
    assert cluster[NodeId("n3")].commit_index == 5


async def test_divergent_follower_is_overwritten_from_the_leader() -> None:
    """Uncommitted entries from a term-2 leader that lost are replaced by term-3 ones."""
    cluster, leader, _ = make_cluster(term=3)
    leader.log = [entry(1, 1), entry(2, 3), entry(3, 3), entry(4, 3)]
    follower = cluster[NodeId("n2")]
    follower.log = [entry(1, 1), entry(2, 2, "stale"), entry(3, 2, "stale")]
    replication.init_leader_state(leader)

    await replication.replicate_to_peer(leader, NodeId("n2"))
    assert follower.log == leader.log
    assert leader.next_index[NodeId("n2")] == 5


async def test_catch_up_is_split_into_bounded_rpcs(monkeypatch: pytest.MonkeyPatch) -> None:
    """A long catch-up is sent in several RPCs of at most MAX_ENTRIES_PER_RPC entries."""
    monkeypatch.setattr(replication, "MAX_ENTRIES_PER_RPC", 2)
    cluster, leader, client = make_cluster()
    leader.log = [entry(i, 1) for i in range(1, 6)]
    replication.init_leader_state(leader)
    leader.next_index[NodeId("n2")] = 1

    await replication.replicate_to_peer(leader, NodeId("n2"))
    assert cluster[NodeId("n2")].log == leader.log
    assert all(len(req.entries) <= 2 for _, req in client.sent)


async def test_higher_term_reply_makes_leader_step_down() -> None:
    _, leader, _ = make_cluster(term=2)
    await replication.handle_append_entries_reply(
        leader, NodeId("n2"), AppendEntriesReply(term=5, success=False, match_index=0)
    )
    assert leader.role == Role.FOLLOWER
    assert leader.current_term == 5


async def test_delayed_success_reply_does_not_lower_match_index() -> None:
    """Replies can arrive out of order; an older, smaller match_index is ignored."""
    _, leader, _ = make_cluster()
    leader.log = [entry(1, 1), entry(2, 1), entry(3, 1)]
    replication.init_leader_state(leader)
    peer = NodeId("n2")
    await replication.handle_append_entries_reply(
        leader, peer, AppendEntriesReply(term=1, success=True, match_index=3)
    )
    await replication.handle_append_entries_reply(
        leader, peer, AppendEntriesReply(term=1, success=True, match_index=1)
    )
    assert leader.match_index[peer] == 3
    assert leader.next_index[peer] == 4


async def test_single_node_commits_immediately() -> None:
    node = make_node("solo", [])
    node.role = Role.LEADER
    node.current_term = 1
    result = await node.propose(cmd("me"))
    assert result.accepted
    assert node.commit_index == 1
    assert [e.index for e in pushed(node)] == [1]


# ----------------------------------------------------------------------
# Forwarding
# ----------------------------------------------------------------------


async def test_follower_forwards_proposal_to_leader() -> None:
    cluster, leader, _ = make_cluster()
    result = await cluster[NodeId("n2")].propose(cmd("via follower"))
    assert result.accepted
    assert result.leader_id == "n1"
    assert leader.log[-1].command == cmd("via follower")
    await drain()
    assert leader.commit_index == 1


async def test_proposal_without_leader_is_rejected() -> None:
    node = make_node("n2", ["n1", "n3"])
    result = await node.propose(cmd("x"))
    assert not result.accepted
    assert result.error == "no leader"


async def test_unreachable_leader_is_reported() -> None:
    cluster, _, client = make_cluster()
    client.down = {NodeId("n1")}
    result = await cluster[NodeId("n2")].propose(cmd("x"))
    assert not result.accepted
    assert result.error == "leader unreachable"


async def test_non_leader_rejects_forwarded_proposal() -> None:
    cluster, _, _ = make_cluster()
    result = await cluster[NodeId("n2")].handle_forward_proposal(cmd("x"))
    assert not result.accepted
    assert result.leader_id == "n1"
    assert cluster[NodeId("n2")].log == []


# ----------------------------------------------------------------------
# Applying and history
# ----------------------------------------------------------------------


async def test_failed_push_is_not_retried_and_does_not_stop_applying() -> None:
    """A broken chat server connection on entry 1 still lets entry 2 through."""
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(1, 1), entry(2, 1)]
    node.commit_index = 2
    seen: list[int] = []

    class FlakyChatApi:
        async def notify_committed_entry(self, e: LogEntry) -> None:
            seen.append(e.index)
            if e.index == 1:
                raise ConnectionError("chat server gone")

    node.chat_api = cast("ChatApiServer", FlakyChatApi())
    await replication.apply_committed_entries(node)
    assert seen == [1, 2]
    assert node.last_applied == 2


async def test_committed_entries_never_returns_uncommitted() -> None:
    node = make_node("n2", ["n1", "n3"])
    node.log = [entry(i, 1) for i in range(1, 6)]
    node.commit_index = 3
    assert [e.index for e in replication.committed_entries(node)] == [1, 2, 3]
    assert [e.index for e in replication.committed_entries(node, 2, 10)] == [2, 3]
    assert replication.committed_entries(node, 4) == []
