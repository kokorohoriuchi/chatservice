"""Tests for raft/election.py: voting, role transitions, timers, and heartbeats.

The nodes run in memory: a fake PeerClient delivers each RPC straight to the
target RaftNode's handler. Unlike test_replication.py, nothing in election.py is
stubbed, so these tests exercise the real role transitions and background tasks.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, cast

import pytest

from raft import election
from raft.node import RaftNode
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    LogEntry,
    NodeId,
    PeerAddress,
    RaftConfig,
    RequestVote,
    RequestVoteReply,
    Role,
)

if TYPE_CHECKING:
    from raft.networking.peer_client import PeerClient

# Short timeouts so timer-driven tests finish quickly.
FAST_TIMEOUTS = (0.02, 0.04)
FAST_HEARTBEAT = 0.005


class FakePeerClient:
    """Delivers RPCs directly to in-memory nodes. Nodes in `down` are unreachable."""

    def __init__(self, cluster: dict[NodeId, RaftNode]) -> None:
        self.cluster = cluster
        # RPCs to these nodes return None, like the real PeerClient for a dead peer.
        self.down: set[NodeId] = set()
        self.vote_requests: list[tuple[NodeId, RequestVote]] = []
        self.append_entries: list[tuple[NodeId, AppendEntries]] = []

    async def send_request_vote(
        self, peer_id: NodeId, request: RequestVote
    ) -> RequestVoteReply | None:
        """Deliver RequestVote straight to the target node's handler."""
        self.vote_requests.append((peer_id, request))
        if peer_id in self.down:
            return None
        return await self.cluster[peer_id].handle_request_vote(request)

    async def send_append_entries(
        self, peer_id: NodeId, request: AppendEntries
    ) -> AppendEntriesReply | None:
        """Deliver AppendEntries straight to the target node's handler."""
        self.append_entries.append((peer_id, request))
        if peer_id in self.down:
            return None
        return await self.cluster[peer_id].handle_append_entries(request)


NodeFactory = Callable[..., RaftNode]


@pytest.fixture
async def make_node() -> AsyncIterator[NodeFactory]:
    """Build stopped RaftNodes, and cancel every task they started at teardown."""
    created: list[RaftNode] = []

    def factory(
        node_id: str,
        peer_ids: list[str],
        timeouts: tuple[float, float] = (10.0, 20.0),
        heartbeat: float = 0.05,
    ) -> RaftNode:
        # The default timeouts are long, so a timer never fires mid-test unless
        # a test asks for FAST_TIMEOUTS.
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
            election_timeout_range=timeouts,
            heartbeat_interval=heartbeat,
        )
        node = RaftNode(config)
        created.append(node)
        return node

    yield factory

    tasks: list[asyncio.Task[None]] = []
    for node in created:
        node.running = False
        for task in (node.election_timer_task, node.heartbeat_task):
            if task is not None:
                task.cancel()
                tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)


def make_cluster(
    make_node: NodeFactory,
    size: int = 3,
    running: bool = False,
    timeouts: tuple[float, float] = (10.0, 20.0),
) -> tuple[dict[NodeId, RaftNode], FakePeerClient]:
    """Build a cluster of followers in term 0 wired to one fake peer client."""
    ids = [f"n{i}" for i in range(1, size + 1)]
    cluster = {
        NodeId(i): make_node(
            i, [p for p in ids if p != i], timeouts=timeouts, heartbeat=FAST_HEARTBEAT
        )
        for i in ids
    }
    client = FakePeerClient(cluster)
    for node in cluster.values():
        node.peer_client = cast("PeerClient", client)
        node.running = running
    return cluster, client


def entry(index: int, term: int) -> LogEntry:
    """Build a log entry with a placeholder command."""
    return LogEntry(index=index, term=term, command=Command(type="POST_MESSAGE"))


def vote_request(
    term: int, candidate: str = "n1", last_log_index: int = 0, last_log_term: int = 0
) -> RequestVote:
    """Build a RequestVote with defaults for an empty log."""
    return RequestVote(
        term=term,
        candidate_id=NodeId(candidate),
        last_log_index=last_log_index,
        last_log_term=last_log_term,
    )


def leaders(cluster: dict[NodeId, RaftNode]) -> list[RaftNode]:
    """Return every node that currently believes it is leader."""
    return [n for n in cluster.values() if n.role is Role.LEADER]


async def wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Poll until predicate holds, failing the test if it never does."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            pytest.fail("condition not reached before timeout")
        await asyncio.sleep(0.005)


# ----------------------------------------------------------------------
# handle_request_vote
# ----------------------------------------------------------------------


async def test_grants_vote_to_up_to_date_candidate(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    reply = await node.handle_request_vote(vote_request(term=1))
    assert reply == RequestVoteReply(term=1, vote_granted=True)
    assert node.current_term == 1
    assert node.voted_for == "n1"
    assert node.role is Role.FOLLOWER


async def test_granting_vote_resets_election_timer(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.running = True
    node.current_term = 1
    await election.reset_election_timer(node)
    old_timer = node.election_timer_task
    assert old_timer is not None

    await node.handle_request_vote(vote_request(term=1))

    assert node.election_timer_task is not None
    assert node.election_timer_task is not old_timer
    await asyncio.sleep(0)
    assert old_timer.cancelled()


async def test_rejects_stale_term(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 5
    reply = await node.handle_request_vote(vote_request(term=4))
    assert reply == RequestVoteReply(term=5, vote_granted=False)
    assert node.voted_for is None
    assert node.current_term == 5


async def test_votes_at_most_once_per_term(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    first = await node.handle_request_vote(vote_request(term=1, candidate="n1"))
    second = await node.handle_request_vote(vote_request(term=1, candidate="n3"))
    assert first.vote_granted
    assert not second.vote_granted
    assert node.voted_for == "n1"


async def test_repeated_request_from_same_candidate_is_granted(make_node: NodeFactory) -> None:
    # A candidate may retry if the first reply was lost.
    node = make_node("n2", ["n1", "n3"])
    await node.handle_request_vote(vote_request(term=1, candidate="n1"))
    reply = await node.handle_request_vote(vote_request(term=1, candidate="n1"))
    assert reply.vote_granted


async def test_new_term_clears_previous_vote(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    await node.handle_request_vote(vote_request(term=1, candidate="n1"))
    reply = await node.handle_request_vote(vote_request(term=2, candidate="n3"))
    assert reply == RequestVoteReply(term=2, vote_granted=True)
    assert node.voted_for == "n3"


async def test_rejects_candidate_with_older_last_log_term(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 3
    node.log = [entry(1, 1), entry(2, 3)]
    # Longer log, but its last entry is from an older term.
    reply = await node.handle_request_vote(vote_request(term=4, last_log_index=5, last_log_term=2))
    assert not reply.vote_granted
    assert node.voted_for is None


async def test_rejects_candidate_with_shorter_log_in_same_last_term(
    make_node: NodeFactory,
) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 2
    node.log = [entry(1, 2), entry(2, 2)]
    reply = await node.handle_request_vote(vote_request(term=3, last_log_index=1, last_log_term=2))
    assert not reply.vote_granted


async def test_grants_candidate_with_newer_last_log_term_but_shorter_log(
    make_node: NodeFactory,
) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 2
    node.log = [entry(1, 1), entry(2, 1), entry(3, 1)]
    reply = await node.handle_request_vote(vote_request(term=3, last_log_index=1, last_log_term=2))
    assert reply.vote_granted


async def test_grants_candidate_with_identical_log(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 2
    node.log = [entry(1, 1), entry(2, 2)]
    reply = await node.handle_request_vote(vote_request(term=3, last_log_index=2, last_log_term=2))
    assert reply.vote_granted


async def test_leader_steps_down_on_higher_term_even_when_refusing_vote(
    make_node: NodeFactory,
) -> None:
    node = make_node("n2", ["n1", "n3"])
    node.current_term = 2
    node.log = [entry(1, 2)]
    await election.become_leader(node)

    # Higher term, but the candidate's log is behind.
    reply = await node.handle_request_vote(vote_request(term=3))

    assert reply == RequestVoteReply(term=3, vote_granted=False)
    assert node.role is Role.FOLLOWER
    assert node.current_term == 3
    assert node.heartbeat_task is None


async def test_candidate_rejects_rival_in_same_term(make_node: NodeFactory) -> None:
    node = make_node("n2", ["n1", "n3"])
    await election.become_candidate(node)
    reply = await node.handle_request_vote(vote_request(term=node.current_term, candidate="n1"))
    assert not reply.vote_granted
    assert node.role is Role.CANDIDATE
    assert node.voted_for == "n2"


# ----------------------------------------------------------------------
# handle_vote_response
# ----------------------------------------------------------------------


async def test_majority_of_votes_makes_leader(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3", "n4", "n5"])
    node.log = [entry(1, 1)]
    await election.become_candidate(node)
    term = node.current_term

    await node.handle_vote_response(NodeId("n2"), RequestVoteReply(term=term, vote_granted=True))
    assert leaders({node.node_id: node}) == []

    await node.handle_vote_response(NodeId("n3"), RequestVoteReply(term=term, vote_granted=True))
    assert node.role is Role.LEADER
    assert node.leader_id == "n1"
    assert node.heartbeat_task is not None
    assert node.election_timer_task is None
    # init_leader_state ran
    assert node.next_index == {NodeId(p): 2 for p in ["n2", "n3", "n4", "n5"]}
    assert node.match_index == {NodeId(p): 0 for p in ["n2", "n3", "n4", "n5"]}


async def test_denied_votes_are_not_counted(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    await election.become_candidate(node)
    term = node.current_term
    await node.handle_vote_response(NodeId("n2"), RequestVoteReply(term=term, vote_granted=False))
    await node.handle_vote_response(NodeId("n3"), RequestVoteReply(term=term, vote_granted=False))
    assert node.role is Role.CANDIDATE
    assert node.votes_received == {"n1"}


async def test_duplicate_vote_from_same_peer_counts_once(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3", "n4", "n5"])
    await election.become_candidate(node)
    term = node.current_term
    for _ in range(3):
        await node.handle_vote_response(
            NodeId("n2"), RequestVoteReply(term=term, vote_granted=True)
        )
    assert node.role is Role.CANDIDATE
    assert node.votes_received == {"n1", "n2"}


async def test_higher_term_reply_makes_candidate_step_down(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    await election.become_candidate(node)
    await node.handle_vote_response(NodeId("n2"), RequestVoteReply(term=7, vote_granted=False))
    assert node.role is Role.FOLLOWER
    assert node.current_term == 7
    assert node.voted_for is None
    assert node.votes_received == set()


async def test_vote_from_earlier_term_is_ignored(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    await election.become_candidate(node)  # term 1
    await election.become_candidate(node)  # term 2, after a split vote
    await node.handle_vote_response(NodeId("n2"), RequestVoteReply(term=1, vote_granted=True))
    assert node.role is Role.CANDIDATE
    assert node.votes_received == {"n1"}


async def test_vote_after_election_ends_is_ignored(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    await election.become_candidate(node)
    term = node.current_term
    await election.become_follower(node, term)  # e.g. another leader's heartbeat
    await node.handle_vote_response(NodeId("n2"), RequestVoteReply(term=term, vote_granted=True))
    assert node.role is Role.FOLLOWER


# ----------------------------------------------------------------------
# Role transitions
# ----------------------------------------------------------------------


async def test_become_candidate_increments_term_and_votes_for_self(
    make_node: NodeFactory,
) -> None:
    node = make_node("n1", ["n2", "n3"])
    node.current_term = 4
    node.leader_id = NodeId("n2")
    await election.become_candidate(node)
    assert node.role is Role.CANDIDATE
    assert node.current_term == 5
    assert node.voted_for == "n1"
    assert node.votes_received == {"n1"}
    assert node.leader_id is None


async def test_become_follower_with_higher_term_clears_vote(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    node.current_term = 2
    node.voted_for = NodeId("n3")
    await election.become_follower(node, 3)
    assert node.role is Role.FOLLOWER
    assert node.current_term == 3
    assert node.voted_for is None


async def test_become_follower_in_same_term_keeps_vote(make_node: NodeFactory) -> None:
    # Clearing the vote here would let the node vote twice in one term.
    node = make_node("n1", ["n2", "n3"])
    await election.become_candidate(node)
    term = node.current_term
    await election.become_follower(node, term)
    assert node.current_term == term
    assert node.voted_for == "n1"


async def test_leader_stepping_down_stops_heartbeat_and_starts_timer(
    make_node: NodeFactory,
) -> None:
    node = make_node("n1", ["n2", "n3"])
    node.running = True
    await election.become_candidate(node)
    await election.become_leader(node)
    heartbeat = node.heartbeat_task
    assert heartbeat is not None
    assert node.election_timer_task is None

    await election.become_follower(node, node.current_term + 1)

    assert node.heartbeat_task is None
    assert node.election_timer_task is not None
    await asyncio.sleep(0)
    assert heartbeat.done()


# ----------------------------------------------------------------------
# start_election
# ----------------------------------------------------------------------


async def test_single_node_cluster_elects_itself(make_node: NodeFactory) -> None:
    node = make_node("n1", [])
    await node.start_election()
    assert node.role is Role.LEADER
    assert node.current_term == 1


async def test_election_wins_with_all_peers_voting(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node)
    n1 = cluster[NodeId("n1")]
    await n1.start_election()

    assert n1.role is Role.LEADER
    assert n1.current_term == 1
    assert {peer for peer, _ in client.vote_requests} == {"n2", "n3"}
    for peer in ("n2", "n3"):
        assert cluster[NodeId(peer)].voted_for == "n1"
        assert cluster[NodeId(peer)].current_term == 1


async def test_request_vote_carries_candidate_log_position(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node)
    n1 = cluster[NodeId("n1")]
    n1.current_term = 3
    n1.log = [entry(1, 1), entry(2, 3)]
    await n1.start_election()
    _, request = client.vote_requests[0]
    assert request == RequestVote(
        term=4, candidate_id=NodeId("n1"), last_log_index=2, last_log_term=3
    )


async def test_election_wins_with_one_peer_down(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node)
    client.down.add(NodeId("n3"))
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    assert n1.role is Role.LEADER


async def test_election_fails_without_majority(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node)
    client.down.update({NodeId("n2"), NodeId("n3")})
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    assert n1.role is Role.CANDIDATE
    assert n1.current_term == 1


async def test_election_lost_to_more_up_to_date_logs(make_node: NodeFactory) -> None:
    cluster, _ = make_cluster(make_node)
    for peer in ("n2", "n3"):
        cluster[NodeId(peer)].log = [entry(1, 1)]
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    assert n1.role is Role.CANDIDATE
    assert leaders(cluster) == []


async def test_candidate_steps_down_when_peer_has_higher_term(make_node: NodeFactory) -> None:
    cluster, _ = make_cluster(make_node)
    cluster[NodeId("n2")].current_term = 9
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    assert n1.role is Role.FOLLOWER
    assert n1.current_term == 9


async def test_slow_peer_does_not_delay_victory(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node)
    slow_peer_released = asyncio.Event()
    original = client.send_request_vote

    async def send_request_vote(peer_id: NodeId, request: RequestVote) -> RequestVoteReply | None:
        if peer_id == "n3":
            await slow_peer_released.wait()
        return await original(peer_id, request)

    client.send_request_vote = send_request_vote  # type: ignore[method-assign]
    n1 = cluster[NodeId("n1")]
    election_task = asyncio.create_task(n1.start_election())
    await wait_for(lambda: n1.role is Role.LEADER)
    assert not election_task.done()
    slow_peer_released.set()
    await election_task
    assert n1.role is Role.LEADER


# ----------------------------------------------------------------------
# Timers and heartbeats
# ----------------------------------------------------------------------


async def test_stopped_node_never_starts_timer(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"])
    await election.reset_election_timer(node)
    assert node.election_timer_task is None


async def test_timeout_is_ignored_by_leader_and_stopped_node(make_node: NodeFactory) -> None:
    stopped = make_node("n1", ["n2", "n3"])
    await election.on_election_timeout(stopped)
    assert stopped.current_term == 0

    leader = make_node("n2", ["n1", "n3"])
    leader.running = True
    leader.current_term = 3
    await election.become_leader(leader)
    await election.on_election_timeout(leader)
    assert leader.role is Role.LEADER
    assert leader.current_term == 3


async def test_election_timer_starts_an_election(make_node: NodeFactory) -> None:
    node = make_node("n1", ["n2", "n3"], timeouts=FAST_TIMEOUTS, heartbeat=FAST_HEARTBEAT)
    node.running = True
    await election.reset_election_timer(node)
    await wait_for(lambda: node.current_term >= 1)
    assert node.role is Role.CANDIDATE
    assert node.voted_for == "n1"


async def test_heartbeats_keep_followers_from_timing_out(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node, running=True, timeouts=FAST_TIMEOUTS)
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    assert n1.role is Role.LEADER
    for peer in ("n2", "n3"):
        await election.reset_election_timer(cluster[NodeId(peer)])

    # Several election timeouts pass, but heartbeats keep resetting the timers.
    await asyncio.sleep(FAST_TIMEOUTS[1] * 5)

    assert n1.role is Role.LEADER
    assert n1.current_term == 1
    for peer in ("n2", "n3"):
        follower = cluster[NodeId(peer)]
        assert follower.role is Role.FOLLOWER
        assert follower.current_term == 1
        assert follower.leader_id == "n1"
    assert all(not request.entries for _, request in client.append_entries)
    assert len(client.append_entries) >= 4


async def test_heartbeat_stops_after_losing_leadership(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node, running=True)
    n1 = cluster[NodeId("n1")]
    await n1.start_election()
    await wait_for(lambda: len(client.append_entries) >= 2)

    await election.become_follower(n1, 5)
    await asyncio.sleep(0)
    sent = len(client.append_entries)
    await asyncio.sleep(FAST_HEARTBEAT * 5)
    assert len(client.append_entries) == sent


async def test_cluster_elects_exactly_one_leader(make_node: NodeFactory) -> None:
    cluster, _ = make_cluster(make_node, size=5, running=True, timeouts=FAST_TIMEOUTS)
    for node in cluster.values():
        await election.reset_election_timer(node)

    await wait_for(lambda: len(leaders(cluster)) == 1)
    # Let heartbeats settle, then check nobody else claims leadership.
    await asyncio.sleep(FAST_TIMEOUTS[1] * 3)

    current = leaders(cluster)
    assert len(current) == 1
    leader = current[0]
    for node in cluster.values():
        assert node.current_term == leader.current_term
        assert node.leader_id == leader.node_id


async def test_new_leader_elected_after_leader_fails(make_node: NodeFactory) -> None:
    cluster, client = make_cluster(make_node, running=True, timeouts=FAST_TIMEOUTS)
    for node in cluster.values():
        await election.reset_election_timer(node)
    await wait_for(lambda: len(leaders(cluster)) == 1)
    old_leader = leaders(cluster)[0]
    old_term = old_leader.current_term

    # Crash the leader: it stops running and becomes unreachable.
    old_leader.running = False
    await election.stop_heartbeat(old_leader)
    client.down.add(old_leader.node_id)

    survivors = [n for n in cluster.values() if n is not old_leader]
    await wait_for(lambda: any(n.role is Role.LEADER for n in survivors))
    new_leader = next(n for n in survivors if n.role is Role.LEADER)
    assert new_leader.current_term > old_term
