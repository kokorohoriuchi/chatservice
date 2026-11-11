"""Raft leader election.

Every function here takes the RaftNode as its first argument and mutates that
node's state directly. RaftNode's methods are thin delegations to these
functions, so this module is where the election logic is implemented.

This module also owns the leader's heartbeat task, because a heartbeat exists to
stop followers from starting elections. A heartbeat is an empty AppendEntries, so
the loop calls replication.broadcast_append_entries rather than building the
message itself. The role transitions below start and stop it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from raft.types import NodeId, RequestVote, RequestVoteReply

if TYPE_CHECKING:
    from raft.node import RaftNode


async def start_election(node: RaftNode) -> None:
    """Become a candidate and request votes from every peer.

    Call become_candidate, then PeerClient.send_request_vote for each
    peer, then handle_vote_response for each reply.
    """
    raise NotImplementedError


async def handle_request_vote(node: RaftNode, request: RequestVote) -> RequestVoteReply:
    """Decide whether to grant a candidate's vote request.

    Call become_follower when the request carries a higher term, and
    reset_election_timer when the vote is granted.
    """
    raise NotImplementedError


async def handle_vote_response(node: RaftNode, voter_id: NodeId, reply: RequestVoteReply) -> None:
    """Record a vote reply and become leader once a majority is reached.

    Call become_follower on a higher term, and become_leader on a majority.
    """
    raise NotImplementedError


async def reset_election_timer(node: RaftNode) -> None:
    """Restart the election countdown from a fresh random timeout.

    Schedule on_election_timeout as the timer callback. Called by
    replication.handle_append_entries and by the role transitions here.
    """
    raise NotImplementedError


async def on_election_timeout(node: RaftNode) -> None:
    """React to an expired election timer by starting a new election.

    Call start_election.
    """
    raise NotImplementedError


async def become_follower(node: RaftNode, term: int) -> None:
    """Step down to follower in the given term and stop any heartbeat task.

    Call replication.stop_heartbeat. Called by replication.py
    whenever a higher term is observed.
    """
    raise NotImplementedError


async def become_candidate(node: RaftNode) -> None:
    """Increment the term, vote for self, and reset the election timer.

    Call replication.stop_heartbeat and reset_election_timer.
    """
    raise NotImplementedError


async def become_leader(node: RaftNode) -> None:
    """Initialize leader bookkeeping and start sending heartbeats.

    Call replication.start_heartbeat.
    """
    raise NotImplementedError

async def start_heartbeat(node: RaftNode) -> None:
    """Start the leader's periodic heartbeat task.
    
    Interval is defined by node.config.heartbeat_interval
    """

    raise NotImplementedError


async def stop_heartbeat(node: RaftNode) -> None:
    """Stop the leader's periodic heartbeat task."""
    raise NotImplementedError