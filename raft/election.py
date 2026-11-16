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

import asyncio
from typing import TYPE_CHECKING

from raft.types import NodeId, RequestVote, RequestVoteReply, Role

if TYPE_CHECKING:
    from raft.node import RaftNode


async def start_election(node: RaftNode) -> None:
    """Become a candidate and request votes from every peer.

    Call become_candidate, then PeerClient.send_request_vote for each
    peer, then handle_vote_response for each reply.
    """
    await become_candidate(node)

    # A single-node cluster wins on its own vote
    if node.role is Role.CANDIDATE and len(node.votes_received) >= node.majority:
        await become_leader(node)
        return

    # peer clients are created in RaftNode.start, so this should never be None
    peer_client = node.peer_client
    if peer_client is None:
        return

    request = RequestVote(
        term=node.current_term,
        candidate_id=node.node_id,
        last_log_index=node.last_log_index,
        last_log_term=node.last_log_term,
    )

    async def request_vote_from(peer_id: NodeId) -> None:
        # Send a RequestVote to a peer and handle the reply, if any.
        reply = await peer_client.send_request_vote(peer_id, request)
        if reply is not None:
            await handle_vote_response(node, peer_id, reply)

    # Ask every peer at once and handle each reply as it arrives, so a majority
    # makes this node leader without waiting on slow or unreachable peers.
    await asyncio.gather(*(request_vote_from(peer_id) for peer_id in node.peers)) # Request votes from all peers concurrently


async def handle_request_vote(node: RaftNode, request: RequestVote) -> RequestVoteReply:
    """Decide whether to grant a candidate's vote request.

    Call become_follower when the request carries a higher term, and
    reset_election_timer when the vote is granted.
    """
    # If the request's term is higher, update term and become follower.
    if request.term > node.current_term:
        await become_follower(node, request.term)

    # If the request's term is lower, reject it.
    if request.term < node.current_term:
        return RequestVoteReply(term=node.current_term, vote_granted=False)

    # If the request's term is equal, grant the vote if haven't voted yet
    can_vote = node.voted_for is None or node.voted_for == request.candidate_id

    # Last log term and index must be at least as up-to-date as this node's log.
    # Last log index is used as a tiebreaker when last log terms are equal.
    log_ok = (request.last_log_term, request.last_log_index) >= (
        node.last_log_term,
        node.last_log_index,
    )

    # Conditions met, grant vote and reset election timer.
    if can_vote and log_ok:
        node.voted_for = request.candidate_id
        await reset_election_timer(node)
        return RequestVoteReply(term=node.current_term, vote_granted=True)

    # Reject vote
    return RequestVoteReply(term=node.current_term, vote_granted=False)


async def handle_vote_response(node: RaftNode, voter_id: NodeId, reply: RequestVoteReply) -> None:
    """Record a vote reply and become leader once a majority is reached.

    Call become_follower on a higher term, and become_leader on a majority.
    """
    # If the reply's term is higher, update term and become follower.
    if reply.term > node.current_term:
        await become_follower(node, reply.term)
        return

    # Ignore replies once the election is over, and replies from an earlier
    # election that arrive after this node has moved to a newer term.
    if node.role is not Role.CANDIDATE or reply.term != node.current_term:
        return

    # Ignore replies that don't grant a vote
    if not reply.vote_granted:
        return
    
    # Record the vote and check for a majority
    node.votes_received.add(voter_id)
    if len(node.votes_received) >= node.majority:
        await become_leader(node)


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