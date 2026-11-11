"""Raft log replication and commit tracking.

Every function here takes the RaftNode as its first argument and mutates that
node's state directly. RaftNode's methods are thin delegations to these
functions, so this module is where the replication logic is implemented.

The leader's heartbeat task lives in election.py, because a heartbeat exists to
suppress elections. It calls broadcast_append_entries below.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    NodeId,
    ProposalResult,
)

if TYPE_CHECKING:
    from raft.node import RaftNode


async def propose(node: RaftNode, command: Command) -> ProposalResult:
    """Append a command to the log, or forward it to the leader.

    Call PeerClient.send_forward_proposal when this node is a
    follower, and replicate_to_peer when it is the leader.
    """
    raise NotImplementedError


async def handle_append_entries(node: RaftNode, request: AppendEntries) -> AppendEntriesReply:
    """Apply a leader's replication or heartbeat message to the local log.

    Call election.become_follower and election.reset_election_timer,
    then apply_committed_entries.
    """
    raise NotImplementedError


async def handle_append_entries_reply(
    node: RaftNode, peer_id: NodeId, reply: AppendEntriesReply
) -> None:
    """Update a follower's replication progress and advance the commit index.

    Call election.become_follower on a higher term, replicate_to_peer
    on a rejected reply, and advance_commit_index on success.
    """
    raise NotImplementedError


async def handle_forward_proposal(node: RaftNode, command: Command) -> ProposalResult:
    """Accept a follower's forwarded proposal when this node is the leader.

    Call election.become_follower on a higher term, then propose.
    """
    raise NotImplementedError


async def advance_commit_index(node: RaftNode) -> None:
    """Advance commit_index to the highest index replicated on a majority.

    Call apply_committed_entries.
    """
    raise NotImplementedError


async def apply_committed_entries(node: RaftNode) -> None:
    """Apply newly committed entries and notify the local chat server.

    Call ChatApiServer.notify_committed_entry for each new entry.
    """
    raise NotImplementedError


async def replicate_to_peer(node: RaftNode, peer_id: NodeId) -> None:
    """Send the entries a follower is missing, then handle its reply.

    Call PeerClient.send_append_entries, then
    handle_append_entries_reply.
    """
    raise NotImplementedError


async def broadcast_append_entries(node: RaftNode) -> None:
    """Send an AppendEntries to every peer, in parallel.

    Call replicate_to_peer for each peer.
    """
    raise NotImplementedError


async def start_heartbeat(node: RaftNode) -> None:
    """Start the leader's periodic heartbeat task.

    The task calls broadcast_append_entries. Called by election.become_leader.
    """
    raise NotImplementedError


async def stop_heartbeat(node: RaftNode) -> None:
    """Cancel the leader's heartbeat task, if one is running.

    Called by election.become_follower and election.become_candidate
    on every step-down.
    """
    raise NotImplementedError
