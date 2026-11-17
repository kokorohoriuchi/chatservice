"""Raft log replication and commit tracking.

Every function here takes the RaftNode as its first argument and mutates that
node's state directly. RaftNode's methods are thin delegations to these
functions, so this module is where the replication logic is implemented.

The leader's heartbeat task lives in election.py, because a heartbeat exists to
suppress elections. It calls broadcast_append_entries below.

Log indices are 1-based: entry i is node.log[i - 1], and index 0 means "before
the first entry" with term 0.

Everything runs on a single asyncio loop, so there are no thread races, but the
node's term, role and log can change across every await. Every handler that
awaits re-checks the state it depends on afterwards.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from raft import election
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    LogEntry,
    NodeId,
    ProposalResult,
    Role,
)

if TYPE_CHECKING:
    from raft.node import RaftNode

logger = logging.getLogger(__name__)

# Most entries sent in one AppendEntries, so a follower that is far behind
# catches up in several bounded RPCs instead of one that may time out.
MAX_ENTRIES_PER_RPC = 100

# Replication tasks started by propose. asyncio keeps only weak references to
# tasks, so a reference is held here until each one finishes.
_background_tasks: set[asyncio.Task[None]] = set()


# ----------------------------------------------------------------------
# Log helpers
# ----------------------------------------------------------------------


def _term_at(node: RaftNode, index: int) -> int:
    """Return the term of entry index, or 0 for index 0."""
    if index == 0:
        return 0
    # Indices are 1-based, the Python list is 0-based.
    return node.log[index - 1].term


def _entries_from(node: RaftNode, index: int, limit: int | None = None) -> list[LogEntry]:
    """Return the entries starting at index, at most limit of them."""
    entries = node.log[index - 1 :]
    return entries if limit is None else entries[:limit]


def _has_matching_entry(node: RaftNode, index: int, term: int) -> bool:
    """Return whether the log holds an entry at index with the given term.

    This is Raft's Log Matching check: if two logs agree on the term at an
    index, they agree on every entry up to that index.
    """
    if index == 0:
        # Every log "contains" the empty prefix.
        return True
    return index <= node.last_log_index and _term_at(node, index) == term


def committed_entries(
    node: RaftNode, from_index: int = 1, to_index: int | None = None
) -> list[LogEntry]:
    """Return the committed entries in [from_index, to_index].

    For serving chat history requests. to_index defaults to commit_index, and is
    capped at it, so an uncommitted entry is never returned.
    """
    last = node.commit_index if to_index is None else min(to_index, node.commit_index)
    first = max(from_index, 1)
    if first > last:
        return []
    return node.log[first - 1 : last]


def init_leader_state(node: RaftNode) -> None:
    """Reset next_index and match_index for every peer.

    Called by election.become_leader before the first heartbeat.
    """
    # Optimistically assume each follower is up to date. A rejected
    # AppendEntries walks next_index back until the logs match.
    node.next_index = {peer_id: node.last_log_index + 1 for peer_id in node.peers}
    # Nothing is known to be replicated on any follower yet.
    node.match_index = {peer_id: 0 for peer_id in node.peers}


# ----------------------------------------------------------------------
# Proposals
# ----------------------------------------------------------------------


async def propose(node: RaftNode, command: Command) -> ProposalResult:
    """Append a command to the log, or forward it to the leader.

    accepted=True means the entry was appended to the leader's log, not that it
    is committed. The chat server broadcasts only on COMMITTED_ENTRY. There is no
    automatic retry: a retry after a lost reply could append the message twice.
    """
    # Leader: the leader alone decides the order of messages, so it appends the
    # new entry at the end of its own log in the current term.
    if node.role == Role.LEADER:
        entry = LogEntry(index=node.last_log_index + 1, term=node.current_term, command=command)
        node.log.append(entry)
        logger.debug("node %s appended entry %d term %d", node.node_id, entry.index, entry.term)
        if node.peers:
            # Replicate in the background so the proposer gets its answer
            # without waiting for the slowest follower.
            _spawn(broadcast_append_entries(node))
        else:
            # A single node is its own majority, and no reply will ever arrive.
            await advance_commit_index(node)
        return ProposalResult(
            accepted=True, index=entry.index, term=entry.term, leader_id=node.node_id
        )

    # Not the leader: forward to the leader if one is known.
    leader_id = node.leader_id
    if node.role == Role.CANDIDATE or leader_id is None:
        # An election is in progress. The client resends once a leader exists.
        return ProposalResult(accepted=False, term=node.current_term, error="no leader")
    if node.peer_client is None:
        return ProposalResult(
            accepted=False, term=node.current_term, leader_id=leader_id, error="not started"
        )

    result = await node.peer_client.send_forward_proposal(leader_id, command)
    if result is None:
        # Leader crashed or is partitioned away. Report it instead of retrying,
        # since the leader may have appended the entry before the reply was lost.
        return ProposalResult(
            accepted=False,
            term=node.current_term,
            leader_id=leader_id,
            error="leader unreachable",
        )
    return result


async def handle_forward_proposal(node: RaftNode, command: Command) -> ProposalResult:
    """Accept a follower's forwarded proposal when this node is the leader.

    A non-leader rejects it and names the leader it knows. It never forwards a
    second time, which avoids forwarding loops during an election.
    """
    if node.role == Role.LEADER:
        return await propose(node, command)
    return ProposalResult(
        accepted=False, term=node.current_term, leader_id=node.leader_id, error="not leader"
    )


# ----------------------------------------------------------------------
# Follower side
# ----------------------------------------------------------------------


async def handle_append_entries(node: RaftNode, request: AppendEntries) -> AppendEntriesReply:
    """Apply a leader's replication or heartbeat message to the local log."""
    # 1. Reject a leader from an old term. Do not reset the election timer:
    #    an outdated leader must not keep this node from starting an election.
    if request.term < node.current_term:
        return AppendEntriesReply(term=node.current_term, success=False, match_index=0)

    # 2. A valid leader exists for this term. Step down if this node is behind
    #    in term, or is a candidate that lost the race in the same term.
    if request.term > node.current_term or node.role != Role.FOLLOWER:
        await election.become_follower(node, request.term)
    node.leader_id = request.leader_id
    await election.reset_election_timer(node)

    # 3. Consistency check: the entry just before the new ones must match the
    #    leader's. If not, reject and let the leader send from further back.
    if not _has_matching_entry(node, request.prev_log_index, request.prev_log_term):
        # Hint lets the leader jump back instead of stepping one entry at a time.
        hint = min(node.last_log_index, request.prev_log_index - 1)
        return AppendEntriesReply(term=node.current_term, success=False, match_index=hint)

    # 4. Append the new entries, replacing any conflicting suffix.
    for offset, entry in enumerate(request.entries):
        index = request.prev_log_index + 1 + offset
        if index <= node.last_log_index:
            if _term_at(node, index) == entry.term:
                # Already have it. Never truncate on a match: a delayed or
                # duplicate AppendEntries must not erase newer entries.
                continue
            # Conflict: this entry and everything after it came from a leader
            # that lost. A committed entry can never be in conflict, so
            # reaching one here means a safety bug, and we fail loudly.
            if index <= node.commit_index:
                raise RuntimeError(
                    f"node {node.node_id} asked to overwrite committed entry {index}"
                )
            del node.log[index - 1 :]
        node.log.append(entry)

    # 5. Advance the commit index from the leader's, but only over entries
    #    this message confirmed to match the leader's log.
    match = request.prev_log_index + len(request.entries)
    if request.leader_commit > node.commit_index:
        # Entries past match were not checked against this leader, so they
        # cannot be marked committed from this message. max() keeps the
        # commit index from moving backwards on a delayed message.
        node.commit_index = max(node.commit_index, min(request.leader_commit, match))

    # 6. Push any newly committed entries to the local chat server.
    await apply_committed_entries(node)
    return AppendEntriesReply(term=node.current_term, success=True, match_index=match)


# ----------------------------------------------------------------------
# Leader side
# ----------------------------------------------------------------------


async def replicate_to_peer(node: RaftNode, peer_id: NodeId) -> None:
    """Send the entries a follower is missing, then handle its reply.

    On a log mismatch this retries right away with an earlier next_index, in a
    loop rather than by recursion, until the follower accepts. An unreachable
    peer is left for the next heartbeat.
    """
    while node.role == Role.LEADER and node.peer_client is not None:
        # Snapshot the term: if it changes while we wait for the reply, the
        # reply belongs to an old leadership and must be ignored.
        term = node.current_term
        next_index = node.next_index.get(peer_id, node.last_log_index + 1)
        prev_index = next_index - 1
        # An empty entries list is a heartbeat.
        request = AppendEntries(
            term=term,
            leader_id=node.node_id,
            prev_log_index=prev_index,
            prev_log_term=_term_at(node, prev_index),
            entries=_entries_from(node, next_index, MAX_ENTRIES_PER_RPC),
            leader_commit=node.commit_index,
        )

        reply = await node.peer_client.send_append_entries(peer_id, request)
        if reply is None:
            # Peer is down or unreachable. The next heartbeat tries again.
            return
        if reply.term > node.current_term:
            # A newer term exists: step down, even if the reply is late.
            await handle_append_entries_reply(node, peer_id, reply)
            return
        if node.role != Role.LEADER or node.current_term != term:
            return  # Stale: this node lost leadership while waiting.

        await handle_append_entries_reply(node, peer_id, reply)
        if reply.success:
            if node.next_index[peer_id] > node.last_log_index:
                return  # Follower is fully caught up.
            continue  # More entries than one RPC carries: keep sending.
        if prev_index == 0:
            # prev 0 always matches, so a rejection here is not a log mismatch.
            logger.error("peer %s rejected AppendEntries at index 0", peer_id)
            return
        # Log mismatch: next_index was moved back, so loop and send again.


async def handle_append_entries_reply(
    node: RaftNode, peer_id: NodeId, reply: AppendEntriesReply
) -> None:
    """Update a follower's replication progress and advance the commit index.

    Steps down on a higher term. On a rejected reply it moves next_index back;
    replicate_to_peer then sends again. On success it advances the commit index.
    """
    if reply.term > node.current_term:
        await election.become_follower(node, reply.term)
        return
    if node.role != Role.LEADER:
        return

    if reply.success:
        # max: a delayed reply must not lower the follower's progress.
        node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), reply.match_index)
        node.next_index[peer_id] = node.match_index[peer_id] + 1
        # More replicas may mean a new majority, so check for new commits.
        await advance_commit_index(node)
    else:
        # Move back at least one entry, or straight to the follower's hint if
        # that is further back. Never below 1, the first log index.
        current = node.next_index.get(peer_id, node.last_log_index + 1)
        node.next_index[peer_id] = max(1, min(current - 1, reply.match_index + 1))


async def advance_commit_index(node: RaftNode) -> None:
    """Advance commit_index to the highest index replicated on a majority.

    Only an entry from the current term is committed by counting replicas
    (Raft section 5.4.2). Earlier entries commit with it.
    """
    if node.role != Role.LEADER:
        return

    # Walk down from the newest entry and stop at the first one a majority has.
    for index in range(node.last_log_index, node.commit_index, -1):
        if _term_at(node, index) != node.current_term:
            # An entry from an earlier term could still be overwritten by a
            # future leader even if a majority has it (Raft Figure 8), so it is
            # never committed by counting. Terms only fall going back, so no
            # earlier index qualifies either.
            break
        # The leader always has its own entries, hence the 1.
        replicas = 1 + sum(1 for match in node.match_index.values() if match >= index)
        if replicas >= node.majority:
            node.commit_index = index
            logger.debug("node %s committed up to %d", node.node_id, index)
            break

    await apply_committed_entries(node)


async def apply_committed_entries(node: RaftNode) -> None:
    """Apply newly committed entries and notify the local chat server.

    last_applied moves before each await, so two overlapping calls never push
    the same entry twice. A failed push is logged and not retried: a chat server
    that missed it recovers through a history request.
    """
    while node.last_applied < node.commit_index:
        # Claim the entry first, then push it. Another call running during the
        # await below then starts from the next entry instead of this one.
        node.last_applied += 1
        entry = node.log[node.last_applied - 1]
        if node.chat_api is None:
            continue  # No chat server attached, e.g. in tests.
        try:
            await node.chat_api.notify_committed_entry(entry)
        except Exception:
            # One broken chat server connection must not stop later entries.
            logger.exception("node %s failed to push entry %d", node.node_id, entry.index)


async def broadcast_append_entries(node: RaftNode) -> None:
    """Send an AppendEntries to every peer, in parallel.

    A slow or dead peer does not hold up the others. Used for both new entries
    and heartbeats.
    """
    peer_ids = list(node.peers)
    # return_exceptions=True: one peer's failure does not cancel the others.
    results = await asyncio.gather(
        *(replicate_to_peer(node, peer_id) for peer_id in peer_ids), return_exceptions=True
    )
    for peer_id, result in zip(peer_ids, results, strict=True):
        if isinstance(result, BaseException):
            logger.error(
                "replication to %s failed",
                peer_id,
                exc_info=(type(result), result, result.__traceback__),
            )


def _spawn(coroutine: Coroutine[Any, Any, None]) -> None:
    """Run a coroutine in the background, holding a reference until it ends."""
    task = asyncio.create_task(coroutine)
    _background_tasks.add(task)
    # Drop the reference once the task is done, so the set does not grow.
    task.add_done_callback(_background_tasks.discard)
