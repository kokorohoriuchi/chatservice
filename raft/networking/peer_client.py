"""Outbound Raft peer client.

Keeps one persistent connection per peer, lazily opened and reused, so a
heartbeat every few hundred milliseconds does not pay for a TCP handshake. Each
peer has a lock, so only one request is in flight at a time and the reply is
matched by request order on that connection.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

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


@dataclass
class _PeerConnection:
    """One open connection to a peer, holding both halves of the stream."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

class PeerClient:
    """Sends Raft RPCs to peers over persistent per-peer connections.

    Constructed by RaftNode.start.
    """

    def __init__(
        self, peers: dict[NodeId, PeerAddress], request_timeout: float = 1.0
    ) -> None:
        """Store the peer addresses and prepare an empty connection table.

        """
        self.peers = peers
        self.request_timeout = request_timeout
        self.connections: dict[NodeId, _PeerConnection] = {}
        self.locks: dict[NodeId, asyncio.Lock] = {peer_id: asyncio.Lock() for peer_id in peers}


    async def send_request_vote(
        self, peer_id: NodeId, request: RequestVote
    ) -> RequestVoteReply | None:
        """Ask one peer for its vote, or return None if it is unreachable.

        Awaited by election.start_election.
        """
        raise NotImplementedError

    async def send_append_entries(
        self, peer_id: NodeId, request: AppendEntries
    ) -> AppendEntriesReply | None:
        """Replicate entries or send a heartbeat to one peer, or None if unreachable.

        Awaited by replication.replicate_to_peer.
        """
        raise NotImplementedError

    async def send_forward_proposal(
        self, peer_id: NodeId, command: Command
    ) -> ProposalResult | None:
        """Ask the leader to append a command on this node's behalf.

        Awaited by replication.propose when this node is a follower.
        The leader replies with a ProposalResult.
        """
        raise NotImplementedError

    async def close(self) -> None:
        """Close every open peer connection.

        Called by RaftNode.stop.
        """
        raise NotImplementedError
