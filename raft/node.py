"""The Raft node.

RaftNode owns every piece of shared Raft state and every networking handle. The
election and replication logic lives in election.py and replication.py as free
functions that take the node, so this class stays a thin, readable surface with
no duplicated attribute declarations.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from raft import election, replication
from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    LogEntry,
    NodeId,
    PeerAddress,
    ProposalResult,
    RaftConfig,
    RequestVote,
    RequestVoteReply,
    Role,
)

if TYPE_CHECKING:
    from raft.networking.chat_api import ChatApiServer
    from raft.networking.peer_client import PeerClient
    from raft.networking.peer_server import PeerServer


class RaftNode:
    """One Raft consensus participant, owning all shared Raft state.

    The election and replication logic lives in election.py and replication.py
    as free functions that take this node.
    """

    def __init__(self, config: RaftConfig) -> None:
        """Store the configuration and initialize every field to its follower default."""
        self.config = config
        self.node_id: NodeId = config.node_id
        self.peers: dict[NodeId, PeerAddress] = dict(config.peers)

        self.role: Role = Role.FOLLOWER
        self.current_term: int = 0
        self.voted_for: NodeId | None = None
        self.leader_id: NodeId | None = None

        self.log: list[LogEntry] = []
        self.commit_index: int = 0
        self.last_applied: int = 0

        self.next_index: dict[NodeId, int] = {}
        self.match_index: dict[NodeId, int] = {}
        self.votes_received: set[NodeId] = set()

        self.running: bool = False
        self.election_timer_task: asyncio.Task[None] | None = None
        self.heartbeat_task: asyncio.Task[None] | None = None

        self.peer_server: PeerServer | None = None
        self.peer_client: PeerClient | None = None
        self.chat_api: ChatApiServer | None = None

    # ------------------------------------------------------------------
    # Shared derived state. Both election.py and replication.py need these,
    # so they live here rather than being duplicated.
    # ------------------------------------------------------------------

    @property
    def last_log_index(self) -> int:
        """Return the index of the last log entry, or 0 when the log is empty."""
        return len(self.log)

    @property
    def last_log_term(self) -> int:
        """Return the term of the last log entry, or 0 when the log is empty."""
        return self.log[-1].term if self.log else 0

    @property
    def majority(self) -> int:
        """Return the number of votes needed to win an election."""
        return (len(self.peers) + 1) // 2 + 1

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Wire up networking, start both servers, and begin the election timer."""
        from raft.networking.chat_api import ChatApiServer
        from raft.networking.peer_client import PeerClient
        from raft.networking.peer_server import PeerServer

        self.peer_client = PeerClient(self.peers)
        self.peer_server = PeerServer(self.config.peer_host, self.config.peer_port, self)
        self.chat_api = ChatApiServer(
            self.config.chat_api_host, self.config.chat_api_port, self
        )

        await self.peer_server.start()
        await self.chat_api.start()

        self.running = True
        await election.reset_election_timer(self)

    async def stop(self) -> None:
        """Stop the election timer, both servers, and the peer client."""
        self.running = False

        if self.election_timer_task is not None:
            self.election_timer_task.cancel()
            self.election_timer_task = None

        await election.stop_heartbeat(self)

        if self.peer_server is not None:
            await self.peer_server.stop()
        if self.chat_api is not None:
            await self.chat_api.stop()
        if self.peer_client is not None:
            await self.peer_client.close()

    # ------------------------------------------------------------------
    # Election
    # ------------------------------------------------------------------

    async def start_election(self) -> None:
        """Begin a new election term and request votes from every peer."""
        await election.start_election(self)

    async def handle_request_vote(self, request: RequestVote) -> RequestVoteReply:
        """Decide whether to grant a candidate's vote request."""
        return await election.handle_request_vote(self, request)

    async def handle_vote_response(self, voter_id: NodeId, reply: RequestVoteReply) -> None:
        """Record a vote reply and become leader once a majority is reached."""
        await election.handle_vote_response(self, voter_id, reply)

    # ------------------------------------------------------------------
    # Replication
    # ------------------------------------------------------------------

    async def propose(self, command: Command) -> ProposalResult:
        """Append a command to the log, or forward it to the leader."""
        return await replication.propose(self, command)

    async def handle_append_entries(self, request: AppendEntries) -> AppendEntriesReply:
        """Apply a leader's replication or heartbeat message to the local log."""
        return await replication.handle_append_entries(self, request)

    async def handle_append_entries_reply(
        self, peer_id: NodeId, reply: AppendEntriesReply
    ) -> None:
        """Update a follower's replication progress and advance the commit index."""
        await replication.handle_append_entries_reply(self, peer_id, reply)

    async def handle_forward_proposal(self, command: Command) -> ProposalResult:
        """Accept a follower's forwarded proposal when this node is the leader."""
        return await replication.handle_forward_proposal(self, command)

    async def advance_commit_index(self) -> None:
        """Advance commit_index to the highest index replicated on a majority."""
        await replication.advance_commit_index(self)

    async def apply_committed_entries(self) -> None:
        """Apply newly committed entries and notify the local chat server."""
        await replication.apply_committed_entries(self)
