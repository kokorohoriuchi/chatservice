"""Shared Raft types.

This module is the frozen contract between all four work areas. Every other
module imports its message and state types from here, so a change to this file
requires agreement from all four people.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, NewType

# A Raft node's identity, used as the key in every peer mapping.
NodeId = NewType("NodeId", str)


class Role(Enum):
    """The three roles a Raft node can hold."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class Command:
    """An opaque state machine command carried by a log entry.

    Raft never inspects payload. chat_server/message.py owns its schema.
    """

    type: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Command:
        """Rebuild a Command from a dictionary produced by dataclasses.asdict."""
        return cls(type=data["type"], payload=dict(data.get("payload", {})))


@dataclass(frozen=True)
class LogEntry:
    """One entry in the replicated log."""

    index: int
    term: int
    command: Command

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogEntry:
        """Rebuild a LogEntry from a dictionary produced by dataclasses.asdict."""
        return cls(
            index=data["index"],
            term=data["term"],
            command=Command.from_dict(data["command"]),
        )


@dataclass(frozen=True)
class RequestVote:
    """A candidate's request for a peer's vote in a given term."""

    term: int
    candidate_id: NodeId
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    """A peer's answer to a RequestVote."""

    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    """A leader's log replication or heartbeat message.

    An empty entries list is a heartbeat.
    """

    term: int
    leader_id: NodeId
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass(frozen=True)
class AppendEntriesReply:
    """A follower's answer to an AppendEntries."""

    term: int
    success: bool
    match_index: int


@dataclass(frozen=True)
class ProposalResult:
    """The outcome of a proposal, returned to the proposing chat server.

    Also used as the FORWARD_PROPOSAL_REPLY payload.
    """

    accepted: bool
    index: int | None = None
    term: int = 0
    leader_id: NodeId | None = None
    error: str | None = None


@dataclass(frozen=True)
class PeerAddress:
    """The network location of one Raft peer."""

    node_id: NodeId
    host: str
    port: int


@dataclass(frozen=True)
class RaftConfig:
    """Everything a RaftNode needs in order to run."""

    node_id: NodeId
    peers: dict[NodeId, PeerAddress]
    peer_host: str
    peer_port: int
    chat_api_host: str
    chat_api_port: int
    election_timeout_range: tuple[float, float] = (0.15, 0.30)
    heartbeat_interval: float = 0.05

    def __post_init__(self) -> None:
        """Reject a heartbeat interval that is not safely below the election timeout."""
        low, high = self.election_timeout_range
        if low <= 0 or high <= low:
            raise ValueError(f"invalid election_timeout_range: {self.election_timeout_range!r}")
        if self.heartbeat_interval <= 0 or self.heartbeat_interval >= low:
            raise ValueError(
                f"heartbeat_interval {self.heartbeat_interval!r} must be positive and "
                f"below the lower election timeout {low!r}"
            )
