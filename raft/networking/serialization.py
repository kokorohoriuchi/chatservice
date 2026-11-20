"""Wire format for every connection in the project.

Messages are newline-delimited JSON. `json.dumps` escapes any newline inside a
string, so a newline is an message boundary.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from enum import Enum
from typing import Any

from raft.types import (
    AppendEntries,
    AppendEntriesReply,
    Command,
    LogEntry,
    NodeId,
    ProposalResult,
    RequestVote,
    RequestVoteReply,
)


class PeerMessageType(Enum):
    """The kinds of message that travel between Raft peers."""

    REQUEST_VOTE = "REQUEST_VOTE"
    REQUEST_VOTE_REPLY = "REQUEST_VOTE_REPLY"
    APPEND_ENTRIES = "APPEND_ENTRIES"
    APPEND_ENTRIES_REPLY = "APPEND_ENTRIES_REPLY"
    FORWARD_PROPOSAL = "FORWARD_PROPOSAL"
    FORWARD_PROPOSAL_REPLY = "FORWARD_PROPOSAL_REPLY"


def to_dict(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: to_dict(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_dict(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: to_dict(item) for key, item in asdict(value).items()}
    return value


async def write_message(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    """Write one newline-delimited JSON message and drain the writer."""
    writer.write((json.dumps(to_dict(payload)) + "\n").encode())
    await writer.drain()


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    """Read one newline-delimited JSON message, or None on a clean end of stream."""
    line = await reader.readline()
    if not line:
        return None
    decoded: dict[str, Any] = json.loads(line)
    return decoded


def command_from_dict(data: dict[str, Any]) -> Command:
    """Rebuild a Command from a decoded message payload."""
    return Command.from_dict(data)


def entry_from_dict(data: dict[str, Any]) -> LogEntry:
    """Rebuild a LogEntry from a decoded message payload."""
    return LogEntry.from_dict(data)


def proposal_result_from_dict(data: dict[str, Any]) -> ProposalResult:
    """Rebuild a ProposalResult from a decoded message payload."""
    leader_id = data.get("leader_id")
    return ProposalResult(
        accepted=data["accepted"],
        index=data.get("index"),
        term=data.get("term", 0),
        leader_id=NodeId(leader_id) if leader_id is not None else None,
        error=data.get("error"),
    )


def append_entries_from_dict(data: dict[str, Any]) -> AppendEntries:
    """Rebuild an AppendEntries from a decoded message payload."""
    return AppendEntries(
        term=data["term"],
        leader_id=NodeId(data["leader_id"]),
        prev_log_index=data["prev_log_index"],
        prev_log_term=data["prev_log_term"],
        entries=[LogEntry.from_dict(entry) for entry in data.get("entries", [])],
        leader_commit=data.get("leader_commit", 0),
    )


def append_entries_reply_from_dict(data: dict[str, Any]) -> AppendEntriesReply:
    """Rebuild an AppendEntriesReply from a decoded message payload."""
    return AppendEntriesReply(
        term=data["term"],
        success=data["success"],
        match_index=data["match_index"],
    )


def request_vote_from_dict(data: dict[str, Any]) -> RequestVote:
    """Rebuild a RequestVote from a decoded message payload."""
    return RequestVote(
        term=data["term"],
        candidate_id=NodeId(data["candidate_id"]),
        last_log_index=data["last_log_index"],
        last_log_term=data["last_log_term"],
    )


def request_vote_reply_from_dict(data: dict[str, Any]) -> RequestVoteReply:
    """Rebuild a RequestVoteReply from a decoded message payload."""
    return RequestVoteReply(term=data["term"], vote_granted=data["vote_granted"])
