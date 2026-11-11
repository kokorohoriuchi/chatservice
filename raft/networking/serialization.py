"""Wire format for every TCP connection in the project.

Messages are newline-delimited JSON. `json.dumps` escapes any newline inside a
string, so a newline is an unambiguous message boundary and no length prefix is
needed.

The helpers here never inspect Command.payload, so this module has no dependency
on the chat application.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from enum import Enum
from typing import Any

from raft.types import (
    Command,
    LogEntry,
    NodeId,
    ProposalResult,
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
    """Convert a dataclass, enum, or container of them into plain JSON data."""
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
