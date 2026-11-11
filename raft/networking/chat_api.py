"""The local chat-to-Raft API.

This is the TCP interface between a chat server and its local Raft node. The
chat server sends PROPOSE and receives PROPOSAL_RESULT as the reply. The Raft
node pushes COMMITTED_ENTRY to every connected chat server without being asked.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from raft.types import Command, LogEntry

if TYPE_CHECKING:
    from raft.node import RaftNode


class ChatRaftMessageType(Enum):
    """The kinds of message that travel between a chat server and its Raft node."""

    PROPOSE = "PROPOSE"
    PROPOSAL_RESULT = "PROPOSAL_RESULT"
    COMMITTED_ENTRY = "COMMITTED_ENTRY"


@dataclass(frozen=True)
class ProposeRequest:
    """A chat server's request that a command be replicated."""

    command: Command


@dataclass(frozen=True)
class CommittedEntryNotification:
    """A push telling the chat server that an entry is now committed."""

    entry: LogEntry


class ChatApiServer:
    """Serves the local chat server over TCP and pushes committed entries to it.

    Constructed by RaftNode.start.
    """

    def __init__(self, host: str, port: int, node: RaftNode) -> None:
        """Store the bind address and the node whose events will be pushed."""
        self.host = host
        self.port = port
        self.node = node
        self.server: asyncio.Server | None = None
        self.connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        """Bind the listening socket and begin accepting chat server connections.

        Called by RaftNode.start.
        """
        raise NotImplementedError

    async def stop(self) -> None:
        """Close every chat server connection and the listening socket.

        Called by RaftNode.stop.
        """
        raise NotImplementedError

    async def notify_committed_entry(self, entry: LogEntry) -> None:
        """Push a COMMITTED_ENTRY to every connected chat server.

        Called by replication.apply_committed_entries. Reaches
        ChatServer.handle_committed_entry.
        """
        raise NotImplementedError

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Register a chat server and serve its proposals until it disconnects.

        Use serialization.read_message and write_message. Dispatch PROPOSE to
        RaftNode.propose and reply with PROPOSAL_RESULT.
        """
        raise NotImplementedError

    async def _broadcast(
        self, message_type: ChatRaftMessageType, payload: dict[str, object]
    ) -> None:
        """Write one message to every connected chat server.

        Called by notify_committed_entry.
        """
        raise NotImplementedError
