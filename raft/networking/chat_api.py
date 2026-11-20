from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from raft.networking import serialization
from raft.types import Command, LogEntry

if TYPE_CHECKING:
    from raft.node import RaftNode

logger = logging.getLogger(__name__)


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
    """Serves the chat server and pushes committed entries to it."""

    def __init__(self, host: str, port: int, node: RaftNode) -> None:
        self.host = host
        self.port = port
        self.node = node
        self.server: asyncio.Server | None = None
        self.connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        """Start listening for connections from chat servers."""
        self.server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        logger.info("chat server listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        """Stop accepting connections and close every open chat server connection."""
        
        logger.info("stopping chat server on %s:%d", self.host, self.port)
        for writer in list(self.connections):
            writer.close()
        self.connections.clear()

        if self.server is None:
            return
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def notify_committed_entry(self, entry: LogEntry) -> None:
        """Disseminate a COMMITTED_ENTRY to every connected chat server."""
        await self._disseminate(
            ChatRaftMessageType.COMMITTED_ENTRY,
            {"entry": entry},
        )

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handler for connections with chat servers."""

        peer = writer.get_extra_info("peername")
        logger.info("chat server connected from %s", peer)
        self.connections.add(writer)
        try:
            while True:
                message = await serialization.read_message(reader)
                if message is None:
                    logger.info("chat server %s disconnected", peer)
                    return
                if message.get("type") != ChatRaftMessageType.PROPOSE.value:
                    logger.warning("chat server %s sent an unsupported message", peer)
                    continue
                payload = message.get("payload")
                if not isinstance(payload, dict):
                    logger.warning("chat server %s sent a malformed payload", peer)
                    continue
                logger.debug("handling PROPOSE from chat server %s", peer)
                command = serialization.command_from_dict(payload["command"])
                result = await self.node.propose(command)
                await serialization.write_message(
                    writer,
                    {
                        "type": ChatRaftMessageType.PROPOSAL_RESULT.value,
                        "payload": result,
                    },
                )
        except Exception as exc:
            logger.exception("error handling chat server connection from %s: %s", peer, exc)
            return
        finally:
            self.connections.discard(writer)
            writer.close()

    async def _disseminate(
        self, message_type: ChatRaftMessageType, payload: dict[str, object]
    ) -> None:
        """Write one message to every connected chat server."""
        message = {"type": message_type.value, "payload": payload}
        logger.debug(
            "disseminating %s to %d chat server(s)", message_type.value, len(self.connections)
        )
        for writer in list(self.connections):
            if writer.is_closing():
                self.connections.discard(writer)
                continue
            try:
                await serialization.write_message(writer, message)
            except Exception as exc:
                logger.warning(
                    "error disseminating %s to a chat server: %s", message_type.value, exc
                )
                self.connections.discard(writer)
                writer.close()
