from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from raft.networking import serialization
from raft.types import Command, HistoryReply, LogEntry

if TYPE_CHECKING:
    from raft.node import RaftNode

logger = logging.getLogger(__name__)


class ChatRaftMessageType(Enum):
    """The kinds of message that travel between a chat server and its Raft node."""

    PROPOSE_REQUEST = "PROPOSE_REQUEST"
    PROPOSAL_RESULT = "PROPOSAL_RESULT"
    COMMITTED_ENTRY = "COMMITTED_ENTRY"
    HISTORY_REQUEST = "HISTORY_REQUEST"
    HISTORY_REPLY = "HISTORY_REPLY"


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
                raw_type = message.get("type")
                payload = message.get("payload")
                if not isinstance(payload, dict):
                    logger.warning("chat server %s sent a malformed payload", peer)
                    continue
                if raw_type == ChatRaftMessageType.PROPOSE_REQUEST.value:
                    await self._handle_propose_request(writer, peer, payload)
                elif raw_type == ChatRaftMessageType.HISTORY_REQUEST.value:
                    await self._handle_history_request(writer, peer, payload)
                else:
                    logger.warning("chat server %s sent an unsupported message", peer)
                    continue
        except Exception as exc:
            logger.exception("error handling chat server connection from %s: %s", peer, exc)
            return
        finally:
            self.connections.discard(writer)
            writer.close()

    async def _handle_propose_request(
        self, writer: asyncio.StreamWriter, peer: Any, payload: dict[str, Any]
    ) -> None:
        """Propose a command and write the PROPOSAL_RESULT back."""
        logger.debug("handling PROPOSE_REQUEST from chat server %s", peer)
        command = serialization.command_from_dict(payload["command"])
        result = await self.node.propose(command)
        await serialization.write_message(
            writer,
            {
                "type": ChatRaftMessageType.PROPOSAL_RESULT.value,
                "payload": result,
            },
        )

    async def _handle_history_request(
        self, writer: asyncio.StreamWriter, peer: Any, payload: dict[str, Any]
    ) -> None:
        """Serve committed entries from the log and write a HISTORY_REPLY back."""
        logger.debug("handling HISTORY_REQUEST from chat server %s", peer)
        request = serialization.history_request_from_dict(payload)
        entries = self.node.committed_entries(request.from_index, request.to_index)
        await serialization.write_message(
            writer,
            {
                "type": ChatRaftMessageType.HISTORY_REPLY.value,
                "payload": HistoryReply(entries=entries),
            },
        )

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
