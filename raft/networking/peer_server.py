from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from raft.networking import serialization

if TYPE_CHECKING:
    from raft.node import RaftNode

logger = logging.getLogger(__name__)


class PeerServer:
    """Inbound Raft peer listener.

    Handles the received RPC messages and dispatches them.
    """

    def __init__(self, host: str, port: int, node: RaftNode) -> None:
        self.host = host
        self.port = port
        self.node = node
        self.server: asyncio.Server | None = None
        self.connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        """Start listening for connections from peers."""

        self.server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        logger.info("peer server listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        """Stop accepting connections and close every open peer connection."""

        logger.info("stopping peer server on %s:%d", self.host, self.port)
        for writer in list(self.connections):
            writer.close()
        self.connections.clear()

        if self.server is None:
            return
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handler for connections from peers."""
        
        peer = writer.get_extra_info("peername")
        logger.info("peer connected from %s", peer)
        self.connections.add(writer)
        try:
            while True:
                message = await serialization.read_message(reader)
                if message is None:
                    logger.info("peer %s disconnected", peer)
                    return
                reply = await self._dispatch(message)
                if reply is None:
                    logger.warning("peer %s sent an unsupported message", peer)
                    return
                await serialization.write_message(writer, reply)
        except Exception as exc:
            logger.exception("error handling peer connection from %s: %s", peer, exc)
            return
        finally:
            self.connections.discard(writer)
            writer.close()

    async def _dispatch(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """Map a received RPC message to the corresponding handler and return the reply"""

        raw_type = message.get("type")
        payload = message.get("payload")
        if not isinstance(raw_type, str) or not isinstance(payload, dict):
            logger.warning("received malformed peer message %r", message)
            return None

        try:
            message_type = serialization.PeerMessageType(raw_type)
        except ValueError:
            logger.warning("received unknown peer message type %r", raw_type)
            return None

        logger.debug("dispatching peer message %s", message_type.value)

        if message_type is serialization.PeerMessageType.REQUEST_VOTE:
            request = serialization.request_vote_from_dict(payload)
            reply = await self.node.handle_request_vote(request)
            return {
                "type": serialization.PeerMessageType.REQUEST_VOTE_REPLY.value,
                "payload": reply,
            }

        if message_type is serialization.PeerMessageType.APPEND_ENTRIES:
            append_request = serialization.append_entries_from_dict(payload)
            append_reply = await self.node.handle_append_entries(append_request)
            return {
                "type": serialization.PeerMessageType.APPEND_ENTRIES_REPLY.value,
                "payload": append_reply,
            }

        if message_type is serialization.PeerMessageType.FORWARD_PROPOSAL:
            command = serialization.command_from_dict(payload["command"])
            result = await self.node.handle_forward_proposal(command)
            return {
                "type": serialization.PeerMessageType.FORWARD_PROPOSAL_REPLY.value,
                "payload": result,
            }

        logger.warning("received unhandled peer message type %s", message_type.value)
        return None
