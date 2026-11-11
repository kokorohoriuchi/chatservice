"""Inbound Raft peer listener.

This server is inbound only. It handles exactly three message types and replies
on the same connection. It never receives a *_REPLY message, because replies
travel back on the connection the caller opened.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from raft.node import RaftNode


class PeerServer:
    """Accepts Raft peer connections and dispatches requests to the local node.

    Constructed by RaftNode.start.
    """

    def __init__(self, host: str, port: int, node: RaftNode) -> None:
        """Store the bind address and the node that will handle every request."""
        self.host = host
        self.port = port
        self.node = node
        self.server: asyncio.Server | None = None
        self.connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        """Bind the listening socket and begin accepting peer connections.

        Called by RaftNode.start.
        """
        raise NotImplementedError

    async def stop(self) -> None:
        """Stop accepting connections and close every open peer connection.

        Called by RaftNode.stop.
        """
        raise NotImplementedError

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Read messages, dispatch each to the node, and write the reply.

        Use serialization.read_message and write_message. Dispatch REQUEST_VOTE
        to RaftNode.handle_request_vote, APPEND_ENTRIES to
        RaftNode.handle_append_entries, and FORWARD_PROPOSAL to
        RaftNode.handle_forward_proposal.
        """
        raise NotImplementedError
