"""The chat server.

Accepts chat client connections, turns client messages into Raft proposals, and
broadcasts messages only once Raft has committed them. The server keeps no
history. It is a relay between its clients and its Raft node.
"""

from __future__ import annotations

import asyncio

from chat_server.message import ChatMessage, ClientMessage, ServerMessage
from chat_server.raft_client import RaftClient
from raft.types import Command, LogEntry, ProposalResult


class ChatServer:
    """Relays between chat clients and the local Raft node.

    Constructed by chat_server/__main__.py.
    """

    def __init__(
        self,
        host: str,
        port: int,
        raft_client: RaftClient,
    ) -> None:
        """Store the bind address and the Raft client."""
        self.host = host
        self.port = port
        self.raft_client = raft_client
        self.server: asyncio.Server | None = None
        self.clients: dict[str, asyncio.StreamWriter] = {}

    async def start_chat_server(self) -> None:
        """Register the Raft handler, connect to Raft, and accept chat clients.

        Call RaftClient.set_committed_entry_handler with handle_committed_entry,
        then RaftClient.connect, then bind the client listener. Called by
        chat_server/__main__.py.
        """
        raise NotImplementedError

    async def stop(self) -> None:
        """Close every client connection, the listener, and the Raft client.

        Call RaftClient.close. Called by chat_server/__main__.py.
        """
        raise NotImplementedError

    async def handle_client_message(self, client_id: str, message: ClientMessage) -> None:
        """Route one client message to the matching handler.

        SEND_MESSAGE calls propose_message with message.to_command, and sends ERROR if rejected.
        """
        raise NotImplementedError

    async def propose_message(self, command: Command) -> ProposalResult:
        """Propose a command to Raft and report whether it was accepted.

        """
        raise NotImplementedError

    async def handle_committed_entry(self, entry: LogEntry) -> None:
        """Convert a committed entry to a message and broadcast it.

        """
        raise NotImplementedError

    async def broadcast_message(self, message: ChatMessage) -> None:
        """Send a committed message to every connected chat client.

        Call _send_to_client for each entry in self.clients.
        """
        raise NotImplementedError

    async def _handle_client_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Register a chat client and serve its messages until it disconnects.

        Use serialization.read_message, then handle_client_message.
        """
        raise NotImplementedError

    async def _send_to_client(self, client_id: str, message: ServerMessage) -> None:
        """Write one message to a single chat client.

        Use serialization.write_message.
        """
        raise NotImplementedError
