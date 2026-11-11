"""Interactive chat client.

Connects to a chat server, sends messages, and prints whatever the server
broadcasts. The client keeps its own view of the conversation, since the server
holds no history.
"""

from __future__ import annotations

import asyncio

from chat_server.message import ServerMessage


class ChatClient:
    """A minimal terminal chat client.

    Constructed by client/__main__.py.
    """

    def __init__(self, host: str, port: int, username: str) -> None:
        """Store the server address and the name this client sends under."""
        self.host = host
        self.port = port
        self.username = username
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.listen_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        """Open the connection and send a JOIN for this client's username.

        Called by client/__main__.py.
        """
        raise NotImplementedError

    async def send_message(self, text: str) -> None:
        """Send a SEND_MESSAGE to the chat server.

        Use message.encode_client_message.
        """
        raise NotImplementedError

    async def listen(self) -> None:
        """Read server messages and print them until the connection closes.

        Use serialization.read_message, then
        message.decode_server_message and _print_server_message.
        """
        raise NotImplementedError

    async def close(self) -> None:
        """Cancel the listener and close the connection."""
        raise NotImplementedError

    def _print_server_message(self, message: ServerMessage) -> None:
        """Render one server message to standard output.

        Called by listen.
        """
        raise NotImplementedError
