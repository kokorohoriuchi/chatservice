"""The chat server's client for its local Raft node.

This is the chat server's side of the chat API defined in
raft/networking/chat_api.py. The connection is bidirectional, so a proposal is
awaited through a future while unsolicited committed entries are dispatched to a
registered handler.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from raft.types import Command, LogEntry, ProposalResult

CommittedEntryHandler = Callable[[LogEntry], Awaitable[None]]

# A proposal that gets no answer in this long is treated as failed. Without a
# bound, a chat server whose Raft node died would wait forever and the client
# would never see an error.
PROPOSAL_TIMEOUT = 2.0


class RaftClient:
    """Talks to the local Raft node over the chat API.

    Constructed by chat_server/__main__.py.
    """

    def __init__(self, host: str, port: int, proposal_timeout: float = PROPOSAL_TIMEOUT) -> None:
        """Store the Raft node's chat API address and prepare an empty handler."""
        self.host = host
        self.port = port
        self.proposal_timeout = proposal_timeout
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.listen_task: asyncio.Task[None] | None = None
        self.pending_proposal: asyncio.Future[ProposalResult] | None = None
        self.committed_entry_handler: CommittedEntryHandler | None = None

    async def connect(self) -> None:
        """Open the connection to the local Raft node and start listening.

        Start _listen as a background task. Called by
        ChatServer.start_chat_server.
        """
        raise NotImplementedError

    async def propose(self, command: Command) -> ProposalResult:
        """Send a PROPOSE and await the matching PROPOSAL_RESULT.

        A missing connection, a write error, or a silent Raft node all come back
        as a rejected result rather than an exception, so the chat server can
        always answer the client.

        Write the request, then await self.pending_proposal, which _listen
        resolves. Called by ChatServer.propose_message.
        """
        raise NotImplementedError

    def set_committed_entry_handler(self, handler: CommittedEntryHandler) -> None:
        """Register the callback for pushed COMMITTED_ENTRY messages.

        Called by ChatServer.start_chat_server with handle_committed_entry.
        """
        raise NotImplementedError

    async def close(self) -> None:
        """Cancel the listener and close the connection.

        Called by ChatServer.stop.
        """
        raise NotImplementedError

    async def _listen(self) -> None:
        """Read pushed messages and resolve the pending proposal when its reply arrives.

        Use serialization.read_message. Dispatch COMMITTED_ENTRY to
        self.committed_entry_handler and PROPOSAL_RESULT to self.pending_proposal.
        (serialization.proposal_result_from_dict, serialization.entry_from_dict())
        """
        raise NotImplementedError
