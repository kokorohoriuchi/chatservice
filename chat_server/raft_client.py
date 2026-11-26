"""The chat server's client for its local Raft node.

This is the chat server's side of the chat API defined in
raft/networking/chat_api.py. The connection is bidirectional, so a proposal is
awaited through a future while unsolicited committed entries are dispatched to a
registered handler.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from raft.networking import serialization
from raft.networking.chat_api import ChatRaftMessageType
from raft.types import Command, HistoryReply, LogEntry, ProposalResult

CommittedEntryHandler = Callable[[LogEntry], Awaitable[None]]

logger = logging.getLogger(__name__)

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
        self.pending_history: asyncio.Future[HistoryReply] | None = None
        self.committed_entry_handler: CommittedEntryHandler | None = None

    async def connect(self) -> None:
        """Open the connection to the local Raft node and start listening.

        Start _listen as a background task. Called by
        ChatServer.start_chat_server.
        """
        logger.debug("Connecting to raft node %s:%s", self.host, self.port)
        
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, 
                self.port,
            )
        except Exception as exc:
            logger.warning(
                "failed to connect to raft node at %s:%d: %s",
                self.host, self.port, exc
            )
            raise
        self.listen_task = asyncio.create_task(self._listen())
        

    async def propose(self, command: Command) -> ProposalResult:
        """Send a PROPOSE_REQUEST and await the matching PROPOSAL_RESULT.

        A missing connection, a write error, or a silent Raft node all come back
        as a rejected result rather than an exception, so the chat server can
        always answer the client.

        Write the request, then await self.pending_proposal, which _listen
        resolves. Called by ChatServer.propose_message.
        """
        logger.debug("Sending PROPOSE_REQUEST to raft node %s:%s", 
                     self.host, self.port)
        
        if self.writer is None:
            return ProposalResult(
                accepted=False,
                error="self.writer is None"
            )

        self.pending_proposal = asyncio.get_running_loop().create_future()

        try:
            await serialization.write_message(
                writer=self.writer,
                payload=
                {
                    "type": ChatRaftMessageType.PROPOSE_REQUEST.value,
                    "payload": command
                }
            )
        except Exception:
            error_str = "Error at write_message()"
            logger.debug("Sending PROPOSE_REQUEST failed: %s", error_str)
            return ProposalResult(
                accepted=False,
                error=error_str
            )

        try:
            return await asyncio.wait_for(
                self.pending_proposal,
                timeout=self.proposal_timeout,
            )
        except TimeoutError:
            logger.debug("Receiving PROPOSAL_RESULT timed out")
            self.pending_proposal = None
            return ProposalResult(
                accepted=False,
                error="Raft node timeout"
            )
        

    def set_committed_entry_handler(self, handler: CommittedEntryHandler) -> None:
        """Register the callback for pushed COMMITTED_ENTRY messages.

        Called by ChatServer.start_chat_server with handle_committed_entry.
        """
        self.committed_entry_handler = handler


    async def request_history(
        self, from_index: int = 1, to_index: int | None = None
    ) -> HistoryReply:
        """Send a HISTORY_REQUEST and await the matching HISTORY_REPLY.

        Write the request, then await self.pending_history, which _listen
        resolves. Called by ChatServer.request_history.
        """
        
        logger.debug("Sending HISTORY_REQUEST to raft node %s:%s", self.host, self.port)
        
        if self.writer is None:
            raise ConnectionError("Not connected to Raft node")
            
        self.pending_history = asyncio.get_running_loop().create_future()

        try:
            await serialization.write_message(
                writer=self.writer,
                payload=
                {
                    "type": ChatRaftMessageType.HISTORY_REQUEST.value,
                    "payload": {
                        "from_index": from_index,
                        "to_index": to_index,
                    }
                }
            )
        except Exception as exc:
            logger.warning(
                "error sending %s to a raft node %r: %s", 
                ChatRaftMessageType.HISTORY_REQUEST.value, self.host, exc
            )
            raise exc
            
        return await asyncio.wait_for(
            self.pending_history,
            timeout=self.proposal_timeout,
        )
            

    async def close(self) -> None:
        """Cancel the listener and close the connection.

        Called by ChatServer.stop.
        """      
        logger.debug("Cancelling listen_task, closing connection to raft node %s:%s", 
            self.host, self.port)
        
        if self.listen_task is not None:
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                logger.info("Listener task to raft node %s:%d cancelled", self.host, self.port)
            self.listen_task = None

        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
            logger.info("Connection to raft node %s:%d closed", self.host, self.port)
            self.writer = None
            self.reader = None


    async def _listen(self) -> None:
        """Read pushed messages and resolve the pending proposal when its reply arrives.

        Use serialization.read_message. Dispatch COMMITTED_ENTRY to
        self.committed_entry_handler, PROPOSAL_RESULT to self.pending_proposal,
        and HISTORY_REPLY to self.pending_history.
        (serialization.proposal_result_from_dict, serialization.entry_from_dict,
        serialization.history_reply_from_dict)
        """
        logger.info("Listening to raft node %s:%s", self.host, self.port)
                   
        try:
            while True:
                message = await serialization.read_message(self.reader)
                
                if message is None:
                    logger.warning("received malformed raft node message %r", message)
                    continue 
                        
                raw_type = message.get("type")
                payload = message.get("payload")
                if not isinstance(raw_type, str) or not isinstance(payload, dict):
                    logger.warning("received malformed raft node message %r", message)
                    continue
                
                try:
                    message_type = ChatRaftMessageType(raw_type)
                except ValueError:
                    logger.warning("received unknown raft node message type %r", raw_type)
                    continue
                        
                logger.debug("dispatching message from raft node: %s", message_type.value)
                        
                if message_type is ChatRaftMessageType.COMMITTED_ENTRY:
                    entry = serialization.entry_from_dict(payload)
                    if self.committed_entry_handler is not None:
                        await self.committed_entry_handler(entry)
                    
                if message_type is ChatRaftMessageType.PROPOSAL_RESULT:
                    proposal_result = serialization.proposal_result_from_dict(payload)
                    if (
                        self.pending_proposal is not None
                        and not self.pending_proposal.done()
                    ):
                        self.pending_proposal.set_result(proposal_result)
                
                if message_type is ChatRaftMessageType.HISTORY_REPLY:
                    history_reply = serialization.history_reply_from_dict(payload)
                    if (
                        self.pending_history is not None
                        and not self.pending_history.done()
                    ):
                        self.pending_history.set_result(history_reply)
                    
        except Exception as exc:
            logger.exception(
                "error handling raft node connection from %s:%s : %s", 
                self.host, self.port, exc
            )
            return
        finally:
            logger.info(
                "Stopped listening to raft node %s:%d", self.host, self.port,
            )
            await self.close()