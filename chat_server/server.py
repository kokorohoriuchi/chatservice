"""The chat server.

Accepts chat client connections, turns client messages into Raft proposals, and
broadcasts messages only once Raft has committed them. The server keeps no
history. It is a relay between its clients and its Raft node.
"""

from __future__ import annotations

import asyncio
import logging

import chat_server.message as csmessage
import raft.networking.serialization as serialization
from chat_server.message import (
    ChatMessage,
    ClientMessage,
    ClientMessageType,
    ServerMessage,
    ServerMessageType,
)
from chat_server.raft_client import RaftClient
from raft.types import Command, LogEntry

logger = logging.getLogger(__name__)


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
        self.raft_client.set_committed_entry_handler(self.handle_committed_entry)
        
        await self.raft_client.connect()
        
        self.server = await asyncio.start_server(
            self._handle_client_connection, self.host, self.port
        )
        
        logger.info("Server listening on %s:%d", self.host, self.port)
        

    async def stop(self) -> None:
        """Close every client connection, the listener, and the Raft client.

        Call RaftClient.close. Called by chat_server/__main__.py.
        """
        logger.info(
            "Stopping server %s:%d, client connections, listener, and raft client", 
            self.host, self.port
        )
        
        # Close client connections
        for writer in self.clients.values():
            writer.close()
        await asyncio.gather(
            *(writer.wait_closed() for writer in self.clients.values()),
            return_exceptions=True,
        )
        self.clients.clear()
        
        # Close listener 
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            
        # Close raft client
        await self.raft_client.close()
        
        
    async def handle_client_message(
        self, client_id: str, message: ClientMessage, writer: asyncio.StreamWriter
    ) -> None:
        """Route one client message to the matching handler.

        SEND_MESSAGE calls propose_message with message.to_command, and sends ERROR if rejected.
        """
        if message.type is ClientMessageType.JOIN:
            await self.handle_join(client_id, writer)
            await self.request_history(client_id, from_index=1, to_index=None)
                                    
        elif message.type is ClientMessageType.SEND_MESSAGE:
            command = csmessage.to_command(message)
            await self.handle_propose_message(command, client_id)
        
        else:
            logger.warning("Unknown message type %s from client %s",
                           message.type, client_id
            )
            await self._send_to_client(
                client_id, 
                ServerMessage(type=ServerMessageType.ERROR)
            )
   
    async def handle_join(
        self, client_id: str, writer: asyncio.StreamWriter
    ) -> None:
        self.clients[client_id] = writer
        
        logger.info("Client %s connected to server", client_id)

                

    async def handle_propose_message(
        self, command: Command, client_id : str
    ) -> None:
        """Propose a command to Raft and report whether it was accepted.

        """
        result = await self.raft_client.propose(command)
        logger.info("proposal is accepted: %s, \t\t reason: %s", 
                    result.accepted, result.error)
        if result.accepted:
            await self._send_to_client(
                client_id,
                ServerMessage(
                    type=ServerMessageType.SEND_MESSAGE_REPLY,
                    payload={
                        "accepted": True,
                        "error": None
                    },
                ),
            )
        else:
            await self._send_to_client(
                client_id,
                ServerMessage(
                    type=ServerMessageType.SEND_MESSAGE_REPLY,
                    payload={
                        "accepted": False,
                        "error": result.error
                    },
                ),
            )
        
            

    async def handle_committed_entry(self, entry: LogEntry) -> None:
        """Convert a committed entry to a message and broadcast it.

        """
        msg = ChatMessage(
            sender=entry.command.payload["sender"],
            text=entry.command.payload["text"],
            log_index=entry.index
        )
        
        await self.broadcast_message(msg)
        
        logger.info("Committed entry for chat message sent by %s broadcasted", msg.sender)
        
        
        
    async def request_history(
        self, client_id: str, from_index: int = 1, to_index: int | None = None
    ) -> None:
        """Serve a client's history request from the local Raft node.

        Call RaftClient.request_history and send the entries to the client.
        """
        try:
            history_reply = await self.raft_client.request_history(from_index, to_index)
        except Exception as exc:
            logger.warning(
                "server %s failed to forward client %s's history request to raft node: %r", 
                self.host, client_id, exc
            )
            return
        
        server_msg = ServerMessage(
            type=ServerMessageType.JOIN_REPLY,
            payload=serialization.to_dict(history_reply)
        )
        
        await self._send_to_client(client_id, server_msg)
        
        logger.info("History reply forwarded to Client %s", client_id)
        
        

    async def broadcast_message(self, message: ChatMessage) -> None:
        """Send a committed message to every connected chat client.

        Call _send_to_client for each entry in self.clients.
        """
        server_msg = ServerMessage(
            type=ServerMessageType.NEW_MESSAGE,
            payload=csmessage.chat_message_to_dict(message)
        )
        
        logger.info("Broadcasting %s to all clients", ServerMessageType.NEW_MESSAGE.value)
        await asyncio.gather(
            *(
                self._send_to_client(client_id, server_msg)
                for client_id in self.clients
            ),
            return_exceptions=True,
        )
        

    async def _handle_client_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Register a chat client and serve its messages until it disconnects.

        Use serialization.read_message, then handle_client_message.
        """
        peer = writer.get_extra_info("peername")
        raw_client_id: str | None = None
        
        try:
            while True:
                message = await csmessage.read_message(reader)
                if message is None:
                    logger.info("client %s disconnected", peer)
                    return
                raw_type = message.get("type")
                raw_client_id = message.get("sender")
                payload = message.get("payload")
                
                if (
                    not isinstance(raw_type, str) 
                    or not isinstance(raw_client_id, str) 
                    or not isinstance(payload, dict)
                ):
                    logger.warning("client sent malformed raft node message %r", message)
                    continue
                
                client_message = csmessage.decode_client_message(message)
                    
                await self.handle_client_message(
                    client_id=raw_client_id,
                    message=client_message,
                    writer=writer
                )
                
        except Exception as exc:
            logger.exception(
                "error handling client %s connection from %s: %s", 
                raw_client_id, peer, exc
            )
            return
        
        finally:
            if raw_client_id is not None:
                self.clients.pop(raw_client_id, None)
            writer.close()
            await writer.wait_closed()

    async def _send_to_client(self, client_id: str, message: ServerMessage) -> None:
        """Write one message to a single chat client.

        Use serialization.write_message.
        """
        writer = self.clients[client_id]
        await csmessage.write_message(
            writer, 
            csmessage.encode_server_message(message)
        )
