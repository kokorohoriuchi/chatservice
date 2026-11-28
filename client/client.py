"""Interactive chat client.

Connects to a chat server, sends messages, and prints whatever the server
broadcasts. The client keeps its own view of the conversation, since the server
holds no history.
"""

from __future__ import annotations

import asyncio
import logging

import chat_server.message as csmessage
from chat_server.message import (
    ChatMessage,
    ClientMessage,
    ClientMessageType,
    ServerMessage,
    ServerMessageType,
)
from client.history import HistoryWriter

logger = logging.getLogger(__name__)

class ChatClient:
    """A minimal terminal chat client.

    Constructed by client/__main__.py.
    """

    def __init__(self, 
                 host: str, 
                 port: int, 
                 username: str,
                 history_filename: str, 
                 request_timeout: float = 0.5
        ) -> None:
        """Store the server address and the name this client sends under."""
        self.host = host
        self.port = port
        self.username = username
        self.history_writer = HistoryWriter(history_filename)
        self.request_timeout = request_timeout
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.listen_task: asyncio.Task[None] | None = None
        self.pending_send_message_reply: asyncio.Future[ServerMessage] | None = None
        self.pending_join_reply: asyncio.Future[ServerMessage] | None = None

    async def connect(self) -> None:
        """Open the connection and send a JOIN for this client's username.

        Called by client/__main__.py.
        """
        logger.debug("Connecting to server %s:%s", self.host, self.port)
        
        # Starting connection to server
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host,
                self.port,
            )
        except Exception as exc:
            logger.warning(
                "Failed to connect to server at %s:%d: %r",
                self.host, self.port, exc
            )
            raise
        
        # Starting listener
        self.listen_task = asyncio.create_task(self.listen())
            
        # Sending server JOIN message and expecting a reply containing chat history
        join_message = ClientMessage(
            type=ClientMessageType.JOIN,
            sender=self.username,
            payload={}
        )
        
        self.pending_join_reply = asyncio.get_running_loop().create_future()
        
        logger.debug("Sending JOIN to server %s:%s", self.host, self.port)
        
        try:
            await csmessage.write_message(
                self.writer, 
                csmessage.encode_client_message(join_message)
            )
        except Exception as exc:
            logger.warning(
                "error sending %s to a server %r: %s", 
                ClientMessageType.SEND_MESSAGE.value, self.host, exc
            )
            raise exc
            
        try:
            history = await asyncio.wait_for(
                self.pending_join_reply,
                timeout=self.request_timeout,
            )
        except TimeoutError:
            logger.warning("Timed out while waiting for server's JOIN_REPLY")
            self.pending_join_reply = None
            return
        
        logger.debug("Received JOIN_REPLY from server %s:%s", self.host, self.port)
        logger.info("Connected to server at %s:%d", self.host, self.port)
        
        # Merge recent and local chat histories
        self.build_history(history)
        logger.info("Built chat history")

        
    async def send_message(self, message: ChatMessage) -> ServerMessage | None:
        """Send a SEND_MESSAGE to the chat server.

        """
        
        if self.writer is None:
            raise RuntimeError("Client is not connected to server")
    
        logger.debug("Sending SEND_MESSAGE to server %s:%s", self.host, self.port)

        client_message = ClientMessage(
            type=ClientMessageType.SEND_MESSAGE,
            sender=self.username,
            payload=csmessage.chat_message_to_dict(message)
        )
        
        self.pending_send_message_reply = asyncio.get_running_loop().create_future()
        
        try:
            await csmessage.write_message(
                self.writer, 
                csmessage.encode_client_message(client_message)
            )
        except Exception as exc:
            logger.warning(
                "error sending %s to a server %r: %s", 
                ClientMessageType.SEND_MESSAGE.value, self.host, exc
            )
            raise exc
            
        try:
            return await asyncio.wait_for(
                self.pending_send_message_reply,
                timeout=self.request_timeout,
            )
        except TimeoutError:
            logger.warning("Timed out waiting for server's SEND_MESSAGE_REPLY")
            self.pending_send_message_reply = None
            return None
        


    async def listen(self) -> None:
        """Read server messages and print them until the connection closes.

        Use serialization.read_message, then
        message.decode_server_message and _print_server_message.
        """
        
        if self.reader is None:
            raise RuntimeError("Client is not connected to server")
        
        logger.info("Listening to server %s:%s", self.host, self.port)
        
        try:
            while True:
                message = await csmessage.read_message(self.reader)
                
                if message is None:
                    logger.warning("received no server message %r", message)
                    break 
                
                try:
                    decoded_message = csmessage.decode_server_message(message)
                except ValueError:
                    logger.warning("received unknown server message type %s", message["type"])
                    continue
                except Exception as exc:
                    logger.warning("received malformed server message: %r", exc)
                    continue                
                
                logger.debug("handling %s message from server", decoded_message.type.value)
                        
                if decoded_message.type is ServerMessageType.NEW_MESSAGE:
                    self._print_server_message(decoded_message)
                    self.history_writer.append(
                        csmessage.chat_message_from_dict(decoded_message.payload) 
                    )
                    
                if decoded_message.type is ServerMessageType.SEND_MESSAGE_REPLY:
                    self._print_server_message(decoded_message)
                    if (
                        self.pending_send_message_reply is not None
                        and not self.pending_send_message_reply.done()
                    ):
                        self.pending_send_message_reply.set_result(decoded_message)
                    # TODO: Show delivered status on client
                    
                if decoded_message.type is ServerMessageType.JOIN_REPLY:
                    self._print_server_message(decoded_message)
                    if (
                        self.pending_join_reply is not None
                        and not self.pending_join_reply.done()
                    ):
                        self.pending_join_reply.set_result(decoded_message)
                
                if decoded_message.type is ServerMessageType.ERROR:
                    logger.warning("Client received ERROR from server")
                    # TODO: Handle this?
                            
        except Exception as exc:
            logger.exception(
                "error handling server connection from %s:%s : %r", 
                self.host, self.port, exc
            )
            return
        finally:
            logger.info(
                "Stopped listening to server %s:%d", self.host, self.port,
            )
            await self.close()
                

    async def close(self) -> None:
        """Cancel the listener and close the connection."""
        
        logger.debug("Cancelling listen_task, closing connection to server %s:%s", 
            self.host, self.port)
        
        if self.listen_task is not None:
            if asyncio.current_task() is not self.listen_task:
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    logger.info("Listener task to server %s:%d cancelled", self.host, self.port)
            self.listen_task = None

        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
            logger.info("Connection to server %s:%d closed", self.host, self.port)
            self.writer = None
            self.reader = None
            
    def build_history(self, message: ServerMessage) -> None:
        """Build the local chat history from a JOIN_REPLY.

        Convert the server's history snapshot into ChatMessages and
        merge it into the client's local history. If the local history
        cannot be merged, replace it with the server snapshot.
        """
        history: list[ChatMessage] = []

        log_entries = message.payload.get("entries", [])

        for log_entry in log_entries:
            command_payload = log_entry["command"]["payload"]
            history.append(
                ChatMessage(
                    sender=command_payload["sender"],
                    text=command_payload["text"],
                    log_index=log_entry["index"],
                )
            )

        try:
            self.history_writer.merge_history(history)
            logger.info("Merged %d fetched messages into local history", len(history))

        except Exception as exc:
            logger.warning("failed to merge server history: %s", exc)

            try:
                self.history_writer.replace_history(history)
                logger.info(
                    "Replaced local history with %d messages from server snapshot",
                    len(history),
                )

            except Exception as replace_exc:
                logger.warning(
                    "failed to rebuild local history from server snapshot: %s",
                    replace_exc,
                )
               
        
    def _print_server_message(self, message: ServerMessage) -> None:
        """Render one server message to standard output.
        
        """
        print(csmessage.encode_server_message(message))
