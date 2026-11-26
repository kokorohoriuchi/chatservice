"""Chat message types and the client protocol.

This module owns the schema of Command.payload, which is why Raft can treat a
Command as opaque. It also defines the wire protocol between a chat client and a
chat server.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from raft.types import Command


class CommandType(Enum):
    """The kinds of command the chat state machine understands."""

    POST_MESSAGE = "POST_MESSAGE"


@dataclass(frozen=True)
class ChatMessage:
    """One chat message, as delivered to a chat client."""

    sender: str
    text: str
    log_index: int


class ClientMessageType(Enum):
    """The kinds of message a chat client sends to a chat server."""

    JOIN = "JOIN"                   # When client starts a connection with server 
    SEND_MESSAGE = "SEND_MESSAGE"   # When user submits a chat message


class ServerMessageType(Enum):
    """The kinds of message a chat server sends to a chat client."""

    NEW_MESSAGE = "NEW_MESSAGE" # Pushing new chat message to client
    JOIN_REPLY = "JOIN_REPLY"   # Reply newly joined client with chat history 
    SEND_MESSAGE_REPLY = "SEND_MESSAGE_REPLY"
    ERROR = "ERROR"
    
    
def to_dict(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: to_dict(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_dict(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: to_dict(item) for key, item in asdict(value).items()}
    return value


@dataclass(frozen=True)
class ClientMessage:
    """An envelope for anything a chat client sends."""

    type: ClientMessageType
    sender: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ServerMessage:
    """An envelope for anything a chat server sends."""

    type: ServerMessageType
    payload: dict[str, Any] = field(default_factory=dict)


def chat_message_to_dict(message: ChatMessage) -> dict[str, Any]:
    """Convert a ChatMessage into a JSON-serializable dictionary."""
    return {
        "sender": message.sender,
        "text": message.text,
        "log_index": message.log_index
    }


def chat_message_from_dict(data: dict[str, Any]) -> ChatMessage:
    """Rebuild a ChatMessage from a dictionary produced by chat_message_to_dict."""
    return ChatMessage(
        sender=data["sender"],
        text=data["text"],
        log_index=data["log_index"]
    )


def to_command(client_message: ClientMessage) -> Command:
    """Convert a client SEND_MESSAGE into the Command that Raft will replicate.

    Set Command.type from CommandType.POST_MESSAGE.value and put sender, text,
    in the payload. Called by ChatServer.handle_client_message.
    """
    return Command(
        type=CommandType.POST_MESSAGE,
        payload={
            "sender": client_message.sender,
            "text": client_message.payload["text"],
        }
    )


def from_command(command: Command, log_index: int) -> ChatMessage:
    """Rebuild a ChatMessage from a committed Command and its log index.

    Read the payload written by to_command. Called by ChatStateMachine.apply.
    """
    payload = command.payload
    return ChatMessage(
        sender=payload["sender"],
        text=payload["text"],
        log_index=log_index
    )


def encode_client_message(message: ClientMessage) -> dict[str, Any]:
    """Convert a ClientMessage into a JSON-serializable dictionary."""
    return {
        "type": message.type,
        "sender": message.sender,
        "payload": message.payload
    }


def decode_client_message(data: dict[str, Any]) -> ClientMessage:
    """Rebuild a ClientMessage from a dictionary produced by encode_client_message."""
    return ClientMessage(
        type=ClientMessageType(data["type"]),
        sender=data["sender"],
        payload=data["payload"]
    )


def encode_server_message(message: ServerMessage) -> dict[str, Any]:
    """Convert a ServerMessage into a JSON-serializable dictionary."""
    return {
        "type": message.type,
        "payload": message.payload
    }


def decode_server_message(data: dict[str, Any]) -> ServerMessage:
    """Rebuild a ServerMessage from a dictionary produced by encode_server_message."""
    return ServerMessage(
        type=data["type"],
        payload=data["payload"]
    )


async def write_message(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    """Write one newline-delimited JSON message and drain the writer."""
    writer.write((json.dumps(to_dict(payload)) + "\n").encode())
    await writer.drain()


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    """Read one newline-delimited JSON message, or None on a clean end of stream."""
    line = await reader.readline()
    if not line:
        return None
    decoded: dict[str, Any] = json.loads(line)
    return decoded