"""Chat message types and the client protocol.

This module owns the schema of Command.payload, which is why Raft can treat a
Command as opaque. It also defines the wire protocol between a chat client and a
chat server.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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

    SEND_MESSAGE = "SEND_MESSAGE"


class ServerMessageType(Enum):
    """The kinds of message a chat server sends to a chat client."""

    MESSAGE = "MESSAGE"
    ERROR = "ERROR"


@dataclass(frozen=True)
class ClientMessage:
    """An envelope for anything a chat client sends."""

    type: ClientMessageType
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ServerMessage:
    """An envelope for anything a chat server sends."""

    type: ServerMessageType
    payload: dict[str, Any] = field(default_factory=dict)


def chat_message_to_dict(message: ChatMessage) -> dict[str, Any]:
    """Convert a ChatMessage into a JSON-serializable dictionary."""
    raise NotImplementedError


def chat_message_from_dict(data: dict[str, Any]) -> ChatMessage:
    """Rebuild a ChatMessage from a dictionary produced by chat_message_to_dict."""
    raise NotImplementedError


def to_command(client_message: ClientMessage) -> Command:
    """Convert a client SEND_MESSAGE into the Command that Raft will replicate.

    Set Command.type from CommandType.POST_MESSAGE.value and put sender, text,
    in the payload. Called by ChatServer.handle_client_message.
    """
    raise NotImplementedError


def from_command(command: Command, log_index: int) -> ChatMessage:
    """Rebuild a ChatMessage from a committed Command and its log index.

    Read the payload written by to_command. Called by ChatStateMachine.apply.
    """
    raise NotImplementedError


def encode_client_message(message: ClientMessage) -> dict[str, Any]:
    """Convert a ClientMessage into a JSON-serializable dictionary."""
    raise NotImplementedError


def decode_client_message(data: dict[str, Any]) -> ClientMessage:
    """Rebuild a ClientMessage from a dictionary produced by encode_client_message."""
    raise NotImplementedError


def encode_server_message(message: ServerMessage) -> dict[str, Any]:
    """Convert a ServerMessage into a JSON-serializable dictionary."""
    raise NotImplementedError


def decode_server_message(data: dict[str, Any]) -> ServerMessage:
    """Rebuild a ServerMessage from a dictionary produced by encode_server_message."""
    raise NotImplementedError
