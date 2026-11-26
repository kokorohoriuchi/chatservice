"""Tests for chat_server/ and client/."""

# tests/test_chat_server_raft_integration.py

import asyncio
import logging
from typing import Any

import pytest

import chat_server.message as csmessage
from chat_server.raft_client import RaftClient
from chat_server.server import ChatServer
from raft.networking import serialization
from raft.networking.chat_api import ChatRaftMessageType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

RAFT_IP = "127.0.0.1"
SERVER_IP = "127.0.0.2"
CLIENT_IP ="127.0.0.3"

class FakeClient:
    def __init__(self, host: str, port: str, client_id: str) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.reader = None
        self.writer = None

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    async def send(self, payload: dict[str, Any]) -> None:
        await csmessage.write_message(self.writer,payload)

    async def receive(self) -> dict[str, Any] | None:
        return await csmessage.read_message(self.reader)


class FakeRaftNode:
    def __init__(self, host : str) -> None:
        self.host = host
        self.server = None
        self.reader = None
        self.writer = None
        self.received_messages = []
        self.client_connected = asyncio.Event()

    async def start(self) -> str:
        self.server = await asyncio.start_server(
            self._handle_connection, self.host, 0)
        return self.server.sockets[0].getsockname()[1] 

    async def stop(self) -> None:
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()

        self.server.close()
        await self.server.wait_closed()

    async def _handle_connection(
        self, 
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.client_connected.set()

        while True:
            msg = await serialization.read_message(reader)
            if msg is None:
                return
            self.received_messages.append(msg)

    async def send_proposal_result(
        self, 
        accepted: bool = True, 
        error: str | None = None
    ) -> None:
        await serialization.write_message(
            self.writer,
            {
                "type": ChatRaftMessageType.PROPOSAL_RESULT.value,
                "payload": {
                    "accepted": accepted,
                    "error": error,
                },
            },
        )

    async def send_history_reply(self, payload: dict[str, Any]) -> None:
        await serialization.write_message(
            self.writer,
            {
                "type": ChatRaftMessageType.HISTORY_REPLY.value,
                "payload": payload,
            },
        )

    async def send_committed_entry(self, payload: dict[str, Any]) -> None:
        await serialization.write_message(
            self.writer,
            {
                "type": ChatRaftMessageType.COMMITTED_ENTRY.value,
                "payload": payload,
            },
        )

async def init_test_env() -> None:
    raft_node = FakeRaftNode(RAFT_IP)
    raft_port = await raft_node.start()
    raft_client = RaftClient(RAFT_IP, raft_port)
    
    server = ChatServer(SERVER_IP, 0, raft_client)

    logger.info("starting chat server")
    await server.start_chat_server()
    logger.info("Chat server started")
    await raft_node.client_connected.wait()

    server_port = (server.server.sockets[0].getsockname()[1])
    client = FakeClient(SERVER_IP, server_port, "alice")

    await client.connect()
    
    return raft_node, server, client
       
            

# ----------------------------------------------------------------------
# chat_server/server.py and chat_server/raft_client.py
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_client_joins() -> None:
    """
    Test behaviour of client joining the server and getting chat history in the reply.
    """
    
    raft_node, server, client = await init_test_env()

    await client.send(
        {
            "type": "JOIN",
            "sender": "alice",
            "payload": {},
        }
    )

    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    assert (
        raft_node.received_messages[-1]["type"]
        == "HISTORY_REQUEST"
    )

    await raft_node.send_history_reply(
        {
            "entries": [
                {
                    "index": 1,
                    "term": 1,
                    "command": {
                        "type": "SEND_MESSAGE",
                        "payload": {
                            "sender": "alice",
                            "text": "Hello everyone!",
                        },
                    },
                },
                {
                    "index": 2,
                    "term": 1,
                    "command": {
                        "type": "SEND_MESSAGE",
                        "payload": {
                            "sender": "bob",
                            "text": "Hi Alice!",
                        },
                    },
                },
            ],
        }
    )
    
    join_reply = await client.receive()

    assert join_reply["type"] == "JOIN_REPLY"
    assert join_reply["payload"]["entries"][0]["command"]["payload"]["text"] == "Hello everyone!"


    

@pytest.mark.asyncio
async def test_raft_node_push_message() -> None:
    """
    Raft node pushes a committed chat message which is forwarded
    by the chat server to connected clients.
    """
    
    raft_node, server, client = await init_test_env()
    
    # Join the chat first
    await client.send(
        {
            "type": "JOIN",
            "sender": "alice",
            "payload": {},
        }
    )

    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    assert raft_node.received_messages[-1]["type"] == "HISTORY_REQUEST"

    # Complete the join process
    await raft_node.send_history_reply(
        {
            "entries": []
        }
    )

    join_reply = await client.receive()

    assert join_reply["type"] == "JOIN_REPLY"

    # Push a committed log entry from Raft
    await raft_node.send_committed_entry(
        {
            "index": 1,
            "term": 1,
            "command": {
                "type": "SEND_MESSAGE",
                "payload": {
                    "sender": "bob",
                    "text": "Hello Alice!",
                },
            },
        }
    )

    server_msg = await client.receive()

    assert server_msg["type"] == "NEW_MESSAGE"
    assert server_msg["payload"]["sender"] == "bob"
    assert server_msg["payload"]["text"] == "Hello Alice!"
    
    
@pytest.mark.asyncio
async def test_client_submit_message_success() -> None:
    """
    User submits a new chat message into client, must be received by raft node. 
    Raft node approves and client gets acknowledgement 
    """
    
    raft_node, server, client = await init_test_env()
    
    # Join first
    await client.send(
        {
            "type": "JOIN",
            "sender": "alice",
            "payload": {},
        }
    )

    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    await raft_node.send_history_reply(
        {
            "entries": []
        }
    )

    join_reply = await client.receive()
    assert join_reply["type"] == "JOIN_REPLY"

    # Clear out the history request message
    raft_node.received_messages.clear()

    # Send a chat message
    await client.send(
        {
            "type": "SEND_MESSAGE",
            "sender": "alice",
            "payload": {
                "text": "Hello everyone!"
            },
        }
    )

    # Wait for server -> raft forwarding
    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    proposal_request = raft_node.received_messages[-1]
    print(proposal_request)
    
    assert proposal_request["type"] == "PROPOSE_REQUEST"
    assert proposal_request["payload"]["type"] == "POST_MESSAGE"
    assert (
        proposal_request["payload"]["payload"]["text"]
        == "Hello everyone!"
    )
    assert (
        proposal_request["payload"]["payload"]["sender"]
        == "alice"
    )

    # Raft accepts the proposal
    await raft_node.send_proposal_result(accepted=True)

    # Client should receive acknowledgement
    reply = await client.receive()

    assert reply["type"] == "SEND_MESSAGE_REPLY"
    assert reply["payload"]["accepted"] is True
    
    
@pytest.mark.asyncio
async def test_client_submit_message_fail() -> None:
    """
    User submits a new chat message into client, must be received by raft node. 
    Raft node rejects and client gets acknowledgement 
    """
    
    raft_node, server, client = await init_test_env()

    # Join first
    await client.send(
        {
            "type": "JOIN",
            "sender": "alice",
            "payload": {},
        }
    )

    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    await raft_node.send_history_reply(
        {
            "entries": []
        }
    )

    join_reply = await client.receive()
    assert join_reply["type"] == "JOIN_REPLY"

    # Clear the history request
    raft_node.received_messages.clear()

    # Submit chat message
    await client.send(
        {
            "type": "SEND_MESSAGE",
            "sender": "alice",
            "payload": {
                "text": "Hello everyone!"
            },
        }
    )

    # Wait until forwarded to Raft
    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    proposal_request = raft_node.received_messages[-1]

    assert proposal_request["type"] == "PROPOSE_REQUEST"
    assert (
        proposal_request["payload"]["payload"]["text"]
        == "Hello everyone!"
    )

    # Raft rejects proposal
    await raft_node.send_proposal_result(
        accepted=False,
        error="Rejected by leader"
    )

    reply = await client.receive()

    assert reply["type"] == "SEND_MESSAGE_REPLY"
    assert reply["payload"]["accepted"] is False


   
    
    
# ----------------------------------------------------------------------
# client.py
# ---------------------------------------------------------------------

