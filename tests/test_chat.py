"""Tests for chat_server/ and client/."""

# tests/test_chat_server_raft_integration.py

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import pytest

import chat_server.message as csmessage
from chat_server.message import ChatMessage
from chat_server.raft_client import RaftClient
from chat_server.server import ChatServer
from client.client import ChatClient
from raft.networking import serialization
from raft.networking.chat_api import ChatRaftMessageType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ----------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------
RAFT_IP = "127.0.0.1"
SERVER_IP = "127.0.0.2"
# CLIENT1_IP ="127.0.0.3"
# CLIENT2_IP ="127.0.0.4"

# ----------------------------------------------------------------------
# Test data constants
# ---------------------------------------------------------------------
ARCEUS_MSG = {
    "sender": "Arceus",
    "text": "Hello I'm Arceus",
}

BULBASAUR_MSG = {
    "sender": "Bulbasaur",
    "text": "Hello I'm Bulbasaur",
}

CHARIZARD_MSG = {
    "sender": "Charizard",
    "text": "Hello I'm Charizard",
}

DITTO_MSG = {
    "sender": "Ditto",
    "text": "Hello I'm Ditto.",
}

EEVEE_MSG = {
    "sender": "Eevee",
    "text": "Hello I'm Eevee",
}

FROAKIE_MSG = {
    "sender": "Froakie",
    "text": "Hello I'm Froakie",
}

CLIENT1_USERNAME = CHARIZARD_MSG["sender"]
CLIENT1_MSG = CHARIZARD_MSG

CLIENT2_USERNAME = DITTO_MSG["sender"]
CLIENT2_MSG = DITTO_MSG

FILENAME_HISTORY_CLIENT1 = "history_CLIENT1.jsonl" 
FILENAME_HISTORY_CLIENT2 = "history_CLIENT2.jsonl" 

# ----------------------------------------------------------------------
# Helper classes
# ---------------------------------------------------------------------
class FakeClient:
    def __init__(self, host: str, port: str, client_id: str) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )

    async def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()

    async def send(self, payload: dict[str, Any]) -> None:
        if self.writer is not None:
            await csmessage.write_message(self.writer,payload)

    async def receive(self) -> dict[str, Any]:
        if self.reader is None:
            raise RuntimeError("Client is not connected")

        message = await csmessage.read_message(self.reader)

        if message is None:
            raise RuntimeError("Connection closed")

        return message


class FakeRaftNode:
    def __init__(self, host : str) -> None:
        self.host = host
        self.server: asyncio.Server | None = None
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.received_messages: list[dict[str, Any]] = []
        self.client_connected = asyncio.Event()

    async def start(self) -> int:
        self.server = await asyncio.start_server(
            self._handle_connection, self.host, 0)
        return int(self.server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()

        if self.server is not None:
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
        if self.writer is None:
            raise RuntimeError("No raft connection")
        
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
        if self.writer is None:
            raise RuntimeError("No raft connection")
        await serialization.write_message(
            self.writer,
            {
                "type": ChatRaftMessageType.HISTORY_REPLY.value,
                "payload": payload,
            },
        )

    async def send_committed_entry(self, payload: dict[str, Any]) -> None:
        if self.writer is None:
            raise RuntimeError("No raft connection")
        await serialization.write_message(
            self.writer,
            {
                "type": ChatRaftMessageType.COMMITTED_ENTRY.value,
                "payload": payload,
            },
        )
        
# ----------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def _history_entry(index: int, sender: str, text: str, term: int = 1) -> dict[str, Any]:
    return {
        "index": index,
        "term": term,
        "command": {
            "type": "POST_MESSAGE",
            "payload": {
                "sender": sender,
                "text": text,
            },
        },
    }
    
async def _join_client(
    client: FakeClient,
    raft_node: FakeRaftNode,
    history_entries: list[dict[str, Any]] | None = None,
) -> None:
    await client.send(
        {
            "type": "JOIN",
            "sender": CLIENT1_USERNAME,
            "payload": {},
        }
    )

    while not raft_node.received_messages:
        await asyncio.sleep(0.01)

    assert raft_node.received_messages[-1]["type"] == "HISTORY_REQUEST"

    await raft_node.send_history_reply(
        {"entries": history_entries or []}
    )

    reply = await client.receive()
    assert reply["type"] == "JOIN_REPLY"


async def _init_server_test_env() -> tuple[FakeRaftNode, ChatServer, FakeClient]:
    raft_node = FakeRaftNode(RAFT_IP)
    raft_port = await raft_node.start()
    raft_client = RaftClient(RAFT_IP, raft_port)
    
    server = ChatServer(SERVER_IP, 0, raft_client)

    logger.info("starting chat server")
    await server.start_chat_server()
    logger.info("Chat server started")
    await raft_node.client_connected.wait()
    
    assert server.server is not None
    server_port = (server.server.sockets[0].getsockname()[1])
    client = FakeClient(SERVER_IP, server_port, CLIENT1_USERNAME)

    await client.connect()
    
    return raft_node, server, client
       
       
async def init_client_test_env() -> tuple[FakeRaftNode, ChatServer, ChatClient]:
    """
    Start a fake Raft node, Raft client, chat server, and chat client, and connect them all
    for integration tests.

    """
    raft_node = FakeRaftNode(RAFT_IP)
    raft_port = await raft_node.start()
    
    raft_client = RaftClient(RAFT_IP, raft_port)
    server = ChatServer(SERVER_IP, 0, raft_client)

    logger.info("starting chat server")
    await server.start_chat_server()
    logger.info("Chat server started")
    
    await raft_node.client_connected.wait()
        
    assert server.server is not None
    server_port = (server.server.sockets[0].getsockname()[1])
    client = ChatClient(SERVER_IP, server_port, CLIENT1_USERNAME, FILENAME_HISTORY_CLIENT1)

    connect_task = asyncio.create_task(
        client.connect()
    )
    
    while not raft_node.received_messages:
        await asyncio.sleep(0.01)

    assert (
        raft_node.received_messages[-1]["type"]
        == "HISTORY_REQUEST"
    )
    
    await raft_node.send_history_reply(
        {
            "entries": []
        }
    )

    await connect_task

    logger.info("Client connected to server")
    
    return raft_node, server, client 


# ----------------------------------------------------------------------
# Test cases for chat_server/server.py and chat_server/raft_client.py
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cs_client_join() -> None:
    """
    Test behaviour of client joining the server and getting chat history in the reply.
    """
    
    raft_node, server, client = await _init_server_test_env()

    await client.send(
        {
            "type": "JOIN",
            "sender": CLIENT1_USERNAME,
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
                _history_entry(1, sender=ARCEUS_MSG["sender"], text=ARCEUS_MSG["text"]),
                _history_entry(2, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]),
            ]
        }
    )
    
    join_reply = await client.receive()

    assert join_reply["type"] == "JOIN_REPLY"
    assert join_reply["payload"]["entries"][0]["command"]["payload"]["text"] == ARCEUS_MSG["text"]


    

@pytest.mark.asyncio
async def test_cs_raft_node_push_message() -> None:
    """
    Raft node pushes a committed chat message which is forwarded
    by the chat server to connected clients.
    """
    
    raft_node, server, client = await _init_server_test_env()
    
    # Join the chat first
    await _join_client(client, raft_node)

    # Push a committed log entry from Raft
    await raft_node.send_committed_entry(_history_entry(
        1, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]))

    server_msg = await client.receive()

    assert server_msg["type"] == "NEW_MESSAGE"
    assert server_msg["payload"]["sender"] == BULBASAUR_MSG["sender"]
    assert server_msg["payload"]["text"] == BULBASAUR_MSG["text"]
    
    
@pytest.mark.asyncio
async def test_cs_client_submit_message_success() -> None:
    """
    User submits a new chat message into client, must be received by raft node. 
    Raft node approves and client gets acknowledgement 
    """
    
    raft_node, server, client = await _init_server_test_env()
    
    # Join the chat first
    await _join_client(client, raft_node)

    # Clear out the history request message
    raft_node.received_messages.clear()

    # Send a chat message
    await client.send(
        {
            "type": "SEND_MESSAGE",
            "sender": CLIENT1_USERNAME,
            "payload": {
                "text": CLIENT1_MSG["text"]
            },
        }
    )

    # Wait for server -> raft forwarding
    while len(raft_node.received_messages) == 0:
        await asyncio.sleep(0.01)

    proposal_request = raft_node.received_messages[-1]
    
    assert proposal_request["type"] == "PROPOSE_REQUEST"
    assert proposal_request["payload"]["type"] == "POST_MESSAGE"
    assert (
        proposal_request["payload"]["payload"]["text"]
        == CLIENT1_MSG["text"]
    )
    assert (
        proposal_request["payload"]["payload"]["sender"]
        == CLIENT1_MSG["sender"]
    )

    # Raft accepts the proposal
    await raft_node.send_proposal_result(accepted=True)

    # Client should receive acknowledgement
    reply = await client.receive()

    assert reply["type"] == "SEND_MESSAGE_REPLY"
    assert reply["payload"]["accepted"] is True
        
    
    
@pytest.mark.asyncio
async def test_cs_client_submit_message_fail() -> None:
    """
    User submits a new chat message into client, must be received by raft node. 
    Raft node rejects and client gets acknowledgement 
    """
    
    raft_node, server, client = await _init_server_test_env()

    # Join the chat first
    await _join_client(client, raft_node)

    # Clear the history request
    raft_node.received_messages.clear()

    # Submit chat message
    await client.send(
        {
            "type": "SEND_MESSAGE",
            "sender": CLIENT1_USERNAME,
            "payload": {
                "text": CLIENT1_MSG["text"]
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
        == CLIENT1_MSG["text"]
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
# Test cases for client.py, chat_server/server.py and chat_server/raft_client.py
# ---------------------------------------------------------------------
    
@pytest.mark.asyncio
async def test_integrate_client_join() -> None:
    """
    Test that a ChatClient joins a ChatServer and receives the chat history returned by the Raft 
    node. Client begins with empty chat history
    """
    # Ensure no history file before the test
    history_path = Path("client") / FILENAME_HISTORY_CLIENT1
    history_path.unlink(missing_ok=True)

    raft_node = FakeRaftNode(RAFT_IP)
    raft_port = await raft_node.start()

    raft_client = RaftClient(RAFT_IP, raft_port)
    server = ChatServer(SERVER_IP, 0, raft_client)
    
    client = None
    try:
        await server.start_chat_server()
        await raft_node.client_connected.wait()
        
        assert server.server is not None
        server_port = server.server.sockets[0].getsockname()[1]
        client = ChatClient(SERVER_IP, server_port, CLIENT1_USERNAME, FILENAME_HISTORY_CLIENT1)

        # connect() will block waiting for JOIN_REPLY
        connect_task = asyncio.create_task(client.connect())

        # Wait until server forwards HISTORY_REQUEST to Raft
        while len(raft_node.received_messages) == 0:
            await asyncio.sleep(0.01)

        assert (
            raft_node.received_messages[-1]["type"]
            == "HISTORY_REQUEST"
        )

        await raft_node.send_history_reply(
            {
                "entries": [
                    _history_entry(1, sender=ARCEUS_MSG["sender"], text=ARCEUS_MSG["text"]),
                    _history_entry(2, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]),
                ]
            }
        )

        await connect_task

        assert client.listen_task is not None
        assert not client.listen_task.done()
        
        with open(history_path, encoding="utf-8") as f:
            history = [json.loads(line) for line in f]
            assert history == [
                {
                    "sender": ARCEUS_MSG["sender"],
                    "text":ARCEUS_MSG["text"],
                    "log_index": 1,
                },
                {
                    "sender": BULBASAUR_MSG["sender"],
                    "text": BULBASAUR_MSG["text"],
                    "log_index": 2,
                },
            ]
    finally:
        if client is not None:
            await client.close()

        await server.stop()
        await raft_client.close()
        await raft_node.stop()
        
        

@pytest.mark.asyncio
async def test_integrate_client_submit_message_accepted() -> None:
    """
    Test that when a client submits a message, which gets committed by the raft node,
    the client is able to receive replies and update its chat history.
        
    Precondition:
    test_integrate_client_join() has already produced history.jsonl
    containing two messages.
    """
    
    raft_node, server, client = await init_client_test_env()
    raft_node_msgs_initial_count = len(raft_node.received_messages)

    try:
        # Submit a message
        client_message = ChatMessage(
            sender=CLIENT1_USERNAME,
            text=CLIENT1_MSG["text"],
            log_index = None # Client doesn't know the final log_index             
        )
        
        send_message_task = asyncio.create_task(client.send_message(client_message))
        

        # Wait for proposal to reach Raft
        while len(raft_node.received_messages) == raft_node_msgs_initial_count:
            await asyncio.sleep(0.01)

        proposal = raft_node.received_messages[-1]
        
        assert proposal["type"] == "PROPOSE_REQUEST" 
        assert proposal["payload"]["payload"]["sender"] == CLIENT1_USERNAME
        assert proposal["payload"]["payload"]["text"] == CLIENT1_MSG["text"]

        # Simulate Raft accepting the proposal
        await raft_node.send_proposal_result(
            accepted=True,
        )
        
        # send_message() should now complete.
        await send_message_task

        # Simulate Raft committing the message
        await raft_node.send_committed_entry(
            {
                "index": 3,
                "term": 1,
                "command": {
                    "type": "POST_MESSAGE",
                    "payload": {
                        "sender": CLIENT1_MSG["sender"],
                        "text": CLIENT1_MSG["text"]
                    },
                },
            }
        )

        # Allow listener tasks to process the commit
        await asyncio.sleep(0.1)

        # Verify history file updated
        with open(Path("client") / FILENAME_HISTORY_CLIENT1, encoding="utf-8") as file:
            history = [json.loads(line) for line in file]

        assert len(history) == 3

        assert [(m["sender"], m["text"]) for m in history] == [
            (ARCEUS_MSG["sender"], ARCEUS_MSG["text"]),
            (BULBASAUR_MSG["sender"], BULBASAUR_MSG["text"]),
            (CLIENT1_MSG["sender"], CLIENT1_MSG["text"]),
        ]

    finally:
        await client.close()
        await server.stop()
        await raft_node.stop()
    
    
    
@pytest.mark.asyncio
async def test_integrate_client_submit_message_rejected() -> None:
    """
    Test that when a client submits a message, which is NOT committed by the raft node,
    the client is able to receive replies and avoid updating its chat history.
            
    Precondition:
    test_integrate_client_submit_message_accepted() has already produced history.jsonl
    containing three messages.
    
    """
    
    raft_node, server, client = await init_client_test_env()

    try:
        submitted_text = "this message should be rejected"

        initial_count = len(raft_node.received_messages)

        send_task = asyncio.create_task(
            client.send_message(
                ChatMessage(
                    sender=CLIENT1_USERNAME,
                    text=submitted_text,
                    log_index=None,
                )
            )
        )

        # Wait for proposal to reach Raft.
        while len(raft_node.received_messages) == initial_count:
            await asyncio.sleep(0.01)

        proposal = raft_node.received_messages[-1]

        assert proposal["type"] == "PROPOSE_REQUEST"
        assert proposal["payload"]["payload"]["sender"] == CLIENT1_USERNAME
        assert proposal["payload"]["payload"]["text"] == submitted_text

        # Simulate Raft rejecting the proposal.
        await raft_node.send_proposal_result(
            accepted=False,
            error="proposal rejected"
        )

        # Wait for client.send_message() to receive the reply.
        await send_task

        # No commit should be sent here.
        await asyncio.sleep(0.1)

        # Verify history file was NOT modified.
        with open(Path("client") / FILENAME_HISTORY_CLIENT1, encoding="utf-8") as file:
            history = [json.loads(line) for line in file]

        assert len(history) == 3

        assert [(m["sender"], m["text"]) for m in history] == [
            (ARCEUS_MSG["sender"], ARCEUS_MSG["text"]),
            (BULBASAUR_MSG["sender"], BULBASAUR_MSG["text"]),
            (CLIENT1_MSG["sender"], CLIENT1_MSG["text"]),
        ]

    finally:
        await client.close()
        await server.stop()
        await raft_node.stop()


@pytest.mark.asyncio
async def test_integrate_client_server_push() -> None:
    """
    Test that when the client receives newly committed chat messages from the raft node
    via the server, the client is able to update its history.
    
    Precondition:
    test_integrate_client_submit_message_rejected() has already produced history.jsonl
    containing three messages.
    """
    
    raft_node, server, client = await init_client_test_env()

    try:
        # Simulate a message committed by another user.
        await raft_node.send_committed_entry(
            {
                "index": 4,
                "term": 1,
                "command": {
                    "type": "POST_MESSAGE",
                    "payload": {
                        "sender": DITTO_MSG["sender"],
                        "text": DITTO_MSG["text"],
                    },
                },
            }
        )

        # Allow ChatServer and ChatClient listener tasks
        # time to process the commit.
        await asyncio.sleep(0.1)

        # Verify history file was updated.
        with open(Path("client") / FILENAME_HISTORY_CLIENT1, encoding="utf-8") as file:
            history = [json.loads(line) for line in file]

        assert len(history) == 4

        assert history[-1] == {
            "sender": DITTO_MSG["sender"],
            "text": DITTO_MSG["text"],
            "log_index": 4,
        }

        assert [(m["sender"], m["text"]) for m in history] == [
            (ARCEUS_MSG["sender"], ARCEUS_MSG["text"]),
            (BULBASAUR_MSG["sender"], BULBASAUR_MSG["text"]),
            (CLIENT1_MSG["sender"], CLIENT1_MSG["text"]),
            (DITTO_MSG["sender"], DITTO_MSG["text"]),
        ]

    finally:
        await client.close()
        await server.stop()
        await raft_node.stop()
    

@pytest.mark.asyncio
async def test_integrate_client_leave() -> None:
    """
    Test that client process can stop without triggering errors from server side.
    
    """
    raft_node, server, client = await init_client_test_env()

    try:
        # Verify client is connected.
        assert client.listen_task is not None
        assert not client.listen_task.done()

        # Client leaves.
        await client.close()

        # Server should still be running.
        assert server.server is not None
        assert server.server.is_serving()

    finally:
        await server.stop()
        await raft_node.stop()
 
 
@pytest.mark.asyncio
async def test_integrate_client_rejoin() -> None:
    """
    Test that client process can stop and leave the chat group.
    More chat messages are committed while the client is away.
    When the client rejoins, it should obtain the chat messages
    it missed.
    
    Precondition:
        test_integrate_client_server_push() has already produced history.jsonl
        containing four messages.
    """

    raft_node, server, client = await init_client_test_env()

    try:
        # Client leaves the chat group.
        await client.close()

        # Clear old traffic so we can detect the next HISTORY_REQUEST.
        raft_node.received_messages.clear()

        # Recreate client process.
        assert server.server is not None
        client = ChatClient(
            SERVER_IP,
            server.server.sockets[0].getsockname()[1],
            CLIENT1_USERNAME,
            FILENAME_HISTORY_CLIENT1,
        )

        # Reconnect. This should trigger HISTORY_REQUEST.
        connect_task = asyncio.create_task(client.connect())

        while not raft_node.received_messages:
            await asyncio.sleep(0.01)

        assert (
            raft_node.received_messages[-1]["type"]
            == "HISTORY_REQUEST"
        )

        # Raft now returns the original four messages plus two
        # messages committed while the client was offline.
        await raft_node.send_history_reply(
            {
                "entries": [
                    _history_entry(1, sender=ARCEUS_MSG["sender"], text=ARCEUS_MSG["text"]),
                    _history_entry(2, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]),
                    _history_entry(3, sender=CLIENT1_MSG["sender"], text=CLIENT1_MSG["text"]),
                    _history_entry(4, sender=DITTO_MSG["sender"], text=DITTO_MSG["text"]),
                    _history_entry(
                        5, sender=EEVEE_MSG["sender"], text=EEVEE_MSG["text"], term=2
                    ),
                    _history_entry(
                        6, sender=FROAKIE_MSG["sender"], text=FROAKIE_MSG["text"], term=2
                    ),
                ]
            }
        )

        await connect_task

        # Verify history was updated with missed messages.
        history_path = Path("client") / FILENAME_HISTORY_CLIENT1

        with history_path.open(encoding="utf-8") as file:
            history = [json.loads(line) for line in file]

        assert len(history) == 6

        assert [(m["sender"], m["text"]) for m in history] == [
            (ARCEUS_MSG["sender"], ARCEUS_MSG["text"]),
            (BULBASAUR_MSG["sender"], BULBASAUR_MSG["text"]),
            (CLIENT1_MSG["sender"], CLIENT1_MSG["text"]),
            (DITTO_MSG["sender"], DITTO_MSG["text"]),
            (EEVEE_MSG["sender"], EEVEE_MSG["text"]),
            (FROAKIE_MSG["sender"], FROAKIE_MSG["text"]),
        ]

    finally:
        await client.close()
        await server.stop()
        await raft_node.stop()
        
        
        

@pytest.mark.asyncio
async def test_integrate_two_clients() -> None:
    """
    Test that two clients can join the group chat and request history. One client submits a chat 
    message which the raft node commits. 
    
    Expected outcome:
    - Both clients receive the committed message.
    - Both clients append the committed message to their local history.
    """
    
    Path("client", FILENAME_HISTORY_CLIENT1).unlink(missing_ok=True)
    Path("client", FILENAME_HISTORY_CLIENT2).unlink(missing_ok=True)

    raft_node = FakeRaftNode(RAFT_IP)
    raft_port = await raft_node.start()

    raft_client = RaftClient(RAFT_IP, raft_port)

    server = ChatServer(SERVER_IP, 0, raft_client)

    client1 = None
    client2 = None

    try:
        await server.start_chat_server()
        await raft_node.client_connected.wait()

        assert server.server is not None
        server_port = server.server.sockets[0].getsockname()[1]

        client1 = ChatClient(SERVER_IP, server_port, CLIENT1_USERNAME, FILENAME_HISTORY_CLIENT1)
        client2 = ChatClient(SERVER_IP, server_port, CLIENT2_USERNAME, FILENAME_HISTORY_CLIENT2)

        # Client 1 joins
        connect_task1 = asyncio.create_task(client1.connect())

        while not raft_node.received_messages:
            await asyncio.sleep(0.01)

        assert raft_node.received_messages[-1]["type"] == "HISTORY_REQUEST"

        await raft_node.send_history_reply(
            {
                "entries": [
                    _history_entry(1, sender=ARCEUS_MSG["sender"], text=ARCEUS_MSG["text"]),
                    _history_entry(2, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]),
                ]
            }
        )

        await connect_task1

        # Client 2 joins
        received_count = len(raft_node.received_messages)

        connect_task2 = asyncio.create_task(client2.connect())

        while len(raft_node.received_messages) == received_count:
            await asyncio.sleep(0.01)

        assert raft_node.received_messages[-1]["type"] == "HISTORY_REQUEST"

        await raft_node.send_history_reply(
            {
                "entries": [
                    _history_entry(1, sender=ARCEUS_MSG["sender"], text=ARCEUS_MSG["text"]),
                    _history_entry(2, sender=BULBASAUR_MSG["sender"], text=BULBASAUR_MSG["text"]),
                ]
            }
        )

        await connect_task2

        # Client 1 submits a message
        submitted_text = CLIENT1_MSG["text"]

        previous_count = len(raft_node.received_messages)

        send_task = asyncio.create_task(
            client1.send_message(
                ChatMessage(
                    sender=CLIENT1_USERNAME,
                    text=submitted_text,
                    log_index=None,
                )
            )
        )

        while len(raft_node.received_messages) == previous_count:
            await asyncio.sleep(0.01)

        proposal = raft_node.received_messages[-1]

        assert proposal["type"] == "PROPOSE_REQUEST"

        await raft_node.send_proposal_result(accepted=True)

        await send_task

        # Raft commits the message
        await raft_node.send_committed_entry(
            {
                "index": 3,
                "term": 1,
                "command": {
                    "type": "POST_MESSAGE",
                    "payload": {
                        "sender": CLIENT1_USERNAME,
                        "text": submitted_text,
                    },
                },
            }
        )

        # Allow both listeners to process push notification
        await asyncio.sleep(0.1)

        # Verify Client 1 history
        with open(Path("client") / FILENAME_HISTORY_CLIENT1, encoding="utf-8") as file:
            history1 = [json.loads(line) for line in file]

        # Verify Client 2 history
        with open(Path("client") / FILENAME_HISTORY_CLIENT2, encoding="utf-8") as file:
            history2 = [json.loads(line) for line in file]

        expected = [
                    (ARCEUS_MSG["sender"], ARCEUS_MSG["text"]),
                    (BULBASAUR_MSG["sender"], BULBASAUR_MSG["text"]),
                    (CLIENT1_MSG["sender"], CLIENT1_MSG["text"]),
                ]
    
        
        assert [(m["sender"], m["text"]) for m in history1] == expected
        assert [(m["sender"], m["text"]) for m in history2] == expected


    finally:
        if client1 is not None:
            await client1.close()

        if client2 is not None:
            await client2.close()

        await server.stop()
        await raft_client.close()
        await raft_node.stop()