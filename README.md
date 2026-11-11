# Distributed Chat with Raft

A distributed group chat based on Raft implementation, written in
Python. Each participant machine runs two processes, a chat server and a Raft
node. Chat messages are replicated through Raft and broadcast to every client
only after they are committed. All state is held in memory.

## Prerequisites

- Python 3.12 or newer. `uv` can install it for you.
- [uv](https://docs.astral.sh/uv/) for project and dependency management.

## Development installation

```bash
uv sync
```

This creates `.venv` and installs the development dependencies.

Run the checks.

```bash
uv run pytest
uv run ruff check .
uv run mypy .
```

## Project structure

```text
.
├── chat_server/          # Chat server process
│   ├── server.py         # Client connections and broadcasting
│   ├── message.py        # Chat message types and the client protocol
│   └── raft_client.py    # Client for the local Raft node
├── raft/                 # Raft consensus
│   ├── node.py           # RaftNode, which owns all shared Raft state
│   ├── types.py          # Shared Raft types, the frozen contract
│   ├── election.py       # Leader election and failure detection
│   ├── replication.py    # Log replication and commit tracking
│   └── networking/       # TCP transport, serialization, and the chat API
├── client/               # Interactive terminal chat client
├── tests/                # Tests
├── docs/
│   ├── architecture.md   # Diagram, message lifecycle, known limitations
│   └── responsibilities.md  # The four-person work division
├── config.py             # Cluster configuration schema and loader
└── config.example.json   # Example three-node localhost topology
```

## Configuration

`config.example.json` describes a three-node cluster on localhost. Copy it to
`config.json`, which is gitignored, and edit it for your setup.

```bash
cp config.example.json config.json
```

Each Raft node declares a peer port for talking to other Raft nodes and a chat
API port for talking to its local chat server. Each chat server declares a client
port and names the Raft node it is pinned to.

## Work division

This is a rough division of each person's main responsibility

| Person | Area             | Files                     |
| ------ | ---------------- | ------------------------- |
| 1      | Raft elections   | `raft/election.py`        |
| 2      | Raft replication | `raft/replication.py`     |
| 3      | Networking       | `raft/networking/`        |
| 4      | Chat application | `chat_server/`, `client/` |

## Documentation

- [docs/architecture.md](docs/architecture.md) shows diagrams of the architecture
