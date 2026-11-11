"""Tests for the cluster configuration loader.

These tests load config.example.json rather than config.json, so they pass on a
fresh clone where no local configuration exists yet.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from config import ClusterConfig, ConfigError, load_config
from raft.types import NodeId

EXAMPLE_PATH = Path(__file__).resolve().parent.parent / "config.example.json"


@pytest.fixture
def cluster() -> ClusterConfig:
    """Load the committed example topology."""
    return load_config(EXAMPLE_PATH)


def test_example_config_loads(cluster: ClusterConfig) -> None:
    """The committed example configuration parses into a ClusterConfig."""
    assert len(cluster.raft_nodes) == 3
    assert len(cluster.chat_servers) == 3


def test_raft_node_addresses(cluster: ClusterConfig) -> None:
    """Each Raft node exposes its peer address."""
    node = cluster.raft_node(NodeId("node1"))
    assert node.host == "127.0.0.1"
    assert node.peer_port == 7001
    assert node.chat_api_port == 8001
    assert node.peer_address.port == 7001


def test_peers_exclude_self(cluster: ClusterConfig) -> None:
    """peers_for returns every other node and never the node itself."""
    peers = cluster.peers_for(NodeId("node1"))
    assert set(peers) == {NodeId("node2"), NodeId("node3")}
    assert peers[NodeId("node2")].port == 7002


def test_chat_server_to_raft_mapping(cluster: ClusterConfig) -> None:
    """Each chat server resolves to the Raft node it is pinned to."""
    assert cluster.raft_node_for_chat_server("chat2").node_id == NodeId("node2")
    assert cluster.chat_server("chat3").raft_node_id == NodeId("node3")


def test_raft_config_for_node(cluster: ClusterConfig) -> None:
    """raft_config_for produces a RaftConfig wired to the right addresses."""
    raft_config = cluster.raft_config_for(NodeId("node2"))
    assert raft_config.node_id == NodeId("node2")
    assert raft_config.peer_port == 7002
    assert raft_config.chat_api_port == 8002
    assert set(raft_config.peers) == {NodeId("node1"), NodeId("node3")}


def test_unknown_node_raises(cluster: ClusterConfig) -> None:
    """Looking up a node that does not exist raises ConfigError."""
    with pytest.raises(ConfigError):
        cluster.raft_node(NodeId("node9"))


def test_unknown_chat_server_raises(cluster: ClusterConfig) -> None:
    """Looking up a chat server that does not exist raises ConfigError."""
    with pytest.raises(ConfigError):
        cluster.chat_server("chat9")


def test_missing_file_raises() -> None:
    """A missing configuration file raises ConfigError, not FileNotFoundError."""
    with pytest.raises(ConfigError):
        load_config(Path("/nonexistent/config.json"))


def test_invalid_json_raises(tmp_path: Path) -> None:
    """A malformed configuration file raises ConfigError."""
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(bad)


def test_chat_server_referring_to_unknown_node_raises(tmp_path: Path) -> None:
    """A chat server pinned to a nonexistent Raft node raises ConfigError."""
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {
                "raft_nodes": {
                    "node1": {"host": "127.0.0.1", "peer_port": 7001, "chat_api_port": 8001}
                },
                "chat_servers": {
                    "chat1": {"host": "127.0.0.1", "port": 9001, "raft_node": "node9"}
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(bad)


def test_invalid_port_raises(tmp_path: Path) -> None:
    """A port outside the valid TCP range raises ConfigError."""
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {
                "raft_nodes": {
                    "node1": {"host": "127.0.0.1", "peer_port": 99999, "chat_api_port": 8001}
                },
                "chat_servers": {
                    "chat1": {"host": "127.0.0.1", "port": 9001, "raft_node": "node1"}
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(bad)
