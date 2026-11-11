"""Cluster topology configuration.

This module owns the JSON configuration schema and the loader that turns a
configuration file into typed objects. It is the single source of truth for
which Raft nodes and chat servers exist, where they listen, and which chat
server talks to which Raft node.

This module is shared infrastructure rather than one of the four work areas, so
it is implemented rather than stubbed. Everything under raft/, chat_server/, and
client/ is a stub.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from raft.types import NodeId, PeerAddress, RaftConfig


class ConfigError(Exception):
    """Raised when a configuration file is missing, malformed, or inconsistent."""


@dataclass(frozen=True)
class LoggingConfig:
    """Where and how each process writes its local event log."""

    directory: str = "logs"
    level: str = "INFO"
    format: str = "%(asctime)s %(levelname)-8s %(name)s %(message)s"


@dataclass(frozen=True)
class RaftNodeConfig:
    """Static description of one Raft node's identity and listening addresses."""

    node_id: NodeId
    host: str
    peer_port: int
    chat_api_port: int

    @property
    def peer_address(self) -> PeerAddress:
        """Return this node's peer-to-peer address for use in RaftConfig.peers."""
        return PeerAddress(node_id=self.node_id, host=self.host, port=self.peer_port)


@dataclass(frozen=True)
class ChatServerConfig:
    """Static description of one chat server and the Raft node it is pinned to."""

    server_id: str
    host: str
    port: int
    raft_node_id: NodeId


@dataclass(frozen=True)
class ClusterConfig:
    """The whole cluster topology, as loaded from a configuration file."""

    raft_nodes: dict[NodeId, RaftNodeConfig]
    chat_servers: dict[str, ChatServerConfig]
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    def raft_node(self, node_id: NodeId) -> RaftNodeConfig:
        """Return the Raft node config for node_id, or raise ConfigError."""
        try:
            return self.raft_nodes[node_id]
        except KeyError:
            raise ConfigError(f"unknown raft node: {node_id!r}") from None

    def chat_server(self, server_id: str) -> ChatServerConfig:
        """Return the chat server config for server_id, or raise ConfigError."""
        try:
            return self.chat_servers[server_id]
        except KeyError:
            raise ConfigError(f"unknown chat server: {server_id!r}") from None

    def peers_for(self, node_id: NodeId) -> dict[NodeId, PeerAddress]:
        """Return every other Raft node's address, keyed by node id."""
        return {
            other_id: node.peer_address
            for other_id, node in self.raft_nodes.items()
            if other_id != node_id
        }

    def raft_config_for(self, node_id: NodeId) -> RaftConfig:
        """Build the RaftConfig that RaftNode is constructed from."""
        node = self.raft_node(node_id)
        return RaftConfig(
            node_id=node.node_id,
            peers=self.peers_for(node_id),
            peer_host=node.host,
            peer_port=node.peer_port,
            chat_api_host=node.host,
            chat_api_port=node.chat_api_port,
        )

    def raft_node_for_chat_server(self, server_id: str) -> RaftNodeConfig:
        """Return the Raft node that the given chat server is pinned to."""
        return self.raft_node(self.chat_server(server_id).raft_node_id)


def load_config(path: str | Path) -> ClusterConfig:
    """Load and validate a cluster configuration file."""
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"configuration file not found: {config_path}") from None
    except json.JSONDecodeError as exc:
        raise ConfigError(f"configuration file is not valid JSON: {exc}") from None

    if not isinstance(raw, dict):
        raise ConfigError("configuration root must be a JSON object")

    raft_nodes = _parse_raft_nodes(raw.get("raft_nodes"))
    chat_servers = _parse_chat_servers(raw.get("chat_servers"), raft_nodes)
    logging_config = _parse_logging(raw.get("logging"))
    return ClusterConfig(
        raft_nodes=raft_nodes, chat_servers=chat_servers, logging=logging_config
    )


def _parse_raft_nodes(raw: Any) -> dict[NodeId, RaftNodeConfig]:
    """Parse and validate the "raft_nodes" section of a configuration file."""
    if not isinstance(raw, dict) or not raw:
        raise ConfigError('"raft_nodes" must be a non-empty JSON object')

    nodes: dict[NodeId, RaftNodeConfig] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise ConfigError(f'raft node "{key}" must be a JSON object')
        node_id = NodeId(key)
        nodes[node_id] = RaftNodeConfig(
            node_id=node_id,
            host=_require_str(value, "host", f'raft node "{key}"'),
            peer_port=_require_port(value, "peer_port", f'raft node "{key}"'),
            chat_api_port=_require_port(value, "chat_api_port", f'raft node "{key}"'),
        )
    return nodes


def _parse_chat_servers(
    raw: Any, raft_nodes: dict[NodeId, RaftNodeConfig]
) -> dict[str, ChatServerConfig]:
    """Parse and validate the "chat_servers" section of a configuration file."""
    if not isinstance(raw, dict) or not raw:
        raise ConfigError('"chat_servers" must be a non-empty JSON object')

    servers: dict[str, ChatServerConfig] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise ConfigError(f'chat server "{key}" must be a JSON object')
        raft_node_id = NodeId(_require_str(value, "raft_node", f'chat server "{key}"'))
        if raft_node_id not in raft_nodes:
            raise ConfigError(
                f'chat server "{key}" refers to unknown raft node "{raft_node_id}"'
            )
        servers[key] = ChatServerConfig(
            server_id=key,
            host=_require_str(value, "host", f'chat server "{key}"'),
            port=_require_port(value, "port", f'chat server "{key}"'),
            raft_node_id=raft_node_id,
        )
    return servers


def _parse_logging(raw: Any) -> LoggingConfig:
    """Parse and validate the optional "logging" section of a configuration file."""
    if raw is None:
        return LoggingConfig()
    if not isinstance(raw, dict):
        raise ConfigError('"logging" must be a JSON object')

    directory = raw.get("directory", LoggingConfig.directory)
    if not isinstance(directory, str) or not directory:
        raise ConfigError('"logging" is missing a non-empty "directory" string')

    level = raw.get("level", LoggingConfig.level)
    if not isinstance(level, str) or level.upper() not in logging.getLevelNamesMapping():
        raise ConfigError(f'"logging" has an invalid "level": {level!r}')

    log_format = raw.get("format", LoggingConfig.format)
    if not isinstance(log_format, str) or not log_format:
        raise ConfigError('"logging" is missing a non-empty "format" string')

    return LoggingConfig(directory=directory, level=level.upper(), format=log_format)


def _require_str(source: dict[str, Any], key: str, owner: str) -> str:
    """Return source[key] as a non-empty string, or raise ConfigError."""
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigError(f'{owner} is missing a non-empty "{key}" string')
    return value


def _require_port(source: dict[str, Any], key: str, owner: str) -> int:
    """Return source[key] as a valid TCP port number, or raise ConfigError."""
    value = source.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 65536:
        raise ConfigError(f'{owner} is missing a valid "{key}" port number')
    return value
