"""Entry point for one chat server process.

Loads the cluster configuration, resolves which Raft node this chat server is
pinned to, constructs the RaftClient and ChatServer, and runs until interrupted.
"""

from __future__ import annotations

import argparse
import asyncio

from chat_server.raft_client import RaftClient
from chat_server.server import ChatServer
from config import load_config
from logging_setup import setup_logging


def parse_args() -> argparse.Namespace:
    """Parse the --config and --server-id command line arguments."""
    parser = argparse.ArgumentParser(description="Run one chat server process.")
    parser.add_argument("--config", required=True, help="Path to the cluster configuration file.")
    parser.add_argument("--server-id", required=True, help="Which chat server to run.")
    return parser.parse_args()


async def main() -> None:
    """Load the configuration, start the chat server, and wait until cancelled."""
    args = parse_args()
    cluster = load_config(args.config)
    logger = setup_logging(f"chat-{args.server_id}", cluster.logging)
    server_config = cluster.chat_server(args.server_id)
    raft_node = cluster.raft_node_for_chat_server(args.server_id)

    chat_server = ChatServer(
        host=server_config.host,
        port=server_config.port,
        raft_client=RaftClient(raft_node.host, raft_node.chat_api_port),
    )

    logger.info("starting chat server %s", args.server_id)
    await chat_server.start_chat_server()
    logger.info("chat server %s started", args.server_id)
    try:
        await asyncio.Event().wait()
    finally:
        await chat_server.stop()
        logger.info("chat server %s stopped", args.server_id)


if __name__ == "__main__":
    asyncio.run(main())
