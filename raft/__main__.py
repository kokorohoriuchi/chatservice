"""Entry point for one Raft node process.

Loads the cluster configuration, builds the RaftConfig for the requested node,
constructs the RaftNode, and runs it until interrupted.
"""

from __future__ import annotations

import argparse
import asyncio

from config import load_config
from logging_setup import setup_logging
from raft.node import RaftNode
from raft.types import NodeId


def parse_args() -> argparse.Namespace:
    """Parse the --config and --node-id command line arguments."""
    parser = argparse.ArgumentParser(description="Run one Raft node process.")
    parser.add_argument("--config", required=True, help="Path to the cluster configuration file.")
    parser.add_argument("--node-id", required=True, help="Which raft node to run.")
    return parser.parse_args()


async def main() -> None:
    """Load the configuration, start the node, and wait until cancelled."""
    args = parse_args()
    cluster = load_config(args.config)
    logger = setup_logging(f"node-{args.node_id}", cluster.logging)
    node = RaftNode(cluster.raft_config_for(NodeId(args.node_id)))

    logger.info("starting raft node %s", args.node_id)
    await node.start()
    logger.info("raft node %s started", args.node_id)
    try:
        await asyncio.Event().wait()
    finally:
        await node.stop()
        logger.info("raft node %s stopped", args.node_id)


if __name__ == "__main__":
    asyncio.run(main())
