"""Entry point for the interactive chat client.

Connects to a chat server, sends whatever the user types, and prints incoming
messages.
"""

from __future__ import annotations

import argparse
import asyncio

from client.client import ChatClient


def parse_args() -> argparse.Namespace:
    """Parse the --host, --port, and --username command line arguments."""
    parser = argparse.ArgumentParser(description="Connect to a chat server.")
    parser.add_argument("--host", required=True, help="Chat server host.")
    parser.add_argument("--port", required=True, type=int, help="Chat server port.")
    parser.add_argument("--username", required=True, help="Name to join under.")
    return parser.parse_args()


async def main() -> None:
    """Connect, then read from standard input until the user quits."""
    args = parse_args()
    client = ChatClient(args.host, args.port, args.username)
    await client.connect()

    client.listen_task = asyncio.create_task(client.listen())
    try:
        while True:
            line = await asyncio.to_thread(input, "")
            if line.strip() in {"/quit", "/exit"}:
                break
            if line.strip():
                await client.send_message(line)
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
