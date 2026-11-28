"""
"""

from __future__ import annotations

import json
from pathlib import Path

from chat_server.message import ChatMessage


class HistoryWriter:
    """Persist and retrieve a client's chat history using a JSONL file.

    Each line of the file stores one ChatMessage as a JSON object.
    The file persists chat history because the server does not save any history,
    while the raft nodes only save the most recent history.
    """
    
    def __init__(self, filename: str) -> None:
        """Create a chat history backed by a JSONL file.

        """
        path = Path("client") / filename
        if path.suffix:
            path = path.with_suffix(".jsonl")
        else:
            path = path.parent / f"{path.name}.jsonl"
            
        self.path = path
    
    
    def append(self, message: ChatMessage) -> None:
        """Append a committed chat message to the history file.

        """
        with self.path.open("a", encoding="utf-8") as file:
            json.dump(
                {
                    "sender": message.sender,
                    "text": message.text,
                    "log_index": message.log_index,
                },
                file,
            )
            file.write("\n")


    def all_messages(self) -> list[ChatMessage]:
        """Load and return the complete chat history.
        
        Reads the entire JSONL file and reconstructs every stored
        ChatMessage.
        """
        if not self.path.exists():
            return []

        messages: list[ChatMessage] = []

        with self.path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                data = json.loads(line)

                messages.append(
                    ChatMessage(
                        sender=data["sender"],
                        text=data["text"],
                        log_index=data["log_index"],
                    )
                )

        return messages
    
    
    def merge_history(self, messages: list[ChatMessage]) -> None:
        """Merge a history snapshot received from the server.

        The server may only provide recent history. Existing local
        messages are preserved. Messages whose log_index is already
        present locally are ignored.
        """
        local_messages = self.all_messages()

        known_indices = {
            message.log_index
            for message in local_messages
        }

        merged_messages = list(local_messages)

        for message in messages:
            if message.log_index not in known_indices:
                merged_messages.append(message)

    
        merged_messages.sort(
            key=lambda message: (
                message.log_index
                if message.log_index is not None
                else -1
            )
        )

        self._rewrite(merged_messages)
        
        
    def replace_history(self, messages: list[ChatMessage]) -> None:
        """Rewrite entire chat history with the snapshot received from the server

        """
        messages = sorted(
            messages,
            key=lambda message: message.log_index or -1,
        )
        self._rewrite(messages)
        

    def latest_log_index(self) -> int:
        """Return the highest log index stored locally.

        """
        messages = self.all_messages()

        if not messages:
            return 0
        
        latest = messages[-1].log_index

        # To keep mypy static type checker happy
        if latest is None:
            raise ValueError("History contains message without log_index")
        
        return latest
    
    
    def _rewrite(self, messages: list[ChatMessage]) -> None:
        """Rewrite the JSONL file from a collection of messages.

        """
        with self.path.open("w", encoding="utf-8") as file:
            for message in messages:
                json.dump(
                    {
                        "sender": message.sender,
                        "text": message.text,
                        "log_index": message.log_index,
                    },
                    file,
                )
                file.write("\n")   