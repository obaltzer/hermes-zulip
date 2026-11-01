"""Per-profile Zulip topic registry: a stable id for every (channel, topic) conversation.

Zulip has no server-side topic id, matches topic names case-insensitively, and lets users rename,
resolve (``✔ `` prefix), move and split topics. Hermes keys sessions on ``thread_id`` and splits
session keys on ``:``. The registry bridges the two: each conversation gets an opaque ``t<hex>``
id that never changes, and rename/move events update the row instead of the session key.

SQLite file under the owning profile's ``HERMES_HOME`` (path resolved by the adapter inside its
profile scope); every call is a short synchronous query guarded by one lock.
"""

from __future__ import annotations

import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

RESOLVED_PREFIX = "✔ "

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY,
    origin_stream_id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT 'user',
    parent_topic_id TEXT,
    seed TEXT,
    last_addressed_bot INTEGER,
    bot_hops INTEGER NOT NULL DEFAULT 0,
    engaged INTEGER NOT NULL DEFAULT 0,
    last_handled_id INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    UNIQUE (stream_id, name_key)
);
CREATE TABLE IF NOT EXISTS cursor (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_COLUMNS = ("topic_id", "origin_stream_id", "stream_id", "name", "name_key", "created_by",
            "parent_topic_id", "seed", "last_addressed_bot", "bot_hops", "engaged", "last_handled_id")


def topic_key(name: str) -> str:
    """Identity of a topic name: resolve prefix stripped, whitespace trimmed, case folded."""
    name = (name or "").strip()
    if name.startswith(RESOLVED_PREFIX.strip()):
        name = name[len(RESOLVED_PREFIX.strip()):].strip()
    return name.casefold()


@dataclass
class Topic:
    topic_id: str
    origin_stream_id: int
    stream_id: int
    name: str
    name_key: str
    created_by: str = "user"
    parent_topic_id: Optional[str] = None
    seed: Optional[str] = None
    last_addressed_bot: Optional[int] = None
    bot_hops: int = 0
    engaged: bool = False
    last_handled_id: int = 0  # newest message id already in this topic's session transcript

    @property
    def chat_id(self) -> str:
        """Hermes ``chat_id`` for this topic's session: the channel it was born in."""
        return f"s{self.origin_stream_id}"


class TopicRegistry:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _row(self, sql: str, args: tuple) -> Optional[Topic]:
        with self._lock:
            row = self._db.execute(f"SELECT {', '.join(_COLUMNS)} FROM topics {sql}", args).fetchone()
        if row is None:
            return None
        topic = Topic(*row)
        topic.engaged = bool(topic.engaged)
        return topic

    def get(self, topic_id: str) -> Optional[Topic]:
        return self._row("WHERE topic_id = ?", (topic_id,))

    def find(self, stream_id: int, name: str) -> Optional[Topic]:
        return self._row("WHERE stream_id = ? AND name_key = ?", (int(stream_id), topic_key(name)))

    def create(self, stream_id: int, name: str, *, created_by: str = "user",
               parent_topic_id: Optional[str] = None, seed: Optional[str] = None,
               last_addressed_bot: Optional[int] = None) -> Topic:
        topic = Topic(topic_id="t" + secrets.token_hex(8), origin_stream_id=int(stream_id),
                      stream_id=int(stream_id), name=name, name_key=topic_key(name),
                      created_by=created_by, parent_topic_id=parent_topic_id, seed=seed,
                      last_addressed_bot=last_addressed_bot)
        with self._lock:
            self._db.execute(
                f"INSERT INTO topics ({', '.join(_COLUMNS)}, updated_at) VALUES ({', '.join('?' * len(_COLUMNS))}, ?)",
                (topic.topic_id, topic.origin_stream_id, topic.stream_id, topic.name, topic.name_key,
                 topic.created_by, topic.parent_topic_id, topic.seed, topic.last_addressed_bot,
                 topic.bot_hops, int(topic.engaged), topic.last_handled_id, time.time()))
        return topic

    def get_or_create(self, stream_id: int, name: str, **kwargs) -> tuple[Topic, bool]:
        """(topic, created). Display name follows the latest casing seen."""
        existing = self.find(stream_id, name)
        if existing is not None:
            if existing.name != name:
                self.update(existing.topic_id, name=name)
                existing.name = name
            return existing, False
        return self.create(stream_id, name, **kwargs), True

    def update(self, topic_id: str, **fields) -> None:
        if "name" in fields:
            fields["name_key"] = topic_key(fields["name"])
        if "engaged" in fields:
            fields["engaged"] = int(bool(fields["engaged"]))
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self._lock:
            self._db.execute(f"UPDATE topics SET {assignments}, updated_at = ? WHERE topic_id = ?",
                             (*fields.values(), time.time(), topic_id))

    def move(self, stream_id: int, old_name: str, new_stream_id: int, new_name: str) -> Optional[Topic]:
        """Whole-topic rename / resolve / move. The session follows the row. When the destination
        already has a registered topic (topics merged) the destination wins and the source row is
        dropped, since its messages now live in the destination conversation."""
        source = self.find(stream_id, old_name)
        if source is None:
            return None
        dest = self.find(new_stream_id, new_name)
        if dest is not None and dest.topic_id != source.topic_id:
            with self._lock:
                self._db.execute("DELETE FROM topics WHERE topic_id = ?", (source.topic_id,))
            return dest
        self.update(source.topic_id, stream_id=int(new_stream_id), name=new_name)
        return self.get(source.topic_id)

    def pop_seed(self, topic_id: str) -> Optional[str]:
        topic = self.get(topic_id)
        if topic is None or not topic.seed:
            return None
        self.update(topic_id, seed=None)
        return topic.seed

    def get_cursor(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._db.execute("SELECT value FROM cursor WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_cursor(self, key: str, value: str) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO cursor (key, value) VALUES (?, ?)", (key, str(value)))
