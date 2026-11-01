"""Zulip addressing: chat ids, host-driven send targets, and the ``TOPIC:`` reply directive.

Chat ids the adapter hands to Hermes (and gets back on every send):

* ``s<stream_id>``          a channel conversation; the topic comes from ``thread_id``
* ``d<uid>[-<uid>...]``     a direct-message conversation (other participants, sorted)

Host-driven targets (``hermes send --to zulip:...``, cron ``deliver=zulip:...``, the home channel)
may also name a channel: ``123``, ``#name`` or ``name``, optionally followed by ``>topic`` or
``:topic``. ``thread_id`` is either a topic-registry id or a literal topic name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from plugins.platforms.zulip.topics import Topic, TopicRegistry

DEFAULT_TOPIC = "Hermes"

_DIRECTIVE_RE = re.compile(r"\ATOPIC:[ \t]*(?P<name>[^\n]*?)[ \t]*(?:\n|\Z)")
_DM_RE = re.compile(r"\Ad(\d+(?:-\d+)*)\Z")
_STREAM_ID_RE = re.compile(r"\As?(\d+)\Z")
_REGISTRY_ID_RE = re.compile(r"\At[0-9a-f]{16}\Z")


def parse_topic_directive(content: str) -> Optional[tuple[str, str]]:
    """``(topic name, body)`` when the reply's first line is ``TOPIC: <name>``, else None."""
    match = _DIRECTIVE_RE.match((content or "").lstrip())
    if not match or not match.group("name").strip():
        return None
    return match.group("name").strip(), (content or "").lstrip()[match.end():].strip()


def clamp_topic(name: str, max_length: int) -> str:
    name = " ".join((name or "").split())
    return name if len(name) <= max_length else name[: max_length - 1].rstrip() + "…"


def dm_chat_id(user_ids: list[int], self_id: int) -> str:
    others = sorted({int(u) for u in user_ids} - {int(self_id)}) or [int(self_id)]
    return "d" + "-".join(str(u) for u in others)


def split_channel_ref(ref: str) -> tuple[str, Optional[str]]:
    """``"eng>Deploys"`` / ``"eng:Deploys"`` / ``"#eng"`` → (channel ref, topic or None)."""
    ref = (ref or "").strip()
    for sep in (">", ":"):
        if sep in ref:
            channel, topic = ref.split(sep, 1)
            return channel.strip(), topic.strip() or None
    return ref, None


def parse_target_ref(raw: str) -> Optional[tuple[str, Optional[str]]]:
    """``parse_target_ref_fn``: native Zulip target syntax → (chat_id, thread_id)."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("dm:"):
        ids = [p.strip() for p in raw[3:].split(",") if p.strip()]
        if ids and all(p.isdigit() for p in ids):
            return "d" + "-".join(sorted(ids, key=int)), None
        return None
    if _DM_RE.match(raw):
        return raw, None
    channel, topic = split_channel_ref(raw)
    if not channel:
        return None
    if (m := _STREAM_ID_RE.match(channel)):
        return f"s{m.group(1)}", topic
    return "#" + channel.lstrip("#"), topic


def validate_target_ref(chat_id: str) -> bool | str:
    if _DM_RE.match(chat_id) or _STREAM_ID_RE.match(chat_id) or (chat_id.startswith("#") and len(chat_id) > 1):
        return True
    if ">" in chat_id:  # home-channel form "channel>topic"
        return True
    return "expected s<channel id>, #channel[>topic], or dm:<user id>[,<user id>...]"


@dataclass
class Destination:
    kind: str  # "channel" | "direct"
    stream_id: int = 0
    topic: str = ""
    user_ids: tuple[int, ...] = ()
    topic_id: Optional[str] = None  # registry id when the topic is already known

    @property
    def key(self) -> str:
        if self.kind == "direct":
            return "d" + "-".join(str(u) for u in self.user_ids)
        return f"s{self.stream_id}:{self.topic.casefold()}"


async def resolve_destination(chat_id: str, thread_id: Optional[str], *, registry: TopicRegistry | None,
                              stream_id_for, default_topic: str = DEFAULT_TOPIC) -> Destination:
    """Turn a Hermes (chat_id, thread_id) into a concrete Zulip destination.

    ``stream_id_for(name) -> int`` resolves channel names (awaitable). A registry ``thread_id``
    wins over ``chat_id`` so replies follow topics that were renamed or moved mid-turn."""
    chat_id = str(chat_id or "").strip()
    known: Topic | None = registry.get(thread_id) if (registry is not None and thread_id) else None
    if known is not None:
        return Destination("channel", known.stream_id, known.name, topic_id=known.topic_id)
    if (m := _DM_RE.match(chat_id)):
        return Destination("direct", user_ids=tuple(int(u) for u in m.group(1).split("-")))
    channel, topic = split_channel_ref(chat_id)
    if thread_id and _REGISTRY_ID_RE.match(thread_id):
        thread_id = None  # a registry id whose row is gone (topics merged): never a literal topic name
    topic = thread_id or topic or default_topic
    if (m := _STREAM_ID_RE.match(channel)):
        stream_id = int(m.group(1))
    else:
        stream_id = await stream_id_for(channel.lstrip("#"))
    existing = registry.find(stream_id, topic) if registry is not None else None
    return Destination("channel", stream_id, existing.name if existing else topic,
                       topic_id=existing.topic_id if existing else None)
