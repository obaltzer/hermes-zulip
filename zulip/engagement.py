"""Who answers a Zulip message: mention parsing and the per-bot engagement rules.

Every Hermes bot subscribed to a channel sees every message there, so each bot decides on its own,
from the same event stream, whether a message is for it. The rules are deterministic over shared
inputs, so bots never need to coordinate:

1. An explicit (non-silent) mention of one or more bots: only those bots answer, and the first
   mentioned bot becomes the topic's ``last_addressed`` bot.
2. Unmentioned message: the topic's ``last_addressed`` bot answers (topic stickiness).
3. In a free-response channel, an unmentioned message in a topic nobody claimed claims it.
4. Bots only answer other bots that mention them, for at most ``hop_limit`` consecutive bot turns.
   Any human message resets the counter.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Optional

WILDCARDS = frozenset({"all", "everyone", "stream", "channel", "topic"})

# @**Name**, @**Name|123**, silent @_**Name** (user group mentions use single stars and never match).
_MENTION_RE = re.compile(r"@(_?)\*\*([^*\n]+?)\*\*")


@dataclass(frozen=True)
class ZulipUser:
    user_id: int
    full_name: str
    email: str = ""
    is_bot: bool = False


def mentioned_user_ids(content: str, users_by_name: Mapping[str, list[int]]) -> list[int]:
    """User ids explicitly mentioned (not silently, not wildcards), in order of first mention."""
    found: list[int] = []
    for silent, body in _MENTION_RE.findall(content or ""):
        if silent:
            continue
        name, _, uid = body.partition("|")
        if uid.strip().isdigit():
            ids = [int(uid)]
        elif name.strip().casefold() in WILDCARDS:
            continue
        else:
            ids = users_by_name.get(name.strip().casefold(), [])
        found.extend(i for i in ids if i not in found)
    return found


def strip_mentions_of(content: str, user: ZulipUser) -> str:
    """Remove non-silent mentions of ``user`` so the agent sees clean input."""
    def _drop(match: re.Match) -> str:
        silent, body = match.group(1), match.group(2)
        name, _, uid = body.partition("|")
        is_user = uid.strip() == str(user.user_id) or (not uid and name.strip().casefold() == user.full_name.casefold())
        return "" if is_user and not silent else match.group(0)
    return re.sub(r"[ \t]{2,}", " ", _MENTION_RE.sub(_drop, content or "")).strip()


@dataclass(frozen=True)
class Decision:
    respond: bool
    last_addressed: Optional[int]
    bot_hops: int


def decide(*, self_id: int, sender_is_bot: bool, self_mentioned: bool, other_bots_mentioned: list[int],
           last_addressed: Optional[int], bot_hops: int, hop_limit: int, free_response: bool,
           is_dm: bool) -> Decision:
    hops = bot_hops + 1 if sender_is_bot else 0
    within_hops = not sender_is_bot or hops <= hop_limit
    if is_dm:
        return Decision(within_hops, last_addressed, hops)
    if self_mentioned or other_bots_mentioned:
        addressed = self_id if self_mentioned else other_bots_mentioned[0]
        return Decision(self_mentioned and within_hops, addressed, hops)
    if sender_is_bot:
        return Decision(False, last_addressed, hops)
    if last_addressed == self_id:
        return Decision(True, last_addressed, hops)
    if last_addressed is None and free_response:
        return Decision(True, self_id, hops)
    return Decision(False, last_addressed, hops)
