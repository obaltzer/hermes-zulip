"""Full-stack E2E: a real `hermes gateway run` (two profiles, one bot each, stub model) against a
real Zulip server, driven by human accounts over the Zulip API.

    PYTHONPATH=$WT $PY e2e/run_gateway_e2e.py e2e/creds.json
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

import aiohttp

from plugins.platforms.zulip.client import ZulipClient

SITE, CHANNEL = "https://zulip.localhost:8443", "hermes"
PNG = bytes.fromhex("89504e470d0a1a0a0000000d4948445200000001000000010806000000"
                    "1f15c4890000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082")
results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f" — {detail}"), flush=True)


async def bot_replies(admin, topic, bot_id, after_id):
    msgs = await admin.get_messages([{"operator": "channel", "operand": CHANNEL},
                                     {"operator": "topic", "operand": topic}], num_before=100)
    return [m for m in msgs if m["sender_id"] == bot_id and m["id"] > after_id
            and not m["content"].startswith(("↪ Redirected", "⚠️ **Confirm"))]


async def ask(admin, stream_id, topic, text, bot_id, timeout=60, expect=1):
    sent = await admin.send_channel(stream_id, topic, text)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        replies = await bot_replies(admin, topic, bot_id, sent)
        if len(replies) >= expect:
            return sent, replies
        await asyncio.sleep(0.5)
    return sent, []


def turn(reply):
    m = re.search(r"turn=(\d+)", reply["content"] if reply else "")
    return int(m.group(1)) if m else None


async def main(creds_path):
    creds = json.loads(Path(creds_path).read_text())
    hermes_id, research_id = creds["Hermes"]["user_id"], creds["Research"]["user_id"]
    async with aiohttp.ClientSession() as session:
        admin = ZulipClient(SITE, creds["Admin"]["email"], creds["Admin"]["api_key"], session)
        bob = ZulipClient(SITE, creds["Bob"]["email"], creds["Bob"]["api_key"], session)
        stream_id = await admin.get_stream_id(CHANNEL)
        tag = str(int(time.time()))[-5:]
        t1, t2 = f"gw alpha {tag}", f"gw beta {tag}"

        _, r = await ask(admin, stream_id, t1, "@**Hermes** first in alpha", hermes_id)
        check("real agent answers a mention in its topic", bool(r) and "[stub-a] turn=1" in r[0]["content"],
              r and r[0]["content"])
        _, r = await ask(admin, stream_id, t1, "second in alpha, no mention", hermes_id)
        check("same topic continues the same session (turn 2)", bool(r) and turn(r[0]) == 2, r and r[0]["content"])
        _, r = await ask(admin, stream_id, t2, "@**Hermes** first in beta", hermes_id)
        check("other topic is a fresh session (turn 1)", bool(r) and turn(r[0]) == 1, r and r[0]["content"])

        await admin.send_channel(stream_id, t1, "/new")
        await asyncio.sleep(3)
        _, r = await ask(admin, stream_id, t1, "/approve", hermes_id)
        _, r = await ask(admin, stream_id, t1, "after reset", hermes_id)
        check("/new resets only this topic's session", bool(r) and turn(r[0]) == 1, r and r[0]["content"])
        _, r = await ask(admin, stream_id, t2, "beta again", hermes_id)
        check("…while the other topic keeps its history", bool(r) and turn(r[0]) == 2, r and r[0]["content"])

        # Images reach the model as images.
        links = []
        for i in range(2):
            url = await admin.upload(PNG, f"pic{i}.png", "image/png")
            links.append(f"[pic{i}.png]({url})")
        _, r = await ask(admin, stream_id, t2, "two pictures " + " ".join(links), hermes_id, timeout=90)
        check("multi-image message is one agent turn", bool(r) and turn(r[0]) == 3, r and r[0]["content"])
        print("   model saw:", r and r[0]["content"][:160])

        # Agent opens a topic.
        _, r = await ask(admin, stream_id, t2, f"please spin off the side work {tag}", hermes_id)
        spun = await bot_replies(admin, f"Gateway spin-off {tag}", hermes_id, 0)
        check("agent reply with TOPIC: opens a new topic",
              bool(spun) and "Opening message written by the agent" in spun[-1]["content"], spun)
        check("origin topic gets the link", bool(r) and f"Gateway spin-off {tag}" in r[-1]["content"], r)
        _, r = await ask(bob, stream_id, f"Gateway spin-off {tag}", "Bob continues here without a mention", hermes_id)
        latest = r[-1] if r else None
        check("human reply in the new topic → new session with origin context",
              bool(latest) and turn(latest) == 1 and "ctx=yes" in latest["content"], latest and latest["content"])

        # Profile hand-off.
        sent, r = await ask(admin, stream_id, t1, "@**Research** your turn", research_id)
        check("other profile's bot answers when mentioned", bool(r) and "[stub-b]" in r[0]["content"],
              r and r[0]["content"])
        await asyncio.sleep(3)
        hermes_after = await bot_replies(admin, t1, hermes_id, sent)
        check("first bot stays quiet after the hand-off", not hermes_after, hermes_after)

        # Rename keeps the session.
        first = (await admin.get_messages([{"operator": "channel", "operand": CHANNEL},
                                           {"operator": "topic", "operand": t2}], anchor="oldest", num_after=1))
        await admin.call("PATCH", f"messages/{first[0]['id']}", {
            "topic": f"✔ {t2}", "propagate_mode": "change_all",
            "send_notification_to_old_thread": False, "send_notification_to_new_thread": False})
        await asyncio.sleep(2)
        _, r = await ask(admin, stream_id, f"✔ {t2}", "after resolving", hermes_id, expect=1)
        check("resolved (renamed) topic keeps its session", bool(r) and (turn(r[-1]) or 0) >= 4,
              r and r[-1]["content"])

    print(f"\n{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1])))
