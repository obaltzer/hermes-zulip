"""End-to-end check of the Hermes Zulip adapter against a real Zulip server.

Two adapter instances play two Hermes profiles (bots "Hermes" and "Research"); a stand-in agent
answers each inbound event so we can observe routing, sessions, topic creation and media on the
real server. Run as in e2e/README.md (WT/PY set there):

    HERMES_HOME=$(mktemp -d) PYTHONPATH=$WT $PY e2e/run_e2e.py creds.json
"""

import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

import aiohttp

from gateway.config import PlatformConfig
from gateway.platforms.event import ProcessingOutcome
from plugins.platforms.zulip.adapter import ZulipAdapter, _standalone_send
from plugins.platforms.zulip.client import ZulipClient

SITE = "https://zulip.localhost:8443"
CHANNEL = "hermes"
PNG = bytes.fromhex("89504e470d0a1a0a0000000d4948445200000001000000010806000000"
                    "1f15c4890000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082")

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(("PASS " if ok else "FAIL ") + name + (f" — {detail}" if detail and not ok else ""))


async def wait_for(predicate, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        await asyncio.sleep(0.3)
    return None


class StandInAgent:
    """Replaces the Hermes agent: records events and answers the way the real agent would."""

    def __init__(self, adapter: ZulipAdapter, name: str, files_dir: Path):
        self.adapter, self.name, self.files_dir, self.events = adapter, name, files_dir, []
        adapter.handle_message = self.handle

    async def handle(self, event):
        self.events.append(event)
        meta = {"thread_id": event.source.thread_id} if event.source.thread_id else None
        await self.adapter.on_processing_start(event)
        text = event.text or ""
        if "spin off" in text:
            reply = "TOPIC: Spun off work\nOpening message from Hermes about the side project."
        else:
            reply = (f"[{self.name}] got: {text[:60]!r} media={len(event.media_urls)} "
                     f"context={'yes' if event.channel_context else 'no'}")
        await self.adapter.send(event.source.chat_id, reply, metadata=meta)
        if "send files" in text:
            a, b, doc = (self.files_dir / n for n in ("one.png", "two.png", "notes.txt"))
            await self.adapter.send_multiple_images(event.source.chat_id, [(f"file://{a}", ""), (f"file://{b}", "")],
                                                    metadata=meta)
            await self.adapter.send_document(event.source.chat_id, str(doc), metadata=meta)
        await self.adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)


async def topic_messages(admin: ZulipClient, topic: str) -> list[dict]:
    return await admin.get_messages([{"operator": "channel", "operand": CHANNEL},
                                     {"operator": "topic", "operand": topic}], num_before=50)


async def main(creds_path: str) -> int:
    creds = json.loads(Path(creds_path).read_text())
    files_dir = Path(tempfile.mkdtemp())
    (files_dir / "one.png").write_bytes(PNG)
    (files_dir / "two.png").write_bytes(PNG)
    (files_dir / "notes.txt").write_text("meeting notes")

    def adapter_for(name: str, **extra) -> ZulipAdapter:
        return ZulipAdapter(PlatformConfig(enabled=True, extra={
            "site": SITE, "email": creds[name]["email"], "api_key": creds[name]["api_key"],
            "channels": [CHANNEL], "text_batch_delay_seconds": 0, **extra}))

    async with aiohttp.ClientSession() as session:
        admin = ZulipClient(SITE, creds["Admin"]["email"], creds["Admin"]["api_key"], session)
        bob = ZulipClient(SITE, creds["Bob"]["email"], creds["Bob"]["api_key"], session)
        await admin.subscribe([CHANNEL])
        await bob.subscribe([CHANNEL])
        stream_id = await admin.get_stream_id(CHANNEL)
        await bob.send_channel(stream_id, "incident", "Bob: the deploy failed at step 3")

        hermes, research = adapter_for("Hermes"), adapter_for("Research")
        agent_h, agent_r = StandInAgent(hermes, "Hermes", files_dir), StandInAgent(research, "Research", files_dir)
        check("connect bot 1 (profile A)", await hermes.connect())
        check("connect bot 2 (profile B)", await research.connect())
        try:
            await scenario(admin, stream_id, agent_h, agent_r, creds)
        finally:
            await hermes.disconnect()
            await research.disconnect()
        sent = await _standalone_send(PlatformConfig(enabled=True, extra={
            "site": SITE, "email": creds["Hermes"]["email"], "api_key": creds["Hermes"]["api_key"]}),
            CHANNEL, "Cron report: all green", thread_id="Cron results")
        msgs = await topic_messages(admin, "Cron results")
        check("standalone (cron) send creates topic", sent.get("success") and
              any("all green" in m["content"] for m in msgs), str(sent))

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


async def scenario(admin, stream_id, agent_h, agent_r, creds):
    hermes = agent_h.adapter

    await admin.send_channel(stream_id, "Topic A", "@**Hermes** hello from topic A")
    await admin.send_channel(stream_id, "Topic B", "@**Hermes** hello from topic B")
    ok = await wait_for(lambda: len(agent_h.events) >= 2)
    a, b = (agent_h.events + [None, None])[:2]
    check("mention brings Hermes into a topic", bool(ok))
    check("two topics are two sessions", bool(ok) and a.source.thread_id != b.source.thread_id
          and hermes._event_session_key(a) != hermes._event_session_key(b))
    replies = await wait_for(lambda: topic_messages(admin, "Topic A"))
    check("reply posted in the same topic", bool(replies) and any("[Hermes] got" in m["content"] for m in replies))

    await admin.send_channel(stream_id, "topic a", "follow-up without mention")
    ok = await wait_for(lambda: len(agent_h.events) >= 3)
    check("unmentioned follow-up handled (sticky); case-insensitive topic",
          bool(ok) and agent_h.events[2].source.thread_id == a.source.thread_id)

    await admin.send_channel(stream_id, "incident", "@**Hermes** what happened here?")
    ok = await wait_for(lambda: len(agent_h.events) >= 4)
    ctx = agent_h.events[3].channel_context or "" if ok else ""
    check("first engagement gets earlier topic history", "deploy failed at step 3" in ctx, ctx[:200])

    # Three attachments in one message.
    links = []
    for name, data in (("one.png", PNG), ("two.png", PNG), ("spec.txt", b"spec body")):
        url = await admin.upload(data, name, "image/png" if name.endswith(".png") else "text/plain")
        links.append(f"[{name}]({url})")
    await admin.send_channel(stream_id, "Media", "@**Hermes** look at " + " ".join(links))
    ok = await wait_for(lambda: len(agent_h.events) >= 5)
    media = agent_h.events[4] if ok else None
    check("inbound message with 3 attachments → one event, 3 files",
          bool(media) and len(media.media_urls) == 3 and media.media_types[:2] == ["image/png", "image/png"],
          str(media and media.media_types))

    await admin.send_channel(stream_id, "Media", "please send files")
    ok = await wait_for(lambda: len(agent_h.events) >= 6)
    await asyncio.sleep(2)
    msgs = await topic_messages(admin, "Media")
    file_msgs = [m for m in msgs if m["sender_id"] == creds["Hermes"]["user_id"] and "/user_uploads/" in m["content"]]
    check("outbound images + document → one message with 3 uploads",
          len(file_msgs) == 1 and file_msgs[0]["content"].count("/user_uploads/") == 3,
          json.dumps([m["content"] for m in file_msgs])[:300])

    # Hermes opens a topic.
    await admin.send_channel(stream_id, "Topic B", "please spin off the side project")
    ok = await wait_for(lambda: len(agent_h.events) >= 7)
    spun = await wait_for(lambda: topic_messages(admin, "Spun off work"))
    origin = await topic_messages(admin, "Topic B")
    check("TOPIC: directive posts the opening message in a new topic",
          bool(spun) and "Opening message from Hermes" in spun[0]["content"])
    check("origin topic links to the new topic", any("#**hermes>Spun off work**" in m["content"] for m in origin))
    await admin.send_channel(stream_id, "Spun off work", "great, continue here (no mention)")
    ok = await wait_for(lambda: len(agent_h.events) >= 8)
    new_ev = agent_h.events[7] if ok else None
    check("reply in Hermes-opened topic: new session seeded with its origin",
          bool(new_ev) and new_ev.source.thread_id not in (a.source.thread_id, b.source.thread_id)
          and "you opened this topic from" in (new_ev.channel_context or ""), new_ev and new_ev.channel_context)

    # Rename keeps the session.
    first_a = (await topic_messages(admin, "Topic A"))[0]["id"]
    await admin.call("PATCH", f"messages/{first_a}", {"topic": "Topic A renamed", "propagate_mode": "change_all",
                                                      "send_notification_to_old_thread": False,
                                                      "send_notification_to_new_thread": False})
    await asyncio.sleep(2)
    await admin.send_channel(stream_id, "Topic A renamed", "after rename")
    ok = await wait_for(lambda: len(agent_h.events) >= 9)
    check("renamed topic keeps its session", bool(ok) and agent_h.events[8].source.thread_id == a.source.thread_id)
    renamed = await wait_for(lambda: topic_messages(admin, "Topic A renamed"))
    check("reply lands in the renamed topic", any("after rename" in m["content"] and m["sender_id"] ==
                                                  creds["Hermes"]["user_id"] for m in renamed or []))

    # Hand-off to the other profile's bot.
    before_h = len(agent_h.events)
    await admin.send_channel(stream_id, "Topic A renamed", "@**Research** please take over")
    ok_r = await wait_for(lambda: len(agent_r.events) >= 1)
    await admin.send_channel(stream_id, "Topic A renamed", "unmentioned follow-up for research")
    ok_r2 = await wait_for(lambda: len(agent_r.events) >= 2)
    await asyncio.sleep(2)
    check("mentioning another profile's bot hands the topic over",
          bool(ok_r) and bool(ok_r2) and len(agent_h.events) == before_h,
          f"hermes events +{len(agent_h.events) - before_h}, research {len(agent_r.events)}")

    # Direct message.
    await admin.send_direct([creds["Hermes"]["user_id"]], "hi in a DM")
    ok = await wait_for(lambda: any(e.source.chat_type == "dm" for e in agent_h.events))
    check("direct message handled", bool(ok))


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1])))
