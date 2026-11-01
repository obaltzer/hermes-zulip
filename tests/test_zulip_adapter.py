"""Zulip adapter against an in-process fake Zulip server: sessions per topic, topic lifecycle,
Hermes-opened topics, multi-attachment media, multi-bot engagement and the event loop."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType
from plugins.platforms.zulip.adapter import ZulipAdapter, _standalone_send
from plugins.platforms.zulip.topics import TopicRegistry
from tests.gateway.zulip_fake_server import (
    BOT_EMAIL, BOB_ID, HUMAN_ID, OTHER_BOT_ID, OTHER_STREAM_ID, STREAM, STREAM_ID, FakeZulip)
from tests.waiting import async_wait_until


@pytest_asyncio.fixture
async def fake():
    server = await FakeZulip().start()
    yield server
    await server.close()


def _config(fake, **extra):
    return PlatformConfig(enabled=True, extra={
        "site": fake.site, "email": BOT_EMAIL, "api_key": "secret", "text_batch_delay_seconds": 0,
        "history_backfill": 0, **extra})


@pytest_asyncio.fixture
async def adapter(fake):
    zulip = ZulipAdapter(_config(fake))
    zulip.handle_message = AsyncMock()
    assert await zulip.connect()
    yield zulip
    await zulip.disconnect()


def _events(adapter):
    return [call.args[0] for call in adapter.handle_message.await_args_list]


async def _deliver(adapter, msg, flags=()):
    await adapter._on_message(msg, list(flags))


class TestTopicSessions:
    @pytest.mark.asyncio
    async def test_each_topic_is_its_own_session(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** plan A", topic="Plan A"))
        await _deliver(adapter, fake.message("@**Hermes** plan B", topic="Plan B"))
        await _deliver(adapter, fake.message("@**Hermes** more", topic="plan a"))
        a, b, a_again = _events(adapter)
        assert a.text == "plan A"
        assert (a.source.chat_id, a.source.chat_type, a.source.chat_topic) == (f"s{STREAM_ID}", "channel", "Plan A")
        assert a.source.thread_id != b.source.thread_id
        assert a.source.thread_id == a_again.source.thread_id
        keys = {adapter._event_session_key(e) for e in (a, b, a_again)}
        assert len(keys) == 2

    @pytest.mark.asyncio
    async def test_followups_in_a_claimed_topic_need_no_mention(self, adapter, fake):
        await _deliver(adapter, fake.message("hello, nobody asked the bot", topic="t"))
        assert _events(adapter) == []
        await _deliver(adapter, fake.message("@**Hermes** help", topic="t"))
        await _deliver(adapter, fake.message("and another thing", topic="t", sender=BOB_ID))
        assert [e.text for e in _events(adapter)] == ["help", "and another thing"]
        assert _events(adapter)[1].source.user_name == "Bob"

    @pytest.mark.asyncio
    async def test_reply_goes_to_the_topic_even_after_rename(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** hi", topic="draft"))
        thread_id = _events(adapter)[0].source.thread_id
        await adapter._on_update_message({"type": "update_message", "stream_id": STREAM_ID, "orig_subject": "draft",
                                          "subject": "✔ final", "propagate_mode": "change_all", "message_ids": [1]})
        result = await adapter.send(f"s{STREAM_ID}", "done", metadata={"thread_id": thread_id})
        assert result.success
        assert (fake.sent[-1]["to"], fake.sent[-1]["topic"]) == (str(STREAM_ID), "✔ final")
        await _deliver(adapter, fake.message("still here?", topic="✔ final"))
        assert _events(adapter)[-1].source.thread_id == thread_id

    @pytest.mark.asyncio
    async def test_moving_a_topic_to_another_channel_keeps_the_session(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** hi", topic="move me"))
        first = _events(adapter)[0]
        await adapter._on_update_message({"stream_id": STREAM_ID, "new_stream_id": OTHER_STREAM_ID,
                                          "orig_subject": "move me", "propagate_mode": "change_all"})
        await _deliver(adapter, fake.message("now in ops", topic="move me", stream_id=OTHER_STREAM_ID))
        moved = _events(adapter)[-1]
        assert adapter._event_session_key(moved) == adapter._event_session_key(first)

    @pytest.mark.asyncio
    async def test_split_topic_starts_a_new_session_with_moved_messages(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** hi", topic="big"))
        moved = fake.message("side issue details", topic="side issue")
        fake.history = [fake.message("still in big", topic="big"), moved]
        await adapter._on_update_message({"stream_id": STREAM_ID, "orig_subject": "big", "subject": "side issue",
                                          "propagate_mode": "change_one", "message_ids": [moved["id"]]})
        await _deliver(adapter, fake.message("continue the side issue", topic="side issue"))
        original, split = _events(adapter)
        assert split.source.thread_id != original.source.thread_id
        assert "split off from #**eng>big**" in split.channel_context
        assert "side issue details" in split.channel_context


class TestHermesOpensTopics:
    @pytest.mark.asyncio
    async def test_topic_directive_opens_seeds_and_links(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** let's split the deploy work out", topic="planning"))
        origin = _events(adapter)[0]
        result = await adapter.send(origin.source.chat_id, "TOPIC: Deploy redesign\nHere is the plan.",
                                    metadata={"thread_id": origin.source.thread_id})
        assert result.success
        opening, link = fake.sent[-2:]
        assert (opening["topic"], opening["content"]) == ("Deploy redesign", "Here is the plan.")
        assert link["topic"] == "planning" and "#**eng>Deploy redesign**" in link["content"]
        # Files of the same turn follow the reply into the new topic.
        await adapter.send(origin.source.chat_id, "a follow-up note", metadata={"thread_id": origin.source.thread_id})
        assert fake.sent[-1]["topic"] == "Deploy redesign"
        await adapter.on_processing_complete(origin, __import__("gateway.platforms.event", fromlist=["x"]).ProcessingOutcome.SUCCESS)
        # A human answering in the new topic needs no mention, and the session knows its origin.
        await _deliver(adapter, fake.message("looks good, go on", topic="Deploy redesign"))
        spun = _events(adapter)[-1]
        assert spun.source.thread_id not in (None, origin.source.thread_id)
        assert "you opened this topic from #**eng>planning**" in spun.channel_context
        assert "Here is the plan." in spun.channel_context

    @pytest.mark.asyncio
    async def test_directive_in_a_dm_without_spinoff_channel_replies_inline(self, adapter, fake):
        result = await adapter.send(f"d{HUMAN_ID}", "TOPIC: New\nbody")
        assert result.success
        assert fake.sent[-1]["type"] == "direct" and fake.sent[-1]["content"].startswith("body")

    @pytest.mark.asyncio
    async def test_host_driven_send_to_a_new_topic_registers_it(self, adapter, fake):
        result = await adapter.send("eng>Weekly report", "Report for week 44")
        assert result.success and fake.sent[-1]["topic"] == "Weekly report"
        await _deliver(adapter, fake.message("thanks, what about X?", topic="weekly report"))
        event = _events(adapter)[-1]
        assert "Report for week 44" in event.channel_context

    @pytest.mark.asyncio
    async def test_standalone_send_registers_topic_for_the_gateway(self, fake, tmp_path):
        result = await _standalone_send(_config(fake), "#eng", "cron says hi", thread_id="Cron results")
        assert result["success"], result
        assert fake.sent[-1]["topic"] == "Cron results"
        zulip = ZulipAdapter(_config(fake))
        zulip.handle_message = AsyncMock()
        assert await zulip.connect()
        try:
            await _deliver(zulip, fake.message("what did cron find?", topic="cron results"))
            assert "cron says hi" in _events(zulip)[0].channel_context
        finally:
            await zulip.disconnect()


class TestMedia:
    @pytest.mark.asyncio
    async def test_inbound_message_with_several_attachments(self, adapter, fake):
        fake.uploads["/user_uploads/2/aa/1/one.png"] = b"\x89PNG\r\n\x1a\n" + b"0" * 64
        fake.uploads["/user_uploads/2/aa/2/two.png"] = b"\x89PNG\r\n\x1a\n" + b"1" * 64
        fake.uploads["/user_uploads/2/aa/3/spec.pdf"] = b"%PDF-1.7 fake"
        await _deliver(adapter, fake.message(
            "@**Hermes** compare these [one.png](/user_uploads/2/aa/1/one.png) "
            "[two.png](/user_uploads/2/aa/2/two.png) and [spec.pdf](/user_uploads/2/aa/3/spec.pdf)", topic="m"))
        event = _events(adapter)[0]
        assert len(event.media_urls) == 3
        assert event.media_types == ["image/png", "image/png", "application/pdf"]
        if hasattr(event, "media_filenames"):  # core > 0.22.5
            assert event.media_filenames == ["one.png", "two.png", "spec.pdf"]
        assert event.message_type == MessageType.DOCUMENT
        assert event.text == "compare these 📎 one.png 📎 two.png and 📎 spec.pdf"

    @pytest.mark.asyncio
    async def test_oversized_and_missing_attachments_are_reported_not_fatal(self, adapter, fake):
        fake.uploads["/user_uploads/2/aa/1/big.bin"] = b"0" * (26 * 1024 * 1024)
        await _deliver(adapter, fake.message(
            "@**Hermes** [big.bin](/user_uploads/2/aa/1/big.bin) [gone.txt](/user_uploads/2/aa/9/gone.txt)", topic="m"))
        event = _events(adapter)[0]
        assert event.media_urls == []
        assert "big.bin: not downloaded (larger than 25 MB)" in event.text
        assert "gone.txt: download failed" in event.text

    @pytest.mark.asyncio
    async def test_direct_download_never_sends_credentials_to_another_host(self, adapter, fake):
        from aiohttp import web
        from aiohttp.test_utils import TestServer
        seen = []

        async def storage(request):
            seen.append(request.headers.get("Authorization"))
            return web.Response(body=b"stored bytes")
        app = web.Application()
        app.router.add_get("/{rest:.+}", storage)
        other_host = TestServer(app, host="localhost")  # fake Zulip listens on 127.0.0.1: a different origin
        await other_host.start_server()
        try:
            fake.temp_urls, fake.redirect_uploads_to = False, str(other_host.make_url("")).rstrip("/")
            fake.uploads["/user_uploads/2/aa/1/notes.txt"] = b"unused"
            await _deliver(adapter, fake.message("@**Hermes** [notes.txt](/user_uploads/2/aa/1/notes.txt)", topic="s3"))
            event = _events(adapter)[0]
            assert len(event.media_urls) == 1
            assert seen == [None]
        finally:
            await other_host.close()

    @pytest.mark.asyncio
    async def test_outbound_images_and_files_become_one_message(self, adapter, fake, tmp_path):
        paths = []
        for name in ("a.png", "b.png", "report.pdf"):
            (tmp_path / name).write_bytes(b"data-" + name.encode())
            paths.append(tmp_path / name)
        await _deliver(adapter, fake.message("@**Hermes** draw", topic="art"))
        meta = {"thread_id": _events(adapter)[0].source.thread_id}
        result = await adapter.send_multiple_images(
            f"s{STREAM_ID}", [(f"file://{paths[0]}", ""), (f"file://{paths[1]}", "")], metadata=meta)
        assert result.success
        assert len(fake.sent) == 1 and fake.sent[0]["content"].count("/user_uploads/") == 2
        doc = await adapter.send_document(f"s{STREAM_ID}", str(paths[2]), metadata=meta)
        assert doc.success and doc.message_id == str(fake.sent[0]["id"])
        assert len(fake.sent) == 1 and fake.edits[-1]["content"].count("/user_uploads/") == 3
        assert fake.uploads[fake.edits[-1]["content"].rsplit("(", 1)[1].rstrip(")")] == b"data-report.pdf"

    @pytest.mark.asyncio
    async def test_upload_larger_than_server_limit_is_refused_with_a_note(self, adapter, fake, tmp_path):
        big = tmp_path / "huge.zip"
        big.write_bytes(b"0" * (2 * 1024 * 1024))  # fake server allows 1 MiB
        result = await adapter.send_document(f"d{HUMAN_ID}", str(big))
        assert not result.success
        assert "too large" in fake.sent[-1]["content"] and not fake.uploads


class TestMultipleBots:
    @pytest.mark.asyncio
    async def test_message_for_another_bot_is_left_alone(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** start", topic="t"))
        await _deliver(adapter, fake.message("@**Research** your turn", topic="t"))
        await _deliver(adapter, fake.message("unaddressed follow-up for research", topic="t"))
        assert [e.text for e in _events(adapter)] == ["start"]

    @pytest.mark.asyncio
    async def test_bot_to_bot_requires_mention_and_respects_hop_limit(self, fake):
        zulip = ZulipAdapter(_config(fake, bot_hop_limit=1))
        zulip.handle_message = AsyncMock()
        assert await zulip.connect()
        try:
            await _deliver(zulip, fake.message("@**Hermes** delegate", topic="t", sender=OTHER_BOT_ID))
            await _deliver(zulip, fake.message("@**Hermes** again", topic="t", sender=OTHER_BOT_ID))
            assert [e.text for e in _events(zulip)] == ["delegate"]
            assert _events(zulip)[0].source.is_bot
        finally:
            await zulip.disconnect()

    @pytest.mark.asyncio
    async def test_bot_credentials_held_by_another_gateway_refuse_to_connect(self, fake, monkeypatch):
        import gateway.status as gateway_status
        monkeypatch.setattr(gateway_status, "acquire_scoped_lock",
                            lambda scope, identity, metadata=None: (False, {"pid": 4242, "profile": "other"}))
        zulip = ZulipAdapter(_config(fake))
        assert await zulip.connect() is False
        assert zulip._fatal_error_code == "zulip_lock"
        assert fake.registrations == 0


class TestContextAndDirect:
    @pytest.mark.asyncio
    async def test_first_engagement_includes_earlier_topic_history(self, fake):
        fake.history = [fake.message("we need a rollback plan", topic="incident", sender=BOB_ID)]
        zulip = ZulipAdapter(_config(fake, history_backfill=20))
        zulip.handle_message = AsyncMock()
        assert await zulip.connect()
        try:
            await _deliver(zulip, fake.message("@**Hermes** thoughts?", topic="incident"))
            context = _events(zulip)[0].channel_context
            assert "Earlier messages in this topic" in context and "Bob: we need a rollback plan" in context
            fake.history.append(fake.message("unaddressed aside", topic="incident", sender=BOB_ID))
            await _deliver(zulip, fake.message("and now?", topic="incident"))
            later = _events(zulip)[1].channel_context
            assert "since your last reply" in later and "unaddressed aside" in later
            assert "rollback plan" not in later
        finally:
            await zulip.disconnect()

    @pytest.mark.asyncio
    async def test_direct_messages(self, adapter, fake):
        await _deliver(adapter, fake.dm("hi there"))
        await _deliver(adapter, fake.dm("group chat", others=(BOB_ID,)))
        one, group = _events(adapter)
        assert (one.source.chat_id, one.source.chat_type, one.source.thread_id) == (f"d{HUMAN_ID}", "dm", None)
        assert (group.source.chat_id, group.source.chat_type) == (f"d{HUMAN_ID}-{BOB_ID}", "group")
        await adapter.send(group.source.chat_id, "hello both")
        assert fake.sent[-1]["type"] == "direct" and json.loads(fake.sent[-1]["to"]) == [HUMAN_ID, BOB_ID]

    @pytest.mark.asyncio
    async def test_commands_and_wildcard_defusing(self, adapter, fake):
        await _deliver(adapter, fake.message("@**Hermes** /new", topic="cmd"))
        assert _events(adapter)[0].message_type == MessageType.COMMAND
        await adapter.send(f"s{STREAM_ID}", "ping @**all** now", metadata={"thread_id": _events(adapter)[0].source.thread_id})
        assert "@**all**" not in fake.sent[-1]["content"]

    @pytest.mark.asyncio
    async def test_ack_reactions(self, adapter, fake):
        from gateway.platforms.event import ProcessingOutcome
        await _deliver(adapter, fake.message("@**Hermes** go", topic="r", msg_id=7777))
        event = _events(adapter)[0]
        await adapter.on_processing_start(event)
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        assert fake.reactions == [("POST", 7777, "eyes"), ("DELETE", 7777, "eyes"), ("POST", 7777, "check")]


class TestEventLoop:
    @pytest.mark.asyncio
    async def test_events_flow_and_expired_queue_recovers(self, adapter, fake):
        await fake.events.put({"type": "message", "id": 1, "flags": ["mentioned"],
                               "message": fake.message("@**Hermes** via the queue", topic="live")})
        await async_wait_until(lambda: adapter.handle_message.await_count == 1, timeout=10)
        fake.expire_queue_once = True
        await fake.events.put({"type": "message", "id": 2, "flags": [],
                               "message": fake.message("after re-register", topic="live")})
        await async_wait_until(lambda: adapter.handle_message.await_count == 2, timeout=15)
        assert fake.registrations == 2
        assert _events(adapter)[1].text == "after re-register"

    @pytest.mark.asyncio
    async def test_redelivered_message_is_dropped(self, adapter, fake):
        msg = fake.message("@**Hermes** once", topic="dup")
        await _deliver(adapter, msg)
        await _deliver(adapter, msg)
        assert adapter.handle_message.await_count == 1

    @pytest.mark.asyncio
    async def test_allowlist_emails_resolve_to_user_ids(self, fake, caplog):
        zulip = ZulipAdapter(_config(fake, allowed_users="alice@example.com, 42, ghost@example.com"))
        assert await zulip.connect()
        try:
            assert zulip.resolved_allowlist_user_ids() == {str(HUMAN_ID)}
            assert "ghost@example.com matches no user" in caplog.text
        finally:
            await zulip.disconnect()

    @pytest.mark.asyncio
    async def test_old_server_is_refused(self, fake):
        fake.feature_level = 200
        zulip = ZulipAdapter(_config(fake))
        assert await zulip.connect() is False
        assert zulip._fatal_error_code == "zulip_unsupported_server"

    @pytest.mark.asyncio
    async def test_registry_is_per_profile_home(self, adapter, fake, tmp_path):
        from hermes_constants import get_hermes_home
        await _deliver(adapter, fake.message("@**Hermes** x", topic="persisted"))
        db = get_hermes_home() / "platforms" / "zulip" / f"{BOT_EMAIL}.db"
        assert db.is_file()
        assert TopicRegistry(db).find(STREAM_ID, "persisted") is not None
