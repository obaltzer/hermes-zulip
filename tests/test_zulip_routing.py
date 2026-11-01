"""Zulip plugin: topic identity, engagement rules, targets and the TOPIC: directive (pure units)."""

import pytest

from plugins.platforms.zulip.engagement import ZulipUser, decide, mentioned_user_ids, strip_mentions_of
from plugins.platforms.zulip.media import find_uploads, message_type_for
from plugins.platforms.zulip.targets import (
    Destination, clamp_topic, parse_target_ref, parse_topic_directive, resolve_destination, validate_target_ref)
from plugins.platforms.zulip.topics import TopicRegistry, topic_key
from gateway.platforms.event import MessageType

USERS_BY_NAME = {"hermes": [100], "research": [200], "alice": [10]}


@pytest.fixture
def registry(tmp_path):
    reg = TopicRegistry(tmp_path / "topics.db")
    yield reg
    reg.close()


class TestTopicRegistry:
    def test_case_and_resolve_prefix_share_one_topic(self, registry):
        topic = registry.create(5, "Deploy Plan")
        assert registry.find(5, "deploy plan").topic_id == topic.topic_id
        assert registry.find(5, "✔ Deploy Plan").topic_id == topic.topic_id
        assert registry.find(6, "Deploy Plan") is None
        assert topic_key("✔ Deploy Plan") == topic_key("deploy plan")

    def test_topic_ids_never_contain_session_key_separator(self, registry):
        assert ":" not in registry.create(5, "a: b: c").topic_id

    def test_rename_and_move_keep_the_topic_id_and_origin(self, registry):
        topic = registry.create(5, "old")
        moved = registry.move(5, "old", 6, "new name")
        assert moved.topic_id == topic.topic_id
        assert (moved.stream_id, moved.name, moved.origin_stream_id) == (6, "new name", 5)
        assert moved.chat_id == "s5"
        assert registry.find(5, "old") is None

    def test_merge_into_existing_topic_keeps_destination(self, registry):
        source, dest = registry.create(5, "a"), registry.create(5, "b")
        assert registry.move(5, "a", 5, "b").topic_id == dest.topic_id
        assert registry.get(source.topic_id) is None

    def test_seed_is_consumed_once(self, registry):
        topic = registry.create(5, "x", seed="context")
        assert registry.pop_seed(topic.topic_id) == "context"
        assert registry.pop_seed(topic.topic_id) is None

    def test_state_survives_reopen(self, tmp_path):
        reg = TopicRegistry(tmp_path / "t.db")
        topic = reg.create(5, "persist", last_addressed_bot=100)
        reg.set_cursor("last_message_id", "42")
        reg.close()
        reg = TopicRegistry(tmp_path / "t.db")
        assert reg.find(5, "PERSIST").last_addressed_bot == 100
        assert reg.get_cursor("last_message_id") == "42"
        assert reg.get(topic.topic_id) is not None
        reg.close()


class TestMentions:
    def test_explicit_mentions_in_order_ignoring_silent_and_wildcards(self):
        content = "@**Research** and @**Hermes|100** but not @_**Alice** or @**all** / @**topic**"
        assert mentioned_user_ids(content, USERS_BY_NAME) == [200, 100]

    def test_strip_own_mention_only(self):
        me = ZulipUser(100, "Hermes")
        assert strip_mentions_of("@**Hermes** ask @**Research** please", me) == "ask @**Research** please"
        assert strip_mentions_of("hi @**Hermes|100**", me) == "hi"


def _decide(**overrides):
    args = dict(self_id=100, sender_is_bot=False, self_mentioned=False, other_bots_mentioned=[],
                last_addressed=None, bot_hops=0, hop_limit=3, free_response=False, is_dm=False)
    args.update(overrides)
    return decide(**args)


class TestEngagement:
    def test_mention_claims_the_topic(self):
        d = _decide(self_mentioned=True)
        assert d.respond and d.last_addressed == 100

    def test_sticky_topic_answers_unmentioned_followups(self):
        assert _decide(last_addressed=100).respond

    def test_unclaimed_topic_is_ignored_outside_free_response(self):
        assert not _decide().respond
        free = _decide(free_response=True)
        assert free.respond and free.last_addressed == 100

    def test_mention_of_another_bot_hands_the_topic_over(self):
        d = _decide(last_addressed=100, other_bots_mentioned=[200])
        assert not d.respond and d.last_addressed == 200
        assert not _decide(last_addressed=200).respond

    def test_bots_need_a_mention_and_stop_at_hop_limit(self):
        assert not _decide(sender_is_bot=True, last_addressed=100).respond
        assert _decide(sender_is_bot=True, self_mentioned=True, bot_hops=2).respond
        assert not _decide(sender_is_bot=True, self_mentioned=True, bot_hops=3).respond
        assert _decide(bot_hops=3, last_addressed=100).bot_hops == 0  # a human resets the chain

    def test_dm_always_answers_humans(self):
        assert _decide(is_dm=True).respond


class TestTopicDirective:
    def test_first_line_directive(self):
        assert parse_topic_directive("TOPIC: Deploy redesign\nLet's plan it.\n\nMore") == (
            "Deploy redesign", "Let's plan it.\n\nMore")
        assert parse_topic_directive("\n  TOPIC:  Only name  ") == ("Only name", "")

    def test_not_a_directive(self):
        assert parse_topic_directive("Sure.\nTOPIC: nope") is None
        assert parse_topic_directive("```\nTOPIC: in code\n```") is None
        assert parse_topic_directive("TOPIC:   \nbody") is None
        assert parse_topic_directive("topics: lower case") is None

    def test_clamp_topic(self):
        assert clamp_topic("a" * 80, 60) == "a" * 59 + "…"
        assert clamp_topic("two\nlines", 60) == "two lines"


class TestTargets:
    @pytest.mark.parametrize("raw, expected", [
        ("eng>Weekly report", ("#eng", "Weekly report")),
        ("#eng:Topic: with colon", ("#eng", "Topic: with colon")),
        ("5", ("s5", None)),
        ("s5>x", ("s5", "x")),
        ("dm:11,10", ("d10-11", None)),
        ("d10", ("d10", None)),
        ("dm:alice", None),
    ])
    def test_parse_target_ref(self, raw, expected):
        assert parse_target_ref(raw) == expected

    def test_validate(self):
        assert validate_target_ref("s5") is True
        assert validate_target_ref("eng>topic") is True
        assert isinstance(validate_target_ref("bogus"), str)

    @pytest.mark.asyncio
    async def test_registry_id_wins_over_chat_id(self, registry):
        topic = registry.create(5, "renamed later")
        registry.move(5, "renamed later", 6, "Now Here")

        async def no_lookup(name):
            raise AssertionError("no channel lookup expected")
        dest = await resolve_destination("s5", topic.topic_id, registry=registry, stream_id_for=no_lookup)
        assert (dest.kind, dest.stream_id, dest.topic, dest.topic_id) == ("channel", 6, "Now Here", topic.topic_id)

    @pytest.mark.asyncio
    async def test_named_channel_and_default_topic(self, registry):
        async def lookup(name):
            return {"eng": 5}[name]
        dest = await resolve_destination("#eng", None, registry=registry, stream_id_for=lookup)
        assert dest == Destination("channel", 5, "Hermes")
        direct = await resolve_destination("d10-11", None, registry=registry, stream_id_for=lookup)
        assert direct.user_ids == (10, 11)

    @pytest.mark.asyncio
    async def test_stale_registry_id_is_never_used_as_a_topic_name(self, registry):
        async def lookup(name):
            return 5
        dest = await resolve_destination("s5", "t0123456789abcdef", registry=registry, stream_id_for=lookup)
        assert dest.topic == "Hermes"


class TestUploadLinks:
    def test_finds_every_upload_in_order(self):
        content = ("look [a.png](/user_uploads/2/ab/x/a.png) and [b.pdf](https://chat.example/user_uploads/2/cd/y/b.pdf)"
                   " not [c.png](https://evil.example/user_uploads/2/c.png)")
        links = find_uploads(content, "https://chat.example")
        assert [(link.name, link.path) for link in links] == [
            ("a.png", "/user_uploads/2/ab/x/a.png"), ("b.pdf", "/user_uploads/2/cd/y/b.pdf")]

    def test_message_type(self):
        assert message_type_for(["image/png", "image/jpeg"]) == MessageType.PHOTO
        assert message_type_for(["image/png", "application/pdf"]) == MessageType.DOCUMENT
        assert message_type_for(["audio/ogg"]) == MessageType.VOICE
