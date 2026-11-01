"""Inbound side of the Zulip adapter: message events → Hermes ``MessageEvent``s, topic lifecycle.

Mixed into ``ZulipAdapter``; relies on its client, registry, user/channel caches and settings.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from gateway.platforms.event import MessageEvent, MessageType

from plugins.platforms.zulip.client import AttachmentTooLarge, ZulipError
from plugins.platforms.zulip.engagement import ZulipUser, decide, mentioned_user_ids, strip_mentions_of
from plugins.platforms.zulip.media import cache_download, effective_mime, find_uploads, message_type_for, replace_link
from plugins.platforms.zulip.targets import dm_chat_id
from plugins.platforms.zulip.topics import Topic

logger = logging.getLogger(__name__)

_HISTORY_LINE_CHARS = 2000
# After a restart or an expired event queue, replay at most this much missed history.
_CATCH_UP_WINDOW_S = 15 * 60
_CATCH_UP_MAX_MESSAGES = 200


def _media_filenames_kw(names: list[str]) -> dict:
    """``media_filenames`` only where core has it (Hermes > 0.22.5); older cores show cache names."""
    if "media_filenames" in getattr(MessageEvent, "__dataclass_fields__", {}):
        return {"media_filenames": names}
    return {}


def _topic_narrow(stream_id: int, topic: str) -> list[dict]:
    return [{"operator": "channel", "operand": stream_id}, {"operator": "topic", "operand": topic}]


class ZulipInboundMixin:
    # --- event dispatch ---

    async def _handle_event(self, event: dict) -> None:
        handler = {
            "message": lambda e: self._on_message(e.get("message") or {}, e.get("flags") or []),
            "update_message": self._on_update_message,
            "realm_user": self._on_realm_user,
            "subscription": self._on_subscription,
        }.get(event.get("type"))
        if handler is None:
            return
        try:
            await handler(event)
        except Exception:
            logger.exception("Zulip: failed to handle %s event", event.get("type"))

    async def _on_realm_user(self, event: dict) -> None:
        person = event.get("person") or {}
        uid = person.get("user_id")
        if uid is None or event.get("op") not in {"add", "update"}:
            return
        current = self._users.get(int(uid))
        self._remember_user(ZulipUser(
            user_id=int(uid), full_name=person.get("full_name") or (current.full_name if current else ""),
            email=person.get("email") or (current.email if current else ""),
            is_bot=bool(person.get("is_bot", current.is_bot if current else False))))

    async def _on_subscription(self, event: dict) -> None:
        if event.get("op") == "add":
            for sub in event.get("subscriptions") or []:
                self._remember_stream(int(sub["stream_id"]), sub.get("name") or "")

    # --- messages ---

    async def _on_message(self, msg: dict, flags: list[str]) -> None:
        if not msg.get("id") or self._dedup.is_duplicate(str(msg["id"])):
            return
        self._remember_cursor(int(msg["id"]))
        if int(msg.get("sender_id") or 0) == self._user_id:
            return
        if msg.get("type") == "stream":
            await self._on_channel_message(msg, flags)
        else:
            await self._on_direct_message(msg, flags)

    def _sender(self, msg: dict) -> ZulipUser:
        uid = int(msg["sender_id"])
        known = self._users.get(uid)
        if known is not None:
            return known
        return ZulipUser(uid, msg.get("sender_full_name") or str(uid), msg.get("sender_email") or "")

    def _mentions(self, content: str, flags: list[str]) -> tuple[bool, list[int]]:
        """(this bot mentioned, other bots mentioned in order)."""
        ids = mentioned_user_ids(content, self._users_by_name)
        self_mentioned = self._user_id in ids
        others = [u for u in ids if u != self._user_id and getattr(self._users.get(u), "is_bot", False)]
        return self_mentioned, others

    async def _on_channel_message(self, msg: dict, flags: list[str]) -> None:
        stream_id, stream_name = int(msg["stream_id"]), str(msg.get("display_recipient") or "")
        self._remember_stream(stream_id, stream_name)
        if not self._channel_allowed(stream_id, stream_name):
            return
        sender = self._sender(msg)
        self_mentioned, other_bots = self._mentions(msg.get("content") or "", flags)
        free = self._is_free_response(stream_id, stream_name)
        topic = self._registry.find(stream_id, msg.get("subject") or "")
        if topic is None and not (self_mentioned or other_bots or (free and not sender.is_bot)):
            return
        if topic is None:
            topic = self._registry.create(stream_id, msg.get("subject") or "")
        decision = decide(self_id=self._user_id, sender_is_bot=sender.is_bot, self_mentioned=self_mentioned,
                          other_bots_mentioned=other_bots, last_addressed=topic.last_addressed_bot,
                          bot_hops=topic.bot_hops, hop_limit=self._settings.bot_hop_limit,
                          free_response=free, is_dm=False)
        if (decision.last_addressed, decision.bot_hops) != (topic.last_addressed_bot, topic.bot_hops):
            self._registry.update(topic.topic_id, last_addressed_bot=decision.last_addressed,
                                  bot_hops=decision.bot_hops)
        if not decision.respond:
            return
        self._redirects.pop(topic.topic_id, None)
        await self._dispatch(msg, sender, chat_id=topic.chat_id, chat_type="channel",
                             chat_name=f"#{stream_name} > {topic.name}", topic=topic)

    async def _on_direct_message(self, msg: dict, flags: list[str]) -> None:
        recipients = [int(r["id"]) for r in msg.get("display_recipient") or [] if isinstance(r, dict)]
        chat_id = dm_chat_id(recipients, self._user_id)
        sender = self._sender(msg)
        self_mentioned, other_bots = self._mentions(msg.get("content") or "", flags)
        is_group = "-" in chat_id
        if is_group and other_bots and not self_mentioned:
            return  # a group DM addressed to another bot
        decision = decide(self_id=self._user_id, sender_is_bot=sender.is_bot, self_mentioned=self_mentioned,
                          other_bots_mentioned=other_bots, last_addressed=None,
                          bot_hops=self._dm_hops.get(chat_id, 0), hop_limit=self._settings.bot_hop_limit,
                          free_response=True, is_dm=True)
        self._dm_hops[chat_id] = decision.bot_hops
        if not decision.respond:
            return
        self._redirects.pop(chat_id, None)
        names = [self._users[u].full_name for u in recipients if u != self._user_id and u in self._users]
        await self._dispatch(msg, sender, chat_id=chat_id, chat_type="group" if is_group else "dm",
                             chat_name=", ".join(names) or sender.full_name, topic=None)

    async def _dispatch(self, msg: dict, sender: ZulipUser, *, chat_id: str, chat_type: str, chat_name: str,
                        topic: Optional[Topic]) -> None:
        mid = int(msg["id"])
        text = strip_mentions_of(msg.get("content") or "", self._me)
        text, media_urls, media_types, media_names = await self._collect_attachments(text)
        context = await self._context_for(topic, mid) if topic is not None else None
        if topic is not None:
            self._registry.update(topic.topic_id, engaged=True, last_handled_id=max(mid, topic.last_handled_id))
        source = self.build_source(
            chat_id=chat_id, chat_name=chat_name, chat_type=chat_type, user_id=str(sender.user_id),
            user_name=sender.full_name, thread_id=topic.topic_id if topic else None,
            chat_topic=topic.name if topic else None, user_id_alt=sender.email or None,
            is_bot=sender.is_bot, message_id=str(mid))
        is_command = text.lstrip().startswith("/")
        if is_command:
            message_type = MessageType.COMMAND
        elif media_types:
            message_type = message_type_for(media_types)
        else:
            message_type = MessageType.TEXT
        event = MessageEvent(
            text=text.lstrip() if is_command else text, message_type=message_type, source=source,
            raw_message=msg, message_id=str(mid), media_urls=media_urls, media_types=media_types,
            channel_context=context, **_media_filenames_kw(media_names))
        if is_command or self._text_batch_delay_seconds <= 0:
            await self.handle_message(event)
        else:
            self._enqueue_text_event(event)

    # --- attachments ---

    async def _collect_attachments(self, text: str) -> tuple[str, list[str], list[str], list[str]]:
        """Download every upload linked in ``text`` (bounded); links become ``📎 name`` markers."""
        urls: list[str] = []
        types: list[str] = []
        names: list[str] = []
        notes: list[str] = []
        max_bytes = self._settings.max_attachment_mb * 1024 * 1024
        for link in find_uploads(text, self._site):
            if len(urls) >= self._settings.max_attachments:
                notes.append(f"{link.name}: not downloaded (more than {self._settings.max_attachments} attachments)")
                text = replace_link(text, link, f"📎 {link.name}")
                continue
            try:
                data, mime = await self._client.download(link.path, max_bytes)
                mime = effective_mime(mime, link.name)
                urls.append(await cache_download(data, link.name, mime))
            except AttachmentTooLarge:
                notes.append(f"{link.name}: not downloaded (larger than {self._settings.max_attachment_mb} MB)")
            except Exception as exc:
                logger.warning("Zulip: could not fetch attachment %s: %s", link.name, exc, exc_info=True)
                notes.append(f"{link.name}: download failed")
            else:
                types.append(mime)
                names.append(link.name)
            text = replace_link(text, link, f"📎 {link.name}")
        if notes:
            text = f"{text}\n\n[Attachment notice: {'; '.join(notes)}]".strip()
        return text, urls, types, names

    # --- conversation context ---

    async def _context_for(self, topic: Topic, message_id: int) -> Optional[str]:
        """One-shot context for this turn: a spin-off/split seed, then topic messages the session has
        not seen yet (earlier history on first engagement, unaddressed chatter since the last turn)."""
        parts: list[str] = []
        seed = self._registry.pop_seed(topic.topic_id)
        if seed:
            parts.append(seed)
        if self._settings.history_backfill > 0:
            try:
                history = await self._client.get_messages(
                    _topic_narrow(topic.stream_id, topic.name), anchor=message_id,
                    num_before=self._settings.history_backfill, include_anchor=False)
            except (ZulipError, OSError) as exc:
                logger.warning("Zulip: topic history unavailable: %s", exc)
                history = []
            unseen = [m for m in history
                      if int(m["id"]) > topic.last_handled_id and int(m.get("sender_id") or 0) != self._user_id]
            if unseen:
                what = ("Messages posted in this topic since your last reply" if topic.engaged
                        else "Earlier messages in this topic")
                parts.append(self._format_history(f"[Zulip context: {what} {self._topic_link(topic)}]", unseen))
        return "\n\n".join(parts) or None

    def _topic_link(self, topic: Topic) -> str:
        return f"#**{self._streams.get(topic.stream_id, topic.stream_id)}>{topic.name}**"

    def _format_history(self, header: str, messages: list[dict]) -> str:
        lines = [header]
        for m in messages:
            content = m.get("content") or ""
            for link in find_uploads(content, self._site):
                content = replace_link(content, link, f"📎 {link.name}")
            if len(content) > _HISTORY_LINE_CHARS:
                content = content[:_HISTORY_LINE_CHARS] + " …"
            lines.append(f"{m.get('sender_full_name') or m.get('sender_id')}: {content}")
        return "\n".join(lines)

    # --- topic lifecycle ---

    async def _on_update_message(self, event: dict) -> None:
        if event.get("rendering_only") or event.get("orig_subject") is None and "new_stream_id" not in event:
            return
        stream_id = event.get("stream_id")
        if stream_id is None:
            return
        old_name = event.get("orig_subject") or ""
        new_name = event.get("subject", old_name)
        new_stream = int(event.get("new_stream_id") or stream_id)
        if (new_stream, new_name) == (int(stream_id), old_name):
            return
        whole_topic = event.get("propagate_mode") == "change_all" or not await self._topic_has_messages(
            int(stream_id), old_name)
        if whole_topic:
            moved = self._registry.move(int(stream_id), old_name, new_stream, new_name)
            if moved is not None:
                logger.info("Zulip: topic %s now #%s>%s", moved.topic_id, new_stream, new_name)
            return
        await self._register_split(int(stream_id), old_name, new_stream, new_name, event.get("message_ids") or [])

    async def _topic_has_messages(self, stream_id: int, name: str) -> bool:
        try:
            return bool(await self._client.get_messages(_topic_narrow(stream_id, name), num_before=1))
        except (ZulipError, OSError):
            return True  # unknown: treat the move as partial, never re-point a live session

    async def _register_split(self, stream_id: int, old_name: str, new_stream: int, new_name: str,
                              message_ids: list[int]) -> None:
        source = self._registry.find(stream_id, old_name)
        if source is None or self._registry.find(new_stream, new_name) is not None:
            return
        try:
            moved = await self._client.get_messages(_topic_narrow(new_stream, new_name),
                                                    num_before=max(self._settings.history_backfill, 1))
        except (ZulipError, OSError):
            moved = []
        moved = [m for m in moved if not message_ids or int(m["id"]) in set(message_ids)]
        dest_topic = self._registry.create(new_stream, new_name, created_by="split",
                                           parent_topic_id=source.topic_id,
                                           last_addressed_bot=source.last_addressed_bot)
        origin = f"#**{self._streams.get(stream_id, stream_id)}>{old_name}**"
        seed = self._format_history(
            f"[Zulip context: this topic was split off from {origin} and is a separate conversation. "
            "Messages moved here:]", moved)
        self._registry.update(dest_topic.topic_id, seed=seed,
                              last_handled_id=max([int(m["id"]) for m in moved] + [max(message_ids or [0])]))

    # --- restart / queue-expiry catch-up ---

    def _remember_cursor(self, message_id: int) -> None:
        if message_id > self._last_message_id:
            self._last_message_id = message_id
            self._registry.set_cursor("last_message_id", str(message_id))

    async def _catch_up(self) -> None:
        """Replay messages that arrived while no event queue was listening (bounded window)."""
        if not self._last_message_id:
            return
        try:
            messages = await self._client.get_messages([], anchor=self._last_message_id,
                                                       num_after=_CATCH_UP_MAX_MESSAGES, include_anchor=False)
        except (ZulipError, OSError) as exc:
            logger.warning("Zulip: catch-up after reconnect failed: %s", exc)
            return
        cutoff = time.time() - _CATCH_UP_WINDOW_S
        replay = [m for m in messages if float(m.get("timestamp") or 0) >= cutoff]
        if replay:
            logger.info("Zulip: replaying %d message(s) missed while disconnected", len(replay))
        for message in replay:
            await self._on_message(message, message.get("flags") or [])

    # --- caches (filled at register time, kept current by events) ---

    def _remember_user(self, user: ZulipUser) -> None:
        previous = self._users.get(user.user_id)
        if previous is not None and previous.full_name.casefold() != user.full_name.casefold():
            ids = self._users_by_name.get(previous.full_name.casefold(), [])
            if user.user_id in ids:
                ids.remove(user.user_id)
        self._users[user.user_id] = user
        ids = self._users_by_name.setdefault(user.full_name.casefold(), [])
        if user.user_id not in ids:
            ids.append(user.user_id)

    def _remember_stream(self, stream_id: int, name: str) -> None:
        if name:
            self._streams[stream_id] = name
            self._stream_ids[name.casefold()] = stream_id

    def _channel_allowed(self, stream_id: int, name: str) -> bool:
        allowed = self._settings.channels
        return not allowed or name.casefold() in allowed or str(stream_id) in allowed

    def _is_free_response(self, stream_id: int, name: str) -> bool:
        free = self._settings.free_response_channels
        return name.casefold() in free or str(stream_id) in free

    def _text_batch_key(self, event: MessageEvent) -> str:
        """Batch per sender: two people typing in one topic are two turns, not one merged message."""
        return f"{self._event_session_key(event)}:{event.source.user_id or ''}"

    def _users_from_register(self, payload: dict[str, Any]) -> None:
        for person in (payload.get("realm_users") or []) + (payload.get("cross_realm_bots") or []):
            if person.get("user_id") is not None:
                self._remember_user(ZulipUser(int(person["user_id"]), person.get("full_name") or "",
                                              person.get("email") or "", bool(person.get("is_bot"))))
        for sub in payload.get("subscriptions") or []:
            self._remember_stream(int(sub["stream_id"]), sub.get("name") or "")
