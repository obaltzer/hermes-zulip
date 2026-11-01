"""Outbound side of the Zulip adapter: text, the ``TOPIC:`` spin-off directive, files, typing, acks.

Mixed into ``ZulipAdapter``; relies on its client, registry, caches and settings.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote

from gateway.platforms.base import SendResult
from gateway.platforms.event import MessageEvent, ProcessingOutcome

from plugins.platforms.zulip.client import ZulipError, transport_errors
from plugins.platforms.zulip.media import upload_markdown
from plugins.platforms.zulip.targets import (
    Destination, clamp_topic, parse_topic_directive, resolve_destination)

logger = logging.getLogger(__name__)

_Metadata = Optional[dict[str, Any]]

# Files sent to one conversation within this window are appended (by edit) to the same message,
# so a turn that produces several files posts one message instead of one per file.
_FILE_MERGE_WINDOW_S = 60.0
_WILDCARD_MENTION_RE = re.compile(r"@(_?)\*\*(all|everyone|stream|channel|topic)\*\*", re.IGNORECASE)
_MARKDOWN_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")


@dataclass
class _FileMessage:
    message_id: int
    content: str
    sent_at: float


def _error_result(exc: Exception) -> SendResult:
    status = getattr(exc, "status", 0)
    retryable = not isinstance(exc, ZulipError) or status >= 500 or status == 429
    return SendResult(success=False, error=str(exc), retryable=retryable)


class ZulipOutboundMixin:
    def format_message(self, content: str) -> str:
        """Zulip Markdown is close to CommonMark; images are plain links (Zulip previews them) and
        wildcard mentions are defused so an echoed ``@**all**`` never pings a whole channel."""
        content = _MARKDOWN_IMAGE_RE.sub(r"[\1](\2)", content or "")
        return _WILDCARD_MENTION_RE.sub(lambda m: f"@{m.group(1)}\u200b**{m.group(2)}**", content)

    async def _destination(self, chat_id: str, metadata: _Metadata) -> Destination:
        thread_id = (metadata or {}).get("thread_id")
        redirect = self._redirects.get(str(thread_id or chat_id))
        if redirect:
            thread_id = redirect
        return await resolve_destination(chat_id, thread_id, registry=self._registry,
                                         stream_id_for=self._stream_id_for, default_topic=self._settings.default_topic)

    async def _stream_id_for(self, name: str) -> int:
        cached = self._stream_ids.get(name.casefold())
        if cached is not None:
            return cached
        stream_id = await self._client.get_stream_id(name)
        self._remember_stream(stream_id, name)
        return stream_id

    async def _post(self, dest: Destination, content: str) -> int:
        if dest.kind == "direct":
            return await self._client.send_direct(list(dest.user_ids), content)
        return await self._client.send_channel(dest.stream_id, dest.topic, content)

    async def _post_text(self, dest: Destination, content: str) -> int:
        message_id = 0
        for chunk in self.truncate_message(self.format_message(content), self._max_message_length):
            message_id = await self._post(dest, chunk)
        return message_id

    # --- text ---

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: _Metadata = None) -> SendResult:
        if not content or not content.strip():
            return SendResult(success=True)
        try:
            dest = await self._destination(chat_id, metadata)
            directive = parse_topic_directive(content)
            if directive is not None:
                return await self._open_topic(dest, chat_id, metadata, *directive)
            message_id = await self._post_text(dest, content)
            self._register_posted_topic(dest, content, message_id)
            self._last_files.pop(dest.key, None)
            return SendResult(success=True, message_id=str(message_id))
        except Exception as exc:
            logger.warning("Zulip: send to %s failed: %s", chat_id, exc, exc_info=True)
            return _error_result(exc)

    async def edit_message(self, chat_id: str, message_id: str, content: str, *,
                           finalize: bool = False) -> SendResult:
        try:
            text = self.truncate_message(self.format_message(content), self._max_message_length)[0]
            await self._client.edit_content(int(message_id), text)
            return SendResult(success=True, message_id=str(message_id))
        except Exception as exc:
            logger.warning("Zulip: edit of %s failed: %s", message_id, exc, exc_info=True)
            return _error_result(exc)

    def _register_posted_topic(self, dest: Destination, content: str, message_id: int) -> None:
        """A host-driven post (cron, ``hermes send``) into a topic this bot never saw: register it so
        a reply there continues with the posted message as context."""
        if dest.kind != "channel" or dest.topic_id is not None or self._registry is None:
            return
        topic, created = self._registry.get_or_create(
            dest.stream_id, dest.topic, created_by="hermes", last_addressed_bot=self._user_id,
            seed=f"[Zulip context: you posted this message in this topic earlier:]\n{content}")
        if created:
            self._registry.update(topic.topic_id, last_handled_id=message_id)

    # --- TOPIC: directive ---

    async def _open_topic(self, origin: Destination, chat_id: str, metadata: _Metadata,
                          name: str, body: str) -> SendResult:
        """Post ``body`` as the opening message of topic ``name`` (same channel; a DM uses the
        configured spin-off channel), seed that topic's session, and link it from the origin."""
        if origin.kind == "direct":
            if not self._settings.dm_spinoff_channel:
                note = "_(Topics can't be opened from a direct message; replying here instead.)_"
                message_id = await self._post_text(origin, f"{body or name}\n\n{note}")
                return SendResult(success=True, message_id=str(message_id))
            stream_id = await self._stream_id_for(self._settings.dm_spinoff_channel)
            origin_label = "a direct message"
        else:
            stream_id = origin.stream_id
            origin_label = f"#**{self._streams.get(origin.stream_id, origin.stream_id)}>{origin.topic}**"
        name = clamp_topic(name, self._max_topic_length)
        target = Destination("channel", stream_id, name)
        message_id = await self._post_text(target, body or name)
        seed = (f"[Zulip context: you opened this topic from {origin_label}. "
                f"Your opening message here was:]\n{body or name}")
        topic, created = self._registry.get_or_create(
            stream_id, name, created_by="hermes", parent_topic_id=origin.topic_id, seed=seed,
            last_addressed_bot=self._user_id)
        updates: dict[str, Any] = {"last_addressed_bot": self._user_id}
        if created:
            updates["last_handled_id"] = message_id
        else:
            updates["seed"] = "\n\n".join(filter(None, [topic.seed, seed]))
        self._registry.update(topic.topic_id, **updates)
        link = f"↪ Continued in #**{self._streams.get(stream_id, stream_id)}>{topic.name}**"
        try:
            await self._post(origin, link)
        except ZulipError as exc:
            logger.warning("Zulip: could not post spin-off link: %s", exc)
        self._redirects[str((metadata or {}).get("thread_id") or chat_id)] = topic.topic_id
        return SendResult(success=True, message_id=str(message_id))

    # --- files ---

    async def _send_files(self, chat_id: str, files: list[tuple[bytes, str]], caption: Optional[str],
                          metadata: _Metadata) -> SendResult:
        """Upload ``(data, filename)`` pairs and post them as one message (or append them to this
        conversation's latest file message)."""
        try:
            dest = await self._destination(chat_id, metadata)
            links, notes = [], []
            for data, name in files:
                if len(data) > self._max_upload_bytes:
                    notes.append(f"_{name} is too large to upload ({len(data) // (1024 * 1024)} MB)._")
                    continue
                content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
                url = await self._client.upload(data, name, content_type)
                links.append(upload_markdown(name, url))
            if not links and not notes:
                return SendResult(success=False, error="no files to send")
            if links and not caption and (merged := await self._append_to_file_message(dest, links)):
                return merged
            content = "\n".join(filter(None, [self.format_message(caption or ""), *links, *notes]))
            message_id = await self._post(dest, content)
            if links:
                self._last_files[dest.key] = _FileMessage(message_id, content, time.monotonic())
            return SendResult(success=bool(links), message_id=str(message_id),
                              error=None if links else "file too large")
        except Exception as exc:
            logger.warning("Zulip: file send to %s failed: %s", chat_id, exc, exc_info=True)
            return _error_result(exc)

    async def _append_to_file_message(self, dest: Destination, links: list[str]) -> Optional[SendResult]:
        previous = self._last_files.get(dest.key)
        if previous is None or time.monotonic() - previous.sent_at > _FILE_MERGE_WINDOW_S:
            return None
        content = "\n".join([previous.content, *links])
        if len(content) > self._max_message_length:
            return None
        try:
            await self._client.edit_content(previous.message_id, content)
        except ZulipError as exc:
            logger.debug("Zulip: could not extend file message (%s), posting a new one", exc.code)
            return None
        self._last_files[dest.key] = _FileMessage(previous.message_id, content, time.monotonic())
        return SendResult(success=True, message_id=str(previous.message_id))

    async def _send_local_file(self, chat_id: str, path: str, caption: Optional[str], metadata: _Metadata,
                               file_name: Optional[str] = None) -> SendResult:
        p = Path(path)
        if not await asyncio.to_thread(p.is_file):
            return SendResult(success=False, error=f"file not found: {path}")
        return await self._send_files(chat_id, [(await asyncio.to_thread(p.read_bytes), file_name or p.name)],
                                      caption, metadata)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, metadata)

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            file_name: Optional[str] = None, reply_to: Optional[str] = None,
                            metadata: _Metadata = None, **kwargs) -> SendResult:
        return await self._send_local_file(chat_id, file_path, caption, metadata, file_name)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, metadata)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption, metadata)

    async def send_animation(self, chat_id: str, animation_url: str, caption: Optional[str] = None,
                             reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs) -> SendResult:
        return await self.send_image(chat_id, animation_url, caption, reply_to, metadata)

    async def _load_image(self, url: str, index: int) -> Optional[tuple[bytes, str]]:
        """``file://`` path or remote URL (SSRF-checked) → (data, filename); None when unavailable."""
        import aiohttp
        if url.startswith("file://"):
            p = Path(unquote(url[7:]))
            if not await asyncio.to_thread(p.is_file):
                return None
            return await asyncio.to_thread(p.read_bytes), p.name
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("Zulip: blocked unsafe image URL")
            return None
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.read()
                ext = mimetypes.guess_extension(resp.content_type or "") or ".png"
        except (TimeoutError, aiohttp.ClientError) as exc:
            logger.warning("Zulip: image download failed: %s", exc)
            return None
        name = url.rsplit("/", 1)[-1].split("?")[0] or f"image_{index}{ext}"
        return data, name if "." in name else name + ext

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs) -> SendResult:
        loaded = await self._load_image(image_url, 0)
        if loaded is None:
            return await self.send(chat_id, f"{caption or ''}\n{image_url}".strip(), metadata=metadata)
        return await self._send_files(chat_id, [loaded], caption, metadata)

    async def send_multiple_images(self, chat_id: str, images: list[tuple[str, str]],
                                   metadata: _Metadata = None, human_delay: float = 0.0) -> SendResult:
        """All images in one Zulip message (Zulip previews each link)."""
        loaded = [item for i, (url, _alt) in enumerate(images) if (item := await self._load_image(url, i))]
        if not loaded:
            return SendResult(success=False, error="no images could be loaded")
        caption = "\n".join(alt for _url, alt in images if alt) or None
        return await self._send_files(chat_id, loaded, caption, metadata)

    # --- typing / acks ---

    async def send_typing(self, chat_id: str, metadata: _Metadata = None) -> None:
        try:
            dest = await self._destination(chat_id, metadata)
            if dest.kind == "direct":
                await self._client.typing_direct(list(dest.user_ids))
            else:
                await self._client.typing_channel(dest.stream_id, dest.topic)
        except transport_errors() as exc:
            logger.debug("Zulip: typing indicator failed: %s", exc)

    async def _react(self, event: MessageEvent, add: Optional[str], remove: Optional[str]) -> None:
        if not self._settings.ack_reactions or not str(event.message_id or "").isdigit():
            return
        try:
            if remove:
                await self._client.remove_reaction(int(event.message_id), remove)
            if add:
                await self._client.add_reaction(int(event.message_id), add)
        except transport_errors() as exc:
            logger.debug("Zulip: reaction update failed: %s", exc)

    async def on_processing_start(self, event: MessageEvent) -> None:
        await self._react(event, "eyes", None)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        source = event.source
        if source is not None:
            self._redirects.pop(str(source.thread_id or source.chat_id), None)
        done = {ProcessingOutcome.SUCCESS: "check", ProcessingOutcome.FAILURE: "warning"}.get(outcome)
        await self._react(event, done, "eyes")
