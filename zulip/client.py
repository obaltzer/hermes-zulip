"""Thin async Zulip REST client (aiohttp, HTTP Basic ``email:api_key``) — only what the adapter needs.

Zulip takes form-encoded parameters with non-string values JSON-encoded; every response carries
``result`` / ``msg`` and errors a machine-readable ``code``. A 429 is retried once after the
server's ``retry-after``; everything else surfaces as :class:`ZulipError`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 30.0
# /events long-polls; the server answers with a heartbeat well inside this read window.
_EVENTS_READ_TIMEOUT_S = 120.0
_UPLOAD_TIMEOUT_S = 300.0
_MAX_RATE_LIMIT_WAIT_S = 60.0

CLIENT_CAPABILITIES = {
    "notification_settings_null": True,
    "bulk_message_deletion": True,
    "user_avatar_url_field_optional": True,
    "user_settings_object": True,
    "empty_topic_name": True,
}


class ZulipError(Exception):
    def __init__(self, status: int, code: str, msg: str, payload: Optional[dict] = None):
        super().__init__(f"Zulip API error {status} {code}: {msg}")
        self.status, self.code, self.msg = status, code, msg
        self.payload = payload or {}


class AttachmentTooLarge(Exception):
    pass


def transport_errors() -> tuple[type[BaseException], ...]:
    """Everything a Zulip call can raise for a server/network problem (aiohttp imported lazily)."""
    import aiohttp
    return ZulipError, aiohttp.ClientError, TimeoutError, OSError


def _encode(params: Optional[dict[str, Any]]) -> dict[str, str]:
    encoded: dict[str, str] = {}
    for key, value in (params or {}).items():
        if value is None:
            continue
        encoded[key] = value if isinstance(value, str) else json.dumps(value)
    return encoded


class ZulipClient:
    """One bot identity on one Zulip server. ``session`` is owned by the caller."""

    def __init__(self, site: str, email: str, api_key: str, session: Any):
        import aiohttp
        self.site = site.rstrip("/")
        self.email = email
        self._auth = {"Authorization": aiohttp.encode_basic_auth(email, api_key)}
        self._session = session

    def absolute_url(self, url: str) -> str:
        return urljoin(self.site + "/", url)

    async def call(self, method: str, path: str, params: Optional[dict[str, Any]] = None, *,
                   timeout_s: float = _DEFAULT_TIMEOUT_S, retry_rate_limit: bool = True) -> dict:
        import aiohttp
        url = f"{self.site}/api/v1/{path.lstrip('/')}"
        data = _encode(params)
        kwargs: dict[str, Any] = {"headers": self._auth,
                                  "timeout": aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=timeout_s)}
        kwargs["params" if method in {"GET", "DELETE"} else "data"] = data
        async with self._session.request(method, url, **kwargs) as resp:
            try:
                payload = await resp.json(content_type=None)
            except (json.JSONDecodeError, aiohttp.ContentTypeError):
                payload = {"result": "error", "code": "BAD_RESPONSE", "msg": (await resp.text())[:200]}
        if not isinstance(payload, dict):
            payload = {"result": "error", "code": "BAD_RESPONSE", "msg": str(payload)[:200]}
        if resp.status == 429 and retry_rate_limit:
            wait = min(float(payload.get("retry-after") or resp.headers.get("Retry-After") or 1.0),
                       _MAX_RATE_LIMIT_WAIT_S)
            logger.warning("Zulip: rate limited on %s %s, retrying in %.1fs", method, path, wait)
            await asyncio.sleep(wait)
            return await self.call(method, path, params, timeout_s=timeout_s, retry_rate_limit=False)
        if resp.status >= 400 or payload.get("result") != "success":
            raise ZulipError(resp.status, str(payload.get("code") or "BAD_REQUEST"),
                             str(payload.get("msg") or ""), payload)
        return payload

    # --- identity / state ---

    async def get_me(self) -> dict:
        return await self.call("GET", "users/me")

    async def register(self, event_types: list[str], fetch_event_types: list[str]) -> dict:
        return await self.call("POST", "register", {
            "event_types": event_types, "fetch_event_types": fetch_event_types,
            "apply_markdown": False, "client_gravatar": True,
            "client_capabilities": CLIENT_CAPABILITIES})

    async def get_events(self, queue_id: str, last_event_id: int) -> list[dict]:
        payload = await self.call("GET", "events", {"queue_id": queue_id, "last_event_id": last_event_id},
                                  timeout_s=_EVENTS_READ_TIMEOUT_S)
        return payload.get("events") or []

    async def delete_queue(self, queue_id: str) -> None:
        await self.call("DELETE", "events", {"queue_id": queue_id})

    async def subscribe(self, channel_names: list[str]) -> None:
        await self.call("POST", "users/me/subscriptions",
                        {"subscriptions": [{"name": name} for name in channel_names]})

    async def get_stream_id(self, name: str) -> int:
        return int((await self.call("GET", "get_stream_id", {"stream": name}))["stream_id"])

    async def get_messages(self, narrow: list[dict], *, anchor: Any = "newest", num_before: int = 0,
                           num_after: int = 0, include_anchor: bool = True) -> list[dict]:
        payload = await self.call("GET", "messages", {
            "narrow": narrow, "anchor": anchor if isinstance(anchor, str) else str(anchor),
            "num_before": num_before, "num_after": num_after, "include_anchor": include_anchor,
            "apply_markdown": False, "allow_empty_topic_name": True})
        return payload.get("messages") or []

    # --- sending ---

    async def send_channel(self, stream_id: int, topic: str, content: str) -> int:
        payload = await self.call("POST", "messages", {
            "type": "channel", "to": str(stream_id), "topic": topic, "content": content})
        return int(payload["id"])

    async def send_direct(self, user_ids: list[int], content: str) -> int:
        payload = await self.call("POST", "messages", {"type": "direct", "to": user_ids, "content": content})
        return int(payload["id"])

    async def edit_content(self, message_id: int, content: str) -> None:
        await self.call("PATCH", f"messages/{message_id}", {"content": content})

    async def typing_channel(self, stream_id: int, topic: str, op: str = "start") -> None:
        await self.call("POST", "typing", {"type": "channel", "op": op, "stream_id": stream_id, "topic": topic})

    async def typing_direct(self, user_ids: list[int], op: str = "start") -> None:
        await self.call("POST", "typing", {"type": "direct", "op": op, "to": user_ids})

    async def add_reaction(self, message_id: int, emoji_name: str) -> None:
        try:
            await self.call("POST", f"messages/{message_id}/reactions", {"emoji_name": emoji_name})
        except ZulipError as exc:
            if exc.code != "REACTION_ALREADY_EXISTS":
                raise

    async def remove_reaction(self, message_id: int, emoji_name: str) -> None:
        try:
            await self.call("DELETE", f"messages/{message_id}/reactions", {"emoji_name": emoji_name})
        except ZulipError as exc:
            if exc.code != "REACTION_DOES_NOT_EXIST":
                raise

    # --- files ---

    async def upload(self, data: bytes, filename: str, content_type: str) -> str:
        """Upload one file; returns its ``/user_uploads/...`` URL."""
        import aiohttp
        form = aiohttp.FormData()
        form.add_field("filename", data, filename=filename, content_type=content_type)
        async with self._session.post(
                f"{self.site}/api/v1/user_uploads", data=form, headers=self._auth,
                timeout=aiohttp.ClientTimeout(total=_UPLOAD_TIMEOUT_S)) as resp:
            payload = await resp.json(content_type=None)
        if resp.status >= 400 or payload.get("result") != "success":
            raise ZulipError(resp.status, str(payload.get("code") or "BAD_REQUEST"), str(payload.get("msg") or ""),
                             payload)
        return str(payload.get("url") or payload.get("uri"))

    async def download(self, upload_path: str, max_bytes: int) -> tuple[bytes, str]:
        """Fetch an uploaded file: the documented temporary-URL route, falling back to an
        authenticated GET of the upload path. Returns (data, content type)."""
        rel = upload_path.split("/user_uploads/", 1)[-1]
        try:
            temp = await self.call("GET", f"user_uploads/{rel}")
            return await self._fetch(self.absolute_url(temp["url"]), max_bytes, auth=False)
        except ZulipError as exc:
            logger.debug("Zulip: temporary upload URL unavailable (%s), using direct download", exc.code)
        return await self._fetch(self.absolute_url(f"/user_uploads/{rel}"), max_bytes, auth=True)

    async def _fetch(self, url: str, max_bytes: int, *, auth: bool, hops: int = 0) -> tuple[bytes, str]:
        """GET a file. Redirects are followed by hand so credentials only ever reach this server
        (uploads may live behind a signed S3 URL)."""
        import aiohttp
        send_auth = auth and urlparse(url).netloc == urlparse(self.site).netloc
        async with self._session.get(url, headers=self._auth if send_auth else None, allow_redirects=False,
                                     timeout=aiohttp.ClientTimeout(total=_UPLOAD_TIMEOUT_S)) as resp:
            location = resp.headers.get("Location")
            if 300 <= resp.status < 400 and location and hops < 5:
                return await self._fetch(urljoin(url, location), max_bytes, auth=auth, hops=hops + 1)
            if resp.status >= 400 or 300 <= resp.status < 400:
                raise ZulipError(resp.status, "DOWNLOAD_FAILED", f"HTTP {resp.status}")
            if resp.content_length and resp.content_length > max_bytes:
                raise AttachmentTooLarge(resp.content_length)
            chunks, total = [], 0
            async for chunk in resp.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    raise AttachmentTooLarge(total)
                chunks.append(chunk)
            return b"".join(chunks), resp.content_type or "application/octet-stream"
