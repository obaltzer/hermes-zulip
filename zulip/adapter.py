"""Zulip gateway adapter — REST API + event-queue long polling via aiohttp (no Zulip SDK).

Every Zulip topic is its own Hermes session: ``thread_id`` is a stable topic id from a per-profile
registry (``topics.py``) that follows renames, resolves, moves and splits. Each Hermes profile runs
its own Zulip bot user; several bots can share a channel (``engagement.py`` decides who answers).
Hermes opens a new topic by starting its reply with ``TOPIC: <name>`` (``outbound.py``).

Secrets (``.env``, per profile):
    ZULIP_SITE, ZULIP_EMAIL, ZULIP_API_KEY       bot credentials (from the bot's zuliprc)
    ZULIP_ALLOWED_USERS / ZULIP_ALLOW_ALL_USERS  who may talk to the bot (ids or emails)
    ZULIP_HOME_CHANNEL                           "channel>topic" for cron / notifications
Behaviour (``config.yaml`` → ``zulip:``): see ``ZulipSettings``.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import env_is_connected, extra_or_secret, send_error
from gateway.platforms.base import BasePlatformAdapter, gateway_trust_env
from gateway.platforms.helpers import MessageDeduplicator, cancel_task

from plugins.platforms.zulip.client import ZulipClient, ZulipError
from plugins.platforms.zulip.engagement import ZulipUser
from plugins.platforms.zulip.inbound import ZulipInboundMixin
from plugins.platforms.zulip.media import upload_markdown
from plugins.platforms.zulip.outbound import ZulipOutboundMixin
from plugins.platforms.zulip.targets import DEFAULT_TOPIC, parse_target_ref, resolve_destination, validate_target_ref
from plugins.platforms.zulip.topics import TopicRegistry

logger = logging.getLogger(__name__)

PLATFORM = Platform("zulip")

# Zulip 10.0 (FL 334): "channel" send/typing type, empty-topic support, temporary upload URLs.
MIN_FEATURE_LEVEL = 334
DEFAULT_MAX_MESSAGE_LENGTH = 10000
DEFAULT_MAX_TOPIC_LENGTH = 60
DEFAULT_MAX_UPLOAD_MIB = 25

_RECONNECT_BASE_DELAY, _RECONNECT_MAX_DELAY, _RECONNECT_JITTER = 2.0, 60.0, 0.2
_EVENT_TYPES = ["message", "update_message", "realm_user", "subscription"]
_FETCH_EVENT_TYPES = ["realm", "realm_user", "subscription"]


def _csv_set(value: Any) -> frozenset[str]:
    items = value if isinstance(value, (list, tuple, set)) else str(value or "").split(",")
    return frozenset(str(item).strip().lstrip("#").casefold() for item in items if str(item).strip())


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class ZulipSettings:
    """Behaviour settings from ``config.yaml`` ``zulip:`` (bridged into ``PlatformConfig.extra``)."""
    channels: frozenset[str] = field(default_factory=frozenset)  # subscribe + only listen here; empty = all
    free_response_channels: frozenset[str] = field(default_factory=frozenset)
    history_backfill: int = 20
    max_attachments: int = 10
    max_attachment_mb: int = 25
    bot_hop_limit: int = 3
    ack_reactions: bool = True
    dm_spinoff_channel: str = ""
    default_topic: str = DEFAULT_TOPIC

    @classmethod
    def from_extra(cls, extra: dict) -> ZulipSettings:
        return cls(
            channels=_csv_set(extra.get("channels")),
            free_response_channels=_csv_set(extra.get("free_response_channels")),
            history_backfill=max(0, _int(extra.get("history_backfill"), 20)),
            max_attachments=max(0, _int(extra.get("max_attachments"), 10)),
            max_attachment_mb=max(1, _int(extra.get("max_attachment_mb"), 25)),
            bot_hop_limit=max(0, _int(extra.get("bot_hop_limit"), 3)),
            ack_reactions=_bool(extra.get("ack_reactions"), True),
            dm_spinoff_channel=str(extra.get("dm_spinoff_channel") or "").strip().lstrip("#"),
            default_topic=str(extra.get("default_topic") or DEFAULT_TOPIC).strip() or DEFAULT_TOPIC)


def _credentials(config: Any) -> tuple[str, str, str]:
    """(site, email, api_key): profile-scoped env first, then ``config.extra``."""
    extra = getattr(config, "extra", None) or {}
    site = str(extra_or_secret(extra, "site", "ZULIP_SITE") or "").strip().rstrip("/")
    email = str(extra_or_secret(extra, "email", "ZULIP_EMAIL") or "").strip()
    api_key = str(extra_or_secret(extra, "api_key", "ZULIP_API_KEY") or getattr(config, "api_key", None)
                  or getattr(config, "token", None) or "").strip()
    return site, email, api_key


def check_zulip_requirements() -> bool:
    try:
        import aiohttp
        return True
    except ImportError:
        logger.warning("Zulip: aiohttp not installed")
        return False


def validate_zulip_config(config: PlatformConfig) -> bool:
    site, email, api_key = _credentials(config)
    if not (site and email and api_key):
        logger.debug("Zulip: ZULIP_SITE, ZULIP_EMAIL and ZULIP_API_KEY are all required")
        return False
    return True


def _registry_path(email: str) -> Path:
    """Per-profile, per-bot registry file; resolved inside the owning profile's scope."""
    from hermes_constants import get_hermes_home
    return get_hermes_home() / "platforms" / "zulip" / f"{re.sub(r'[^A-Za-z0-9_.@-]', '_', email)}.db"


class ZulipAdapter(ZulipInboundMixin, ZulipOutboundMixin, BasePlatformAdapter):
    """Gateway adapter for Zulip (self-hosted or Zulip Cloud)."""

    splits_long_messages = True  # send() chunks at the server's max_message_length
    supports_code_blocks = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, PLATFORM)
        self._site, self._email, self._api_key = _credentials(config)
        self._settings = ZulipSettings.from_extra(config.extra or {})
        self._dedup = MessageDeduplicator()
        self._configure_text_batch_delays()
        self._session: Any = None
        self._client: Optional[ZulipClient] = None
        self._registry: Optional[TopicRegistry] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._closing = False
        self._queue_id: Optional[str] = None
        self._last_event_id = -1
        self._last_message_id = 0
        self._user_id = 0
        self._me = ZulipUser(0, "")
        self._users: dict[int, ZulipUser] = {}
        self._users_by_name: dict[str, list[int]] = {}
        self._streams: dict[int, str] = {}
        self._stream_ids: dict[str, int] = {}
        self._max_message_length = DEFAULT_MAX_MESSAGE_LENGTH
        self._max_topic_length = DEFAULT_MAX_TOPIC_LENGTH
        self._max_upload_bytes = DEFAULT_MAX_UPLOAD_MIB * 1024 * 1024
        self._redirects: dict[str, str] = {}  # origin thread/chat → spun-off topic id, for the rest of the turn
        self._last_files: dict[str, Any] = {}
        self._dm_hops: dict[str, int] = {}

    # --- lifecycle ---

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not (self._site and self._email and self._api_key):
            logger.error("Zulip: ZULIP_SITE, ZULIP_EMAIL and ZULIP_API_KEY must all be set")
            return False
        if not self._acquire_platform_lock("zulip", f"{self._site}|{self._email.lower()}",
                                           f"Zulip bot {self._email} on {self._site}"):
            return False
        import aiohttp
        self._session = aiohttp.ClientSession(trust_env=gateway_trust_env(),
                                              headers={"User-Agent": "Hermes-Agent-Zulip/1.0"})
        self._client = ZulipClient(self._site, self._email, self._api_key, self._session)
        try:
            me = await self._client.get_me()
            self._user_id = int(me["user_id"])
            self._me = ZulipUser(self._user_id, me.get("full_name") or "", me.get("email") or self._email, True)
            self._registry = TopicRegistry(_registry_path(self._email))
            self._last_message_id = _int(self._registry.get_cursor("last_message_id"), 0)
            await self._subscribe_configured_channels()
            await self._register_queue()
        except (TimeoutError, ZulipError, aiohttp.ClientError, OSError) as exc:
            await self._fail_connect(exc)
            return False
        self._closing = False
        self._poll_task = asyncio.create_task(self._poll_loop(catch_up=True))
        self._mark_connected()
        self._wire_plugin_handlers(None)
        logger.info("Zulip: connected as %s (%s) on %s", self._me.full_name, self._user_id, self._site)
        return True

    async def _fail_connect(self, exc: Exception) -> None:
        if isinstance(exc, ZulipError) and exc.status in {401, 403}:
            self._set_fatal_error("zulip_auth_error", f"Zulip rejected the bot credentials ({exc.msg}). "
                                  "Check ZULIP_EMAIL / ZULIP_API_KEY.", retryable=False)
        elif isinstance(exc, ZulipError) and exc.code == "UNSUPPORTED_SERVER":
            self._set_fatal_error("zulip_unsupported_server", exc.msg, retryable=False)
        logger.error("Zulip: connect failed: %s", exc)
        await self._close_resources()

    async def _subscribe_configured_channels(self) -> None:
        if not self._settings.channels:
            return
        try:
            await self._client.subscribe(sorted(c for c in self._settings.channels if not c.isdigit()))
        except ZulipError as exc:
            logger.warning("Zulip: could not subscribe to configured channels: %s", exc.msg)

    async def _register_queue(self) -> None:
        payload = await self._client.register(_EVENT_TYPES, _FETCH_EVENT_TYPES)
        level = _int(payload.get("zulip_feature_level"), 0)
        if level < MIN_FEATURE_LEVEL:
            raise ZulipError(0, "UNSUPPORTED_SERVER",
                             f"Zulip server {payload.get('zulip_version')} (feature level {level}) is too old; "
                             f"feature level {MIN_FEATURE_LEVEL}+ (Zulip 10.0+) is required")
        self._queue_id, self._last_event_id = payload["queue_id"], int(payload["last_event_id"])
        self._max_message_length = _int(payload.get("max_message_length"), DEFAULT_MAX_MESSAGE_LENGTH)
        self._max_topic_length = _int(payload.get("max_topic_length"), DEFAULT_MAX_TOPIC_LENGTH)
        upload_mib = _int(payload.get("max_file_upload_size_mib"), DEFAULT_MAX_UPLOAD_MIB) or DEFAULT_MAX_UPLOAD_MIB
        self._max_upload_bytes = upload_mib * 1024 * 1024
        self._users_from_register(payload)

    async def _poll_loop(self, *, catch_up: bool = False) -> None:
        import aiohttp
        if catch_up:
            await self._catch_up()
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                if self._queue_id is None:
                    await self._register_queue()
                    await self._catch_up()
                events = await self._client.get_events(self._queue_id, self._last_event_id)
                delay = _RECONNECT_BASE_DELAY
                for event in events:
                    self._last_event_id = max(self._last_event_id, int(event.get("id", -1)))
                    await self._handle_event(event)
                continue
            except asyncio.CancelledError:
                raise
            except ZulipError as exc:
                if exc.code == "BAD_EVENT_QUEUE_ID":
                    logger.info("Zulip: event queue expired, re-registering")
                    self._queue_id = None
                    continue
                if exc.status in {401, 403}:
                    self._set_fatal_error("zulip_auth_error", f"Zulip rejected the bot credentials ({exc.msg})",
                                          retryable=False)
                    await self._notify_fatal_error()
                    return
                logger.warning("Zulip: event poll failed: %s", exc)
            except (TimeoutError, aiohttp.ClientError, OSError) as exc:
                logger.warning("Zulip: event poll network error: %s", exc)
            except Exception:
                logger.exception("Zulip: unexpected event loop error")
            if self._closing:
                return
            await asyncio.sleep(delay + delay * _RECONNECT_JITTER * random.random())
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _close_resources(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        if self._registry is not None:
            self._registry.close()
            self._registry = None
        self._release_platform_lock()

    async def disconnect(self) -> None:
        self._closing = True
        await cancel_task(self._poll_task)
        self._poll_task = None
        if self._queue_id and self._client is not None:
            try:
                await self._client.delete_queue(self._queue_id)
            except (ZulipError, OSError) as exc:
                logger.debug("Zulip: could not delete event queue: %s", exc)
        self._queue_id = None
        await self._close_resources()
        self._mark_disconnected()
        logger.info("Zulip: disconnected")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        if chat_id.startswith("d"):
            ids = [int(u) for u in chat_id[1:].split("-") if u.isdigit()]
            names = [self._users[u].full_name for u in ids if u in self._users]
            return {"name": ", ".join(names) or chat_id, "type": "group" if len(ids) > 1 else "dm",
                    "chat_id": chat_id}
        stream_id = _int(chat_id.lstrip("s"), 0)
        return {"name": "#" + self._streams.get(stream_id, chat_id), "type": "channel", "chat_id": chat_id}


# --- out-of-process delivery (cron, `hermes send`) ---

async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[list] = None, force_document: bool = False) -> dict[str, Any]:
    """Send without a live gateway: resolve the target, upload media, post one message, and
    register a new topic (with the message as context) so a reply there continues it."""
    import aiohttp
    site, email, api_key = _credentials(pconfig)
    if not (site and email and api_key):
        return send_error("Zulip standalone send: ZULIP_SITE, ZULIP_EMAIL and ZULIP_API_KEY must be set")
    registry = None
    try:
        async with aiohttp.ClientSession(trust_env=gateway_trust_env()) as session:
            client = ZulipClient(site, email, api_key, session)
            registry = TopicRegistry(_registry_path(email))
            dest = await resolve_destination(chat_id, thread_id, registry=registry, stream_id_for=client.get_stream_id,
                                             default_topic=ZulipSettings.from_extra(
                                                 getattr(pconfig, "extra", None) or {}).default_topic)
            links = []
            for media in media_files or []:
                path = Path(media.get("path") if isinstance(media, dict) else media)
                if await asyncio.to_thread(path.is_file):
                    import mimetypes
                    url = await client.upload(await asyncio.to_thread(path.read_bytes), path.name,
                                              mimetypes.guess_type(path.name)[0] or "application/octet-stream")
                    links.append(upload_markdown(path.name, url))
            content = "\n".join(filter(None, [message, *links]))
            if dest.kind == "direct":
                message_id = await client.send_direct(list(dest.user_ids), content)
            else:
                message_id = await client.send_channel(dest.stream_id, dest.topic, content)
                if dest.topic_id is None:
                    me = await client.get_me()
                    topic, created = registry.get_or_create(
                        dest.stream_id, dest.topic, created_by="hermes", last_addressed_bot=int(me["user_id"]),
                        seed=f"[Zulip context: you posted this message in this topic earlier:]\n{message}")
                    if created:
                        registry.update(topic.topic_id, last_handled_id=message_id)
        return {"success": True, "platform": "zulip", "chat_id": chat_id, "message_id": str(message_id)}
    except ZulipError as exc:
        return send_error(f"Zulip API error ({exc.code}): {exc.msg}")
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        return send_error(f"Zulip send failed: {exc}")
    finally:
        if registry is not None:
            registry.close()


# --- setup wizard ---

def interactive_setup() -> None:
    """Guide the user through Zulip bot setup (credentials, allowlist, home channel)."""
    from hermes_cli.cli_output import print_header, print_info, print_success, prompt
    from hermes_cli.config import save_env_value
    from hermes_cli.setup_platforms import declines_reconfigure

    print_header("Zulip")
    if declines_reconfigure("Zulip", "Reconfigure Zulip?", "ZULIP_API_KEY"):
        return
    for line in ("Each Hermes profile needs its own Zulip bot user:",
                 "   1. In Zulip: Settings → Personal settings → Bots → Add a new bot (type: Generic bot)",
                 "   2. Download its zuliprc, or copy the bot email and API key",
                 "   3. Subscribe the bot to the channels it should work in"):
        print_info(line)
    print()
    for env, label, secret in (("ZULIP_SITE", "Zulip server URL (e.g. https://chat.example.com)", False),
                               ("ZULIP_EMAIL", "Bot email", False), ("ZULIP_API_KEY", "Bot API key", True)):
        value = prompt(label, password=secret).strip()
        if not value:
            return
        save_env_value(env, value.rstrip("/") if env == "ZULIP_SITE" else value)
    print_success("Zulip credentials saved")
    allowed = prompt("Allowed users (comma-separated Zulip user ids or emails; empty = pairing)").strip()
    if allowed:
        save_env_value("ZULIP_ALLOWED_USERS", allowed.replace(" ", ""))
    home = prompt("Home channel for cron/notifications as channel>topic (empty to skip)").strip()
    if home:
        save_env_value("ZULIP_HOME_CHANNEL", home)
    print_info("Behaviour settings (channels, free_response_channels, ...) go under `zulip:` in config.yaml.")


# --- config.yaml → PlatformConfig.extra ---

_YAML_KEYS = ("channels", "free_response_channels", "history_backfill", "max_attachments", "max_attachment_mb",
              "bot_hop_limit", "ack_reactions", "dm_spinoff_channel", "default_topic", "allow_bots",
              "text_batch_delay_seconds", "text_batch_split_delay_seconds")


def _apply_yaml_config(yaml_cfg: dict, zulip_cfg: dict) -> Optional[dict]:
    """``zulip:`` behaviour keys → ``PlatformConfig.extra`` (behaviour never goes through env)."""
    seeded = {key: zulip_cfg[key] for key in _YAML_KEYS if isinstance(zulip_cfg, dict) and key in zulip_cfg}
    return seeded or None


_is_connected = env_is_connected("ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY")


# System-prompt platform hint (PlatformEntry.platform_hint); cached-prompt bytes, keep stable.
_PLATFORM_HINT = (
    "You are chatting in Zulip. Zulip conversations are organized as topics inside channels; each topic is "
    "its own conversation with you, and your reply is posted to the current topic automatically. Zulip renders "
    "Markdown: code blocks, tables, quotes, `$$...$$` math, ```spoiler blocks, @**Full Name** mentions and "
    "#**channel>topic** links. To move a clearly separate piece of work into a new topic of the same channel, "
    "start your reply with a first line `TOPIC: <short topic name>` (max 60 characters) followed by the opening "
    "message for that topic; it is posted there, linked from the current topic, and continues as its own "
    "conversation. Do this when the user asks to split something off or when a new workstream would clutter the "
    "current topic, not for ordinary replies. To send files, include MEDIA:/absolute/path/to/file in your "
    "response; several files are posted together in one message. Messages from other people in the topic that "
    "were not addressed to you may be included as context before the user's message."
)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    from plugins.platforms.zulip import gateway_env
    ctx.register_platform(
        name="zulip", label="Zulip", adapter_factory=ZulipAdapter,
        dashboard_description="Connect Hermes to Zulip channels, topics and direct messages.",
        docs_url="https://zulip.com/help/add-a-bot-or-integration",
        dashboard_env_vars=("ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY", "ZULIP_ALLOWED_USERS"),
        check_fn=check_zulip_requirements, validate_config=validate_zulip_config, is_connected=_is_connected,
        required_env=["ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"], install_hint="pip install aiohttp",
        setup_fn=interactive_setup, apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="ZULIP_HOME_CHANNEL", standalone_sender_fn=_standalone_send,
        parse_target_ref_fn=parse_target_ref, validate_target_ref_fn=validate_target_ref,
        max_message_length=DEFAULT_MAX_MESSAGE_LENGTH, emoji="💬", allow_update_command=True,
        platform_hint=_PLATFORM_HINT, display_tier="medium",
        env_overrides=gateway_env.ENV_OVERRIDES, env_enable_credentials=gateway_env.ENV_ENABLE_CREDENTIALS,
        **gateway_env.AUTHZ,
    )
