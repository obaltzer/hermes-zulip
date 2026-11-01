"""In-process fake Zulip server (aiohttp) for the Zulip adapter tests.

Implements the slice of the REST API the adapter uses, records every write, and serves events
from a queue so tests can drive the real long-poll loop.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web
from aiohttp.test_utils import TestServer

BOT_ID, HUMAN_ID, OTHER_BOT_ID, BOB_ID = 100, 10, 200, 11
BOT_EMAIL = "hermes-bot@zulip.example"
STREAM_ID, STREAM = 5, "eng"
OTHER_STREAM_ID, OTHER_STREAM = 6, "ops"

REALM_USERS = [
    {"user_id": BOT_ID, "full_name": "Hermes", "email": BOT_EMAIL, "is_bot": True},
    {"user_id": HUMAN_ID, "full_name": "Alice", "email": "alice@example.com", "is_bot": False},
    {"user_id": BOB_ID, "full_name": "Bob", "email": "bob@example.com", "is_bot": False},
    {"user_id": OTHER_BOT_ID, "full_name": "Research", "email": "research-bot@zulip.example", "is_bot": True},
]


@dataclass
class FakeZulip:
    events: asyncio.Queue = field(default_factory=asyncio.Queue)
    sent: list[dict] = field(default_factory=list)
    edits: list[dict] = field(default_factory=list)
    uploads: dict[str, bytes] = field(default_factory=dict)
    reactions: list[tuple[str, int, str]] = field(default_factory=list)
    typing: list[dict] = field(default_factory=list)
    history: list[dict] = field(default_factory=list)  # served by GET /messages
    registrations: int = 0
    expire_queue_once: bool = False
    feature_level: int = 400
    temp_urls: bool = True  # False: the temporary-URL endpoint is unavailable (direct download path)
    redirect_uploads_to: str = ""  # direct /user_uploads/ GETs redirect here (e.g. object storage)
    next_id: int = 1000
    server: Any = None

    @property
    def site(self) -> str:
        return str(self.server.make_url("")).rstrip("/")

    def message(self, content: str, *, topic: str = "general", stream_id: int = STREAM_ID,
                sender: int = HUMAN_ID, msg_id: int | None = None) -> dict:
        user = next(u for u in REALM_USERS if u["user_id"] == sender)
        self.next_id += 1
        return {"id": msg_id or self.next_id, "type": "stream", "stream_id": stream_id,
                "display_recipient": STREAM if stream_id == STREAM_ID else OTHER_STREAM,
                "subject": topic, "sender_id": sender, "sender_full_name": user["full_name"],
                "sender_email": user["email"], "content": content, "timestamp": 2_000_000_000}

    def dm(self, content: str, *, sender: int = HUMAN_ID, others: tuple[int, ...] = ()) -> dict:
        user = next(u for u in REALM_USERS if u["user_id"] == sender)
        self.next_id += 1
        recipients = [{"id": uid} for uid in sorted({BOT_ID, sender, *others})]
        return {"id": self.next_id, "type": "private", "display_recipient": recipients, "sender_id": sender,
                "sender_full_name": user["full_name"], "sender_email": user["email"], "content": content,
                "timestamp": 2_000_000_000}

    # --- handlers ---

    @staticmethod
    def ok(**payload) -> web.Response:
        return web.json_response({"result": "success", "msg": "", **payload})

    async def users_me(self, request):
        return self.ok(user_id=BOT_ID, email=BOT_EMAIL, full_name="Hermes", is_bot=True)

    async def user_by_email(self, request):
        email = request.match_info["email"]
        user = next((u for u in REALM_USERS if u["email"] == email), None)
        if user is None:
            return web.json_response({"result": "error", "msg": "No such user", "code": "BAD_REQUEST"}, status=400)
        return self.ok(user=user)

    async def register(self, request):
        self.registrations += 1
        return self.ok(queue_id=f"q{self.registrations}", last_event_id=-1, zulip_feature_level=self.feature_level,
                       zulip_version="13.0", max_message_length=10000, max_topic_length=60,
                       max_file_upload_size_mib=1, realm_users=REALM_USERS,
                       subscriptions=[{"stream_id": STREAM_ID, "name": STREAM},
                                      {"stream_id": OTHER_STREAM_ID, "name": OTHER_STREAM}])

    async def get_events(self, request):
        if self.expire_queue_once:
            self.expire_queue_once = False
            return web.json_response({"result": "error", "code": "BAD_EVENT_QUEUE_ID", "msg": "Bad event queue ID",
                                      "queue_id": request.query["queue_id"]}, status=400)
        try:
            event = await asyncio.wait_for(self.events.get(), timeout=0.5)
            return self.ok(events=[event])
        except TimeoutError:
            return self.ok(events=[{"type": "heartbeat", "id": 0}])

    async def delete_events(self, request):
        return self.ok()

    async def send_message(self, request):
        form = dict(await request.post())
        self.next_id += 1
        self.sent.append({**form, "id": self.next_id})
        return self.ok(id=self.next_id)

    async def edit_message(self, request):
        self.edits.append({"id": int(request.match_info["mid"]), **dict(await request.post())})
        return self.ok()

    async def upload(self, request):
        reader = await request.multipart()
        part = await reader.next()
        data = await part.read()
        path = f"/user_uploads/2/ab/{len(self.uploads)}/{part.filename}"
        self.uploads[path] = bytes(data)
        return self.ok(url=path, filename=part.filename)

    async def temp_url(self, request):
        if not self.temp_urls:
            return web.json_response({"result": "error", "code": "BAD_REQUEST", "msg": "nope"}, status=404)
        return self.ok(url=f"/tmp-download/{request.match_info['rest']}")

    async def direct_upload(self, request):
        if "Authorization" not in request.headers:
            return web.Response(status=401)
        if self.redirect_uploads_to:
            raise web.HTTPFound(f"{self.redirect_uploads_to}/{request.match_info['rest']}")
        return await self.tmp_download(request)

    async def tmp_download(self, request):
        data = self.uploads.get("/user_uploads/" + request.match_info["rest"])
        return web.Response(body=data, content_type="application/octet-stream") if data is not None \
            else web.Response(status=404)

    async def get_messages(self, request):
        narrow = json.loads(request.query.get("narrow") or "[]")
        topic = next((n["operand"] for n in narrow if n["operator"] == "topic"), None)
        before = int(request.query.get("num_before", 0))
        msgs = [m for m in self.history if topic is None or m["subject"].casefold() == topic.casefold()]
        anchor = request.query.get("anchor", "newest")
        if anchor.isdigit():
            msgs = [m for m in msgs if m["id"] < int(anchor)] if before else [m for m in msgs if m["id"] > int(anchor)]
        return self.ok(messages=msgs[-before:] if before else msgs)

    async def reaction(self, request):
        data = dict(await request.post()) if request.method == "POST" else dict(request.query)
        self.reactions.append((request.method, int(request.match_info["mid"]), data["emoji_name"]))
        return self.ok()

    async def typing_status(self, request):
        self.typing.append(dict(await request.post()))
        return self.ok()

    async def subscribe(self, request):
        return self.ok(subscribed={}, already_subscribed={})

    async def stream_id(self, request):
        return self.ok(stream_id={STREAM: STREAM_ID, OTHER_STREAM: OTHER_STREAM_ID}[request.query["stream"]])

    async def start(self) -> FakeZulip:
        app = web.Application()
        r = app.router
        r.add_get("/api/v1/users/me", self.users_me)
        r.add_get("/api/v1/users/{email}", self.user_by_email)
        r.add_post("/api/v1/register", self.register)
        r.add_get("/api/v1/events", self.get_events)
        r.add_delete("/api/v1/events", self.delete_events)
        r.add_post("/api/v1/messages", self.send_message)
        r.add_get("/api/v1/messages", self.get_messages)
        r.add_patch("/api/v1/messages/{mid}", self.edit_message)
        r.add_post("/api/v1/messages/{mid}/reactions", self.reaction)
        r.add_delete("/api/v1/messages/{mid}/reactions", self.reaction)
        r.add_post("/api/v1/user_uploads", self.upload)
        r.add_get("/api/v1/user_uploads/{rest:.+}", self.temp_url)
        r.add_get("/tmp-download/{rest:.+}", self.tmp_download)
        r.add_get("/user_uploads/{rest:.+}", self.direct_upload)
        r.add_post("/api/v1/typing", self.typing_status)
        r.add_post("/api/v1/users/me/subscriptions", self.subscribe)
        r.add_get("/api/v1/get_stream_id", self.stream_id)
        self.server = TestServer(app)
        await self.server.start_server()
        return self

    async def close(self) -> None:
        await self.server.close()
