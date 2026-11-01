"""Deterministic OpenAI-compatible stand-in for a model, for the full-gateway E2E run.

Replies "[<model>] turn=<n> images=<k> ctx=<yes|no>: <last user text>", where n counts user turns in
the conversation it was given (so separate topics show separate sessions). When the user asks to
"spin off", it answers with a TOPIC: directive like the real agent would.
"""

import json
import sys
import time

from aiohttp import web


def _text(content) -> str:
    if isinstance(content, str):
        return content
    return " ".join(p.get("text", "") for p in content or [] if isinstance(p, dict) and p.get("type") == "text")


def _images(content) -> int:
    if isinstance(content, str):
        return 0
    return sum(1 for p in content or [] if isinstance(p, dict) and p.get("type") in {"image_url", "input_image"})


def reply_for(body: dict) -> str:
    messages = [m for m in body.get("messages", []) if m.get("role") == "user"]
    last = messages[-1] if messages else {"content": ""}
    text = _text(last.get("content"))
    if "spin off" in text.lower():
        tag = text.lower().rsplit("spin off", 1)[1].split()[-1] if text.lower().rsplit("spin off", 1)[1].split() else ""
        return f"TOPIC: Gateway spin-off {tag}\nOpening message written by the agent for the new topic."
    ctx = "yes" if "[Zulip context" in text else "no"
    tail = text.rsplit("[New message]", 1)[-1].strip().replace("\n", " ")[-120:]
    return f"[{body.get('model')}] turn={len(messages)} images={_images(last.get('content'))} ctx={ctx}: {tail}"


async def chat(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    content = reply_for(body)
    print(json.dumps({"model": body.get("model"), "reply": content[:200]}), flush=True)
    created, rid = int(time.time()), f"chatcmpl-{time.time_ns()}"
    if not body.get("stream"):
        return web.json_response({
            "id": rid, "object": "chat.completion", "created": created, "model": body.get("model"),
            "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}})
    resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
    await resp.prepare(request)
    for delta, finish in (({"role": "assistant", "content": content}, None), ({}, "stop")):
        chunk = {"id": rid, "object": "chat.completion.chunk", "created": created, "model": body.get("model"),
                 "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
        await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
    await resp.write(b"data: [DONE]\n\n")
    return resp


async def models(request: web.Request) -> web.Response:
    return web.json_response({"object": "list", "data": [{"id": m, "object": "model"} for m in ("stub-a", "stub-b")]})


app = web.Application()
app.router.add_post("/v1/chat/completions", chat)
app.router.add_get("/v1/models", models)
web.run_app(app, host="127.0.0.1", port=int(sys.argv[1]) if len(sys.argv) > 1 else 18081)
