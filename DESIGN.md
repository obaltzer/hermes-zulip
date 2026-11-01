# Hermes ↔ Zulip Integration — Design

Status: implemented 2026-11-01; standalone plugin repo since the same day (no Hermes fork).
Targets: **Hermes Agent v0.22.5+** (tested against v0.22.5 and upstream `main`) and the **latest
Zulip server / API feature level**.

Decisions so far: platform plugin, maintained outside Hermes · streaming edits **off** · resolving
a topic **keeps** its session.

## 1. Goals

| # | Requirement | Design answer |
|---|-------------|---------------|
| G1 | Zulip's topic model is the conversation model: **one topic = one Hermes session** with its own context | `thread_id` is a stable *topic id* from a per-profile registry that follows renames and moves (§4) |
| G2 | **Users or Hermes** can start new topics | Users: create a topic in Zulip. Hermes: a reply directive opens a new topic and seeds its session (§5). Cron / `hermes send` can also target new topics |
| G3 | **One Zulip bot user per Hermes profile** | Each profile has its own Zulip bot credentials. Hermes's multiplexing gateway runs one adapter per profile and routes each bot's events to that profile (§6) |
| G4 | **Media**, including **multiple images/files per message**, both directions | Inbound: all `/user_uploads/` links → one event with N `media_urls`. Outbound: N files → one Zulip message (§7) |

Out of scope for v1: outgoing-webhook bots, Zulip polls/todo widgets, and calls.

## 2. Approach: a platform plugin, zero core changes

The current Hermes adds platforms as **plugins** (`plugins/platforms/<name>/`). Mattermost, Slack,
Discord and IRC are all shipped this way. `ctx.register_platform(...)` covers every integration point:
adapter factory, config/env bridge, authorization allowlists, cron delivery
(`cron_deliver_env_var` + `standalone_sender_fn`), the `hermes send` / cron target parser,
platform prompt hint, message chunking, setup wizard, status, and dashboard.

The project rule is that *plugins never touch core*. The Zulip integration is therefore:

```
zulip/               → installed as <hermes>/plugins/platforms/zulip/
  plugin.yaml        manifest + requires_env / optional_env (setup UI)
  __init__.py        re-exports register
  gateway_env.py     EnvCred / home_step / AUTHZ (env → config bridge)
  adapter.py         ZulipAdapter(BasePlatformAdapter) + register()
  client.py          thin async Zulip REST client (aiohttp, Basic auth)
  topics.py          topic registry (SQLite, per profile)
  engagement.py      "should this bot answer?" rules
  media.py           upload-link parsing, download/upload, batching
tests/test_zulip_*.py   fake-Zulip (aiohttp test server) behaviour tests (run inside a Hermes tree)
docs/zulip.md           user guide
```

Because nothing in core is touched, the plugin lives in its own repo and is installed by copying
`zulip/` to `<hermes>/plugins/platforms/zulip/`, where Hermes loads it as a bundled platform. It
imports itself as `plugins.platforms.zulip`, so it must sit at exactly that path; a
`~/.hermes/plugins/zulip/` user install would need those imports made relative first. It could
later go upstream or into the plugin catalog unchanged.

Transport: **raw Zulip REST over `aiohttp`**, the same approach Mattermost uses. No new
dependency. The official `zulip` SDK is synchronous and thread-based.

## 3. Zulip concepts → Hermes concepts

| Zulip | Hermes `SessionSource` (via `self.build_source`) | Notes |
|-------|------------------------------------------------|-------|
| Channel (stream) message | `chat_type="channel"`, `chat_id="s<origin_stream_id>"` | Numeric id, not name |
| Topic | `thread_id="<topic_id>"` (e.g. `t01J…`) | Registry id. **Never contains `:`**, because Hermes splits session keys on `:` |
| Topic name | `chat_topic` | Current display name, visible to the agent |
| 1:1 DM | `chat_type="dm"`, `chat_id="d<sorted user ids>"` | No topics → one session per DM conversation; `/new` resets it |
| Group DM | `chat_type="group"`, `chat_id="d<sorted user ids>"` | Same as above |
| Sender | `user_id=<zulip user id>`, `user_name=<full name>`, `is_bot` | `user_id_alt=<email>` so allowlists can use either |
| Message id | `message_id` | Reactions, dedup, replies |

Resulting session key (Hermes builds it, namespaced per profile):
`agent:<profile>:zulip:channel:s<stream_id>:<topic_id>`. Threads are shared by all participants
(`thread_sessions_per_user=false`, the default). This is intended: everyone in a topic talks to
the same Hermes conversation.

## 4. Topic identity (core of G1)

Using the raw topic name as `thread_id` breaks because:

* Zulip matches topic names **case-insensitively**.
* Topics are **renamed**, **resolved** (`✔ ` prefix), **moved** across channels, and **split**.
* Names may contain `:`. Hermes session keys are `:`-separated and parsed by splitting.
* Zulip has no server-side topic id in the API.

So each profile keeps a **topic registry**: SQLite at
`<profile HERMES_HOME>/platforms/zulip/topics.db`. The path is resolved with `get_hermes_home()`
inside `connect()`, which runs under the owning profile's scope, and is then held on the adapter.

```
topics(topic_id TEXT PK,            -- "t" + ULID, never reused, no ':'
       origin_stream_id INT,        -- part of the session key; never changes
       stream_id INT,               -- current channel (routing)
       name TEXT,                   -- current display name (original casing)
       name_key TEXT,               -- casefold(strip "✔ " prefix)
       created_by TEXT,             -- 'user' | 'hermes' | 'split'
       parent_topic_id TEXT NULL,
       seed TEXT NULL,              -- one-shot context to prepend on first inbound turn (§5)
       last_addressed_bot INT NULL, -- §6 engagement
       bot_hops INT DEFAULT 0,
       last_message_id INT,
       UNIQUE(stream_id, name_key))
cursor(key TEXT PK, value TEXT)     -- event-queue state, last processed message id
```

Driven by Zulip `update_message` events:

| Event | Detection | Effect |
|-------|-----------|--------|
| New topic | Message for an unknown `(stream_id, name_key)` | New row → new session |
| Rename (`propagate_mode=change_all`) | `orig_subject` ≠ `subject` | Update name/key → **same session** |
| Resolve / unresolve | `✔ ` prefix added or removed | Same `name_key` → **same session** (decided) |
| Move to another channel (`change_all`) | `new_stream_id` | Update `stream_id`. Key keeps `origin_stream_id` → same session |
| Split (`change_one` / `change_later`) | Only some messages moved | New row (`created_by='split'`, `parent_topic_id`) and a `seed` built from the moved messages → new session that knows where it came from |

Outbound `send(chat_id, …, metadata={"thread_id": topic_id})` resolves the **current**
`(stream_id, name)` from the registry. A topic renamed or moved mid-turn still gets the reply.

## 5. Creating topics

### 5.1 User-initiated
Start a topic in Zulip and address the bot (§6). A new registry row creates a new session.

**History backfill:** the first time the bot is engaged in a topic that already has human
messages, the adapter fetches the last *N* messages there (`GET /messages`, narrow channel+topic,
default 20). It prepends them once to the inbound text as a quoted context block. The context
becomes part of the transcript, which keeps the prompt cache intact. It isn't a system-prompt
mutation.

### 5.2 Hermes-initiated (agent opens a topic)
Upstream deliberately does **not** let the agent message arbitrary targets: `send_message` is
host-driven only, and plugins must not add an equivalent tool. Hermes therefore opens topics as
**part of its reply** in the current conversation, using a reply directive that the adapter
interprets. This is the same pattern as the existing `MEDIA:` directive:

```
TOPIC: Deploy pipeline redesign
<the opening message for the new topic>
```

* The directive must be on the reply's first line. It always targets the **same channel**. Only
  the current conversation's channel is reachable, so the agent can't post into channels or DMs
  it isn't in.
* The adapter posts the body into the new topic. If the name is already in use, the adapter
  posts into that topic instead of failing. It replies in the origin topic with
  `↪ continued in #**channel>Deploy pipeline redesign**`.
* It registers the topic (`created_by='hermes'`, `parent_topic_id`), with `seed` = "You opened
  this topic from #**channel>origin**; your opening message was: …" and `last_addressed_bot = self`.
* On the first user message in the new topic, the adapter prepends the seed to the event text. The
  new session therefore starts with the right context and no core session-store calls.
* The platform hint teaches the agent about the directive, Zulip's topic length limit (taken from
  the server's `max_topic_length`), and when to spin off. Spin off for a clearly separate
  workstream, or when the user asks.
* In DMs, which have no topics, the directive is rejected with a short note. If the profile has
  `zulip.dm_spinoff_channel` configured, the new topic opens there instead.

**Host-driven sends** (cron jobs, `hermes send --to`) use the target grammar
`zulip:<channel>:<topic name>`. `<channel>` is a numeric id or a name, and the topic is everything
after the second `:`. This is implemented by `parse_target_ref_fn` / `validate_target_ref_fn` and
`standalone_sender_fn`. Posting to a new topic name creates it and registers it the same way, with
the posted text as seed.

## 6. Multiple profiles → multiple bot users (G3)

Current Hermes runs **one multiplexing gateway** that serves every profile
(`gateway.multiplex_profiles`, on by default). Each profile's platforms come up under that
profile's own secret scope and `HERMES_HOME`, and inbound events run in the profile that owns
the receiving bot. So:

* One Zulip **generic bot** per profile (e.g. `coder-bot@…`, `research-bot@…`). Credentials go
  in that profile's `.env` (`ZULIP_EMAIL`, `ZULIP_API_KEY`; `ZULIP_SITE` may be shared).
* The adapter reads all config through `extra_or_secret` / `get_scoped_secret`, **never
  `os.getenv`**. It resolves its state paths under the owning profile's scope. Background tasks are
  started from `connect()` (scope-bound) so they inherit the context.
* It takes a scoped lock in `connect()` on `zulip:<site>:<bot email>`. Two profiles can't use one
  bot account, and a duplicated credential also blocks the multiplex fold, which is the intended
  behavior.
* Standalone per-profile gateways (`gateway.standalone: true`) work unchanged.

**Engagement rules.** Every bot subscribed to a channel sees every message, so each bot decides
on its own, from the same events, whether to answer:

1. **Explicit mention wins.** If a message @-mentions bots, only those bots answer. Every bot
   records the mention as the topic's `last_addressed_bot`.
2. **Topic stickiness.** An unmentioned message in a topic goes to that topic's
   `last_addressed_bot`, so a conversation flows without repeating `@**Bot**`.
3. **Free-response channels** (`zulip.free_response_channels`): unmentioned messages in topics
   with no `last_addressed_bot` are answered. Use these for channels dedicated to one bot.
4. Topics a bot opened itself (§5.2) start with `last_addressed_bot = self`.
5. Silent mentions (`@_**bot**`) and wildcard mentions (`@**all**`, `@**topic**`) do **not** trigger.

**Bot-to-bot:** messages from bot users are ignored unless they explicitly mention this bot. A
per-topic hop counter (`zulip.bot_hop_limit`, default 3) stops loops and is reset by any human
message. This enables profile→profile delegation ("@**research-bot** dig into X"). A bot always
ignores its own messages and edit echoes.

## 7. Media and attachments (G4)

### Inbound
* The event queue is registered with `apply_markdown=false`, so the adapter gets raw Markdown.
* It extracts **every** `[name](/user_uploads/…)` link, in order.
* It downloads each one with the bot's credentials. Downloads are bounded:
  `zulip.max_attachments` (10) and `zulip.max_attachment_mb` (25).
* Each file is cached with Hermes's `cache_{image,audio,video,document}_from_bytes_async`.
  `media_urls` and `media_types` stay aligned. `message_type` is `PHOTO` (all images),
  `VOICE` (audio), or `DOCUMENT` (anything else).
* The links in the text are rewritten to `📎 name`, so the agent sees where each attachment sat.
  Skipped files are named in a short notice.
* Several quick messages from one sender in one topic (text, then images) are merged by Hermes's
  existing per-session text/media batching. The adapter derives keys only through
  `_event_session_key` / `_source_session_key`.

### Outbound
* `send_image_file` / `send_document` / `send_video` / `send_voice` upload with
  `POST /user_uploads` and post `[name](url)`.
* `send_multiple_images` (and buffered sends of several files to one topic within a short window)
  produce **one** message with every link. Zulip shows a preview for each image.
* Remote image URLs are downloaded (SSRF-checked with `tools.url_safety.is_safe_url`) and
  re-uploaded. On failure the plain link is posted.
* Uploads respect the server's `max_file_upload_size_mib`. A file that's too large produces a
  clear in-topic notice.

## 8. Messaging details

* **Event loop:** `POST /register` (`message`, `update_message`, `delete_message`,
  `subscription`), then long-poll `GET /events`. Reconnects use exponential backoff with jitter.
  On `BAD_EVENT_QUEUE_ID` the adapter re-registers and backfills from the persisted
  last-processed id (bounded), deduplicated with `MessageDeduplicator`. Auth failures (401) go
  through `_set_fatal_error` / `_notify_fatal_error`.
* **Typing:** `POST /typing` for channel topics and DMs, refreshed by the base `_keep_typing`.
* **Acks:** 👀 reaction on processing start, replaced by ✅ or ⚠️ on completion
  (`on_processing_start` / `on_processing_complete`). Configurable.
* **Length:** `max_message_length` comes from the server at register time (10 000 by default).
  The adapter chunks on paragraph and code-fence boundaries.
* **Streaming:** **off** (decided). One finished message per reply. `edit_message` is still
  implemented for Hermes paths that edit status messages.
* **Formatting hint:** Zulip Markdown (fenced code, tables, `$$…$$` math, `spoiler` blocks,
  `@**Name**`, `#**channel>topic**` links), the `TOPIC:` directive, and `MEDIA:` for files. It also
  tells the agent that replies land in the current topic automatically.
* **Commands:** Hermes slash commands work per topic. `/new` resets **this topic's** session.
  `/stop` interrupts it.
* **Reset policy:** the recommended Zulip default is no idle reset, because the topic is the
  context boundary.
* **Authorization:** `ZULIP_ALLOWED_USERS` (ids or emails) / `ZULIP_ALLOW_ALL_USERS`, with
  `allowlist_suppresses_pairing`. `zulip.channels` is both the subscription list and the
  allowlist: messages from other channels are ignored.

## 9. Configuration

Following Hermes policy, `.env` holds **secrets only** and behavior goes in `config.yaml`. The
YAML is bridged into `PlatformConfig.extra` through `apply_yaml_bridge`.

`.env` (per profile):

| Var | Purpose |
|-----|---------|
| `ZULIP_SITE` | Server URL (`https://chat.example.com`) |
| `ZULIP_EMAIL`, `ZULIP_API_KEY` | Bot credentials (from the bot's `zuliprc`) |
| `ZULIP_ALLOWED_USERS` / `ZULIP_ALLOW_ALL_USERS` | Authorization (env, matching every other platform) |
| `ZULIP_HOME_CHANNEL` | `channel>topic` for cron / default delivery |

`config.yaml` (per profile):

```yaml
zulip:
  channels: [hermes, eng]          # subscribe + listen
  free_response_channels: [hermes]
  history_backfill: 20
  max_attachments: 10
  max_attachment_mb: 25
  bot_hop_limit: 3
  ack_reactions: true
  dm_spinoff_channel: hermes       # optional; enables TOPIC: from DMs
```

## 10. Risks / to verify during implementation

* **Zulip API details.** Check every endpoint and field against the current API docs: upload
  response field, authenticated download path, `topic` vs `subject` naming, the empty "general
  chat" topic, and `max_topic_length` / `max_message_length` / `max_file_upload_size_mib` in
  `/register`. Pin a minimum `zulip_feature_level` and refuse to start below it.
* **No server-side topic id.** The registry follows renames and moves only through events the bot
  receives. A rename while the gateway is down is caught on backfill when possible, and otherwise
  degrades to a new session.
* **Directive robustness.** `TOPIC:` must never trigger from quoted or code-fenced text. It is
  first-line only, and tests cover this.
* **Rate limits:** honor `Retry-After` on 429.

## 11. Delivery plan

1. **Skeleton.** Plugin manifest, `gateway_env`, register, REST client, connect/disconnect, the
   event loop, inbound text from channels and DMs, send, typing, and the scoped lock.
2. **Topic registry + engagement.** Session mapping, renames/moves/resolves/splits, mention and
   stickiness rules, bot hops.
3. **Media.** Inbound multi-attachment, outbound upload and batching, limits.
4. **Topic creation.** The `TOPIC:` directive, seeding, history backfill, the target parser,
   and `standalone_sender_fn` for cron.
5. **Polish.** Setup wizard, platform hint, docs page, and the full test suite
   (`scripts/run_tests.sh tests/gateway/test_zulip_*`), plus `python scripts/check`.
6. **End-to-end** against a real Zulip, either a Docker dev server or your instance, with two bots
   and two profiles under one multiplexing gateway.

## 12. Open questions

1. ~~Zulip version~~ → latest. ~~Streaming~~ → off. ~~Resolve~~ → keep session. ~~Delivery~~ → standalone plugin repo, copied into the Hermes install.
2. ~~`TOPIC:` directive~~ → approved.
3. ~~E2E server~~ → local Docker Zulip 12.3 (`e2e/`). Zulip 12 refuses plain HTTP without a TLS proxy, so the
   harness uses its self-signed HTTPS. Minimum supported server: Zulip 10.0 (feature level 334).
4. Bot-to-bot hand-offs need the other bot authorized: add its email or id to `ZULIP_ALLOWED_USERS`, or set
   `zulip.allow_bots: mentions`. This isn't opened by default because any realm user can create a bot.
