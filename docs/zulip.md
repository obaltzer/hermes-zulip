---
sidebar_position: 9
title: "Zulip"
description: "Set up Hermes Agent as a Zulip bot: one session per topic, one bot per profile"
---

# Zulip Setup

Hermes Agent connects to Zulip (self-hosted or Zulip Cloud) as a **generic bot**. Zulip organizes
conversations as **topics inside channels**, and Hermes follows that model: **every topic is its own
Hermes session** with its own context. Hermes can also open new topics itself, each Hermes profile
can join Zulip as its own bot user, and messages may carry any number of images and files in either
direction.

The adapter talks to Zulip's REST API and event queue with `aiohttp` (no Zulip SDK). It requires
Zulip Server 10.0 or newer (API feature level 334+).

## How Hermes behaves

| Context | Behavior |
|---------|----------|
| **Channel topic** | `@**Hermes**` a message to bring Hermes into a topic. After that, Hermes answers every message in that topic without a mention, until someone mentions a different bot there. |
| **New topic by you** | Start a topic in Zulip's UI and mention Hermes. It is a fresh session. Earlier messages in the topic (up to `history_backfill`) are given to Hermes as context. |
| **New topic by Hermes** | Ask Hermes to "take this to a new topic". Hermes posts an opening message into a new topic of the same channel, links it from the current topic, and that topic continues as its own session that knows where it came from. |
| **Renamed / resolved / moved topic** | Same session. Hermes keeps replying in the right place even if the topic is renamed while it is working. |
| **Split topic** (some messages moved to a new topic) | The new topic gets a new session, seeded with the moved messages. |
| **Direct messages** | Hermes answers every message. Each DM conversation is one session (`/new` resets it). |
| **Free-response channels** | In channels listed in `free_response_channels`, Hermes answers unmentioned messages in topics no other bot has claimed. |
| **Messages you didn't address to Hermes** | Not answered, but the next time Hermes is addressed in that topic it sees what was said in between. |

Slash commands work per topic: `/new` resets **this topic's** session, `/stop` interrupts the run in
this topic. Hermes reacts 👀 while working and ✅ / ⚠️ when done (`ack_reactions: false` to disable).

## Step 1: Create a bot

1. In Zulip: **Settings → Personal settings → Bots → Add a new bot**.
2. Bot type: **Generic bot**. Give it the name people will mention (e.g. `Hermes`).
3. Download the bot's `zuliprc`, or copy its **email** and **API key**.
4. Subscribe the bot to the channels where it should work (or list them under `zulip.channels`
   below and Hermes subscribes itself).

## Step 2: Configure Hermes

Run `hermes gateway setup` and pick **Zulip**, or put the credentials into the profile's `.env`:

```bash
ZULIP_SITE=https://chat.example.com
ZULIP_EMAIL=hermes-bot@chat.example.com
ZULIP_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
ZULIP_ALLOWED_USERS=alice@example.com,15        # emails or user ids; empty = pairing
ZULIP_HOME_CHANNEL=hermes>notifications           # channel>topic for cron delivery
```

Behaviour settings live in `config.yaml` (all optional):

```yaml
zulip:
  channels: [hermes, eng]           # subscribe and listen only here (default: every subscribed channel)
  free_response_channels: [hermes]  # no mention needed in unclaimed topics here
  history_backfill: 20              # earlier topic messages given as context (0 = off)
  max_attachments: 10               # per inbound message
  max_attachment_mb: 25
  bot_hop_limit: 3                  # max consecutive bot↔bot turns in a topic
  ack_reactions: true
  dm_spinoff_channel: hermes        # lets Hermes open topics from a DM conversation
  default_topic: Hermes             # topic for sends that name only a channel
```

Then start the gateway: `hermes gateway run` (or `hermes gateway start` for the service).

## Several profiles, several bots

Each [profile](../profiles.md) gets its **own Zulip bot** — create one bot per profile and put its
credentials in that profile's `.env`. The multiplexing gateway serves all profiles from one process;
each bot's messages run in its own profile (its own memory, skills, model and sessions). Two profiles
cannot share one bot account.

Several Hermes bots can work in the same channel. Each one decides independently, using the same
rules, so they never talk over each other:

1. A message that mentions bots is answered only by those bots, and the first mentioned bot "owns"
   the topic from then on.
2. An unmentioned message is answered by the topic's owner.
3. Bots ignore other bots unless mentioned, and a chain of bot↔bot replies stops after
   `bot_hop_limit` turns until a human writes again.

So `@**Research** please dig into this` hands a topic from one profile to another, and the research
bot can hand it back with `@**Hermes**`. Bot senders must be authorized like users: add the other
bots' emails or ids to `ZULIP_ALLOWED_USERS` (or set `zulip.allow_bots: mentions`).

## Opening topics from Hermes

Hermes opens a topic by starting its reply with a line `TOPIC: <name>`. The rest of the reply is
posted as the opening message of that topic (same channel), the current topic gets a
`↪ Continued in #**channel>name**` link, and files from the same reply go along with it. The new
topic's session starts with a note saying where it came from and what Hermes wrote there. You can just
ask: *"Spin the migration work off into its own topic."*

Scheduled jobs and `hermes send` can also start topics:

```bash
hermes send --to "zulip:eng>Weekly report" "Report for week 44"
```

Target syntax: `zulip:<channel>` (default topic), `zulip:<channel>><topic>` or
`zulip:<channel>:<topic>` (channel by name or id), `zulip:dm:<user id>[,<user id>...]`. Replying
in a topic created this way continues with the posted message as context.

## Attachments

- **Inbound:** every file uploaded in a message (Zulip puts them inline as `/user_uploads/` links)
  is downloaded and handed to Hermes together — several images are one multi-image turn. Files over
  the size limit or beyond `max_attachments` are skipped and Hermes is told which.
- **Outbound:** files Hermes produces (`MEDIA:` paths) are uploaded and posted as **one** message per
  reply; Zulip shows a preview for each image.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Hermes ignores a channel | Is the bot subscribed, and is the channel in `zulip.channels` (when set)? Mention the bot to claim the topic. |
| `Zulip bot … already in use` | Another gateway or profile uses the same bot credentials. Give each profile its own bot. |
| `feature level … is too old` | Upgrade the Zulip server to 10.0 or newer. |
| Bot-to-bot hand-offs are ignored | Authorize the other bot (`ZULIP_ALLOWED_USERS`) and check `bot_hop_limit`. |
| Replies land in the wrong topic after a rename | Renames are tracked from live events; a rename while the gateway was down for more than 15 minutes starts a new session for the new name. |
