# hermes-zulip

A Zulip platform plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent), maintained
outside Hermes: no fork, no core changes.

- **One Zulip topic = one Hermes session.** Renames, resolves, moves and splits are followed.
- **Hermes can open topics** (a `TOPIC:` reply line), and cron / `hermes send` can target them.
- **One bot user per Hermes profile**, with mention/stickiness rules so several bots share a channel.
- **Multiple images and files per message**, both directions.

Design and rationale: [DESIGN.md](DESIGN.md). User guide: [docs/zulip.md](docs/zulip.md).

## Layout

```
zulip/            the plugin — install as <hermes>/plugins/platforms/zulip/
tests/            pytest behaviour tests against a fake Zulip server
e2e/              optional end-to-end harness against a local Docker Zulip
HERMES_VERSION    the Hermes release this is tested against (what Flexo deploys)
scripts/test.sh   runs tests/ inside an unmodified upstream Hermes checkout
```

## Install

Copy (or bake into your image) `zulip/` as `plugins/platforms/zulip/` of the Hermes install. Hermes
loads it as a bundled platform; it stays dormant until `ZULIP_SITE`, `ZULIP_EMAIL` and
`ZULIP_API_KEY` are set. The path matters: the plugin imports itself as `plugins.platforms.zulip`.

Flexo bakes a pinned commit into its image; see `calculon/stacks/flexo/hermes.Dockerfile` in
`obaltzer/infra`.

## Develop

```bash
git clone https://github.com/NousResearch/hermes-agent.git hermes-agent   # plain upstream, gitignored
(cd hermes-agent && uv venv && uv pip install -e '.[dev]')                 # or however upstream says
scripts/test.sh              # against HERMES_VERSION
scripts/test.sh main         # against upstream main
```

Supported range: Hermes **v0.22.5** to current `main`. Newer-core features are used when present
(`MessageEvent.media_filenames`, `clean_attachment_filename`) and fall back otherwise.

When Flexo moves to a new Hermes release: bump `HERMES_VERSION`, run `scripts/test.sh`, commit, and
pin the new commit in Flexo's Dockerfile.
