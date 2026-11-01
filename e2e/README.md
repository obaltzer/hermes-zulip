# End-to-end harness (local Zulip in Docker)

Secrets here (all gitignored) (`creds.json`, `docker-zulip/.env`, `gwhome/**/.env`) are
throwaway test credentials for the local server only.

- `docker-zulip/`: official docker-zulip, with `compose.override.yaml` to run Zulip Server 12.3 on
  `https://zulip.localhost:8443` (self-signed certificate, saved as `zulip-selfsigned.pem`).
  Project name `hermeszulip`.
  - Start: `docker compose -p hermeszulip up -d`
  - Stop: `docker compose -p hermeszulip down`
  - Remove data too: `down -v`
- `bootstrap_realm.py`: creates a realm with Admin, Bob and two bots (Hermes, Research). Writes
  `creds.json`:
  `docker cp bootstrap_realm.py hermeszulip-zulip-1:/tmp/ && docker exec -u zulip hermeszulip-zulip-1 bash -c '/home/zulip/deployments/current/manage.py shell < /tmp/bootstrap_realm.py' | grep CREDENTIALS= | sed 's/^CREDENTIALS=//' > creds.json`
- `run_e2e.py`: adapter-level, 17 checks. Two adapter instances plus a stand-in agent.
- `stub_llm.py` + `run_gateway_e2e.py`: full stack, 12 checks. A real `hermes gateway run` with two
  profiles (`gwhome/.hermes` = bot "Hermes", `gwhome/.hermes/profiles/research` = bot "Research")
  in one multiplexed process, against a deterministic OpenAI-compatible stub model.

Run from the project root, against the Hermes tree that `scripts/test.sh` prepares (an upstream
worktree with `zulip/` linked in; `PYTHONPATH` makes it win over the clone's editable install).
`SSL_CERT_FILE` makes Python trust the test certificate:

```bash
scripts/test.sh            # prepares .cache/hermes-<HERMES_VERSION>
export WT=$PWD/.cache/hermes-$(cat HERMES_VERSION) PY=$PWD/hermes-agent/.venv/bin/python
export SSL_CERT_FILE=$PWD/e2e/zulip-selfsigned.pem
HERMES_HOME=$(mktemp -d) PYTHONPATH=$WT $PY e2e/run_e2e.py e2e/creds.json

$PY e2e/stub_llm.py 18081 &
(cd e2e && HOME=$PWD/gwhome HERMES_HOME=$PWD/gwhome/.hermes PYTHONPATH=$WT $PY -m hermes_cli.main gateway run &)
PYTHONPATH=$WT $PY e2e/run_gateway_e2e.py e2e/creds.json
```
