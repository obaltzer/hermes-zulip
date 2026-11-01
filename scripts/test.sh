#!/bin/bash
# Run the plugin's tests inside an unmodified upstream Hermes checkout.
#
#   scripts/test.sh [ref] [pytest args...]
#
# ref defaults to the release in HERMES_VERSION (what Flexo deploys). The
# tests import `plugins.platforms.zulip` and `tests.gateway.*`, so they only
# run from within a hermes-agent tree: this makes a detached worktree of
# ./hermes-agent (a plain clone of NousResearch/hermes-agent) at <ref> under
# .cache/, symlinks zulip/ and tests/ into it, and runs pytest with the
# clone's venv. Nothing is committed to the clone.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
ref="${1:-$(cat "$root/HERMES_VERSION")}"
shift || true
clone="${HERMES_AGENT:-$root/hermes-agent}"
py="$clone/.venv/bin/python"
wt="$root/.cache/hermes-$ref"

[ -x "$py" ] || { echo "no venv at $clone/.venv — set up the clone first" >&2; exit 1; }
git -C "$clone" rev-parse -q --verify "$ref^{commit}" >/dev/null \
    || git -C "$clone" fetch -q origin tag "$ref" 2>/dev/null \
    || git -C "$clone" fetch -q origin "$ref"
if [ ! -d "$wt" ]; then
    git -C "$clone" worktree add -q --detach "$wt" "$ref"
fi

ln -sfn "$root/zulip" "$wt/plugins/platforms/zulip"
for f in "$root"/tests/*.py; do
    ln -sf "$f" "$wt/tests/gateway/$(basename "$f")"
done

cd "$wt"
exec "$py" -m pytest -q -p no:cacheprovider \
    tests/gateway/test_zulip_adapter.py tests/gateway/test_zulip_routing.py "$@"
