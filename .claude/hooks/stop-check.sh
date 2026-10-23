#!/usr/bin/env bash
# Claude Code Stop hook.
#
# Before the agent ends its turn, run the fast check tier without rewriting files. On failure
# the output goes to stderr and the hook exits 2, so the agent keeps working to fix it. When
# the agent is already continuing because of this hook (stop_hook_active), it exits 0 to
# avoid an endless loop.
set -euo pipefail

# The hook payload is parsed with the system python3 in isolated mode (-I), not `uv run`:
# parsing JSON needs no project dependencies, and uv would add a sync to every turn.
read -r active cwd < <(python3 -I -c '
import json, sys
payload = json.load(sys.stdin)
print("yes" if payload.get("stop_hook_active") else "no", payload.get("cwd", ""))
')
[[ "$active" == "no" ]] || exit 0

# Resolve the checkout from the session's working directory (the payload's cwd), because
# CLAUDE_PROJECT_DIR does not follow agent worktrees.
root="$(git -C "${cwd:-.}" rev-parse --show-toplevel 2>/dev/null || echo "${CLAUDE_PROJECT_DIR:-}")"
[[ -n "$root" && -x "$root/scripts/check.sh" ]] || exit 0

cd "$root"
if ! output="$(./scripts/check.sh fast --no-fix 2>&1)"; then
    echo "scripts/check.sh fast --no-fix failed; fix these problems before stopping:" >&2
    echo "$output" >&2
    exit 2
fi
