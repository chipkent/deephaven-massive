#!/usr/bin/env bash
# Claude Code Stop hook.
#
# Before the agent ends its turn, run the fast check tier without rewriting files. On failure
# the output goes to stderr and the hook exits 2, so the agent keeps working to fix it. When
# the agent is already continuing because of this hook (stop_hook_active), it exits 0 to
# avoid an endless loop.
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

active="$(python3 -I -c '
import json, sys
print("yes" if json.load(sys.stdin).get("stop_hook_active") else "no")
')"
[[ "$active" == "no" ]] || exit 0

cd "$project_dir"
if ! output="$(./scripts/check.sh fast --no-fix 2>&1)"; then
    echo "scripts/check.sh fast --no-fix failed; fix these problems before stopping:" >&2
    echo "$output" >&2
    exit 2
fi
