#!/usr/bin/env bash
# Claude Code PostToolUse hook for Edit and Write.
#
# When the edited file is a Python file in a checkout of this project, ruff fixes and formats
# it. Any diagnostics ruff cannot fix are written to stderr and the hook exits 2, which hands
# them back to the agent. Files outside a checkout, and files ruff excludes, are left alone.
set -euo pipefail

# The hook payload is parsed with the system python3 in isolated mode (-I), not `uv run`:
# parsing JSON needs no project dependencies, and uv would add a sync to every edit.
file_path="$(python3 -I -c '
import json, sys
print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))
')"

[[ "$file_path" == *.py && -f "$file_path" ]] || exit 0

# Resolve the checkout from the edited file, not CLAUDE_PROJECT_DIR, which does not follow
# agent worktrees.
root="$(git -C "$(dirname "$file_path")" rev-parse --show-toplevel 2>/dev/null)" || exit 0
[[ -f "$root/pyproject.toml" && -x "$root/scripts/check.sh" ]] || exit 0

cd "$root"
ruff=(uv run --quiet --project "$root" ruff)
# --force-exclude applies the project's exclude lists to a file named on the command line.
"${ruff[@]}" check --fix --force-exclude --quiet "$file_path" >/dev/null 2>&1 || true
"${ruff[@]}" format --force-exclude --quiet "$file_path" >/dev/null 2>&1 || true

if ! diagnostics="$("${ruff[@]}" check --force-exclude --output-format concise "$file_path" 2>&1)"; then
    echo "ruff found problems it cannot fix in $file_path:" >&2
    echo "$diagnostics" >&2
    exit 2
fi
