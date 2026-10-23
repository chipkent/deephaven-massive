#!/usr/bin/env bash
# Claude Code PostToolUse hook for Edit and Write.
#
# When the edited file is a Python file in this project, ruff fixes and formats it. Any
# diagnostics ruff cannot fix are written to stderr and the hook exits 2, which hands them
# back to the agent. Files outside the project, and files ruff excludes, are left alone.
set -euo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

file_path="$(python3 -I -c '
import json, sys
print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))
')"

case "$file_path" in
    "$project_dir"/*.py) ;;
    *) exit 0 ;;
esac
[[ -f "$file_path" ]] || exit 0

cd "$project_dir"
# --force-exclude applies the project's exclude lists to a file named on the command line.
uv run --quiet ruff check --fix --force-exclude --quiet "$file_path" >/dev/null 2>&1 || true
uv run --quiet ruff format --force-exclude --quiet "$file_path" >/dev/null 2>&1 || true

if ! diagnostics="$(uv run --quiet ruff check --force-exclude --output-format concise "$file_path" 2>&1)"; then
    echo "ruff found problems it cannot fix in $file_path:" >&2
    echo "$diagnostics" >&2
    exit 2
fi
