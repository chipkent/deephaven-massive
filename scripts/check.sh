#!/usr/bin/env bash
# The single verification entry point for this repository.
#
# Usage: scripts/check.sh [fast|full] [--no-fix]
#
#   fast      uv sync --upgrade (newest versions), ruff check --fix, ruff format, mypy
#   full      fast, then pytest with coverage (the default)
#   --no-fix  report instead of rewrite: ruff check and ruff format --check (CI and git hooks)
#
# With CI=true, ruff reports in GitHub annotation format. The script stops at the first
# failing step and exits non-zero.
set -euo pipefail

usage() {
    sed -n '4,8s/^# \{0,1\}//p' "$0"
}

tier=full
fix=1
for arg in "$@"; do
    case "$arg" in
        fast | full) tier="$arg" ;;
        --no-fix) fix=0 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "check.sh: unknown argument '$arg'" >&2
            usage >&2
            exit 2
            ;;
    esac
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ruff_output=()
if [[ "${CI:-}" == "true" ]]; then
    ruff_output=(--output-format github)
fi

step() {
    echo "==> $1"
    shift
    "$@"
}

# uv.lock is not committed, so every run resolves the newest versions. Offline, a local run
# keeps what is installed and says so; CI must always test the newest versions, so there a
# failed upgrade is an error.
echo "==> uv sync --upgrade"
if ! uv sync --quiet --upgrade; then
    if [[ "${CI:-}" == "true" ]]; then
        echo "check.sh: uv sync --upgrade failed" >&2
        exit 1
    fi
    echo "check.sh: WARNING: uv sync --upgrade failed (offline or a resolution error); using the installed versions" >&2
fi
# The environment is synced; the steps below run in it as is.
export UV_NO_SYNC=1

if ((fix)); then
    step "ruff check --fix" uv run ruff check --fix ${ruff_output[@]+"${ruff_output[@]}"} src tests scripts
    step "ruff format" uv run ruff format src tests scripts
else
    step "ruff check" uv run ruff check ${ruff_output[@]+"${ruff_output[@]}"} src tests scripts
    step "ruff format --check" uv run ruff format --check ${ruff_output[@]+"${ruff_output[@]}"} src tests scripts
fi
step "mypy" uv run mypy src tests scripts

if [[ "$tier" == "full" ]]; then
    step "pytest with coverage" uv run pytest --cov --cov-report=term-missing:skip-covered
fi

echo "==> check.sh $tier: all steps passed"
