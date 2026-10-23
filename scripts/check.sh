#!/usr/bin/env bash
# The single verification entry point for this repository.
#
# Usage: scripts/check.sh [fast|full] [--no-fix]
#
#   fast      ruff check --fix, ruff format, mypy src tests
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

if ((fix)); then
    step "ruff check --fix" uv run ruff check --fix ${ruff_output[@]+"${ruff_output[@]}"} src tests
    step "ruff format" uv run ruff format src tests
else
    step "ruff check" uv run ruff check ${ruff_output[@]+"${ruff_output[@]}"} src tests
    step "ruff format --check" uv run ruff format --check ${ruff_output[@]+"${ruff_output[@]}"} src tests
fi
step "mypy" uv run mypy src tests

if [[ "$tier" == "full" ]]; then
    step "pytest with coverage" uv run pytest --cov --cov-report=term-missing:skip-covered
fi

echo "==> check.sh $tier: all steps passed"
