#!/usr/bin/env bash
# The single verification entry point for this repository.
#
# Usage: scripts/check.sh [fast|full] [--no-fix]
#
#   fast      refresh massive to the latest deephaven-fixes, ruff check --fix, ruff format, mypy
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

# The SDK fork is always the latest `deephaven-fixes` commit (DESIGN § 19).
# Re-lock only `massive` to the branch head, so every `uv run` below syncs the environment to
# it; every other dependency stays as locked. The refreshed uv.lock serves this run only: the
# file as it was is restored on exit, so the working tree is never left dirty, and the `massive`
# commit recorded in the committed lock may go stale without anyone bumping it.
lock_backup="$(mktemp)"
cp uv.lock "$lock_backup"
restore_lock() {
    cp "$lock_backup" uv.lock
    rm -f "$lock_backup"
}
trap restore_lock EXIT
step "uv lock --upgrade-package massive (latest deephaven-fixes)" \
    uv lock --quiet --upgrade-package massive
step "installed massive commit" uv run --quiet python -c \
    "import json, importlib.metadata as m; print(json.loads(m.distribution('massive').read_text('direct_url.json'))['vcs_info']['commit_id'])"

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
