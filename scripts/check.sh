#!/usr/bin/env bash
# The single verification entry point for this repository.
#
# Usage: scripts/check.sh [fast|full] [--all] [--no-fix]
#
#   fast      uv sync --upgrade (newest versions), ruff check --fix, ruff format, mypy
#   full      fast, then the tests of the files the branch changes, without coverage (the default)
#   --all     with full: run the whole suite with coverage, whatever changed
#   --no-fix  report instead of rewrite: ruff check and ruff format --check (CI and git hooks)
#
# full lists the files the working tree changes (scripts/check_scope.py): run by hand, against
# origin/main; in the pre-push hook to a branch the remote already has, since that branch's
# remote tip, so a push runs only the tests its own commits need. Staged and untracked files
# count; pre-commit hides unstaged edits to tracked files while the hook runs.
# It runs each changed test file, the unit test that mirrors each changed module, and the
# documentation tests when a user page, an image under docs/, or the tester guide changed;
# a change with none of these runs no tests. CI runs the whole suite with coverage on every
# pull request. It prints the tests it chose and why.
#
# pytest runs on as many workers as the machine has room for (scripts/worker_count.py): two
# fewer than its CPUs, and one per 3 GiB of available memory, since each worker starts its own
# JVM. CHECK_WORKERS overrides the count. A targeted run uses no more workers than it has
# test files, and one worker runs in pytest's own process.
#
# With CI=true, ruff reports in GitHub annotation format. The script stops at the first
# failing step and exits non-zero.
set -euo pipefail

usage() {
    sed -n '4,9s/^# \{0,1\}//p' "$0"
}

tier=full
fix=1
all=0
for arg in "$@"; do
    case "$arg" in
        fast | full) tier="$arg" ;;
        --no-fix) fix=0 ;;
        --all) all=1 ;;
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
    step "check_scope.py doctests" uv run python -m doctest scripts/check_scope.py
    step "worker_count.py doctests" uv run python -m doctest scripts/worker_count.py
    workers=$(uv run python scripts/worker_count.py)
    tests=()
    if ((all)); then
        scope=full
        echo "check.sh: scope full: --all runs the whole suite"
    else
        read -r scope tests_line <<< "$(uv run python scripts/check_scope.py)"
        read -r -a tests <<< "${tests_line:-}"
        if ((${#tests[@]} > 0 && ${#tests[@]} < workers)); then
            workers=${#tests[@]}
            echo "check.sh: one test worker per test file: $workers"
        fi
    fi
    # One worker runs in pytest's own process (-n 0): xdist would start a second JVM for it.
    if ((workers == 1)); then
        workers=0
    fi
    case "$scope" in
        # -n stays out of addopts, so a plain `uv run pytest` runs in one process for debuggers.
        full) step "pytest with coverage" uv run pytest -n "$workers" --cov --cov-report=term-missing:skip-covered ;;
        # pytest exits 5 when the files hold no test the default run selects, such as live
        # tests only; that is no failure.
        tests) step "pytest of the changed files" uv run pytest -n "$workers" "${tests[@]}" || {
            status=$?
            if ((status != 5)); then
                exit "$status"
            fi
            echo "check.sh: the changed test files hold no test the default run selects"
        } ;;
        lint) ;;
        *)
            echo "check.sh: unknown scope '$scope' from scripts/check_scope.py" >&2
            exit 1
            ;;
    esac
    tier="full ($scope)"
fi

echo "==> check.sh $tier: all steps passed"
