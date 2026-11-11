#!/usr/bin/env bash
# The single verification entry point for this repository.
#
# Usage: scripts/check.sh [fast|full] [--all] [--no-fix]
#
#   fast      uv sync --upgrade (newest versions), ruff check --fix, ruff format, mypy
#   full      fast, then the tests the branch's changes need, by CI's path rules (the default)
#   --all     with full: run the whole suite with coverage, whatever changed
#   --no-fix  report instead of rewrite: ruff check and ruff format --check (CI and git hooks)
#
# full lists the files the working tree changes (scripts/check_scope.py): run by hand, against
# origin/main; in the pre-push hook to a branch the remote already has, since that branch's
# remote tip, so a push runs only the tests its own commits need. Staged and untracked files
# count; pre-commit hides unstaged edits to tracked files while the hook runs.
# When CI would run the tests for the change, full runs pytest with coverage on up to 4
# workers; when CI would not, full runs only the documentation tests if a user page, an image
# under docs/, an API reference page, the site configuration, or the tester guide changed, and
# no tests otherwise. It also builds the documentation site in strict mode, in an environment
# of its own (.venv-site), when one of the site's sources changed, as CI's site job does. It
# prints the scope it chose and why.
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
    scope=full
    site=site
    if ((all)); then
        echo "check.sh: scope full: --all runs the whole suite and builds the documentation site"
    else
        step "check_scope.py doctests" uv run python -m doctest scripts/check_scope.py
        read -r scope site <<< "$(uv run python scripts/check_scope.py)"
    fi
    case "$scope" in
        # -n stays out of addopts, so a plain `uv run pytest` runs in one process for debuggers.
        full) step "pytest with coverage" uv run pytest -n auto --maxprocesses=4 --cov --cov-report=term-missing:skip-covered ;;
        docs) step "pytest of the documentation" uv run pytest tests/test_docs.py tests/live/tester/test_examples.py ;;
        lint) ;;
        *)
            echo "check.sh: unknown scope '$scope' from scripts/check_scope.py" >&2
            exit 1
            ;;
    esac
    case "$site" in
        # The site generator is not a dev dependency, so it gets an environment of its own,
        # synced here to the newest releases like the project's; the site reads the package
        # source without importing it.
        site) step "build_site.sh" env -u UV_NO_SYNC UV_PROJECT_ENVIRONMENT=.venv-site \
            scripts/build_site.sh --quiet --upgrade --only-group site ;;
        no-site) ;;
        *)
            echo "check.sh: unknown site decision '$site' from scripts/check_scope.py" >&2
            exit 1
            ;;
    esac
    tier="full ($scope, $site)"
fi

echo "==> check.sh $tier: all steps passed"
