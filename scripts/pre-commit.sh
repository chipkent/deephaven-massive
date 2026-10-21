#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "${ALLOW_MAIN_COMMIT:-0}" != "1" ]]; then
  BRANCH="$(git symbolic-ref --short HEAD 2>/dev/null || true)"
  if [[ "$BRANCH" == "main" || "$BRANCH" == "master" ]]; then
    echo "ERROR: Direct commits or pre-commit runs on '$BRANCH' are forbidden. Create a feature branch and open a Pull Request." >&2
    exit 1
  fi
fi

echo "==> [1/4] Running Ruff linter..."
uv run ruff check src tests

echo "==> [2/4] Running Ruff format check..."
uv run ruff format --check src tests

echo "==> [3/4] Running Mypy static type checker..."
uv run mypy src tests

echo "==> [4/4] Running Pytest with 100% branch coverage gate..."
uv run pytest

echo "==> All pre-commit checks passed!"
