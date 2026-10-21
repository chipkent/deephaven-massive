#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK_PATH="$ROOT_DIR/.git/hooks/pre-commit"

cat > "$HOOK_PATH" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

BRANCH="$(git symbolic-ref --short HEAD 2>/dev/null || true)"
if [[ "$BRANCH" == "main" || "$BRANCH" == "master" ]]; then
  echo "ERROR: Direct commits to '$BRANCH' are forbidden. Create a feature branch and open a Pull Request." >&2
  exit 1
fi

ROOT_DIR="$(git rev-parse --show-toplevel)"
exec "$ROOT_DIR/scripts/pre-commit.sh"
EOF

chmod +x "$ROOT_DIR/scripts/pre-commit.sh" "$HOOK_PATH"
echo "Installed pre-commit hook at $HOOK_PATH -> scripts/pre-commit.sh"
