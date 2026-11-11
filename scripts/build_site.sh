#!/usr/bin/env bash
# Build the documentation site into site/, and fail on anything that would publish it broken.
#
# Usage: scripts/build_site.sh [uv run options]
#
#   The options choose the environment, such as `--only-group site`; the site generator and its
#   API reference plugin are in the site dependency group (docs/dev/docs-site.md).
#
# Zensical's strict mode fails on a broken link or anchor in a page, but not on a problem in a
# docstring the API reference renders: griffe, which reads the docstrings, only prints a
# warning, such as for a malformed Args section. So the build also fails on any warning line,
# and on a reST `::` marker left as text, which means a docstring's example did not become a
# code block. --clean rebuilds every page, since a cached page would hide both.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

log=$(mktemp)
trap 'rm -f "$log"' EXIT

# The extension that turns the docstrings' `::` examples into code blocks checks its own
# examples first.
uv run "$@" python -m doctest scripts/site_literal_blocks.py
uv run "$@" zensical build --strict --clean 2>&1 | tee "$log"

# Zensical colors its output; the patterns match the text without the color codes.
warnings=$(sed $'s/\x1b\\[[0-9;]*m//g' "$log" | grep -E '^(griffe|WARNING|Warning)\b' || [ $? -eq 1 ])
if [ -n "$warnings" ]; then
    echo "build_site.sh: the build printed warnings; fix each one:" >&2
    echo "$warnings" >&2
    exit 1
fi
literal=$(grep -rl '<p>::</p>' site || [ $? -eq 1 ])
if [ -n "$literal" ]; then
    echo "build_site.sh: a docstring's '::' example marker is shown as text on these pages:" >&2
    echo "$literal" >&2
    exit 1
fi
echo "build_site.sh: the site is in site/"
