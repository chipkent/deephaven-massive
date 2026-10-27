#!/usr/bin/env bash
# Keep the one standing "Massive API drift" issue up to date with a drift report.
#
# Usage: scripts/spec-drift-issue.sh < report.md
#
# The weekly drift workflow (.github/workflows/spec-drift.yml) runs it when
# `python -m deephaven_massive._massive.refresh --check` finds drift, with that report on
# standard input. It needs gh, authenticated through GH_TOKEN for the repository GH_REPO names,
# and jq.
#
#   - It creates the `spec-drift` label when the repository lacks it.
#   - It finds the standing issue by its exact title among the issues, open or closed, that
#     carry the label; when several do, the oldest is the standing one.
#   - It replaces that issue's body with the report and reopens the issue if it was closed,
#     or, when there is none, opens it with the label.
#
# It never opens a second issue, comments, or touches anything but that label and that issue.
# A report longer than an issue body may be (65536 characters) is cut at a line boundary with
# a note saying so. An empty report is refused, so a failed check can never blank the issue.
set -euo pipefail

title="Massive API drift"
label="spec-drift"
# GitHub refuses an issue body over 65536 characters; this leaves room for the note.
limit=65000

body=$(mktemp)
trap 'rm -f "$body"' EXIT
cat >"$body"
if [[ ! -s "$body" ]]; then
    echo "spec-drift-issue.sh: the report on standard input is empty" >&2
    exit 1
fi
if (($(wc -c <"$body") > limit)); then
    cut=$(mktemp)
    # Keep the whole lines that fit, then say where the rest is.
    head -c "$limit" "$body" | sed '$d' >"$cut"
    printf '\n_The report is longer than an issue allows; run the check in a source checkout for the rest._\n' >>"$cut"
    mv "$cut" "$body"
fi

# Each gh call stands alone in an assignment or a command, so `set -e` stops on its failure.
labels=$(gh label list --search "$label" --limit 100 --json name)
if ! jq -e --arg label "$label" 'any(.[]; .name == $label)' <<<"$labels" >/dev/null; then
    gh label create "$label" --color D93F0B \
        --description "Massive changed its published API; see the standing drift issue"
fi

standing=$(
    gh issue list --label "$label" --state all --limit 100 --json number,title,state |
        jq -r --arg title "$title" \
            '[.[] | select(.title == $title)] | sort_by(.number) | first // empty | "\(.number) \(.state)"'
)
if [[ -z "$standing" ]]; then
    gh issue create --title "$title" --label "$label" --body-file "$body"
    exit 0
fi
read -r number state <<<"$standing"
gh issue edit "$number" --body-file "$body"
if [[ "$state" != "OPEN" ]]; then
    gh issue reopen "$number"
fi
