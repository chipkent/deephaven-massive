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
#   - It creates the `spec-drift` label unless the repository has it (label names compare
#     without regard to case, as GitHub compares them).
#   - It finds the standing issue by its exact title among the issues, open or closed, that
#     carry the label; when several do, the oldest is the standing one.
#   - With no standing issue, it opens one with the label. Opening notifies the subscribers.
#   - With a closed one, it replaces the body with the report and reopens it, which notifies.
#   - With an open one whose body differs from the report, it replaces the body and posts one
#     short comment ("The drift report changed: N item(s), was M."), because GitHub notifies
#     no one of an edited body. The comment holds counts only, never vendor text.
#   - With an open one whose body already is the report, it does nothing.
#
# It never opens a second issue and touches nothing but that label and that issue. A report
# longer than an issue body may be (65536 characters) is cut at a line boundary with a note
# saying so. An empty report is refused, so a failed check can never blank the issue.
set -euo pipefail

title="Massive API drift"
label="spec-drift"
# GitHub refuses an issue body over 65536 characters; this leaves room for the note.
limit=65000
# Only this job labels issues spec-drift, so a thousand covers every one of them.
listed=1000

body=$(mktemp)
cut=$(mktemp)
current=$(mktemp)
trap 'rm -f "$body" "$cut" "$current"' EXIT
cat >"$body"
if [[ ! -s "$body" ]]; then
    echo "spec-drift-issue.sh: the report on standard input is empty" >&2
    exit 1
fi
if (($(wc -c <"$body") > limit)); then
    # Keep the whole lines that fit, then say where the rest is.
    head -c "$limit" "$body" | sed '$d' >"$cut"
    printf '\n_The report is longer than an issue allows; run the check in a source checkout for the rest._\n' >>"$cut"
    cp "$cut" "$body"
fi

# The number of drift items in a report: its list lines.
items() {
    grep -c '^- ' "$1" || true
}

# Each gh call stands alone in an assignment or a command, so `set -e` stops on its failure.
labels=$(gh label list --search "$label" --limit "$listed" --json name)
if ! jq -e --arg label "$label" \
    'any(.[]; (.name | ascii_downcase) == ($label | ascii_downcase))' <<<"$labels" >/dev/null; then
    gh label create "$label" --color D93F0B \
        --description "Massive changed its published API; see the standing drift issue"
fi

standing=$(
    gh issue list --label "$label" --state all --limit "$listed" --json number,title,state |
        jq -r --arg title "$title" \
            '[.[] | select(.title == $title)] | sort_by(.number) | first // empty | "\(.number) \(.state)"'
)
if [[ -z "$standing" ]]; then
    gh issue create --title "$title" --label "$label" --body-file "$body"
    exit 0
fi
read -r number state <<<"$standing"
issue=$(gh issue view "$number" --json body)
jq -r '.body' <<<"$issue" >"$current"
# GitHub may store the body with other line endings and without the final newline.
if [[ "$(tr -d '\r' <"$current")" == "$(tr -d '\r' <"$body")" ]]; then
    changed=0
else
    changed=1
    was=$(items "$current")
    gh issue edit "$number" --body-file "$body"
fi
if [[ "$state" != "OPEN" ]]; then
    gh issue reopen "$number"
elif ((changed)); then
    gh issue comment "$number" --body "The drift report changed: $(items "$body") item(s), was $was."
fi
