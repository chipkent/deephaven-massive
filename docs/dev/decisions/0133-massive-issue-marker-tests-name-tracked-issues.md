# 133. A check of a Massive problem names the GitHub issue it tracks in a marker

Date: 2026-11-24

Status: accepted

## Question

Massive has confirmed problems in its data and documentation that it has not yet fixed, each
tracked by an open GitHub issue here, such as an index's previous-day bar that differs from its
range day bar. A test can check each one and flip the day Massive fixes it, but `AGENTS.md`
allowed only the `verifies` and `settles` markers to cite numbers, so a test could not say which
issue its pass closes. May a test name the GitHub issue it tracks, through one marker?

## Options

1. Give each problem a verification item that names its issue, and mark its check `verifies`.
2. Mark each check `massive_issue(N)` with the issue's number; the live run and the CI checks
   read the marker and report, by issue, which problems are still present and which Massive
   fixed. No verification items are written for these problems.

## Ruling

Option 2: the owner so ruled in a chat session on 2026-11-24, with the edit to
`AGENTS.md` that adds the `massive_issue` marker to the markers allowed to cite numbers. The
same session included checks for the two problems first left out of scope, the missing
schedules of Trade at Settlement products and the session end date of streamed futures trades,
so that every open tracked problem has a check.

## Reason

The owner needs to know when Massive fixes a problem, and the check must say which issue its
pass closes.

## Consequences

A pass of such a check fails its run until someone closes the issue and removes or changes the
check. The issue numbers live in test code, so a renumbered or moved issue needs its markers
changed by hand.

## Rule

`AGENTS.md`, Code and tests, "No decision or planning references"; `docs/live-tests.md`.
