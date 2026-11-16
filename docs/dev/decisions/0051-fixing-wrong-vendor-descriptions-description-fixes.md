# 51. A clearly wrong field description is corrected by a hand-written override

Date: 2026-10-28

Status: accepted

## Question

Massive's field descriptions become the column descriptions users read. What happens when one is
clearly wrong?

## Options

1. Pass every description through as Massive wrote it.
2. Correct a clearly wrong one by reasoned hand input, and keep terse text.

## Ruling

Massive's field descriptions become the column descriptions users read. One that is clearly
wrong, copied from another class or contradicted by Massive's own docs, is replaced by a
`DESCRIPTION_FIXES` entry that quotes the old text and says why; when Massive fixes its text,
the stale entry makes `scripts/refresh.py` stop with an error. Data itself is never corrected.
From the design documents, condensed into records in PR #514, dated 2026-10-28; no owner words
are on record.

## Reason

Descriptions are documentation, not data, so fixing one repairs no data.

## Consequences

Each correction is a `DESCRIPTION_FIXES` entry that quotes the text it replaces and says why; a
stale entry fails the refresh.

## Rule

`AGENTS.md`, "Supply Massive's data; never repair it", and `docs/dev/spec-refresh.md`.
