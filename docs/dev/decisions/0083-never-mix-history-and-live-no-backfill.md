# 83. History and live data are never mixed by the library

Date: 2026-11-02

Status: accepted

## Question

Should a stream start with history, for example `last_by()` seeded from REST, or a verb that
backfills a live table?

## Options

1. Seed or backfill streams from REST.
2. Never mix history and live data; the user merges them.

## Ruling

No method seeds or backfills a live table from REST. A user who wants history followed by live
rows merges the two tables themselves: `merge([q.history(start, end), q.append_only()])`. From
the design documents, condensed into records in PR #514, dated 2026-11-02; no owner words are on
record. This absorbs decision 3.

## Reason

The library cannot stitch the seam without duplicating or missing ticks at the boundary, and the
user can choose the seam; the owner confirmed this reason on 2026-11-16.

## Consequences

A user who wants history before live rows writes the merge themselves.

## Rule

`README.md`, the history-then-live example.
