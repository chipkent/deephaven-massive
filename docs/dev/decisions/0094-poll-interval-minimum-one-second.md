# 94. Polling refuses an interval shorter than one second

Date: 2026-11-03

Status: accepted

## Question

Should `poll()` accept any interval, however short?

## Options

1. Accept any positive interval.
2. Refuse an interval shorter than one second.

## Ruling

`q.poll(interval=0.2)` raises `ValueError`; the minimum is one second. A user who wants faster
data uses a stream. From the design documents, condensed into records in PR #514, dated
2026-11-03; no owner words are on record.

## Reason

A shorter interval is almost always a typo that would send requests back to back.

## Consequences

A user who wants sub-second data uses a stream.

## Rule

The `poll()` docstrings, `interval`.
