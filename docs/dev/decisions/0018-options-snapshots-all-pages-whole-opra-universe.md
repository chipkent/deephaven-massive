# 18. Option snapshots accept ALL, though it pages every option contract

Date: 2026-10-23

Status: accepted

## Question

Should `m.options.snapshots(dm.ALL)` be allowed, when it pages through the whole OPRA universe
and is slow?

## Options

1. Refuse `ALL` for options, unlike every other class.
2. Allow it and document that it is slow.

## Ruling

`m.options.snapshots(dm.ALL)` is allowed, like `ALL` on every other asset class. It is
documented as slow: it pages through every listed option contract under Massive's rate limits.
From the design documents, condensed into records in PR #514, dated 2026-10-23; no owner words
are on record.

## Reason

One uniform rule for `snapshots()`, with no special case for options.

## Consequences

A user who asks for every option contract waits for many pages and Massive's rate limits.

## Rule

The docstring of `m.options.snapshots` and its dataset page in `docs/datasets/`.
