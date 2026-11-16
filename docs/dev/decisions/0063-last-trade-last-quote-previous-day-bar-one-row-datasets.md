# 63. Last trade, last quote, and the previous day's bar are datasets of their own

Date: 2026-10-29

Status: accepted

## Question

How does a user read Massive's one-row endpoints, the last trade, the last quote, and the
previous day's bar?

## Options

1. A `latest()` verb on existing datasets.
2. Datasets of their own, each with `fetch()` and `poll()`.

## Ruling

Massive's one-row endpoints are `m.stocks.last_trade`, `last_quote`, and `previous_day_bar`, and
the same datasets under each other class Massive serves them for, each with the ordinary
`fetch()` and `poll()`. There is no `latest()` method on `trades`, `quotes`, or `bars`. From the
design documents, condensed into records in PR #514, dated 2026-10-29; no owner words are on
record.

## Reason

`fetch()`, `history()`, and `poll()` each name how a table behaves: one read, a date range, a
repeating read. `latest()` would name what data it holds, which is a dataset's job.

## Consequences

The namespaces hold three more datasets, and the same verbs work on them as on every other.

## Rule

`docs/dev/public-api.md` and the datasets' docstrings.
