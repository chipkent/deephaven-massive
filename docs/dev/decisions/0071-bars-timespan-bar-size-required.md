# 71. Every bars call names its bar size

Date: 2026-10-30

Status: accepted

## Question

Should `bars()` default its bar size?

## Options

1. Default to one size, such as a minute or a day.
2. Require `timespan` on every call.

## Ruling

The bars dataset call has no default size: `m.stocks.bars(["AAPL"],
timespan=dm.Timespan.MINUTE)` is required before any `history()`, `fetch()`, or stream method on
it, and leaving `timespan` out is a `TypeError`. From the design documents, condensed into
records in PR #514, dated 2026-10-30; no owner words are on record.

## Reason

No size suits most uses, and the size is the first thing a reader of the call needs.

## Consequences

Every `bars()` call is a little longer.

## Rule

The `bars()` docstrings.
