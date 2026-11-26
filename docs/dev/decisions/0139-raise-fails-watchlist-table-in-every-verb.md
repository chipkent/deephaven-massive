# 139. Raise fails a watchlist table on one symbol's problem, in every verb

Date: 2026-11-25

Status: accepted

## Question

With `raise` set for a per-symbol problem (a refused symbol, an unavailable symbol, an
unreadable row), should a watchlist read by `fetch()` or `history()` fail the whole table?
Before, it failed only that symbol, while `poll()` and streams failed the table.

## Options

1. Yes: fail the table, one rule across every verb.
2. No: keep the documented behavior, where only the failed symbols are `FAILED`.

## Ruling

Option 1. Under `raise`, a per-symbol problem fails the whole table in every verb. The
default, `log`, keeps every other symbol's rows and lists the problem in `m.problems()`.
Decided by the owner in the coordinating chat session on 2026-11-25.

## Reason

One meaning for `raise` everywhere. A user who doesn't want problem inputs to fail the table
uses `log`.

## Consequences

Under `raise`, a watchlist read by `fetch()` or `history()` stops every symbol's rows when
one symbol meets the problem, as `poll()` and streams already did.

## Rule

`docs/configuration.md`, "Under `raise`", and `on_error.refused_symbols`; decision 137 sets
the default.
