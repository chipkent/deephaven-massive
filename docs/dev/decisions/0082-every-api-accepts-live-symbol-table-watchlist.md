# 82. Every call that takes symbols accepts a live table of symbols and follows it

Date: 2026-11-02

Status: accepted

## Question

Where a call takes symbols, may a user pass a live Deephaven table of symbols, and is it read
once or followed?

## Options

None recorded: one path was proposed.

## Ruling

`fetch()`, `history()`, `poll()`, and the stream methods all accept a live Deephaven table of
symbols. It is never refused and never read just once: an added symbol is fetched or added to
the stream, and a removed one leaves. With `wait=True`, `fetch()` and `history()` return once
every symbol then on the table has been read, then keep following it. `m.symbol_reads()` shows
each read. The owner ruled on 2026-10-30 that every method supports loading new symbols as they
arrive, and again on 2026-11-02; recorded in PR #514. This absorbs decision 4.

## Reason

What a program loads should change with its symbol table, with no restart.

## Consequences

Every verb carries the work of following a table: `history()` and `fetch()` read an added
symbol over the same range, and `m.symbol_reads()` shows each read.

## Rule

The query methods' docstrings, `symbols`.
