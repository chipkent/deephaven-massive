# 91. Last-by on a live symbol table shows a row whenever its symbol is on the table

Date: 2026-11-02

Status: accepted

## Question

On a live symbol table, when does `last_by()` show a symbol's row: from its next tick, or as
soon as the symbol is on the table?

## Options

None recorded: one path was proposed.

## Ruling

`last_by()` keeps the latest row of every symbol the stream has delivered, and shows a symbol's
row while that symbol is on the symbol table, judged per Deephaven update cycle. A symbol on the
table when `last_by()` is called shows its latest row at once, not after its next trade. Remove
AAPL and its row disappears; add it back and AAPL shows again from its next row, since rows from
before it rejoined stay hidden. A symbol the stream has never carried shows nothing until its
first row arrives. From the design documents, condensed into records in PR #514, dated
2026-11-02; no owner words are on record, and the owner kept the rule on 2026-11-16.

## Reason

A quiet symbol's row is not hidden until its next tick.

## Consequences

Two rows received out of order within a cycle show in cycle order.

## Rule

The `last_by()` docstrings.
