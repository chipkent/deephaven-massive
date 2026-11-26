# 137. One symbol's problem never stops another symbol's data

Date: 2026-11-25

Status: partly superseded: a problem of one symbol never stopping another symbol's rows under `raise`, by [139](0139-raise-fails-watchlist-table-in-every-verb.md)

## Question

Massive refuses one of a table's symbols, fails its request, or sends rows the library
can't read. Before, that failed the whole table: `m.stocks.last_quote(watch).poll(5)` stopped
publishing AAPL once the watchlist added a symbol the plan lacks. What happens to the other
symbols, and is a refused symbol asked again?

## Options

1. As before: a refusal or failure of one symbol fails the call or table.
2. Each symbol's problem is its own, in every verb, listed in `m.problems()`, with a new
   `refused_symbols` setting, `LOG` by default.
3. Option 2, but reading a superset of the watchlist and filtering each table to it.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-25. A problem of
one symbol never stops another symbol's rows. The table requests and subscribes exactly its
watchlist. A refusal is final: the symbol is not asked again until it is deleted and added
back, or the call is made again. A refused request naming several symbols is searched once to
find the refused ones. Every symbol whose rows stop shows it in its own `m.problems()` row,
and its `m.symbol_reads()` state where the verb has one. Snapshot error rows move from the
data to `m.problems()`, and the `Error` and `Message` columns of `snapshots`,
`indices.snapshots`, and `options.snapshots` go. Futures exchanges keep failing the table
(decision 107).

## Reason

Symbols that break each other make the library too brittle to use. A refusal won't change
until the plan does, so asking again wastes requests. Errors belong in the standard reporting
channels.

## Consequences

A table runs with symbols missing, so a user must watch `m.problems()`. A refusal that was
a passing glitch keeps its symbol out until re-added. A shared request's search costs extra
requests once.

## Rule

AGENTS.md, "A data table holds only what Massive sent, read whole", and
`docs/configuration.md`, `on_error.refused_symbols`.
