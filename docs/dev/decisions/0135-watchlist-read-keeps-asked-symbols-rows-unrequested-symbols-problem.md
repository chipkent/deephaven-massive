# 135. A read keeps only the rows of the symbols it asked for and lists the rest

Date: 2026-11-25

Status: accepted

## Question

Massive can answer a watchlist read's request with a row naming another symbol:
`m.options.open_interest_by_underlying()` asked for `SPX` answers with rows of `I:SPX`. A
request naming one symbol published every row under it; one naming several left such rows
out, unlisted in `m.problems()`. A dataset whose rows list their tickers had the same split,
in a plain `fetch()` too. What does a read do with such a row, at any size and form of
request?

## Options

1. As before: keep such rows for a request of one symbol, drop them for several.
2. Drop them at every size, listed nowhere.
3. Drop them at every size, each an `unrequested_symbols` problem with its own
   `dm.ErrorPolicy` setting.
4. Option 3, except that a request for one symbol by its own path keeps every row.

## Ruling

Option 3, decided by the owner in the coordinating chat session on 2026-11-25, which also
rejected option 4. One rule for every request, whatever its form (a symbol's own path, a list
parameter, a ticker list the rows carry) or size: each row goes to the asked symbol its
instrument column names, and a row naming no asked symbol is left out as an
`unrequested_symbols` problem, listed in `m.problems()` with the symbol and count, warned
once per table, `LOG` by default, and failing the table under `RAISE`.

## Reason

Two code paths are needless complexity: one symbol uses the implementation of several.
Every row left out is listed in `m.problems()` (decision 124).

## Consequences

A watchlist of one symbol, and a `fetch()` of a ticker-list dataset naming one ticker, lose
rows naming another symbol or none. A dataset whose rows spell the asked symbol another way
would lose every row: report it; never add a shortcut. So the catalog refuses a dataset read
by symbol with no `Symbol` column.

## Rule

`ErrorPolicy.unrequested_symbols`, and `src/deephaven_massive/_ingest/symbol_reads.py`,
"Attribution".
