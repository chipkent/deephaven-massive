# Getting started with deephaven-massive

`deephaven-massive` brings market data from [Massive.com](https://massive.com) (formerly
Polygon.io) into [Deephaven](https://deephaven.io) as ordinary tables. A stream of trades
becomes a table that updates as each trade arrives, and a year of daily prices becomes a
table you can sort, filter, join, and plot like any other. The library runs inside a
Deephaven server; you call it from the Deephaven console or from a script the server runs.

These pages take you from an empty machine to live tables, in order. Each one builds on the
one before it, and every example is code you can paste into the console.

1. [Install and configure](install.md): install the library, start Deephaven, and give the
   library your Massive API key and, if you have them, your flat-file keys.
2. [Your first tables](first-tables.md): a live table, a historical table, and how to find the
   dataset you want.
3. [Verbs and table shapes](table-shapes.md): how `fetch`, `poll`, `history`, `blink`,
   `append_only`, `ring`, and `last_by` decide what kind of table you get.
4. [Reactive watchlists](watchlists.md): drive a stream from a table of symbols that you, or
   another table, can change while it runs.
5. [Plans and entitlement errors](plans.md): what each kind of Massive plan can and cannot
   see, and what the library's errors tell you to do.
6. [Reading `m.streams()` and `m.connections()`](diagnostics.md): the two live tables that
   show what the library is doing.
7. [Releasing tables and closing a session](releasing.md): how live tables stop, and how to
   keep their data afterwards.
8. [Troubleshooting](troubleshooting.md): common problems and their fixes.

The [column reference](../datasets/) lists every dataset's columns and what each one means.
The same descriptions appear in the Deephaven web UI when you hover over a column header, and
in the console through any query's `columns()` method.

## Words used throughout

- **Massive** is the market data vendor. Its data reaches the library over three channels,
  which these pages call *transports*: **REST**, web requests that return a page of data at a
  time; **WebSocket**, a connection that streams each trade or quote as it happens; and
  **flat files**, one compressed file per day holding every symbol, which Massive publishes
  the morning after.
- A **plan** is what you pay Massive for. Massive sells a separate plan for each of stocks,
  options, indices, currencies (forex and crypto together), and futures, so one account can
  hold a different plan for each.
- A **symbol** is Massive's exact name for an instrument: `AAPL` for a stock, `X:BTCUSD` for a
  crypto pair, `C:EURUSD` for a currency pair, `I:SPX` for an index.
- A **live table** is a Deephaven table that changes while you watch it. A **static table**
  never changes once you have it.
- `m` is the name these pages give the session object, the one you create with
  `dm.MassiveSession()`.
