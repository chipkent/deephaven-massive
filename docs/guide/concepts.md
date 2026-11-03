# Concepts

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

This page explains how Massive's data becomes Deephaven tables: the columns every table shares,
how dates and times are written, and how long a live table keeps updating.

<a id="columns"></a>

## Columns every table shares

Every table names the same things the same way, so tables from different datasets join without
renaming.

| Column | What it holds |
|---|---|
| `Symbol` | The instrument, spelled exactly as Massive spells its ticker, such as `AAPL` or `X:BTCUSD` |
| `Timestamp` | When a trade or quote happened, as a Deephaven `Instant` |
| `TimestampStart`, `TimestampEnd` | A bar's window, in place of `Timestamp`: see [bars](history.md#bars) |
| `Date` | The trading day, as `yyyy-MM-dd` text |
| `TimestampInsert` | When the library received the row |

Every calendar date in every table is `yyyy-MM-dd` text, so a date from one table joins to a
date from any other.

Tables of one kind from two asset classes, such as stock and forex quotes, can have different
columns. To stack them, see [stack two asset classes](recipes.md#stack-asset-classes).

### Data the library cannot read

You get every row the library can read. The rest:

| What Massive sent | What you see |
|---|---|
| A value that does not fit its column | A null in that cell, one WARNING in the log per dataset and field, and a `?` entry in the table's `SchemaDrift` column in [`m.streams()`](troubleshooting.md#diagnostics) |
| A symbol in a spelling the library cannot read | A null `Symbol`, with Massive's text in the `RawSymbol` column |
| A row without a symbol or a time | No row, and a line in the log |

Each cause of a skipped symbol or row has its own `dm.MassiveConfig` setting, named for the
cause, such as `unreadable_symbols`. Each takes one of three `dm.OnError` values:

| Value | The table | The log |
|---|---|---|
| `dm.OnError.LOG`, the default | Skips the symbol or row and keeps running | One WARNING per distinct value |
| `dm.OnError.IGNORE` | Skips the symbol or row and keeps running | DEBUG lines only |
| `dm.OnError.RAISE` | The read raises, or the live table stops with the error | The error |

```python
strict = dm.MassiveSession(dm.MassiveConfig(unreadable_symbols=dm.OnError.RAISE))
```

`strict` is a session whose reads fail on a symbol they cannot read, instead of keeping the row
with a null `Symbol`.

A problem that affects every row, such as a rejected API key, fails the table whatever these
settings say.

<a id="time"></a>

## Dates, instants, and the market day

Tables and arguments use two kinds of time:

- **An instant** is an exact moment, such as a trade's `Timestamp`. It is a Deephaven `Instant`.
- **A date** is a calendar day in the market's own time zone, written as `yyyy-MM-dd` text.

The market's day depends on the asset class:

| Asset class | A date is the day in |
|---|---|
| Stocks, options, indices | New York |
| Forex, crypto | UTC |
| Futures | The trading session that closes at 17:00 Chicago time on that date. It opens at 17:00 Chicago time the evening before, or earlier after a holiday. |

A stock's `Date` and a crypto pair's `Date` for the same calendar day cover different hours: the
crypto day starts at midnight UTC, four or five hours before New York's. To compare the same
hours across asset classes, build your figures from minute bars over one window you choose.

To pass dates and instants as arguments, see [`start` and `end`](arguments.md#start-and-end).

<a id="live-tables"></a>

## How long a live table runs

A live table keeps updating while anything uses it:

- the variable you assigned it to;
- a table you derived from it, such as `trades.where(...)`;
- a panel showing it in the Deephaven IDE.

Static tables never update, so they hold nothing open.

### Stop one table: `m.release(table)`

Deleting a variable (`del quotes`) stops the stream only when Python's and Java's garbage
collectors reclaim the table, which can take minutes on a quiet server. `m.release(table)`
drops the library's hold at once:

```python
quotes = m.stocks.quotes(["AAPL", "MSFT"]).last_by()
m.release(quotes)
```

After the release:

- If nothing else uses the table, the library unsubscribes the stream, stops the poll, or stops
  the download.
- If a derived table or an open panel still uses it, the table keeps updating, and its row in
  `m.streams()` reads `RELEASED` until the last user goes.

`m.release()` accepts:

- any live table a verb of the session returned, including a `history()` or `fetch()` that
  follows a [watchlist](watchlists.md);
- a background download (`wait=False`);
- the [diagnostics tables](troubleshooting.md#diagnostics).

Releasing a table twice does nothing. A table you derived yourself raises `ValueError`: release
the live table it came from.

### Stop everything: `m.close()`

`m.close()` stops every stream, poll, and download of the session. Every live table of the
session, and every table derived from one, stops updating and shows this error:

```text
MassiveSession #1 was closed; this table no longer updates
```

The table keeps its rows, but Deephaven cannot build a new live table on it. To keep working
with the rows, take a static copy with `snapshot()`:

```python
trades = m.stocks.trades("AAPL").append_only()
m.close()
kept = trades.snapshot()
```

`kept` is a static table of the trades received before the close.

After `close()`, verbs and the diagnostics tables raise `dm.MassiveError`. Building queries and
`m.release()` still work.

A script that reads only static tables can close its session with a `with` block:
`with dm.MassiveSession() as m:`. A session that nothing references any more closes by itself;
with the [debug log](troubleshooting.md#the-log) on, an INFO line says so.

### When the connection to Massive drops

The library reconnects by itself. Meanwhile, each live table keeps its last rows, and its `Error`
column in `m.streams()` notes the outage.

A live table that still has no data `max_outage` seconds after the drop stops with an error,
because the rows Massive sent meanwhile are missing. The default is 300 seconds. A `poll()`
table stops the same way once its cycles have failed, or a late cycle has received nothing, for
that long. To continue, call the verb again for a new table.

```python
m = dm.MassiveSession(dm.MassiveConfig(max_outage=0))
```

This session's live tables stop at the first drop.

A connected feed can also go quiet. When nothing arrives for 5 minutes during the market's
regular hours, `Error` says so, but the table keeps running: a quiet symbol or a market holiday
looks the same.

### Long reads in the console: `wait=False`

Deephaven runs each console command while holding the update graph's exclusive lock, which every
live table needs to update (see Deephaven's
[engine locking](https://deephaven.io/core/docs/conceptual/query-engine/engine-locking/)). A
`history()` or `fetch()` that downloads for ten minutes in the console therefore pauses every
live table on the server for ten minutes. The library logs a WARNING once a command has held the
lock for 5 seconds.

Pass `wait=False` to run a long read in the background:

```python
year = m.stocks.trades(["AAPL", "MSFT"]).history("2025-01-02", "2025-09-30", wait=False)
```

What you see:

1. The call returns at once with `year`, an empty live table with the right columns.
2. The read's row in `m.streams()` reads `LOADING`.
3. When the download finishes, every row appears in one update, and the row reads `DONE`, or
   `FAILED` with the error. The table never changes again.

Take `year.snapshot()` only after `DONE`; before then the copy is empty.

A read waits out Massive's rate limits for a while before it gives up:

| Read | Waits on rate limits for at most |
|---|---|
| In a console command | 90 seconds |
| In the background (`wait=False`) | 10 minutes |

Releasing the table, or closing the session, stops the download.

### Never call the library from a table listener

A [table listener](https://deephaven.io/core/docs/how-to-guides/table-listeners-python/) runs on
Deephaven's update thread, and every verb refuses that thread with `ValueError`. To change what
a live table reads as data arrives, pass it a live table of symbols instead: see
[watchlists](watchlists.md).

### Filter one live table by another with a join, not `where_in`

In Deephaven 42.7, when both tables of a `where_in` or `where_not_in` fail, the server process
can exit. `m.close()` fails every live table of the session at once, so a `where_in` between two
of its live tables can end the server at the close. Use a join instead:

```python
watch = m.stocks.movers(dm.Direction.GAINERS).poll(60)
quotes = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).last_by()
marks = watch.select_distinct("Symbol").update("Watched = true")
watched = quotes.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")
```

`watched` holds the latest quote of each of the three stocks while it is among the day's top
gainers.
