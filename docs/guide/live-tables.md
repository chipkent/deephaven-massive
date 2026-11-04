# Live tables

A live table keeps updating as long as something uses it. Once you've made one, you can stop it
or its whole session and keep its rows afterwards. You can also handle an outage, and run a long
read without pausing every other table on the server. To pick the right
live table in the first place, see [which live table to use](arguments.md#choose-a-live-table).

Most examples here stream stock trades. As written, they need a Stocks Advanced plan. On Stocks
Developer, whose streams run 15 minutes behind, create each session with
`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})`. Paste each example into the
console of the Deephaven IDE.

<a id="live-tables"></a>

## Keep a table running

A live table keeps updating while a variable holds it, a table derived from it uses it, or a
panel of the Deephaven IDE shows it. A derived table is one you make from it, such as
`trades.where(...)`. Static tables never update, so they hold nothing open. When the last of
these uses goes, the library stops the stream, poll, or read behind the table.

Deleting the variable, as in `del trades`, stops the table whenever Python next reclaims it, and
you can't predict when that is. To stop a table at a moment you choose,
[release it](#stop-one-table).

<a id="stop-one-table"></a>

## Stop one table

`m.release(table)` drops the library's hold on a live table, even while its variable still
exists. In the console, the hold drops when your command ends. This example starts a stream of
Apple's and Microsoft's latest trades, then releases it:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades(["AAPL", "MSFT"]).last_by()
m.release(trades)
```

Once nothing else uses `trades`, it stops updating and keeps the rows it had. In practice you
release a table some time after you made it. The panel the Deephaven IDE opens for a new table
counts as a use, and so does a table derived from it, so close those too. Until the last one
goes, the table keeps updating. Meanwhile its row in
[`m.streams()`](troubleshooting.md#diagnostics), the table that lists the session's live tables,
reads `RELEASED`.

You can release any of these:

- a live table that a session's call returned, such as a `last_by()`, a `poll()`, or a
  `history()` that follows a watchlist
- a background read, made with `wait=False`
- the [diagnostics tables](troubleshooting.md#diagnostics), such as `m.streams()`

Releasing a table twice does nothing. `m.release()` raises `ValueError` on a table you derived
yourself. Release the table it came from instead.

<a id="stop-everything"></a>

## Stop every table of a session

`m.close()` stops every stream, poll, and read of the session. Every live table of the session,
and every table derived from one, stops updating and shows this error, where `#1` is the
session's number:

```text
MassiveSession #1 was closed; this table no longer updates
```

The tables stop updating and show the error. To keep their rows, take a static copy with
`snapshot()`. This example streams Apple's trades, closes the session, and keeps what arrived.
Paste its first three lines, wait until `trades` has rows, then run the last two:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades("AAPL").append_only()
m.close()
kept = trades.snapshot()
```

`kept` is a static table of the trades that arrived before the close. From then on, the
session's calls and its diagnostics methods, such as `m.streams()`, raise `dm.MassiveError` with
the message `MassiveSession #1 is closed`. Building a query and calling `release()` still work.

A script that makes only static tables can close its session at the end of a `with` block, as in
`with dm.MassiveSession() as m:`. The session closes when the block ends, and every table the
script made keeps its rows.

A session also closes itself once neither your code nor any of its live tables refers to it.
None of its tables is running by then, so nothing fails. With the
[debug log](troubleshooting.md#the-log) on, an INFO line records when that happens.

<a id="connection-drops"></a>

## Handle outages and quiet feeds

The library reconnects to Massive by itself, and each live table keeps its last rows meanwhile.
The table's row in `m.streams()` notes the outage in its `Error` column until data flows again.
Its `Gaps` column keeps the time span whose rows may be missing.

A table waits `max_outage` seconds, 300 by default, then stops with an error:

| Table | When it stops |
|---|---|
| A stream | The connection has been down longer than `max_outage` |
| A `poll()` | Every cycle has failed for longer than `max_outage`, or one cycle has received nothing for that long |

The error names the setting, as in `... has been down since <time>, longer than
MassiveConfig.max_outage (300 s); ...`. To continue, make the table again, for example with
another `m.stocks.trades(...).last_by()` call. To wait 10 minutes instead, create the session
with a longer `max_outage`:

```python
import deephaven_massive as dm

patient = dm.MassiveSession(dm.MassiveConfig(max_outage=600))
```

Every live table of `patient` waits out an outage of up to 600 seconds before it stops.

A feed that stays connected can still go quiet. When nothing arrives for 5 minutes during the
market's trading hours, `Error` reads `no data since <time> during stocks trading hours; quiet
symbols or a market holiday may explain it`. The table keeps running, and the note clears when
data arrives.

<a id="long-reads"></a>

## Run a long read in the background

A long `history()` or `fetch()` typed in the console pauses every live table on the server until
it finishes. Deephaven runs each console command under its
[exclusive lock](https://deephaven.io/core/docs/conceptual/query-engine/engine-locking/), and no
live table can update while a command holds it. When a call in a console command runs for 5
seconds or more, the library logs a WARNING that says so.

Pass `wait=False` to run the read in the background instead. This reads a week of Apple's
trades, on a Stocks Developer plan or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades_week = m.stocks.trades("AAPL").history("2025-03-03", "2025-03-07", wait=False)
```

The call and the table go through three stages:

1. The call returns at once with `trades_week`, an empty live table with the right columns.
2. The read's row in `m.streams()` reads `LOADING`.
3. When the read finishes, every row appears in one update, and the row reads `DONE`, or
   `FAILED` with the error. The table never changes again.

Take `trades_week.snapshot()` only after `DONE`, since a copy taken earlier is empty. Closing the
session stops the read, and so does releasing the table once nothing else uses it.

When Massive rate-limits a read, the read waits and tries again. A read made in a console
command gives up sooner than any other, because every live table waits with it:

| Read | Waits on rate limits for at most |
|---|---|
| In a console command | 90 seconds |
| A background read with `wait=False`, or a read outside the console | 10 minutes |

<a id="table-listeners"></a>

## Change symbols with a watchlist, not a listener

A [table listener](https://deephaven.io/core/docs/how-to-guides/table-listeners-python/) is a
function Deephaven calls each time a table changes. A verb called from a listener, such as
`last_by()` or `fetch()`, raises `ValueError`, and so do `release()` and `close()`. To change what a
live table reads as data arrives, give it a live table of symbols. The table then follows each
symbol you add or delete, as [watchlists](watchlists.md) describes.

<a id="filter-with-a-join"></a>

## Filter one live table by another with a join

Use a join to keep the rows of one live table whose symbols appear in another. In the Deephaven
version the library is tested with, a `where_in` or `where_not_in` between two live tables can make
the server exit if both tables fail at once. `m.close()` fails every live table of the session
together, so closing the session can set it off. A join gives the same rows without that risk.

This example keeps the latest trade of three stocks while each is among the day's top gainers.
It needs a Stocks plan that includes movers and trade streams, such as Stocks Advanced:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
latest = m.stocks.trades(["AAPL", "MSFT", "NVDA"]).last_by()
marks = gainers.select_distinct("Symbol").update("Watched = true")
watched = latest.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")
```

`watched` has a row for each of the three stocks that's on the gainers list and has traded. It
changes as the list does each minute.
