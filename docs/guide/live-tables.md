# Live tables

A live table keeps updating while you use it, and the library keeps its stream, poll, or
download running for as long as that lasts. Look here to stop a table or keep its rows after it
stops. You'll also find why a table stops by itself, and how to run a long read without
freezing the rest of the server. For which live table to pick in the first place, see
[which live table to use](arguments.md#choose-a-live-table).

Most examples here stream stock trades. As written, they need a Stocks Advanced plan. On Stocks
Developer, whose streams run 15 minutes behind, create each session with
`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})`. Paste each example into the
console of the Deephaven IDE.

<a id="live-tables"></a>

## How long a live table runs

A live table keeps updating while anything uses it. That's the variable you assigned it to, any
table you derived from it, such as `trades.where(...)`, and any panel of the Deephaven IDE that
shows it. Static tables never update, so they hold nothing open.

### Stop one table

Deleting a variable, as in `del trades`, stops the stream only when Python and Java reclaim the
table's memory, which can take minutes on a quiet server. Call `m.release(table)` to stop it at
once:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades(["AAPL", "MSFT"]).last_by()
m.release(trades)
```

If nothing else uses `trades`, its stream stops. If a derived table or an open panel still uses
it, it keeps updating, and its row in `m.streams()` reads `RELEASED` until the last one goes. A
poll or a background download behaves the same way.

You can release any live table a verb of the session returned, including a `history()` or
`fetch()` that follows a watchlist, a background download, and the
[diagnostics tables](troubleshooting.md#diagnostics). Releasing a table twice does nothing.
Releasing a table you derived yourself raises `ValueError`, so release the table it came from.

### Stop everything

`m.close()` stops every stream, poll, and download of the session. Every live table of the
session, and every table derived from one, stops updating and shows this error:

```text
MassiveSession #1 was closed; this table no longer updates
```

The tables keep their rows, but Deephaven can't build a new live table on a table that has
failed. To keep working with the rows, take a static copy with `snapshot()`:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades("AAPL").append_only()
m.close()
kept = trades.snapshot()
```

`kept` is a static table of the trades that arrived before the close. After `close()`, the
session's verbs and diagnostics tables raise `dm.MassiveError`, while building queries and
`release()` still work.

A script that reads only static tables can close its session at the end of a `with` block, as in
`with dm.MassiveSession() as m:`. A session that nothing refers to any more closes by itself.
With the [debug log](troubleshooting.md#the-log) on, an INFO line says when that happens.

### When the connection to Massive drops

The library reconnects by itself, and each live table keeps its last rows meanwhile. The table's
`Error` column in `m.streams()` notes the outage until data flows again, and its `Gaps` column
keeps the span whose rows may be missing.

If the connection is still down after `max_outage` seconds, 300 by default, the table stops with
an error, because the rows Massive sent meanwhile are missing. A `poll()` table stops the same
way when its cycles have failed, or a late cycle has received nothing, for that long. To go on,
call the verb again for a new table. This session's live tables stop at the first drop:

```python
import deephaven_massive as dm

fragile = dm.MassiveSession(dm.MassiveConfig(max_outage=0))
```

A connected feed can also go quiet. If nothing arrives for 5 minutes during the market's regular
hours, `Error` says so, but the table keeps running, because a quiet symbol and a market holiday
look the same.

### Long reads in the console

Deephaven runs each console command while holding the update graph's
[exclusive lock](https://deephaven.io/core/docs/conceptual/query-engine/engine-locking/), and
every live table needs that lock to update. A `history()` or `fetch()` that downloads for ten
minutes in the console therefore pauses every live table on the server for ten minutes. When a
call in a console command runs for 5 seconds or more, the library logs a WARNING that says so.

Pass `wait=False` to run a long read in the background. This reads nine months of Apple's and
Microsoft's trades, on a Stocks Developer plan or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
year = m.stocks.trades(["AAPL", "MSFT"]).history("2025-01-02", "2025-09-30", wait=False)
```

The call and the table go through three stages:

1. The call returns at once with `year`, an empty live table with the right columns.
2. The read's row in `m.streams()` reads `LOADING`.
3. When the download finishes, every row appears in one update, and the row reads `DONE`, or
   `FAILED` with the error. The table never changes again.

Take `year.snapshot()` only after `DONE`, because a copy taken before then is empty. Closing
the session stops the download, and so does releasing the table once nothing else uses it.

A read waits out Massive's rate limits for a while before it gives up:

| Read | Waits on rate limits for at most |
|---|---|
| In a console command | 90 seconds |
| In the background, with `wait=False` | 10 minutes |

### Calls from a table listener

A [table listener](https://deephaven.io/core/docs/how-to-guides/table-listeners-python/) runs on
Deephaven's update thread, and every verb refuses that thread with `ValueError`. To change what a
live table reads as data arrives, give it a live table of symbols, as
[watchlists](watchlists.md) describes.

### Filter one live table by another

Filter one live table by another with a join. In Deephaven 42.7, when both tables of a
`where_in` or `where_not_in` fail at once, the server process can exit. `m.close()` fails every
live table of the session together, so a `where_in` between two of them can end the server when
you close the session. A join gives the same rows.

This example keeps the latest trade of three stocks while each is among the day's top gainers:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
latest = m.stocks.trades(["AAPL", "MSFT", "NVDA"]).last_by()
marks = gainers.select_distinct("Symbol").update("Watched = true")
watched = latest.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")
```

`watched` has a row for each of the three stocks that's on the gainers list, and it changes as
the list does each minute.
