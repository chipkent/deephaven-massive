# Releasing tables and closing a session

A live table costs something while it runs: a subscription on Massive's connection, memory for
its rows, and for a poll, requests on a timer. This page explains how a live table stops, how
to stop one yourself, and how to keep its data once it has stopped.

## The rule

A live table keeps running for as long as it, or any table derived from it, is in use. Nothing
else keeps it running. You never have to keep the session object or the query around:

```python
import deephaven_massive as dm

big_trades = dm.MassiveSession().stocks.trades("AAPL").append_only().where("Size >= 1000")
```

`big_trades` keeps updating although the session and the query were never stored, because
`big_trades` needs them. When you no longer need `big_trades`, the stream it uses stops.

Static tables, the results of `fetch()` and `history()`, hold nothing open and need no care.

## Stopping one table: `m.release()`

Deleting a variable (`del trades`) stops its stream eventually, when Deephaven's Java side
next reclaims unused memory. That is usually seconds away, but it can take much longer on a
quiet server, and until then the stream keeps running and a poll keeps sending requests. To
stop a table now, release it:

```python
m = dm.MassiveSession()

trades = m.stocks.trades("AAPL").append_only()
m.release(trades)
```

Releasing a table ends the library's hold on it. If nothing else uses the stream, the
library unsubscribes, or for a poll stops the timer. If something does (another table from
the same query, a table derived from this one, or a panel open in the web UI), the stream
keeps running for it, and `m.streams()` shows it `RELEASED`.

`m.release()` accepts the live tables the session's verbs returned, `wait=False` downloads
included, and diagnostics tables. Releasing a table twice does nothing. A static table, or a
table you derived yourself, is refused with a `ValueError`: release the live table it came
from instead.

A released table keeps the rows it had. Whether it goes on updating depends on whether
anything else still uses its stream, so treat a released table as finished.

## Stopping everything: `m.close()`

`m.close()` stops every stream, poll, and download of the session at once:

```python
m.close()
```

Every live table of the session, and every table derived from one, then **fails** with the
error `MassiveSession #1 was closed; this table no longer updates`. Failing the tables, rather
than leaving them frozen, means no table ever looks live while it is not. `m.streams()` shows
the streams `CLOSED`. Connections no other session uses are closed. Calling `close()` twice is
harmless.

After `close()`, verbs, `m.streams()`, `m.connections()`, and `m.rate_limits()` raise
`MassiveError("MassiveSession #1 is closed")`. Building queries and `m.release()` still work.
For new tables, create a new session.

## Keeping the data after a close

A failed table keeps its rows, and you can still read them, but Deephaven refuses to build new
tables on top of a failed one ("Can not listen to failed table"). Take a static copy with
`snapshot()` and work with that:

```python
m = dm.MassiveSession()

trades = m.stocks.trades("AAPL").append_only()
# ... later ...
m.close()
kept = trades.snapshot()
expensive = kept.where("Price > 250")
```

`deephaven.pandas.to_pandas(trades)` and `deephaven.arrow.to_arrow(trades)` also still return
the rows.

To keep a copy without closing, take a snapshot of a live table at any time;
`trades.snapshot()` is a static table of the rows it holds at that moment.

## Scripts: the session as a context manager

A script that only reads static tables can let a `with` block close the session for it:

```python
with dm.MassiveSession() as m:
    bars = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history("2026-01-02", "2026-10-30")
    splits = m.stocks.splits(["AAPL", "MSFT"]).fetch()
# the session is closed here; bars and splits are static and unaffected
```

Live tables created inside the block fail when it ends, so use the `with` form only for static
results.

## A session you forget to close

A session that is no longer referenced, with no live table left, is cleaned up on its own: the
library closes its connections and logs a WARNING that it was garbage-collected without
`close()`. Calling `close()` yourself is tidier, and immediate.

Next: [Troubleshooting](troubleshooting.md).
