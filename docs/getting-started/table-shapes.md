# Verbs and table shapes

The verb you call on a query decides what kind of table you get: static or live, and if live,
how it keeps its rows. This page describes the seven verbs and when to reach for each one.

Not every dataset has every verb. A dataset has the verbs its Massive transports can serve:

| The dataset is served by | Its verbs | The table |
|---|---|---|
| a REST list or snapshot, such as dividends or snapshots | `fetch()`, `poll(interval)` | static, or live and refreshed on a timer |
| a WebSocket stream, such as trades or minute bars | `blink()`, `append_only()`, `ring(n)`, `last_by()` | live, updated as rows arrive |
| a historical source, such as trades or bars | `history(start, end)` | static |

Trades, quotes, and one-second and one-minute bars have both the stream verbs and `history()`.
Calling a verb a dataset lacks raises an `AttributeError` that lists the verbs it has and
names a dataset that has the verb you wanted: `m.stocks.trades("AAPL").fetch()`, for example,
points you to `m.stocks.last_trade("AAPL").fetch()`.

## Static tables from REST: `fetch()`

`fetch()` asks Massive once and returns every row the request covers, however many pages that
takes, as a static table.

```python
import deephaven_massive as dm

m = dm.MassiveSession()

dividends = m.stocks.dividends(["AAPL", "MSFT"], start="2025-01-01").fetch()
last = m.stocks.last_trade(["AAPL", "MSFT"]).fetch()
```

Many list datasets take `start=` and `end=` on their own time axis (the ex-dividend date for
dividends) and `limit=`, a cap on the total number of rows. `help()` on a dataset names its
time axis.

## Live tables from REST: `poll(interval)`

`poll(interval)` runs the same request every `interval` seconds and keeps the result current.
It is how you get a live table from data that Massive offers only over REST, and how a free
plan gets live data at all.

```python
movers = m.stocks.movers(dm.Direction.GAINERS).poll(60)  # the day's top gainers, each minute
```

A poll combines its cycles in one of three ways, chosen with `mode=`:

- `dm.PollMode.REPLACE` keeps one row per key, usually per `Symbol`, and removes a key the
  latest cycle did not return. The table always shows the latest answer. Most datasets use it.
- `dm.PollMode.UPSERT` keeps one row per key, updates it when it changes, and never removes
  one.
- `dm.PollMode.APPEND` adds the rows it has not seen before and never changes or removes a
  row. News and filings use it, so the table grows into a feed.

Each dataset has its natural mode, which its `help()` names, so you rarely pass `mode=`. Pass
it to change what the table means. On a snapshot, `APPEND` records every new state of each
symbol instead of only the latest:

```python
path = m.stocks.snapshots(["AAPL", "MSFT"]).poll(30, mode=dm.PollMode.APPEND)
```

A news or filings poll needs `start=` or `limit=` on the query. Without either, every cycle
would download the whole archive Massive keeps, so the call is refused with a message saying
so. On a free plan each subscription allows five requests a minute, so keep the interval long
and the symbol list short; [Plans and entitlement errors](plans.md) says more.

## Live tables from streams

A stream dataset gives you four shapes over the same stream. Opening two shapes of one query,
or the same shape twice, subscribes once: the tables share one stream from Massive.

### `blink()`

A blink table holds only the rows that arrived in the latest update cycle, about once a
second. Each cycle replaces the last. It keeps no history, so it costs no memory, and it is
what Deephaven's aggregations are built to consume: an aggregation over a blink table counts
every row that ever arrived.

```python
trades = m.stocks.trades(["AAPL", "MSFT"]).blink()
volume = trades.view(["Symbol", "Size"]).sum_by("Symbol")  # total shares since the call
```

### `append_only()`

Every row received since the call, oldest first. It grows without limit, so use it for a
bounded session or a quiet dataset.

```python
today = m.stocks.bars(["AAPL", "MSFT"]).append_only()  # one-minute bars as they close
```

### `ring(n)`

The most recent `n` rows, older ones dropping off as new ones arrive. A fixed amount of
memory, however long it runs.

```python
recent = m.stocks.quotes("AAPL").ring(10_000)
```

### `last_by()`

The latest row for each key, by default one row per `Symbol`. It is the current state of each
instrument: the last trade, the last quote, the latest minute bar.

```python
board = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).last_by()
by_exchange = m.stocks.trades("AAPL").last_by(["Symbol", "Exchange"])
```

Pass one column name or a list to key by something else. The table starts empty and fills in
as each symbol ticks; it never asks REST for a starting value. For the current state of
symbols that may not trade for a while, poll a snapshot instead:
`m.stocks.snapshots(symbols).poll(15)`.

## History: `history(start, end)`

`history()` reads a range of past rows into a static table:

```python
q = m.stocks.trades("AAPL")
day = q.history("2026-10-29", "2026-10-29")  # every trade of that New York day
```

Dates are whole days, both included, in the market's own day: New York for stocks, options,
and indices, UTC for forex and crypto, and the trading session for futures. Instants are exact
times, the start included and the end excluded. Leave out `end` to read through now.

The library reads history from REST or from flat files, and chooses for you
(`source=dm.HistorySource.AUTO`): flat files for many symbols or `dm.ALL`, when you have
flat-file keys and the files for the range are published; REST otherwise. The two sources
occasionally fill different columns. When the choice leaves a column empty that the other
source would fill, the library logs a WARNING naming the columns and the `source=` that fills
them. Pass `source=dm.HistorySource.REST` or `source=dm.HistorySource.FLAT_FILES` to choose
yourself. The table's description, shown in the web UI, names the source it came from.

`dm.ALL` stands for every symbol. It works with flat files, and with REST for daily bars:

```python
market = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2026-10-29", "2026-10-29")
```

### Long downloads: `wait=False`

The Deephaven console runs each command while holding the lock that every live table needs to
update. A `history()` or `fetch()` that takes minutes therefore pauses every live table on the
server until it returns. The library logs a WARNING when that happens, and before a long paced
download on a free plan.

`wait=False` returns at once instead, with a live table that has the right columns and no
rows. The download runs in the background, and when it has finished, the whole result appears
in one update and never changes again. The table is never half-filled.

```python
year = m.stocks.trades(["AAPL", "MSFT"]).history("2025-10-01", "2026-09-30", wait=False)
```

`m.streams()` shows the download with `Kind` `read` and a `State` of `LOADING`, then `DONE`, or
`FAILED` with the error. Until it is `DONE` the table is empty, so `to_pandas(year)` returns no
rows; take `year.snapshot()` or convert it after it has filled. Releasing the table, or closing
the session, cancels the download.

## Choosing a shape

| You want | Use |
|---|---|
| a running total or average over a stream | `blink()` and an aggregation |
| every row of a session, to analyze afterwards | `append_only()` |
| a rolling window of recent rows | `ring(n)` |
| the current state of each symbol, updated as it trades | `last_by()` |
| the current state of each symbol, on any plan | `snapshots(...).poll(n)` |
| a growing feed of news or filings | `poll(n)` on the feed, with a `start=` |
| a fixed range of the past | `history(start, end)` |
| a reference list, read once | `fetch()` |

Next: [Reactive watchlists](watchlists.md).
