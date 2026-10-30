# Your first tables

Every table in this library comes from the same three steps. You pick a **dataset**, such as
the trades of US stocks; you give it **symbols** and any options, which makes a **query**; and
you call a **verb** on the query, which makes the table. The verb you choose decides whether
the table is live or static and what shape it has.

```python
import deephaven_massive as dm

m = dm.MassiveSession()

query = m.stocks.trades(["AAPL", "MSFT"])  # dataset and symbols: nothing is sent yet
table = query.last_by()                    # the verb: now the library subscribes
```

Building a query does nothing on the network, so you can build, store, and reuse queries
freely. Only a verb talks to Massive.

## A first live table

Crypto trades around the clock, so it is the easiest market to watch at any hour. This table
holds the latest trade of each of two pairs, and updates each time either one trades:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

latest = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).last_by()
```

Open `latest` from the console's variable list, or click its name in the output, and watch the
`Price` and `Timestamp` columns change. Each row is one symbol's most recent trade.

Streaming crypto needs a paid Currencies plan. On a free plan the call, or the table soon
after, fails with an `EntitlementError` saying the plan does not include the stream: the free
plans read through REST only. A free plan can still make a live table, one that asks Massive again on a timer.
This one re-reads the day's Apple headlines every five minutes, and adds each new article as
it appears:

```python
from deephaven.time import dh_today

headlines = m.reference.news(symbols="AAPL", start=dh_today()).poll(300)
```

[Plans and entitlement errors](plans.md) says what each plan can stream.

## A first historical table

`history()` reads a range of past data into a static table. Daily bars (one open, high, low,
close, and volume per day) of Apple for the first ten months of 2026:

```python
daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-01-02", "2026-10-30")
```

This works on every plan, the free one included, which keeps two years of history. The dates
are whole days and both are included. Leave out the end to read through today:

```python
since_june = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-06-01")
```

A bar appears only once it has ended, so today's daily bar is not in the table until
tomorrow. Bars have two time columns, `TimestampStart` and `TimestampEnd`, the start and the
exclusive end of the bar's window, and a `Date` column holding the trading day as
`yyyy-MM-dd` text. Stock prices are split-adjusted by default, so a stock that split 10 for 1
shows no 90% drop on the day of the split; pass `adjusted=False` to `bars()` for the prices as
traded.

The dates can be text, Python `date` and `datetime` values, or Deephaven's own time values.
That lets you take a range straight from another table. This finds the minute of 29 October
2026 when Apple traded the most shares, then reads every trade inside that minute:

```python
minutes = m.stocks.bars("AAPL").history("2026-10-29", "2026-10-29")
busiest = next(minutes.sort_descending("Volume").head(1).iter_dict())
trades = m.stocks.trades("AAPL").history(busiest["TimestampStart"], busiest["TimestampEnd"])
```

Two dates mean whole days. Two instants, such as the `TimestampStart` and `TimestampEnd` values
above or timezone-aware Python `datetime` values, mean exact times, with the start included
and the end excluded. A call that mixes a date with an instant is refused.

## What the columns mean

Every query can describe its own columns without asking Massive anything:

```python
cols = m.stocks.trades("AAPL").columns()
```

`cols` has one row per column of the dataset, in order, with its `Name`, its Deephaven
`Type`, a `Description`, and `SuppliedBy`, the transports that fill it. The same descriptions
appear when you hover over a column header in the web UI, and in the
[column reference](../datasets/).

A few conventions hold in every table:

- `Symbol` holds Massive's exact ticker, such as `AAPL`, `X:BTCUSD`, or `C:EURUSD`.
- `Timestamp` is when a trade or quote happened; a bar has `TimestampStart` and
  `TimestampEnd` instead.
- `Date` and every other calendar date are `yyyy-MM-dd` text, so a date from one table joins
  to a date from any other.
- `TimestampInsert` is when the library received the row: for history, the download time.

## Finding a dataset

Datasets are grouped by asset class (`m.stocks`, `m.options`, `m.futures`, `m.indices`,
`m.forex`, `m.crypto`) and by topic (`m.reference`, `m.indicators`, `m.economy`,
`m.partners`). Tab completion in the console lists what each group holds, and
`help(m.stocks.trades)` describes one dataset.

`dm.datasets()` lists every dataset in one static table, with no session or key needed:

```python
catalog = dm.datasets()
streaming = catalog.where("Arrays.asList(Verbs).contains(`blink`)")
```

Its columns are `Dataset` (such as `stocks.trades`), `Call` (how to call it), `Summary`,
`Verbs` and `Transports` (both arrays of text), and `Documentation`, a link to the dataset's
section of the column reference. The second line keeps the datasets that stream.

## History and live data in one table

A common need is a table that starts with the past and keeps growing with the present. Read
the history, open the live stream, and merge the two:

```python
from deephaven import merge
from deephaven.time import dh_today

bars = m.stocks.bars("NVDA", dm.Timespan.MINUTE)
today = merge([bars.history(dh_today()), bars.append_only()])
```

The history ends at the moment you call it, and the stream starts a moment later, so a bar at
the boundary can be missing or appear twice. Sort or deduplicate on `TimestampStart` if that
matters to you.

Next: [Verbs and table shapes](table-shapes.md).
