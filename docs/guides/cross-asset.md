# Join data across asset classes

Every bar the library returns carries its window: `TimestampStart`, when the bar opens, and
`TimestampEnd`, when it closes (the first instant after the bar). Join bars of different asset
classes on `TimestampEnd`. A bar's values are final only once its window has ended, so a
join on the end never pairs a row with a bar that was still open at the row's time.

## Minute bars line up exactly

Second and minute bars of every class start on whole seconds and minutes, so a stock's 10:31
bar and an index's 10:31 bar have the same `TimestampEnd`. An exact join pairs them:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

day = ("2025-03-03", "2025-03-03")
spy = m.stocks.bars("SPY", dm.Timespan.MINUTE).history(*day)
spx = m.indices.bars("I:SPX", dm.Timespan.MINUTE).history(*day)
eur = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).history(*day)

# One row per SPY minute, with the index and the euro over the same minute.
together = spy.natural_join(spx, on="TimestampEnd", joins="SpxClose = Close").natural_join(
    eur, on="TimestampEnd", joins="EurUsdClose = Close"
)
```

A minute in which one market had no trade has no bar, so its joined columns are null. Forex
and crypto trade around the clock, stocks mostly from 04:00 to 20:00 New York time, and an
index publishes during its own hours.

## Ticks against the last closed bar

To give each trade the latest bar of another class that had closed by the trade's time, use an
as-of join from the trade's `Timestamp` to the bar's `TimestampEnd`:

```python
trades = m.stocks.trades("SPY").history(*day)
with_index = trades.aj(spx, on="Timestamp>=TimestampEnd", joins="SpxClose = Close")
```

The same join works on live tables, such as `m.stocks.trades("SPY").append_only()` against
`m.indices.bars("I:SPX", dm.Timespan.MINUTE).append_only()`.

## Daily bars do not line up across classes

A daily bar covers a calendar day in its class's own time zone, so daily bars of different
classes cover different hours:

| Class | A daily bar covers | `Date` is |
|---|---|---|
| Stocks and options | 00:00 to 00:00 New York time | the New York date |
| Indices | 00:00 to 01:00 the next day, New York time (not yet checked against Massive) | the New York date |
| Forex and crypto | 00:00 to 00:00 UTC | the UTC date |
| Futures | one trading session, ending at 17:00 Chicago time | the session's date |

A join on `Date` therefore pairs a stock's New York day with a crypto pair's UTC day, which
starts four or five hours earlier. That is often what you want for a daily report, but it is
not the same span of time. To compare the same hours, build the daily figures from minute bars
over one window of your choosing, such as 09:30 to 16:00 New York time. For futures, the
session runs from the evening before, so no other class's day matches it.

## Stack tables of different classes

Two classes' tables of one kind can differ in their columns: forex quotes have no `Tape`, for
example. To stack them in one table, keep the columns they share:

```python
from deephaven import merge

stock = m.stocks.bars("SPY", dm.Timespan.MINUTE).history(*day)
fx = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).history(*day)
common = [c for c in stock.column_names if c in fx.column_names]
both = merge([t.view(common) for t in (stock, fx)])
```

The column references in [`docs/datasets/`](../datasets/) list each dataset's columns.
