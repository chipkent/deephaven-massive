# Recipes

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

Complete examples beyond the README's [recipes](../../README.md#recipes). Each one runs as
written.

<a id="stack-asset-classes"></a>

## Stack two asset classes in one table

Tables of one kind from two asset classes, such as stock and forex bars, can have different
columns. The columns they share have the same names, so keep those and merge:

```python
from deephaven import merge

stock = m.stocks.bars("SPY", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
fx = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
common = [c for c in stock.column_names if c in fx.column_names]
both = merge([t.view(common) for t in (stock, fx)])
```

`both` has the minute bars of SPY and EUR/USD for June 2, 2025, one after the other, with the
columns the two tables share.

A few size and volume columns of snapshots and movers share a name but not a type: Massive
declares a whole number for one asset class and a decimal for another. `LastTradeSize`, for
example, is a `long` for stocks and a `double` for crypto. The
[dataset reference](../datasets/README.md) lists each column's type. Cast before you merge:

```python
stock_snap = m.stocks.snapshots(["AAPL"]).fetch().update("LastTradeSize = (double) LastTradeSize")
```

`stock_snap` is Apple's snapshot with `LastTradeSize` as a `double`, ready to merge with a
crypto snapshot.

<a id="busiest-minute"></a>

## Read every trade of the busiest minute

Values from one table can be arguments of the next read. This finds the minute of June 2, 2025
with the most AAPL volume, and reads every trade inside it:

```python
minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
for row in minutes.sort_descending("Volume").head(1).iter_dict():
    inside = m.stocks.trades("AAPL").history(row["TimestampStart"], row["TimestampEnd"])
```

`inside` holds the trades from the start of the busiest minute up to, but not including, its
end. `TimestampStart` and `TimestampEnd` are instants, so the range is exact: see
[`start` and `end`](arguments.md#start-and-end).
