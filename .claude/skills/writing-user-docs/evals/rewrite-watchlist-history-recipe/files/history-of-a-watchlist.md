<a id="history-of-a-watchlist"></a>

### History of a watchlist

Pass a live watchlist to `history()` and the table follows it: each symbol you add is read over
the same range, and each one you delete leaves (see [`symbols`](#symbols)). Two other ways fit
other needs. To read the watchlist's symbols once, as they stand now, pass `snapshot()` of it.
A followed `history()` served from flat files (see
[where history comes from](#history-sources)) downloads every file of the range again
for each symbol you add, since each file holds the whole market. To follow it with no request
or download per symbol, read every symbol once and filter by the watchlist: adding or deleting
a symbol then costs nothing. That works where the dataset serves `dm.ALL`
and the whole market fits in memory: daily bars, reference lists, or one day of minute bars,
but not a year of minute bars or any range of trades and quotes. A data index on `Symbol` keeps
each change of the watchlist fast:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col
from deephaven.experimental.data_index import data_index

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))

# Read once: the watchlist's symbols now, never again.
fixed = m.stocks.bars(names.snapshot(), dm.Timespan.DAY).history("2025-06-02", "2025-06-06")

# Every stock's daily bars, read once and filtered by the live watchlist.
every = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
data_index(every, ["Symbol"])
mine = every.where_in(names, "Symbol")
```
