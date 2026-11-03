# Makes a watchlist you can edit and a month of daily bars that follows it: each stock you add
# is read over the same month, and each one you delete leaves. One Stocks request for each
# stock you add, within the free plan. You should see three new tables: names, with two rows;
# followed, with about 20 bars each of AAPL and MSFT; and reads, with one DONE row for each.
# Then follow the steps after this block.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
month_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=30)
followed = m.stocks.bars(names, dm.Timespan.DAY).history(month_ago)
reads = m.symbol_reads()
