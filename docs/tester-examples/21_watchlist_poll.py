# Makes a watchlist you can edit, and a table that reads the previous day's bar of each of
# its stocks every minute: three Stocks requests a minute, within the free plan's five. You
# should see two new tables: stock_watchlist, with three rows, and watched_bars, with one row
# per stock within about a minute. Then follow the steps after this block.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

stock_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
stock_watchlist.add(new_table([string_col("Symbol", ["JPM", "XOM", "TSLA"])]))
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
