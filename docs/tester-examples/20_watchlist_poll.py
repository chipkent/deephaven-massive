# Make two watchlists you can edit, and tables that follow them. Free plan.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
reads = m.symbol_reads()
streams = m.streams()

# Three Stocks requests a minute, within Basic's five.
stock_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
stock_watchlist.add(new_table([string_col("Symbol", ["JPM", "XOM", "TSLA"])]))
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
watched_dividends = m.stocks.dividends(stock_watchlist).fetch()

# One Futures request for each product you add.
product_watchlist = input_table({"ProductCode": dht.string}, key_cols="ProductCode")
product_watchlist.add(new_table([string_col("ProductCode", ["ES"])]))
watched_contracts = m.futures.contracts(product_code=product_watchlist, limit=10).fetch()
