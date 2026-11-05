# Make two watchlists you can edit, and tables that follow them. Free plan.
from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
reads = m.symbol_reads()
streams = m.streams()

# Three Stocks requests a minute, within Basic's five.
stocks = new_table({"Symbol": ["JPM", "XOM", "TSLA"]})
stock_watchlist = input_table(init_table=stocks, key_cols="Symbol")
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
watched_dividends = m.stocks.dividends(stock_watchlist).fetch()

# One Futures request for each product you add.
products = new_table({"ProductCode": ["ES"]})
product_watchlist = input_table(init_table=products, key_cols="ProductCode")
watched_contracts = m.futures.contracts(product_code=product_watchlist, limit=10).fetch()
