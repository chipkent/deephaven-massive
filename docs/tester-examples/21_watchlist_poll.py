# Makes a watchlist you can edit, a table that reads the previous day's bar of each of its
# stocks every minute (three Stocks requests a minute, within the free plan's five), and a
# fetch() of their dividends that follows the watchlist. Then a second watchlist, of futures
# products, and a fetch() of each product's contracts that follows it: one Futures request for
# each product you add. You should see five new tables: stock_watchlist, with three rows;
# watched_bars, with one row per stock within about a minute; watched_dividends, with each
# stock's dividends; product_watchlist, with one row; and watched_contracts, with up to 10 ES
# rows. Then follow the steps after this block.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

stock_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
stock_watchlist.add(new_table([string_col("Symbol", ["JPM", "XOM", "TSLA"])]))
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
watched_dividends = m.stocks.dividends(stock_watchlist).fetch()

product_watchlist = input_table({"ProductCode": dht.string}, key_cols="ProductCode")
product_watchlist.add(new_table([string_col("ProductCode", ["ES"])]))
watched_contracts = m.futures.contracts(product_code=product_watchlist, limit=10).fetch()
