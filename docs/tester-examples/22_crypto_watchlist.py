# Needs a paid Currencies plan (Starter or higher). Makes a watchlist of crypto pairs and
# streams their trades, keeping each pair's latest. You should see three new tables:
# crypto_watchlist, with two rows; crypto_board, with one row per pair after its first trade,
# within seconds; and streams. Then follow the steps after this block. Crypto trades around
# the clock, so this works at any hour.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

crypto_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
crypto_watchlist.add(new_table([string_col("Symbol", ["X:ETHUSD", "X:LINKUSD"])]))
crypto_board = m.crypto.trades(crypto_watchlist).last_by()
streams = m.streams()
