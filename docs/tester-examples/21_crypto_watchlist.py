# Stream the trades of a watchlist of crypto pairs. Needs a paid Currencies plan.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
streams = m.streams()

crypto_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
crypto_watchlist.add(new_table([string_col("Symbol", ["X:ETHUSD", "X:LINKUSD"])]))
crypto_board = m.crypto.trades(crypto_watchlist).last_by()
