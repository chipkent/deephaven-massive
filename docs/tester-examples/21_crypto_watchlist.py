# Stream the latest trade of each crypto pair on a watchlist. Needs a paid Currencies plan.
from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
streams = m.streams()

pairs = new_table({"Symbol": ["X:ETHUSD", "X:LINKUSD"]})
crypto_watchlist = input_table(init_table=pairs, key_cols="Symbol")
latest_trades = m.crypto.trades(crypto_watchlist).last_by()
