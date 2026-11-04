# Read a month of daily bars that follows a watchlist you can edit. Free plan.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
reads = m.symbol_reads()

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
month_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=30)
followed = m.stocks.bars(names, dm.Timespan.DAY).history(month_ago)
