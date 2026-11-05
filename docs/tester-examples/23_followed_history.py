# Read a month of daily bars that follows a watchlist you can edit. Free plan.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
reads = m.symbol_reads()

stocks = new_table({"Symbol": ["AAPL", "MSFT"]})
watchlist = input_table(init_table=stocks, key_cols="Symbol")
month_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=30)
followed = m.stocks.bars(watchlist, dm.Timespan.DAY).history(month_ago)
