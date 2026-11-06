from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago, wait=False)
streams = m.streams()
