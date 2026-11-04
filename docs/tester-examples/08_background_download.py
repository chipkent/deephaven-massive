# Start a one-year download of two stocks in the background, so the console stays free.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago, wait=False)
streams = m.streams()
