# Read two weeks of daily bars for three stocks: three requests to Massive.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday
)
print(daily.size)
