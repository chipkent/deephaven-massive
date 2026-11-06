from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday
)
print(daily.size)
