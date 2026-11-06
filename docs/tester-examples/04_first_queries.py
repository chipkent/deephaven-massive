# Add a column, keep some rows, and summarize by stock, with Deephaven's table operations.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday
)
ranges = daily.update("Range = High - Low").view(["Symbol", "Date", "Close", "Range"])
nvda = ranges.where("Symbol == `NVDA`")
per_symbol = daily.agg_by(
    [agg.avg("AverageClose = Close"), agg.max_("HighestHigh = High"), agg.count_("Days")],
    by=["Symbol"],
)
