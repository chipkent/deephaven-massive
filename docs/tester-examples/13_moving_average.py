# Chart NVDA's daily close over six months with its 20-day moving average: one request.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_avg_tick

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

six_months_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=182)
nvda_daily = (
    m.stocks.bars("NVDA", dm.Timespan.DAY)
    .history(six_months_ago)
    .update_by(ops=[rolling_avg_tick(cols=["Average20 = Close"], rev_ticks=20)], by=["Symbol"])
)
nvda_chart = (
    Figure()
    .plot_xy(series_name="Close", t=nvda_daily, x="TimestampStart", y="Close")
    .plot_xy(series_name="20-day average", t=nvda_daily, x="TimestampStart", y="Average20")
    .show()
)
