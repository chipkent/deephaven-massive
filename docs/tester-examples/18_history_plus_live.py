# Join today's minute bars from history with the live ones that follow. Needs a paid Stocks plan.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

import deephaven_massive as dm

# Stocks Starter and Developer stream 15 minutes late, so this session accepts delayed
# data. On Stocks Advanced or Business, which stream in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
# Open the stream before reading history, so no minute falls between the two.
aapl_live = aapl_minutes.append_only()
aapl_past = aapl_minutes.history(session_day)
aapl_today = merge([aapl_past, aapl_live]).last_by(["Symbol", "TimestampStart"])
