# Needs a paid Stocks plan (Starter or higher). Joins today's minute bars from history with
# the live ones that follow, into one table that keeps growing. You should see a new table,
# aapl_today, with a row per minute since the open, and a new row each minute. On Stocks
# Starter or Developer, use the session example 18 opens with delayed_subscriptions: the live
# bars then run 15 minutes behind, and history is not affected by the setting.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
aapl_today = merge([aapl_minutes.history(session_day), aapl_minutes.append_only()])
