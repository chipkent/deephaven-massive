# Needs a paid Stocks plan (Starter or higher). Joins today's minute bars from history with
# the live ones that follow, into one table that keeps growing. You should see a new table,
# aapl_today, with a row per minute since the open and a new row each minute, while the
# market is open; before 09:30 New York time or on a weekend, it is empty. On Stocks Starter
# or Developer, the live bars run 15 minutes behind; history is not affected.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
aapl_today = merge([aapl_minutes.history(session_day), aapl_minutes.append_only()])
