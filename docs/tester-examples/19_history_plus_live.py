# Needs a paid Stocks plan (Starter or higher). Joins today's minute bars from history with
# the live ones that follow, into one table that keeps growing. You should see a new table,
# aapl_today, with a row for each traded minute since 04:00 New York time, when pre-market
# trading starts, and a new row each minute until 20:00; before 04:00 or on a weekend, it is
# empty. On Stocks Starter or Developer, the live bars run 15 minutes behind; history is not
# affected. The stream opens before history is read, so no minute falls between them, and
# last_by keeps one row per minute: the stream's, where both have it.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
aapl_live = aapl_minutes.append_only()
aapl_past = aapl_minutes.history(session_day)
aapl_today = merge([aapl_past, aapl_live]).last_by(["Symbol", "TimestampStart"])
