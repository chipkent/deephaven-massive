# Join today's minute bars from history with the live ones that follow. Needs a paid Stocks plan.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
# Open the stream before reading history, so no minute falls between the two.
aapl_live = aapl_minutes.append_only()
aapl_past = aapl_minutes.history(session_day)
aapl_today = merge([aapl_past, aapl_live]).last_by(["Symbol", "TimestampStart"])
