# Close a session, and see that a closed session refuses new work.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
closing_status = m.reference.market_status().poll(60)
m.close()
print(m)

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history(yesterday - timedelta(days=6), yesterday)
except dm.MassiveError as error:
    print(f"MassiveError: {error}")

# A with block closes its session when the block ends.
config = dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription))
with dm.MassiveSession(config) as m2:
    last_week = m2.stocks.bars("NVDA", dm.Timespan.DAY).history(
        yesterday - timedelta(days=6), yesterday
    )
print(m2, last_week.size)
