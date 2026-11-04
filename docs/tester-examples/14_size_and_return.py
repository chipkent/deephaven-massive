# Put each company's market value beside its return over the last three months.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

symbols = ["AAPL", "JPM", "XOM"]
quarter_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=91)
closes = m.stocks.bars(symbols, dm.Timespan.DAY).history(quarter_ago)
returns_3m = (
    closes.sort(["Symbol", "Date"])
    .agg_by([agg.first("FirstClose = Close"), agg.last("LastClose = Close")], by=["Symbol"])
    .update("Return3m = LastClose / FirstClose - 1")
)
facts = m.reference.ticker_details(symbols).fetch().view(["Symbol", "Name", "MarketCap"])
size_and_return = facts.natural_join(returns_3m, on=["Symbol"], joins=["Return3m"]).update(
    "MarketCapBillions = MarketCap / 1e9"
)
