# Joins company facts to prices: each company's market value beside its return over the
# last three months. Three Stocks requests and three reference requests. You should see a
# new table, size_and_return, with one row per company.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
