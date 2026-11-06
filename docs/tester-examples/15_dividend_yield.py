from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
year_ago = yesterday - timedelta(days=364)
aapl_daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history(year_ago, yesterday)
dividends = (
    m.stocks.dividends("AAPL", start=year_ago, end=yesterday)
    .fetch()
    .view(["Symbol", "ExDividendDate", "CashAmount"])
)
dividend_yield = dividends.aj(
    aapl_daily, on=["Symbol", "ExDividendDate > Date"], joins=["PriceDate = Date", "Close"]
).update("PaymentYieldPercent = 100 * CashAmount / Close")
