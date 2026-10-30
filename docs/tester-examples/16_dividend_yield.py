# Finds each Apple dividend of the past year and the closing price on the trading day
# before it, to give each payment's yield: two requests. You should see a new table,
# dividend_yield, with about four rows.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
aapl_daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history(year_ago)
dividends = (
    m.stocks.dividends("AAPL")
    .fetch()
    .where(f"ExDividendDate >= `{year_ago}`")
    .view(["Symbol", "ExDividendDate", "CashAmount"])
)
dividend_yield = dividends.aj(
    aapl_daily, on=["Symbol", "ExDividendDate > Date"], joins=["PriceDate = Date", "Close"]
).update("YieldPercent = 100 * CashAmount / Close")
