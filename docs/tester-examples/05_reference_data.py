# Read facts about companies and markets: company details, market hours, Treasury yields.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

details = m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
companies = details.view(["Symbol", "Name", "PrimaryExchange", "ListDate", "MarketCap"])

market_now = m.reference.market_status().fetch()
holidays = m.reference.market_holidays().fetch()
two_weeks_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=14)
yields = m.economy.treasury_yields(start=two_weeks_ago).fetch()
