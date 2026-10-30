# Reads facts about companies and markets rather than prices. You should see four new
# tables: companies (three rows), market_now (one row), holidays, and yields (one row per
# business day).
details = m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
companies = details.view(["Symbol", "Name", "PrimaryExchange", "ListDate", "MarketCap"])

market_now = m.reference.market_status().fetch()
holidays = m.reference.market_holidays().fetch()
yields = m.economy.treasury_yields(start=first_day).fetch()
