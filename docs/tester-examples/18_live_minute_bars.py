# Needs a paid Stocks plan (Starter or higher). Keeps the latest minute bar of five stocks,
# updated live. You should see a new table, latest_bars, with one row per stock while the
# market is open; on a free key you see an EntitlementError instead.
latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
