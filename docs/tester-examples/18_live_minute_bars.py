# Needs a paid Stocks plan (Starter or higher). Keeps the latest minute bar of five stocks,
# updated live. You should see a new table, latest_bars, with one row per stock while the
# market is open. On a free key the table opens, and a few seconds later Massive's live
# connection refuses it: the streams panel shows its row FAILED, with Massive's answer.
latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
