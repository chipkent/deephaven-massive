# Needs a paid Stocks plan (Starter or higher). Keeps the latest minute bar of five stocks,
# updated live. You should see a new table, latest_bars, with one row per stock while the
# market is open. On a free key the table opens, and a few seconds later Massive's live
# connection refuses it: the streams panel shows its row FAILED, with Massive's answer.
# Stocks Starter and Developer stream 15-minute delayed data, which the library serves only
# when asked: on those plans the call stops with an error that names the setting. Then open
# the session with it, and run this again:
#     m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))
latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
