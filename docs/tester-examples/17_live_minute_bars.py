# Keep the latest minute bar of five stocks, updated live. Needs a paid Stocks plan.
import deephaven_massive as dm

# Stocks Starter and Developer stream 15 minutes late, so this session accepts delayed
# data. On Stocks Advanced or Business, which stream in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
streams = m.streams()
