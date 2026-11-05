# Stream the trades of the day's top gainers. Needs Stocks Developer or Advanced.
import deephaven_massive as dm

# Stocks Developer streams 15 minutes late, so this session accepts delayed data. On
# Stocks Advanced, which streams in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))
streams = m.streams()

gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
gainer_trades = m.stocks.trades(gainers).last_by()
