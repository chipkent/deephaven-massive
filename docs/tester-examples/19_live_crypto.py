# Stream Bitcoin and Ether trades, and count them by the minute. Needs a paid Currencies plan.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

crypto_trades = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
crypto_latest = crypto_trades.last_by("Symbol")
trades_per_minute = crypto_trades.update("Minute = lowerBin(Timestamp, MINUTE)").count_by(
    "Trades", by=["Symbol", "Minute"]
)
