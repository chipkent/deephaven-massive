# Needs a paid Currencies plan (Starter or higher). Streams Bitcoin and Ether trades and
# counts them by the minute. You should see two new tables: crypto_latest, one row per coin
# with its latest trade, and trades_per_minute, which grows each minute. Crypto trades
# around the clock, so this works at any hour.
crypto_trades = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
crypto_latest = crypto_trades.last_by("Symbol")
trades_per_minute = crypto_trades.update("Minute = lowerBin(Timestamp, MINUTE)").count_by(
    "Trades", by=["Symbol", "Minute"]
)
