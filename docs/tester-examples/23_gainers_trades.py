# Needs a paid Stocks plan with trades (Developer or higher). Lets one table choose another's
# symbols: the day's top gainers, re-read every minute, choose whose trades to stream. You
# should see two new tables: gainers, with up to 20 rows, and gainer_trades, with one row per
# gainer after its first trade, while the market is open.
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
gainer_trades = m.stocks.trades(gainers).last_by()
