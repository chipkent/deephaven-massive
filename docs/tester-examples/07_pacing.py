import deephaven_massive as dm

m = dm.MassiveSession()

# 12 Stocks requests a minute: six stocks every 30 seconds.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
rate_limits = m.rate_limits()

# The table keeps updating while its panel is open, and stops once you close the panel.
m.release(paced)
