# Ask for more than the free plan allows, then let the library stop the table.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

# 12 Stocks requests a minute: six stocks every 30 seconds, against a limit of 5.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
rate_limits = m.rate_limits()
streams = m.streams()

# The table keeps updating while its panel is open, and stops once you close the panel.
m.release(paced)
