# Asks for more than the free plan allows: the previous day's bar of six stocks every 30
# seconds, 12 Stocks requests a minute against a limit of 5. You should see a new table,
# paced, fill within about a minute, and the stocks row of rate_limits switch between READY
# and PACED.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
