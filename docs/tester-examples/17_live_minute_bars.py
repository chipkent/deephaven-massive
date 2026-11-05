# Keep the latest minute bar of five stocks, updated live. Needs a paid Stocks plan.
import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))

latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
streams = m.streams()
