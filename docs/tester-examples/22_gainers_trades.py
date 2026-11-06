import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
streams = m.streams()

gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
gainer_trades = m.stocks.trades(gainers).last_by()
