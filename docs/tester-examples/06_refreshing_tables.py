import deephaven_massive as dm

m = dm.MassiveSession()

live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
