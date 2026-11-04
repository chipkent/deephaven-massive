# Make a table that asks Massive again every minute, and open the diagnostics tables.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
