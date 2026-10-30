# Makes a table that asks Massive again every minute, and opens the three diagnostics
# tables. You should see four new tables: live_status, which changes about once a minute,
# and rate_limits, streams, and connections.
live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
