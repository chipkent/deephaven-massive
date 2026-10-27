"""The WebSocket client of the Massive wire package (a marker for now, with no contents).

It is the home of the client for one Massive WebSocket endpoint: its loop thread, handshake,
subscriptions, reconnection, and frame decoding. It may import only `net` and `spec` from
`_massive`.
"""
