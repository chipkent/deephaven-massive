# 12. The exact `max_connections` status text and timing

Date: 2026-10-23

Status: settled 2026-11-10: Massive keeps the newer connection and sends the older, authenticated one "Maximum number of websocket connections exceeded. ..." 2.6 to 8.3 s later, then close code 1008 (decision 97); verified by `tests/ingest/test_ws_integration.py`

Settled 2026-11-10: Massive keeps the newer connection and sends the older, authenticated one
"Maximum number of websocket connections exceeded. ..." 2.6 to 8.3 s later, then close code 1008
(decision 97); verified by `tests/ingest/test_ws_integration.py`.
