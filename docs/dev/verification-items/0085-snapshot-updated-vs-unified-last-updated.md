# 85. Whether the v2 snapshots' `updated` is the unified snapshot's `last_updated` for the same ticker

Date: 2026-11-11

Status: open

Kept apart as `Updated` until read. Settled by `tests/massive/refresh/test_overrides_integration.py`
(Stocks Starter and Currencies Starter), which reads both back to back and compares them within the
time between the requests plus 1 second.
