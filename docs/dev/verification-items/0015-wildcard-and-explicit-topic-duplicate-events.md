# 15. Whether a wildcard and an explicit topic of one channel (`T.*` and `T.AAPL`) deliver an event twice

Date: 2026-10-23

Status: open

Explicit topics are off the wire while the wildcard is wanted; transitions accept a brief duplicate.
Settled by `tests/massive/ws/test_client_integration.py`.
