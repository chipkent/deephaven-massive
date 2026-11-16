# 58. Futures session labels: the 16:45 CBOT pre-open's session and when the next session's schedules are published

Date: 2026-10-29

Status: open

REST labels flip at 17:00 Chicago (seen 2026-10-31); stream rows use that rule corrected by the
schedules, else a null `Date`. Settled by `tests/namespaces/test_futures_integration.py`.
