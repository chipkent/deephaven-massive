# 118. What Massive answers to a filter list with an empty item (`ticker.any_of=AAPL,,MSFT`)

Date: 2026-11-12

Status: open

Sent as written; if Massive reads the empty item as no filter, the library should refuse it. Settled
by `tests/datasets/test_filters_integration.py` (Stocks Basic).
