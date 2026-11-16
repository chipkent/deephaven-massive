# 130. Does a date filter make the futures contracts lookup by ticker list fast?

Date: 2026-11-16

Status: open

The calendar's contracts lookup (`ticker.any_of`, no `date`) took 14 s for 100 tickers in four
of five runs and 0.45 s in the fifth, unexplained. If `date` makes it fast, the lookup sends
`date` and stays a cheap fallback. If the time is Massive's variance instead, the lookup's cost
stays unpredictable and the snapshot must carry the load. Until it is read, the lookup sends no
`date`.

Read by `tests/ingest/test_futures_calendar_integration.py::test_contracts_lookup_cost_with_and_without_date`, on a key with futures data. It
times 1, 100, and 500 tickers with `date`, and 1 and 100 without it (500 without it failed with
HTTP 502), five runs each in turn, page by page, recording each run and each failure as it
completes. A run cut at 5 pages says its numbers are lower bounds.
