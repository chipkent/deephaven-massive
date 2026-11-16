# 130. What a futures contracts lookup by ticker list costs with a date filter and without

Date: 2026-11-16

Status: open

Unknown: why the calendar's contracts lookup (`ticker.any_of`, no `date`) took 14 s for 100
tickers in four of five runs and 0.45 s in the fifth, and whether a `date` filter, which
leaves out every other day's rows, makes it fast.

Meanwhile the calendar's lookup sends no `date`.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_contracts_lookup_cost_with_and_without_date`, which times lookups of 1,
100, and 500 tickers with and without `date`, five runs each in turn, and records each page's
seconds (a run cut at 5 pages says its numbers are lower bounds).
