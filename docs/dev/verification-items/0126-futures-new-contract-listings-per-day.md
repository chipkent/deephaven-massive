# 126. How many futures contracts Massive lists each day

Date: 2026-11-15

Status: open

The contracts list is updated daily, so a contract first traded today may be missing from it,
and the calendar then cannot find its product. How many contracts are new each day sizes that
case. Massive's list records no day a contract was added, so the first trade date stands in for
it. Settled by `tests/ingest/test_futures_calendar_integration.py::test_new_futures_listings_per_day`, which counts the contracts first
traded in the 90 days before the most recent published Monday, per day, weekday, and type, up
to 15 pages (beyond them its counts are lower bounds, and it says so).
