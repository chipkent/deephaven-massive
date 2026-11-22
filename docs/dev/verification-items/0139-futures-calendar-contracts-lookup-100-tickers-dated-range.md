# 139. Does the calendar's dated contracts lookup of 100 tickers answer quickly, without a 504?

Date: 2026-11-16

Status: open

Massive answered a 100-ticker `ticker.any_of` lookup of `/futures/v1/contracts` with no `date`
with HTTP 504, "upstream request timeout", twice on 2026-11-16. The futures calendar sends no
such lookup: for tickers the snapshot lacks, it asks the week up to today's session
(`date.gte`, `date.lte`), at most 100 tickers a request. A single `date` measured 0.09 to
0.23 s for 100 and 500 tickers; the week's range is unmeasured. If it answers quickly, the
lookup stays as it is. If it times out or is slow, the calendar sends fewer tickers a request.
Until it is read, the calendar sends 100.

Read by `tests/ingest/test_futures_calendar_integration.py::test_calendar_contracts_lookup_as_sent`,
on a key with futures data. It sends the calendar's own query for 100 tickers spread over the
snapshot five times, and records each run's rows, pages, seconds, and any failure.
