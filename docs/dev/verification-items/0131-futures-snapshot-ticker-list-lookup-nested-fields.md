# 131. What a futures snapshot lookup by ticker list costs, and whether nested fields work

Date: 2026-11-16

Status: open

Unknown: how long `/futures/v1/snapshot?ticker.any_of=` takes for 1, 100, and 500 tickers,
whether a URL of 500 tickers is accepted, and whether Massive honors the nested
`fields=details.ticker,details.product_code` its docs describe, which the OpenAPI spec the
library is built from does not list.

Meanwhile the calendar does not read the snapshot.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_snapshot_lookup_by_ticker_list`, which times each size with and
without `fields`, five runs each in turn, and records the rows, bytes, and keys each returns,
or Massive's refusal.
