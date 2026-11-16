# 131. How fast is a futures snapshot lookup by ticker list, and do nested fields work?

Date: 2026-11-16

Status: open

The snapshot names each ticker's product in its row, so one request could find the products of a
batch of tickers. Its time at 1, 100, and 500 tickers, whether a 500-ticker URL is accepted, and
whether Massive honors `fields=details.ticker,details.product_code` are unmeasured. Massive's
docs describe that parameter, and the OpenAPI spec the library is built from does not list it.
If the lookup is fast, the calendar can use it first, in batches of the largest size Massive
accepts. If `fields` works, the calendar asks for those two fields only; if not, it reads whole
rows. Until it is read, the calendar does not read the snapshot.

Read by `tests/ingest/test_futures_calendar_integration.py::test_snapshot_lookup_by_ticker_list`, on a key with futures data. It times each
size with and without `fields`, five runs each in turn, and records the rows, tickers, bytes,
and keys each returns, or Massive's refusal.
