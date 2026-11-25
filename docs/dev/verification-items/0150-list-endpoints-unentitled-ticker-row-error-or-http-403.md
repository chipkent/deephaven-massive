# 150. Does each list endpoint answer one unentitled ticker with a row error, or refuse the request with HTTP 403?

Date: 2026-11-25

Status: open

Unknown: how the ten endpoints that take a list of tickers answer a request naming one ticker
the key may not read beside others it may: with a row carrying `error` for that ticker and the
other tickers served, or with an HTTP 403 for the whole request. Massive documents a per-row
`NOT_ENTITLED` only for the unified snapshot and the indices snapshot. The ten endpoints are
the unified snapshot, the indices, options, forex, and crypto snapshots, the options greeks,
open interest, open interest by underlying, and unusual trades, and the futures contracts.

Meanwhile the library reads an HTTP 403 as Massive's refusal of the call, and a row with
`error` as Massive's answer for that ticker where the dataset has such rows (the three
snapshots of the unified, indices, and options endpoints). For an endpoint that answers per
row, the other tickers of the request are served. For one that answers 403, a request of many
tickers cannot name the one Massive refused, and the library never sends a request only to
find out which, so every ticker of the request shares the refusal.

Settled by `test_a_list_request_with_an_unentitled_ticker` in
`tests/ingest/test_rest_integration.py`, one case per endpoint. Each sends one request naming
tickers of several classes or sources and records Massive's answer, its rows' `error` values or
its refusal with its text, and when it refuses, each ticker alone. A case skips, quoting the
rows, when Massive answers 200 with no row error.
