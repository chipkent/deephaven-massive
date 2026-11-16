# 134. How many streamed futures tickers neither the snapshot nor the contracts list holds

Date: 2026-11-16

Status: open

Unknown: how many tickers the futures WebSocket sends, singles and combos alike, that neither
`/futures/v1/snapshot` nor `/futures/v1/contracts` (for its latest day) holds: the tickers a
calendar that looks up the snapshot first and the contracts list second could not date.

Meanwhile the calendar looks up single contracts in the contracts list only.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_streamed_futures_tickers_are_listed` (marked `integration_all_symbols`),
which streams every futures trade for 10 minutes, asks the snapshot for the tickers seen, the
contracts list for those it lacks, and records every ticker in neither.
