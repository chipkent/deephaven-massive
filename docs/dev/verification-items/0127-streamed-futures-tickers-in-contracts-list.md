# 127. Whether the futures contracts list holds every single contract that trades on the WebSocket

Date: 2026-11-15

Status: open

The calendar finds a streamed contract's product in the contracts list; a traded contract the
list lacks cannot be dated. Unverified how many such contracts a live stream sees. Settled by
`tests/ingest/test_futures_calendar_integration.py::test_streamed_futures_tickers_are_listed` (marked `integration_all_symbols`), which
streams every futures trade for 10 minutes and looks up each single contract seen.
