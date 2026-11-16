# 134. How many streamed futures tickers neither the snapshot nor the contracts list holds

Date: 2026-11-16

Status: open

Unknown: how many tickers the futures WebSocket sends, singles and combos alike, that neither
`/futures/v1/snapshot` nor `/futures/v1/contracts` (for its latest day) holds: the tickers a
calendar that looks up the snapshot first and the contracts list second could not date.

Meanwhile the calendar looks up single contracts in the contracts list only.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_streamed_futures_tickers_are_listed` (marked `integration_all_symbols`),
which streams every futures trade for 10 minutes. It asks the snapshot about the tickers seen,
up to 3,000, and the contracts list about up to 1,000 of those the snapshot lacks, each spread
evenly when there are more. It records every ticker asked about that neither holds, and says
when the caps left tickers unasked, which makes the count a lower bound.
