# 134. How many streamed futures tickers does neither the snapshot nor the contracts list hold?

Date: 2026-11-16

Status: open

The futures WebSocket sends tickers, singles and combos alike, that a calendar must find a
product for, in `/futures/v1/snapshot` first and `/futures/v1/contracts` second. How many it
finds in neither is unknown. If there are none or few, holding their rows a short while costs
little. If there are many, the memory and the problems they cause need a bound. Until it is
read, the calendar looks up only single contracts, in the contracts list.

Read by `tests/ingest/test_futures_calendar_integration.py::test_streamed_futures_tickers_are_listed` (marked `integration_all_symbols`), on
a key with futures data, while futures trade. It streams every futures trade for 10 minutes. It
asks the snapshot about the tickers seen, up to 3,000, and the contracts list about up to 1,000
of those the snapshot lacks, each spread evenly when there are more. It records every ticker
asked about that neither holds, and says when the caps left tickers unasked, which makes the
count a lower bound.
