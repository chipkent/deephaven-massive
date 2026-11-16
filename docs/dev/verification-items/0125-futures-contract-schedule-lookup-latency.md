# 125. How long the futures calendar's contract and schedule lookups take

Date: 2026-11-15

Status: open

A streamed futures contract's first rows wait for two lookups, its product and then its
product's schedule; how long each takes, and how that grows with the tickers or products in one
request, is unmeasured, so the waits the calendar allows rest on no evidence. Settled by
`tests/ingest/test_futures_calendar_integration.py::test_contract_and_schedule_lookup_latency`, which times each lookup five times at 1 and
100 tickers, and at 1, 20, and 100 products.
