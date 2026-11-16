# 125. How long the futures calendar's contract and schedule lookups take

Date: 2026-11-15

Status: settled 2026-11-16: one contract or schedule lookup takes about 0.1 s; 100 tickers without `date` take 14 s or fail with HTTP 504

A streamed futures contract's first rows wait for two lookups, its product and then its
product's schedule; how long each takes, and how that grows with the tickers or products in one
request, is unmeasured, so the waits the calendar allows rest on no evidence. Settled by
`tests/ingest/test_futures_calendar_integration.py::test_contract_and_schedule_lookup_latency`, which times each lookup five times at 1 and
100 tickers, and at 1, 20, and 100 products.

Evidence, 2026-11-16, two runs. The contract lookup of 1 ticker took a median of 0.08 s and
0.16 s. Of 100 tickers, which the calendar sends with no `date`, it took 14 to 16 s for its
first 4 pages in four of five runs and 0.45 s in the fifth, and in the later run answered HTTP
504 after the client's retries. The schedule lookup took a median of 0.08 s for 1 product,
0.08 s for 20, and 0.51 s for the first 4 pages of 100. Item 130 times the same contract lookup
with `date`: 0.12 s for 100 tickers.
