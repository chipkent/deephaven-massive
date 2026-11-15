# 104. What the contract overview answers to a stock ticker (`AAPL`)

Date: 2026-11-11

Status: settled 2026-11-12: HTTP 404 "Option Ticker not found."; `m.options.contracts("AAPL")` still raises before any request, for its missing `O:` prefix (decision 104)

`m.options.contracts` refuses an underlying passed as `symbols` until read. Settled by
`tests/namespaces/test_options_integration.py` (Options Basic).
