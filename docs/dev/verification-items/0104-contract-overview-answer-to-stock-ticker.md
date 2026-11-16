# 104. What the contract overview answers to a stock ticker (`AAPL`)

Date: 2026-11-11

Status: settled 2026-11-12: HTTP 404 "Option Ticker not found." (`live-evidence-20261112T002605Z.log`, `test_massive_answers_contract_details_of_a_stock_ticker`, request_id a561e437ab448e39ef4334f06a51a742); `m.options.contracts("AAPL")` still raises before any request, since `AAPL` is not an option symbol (decision 58)

`m.options.contracts` refuses an underlying passed as `symbols` until read. Settled by
`tests/namespaces/test_options_integration.py`.
