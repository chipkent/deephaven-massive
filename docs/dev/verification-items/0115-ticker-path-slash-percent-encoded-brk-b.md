# 115. What a path that takes the ticker answers for one holding `/`, sent as `%2F` (`BRK%2FB`)

Date: 2026-11-11

Status: open

Sent as written, and read as no rows on an empty 200 or a 404, until read. Verified by
`tests/namespaces/test_stocks_integration.py` (Stocks Basic); rows there would be silent wrong
data, and a refusal would return with that evidence.
