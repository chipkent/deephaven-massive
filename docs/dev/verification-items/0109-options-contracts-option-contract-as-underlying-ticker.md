# 109. What the options contracts list answers to an option contract as its `underlying_ticker`

Date: 2026-11-12

Status: open

Sent to Massive for it to judge; if it answers with no error, `m.options.contracts` should refuse a
contract as `underlying`. Settled by `tests/namespaces/test_options_integration.py`,
which sends `O:AAPL250117C00150000`.
