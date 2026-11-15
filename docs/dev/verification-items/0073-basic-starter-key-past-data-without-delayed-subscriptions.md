# 73. Whether past data reads on a Basic or Starter key without `delayed_subscriptions`

Date: 2026-10-31

Status: open

Only current-data datasets are checked for `DELAYED`. Settled by
`tests/namespaces/test_stocks_integration.py` (Stocks Basic).
