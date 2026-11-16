# 73. Whether past data reads on a Basic or Starter key without `delayed_subscriptions`

Date: 2026-10-31

Status: retired 2026-11-16: its only check was removed with the second, delayed key the live tests no longer read (decision 121)

Only current-data datasets are checked for `DELAYED`. Settled by
`tests/namespaces/test_stocks_integration.py` (Stocks Basic).
