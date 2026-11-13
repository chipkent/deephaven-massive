# 84. Whether the v2 snapshots' `todaysChange` and `todaysChangePerc` (stocks, forex, and crypto snapshots and movers) are the option chain's `day.change` and `day.change_percent`: the latest price minus the previous day's close, and that change in percent of the close

Date: 2026-11-11

Status: open

Kept apart as `TodaysChange` and `TodaysChangePerc` until read. Settled by
`tests/massive/refresh/test_overrides_integration.py` (Stocks Starter and Options Starter), which
checks each side against that definition within about a cent and 0.01 percentage points.
