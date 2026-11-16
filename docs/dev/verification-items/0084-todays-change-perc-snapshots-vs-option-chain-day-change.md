# 84. Whether the v2 snapshots' `todaysChange` and `todaysChangePerc` (stocks, forex, and crypto snapshots and movers) are the option chain's `day.change` and `day.change_percent`: the latest price minus the previous day's close, and that change in percent of the close

Date: 2026-11-11

Status: settled 2026-11-12: the v2 snapshots' `todaysChange` and `todaysChangePerc` are the option chain's `day.change` and `day.change_percent`

Kept apart as `TodaysChange` and `TodaysChangePerc` until read. Settled by
`tests/massive/refresh/test_overrides_integration.py` (Stocks Starter and Options Starter), which
checks each side against that definition within about a cent and 0.01 percentage points.

Settled 2026-11-11 and 2026-11-12: five stocks' change was the latest price minus `prevDay.c`, SPY's
chain's `day.close` minus `day.previous_close`, and each percent 100 times the change over that
close; the option chain now names them `TodaysChange` and `TodaysChangePerc`, as the v2 snapshots
and movers do.
