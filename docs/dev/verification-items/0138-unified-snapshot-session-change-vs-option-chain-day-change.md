# 138. Is the unified snapshot's `session.change` the option chain's `day.change`?

Date: 2026-11-16

Status: open

Unknown: whether the unified snapshot's `session.change` and `session.change_percent` (the
`SessionChange` and `SessionChangePercent` columns of the unified snapshot datasets, such as
`m.options.snapshots`) carry the quantity of the option chain's `day.change` and
`day.change_percent` (`TodaysChange` and `TodaysChangePerc`, item 84). Massive describes both
as the price change from the previous trading day, and its samples agree: the unified
snapshot's option sample has a `session` with close 21.4, previous close 22.45, change -1.05,
and percent -4.67, the numbers of the option contract snapshot's `day` sample. But the session
also carries early, regular, and late trading changes, so `session.change` may count trades
that `day.change` does not.

Meanwhile the columns keep their two names. If the two are one quantity, the owner decides
whether the session columns take the names `TodaysChange` and `TodaysChangePerc` through an
`overrides.SPELLINGS` entry; if not, the names stay apart. The indices snapshot
(`/v3/snapshot/indices`) names its `session.change` the same way, but an index has no chain,
so this check does not read it.

Settled by `test_session_change_is_the_chain_s_day_change` in
`tests/massive/refresh/test_overrides_integration.py`, which reads up to 20
traded contracts of SPY's chain and then their unified snapshot, back to back, and compares
the two changes within about a cent and 0.01 percentage points. Run it during the regular
session and again after it ends, when late trades could separate the two.
