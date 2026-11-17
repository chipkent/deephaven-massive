# 138. Is the unified snapshot's `session.change` the option chain's `day.change`?

Date: 2026-11-16

Status: settled 2026-11-16: the unified snapshot's `session.change` and `session.change_percent` are the option chain's `day.change` and `day.change_percent`, 20 of 20 SPY contracts; a contract that traded late is not yet compared

Unknown: whether the unified snapshot's `session.change` and `session.change_percent` (the
`SessionChange` and `SessionChangePercent` columns of the unified snapshot datasets, such as
`m.options.snapshots`) carry the quantity of the option chain's `day.change` and
`day.change_percent` (`TodaysChange` and `TodaysChangePerc`, item 84). Massive describes both
as the price change from the previous trading day, and its samples agree: the unified
snapshot's option sample has a `session` with close 21.4, previous close 22.45, change -1.05,
and percent -4.67, the numbers of the option contract snapshot's `day` sample. But the session
also carries early, regular, and late trading changes, so `session.change` may count trades
that `day.change` does not.

Meanwhile the session columns take the names `TodaysChange` and `TodaysChangePerc` through
`overrides.SPELLINGS` entries, in `m.snapshots`, `m.options.snapshots`, and
`m.indices.snapshots`. The indices snapshot (`/v3/snapshot/indices`) documents its
`session.change` in the same words and serves the same `session` object of an index as the
unified snapshot, so it takes the same names; an index has no chain, so this check does not
read it.

Settled by `test_session_change_is_the_chain_s_day_change` in
`tests/massive/refresh/test_overrides_integration.py`, which reads up to 20
traded contracts of SPY's chain and then their unified snapshot, back to back, and compares
the two changes within about a cent and 0.01 percentage points. Run it during the regular
session and again after it ends, when late trades could separate the two. It gives the
contracts with a `late_trading_change` a verdict line of their own, or says none traded late.

The owner's run of 2026-11-16 compared 20 of 20 contracts and found them the same. That run
did not compare contracts that traded late on their own, so a run after the regular session
that sees one settles that case.
Massive's documentation agrees: its unified-snapshot samples give `change` as `close` minus
`previous_close`, with late trading reported apart in `late_trading_change`, and its
knowledge-base article on the stocks snapshot before the open calls the unified snapshot's
`change` "the same measure as todaysChange".
