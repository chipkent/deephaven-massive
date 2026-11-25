# 146. Does the options greeks endpoint take and write an index underlying bare?

Date: 2026-11-24

Status: open

Unknown: whether `/options/v1/analytics` (`m.options.greeks()`) takes an index underlying as
the contracts list does, bare (`underlying_ticker=SPX`) rather than as `I:SPX`, and writes it
bare in its rows' `underlying_ticker`, as the open interest and the unusual trades do. Massive's
page describes the parameter and the field only as "Underlying symbol", and its sample shows a
stock (`AMD`).

Meanwhile the library sends `underlying=` bare for an index, as it does to the contracts list,
and reads the rows' underlying through the per-root lookup that gives `SPX` its `I:`
(`underlying_by_root`). If Massive took `I:SPX` instead, `underlying="I:SPX"` would read no
rows; if it wrote `I:SPX`, the lookup would find nothing to change.

Settled by `test_greeks_take_and_write_an_index_underlying_bare` in
`tests/namespaces/test_options_integration.py`. It reads one page of greeks for
`underlying_ticker=SPX` and one for `underlying_ticker=I:SPX` through the wire client, and
`m.options.greeks(underlying="I:SPX")` through the library, and records each page's row count
and first row's `ticker`, `root`, and `underlying_ticker`, and the library's `Symbol`,
`OptionRoot`, and `Underlying` of each row.
