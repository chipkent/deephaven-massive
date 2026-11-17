# 141. Is the open interest's `underlying_ticker` the contracts reference's `underlying_ticker`?

Date: 2026-11-16

Status: open

Unknown: whether `underlying_ticker` of `/options/v1/open-interest` and
`/options/v1/unusual-trades` ("The ticker of the contract's underlying (e.g., SPX). Weekly and
adjusted roots are folded in, so SPXW contracts report SPX and AMCR1 contracts report AMCR; see
root to tell them apart.") holds the same value as `underlying_ticker` of the contracts
reference, `/v3/reference/options/contracts/{options_ticker}` ("The underlying ticker that the
option contract relates to."), for the same contract. The contracts dataset writes that field as
`Underlying`, with `I:` restored on an index from the contract's CFI code; the two new endpoints
send no CFI code.

Meanwhile the new datasets keep Massive's text as `UnderlyingTicker`, with a note that an index
is bare there. If the values agree for every contract, the owner decides whether the column
takes one name with the contracts' `Underlying`, which would then need the `I:` the new
endpoints do not mark; if they differ, the names stay apart.

Settled by `test_underlying_ticker_against_the_contracts_reference` in
`tests/namespaces/test_options_integration.py`, which reads the open interest of a standard
(`SPY`), a weekly (`SPXW`), and an adjusted (`AMCR1`) root and the contracts reference of two
contracts of each, and records both values side by side.
