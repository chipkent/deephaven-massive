# 141. Is the open interest's `underlying_ticker` the contracts reference's `underlying_ticker`?

Date: 2026-11-16

Status: open

Unknown: whether `underlying_ticker` of `/options/v1/open-interest` and
`/options/v1/unusual-trades` ("The ticker of the contract's underlying (e.g., SPX). Weekly and
adjusted roots are folded in, so SPXW contracts report SPX and AMCR1 contracts report AMCR; see
root to tell them apart.") holds the same value as `underlying_ticker` of the contracts
reference, `/v3/reference/options/contracts/{options_ticker}` ("The underlying ticker that the
option contract relates to."), for the same contract, apart from an index's `I:` prefix, which
neither writes. Neither new endpoint sends a CFI code. Also unknown: whether the contracts
reference finds an expired contract as of a past day.

Meanwhile the two datasets publish `Underlying` as Massive's `underlying_ticker`, with `I:` added
when the contracts reference gives the contract's root the CFI code of an option on an index.
The library asks once per root and session, about the first contract of the root it meets, as
of that row's trading day, and again an hour later about a root it could not classify. If the
values differ for some contract, the datasets take the contracts reference's underlying instead
of the row's.

Verified by `test_underlying_matches_the_contracts_reference` in
`tests/namespaces/test_options_integration.py`. It reads up to two contracts each of the roots
`SPX`, `SPXW`, `NDX`, `VIX`, `RUT`, `DJT`, `AMCR1`, and `AAPL` from both endpoints, and two
expired `SPXW` contracts from a year earlier. It compares Massive's `underlying_ticker` with
`m.options.contracts()`'s `Underlying` apart from `I:`, and the library's `Underlying` on both
datasets with it exactly.
