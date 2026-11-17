# 143. Is every index option's bare underlying a ticker on Massive's index list?

Date: 2026-11-17

Status: open

Unknown: whether, for every option on an index, the bare underlying Massive writes (`SPX` for
`I:SPX`) is the ticker of an index Massive lists under `/v3/reference/tickers?market=indices`
(without its `I:`). Nothing in Massive's documentation ties the options underlyings to that list.

Meanwhile the options open interest and unusual trades read that list once per session and treat
a root whose bare underlying is not on it as a stock's, with no further request; only a root
whose underlying is on it is asked about through the Contract Overview. If an index option's
underlying were missing from the list, its rows would show the bare ticker instead of `I:`, and
the pre-filter would have to go, at one Contract Overview request per root.

Verified by `test_index_option_underlyings_are_on_the_index_list` in
`tests/namespaces/test_options_integration.py`. It reads Massive's whole index list, then up to
20 contracts each of the underlyings `I:SPX`, `I:NDX`, `I:VIX`, `I:RUT`, `I:XSP`, `I:DJX`, and
`I:OEX` from `m.options.contracts()`, and asserts that every contract whose CFI code marks an
option on an index names a listed index. It also records the list's pages and how many roots of
one whole-market page of open interest name a listed index: the requests a first whole-market
read makes.
