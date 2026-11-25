# 147. Does the contracts list as of a past day give a contract, with its CFI code, for an index underlying?

Date: 2026-11-24

Status: open

Unknown: whether `/v3/reference/options/contracts?underlying_ticker=SPX&limit=1&as_of=<day>`
answers with a contract that carries a `cfi` code when `<day>` is a past trading day, such as a
year back, with `expired` left at Massive's default of false. Massive documents `as_of` as "a
point in time for contracts as of this date" and `expired` as "Query for expired contracts.
Default is false.", and neither page says whether a contract listed on that day but expired
since is still found as of it.

Meanwhile `m.options.open_interest_by_underlying()` asks that way, once per bare ticker on
Massive's index list, as of the first row's `Date` (`_ingest/option_roots.py`,
`OptionRootKinds.underlying_kind`), and withholds the ticker's rows as `unreadable_symbols`
when the page has no contract or no code. If `as_of` alone did not reach expired contracts, a
past day's read of an index would be withheld, and the lookup would need `expired=true`
beside `as_of`.

Settled by `test_open_interest_by_underlying_names_an_index_as_the_contracts_reference_does` in
`tests/namespaces/test_options_integration.py`. It reads one page of one contract for `SPX`
and for `AAPL` as of the latest day and as of a year earlier through the wire client, and
records each page's row count and its first row's `ticker` and `cfi`, and then reads
`m.options.open_interest_by_underlying(["I:SPX", "AAPL", "I:DJT"])` for both days through the
library and records each row's `Symbol` and `Date` with the problems it reports.
