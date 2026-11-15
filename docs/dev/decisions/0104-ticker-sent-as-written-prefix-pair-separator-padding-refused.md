# 104. A ticker is sent as written, and Massive judges it

Date: 2026-11-11

Status: accepted

The owner, on 2026-11-11: "I agree on not erroring on missing symbols. That should be a significant
simplification." The library judges no ticker's shape but its class's prefix: no character class or
option symbol pattern refuses or rewrites a value Massive could answer. The prefix stays because the
owner chose, on 2026-11-14, to "Restore only the asset-class prefix check. Each namespace requires
its documented prefix (`O:` for options, `X:` for crypto, `C:` for forex, `I:` for indices), and the
error names the expected prefix. Nothing else about a ticker's shape is checked. A ticker with the
right prefix that Massive has no data for still gives no rows instead of an error." An options,
index, forex, or crypto dataset refuses a symbol without its prefix, naming the prefix and an
example; stocks and futures have none, and a dataset of every class routes by the prefix instead. On
2026-11-14 the owner also asked, "Shouldn't it accept validly specified Massive symbols, and isn't
KIMpL such a valid symbol?", so case is never folded or judged, and answered "crypto pair: do (a),
refuse." for a pair written with a dash: a forex or crypto dataset refuses a pair with `-` or `/`
after its prefix (`C:EUR-USD`), naming the joined ticker (`C:EURUSD`), because a flat-file filter
and a WebSocket topic match only the joined one and would drop it with no error. A symbol with
whitespace at either end or a control character is refused too: Massive's WebSocket strips the
topics it confirms, so a padded topic is never confirmed. A ticker Massive has no data for is no
error: it gives no rows, and a watchlist read with `fetch()` or `history()` shows it in
`m.symbol_reads()`. The library refuses only a value of the wrong Python type, a symbol its class
does not spell that way, and one no request can carry: an empty or padded symbol, a control
character, or one holding `,` (Massive's list parameters and WebSocket subscriptions split on it) or
`*` (the WebSocket's every-ticker wildcard). Such a value raises `ValueError` in code, and a table's
row follows `MassiveConfig.unsendable_symbols`, which replaces `noncanonical_symbols`. A `/` is
sent, percent-encoded so it stays in its path segment, and Massive answers it (verification item
115). A row's ticker is kept as Massive writes it, except the spellings a transport writes
differently from the REST ticker (`EUR/USD` in WebSocket forex frames), which are read back into it.
The converters convert the spellings they know and return other text unchanged; a stock convention
still refuses a spelling Massive reads as another instrument (Yahoo's `VOD.L`, Nasdaq's `ABC.U`),
since that would read the other instrument's data with no error. `canonical_symbol_stock`,
`canonical_symbol_future`, and `canonical_symbol_future_spread`, which only checked text, are
removed, and so is `DatasetSpec.symbols_as_written`, since every dataset now keeps its symbols as
written. A crypto pair that Massive's crypto ticker list does not hold is a pair Massive has no data
for: it follows `unlisted_symbols` whether passed in code or in a table, so under `LOG`, the
default, a stream or read skips it and gives no rows (a followed read shows it `DONE` with 0 rows),
where a pair passed in code used to raise. A list the library cannot read at all affects every pair,
so it fails the call or stream under any setting. A wire spelling is derived only from a symbol with
Massive's prefix (`C:`, `X:`, `I:`); other text goes on the wire as written. The futures schedule
calendar looks every ticker up in Massive's contracts list, combos included, instead of telling a
contract by its shape. Supersedes 58's refusal and 66's `noncanonical_symbols`.
