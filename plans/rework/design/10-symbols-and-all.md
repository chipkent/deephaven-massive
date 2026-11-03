## 10. Symbols and `ALL`

```python
Symbols = str | Sequence[str] | Table | _All      # dm.ALL is the only _All instance; repr "dm.ALL", str "ALL"
```

- A `str` is one symbol. A `Sequence[str]` is materialized to a tuple at construction. An
  exhausted generator raises `ValueError` ("no symbols: the list is empty; ..."), naming the
  next step.
- **`symbols` is required on every market-data dataset, including every `snapshots()`.**
  `None` there raises `ValueError` when the query is built, naming the forms it takes and
  `dm.ALL` where the dataset accepts it. `ALL`
  is accepted by stream verbs (the `<channel>.*` topic), by every per-class `snapshots(ALL)`
  (full market, all pairs, all contracts), and by `history()` when a flat file or a grouped-daily
  REST path exists. Everywhere else `ALL` raises `ValueError` naming the alternative.
- **Canonical only.** Every symbol a dataset takes (`symbols=`, `underlying=`, every row of a
  symbol table) must be Massive's exact REST ticker, case kept: `AAPL`, `BRK.B`, `KIMpL` (Kimco's
  series L preferred: the lower-case `p` is meaningful), `O:BRKB250117C00400000`, `ESZ5`,
  `I:SPX`, `C:EURUSD`, `X:BTCUSD`. Anything else raises `ValueError` when the query is built
  (`check_symbol` in `_datasets/symbols.py`); the message names the canonical form when the
  class's converter gives one (`'EUR/USD' is not Massive's ticker; use 'C:EURUSD' (... and
  dm.canonical_symbol_forex converts)`). A canonical ticker of another class says which class
  the dataset takes (`'I:SPX' is an index ticker; this dataset takes stock tickers`). Every
  `Symbol` column holds exactly that form.
- **The grammar** of each class: a stock is an upper-case root of letters and digits followed
  by the CQS suffixes Massive keeps, `.X` (a class or another dotted suffix, `BRK.B`,
  `NTEST.Q`, one or more, as in the NYSE warrants `AAC.WS` and `NE.WS.A`), `p` and an optional series (`KIMpL`), and the markers `r` and `w`; an option is
  `O:` and an OCC symbol whose root is 1 to 6 letters or digits (no dot) and whose `YYMMDD`
  expiry is a real date; a single futures contract is a root of upper-case letters and digits,
  a month code, and a one- or two-digit year (`ESZ5`, `6CH5`, `0NGH39`; a year is never
  rewritten, and `ESZ25` is not `ESZ5`); a futures combo is the shape below; a futures dataset
  takes a contract or a combo, the same grammar `dm.canonical_symbol(text,
  dm.AssetClass.FUTURES)` checks, and refuses any other text (`'ESZ'`, `'esz5'`, `' ESZ5'`)
  with the converter's message. A futures row keeps any ticker as Massive writes it (not
  empty, no surrounding whitespace, no control character, no class prefix), so no row is lost
  to the grammar; an index is `I:` and letters and digits, as Massive's WebSocket pattern
  `^(I:[a-zA-Z0-9]+)$` writes it, with dotted parts allowed; a forex pair is `C:` and two
  three-letter ISO 4217 codes; a crypto pair is `X:` and the two codes joined, whatever the
  quote, at least four characters (a base and a three-letter quote), so a lone currency such as
  `X:BTC` is refused. A stream needs the WebSocket form (`BTC-USD`), and two REST paths take the pair as
  two codes (`/v1/last/crypto/{from}/{to}` and `/v1/open-close/crypto/{from}/{to}/{date}`), so
  the library splits a pair, never by a guess: the joined ticker does not say where its quote
  starts (`X:DOTUSD` could be `DOT-USD` or `DO-TUSD`; `X:ETHWBTC` ends in `BTC` but quotes in
  `WBTC`), so every split comes from Massive's list of crypto tickers
  (`/v3/reference/tickers?market=crypto`, every page, read with `active=true` and again with
  `active=false`, since the endpoint lists only traded tickers by default and a past date of a
  delisted pair needs its split too; a ticker in both keeps its listed row; each row's
  `base_currency_symbol` and `currency_symbol`, whose join must be the ticker); the library
  keeps no list of quote currencies of its own. The session reads that list once, on first
  need, through its REST client, and keeps it for its life; a failed read is not kept, so the
  next need reads it again. A stream needs it when it starts or when a symbol table adds a
  crypto ticker (on the session's `symbol changes` worker, section 18.1). At the start, a failed
  read refuses the stream with the read's `MassiveError`, and a ticker the list does not hold,
  or whose currencies do not join to it, refuses it with a `MassiveError` naming the ticker.
  That holds for a list in code. A pair of a symbol table, present at the call or added later,
  follows `MassiveConfig.unlisted_symbols` instead, as a misspelled row follows
  `noncanonical_symbols`: under `LOG`, the default, and `IGNORE` the pair is skipped, never
  subscribed, logged once (at WARNING under `LOG`), and, for a reactive table, listed in the
  stream's `Error` in `m.streams()` (its own warning part) until the table no longer holds it,
  while the stream keeps its other pairs; a table whose pairs are all unlisted raises at the
  call. Under `RAISE` such a pair raises at the call and fails the stream when added later. A
  skipped pair stays skipped until its row is deleted and added again. For a pair added later,
  a failed read of the list is retried for the change's whole batch of tickers, after 2 s and
  8 s (`PAIR_RETRY_DELAYS`), each try reading the list at most once for the tickers not yet
  split; the tickers still unsplit after the last try are then treated as unlisted, and the
  `Error` says the list could not be read. A rejected key or a plan without reference data
  ends the retries at once. At the call there is no retry: a failed read treats the pairs it
  could not split as unlisted, and the stream's worker looks a reactive table's skipped pairs
  up again, with the retries, as it subscribes the table's rows. Nothing
  falls back to a guess. On the two REST paths a pair in code raises the same refusal from
  `fetch()` and `poll()`; a pair of a symbol table follows `MassiveConfig.unlisted_symbols`
  (decision 90). Every other REST
  path and every flat file needs no split: a flat-file filter looks for a crypto pair split
  at every point. Every grammar is
  matched against the whole text, so a trailing newline never passes. A dataset that spans classes
  (indicators, reference data, the unified `m.snapshots()`) declares the internal
  `ClassFromPrefix.ANY` in place of an `AssetClass`, never None: the prefix selects the
  grammar (`O:`, `I:`, `C:`, `X:`), a symbol without one is a stock, and its days are New
  York days. The symbol rules (`is_canonical`, `check_symbol`, `canonical_from_wire`,
  `wire_spellings`) and the day rules take `AssetClass | ClassFromPrefix`; `ClassFromPrefix`
  is not exported, since no public signature takes it.
- **Futures combos.** A combo is a spread, strip, pack, or bundle: Massive's contracts list
  returns them with `type: 'combo'`, by default. Massive documents no combo grammar, and its
  live lists show more forms than its samples (`1Y:SA 03M F6`, `10YF5-10YG5XXX` beside
  `GCQ6-GCZ6`, `BOB 02-01 U6`, `CL:BF Q6-U6-V6`, `6A:XF:AUDUSD:Q6`), so the forms are not
  enumerated. A combo is any run of upper-case letters and digits joined by single `-`, `:`,
  or spaces, exactly as written. The reason: every ticker Massive itself writes must
  round-trip, from a row to `symbols` and to a symbol table, while whitespace, lower case, an
  empty part, and a truncated contract (`ESZ`, one token with no year, so no combo) are
  garbage and refused. No function builds a combo ticker from its legs: Massive's grammar is
  unverified, so a builder waits for a live check
  ([section 23](23-open-verification-items.md#23-open-verification-items)).
- **The converters** are the public way to another spelling. `dm.canonical_symbol(text,
  asset_class, *, convention=SymbolConvention.MASSIVE)` takes a required `AssetClass`: the class is never guessed,
  so `SPX` becomes `I:SPX` only because the caller names the class; for `FUTURES` it takes a
  single contract or a combo, as strictly as `canonical_symbol_future` and
  `canonical_symbol_future_spread`, and refuses other text with `canonical_symbol_future`'s
  message. One function per class
  (`canonical_symbol_stock`, `_option`, `_future`, `_index`, `_forex`, `_crypto`), the spread
  function `canonical_symbol_future_spread`, and one per
  stock convention (`canonical_symbol_stock_yahoo`, `_nasdaq`, `_cms`) take only the text, for
  Deephaven query strings: a formula cannot pass a keyword argument, and imported by bare name
  (`from deephaven_massive import canonical_symbol_forex`) they make a `String` column and are
  vectorized; called through the module (`dm.canonical_symbol_forex(Pair)`) Deephaven types the
  column `Object`, so the docstrings say to cast `(String)`. Each maps None to None, returns
  canonical text unchanged, converts its spellings (options: no `O:`, the space-padded OSI form,
  any case; indices: no `I:`; forex: `EUR/USD`, `EUR-USD`, `EURUSD`, with or without `C:`, any
  case; crypto: `BTC-USD`, `BTC/USD`, `BTCUSD`, with or without `X:`, any case, any quote;
  futures: nothing), and raises `ValueError` with a hint for anything else.
  `canonical_symbol_future` refuses a spread naming `canonical_symbol_future_spread`, and the
  other way round. A ticker with another class's prefix names that class's function (`'C:EURUSD'
  is a forex ticker (prefix C:), not a crypto one; call dm.canonical_symbol_forex`). Stock,
  index, and futures text is never re-cased, and nothing is guessed: `aapl` raises saying that
  tickers are case-sensitive, with an upper-case root and possibly a lower-case suffix. The
  converters are pure and thread-safe.
- **Stock conventions.** `SymbolConvention.MASSIVE`, the default of every `convention`
  parameter and the only member every class takes, is Massive's own spelling: canonical text
  passes unchanged and a class's unambiguous spellings convert (`EUR/USD`, `SPX` for the
  indices class). A stock spelling converts only from a named stock convention (`YAHOO`,
  `NASDAQ`, `CMS`), because `-` means a class in Yahoo's and a preferred in Nasdaq's; under
  `MASSIVE`, `-`, `/`, and a space raise a hint naming the conventions, and a stock convention
  for another class raises `ValueError`. No `convention` parameter takes None. Each converts exactly its plain tickers, share
  classes, and preferreds, and refuses the rest, since Massive documents no spelling for
  warrants, units, rights, when-issued, when-distributed, called, or test issues:

  | Kind | `YAHOO` | `NASDAQ` (Integrated Platform) | `CMS` | Massive |
  |---|---|---|---|---|
  | Class | `BRK-B` | `BRK.B` | `BRK B` | `BRK.B` |
  | Preferred with a series | `BAC-PL` | `BAC-L` | `BAC PRL` | `BACpL` |
  | Preferred without a series | refused (`-P` also reads as class P) | `DCOM-` | `DCOM PR` | `DCOMp` |
  | Plain | `AAPL` | `AAPL` | `AAPL` | `AAPL` |

  Under a convention only its grammar applies: text that happens to be a Massive ticker is not
  passed through, since the convention may mean something else by it. A Yahoo ticker with a
  dot (`VOD.L`, `SHOP.TO`) is refused as a listing outside the US, and Nasdaq's `ABC.U` (class
  U) is refused rather than read as Massive's unit `ABC.U`. Class `U` is refused in every
  convention (CQS and CMS write units as `U`). CQS is not a member: it is Massive's own
  spelling of classes and preferreds. Nasdaq's ACT symbology is not
  supported: it writes `.W` for warrants and `.V` for both a class and when-issued.
- **`dm.canonicalize_symbols(table, asset_class, *, column="Symbol",
  convention=dm.SymbolConvention.MASSIVE, on_error=dm.OnError.RAISE)`**, in
  `_tables/symbols.py`, is the table form of the converters. It keeps its own `on_error`
  argument, apart from the session settings below, since it is a standalone function that
  needs no session. The table's
  current rows are converted in Python first (one call per distinct value), static or
  refreshing, so `RAISE` gives a clean `ValueError` listing up to five values; every table is
  then converted by `update`, so under `RAISE` a later bad row of a refreshing table fails the
  table. `OnError.LOG` writes a null symbol and logs each bad value once at WARNING, at most
  20 per table; `OnError.IGNORE` writes a null symbol and logs nothing above DEBUG. The
  result keeps every other column, and the converted column keeps its name.
- **A symbol table** must have a `Symbol` column, and every non-null row should be canonical.
  What a row that is not does follows `MassiveConfig.noncanonical_symbols`
  ([section 5](05-massiveconfig.md#5-massiveconfig)), an `OnError` whose default is `LOG`; a `str` or a sequence
  passed as `symbols` in code always raises, whatever the setting, since a typo in code is
  fixed in code.
  A refreshing table is reactive: streams listen to it and subscribe or unsubscribe as rows are
  added or removed; polls re-read it each cycle; `fetch()` and `history()` read each added
  symbol and drop each removed one, returning a refreshing table
  ([section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)). A static (non-refreshing) table is a
  fixed symbol list: read once when the verb is called, as `fetch()` and `history()` read it,
  it opens the same stream as the equal symbol list, and one holding no symbol raises
  `ValueError`. `poll()` re-reads it each
  cycle, which for a static table returns the same symbols every time. Every verb reads the
  table's current rows first (`read_symbols` in `_tables/watchlist.py`); a static verb raises
  `ValueError` for a table without symbols. A null row is ignored. The setting governs every
  row read from the table, present at the call or added later, for streams, `poll()`, and
  followed reads alike:
  - **`RAISE`**: a bad row present at the call raises `ValueError` at once. A **later** bad
    row is never subscribed or requested: it fails that result (the stream through its
    session's reaper, a `poll()` table with `SymbolRowError`, a followed read through the
    session's follow coordinator) and logs at ERROR, with a message
    that names the row, the reason, and the fix: correct the row, or pass the table through
    `dm.canonicalize_symbols(table, dm.AssetClass.<CLASS>, on_error=dm.OnError.LOG)` (for a
    dataset that spans classes, `dm.canonical_symbol` per row with each row's class).
  - **`LOG`** and **`IGNORE`**: a bad row is skipped, never subscribed or requested, and the
    result keeps running on the good rows. `LOG` logs each distinct value once at WARNING
    (bounded, at most 100 per process), naming the row, the reason, and the fix; `IGNORE` logs
    it at DEBUG only. A stream's, a `poll()` table's, or a followed read's `m.streams()` row
    lists the values
    skipped now in `Error` (`skipping 2 symbol-table rows that are not Massive's tickers:
    'aapl', 'EUR/USD'`, the count and the first five), and the text is cleared once none is
    left. Fixing or deleting a row clears it, and a fixed row subscribes (a stream) or is
    requested on the next cycle (a poll) like any added row. A table whose current rows are all
    bad (at least one row, and no good one) raises `ValueError` at the call rather than start a
    result that follows nothing; a later moment with only bad rows simply follows no symbol.
  A stream also refuses a row whose topic cannot be built. A symbol table that **fails** (a
  `canonicalize_symbols` table under `RAISE` that met a bad row) fails the result too: the
  watch's error callback queues the stream's failure on the reaper, and `read_symbols` refuses a failed table, so a
  `poll()` table fails on its next cycle. `last_by()` filters against the table itself, with
  `watched_rows`.
- **Datasets kept as written.** A dataset whose symbols are a third party's global tickers
  rather than Massive's US-market ones declares `DatasetSpec.symbols_as_written`: its
  `symbols`, the rows of its symbol table, and its rows' symbols are taken exactly as Massive
  writes them (not empty, no surrounding whitespace, no control character, no class prefix).
  Only `partners.tmx.corporate_events` does: it is Wall Street Horizon's global events calendar,
  whose events name their venue's MIC and an ISIN (`rest/partners/tmx/corporate-events.md`).
  Benzinga covers US-listed companies and ETF Global US-listed ETFs, so both keep the stock
  grammar; ETF Global's `ConstituentTicker`, which may name a foreign holding, is not a key and
  is never checked.
- **Wire spellings.** Massive writes some symbols differently per transport: `EUR/USD` in
  WebSocket forex frames, `EUR-USD` in forex topics, `BTC-USD` in crypto frames, `X:BTC-USD`
  in FMV frames, `X:EUR-USD` in the forex quotes flat-file sample. `canonical_from_wire` reads
  every spelling the library knows, without folding case, any crypto quote included; a futures
  row keeps its ticker as written; topics use `ws_symbol`
  (`C.EUR-USD`, as the WebSocket documentation gives every forex channel's `ticker`
  parameter). A flat-file filter holds every spelling of each requested symbol
  (`wire_spellings`): Massive's knowledge base says the crypto and forex files write
  `X:BTC-USD` and `C:EUR-USD`
  (https://massive.com/knowledge-base/article/why-doesnt-a-bar-from-the-rest-api-match-the-flat-files),
  and the forex quotes sample shows `X:EUR-USD`, so a request for `X:BTCUSD` or `C:EURUSD`
  matches those rows, which hold the canonical ticker; a crypto pair is looked for split at
  every point, since its joined ticker does not say where its quote starts.
- **The index prefix comes from the endpoint, never from the name.** A ticker without `I:` on
  a dataset of the indices class is read as `I:` and the ticker; on any other dataset it is a
  stock. No conversion looks a name up (`SPX` is never recognized as an index by its name).
  Which index endpoints send the bare form is a live check ([section 23](23-open-verification-items.md#23-open-verification-items)).
- **Unreadable wire spellings.** What a symbol Massive sends that `canonical_from_wire`
  cannot read does follows `MassiveConfig.unreadable_symbols` ([section 5](05-massiveconfig.md#5-massiveconfig)),
  an `OnError` whose default is `LOG`. Such a spelling is a gap in the library's grammar, so it
  is a library defect, never the user's. Under `LOG`, the default, the row is kept with a null
  `Symbol` and the text in the `RawSymbol` column, and each spelling is logged once at ERROR
  with the library-bug text (at most 100 per process), so every table keeps running and the
  gap is reported. `IGNORE` keeps the row the same way and logs it at DEBUG only. Under
  `RAISE` it fails, naming the spelling, the setting, and where to report the gap: a REST
  request raises `RowDataError` (a `poll()` table fails), a flat-file read raises
  `MassiveError`, and a WebSocket row fails every stream of the dataset on the channel (it
  cannot be routed to one, so no symbol stream may starve on it silently). Under `LOG` and
  `IGNORE` a WebSocket row with a null `Symbol` reaches only the `dm.ALL` streams of its
  channel, since a symbol stream cannot claim it. A `poll()` table keyed on `Symbol` keys on
  `Symbol` and `RawSymbol` together, so each unreadable spelling is its own row: it neither
  fails the null-key check nor merges with another, and `REPLACE` removes it when it leaves.
- `Symbol` is always the instrument the row describes. When the fan-out key is a different
  instrument, it is `Underlying`, in the canonical form (an options contracts row's bare
  index underlying gets its `I:` by its CFI code, [section 8.2](08-namespace-tree.md#82-moptions)): on the
  options rows that carry one (the chain and the snapshots from `underlying_asset.ticker`, the contracts from
  `underlying_ticker`; WebSocket and flat-file options rows carry none) and on
  `related_companies` rows. A non-instrument fan-out key
  keeps the generated column the dataset declares as its `fanout` key (`filer_cik` becomes
  `FilerCik`), declared with `KeySpec(..., instrument=False)`
  ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). Where a source does not carry the instrument in its rows (it takes the symbol
  as a path parameter, as `/v3/trades/{t}` and `/v2/aggs/ticker/{t}/…` do, or holds it outside
  the result key, as `/v1/last/crypto` and `/v1/last_quote` do), `Symbol` is filled from the
  request ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). A dataset whose rows describe no instrument
  (`reference.exchanges`, `economy.*`) has no `Symbol` column. A dataset whose rows each list
  several tickers (the three financial statements, Forms 3 and 4, insider activity and
  holdings, the 8-K disclosures, `reference.news`, and `partners.benzinga.news`) keeps its
  `Tickers` column (`String[]`) and gets a `Symbol` the library fills
  (`DatasetSpec.ticker_list`, decision 90): each ticker the request named that the row
  lists, the row repeated once per such ticker. An article about AAPL and MSFT read for both
  is two rows, `Symbol` AAPL and `Symbol` MSFT. A request that named one ticker gives it to
  every row; a row that lists none of a request's several named tickers keeps a null
  `Symbol`, never dropped, with one WARNING per dataset a session; `symbols=None` leaves it
  null. Requests stay batched (`tickers.any_of`). The datasets' `poll()` keys add `Symbol`,
  which may be null. `Symbol` is never a list.
