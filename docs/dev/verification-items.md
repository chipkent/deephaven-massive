# Verification items

Facts about Massive (and two about Deephaven) that the documentation does not settle. Each one
gets the conservative handling until a live check settles it. Live tests name the items they
answer with `@pytest.mark.verifies("N")` (the test asserts the answer the library assumes) or
`@pytest.mark.settles("N")` (the test records evidence that a person reads), and the live-run
report lists items by number. `tests/live/test_summary.py` fails when a marker names a number
this file does not list. [`docs/live-tests.md`](../live-tests.md) says how the live tests run.

Numbers are permanent: a new item takes the next free number, and a settled item keeps its
number with a one-line settlement. Items 1 to 81 are condensed from the design documents this
file replaced; their full evidence is in git history, in the commit before the one that added
this file.

## Open

Each open item says what is unverified, the handling until it is settled, and the live test that
settles it.

- **2.** Unit of REST forex `/v1/last_quote` `timestamp`, crypto `/v1/last` `timestamp`, and
  forex `/v1/conversion` `last.timestamp`. Read as milliseconds, as the spec tags them; a
  mismatch fails the page loudly. Settled by `tests/datasets/test_timestamps_integration.py`.
- **3.** Unit of WebSocket `FMV` `t` (documented ns, sampled ms). Read by magnitude, the unit
  logged once. Settled by `tests/namespaces/test_crypto_integration.py` (Currencies Business).
- **6.** The TICK host of a base Business key. `socket.massive.com`, as for any snapshot without
  a `timeframe`; a refusal names `websocket_hosts`. Settled by
  `tests/ingest/test_host_decision_integration.py` (Business key).
- **8.** What Massive says when a delayed plan subscribes on the real-time feed. Read as any
  refusal, failing the streams with Massive's text and the `delayed_subscriptions` fix. Settled
  by a delayed key's capture, as `docs/live-tests.md` says.
- **10.** The subscribe frame size limit. At most 1,000 topics per frame. Settled by
  `tests/massive/ws/test_client_integration.py`.
- **11.** Whether the options cap counts contracts or topics. Topics, the stricter count. Settled
  by `tests/massive/ws/test_client_integration.py` (Options Advanced).
- **13.** `orjson` free-threaded wheels. An optional extra; the standard-library codec is the
  default. Checked by CI's 3.14 job; no cp314t wheel exists yet.
- **15.** Whether a wildcard and an explicit topic of one channel (`T.*` and `T.AAPL`) deliver an
  event twice. Explicit topics are off the wire while the wildcard is wanted; transitions accept
  a brief duplicate. Settled by `tests/massive/ws/test_client_integration.py`.
- **16.** Whether a missing flat-file key answers 404 `NoSuchKey`. Only that is a missing file;
  any other 404 raises and a 403 is an entitlement failure. Settled by
  `tests/massive/flatfiles/test_client_integration.py`.
- **18.** Whether the Deephaven web UI shows a table's `TableDescription`. Set on every result
  anyway. No live test: checked by hand in a local web UI.
- **21.** Unit of WebSocket `LULD` `t` (documented ms, sampled with 19 digits). Read by
  magnitude. Settled by `tests/namespaces/test_stocks_integration.py`.
- **22.** Unit of flat-file `us_indices/values_v1` `timestamp` (documented ms, 19-digit rows).
  Read by magnitude. Settled by `tests/ingest/test_flatfiles_integration.py`.
- **26.** Whether Massive reads a percent-encoded colon in a path (`I%3ASPX`) as `I:SPX`. Every
  path value is percent-encoded whole; a value with `/` is refused. Settled by
  `tests/namespaces/test_indices_integration.py`.
- **35.** How bars of `multiplier` above 1 align. Intraday bars align to `{from}` (seen
  2026-11-02); longer bars to period starts. Settled by the stocks, forex, and crypto
  integration tests.
- **38.** Whether crypto range and `/prev` 1-day bars cover the UTC day against a minute
  rebuild. Item 1's rule applies. Settled by `tests/namespaces/test_crypto_integration.py`.
- **42.** Whether `IssuerCik` and `TransactionDate` key insider activity, and `IssuerCik` and
  `OwnerCik` insider holdings. `poll()` keys on them; a null key fails the table. Settled by
  `tests/namespaces/test_stocks_integration.py`.
- **45.** `Underlying` of the options datasets for `BRK.B`, index roots, and adjusted contracts.
  Massive's field in canonical form; nothing maps a root to an underlying. Settled by
  `tests/namespaces/test_options_integration.py`.
- **49.** Whether Massive's crypto ticker list holds every pair its streams and pair paths serve.
  A pair is split only from that list, reread at most hourly; an unlisted pair follows
  `unlisted_symbols`. Settled by `tests/namespaces/test_crypto_integration.py`.
- **52.** The stamps of index 1-day and longer bars on every source, on full and short days. A
  known stamp moves to 00:00 New York time; any other is kept with a null `Date` and a WARNING.
  Settled by `tests/namespaces/test_indices_integration.py`.
- **53.** Split adjustment across NVDA's 2024-06-10 split. `adjusted=True` is REST only; flat
  files and `adjusted=False` are as traded. Settled by
  `tests/namespaces/test_stocks_integration.py`.
- **54.** Whether every page echoes `adjusted`. A missing echo warns once; a different one
  raises. Settled by `tests/namespaces/test_stocks_integration.py`.
- **55.** That Massive never adjusts for cash dividends (COST, 2023-12-27). Nothing adjusts for
  dividends. Settled by `tests/namespaces/test_stocks_integration.py`.
- **56.** Whether `adjusted=true` applies `stock_dividend` events. Documented as unknown.
  Settled by `tests/namespaces/test_stocks_integration.py`.
- **57.** Whether option bars are fully adjusted for underlying splits. Options take `adjusted`
  like stocks. Settled by `tests/namespaces/test_options_integration.py`.
- **58.** Futures session labels: the 16:45 CBOT pre-open's session and when the next session's
  schedules are published. REST labels flip at 17:00 Chicago (seen 2026-10-31); stream rows use
  that rule corrected by the schedules, else a null `Date`. Settled by
  `tests/namespaces/test_futures_integration.py`.
- **60.** Whether an index bar of a day or longer covers the New York or the Chicago day. It ends
  at 01:00 New York time. Settled with item 52.
- **61.** The stamps of indicator values of a day or longer per class. Checked as the class's
  range bars; any other stamp is kept with a WARNING. Settled by the stocks and crypto
  integration tests.
- **65.** The exact text of Massive's 429 for a Basic subscription. A case-insensitive substring
  match teaches the bucket it is Basic. Settled by `tests/massive/rest/test_client_integration.py`
  (a class on Basic).
- **66.** Whether Massive counts reference and cross-asset requests in a bucket of their own, at
  the Basic limit. The library's side was settled 2026-11-02; Massive's needs a class on Basic.
  Settled by `tests/massive/rest/test_client_integration.py`.
- **67.** Whether a refused request counts against Massive's minute. The window is the clock
  minute (2026-10-30); a slot is held 61 s after each response, which is never refused. Settled by
  `tests/massive/rest/test_client_integration.py`.
- **68.** Whether every Benzinga consensus-ratings row carries `ticker`. A row without one fails
  the call. Settled by `tests/namespaces/test_partners_integration.py` (Benzinga add-on).
- **69.** Whether a delayed futures plan's REST snapshot says `DELAYED`. Without it, only
  Massive's refusal on the real-time feed reveals a delayed plan. Settled by a delayed futures
  key, as `docs/live-tests.md` says.
- **71.** Whether a delayed options plan's chain snapshot says `DELAYED`. Without a label the
  table shows `Recency` `UNKNOWN`. Settled by `tests/ingest/test_host_decision_integration.py`
  (Options Starter).
- **73.** Whether past data reads on a Basic or Starter key without `delayed_subscriptions`.
  Only current-data datasets are checked for `DELAYED`. Settled by
  `tests/namespaces/test_stocks_integration.py` (Stocks Basic).
- **76.** Fields Massive sent on 2026-11-02 that its spec omits (options `decimal_size` and
  `ds`, forex `vw`, unified snapshot session times, Forms 3 and 4 `issuer_trading_symbol`).
  Generated from `UNDECLARED_FIELDS`, stale once Massive declares them. Watched by
  `tests/datasets/test_catalog_integration.py`.
- **77.** Undocumented keys seen 2026-11-02 (`max_ticker`, `min_ticker`; `i` on `C`; `lp`, `ls`
  on `XQ`). The financial keys are ignored; the stream keys stay drift, warned once. Watched by
  `tests/datasets/test_catalog_integration.py`.
- **80.** Which day each index day source covers across many indices, and whether index files
  match REST. Items 52 and 60 stand until read. Settled by
  `tests/namespaces/test_indices_integration.py` (Indices Starter and S3).
- **81.** How Massive buckets futures bars of a session or longer: the week grid's first day,
  whether a grid moves with an odd `{from}`, clipping at `{from}`, and weekend sessions. Item 40's
  rule stands until read. Settled by `tests/namespaces/test_futures_integration.py` (Futures
  Starter).
- **82.** Whether Massive acknowledges `AD.*` on `socket.massive.com` for Options Advanced, what
  its admin messages carry, and what it answers a plan without them. The channel is read as
  documented: real-time only, every exchange at once, the text as sent. Verified by
  `tests/namespaces/test_options_integration.py` (Options Advanced, and a plan without the
  channel).
- **83.** The unit of the options snapshot's `greeks.gamma`: per $0.01 move of the underlying, as
  Massive's description and the dataset page say, or per $1, the textbook unit. Passed through as
  sent, with Massive's description. Settled by `tests/namespaces/test_options_integration.py`
  (Options Starter), which records each contract's ratio to Black-Scholes gamma from its own
  inputs.
- **107.** Whether Massive's futures `day` bars are its `session` bars at every multiplier.
  `resolution=1day` answered with the same shape as `1session` (item 100), so `Timespan.DAY` is
  sent as `day` and its bars are dated and kept as session bars. Settled by
  `tests/namespaces/test_futures_integration.py` (Futures Basic), which compares `1day` with
  `1session` and `5day` with `5session` on the front E-mini contract, and records the library's
  `DAY` bars beside them. `tests/queries/test_choice_integration.py` records what the futures
  aggregates answer to a multiplier below 1 (`0session`), which the library sends for Massive to
  judge.
- **108.** What the option chain answers to an option contract as its underlying. Sent to
  Massive for it to judge; if it answers with no error, the chain should refuse a contract.
  Settled by `tests/namespaces/test_options_integration.py` (Options Starter), which sends
  `O:AAPL250117C00150000`.
- **109.** What the options contracts list answers to an option contract as its
  `underlying_ticker`. Sent to Massive for it to judge; if it answers with no error,
  `m.options.contracts` should refuse a contract as `underlying`. Settled by
  `tests/namespaces/test_options_integration.py` (Options Basic), which sends
  `O:AAPL250117C00150000`.
- **115.** What a path that takes the ticker answers for one holding `/`, sent as `%2F`
  (`BRK%2FB`). Sent as written, and read as no rows on an empty 200 or a 404, until read. Verified
  by `tests/namespaces/test_stocks_integration.py` (Stocks Basic); rows there would be silent
  wrong data, and a refusal would return with that evidence.
- **117.** Why a later page of a paged REST read answers HTTP 404. The library assumes the
  page's cursor expired or its page moved, so a new call, with a new cursor, may succeed:
  `CutShortError` is retryable, and a followed read reads the symbol again. Settled by
  `tests/massive/rest/test_client_integration.py` (any Basic plan), which asks for one
  `next_url` of the ticker list at once and again after 90 seconds and records both answers.
- **118.** What Massive answers to a filter list with an empty item (`ticker.any_of=AAPL,,MSFT`).
  Sent as written; if Massive reads the empty item as no filter, the library should refuse it.
  Settled by `tests/datasets/test_filters_integration.py` (Stocks Basic).

## Settled

- **1.** Crypto 1-day bars cut the UTC day on every source. Settled 2026-11-02: X:BTCUSD's range,
  `/prev`, grouped daily, day file, and minute rebuild agreed.
- **4.** Futures flat files hold only their own session. Settled 2026-11-02: every row of the
  2026-10-26 files of all four venues carried that session.
- **5.** Indices `V` streams on the class's own TICK host. Settled 2026-11-02: `V.I:SPX`
  acknowledged and values received.
- **7.** The futures snapshot carries `timeframe`. Settled 2026-11-02: on traded contracts only
  (item 78).
- **9.** Massive acknowledges an unsubscribe. Settled 2026-11-02: "unsubscribed to:", and no row
  afterwards; nothing waits for it.
- **12.** The exact `max_connections` status text and timing. Settled 2026-11-10: Massive keeps
  the newer connection and sends the older, authenticated one "Maximum number of websocket
  connections exceeded. ..." 2.6 to 8.3 s later, then close code 1008 (decision 97); verified by
  `tests/ingest/test_ws_integration.py`.
- **14.** No flat-file publish time is assumed. Settled by decision 78; the 2026-10-30 and
  2026-11-02 checks saw files land from 00:42 to 11:17 ET.
- **17.** Retired: `poll()` results no longer use input tables.
- **19.** WebSocket forex `C` `x` is an integer. Settled 2026-11-02: JSON integer 48.
- **20.** WebSocket crypto `XT` `i` arrives as text. Settled 2026-11-02: `"666025125"`; kept as
  text.
- **23.** The futures snapshot nests `ticker` and `product_code` under `details`. Settled
  2026-11-02: every ES row.
- **24.** Ticker events carry `type`, not `event_type`. Settled 2026-11-02: META's events.
- **25.** Filings rows may lack `ticker`. Settled 2026-11-02: about half of `filings.index` rows
  in 2026-10 had none.
- **27.** Forex pair topics are spelled `C.EUR-USD`. Settled 2026-11-02: acknowledged; rows
  arrived as `C:EURUSD`.
- **28.** Forex and crypto quote files spell pairs `C:AED-AUD` and `X:BTC-USD`. Settled
  2026-11-02: every ticker of every file read.
- **29.** A session after a holiday opens early, and date-named reads need no margin. Settled
  2026-11-02: 2026-01-20's session opened Sunday 23:00 UTC; all 2,520 minute bars kept.
- **30.** `/v3/reference/tickers` has one row per inactive ticker. Settled 2026-11-02: 155
  inactive tickers, none repeated.
- **31.** `(asset_class, id)` identifies an exchange. Settled 2026-11-02: 54 exchanges, none
  repeated.
- **32.** `(asset_class, type, id)` does not identify a condition. Settled 2026-10-30: stocks
  quote condition 30 is listed twice, so the table keys on every column.
- **33.** Week bars start on Sunday 00:00 in the class day zone. Settled 2026-10-30.
- **34.** The aggregates range returns no partial bar for a period `{from}` falls inside.
  Settled 2026-11-02: AAPL's July bar requested from 07-15 was whole.
- **36.** No futures session ends on a Saturday or Sunday. Settled 2026-11-02: a month of nine
  products on four venues.
- **37.** Forex 1-day bars cover the UTC day on every source. Settled 2026-11-02: C:EURUSD's
  sources and hour rebuild agreed.
- **39.** Grouped daily and day files hold the same forex and crypto pairs. Settled 2026-11-02:
  368 of 368 crypto, 1,202 of 1,202 forex.
- **40.** A futures bar of a session or longer is a bucket of whole UTC days labeled by its last
  session. Settled 2026-10-31 and 2026-11-02 on ESZ6; the rest is item 81.
- **41.** An IPO keeps its `issuer_name` across statuses. Settled 2026-11-02: 349 IPOs, none
  renamed.
- **43.** Massive's spelling of US stock tickers in partner datasets. Settled 2026-11-11 as no
  longer open: the library reads no ticker's shape and keeps every row's ticker as Massive
  writes it (decision 104).
- **44.** Every indices dataset reads a bare ticker as `I:`. Settled 2026-11-02: `Symbol` was
  `I:SPX` on five endpoints.
- **46.** Contract deliverable fields. Settled 2026-11-02: adjusted contracts carry deliverable
  lists; standard ones omit them with 100 shares.
- **47.** Single futures tickers have one- or two-digit years. Settled 2026-11-02: every NG
  contract fit the grammar.
- **48.** Futures combo tickers are taken as written. Settled 2026-11-02: every combo on four
  venues accepted.
- **50.** Stock 1-day bar stamps per source. Settled 2026-11-02: range and day file 00:00, grouped
  daily and `/prev` 16:00 New York time.
- **51.** Option 1-day bar stamps per source. Settled 2026-11-02: as item 50, for SPY and SPXW.
- **59.** Futures one-second stream bars span one second. Settled 2026-11-02: every bar.
- **62.** An unknown ticker gets an empty 200, and ticker details a 404. Settled 2026-11-02.
- **63.** Bars of several days count calendar days. Settled 2026-10-30: AAPL's 2-day bars.
- **64.** Forex flat files exist for Sunday but not Saturday. Settled 2026-11-02.
- **70.** Massive accepts a real-time key on the delayed feed and sends rows. Settled 2026-11-02:
  `T.AAPL` acknowledged and trades received.
- **72.** Futures years are one digit for ES and two for NG, never rewritten. Settled 2026-11-02:
  `ESH30` returned no rows.
- **74.** The futures stream sends prices as text, not JSON numbers. Settled 2026-11-02, against
  the documentation.
- **75.** Forex and crypto snapshot `updated` is in nanoseconds. Settled 2026-11-02, against the
  samples.
- **78.** Only traded futures contracts carry a snapshot `timeframe`. Settled 2026-11-02:
  far-dated ES rows had none.
- **79.** The futures snapshot's `last_minute.last_updated` is in nanoseconds. Settled
  2026-11-02, against the sample.
- **84.** The v2 snapshots' `todaysChange` and `todaysChangePerc` are the option chain's
  `day.change` and `day.change_percent`. Settled 2026-11-11 and 2026-11-12: five stocks'
  change was the latest price minus `prevDay.c`, SPY's chain's `day.close` minus
  `day.previous_close`, and each percent 100 times the change over that close; the option chain
  now names them `TodaysChange` and `TodaysChangePerc`, as the v2 snapshots and movers do.
- **85.** The v2 snapshots' `updated` is not the unified snapshot's `last_updated`. Settled
  2026-11-12: the unified snapshot sent no top-level `last_updated`, and the stocks and crypto
  `updated` matched none of its nested times (forex's matched `last_quote.last_updated`), so
  `Updated` keeps its name.
- **86.** The forex conversion's `last` is the last quote of its `from`/`to` pair. Settled
  2026-11-12: in both directions of AUD/USD, 7 of 8 tries read one quote on the conversion,
  `/v1/last_quote`, and the snapshot's `lastQuote`; the conversion's columns now take the
  snapshot's `LastQuote*` names.
- **87.** A forex conversion `precision` outside 0 to 4. Settled 2026-11-12: `precision=5` was
  honored (`converted` the amount times `last.bid`, rounded to 5 decimals); now sent; recorded by
  `tests/namespaces/test_forex_integration.py`.
- **88.** A forex conversion `amount` of 0 or below. Settled 2026-11-12: 0 converted to 0, and -100
  to -68.43; now sent; recorded by `tests/namespaces/test_forex_integration.py`.
- **89.** A forex conversion currency that is not three upper-case letters. Settled 2026-11-12:
  `aud` to `usd` answered HTTP 404 "Couldn't find requested currency pair"; now sent; recorded by
  `tests/namespaces/test_forex_integration.py`.
- **90.** An indicator `timespan` its path does not list. Settled 2026-11-12: `second` answered
  HTTP 200 with no values; still refused; recorded by
  `tests/namespaces/test_indicators_integration.py`.
- **91.** An indicator `window` below 1. Settled 2026-11-12: `window=0` answered HTTP 200 with an
  empty body; still refused; recorded by `tests/namespaces/test_indicators_integration.py`.
- **92.** An `asset_class` a reference endpoint does not list. Settled 2026-11-12: the conditions,
  the exchanges, and the ticker types each answered HTTP 200 with no rows; still refused; recorded
  by `tests/namespaces/test_reference_integration.py`.
- **93.** A `sort` value outside a path's listed sort fields. Settled 2026-11-12: the conditions
  answered `sort=exchange` with HTTP 400; now sent; recorded by
  `tests/namespaces/test_query_building_integration.py`.
- **94.** A text `sort` part that names no field of the rows. Settled 2026-11-12: the dividends
  answered `no_such_field.asc` with HTTP 400; now sent; recorded by
  `tests/namespaces/test_query_building_integration.py`.
- **95.** A `sort` part with surrounding whitespace. Settled 2026-11-12: the dividends served
  `ticker, ex_dividend_date.desc` as written; now sent; recorded by
  `tests/namespaces/test_query_building_integration.py`.
- **96.** A `limit` below 1. Settled 2026-11-12: the dividends answered `limit=0` with HTTP 200 and
  no rows; still refused; recorded by `tests/namespaces/test_query_building_integration.py`.
- **97.** A filter value outside an enum parameter's values. Settled 2026-11-12: the tickers list
  answered `market=equities` with HTTP 200 and no rows; still refused; recorded by
  `tests/datasets/test_filters_integration.py`.
- **98.** A filter list item with surrounding whitespace. Settled 2026-11-12: ` MSFT` in
  `ticker.any_of` returned MSFT's dividends; now sent; recorded by
  `tests/datasets/test_filters_integration.py`.
- **99.** Balance sheets for `trailing_twelve_months`. Settled 2026-11-12: HTTP 200 with no rows;
  still refused; recorded by `tests/namespaces/test_stock_financials_integration.py`.
- **100.** The futures aggregates' `resolution=1day`. Settled 2026-11-12: five bars, each starting
  at 00:00 UTC and labelled with the next day's session; `Timespan.DAY` now sent (item 107);
  recorded by `tests/queries/test_choice_integration.py`.
- **101.** `timespan=session` on the aggregates range outside futures. Settled 2026-11-12: HTTP 400
  "Invalid time span"; now sent; recorded by `tests/queries/test_choice_integration.py`.
- **102.** A bar multiplier below 1. Settled 2026-11-12: HTTP 400 "The parameter 'multiplier' must
  be a positive number"; now sent; recorded by `tests/queries/test_choice_integration.py`.
- **103.** A currency or crypto pair as an options underlying. Settled 2026-11-12: the chain
  answered HTTP 400 "Invalid ticker." and is now sent; the contracts list answered HTTP 200 with no
  rows and is still refused; recorded by `tests/namespaces/test_options_integration.py`.
- **104.** What the contract overview answers to a stock ticker (`AAPL`). Settled 2026-11-12:
  HTTP 404 "Option Ticker not found.". `m.options.contracts("AAPL")` still raises before any
  request, for its missing `O:` prefix, the one ticker check the owner kept on 2026-11-14
  (decision 104).
- **105.** A range whose start is after its end. Settled 2026-11-12: the aggregates range answered
  HTTP 400, the dividends' `ex_dividend_date` HTTP 200 with no rows; still refused, since
  `history()` turns a range into the bars or days inside it and a reversed one has none to send;
  recorded by `tests/datasets/test_dates_integration.py`.
- **106.** A range that starts after now. Settled 2026-11-12: AAPL's day bars 10 to 20 days ahead
  answered HTTP 200 with no rows; still refused; recorded by
  `tests/datasets/test_dates_integration.py`.
