## 8. Namespace tree

Notation:

- `Symbols = str | Sequence[str] | Table | _All` ([section 10](10-symbols-and-all.md#10-symbols-and-all)).
- `DateLike = str | date | datetime | numpy.datetime64`, plus Deephaven's Java `Instant`,
  `ZonedDateTime`, and `LocalDate` ([section 11](11-enums-and-types.md#11-enums-and-types)).
- `…F` is the trailing keyword-only block
  `sort: str | None = None, order: Order | None = None, limit: int | None = None, filters: Mapping[str, FilterValue] | None = None`.
  The block offers only the ordering the dataset's paths accept, so a signature never offers
  one the endpoint cannot apply: a dataset whose paths have neither a `sort` nor an `order`
  parameter (futures `exchanges` and `market_status`, for example) leaves both out, and a
  dataset whose paths have `order` but no `sort` (the indicators) keeps `order` alone. How
  `sort` and `order` reach the wire, and how `sort` is checked, is in [section 9](09-filters.md#9-filters).
- **One name, form, and default per parameter.** `symbols` is the first positional parameter
  of every dataset that takes it, also where it defaults to None (the list datasets, where None
  means no ticker filter). A point-in-time parameter is `as_of`, never `date`, since `date`
  means the trading day itself elsewhere (`daily_open_close(symbols, date)`). An identity
  parameter Massive takes in one-or-many form (`product_code`, `issuer_cik`, `owner_cik`,
  `filer_cik`) takes one `str` or a sequence. Where Massive's spec gives a parameter a default,
  the signature shows that default (the indicators' windows and `adjusted=True`), and a test
  compares each one with the generated `Param.default`. None means only "this parameter is not
  sent" or "no value given", never a mode: a choice among behaviours has an enum member
  (`PollMode.AUTO`) or a real default (`last_by(by="Symbol")`, `expired=False`,
  `venues=frozenset(FuturesVenue)`).
- **Exact input, never cleaned.** Every text a caller passes is taken exactly as written or
  refused with `ValueError`; the library never strips, re-cases, or otherwise repairs it, so
  a typo fails at once instead of reaching Massive as a different request. The message names
  the form to write (`from_currency=' usd' must be a three-letter upper-case currency code;
  write 'USD'`). This holds for symbols ([section 10](10-symbols-and-all.md#10-symbols-and-all)), identifiers such
  as currency codes, date and time text, `filters` values and `sort`, and `MassiveConfig`'s
  text fields, environment variables, and settings file ([section 5](05-massiveconfig.md#5-massiveconfig)). The
  converters (`dm.canonical_symbol*`) exist to turn another spelling into Massive's, and they convert
  only the spellings they document.
- `…T` is `start: DateLike | None = None, end: DateLike | None = None` followed by `…F`. It
  exists only on datasets whose row has a named time axis (shown as "axis"). `start` and `end`
  address that axis with the day rules of [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries),
  as `history()` does: both bounds dates (whole days) or both aware datetimes (exact instants,
  half-open), a date mixed with a datetime raises `ValueError` at construction, and so does a
  range that ends before it starts (two dates may be equal: one whole day; two equal datetimes
  are an empty range). A calendar-date axis (dividends, splits, filings, financials, futures
  `schedules`, and every other axis whose field is a calendar date) holds whole days, so a
  datetime bound there raises `ValueError` ("`<axis> is a calendar date; pass dates`"). A
  bound left None is open: unlike `history()`, an open `end` is not sent (owner decision), so a
  list keeps rows dated in the future: `m.stocks.dividends("AAPL", start="2025-01-01")` returns
  an announced dividend whose ex-date is still to come, unless `end=` is given.
- Each namespace module declares its datasets (`DATASETS: tuple[DatasetSpec, ...]`), and
  `_session.py` composes them, once, at import, into `CATALOG`, the process's `Catalog` (every
  `DatasetSpec` by name); a test asserts that every namespace module contributes. The query
  class in each table below follows from the dataset's transports
  ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)).
- `limit` is Massive's total-row cap: named symbols that share a request share the cap, so a
  `limit` samples and a read without one is complete (decision 87). The exception is where
  each request names one symbol: a per-symbol path
  (the indicators) or a symbol parameter that takes one ticker
  (`reference.tickers`, `stocks.ipos`, `reference.news`). There it caps each symbol's
  request, so one ticker's rows never crowd out another's (owner decision 74). On a dataset
  whose rows list their tickers, the cap is on the rows Massive returns, before each is listed
  under its named tickers, so the table can hold more rows than `limit` (decision 90). Page size is internal. The README states the rule once, and each per-symbol dataset's docstring says so. The cap is
  stored apart from the wire parameters, in `Request.limit`, so it can never be sent as the
  endpoint's page-size `limit`; the page size is the path's documented maximum (or
  `DatasetSpec.page_size` where the dataset sets one), or `Request.limit` when that is smaller
  (`snapshots(ALL, limit=10)` asks for 10 rows, not 250), and paging stops once `limit` rows
  have arrived.
- The page size is the endpoint's documented maximum `limit`, which the refresh generates from
  the specification (`Param.maximum`), so no dataset types it. `DatasetSpec.page_size`
  overrides it in two cases, and the catalog's `page size` gate refuses any other: a maximum
  the specification states only in prose (the aggregates range paths, 50,000), and a lower
  size where the rows are long so one page decodes to some tens of MB at most: Benzinga news
  1,000 (each article carries its whole HTML body, so 50,000 could reach hundreds of MB), Benzinga analyst
  insights and filing risk factors 5,000 (each row quotes prose), and the three financial
  statements 10,000 (some fifty line items a row, over 60 MB at 50,000). Every other page
  size is the documented maximum: 50,000 short rows decode to some 10 to 40 MB, and the
  text-heavy filings endpoints (10-K sections, 8-K texts) document at most 100 rows.
- The REST column names the path template as the OpenAPI spec writes it (the spec's own
  placeholder names, such as `{stocksTicker}`, are abbreviated here to `{t}`, `{o}`, `{u}`).
  Every path is a generated REST source (`_massive.source(Transport.REST, template)`,
  [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). Each dataset names its paths by role (`fetch_path`,
  `detail_path`, `fetch_paths_by_class`, `history_path`, `grouped_daily_path`, `extra_paths`),
  and declares the key columns of each
  source it reads as `KeySpec`s ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas), "Keys are declared
  per source"): the `symbol`, `time`, `end`, instrument `fanout`, and `date` keys become
  `Symbol`, `Timestamp` (a bar's `TimestampStart`), `TimestampEnd`, `Underlying`, and `Date`,
  and a non-instrument `fanout` key keeps its column. A `time` or `end` key is always an
  instant, so those columns are `Instant`s. Every axis below that is a calendar date (all but
  the instants `published_utc`, `published`, and the indicators' `timestamp`: the dividends,
  splits, IPOs, short interest and volume, financial statements, filings, futures schedules,
  economy, and TMX axes) keeps its date column, ISO text, under its own name, such as `Date`
  or `ExDividendDate`, and declares no `time` key; such a dataset has no time columns of the
  library's but `TimestampInsert`, so its vendor `Date` is its own. Datasets whose
  rows each list several tickers (a JSON `tickers` field) describe a filing, a company, or
  an article, not one instrument: the three financial statements,
  `filings.initial_insider_ownership` and `filings.insider_transactions` (Forms 3 and 4),
  `filings.insider_activity` and `filings.insider_holdings`, `filings.disclosures_8k`,
  `reference.news`, and `partners.benzinga.news` keep their `Tickers` column, and their
  `Symbol` is the library's: each ticker the request named that a row lists, one row per
  such ticker ([section 10](10-symbols-and-all.md#10-symbols-and-all)).

### 8.1 `m.stocks`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `T` | history `/v3/trades/{t}` | `us_stocks_sip/trades_v1` |
| `last_trade(symbols)` ([7.5](07-query-classes-and-verbs.md#75-one-row-datasets)) | Query | | `/v2/last/trade/{t}` per symbol | |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{t}` | `us_stocks_sip/quotes_v1` |
| `last_quote(symbols)` | Query | | `/v2/last/nbbo/{t}` per symbol | |
| `bars(symbols, timespan, *, multiplier=1, adjusted=True)` | overloads ([7.1](07-query-classes-and-verbs.md#71-bars-overloads)) | `A` (1 s), `AM` (1 min) | history `/v2/aggs/ticker/{t}/range/{m}/{span}/{from}/{to}`; `ALL` with DAY: `/v2/aggs/grouped/locale/us/market/stocks/{date}` per day | `us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1` (`adjusted=False` only) |
| `previous_day_bar(symbols, *, adjusted=True)` | Query | | `/v2/aggs/ticker/{t}/prev` per symbol | |
| `luld(symbols)` | StreamQuery | `LULD` | | |
| `imbalances(symbols)` | StreamQuery | `NOI` | | |
| `fmv(symbols)` | StreamQuery | `FMV` (FMV family, `business.massive.com/stocks`) | | |
| `snapshots(symbols, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/tickers` (`tickers=`; `ALL` is the full market) | |
| `movers(direction: Direction, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/{gainers\|losers}` | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{t}/{date}` per symbol | |
| `dividends(symbols=None, …T)`, axis `ex_dividend_date` | Query | | `/stocks/v1/dividends` | |
| `splits(symbols=None, …T)`, axis `execution_date` | Query | | `/stocks/v1/splits` | |
| `ipos(symbols=None, …T)`, axis `listing_date` | Query | | `/vX/reference/ipos` | |
| `short_interest(symbols=None, …T)`, axis `settlement_date` | Query | | `/stocks/v1/short-interest` | |
| `short_volume(symbols=None, …T)`, axis `date` | Query | | `/stocks/v1/short-volume` | |
| `share_float(symbols=None, *, …F)`, no axis (the path documents no date parameter; `EffectiveDate` is a column only) | Query | | `/stocks/vX/float` | |
| `financials.balance_sheets`, `financials.income_statements`, `financials.cash_flow_statements` — each `(symbols=None, *, timeframe: Timeframe \| None = None, …T)`, axis `period_end` | Query | | `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements}` | |
| `financials.ratios(symbols=None, *, …F)`, no axis (the path documents no `period_end`, `timeframe`, or `date` parameter; its rows carry one `ticker`, so it has `Symbol`) | Query | | `/stocks/financials/v1/ratios` | |
| `filings.index(symbols=None, *, form_type: str \| None = None, …T)`, axis `filing_date` | Query | | `/stocks/filings/vX/index` | |
| `filings.sections_10k`, `filings.text_8k`, `filings.disclosures_8k`, `filings.risk_factors` — each `(symbols=None, …T)`, axis `filing_date` | Query | | `/stocks/filings/10-K/vX/sections`, `/stocks/filings/8-K/vX/text`, `/stocks/filings/8-K/vX/disclosures`, `/stocks/filings/vX/risk-factors` | |
| `filings.institutional_holdings(*, filer_cik: str \| Sequence[str] \| None = None, …T)`, axis `filing_date` (Form 13-F); one CIK is sent as `filer_cik` and a sequence as `filer_cik.any_of` | Query | | `/stocks/filings/vX/13-F` | |
| `filings.initial_insider_ownership(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)` (Form 3); `filings.insider_transactions(...)` with the same signature (Form 4); axis `filing_date`. A CIK argument sends one CIK as `issuer_cik` or `owner_cik` and a sequence as its `.any_of` form; an empty sequence raises `ValueError` (as futures `product_code`) | Query | | `/stocks/filings/vX/form-3`, `/stocks/filings/vX/form-4` | |
| `filings.insider_activity(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, …T)`, axis `transaction_date`: each issuer's open-market insider purchases and sales summed per transaction day, across every insider; `poll()` replaces its table, keyed by `IssuerCik` and `TransactionDate` (a later filing for the same day revises that day's totals) | Query | | `/stocks/insiders/vX/activity` | |
| `filings.insider_holdings(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)`, axis `last_filing_date`: each insider's current consolidated holding of one issuer, built by Massive from all of the owner's Forms 3, 4, and 5; `poll()` replaces its table, keyed by `IssuerCik` and `OwnerCik`. Every insider dataset takes the title of its Massive page: Form 3 is Massive's "Initial Insider Ownership" | Query | | `/stocks/insiders/vX/holdings` | |
| `filings.risk_factor_taxonomy(…F)`, `filings.disclosure_taxonomy(…F)` | Query | | `/stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures` | |

### 8.2 `m.options`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `T` | history `/v3/trades/{o}` | `us_options_opra/trades_v1` |
| `last_trade(symbols)` | Query | | `/v2/last/trade/{o}` per contract | |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{o}` | `us_options_opra/quotes_v1` |
| `bars(symbols, timespan, *, multiplier=1, adjusted=True)` | overloads | `A`, `AM` | range | `us_options_opra/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols, *, adjusted=True)` | Query | | `/v2/aggs/ticker/{o}/prev` per contract | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/options`) | | |
| `chain(underlying: str \| Table, *, contract_type: ContractType \| None = None, …F)`; `filters` takes `strike_price_*`, `expiration_date_*` | Query | | `/v3/snapshot/options/{u}` | |
| `snapshots(symbols, *, …F)`; OCC contracts; `Underlying` from `underlying_asset.ticker`; `ALL` is every contract, paginated (slow, documented) | Query | | `/v3/snapshot?ticker.any_of=O:…`, 250 per request. `ALL` sends `type=options` in place of tickers, so it lists only options contracts; tickers are sent without `type`, which Massive refuses beside them (HTTP 400, "Cannot specify tickers and type."). `type` is rejected as a filter. Same fields as the per-contract endpoint, including greeks. `/v3/snapshot/options/{u}/{o}` is listed in the catalog as covered by this dataset. | |
| `contracts(symbols=None, *, underlying: str \| Table \| None = None, contract_type: ContractType \| None = None, as_of: DateLike \| None = None, expired: bool = False, …F)` (`expired=False`, Massive's default, lists contracts still trading and is not sent, so the per-contract path accepts it; `True` lists expired ones and is sent); `filters` takes `expiration_date_*`, `strike_price_*` | Query | | `/v3/reference/options/contracts`; with `symbols`: `/v3/reference/options/contracts/{o}` per contract | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{o}/{date}` per contract | |

Every options dataset has `OptionRoot`, the OCC root read from `Symbol` (`SPXW`, `AAPL1`;
[section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). `underlying=` takes Massive's exact stock or index
ticker (`AAPL`, `BRK.B`, `I:SPX`), case kept. The chain sends it as given; the contracts list
sends an index bare (`SPX`), since Massive's `underlying_ticker` finds no contract for `I:SPX`
and every contract of `SPX`. `Underlying` is Massive's field in the canonical form: the chain
and the snapshots write an index as `I:SPX`, and the contracts list writes it bare, so a
contracts row whose CFI code marks an option on an index (an `O`, then its fourth letter `I`,
as in `OCEICS`) gets the `I:` prefix, and the two tables join on `Underlying`. The request
keeps the canonical `I:SPX`; only the wire gets `SPX`. A bare ticker can name another
instrument (`DJT` is a stock), so a row of an index's request whose code marks another kind of
option keeps the underlying Massive wrote, and one WARNING per dataset and index per session
names the request and the rows; a row without a code keeps the bare ticker, with one WARNING
per dataset per session. No row is dropped. It names the
instrument Massive links the contract to, not the deliverable, and is never derived from or
mapped to the root (`AAPL1` is never `AAPL`). The
contracts dataset exposes the raw deliverable fields with their vendor descriptions:
`SharesPerContract`, `AdditionalUnderlyings` (JSON text of further deliverables), `Cfi`, and
`Correction`. There is no derived "standard deliverable" flag until a live check settles the
fields' meaning ([section 23](23-open-verification-items.md#23-open-verification-items)); the chain's docstring gives the
recipe that keeps standard contracts (`natural_join` with `contracts` on `Symbol`, keep
`SharesPerContract == 100` and an empty `AdditionalUnderlyings`).

### 8.3 `m.futures`

Streams always use the `/futures` cluster path. `venues` exists on the three factory methods
below and selects the flat-file prefixes only: it is not part of the stream key
([section 14](14-publishers-fan-out-and-failure-semantics.md#14-publishers-fan-out-and-failure-semantics)). It is a set of `FuturesVenue`,
stored as a `frozenset`; the default, `frozenset(FuturesVenue)`, reads all four prefixes, in
`FuturesVenue` order whatever the set's order. An empty set raises `ValueError`, and anything
but a set of `FuturesVenue` raises `TypeError`.

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | StreamHistoryQuery | `T` | history `/futures/v1/trades/{t}` (dates: one request series per weekday session, `session_end_date=<date>`; datetimes: the exact `timestamp` range; [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/trades_v1` |
| `quotes(symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | StreamHistoryQuery | `Q` | history `/futures/v1/quotes/{t}` (as `trades`) | `us_futures_{venue}/quotes_v1` |
| `bars(symbols, timespan, *, multiplier=1, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | overloads | `A`, `AM` | history `/futures/v1/aggs/{t}` (`resolution`; `window_start` in ns; dates: to 18:00 CT on the last date, from the first session's open less 4 days for intraday and 1-session bars and from 00:00 UTC on the first bucket that can start in the range for longer ones, kept when their whole window lies in the range; datetimes: the exact `window_start` range, bars kept by their own window; [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/minute_aggs_v1`, `us_futures_{venue}/session_aggs_v1` |
| `snapshots(symbols, *, product_code: str \| Sequence[str] \| Table \| None = None, …F)`; `ALL` with `product_code` is every contract of the products; `Symbol` is the contract's `details.ticker`, where the OpenAPI schema and the live wire nest it, though both vendor samples send it at the top level ([section 23](23-open-verification-items.md#23-open-verification-items) item 23) | Query | | `/futures/v1/snapshot` | |
| `contracts(symbols=None, *, product_code: str \| Sequence[str] \| Table \| None = None, as_of: DateLike \| None = None, …F)`: `Symbol` and `RawSymbol` are the row's `ticker`, as on the market data, so a contracts table feeds `trades()`, `quotes()`, `bars()`, and `snapshots()` directly; explicit `symbols` are sent as `ticker.any_of`, 250 a request (the size of the stock, option, index, forex, and crypto snapshots), so `ticker.any_of` is no filter; None lists every contract and `ALL` is refused (leave `symbols` out); the `poll()` key is `Symbol` and `Date`. `products(*, product_code: str \| Sequence[str] \| Table \| None = None, as_of: DateLike \| None = None, …F)`. On both, `as_of` is sent as `date`, the day whose point-in-time specifications are listed; None for Massive's default, today. `exchanges(*, limit: int \| None = None)`, `market_status(*, product_code: str \| Sequence[str] \| Table \| None = None, limit: int \| None = None)` (neither path has a filter beyond the typed arguments, nor `sort`) | Query | | `/futures/v1/{contracts,products,exchanges,market-status}` | |
| `schedules(*, product_code: str \| Sequence[str] \| Table \| None = None, …T)`, axis `session_end_date` | Query | | `/futures/v1/schedules` | |

`product_code` is typed on every futures dataset whose path takes it (`snapshots`,
`contracts`, `products`, `market_status`, `schedules`), so none of its forms is a
`filters` key there. One code is sent as `product_code`; a sequence of codes is sent as
`product_code.any_of`, which every one of those paths takes, so several products still
need one request. An empty sequence raises `ValueError`.
`session_end_date` on `trades` and `quotes` is not their time axis: datetime bounds of
`history()` address `timestamp`, and date bounds name sessions, which `history()` requests one
at a time as `session_end_date=<date>`. The `trades` and `quotes` factories take no `filters`;
a query built with a `session_end_date` filter (any form) and date bounds is refused with
`ValueError` before any request, whatever the source, since the dates set it.

Massive has no futures last-trade, last-quote, or previous-bar endpoint, so futures has no
one-row dataset ([section 7.5](07-query-classes-and-verbs.md#75-one-row-datasets)). `snapshots(symbols).fetch()` is the
one-request answer. There is no futures `day_aggs` flat
file; `Timespan.DAY` is rejected ([section 7.1](07-query-classes-and-verbs.md#71-bars-overloads)).

### 8.4 `m.indices`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `values(symbols)` | StreamHistoryQuery | `V` | none: history flat files only | `us_indices/values_v1` |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: the endpoint has none) | overloads | `A`, `AM` | range | `us_indices/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{t}/prev` per index | |
| `snapshots(symbols, *, …F)` | Query | | `/v3/snapshot/indices`, 250 tickers per request | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{t}/{date}` | |

### 8.5 `m.forex`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `quotes(symbols)` | StreamHistoryQuery | `C` | history `/v3/quotes/{C:…}` | `global_forex/quotes_v1` |
| `last_quote(symbols)` | Query | | `/v1/last_quote/currencies/{from}/{to}` per pair | |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: currency pairs have no corporate actions, so no request sends it and flat files serve every timespan they hold; a 1-day bar is the UTC day, `TimestampStart` 00:00 UTC on every source: grouped daily, `/prev`, and `day_aggs_v1`'s 23:59:59.999 UTC are moved to the day's start, and any other stamp is kept as sent with a null `Date` and an end a period later, and warns (decision 85), [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)) | overloads | `CAS` (1 s), `CA` (1 min) | range; `ALL` with DAY: `/v2/aggs/grouped/locale/global/market/fx/{date}` | `global_forex/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{C:…}/prev` per pair | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/forex`) | | |
| `snapshots(symbols)`; `ALL` is all pairs | Query | | `/v2/snapshot/locale/global/markets/forex/tickers` | |
| `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/forex/{gainers\|losers}` | |
| `conversion(from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2)`; each currency is exactly a three-letter upper-case code (`"USD"`), and anything else (`" usd"`, `"usd"`) raises `ValueError` naming the code to write | Query | | `/v1/conversion/{from}/{to}` | |

### 8.6 `m.crypto`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `XT` | history `/v3/trades/{X:…}` | `global_crypto/trades_v1` |
| `last_trade(symbols)` | Query | | `/v1/last/crypto/{from}/{to}` per pair | |
| `quotes(symbols)` | StreamQuery | `XQ` | | |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: crypto has no corporate actions, so the parameter is never sent; a 1-day bar is the UTC day, `TimestampStart` 00:00 UTC on every source: `day_aggs_v1`'s 23:59:59.999 UTC is moved to the day's start, and any stamp elsewhere in the day than its first or last millisecond (the documented REST samples sit at midnight New York time or, for grouped daily, at times such as 20:48 UTC), or a REST bar of several days or longer not at 00:00 UTC, is still served, kept at its stamp with a null `Date` and an end a period later, and logs one WARNING per form of stamp per session naming the symbol and the stamp (decision 85); `AUTO` chooses as for every dataset; [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), [section 23](23-open-verification-items.md#23-open-verification-items) item 1) | overloads | `XAS` (1 s), `XA` (1 min) | as forex; grouped `/v2/aggs/grouped/locale/global/market/crypto/{date}` | `global_crypto/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{X:…}/prev` per pair | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/crypto`) | | |
| `snapshots(symbols)`, `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/crypto/{tickers,gainers,losers}` | |
| `daily_open_close(symbols, date: DateLike)` (no `adjusted`, as `bars`: the parameter is never sent; a row whose `isUTC` is false keeps its `Date` and `IsUtc`, gets the widest window its date has in any time zone, from 14 hours before 00:00 UTC to 12 hours after the next, and logs one WARNING per dataset per session, decision 85) | Query | | `/v1/open-close/crypto/{from}/{to}/{date}` per pair | |

### 8.7 `m.reference`

All return `Query`.

| Method | REST |
|---|---|
| `tickers(symbols=None, *, market: Market \| None = None, ticker_type: TickerType \| None = None, exchange: str \| None = None, cusip: str \| None = None, cik: str \| None = None, as_of: DateLike \| None = None, active: bool = True, search: str \| None = None, …F)` | `/v3/reference/tickers`. `cusip` and `cik` are `str`, as the spec types them. `ticker_type` is sent as the wire parameter `type`, and `as_of` as `date`. |
| `ticker_details(symbols, *, as_of: DateLike \| None = None)` | `/v3/reference/tickers/{t}` per symbol; `as_of` is sent as `date` |
| `ticker_types(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/tickers/types` |
| `ticker_events(symbols, *, types: Sequence[str] \| None = None)` | `/vX/reference/tickers/{t}/events` per symbol |
| `related_companies(symbols)` | `/v1/related-companies/{t}` per symbol; rows are `{"ticker": peer}`, so `Symbol` is the peer and the requested symbol is `Underlying` |
| `news(symbols=None, *, …T)`, axis `published_utc` (half-open `.gte`/`.lt`) | `/v2/reference/news`. Its `ticker` parameter takes one ticker and the path has no `ticker.any_of`, so `symbols` (one ticker, a list, or a watchlist table, followed as every symbol table is) is sent one request per ticker, and an article several tickers return is listed under each, with that ticker in `Symbol` (decision 90); a `poll()` table is keyed by `Id` and `Symbol`, and fails on an article without an `Id`. Each `poll()` cycle re-reads the whole range for every ticker, so the docstring recommends a recent `start`, a `limit`, or both. Rows come ticker by ticker; `limit`, `sort`, and `order` apply to each ticker's request, as the docstring says, and on a 5-requests-a-minute plan a 50-ticker `poll(60)` outruns its interval and logs the slow-poll WARNING. `symbols=None` reads every article |
| `exchanges(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/exchanges` |
| `conditions(*, asset_class: AssetClass \| None = None, data_type: ConditionDataType \| None = None, condition_id: int \| None = None, sip: Sip \| None = None, …F)` | `/v3/reference/conditions`; `condition_id` is sent as the wire parameter `id` |
| `market_status()` | `/v1/marketstatus/now`; one row; nested maps flattened |
| `market_holidays()` | `/v1/marketstatus/upcoming` |

`AssetClass.FOREX` is sent as `fx` on these endpoints. Each endpoint accepts the classes its
generated `asset_class` enum lists, and any other class raises `ValueError` naming those it
accepts: `ticker_types` takes every class but futures, `exchanges` every class but indices, and
`conditions` neither futures nor indices.

### 8.8 `m.indicators`

All return `Query`. The symbol prefix selects the asset class. The axis is `timestamp`.

```python
sma(symbols, window: int = 50, *, timespan: Timespan = Timespan.DAY,
    series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
    start: DateLike | None = None, end: DateLike | None = None,
    order: Order | None = None, limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None)
ema(...)   # same signature as sma
rsi(...)   # same signature as sma, but window: int = 14
macd(symbols, *, short_window: int = 12, long_window: int = 26, signal_window: int = 9,
     timespan: Timespan = Timespan.DAY,
     series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
     start: DateLike | None = None, end: DateLike | None = None,
     order: Order | None = None, limit: int | None = None,
     filters: Mapping[str, FilterValue] | None = None)
```

REST: `/v1/indicators/{sma,ema,rsi,macd}/{t}` per symbol. The axis is half-open
(`timestamp.gte`/`.lt`). The indicator paths have an `order` parameter but no `sort`, so the
signature keeps `order` and has no `sort` ([section 9](09-filters.md#9-filters)). They have no multiplier,
so none is offered. `expand_underlying` is not offered either: the underlying bars it adds
are not rows of the table, so the library always sends `expand_underlying=false`
(`DatasetSpec.fixed_params`), which also reserves it from `filters`. `Timespan.SECOND` and `Timespan.SESSION` are rejected with
`ValueError`: the endpoints' timespan enum runs from `minute` to `year`. Crypto symbols are
served by the `{cryptoTicker}` templates, chosen per symbol through
`DatasetSpec.fetch_paths_by_class` ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). Every default of the
signature is the default Massive's spec gives the parameter (the windows 50, 50, 14, and 12,
26, 9; `timespan` day; `series_type` close; `adjusted` true). `adjusted=True`, the stock and
option bars' basis too, is sent where the chosen path declares `adjusted` and left out where
it does not: the crypto templates have no such parameter, and a pair has no splits to adjust
for. `adjusted=False` is sent where the path declares it and raises `ValueError` for a symbol
whose path does not (a crypto symbol), rather than being dropped silently. The indicator responses echo no `adjusted`, so their basis cannot
be checked ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), "Price adjustment").

Each row is one indicator value at one bar (`DatasetSpec.row_times` is `BAR`): `Date`,
`TimestampStart`, and `TimestampEnd` follow the bar rule of the query's timespan, in the day
zone of the symbol's class (UTC for forex and crypto, New York otherwise), and `poll()` keys on
`Symbol` and `TimestampStart`. A value is computed over the aggregates range's bars and carries
the stamp of its bar: Massive's samples show the underlying aggregates at 00:00 New York time
for stocks and 01:00 for indices. So a value of a day or longer is checked by the range's rule
for its symbols' class (`row_times.stamp_rule` with the class): 00:00 in the day zone, or
the indices' 01:00, on its period's first day. A value at any other stamp is served too, as
sent: it keeps the stamp, ends one period later, has a null `Date`, and logs one WARNING per
form of stamp per session naming the dataset, the path, the symbol, and the stamp (decision 85);
[section 23](23-open-verification-items.md#23-open-verification-items) item 61 checks it live.

`fetch()` and `poll()` return what Massive reports now, the current period's value included,
which changes until the period ends; `history()` is not offered, and would return finished
periods only. Owner decision D2, 2026-11-02: keep the unfinished value; filter with
`TimestampEnd <= TimestampInsert` (decision 86). The `TimestampEnd` description of every
dataset that has one says so, and each indicator docstring gives the filter. A
`poll(mode=APPEND)` table keeps the first unfinished value it sees for a period, since its
identity, `Symbol` and `TimestampStart`, does not change as the value does, so that table never
receives the finished value; the default `REPLACE` holds the latest.

### 8.9 `m.economy`

All return `Query`; axis `date`; each takes `…T`.

| Method | REST |
|---|---|
| `treasury_yields` | `/fed/v1/treasury-yields` |
| `inflation` | `/fed/v1/inflation` |
| `inflation_expectations` | `/fed/v1/inflation-expectations` |
| `labor_market` | `/fed/v1/labor-market` |
| `funding_conditions` | `/fed/v1/funding-conditions` |

### 8.10 `m.partners`

All return `Query`.

| Vendor | Methods | REST |
|---|---|---|
| `benzinga` | `analysts`, `analyst_insights`, `ratings`, `bulls_bears_say`, `guidance`, `earnings`, `firms` — `(symbols=None, …F)` where the endpoint takes tickers, `(…F)` otherwise | `/benzinga/v1/{analysts,analyst-insights,ratings,bulls-bears-say,guidance,earnings,firms}` |
| `benzinga` | `consensus_ratings(symbols, *, filters: Mapping[str, FilterValue] \| None = None)`; `Symbol` and `RawSymbol` are the row's `ticker`, as on the other Benzinga datasets, and there is no separate `Ticker` column | `/benzinga/v1/consensus-ratings/{t}` per symbol (the ticker is a path segment); `filters` reaches the path's `date` family |
| `benzinga` | `news(symbols=None, *, …T)`, axis `published` (an instant) | `/benzinga/v2/news` |
| `etf_global` | `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies` — `(symbols=None, …F)` | `/etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` |
| `tmx` | `corporate_events(symbols=None, …T)`, axis `date` | `/tmx/v1/corporate-events` |
| `fable` | `merchant_aggregates(…T)`, `merchant_hierarchy(…F)` | `/consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` |

On the `etf_global` datasets `Symbol` is the fund the row describes (for `constituents`, the
ETF, not the holding). Each partner dataset's `poll()` key is a set of its own generated
columns that is unique per row, checked against the generated fields of its path.

### 8.11 `m.snapshots`

`m.snapshots(symbols, *, …F) -> Query` calls the unified `/v3/snapshot` with `ticker.any_of`,
250 tickers per request, any mix of asset classes. Rows are the union schema of the unified
snapshot. An options row also has `Underlying` (its `underlying_asset.ticker`, an optional
`fanout` key, since other classes' rows have none) and `OptionRoot` (`option_rows=True`), as
`m.options.snapshots` has; both are null on the rows of other classes. `ALL` is not accepted
here (the endpoint requires tickers). The dataset is named `snapshots`, the session method's
own name with no namespace (`DatasetSpec` takes one plain name for a dataset of the session
itself), and is declared in `_namespaces/snapshots.py` with `asset_class=ClassFromPrefix.ANY`
(each ticker's prefix selects its class, § 10), so it is in the catalog (`CATALOG` in
`_session.py`); `m.streams()`, messages, and the column reference show that name.

### 8.12 Excluded endpoints

Excluded items fall into two lists.

- **Executable exclusions** are entries `(kind, item, reason)` of `EXCLUSIONS` in
  `_massive/refresh/exclusions.py`, the single list for REST paths, WebSocket channels, and
  flat-file prefixes. Each names an item present in a vendor source at the pinned snapshot
  (the OpenAPI spec, `websocket.json`, `llms.txt` and the REST pages it links, or the
  flat-file pages). An entry that names an item the snapshot lacks fails the refresh as stale
  (the `stale-entry` gate, [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). A path the OpenAPI spec
  itself marks deprecated (`x-polygon-deprecation`) or draft (`x-polygon-draft`) is accounted
  for by the spec and is never an entry; the rows below that name such paths record why
  nothing uses them. An entry whose reason is that no documentation page names the item is marked
  `undocumented`, and the refresh checks that claim against the snapshot both ways: a page
  whose `**Endpoint:**` line names the item fails it, as does a documented entry no page
  names.
- **Documentation-only exclusions** name endpoints that never existed or are absent from every
  vendor source. They are recorded here so nobody re-adds them, and never appear in
  `EXCLUSIONS`. A row of the first table that the snapshot shows absent from every vendor
  source moves to the second.

| Endpoint | Reason |
|---|---|
| `/v3/reference/dividends`, `/v3/reference/splits` | Deprecated; replaced by `/stocks/v1/*`. |
| Crypto `/book` REST endpoint; WebSocket `XL2` | Deprecated and sunset (the REST path is marked deprecated in the OpenAPI spec). |
| WebSocket `LV`; Launchpad hosts and paths | Product retired. |
| `/v1/historic/*`, `/v2/ticks/*` | Deprecated 2022 (marked deprecated in the OpenAPI spec); replaced by `/v3/trades` and `/v3/quotes`. |
| Form 5, per-asset `/v1/exchanges` (`/stocks`, `/options`, `/forex`, `/crypto`), `/options/v3/*`, `/v1/reference/ipos`, `/stocks/dev/*`, `vX_0` paths | Present only in the OpenAPI spec; no page `llms.txt` links names them (`undocumented`). |
| `/v1/reference/sec/filings*` | Marked `x-polygon-draft` in the OpenAPI spec and undocumented. |
| `/v1/indicators/{sma,ema,rsi,macd}/{optionsTicker,indicesTicker,fxTicker}` | The same URL, fields, and parameters as the `{stockTicker}` template, which serves every asset class by the ticker's prefix; a refresh test checks they still repeat it. The `{cryptoTicker}` templates document no `adjusted` parameter, so they are generated in their own right. |
| Single-ticker v2 snapshots `/v2/snapshot/.../tickers/{t}` | Covered by `snapshots(symbols)` with the `tickers` filter. |
| `/v3/snapshot/options/{u}/{o}` | Covered by `m.options.snapshots`; identical fields. |

Documentation only:

| Endpoint | Reason |
|---|---|
| `/benzinga/v1/transcripts` | Never existed. |
| `/v1/summaries` | Removed from the OpenAPI spec (2026-10-28); it was never in the documentation index. |
| `/benzinga/v1/news` | Sunset; replaced by `/benzinga/v2/news`. Absent from the OpenAPI spec and the documentation at the pinned snapshot. |
| `/vX/reference/financials` | Sunset; replaced by `/stocks/financials/v1/*`. Absent from the OpenAPI spec and the documentation at the pinned snapshot. |
| WebSocket `/futures/{venue}` | Not documented: the futures WebSocket pages list `/futures` only, and `websocket.json` has no such channel at the pinned snapshot. |

`x-polygon-ignore` excludes nothing. It is the vendor's hint to its own SDK generator not to
emit a second client method for one URL (the per-asset-class templates of `/v3/trades`,
`/v3/quotes`, `/v2/last/trade`, and the indicators); each such template has its own
documentation page, with its own documented fields, so it is generated like any other path.
Likewise `x-polygon-experimental` excludes nothing: the one path that carries it
(`/vX/reference/tickers/{id}/events`) is documented, and documented endpoints are generated.
The generator knows every vendor extension it accepts, each with the reason in
`refresh/rest_sources.py`; an unknown one stops the refresh (`shape`).

**Coverage.** Everything documented is generated or excluded, and everything generated is
used or deferred:

- **documented = generated ⊎ excluded ⊎ (deprecated ∪ draft)**, tested against the
  pinned snapshot in `tests/massive/refresh/test_generate.py` (WebSocket channels and flat-file
  prefixes) and `tests/massive/refresh/test_rest_sources.py` (REST paths, and every path a
  documentation page names is in the OpenAPI spec);
- **generated = used ⊎ deferred**, tested in `tests/datasets/test_catalog.py`: every generated
  item is read by a declared dataset or deferred (the coverage test, with a negative test that
  drops a dataset), and every item a dataset reads is generated (the generated-sources gate).
  A deferred item is a `DEFERRED` entry of the coverage test: the generated source key, a
  reason, and the number of the issue that will add its dataset, both required. An entry that
  names an item not generated, or one a dataset reads, fails the test, so the list cannot go
  stale. A new generated item, from a refresh that takes a vendor addition, therefore fails the
  coverage test until a dataset reads it, a `DEFERRED` entry defers it, or an `EXCLUSIONS`
  entry, with its reason, keeps the refresh from generating it.

`/fed/v1/funding-conditions` and `/v1/open-close/crypto/{from}/{to}/{date}` are ordinary
generated paths: every path reaches the same REST client in the same way.
