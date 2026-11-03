# Arguments

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

Each argument below means the same on every dataset that takes it. A dataset's `help()`, such
as `help(m.stocks.dividends)`, gives only what differs for that dataset, and its page in the
[dataset reference](../datasets/README.md) lists the values its `sort` and `filters` accept.

<a id="symbols"></a>

## `symbols`

The first argument of most datasets names the instruments to read. It takes:

| Form | Example | Reads |
|---|---|---|
| One symbol | `"AAPL"` | That instrument |
| A list | `["AAPL", "MSFT"]` | Those instruments |
| A table with a `Symbol` column | `watchlist` | The table's rows. A live table is tracked as you add and delete rows: see [watchlists](watchlists.md). |
| `dm.ALL` | `dm.ALL` | Every instrument, where the dataset can serve it |
| `None`, or left out | | On list datasets only: no filter by ticker |

### Spelling

Every symbol must be spelled exactly as Massive spells its ticker:

| Asset class | Example |
|---|---|
| Stock | `AAPL`, `BRK.B` |
| Option | `O:SPY271217C00700000` |
| Index | `I:SPX` |
| Forex | `C:EURUSD` |
| Crypto | `X:BTCUSD` |
| Future | `ESZ6` |

A wrongly spelled symbol in a string or a list raises `ValueError` at the call. A wrongly
spelled row of a table is skipped: see [rows the library skips](watchlists.md#skipped-rows).

An option symbol is its OCC code: `O:SPY271217C00700000` is underlying `SPY`, expiring
`271217` (2027-12-17), `C` for a call (`P` for a put), and a strike of 700, written as the
strike times 1000 in eight digits (`00700000`).

### Converting other spellings

The converters turn other spellings into Massive's:

```python
eurusd = dm.canonical_symbol_forex("EUR/USD")
berkshire = dm.canonical_symbol_stock_yahoo("BRK-B")
```

`eurusd` is `"C:EURUSD"`, and `berkshire` is `"BRK.B"`.

`dm.canonicalize_symbols` converts a whole table, and keeps converting rows as they arrive in a
live one:

```python
from deephaven import new_table
from deephaven.column import string_col

pairs = new_table([string_col("Symbol", ["EUR/USD", "GBP-USD"])])
converted = dm.canonicalize_symbols(pairs, dm.AssetClass.FOREX)
```

`converted` has the same rows, with `Symbol` `C:EURUSD` and `C:GBPUSD`.

A row it cannot convert gets an empty `Symbol`, which every dataset ignores, and one WARNING in
the log; the table keeps updating. Pass `on_error=dm.OnError.RAISE` to fail instead.
`help(dm.canonical_symbol)` lists every converter, including those for use inside Deephaven
query strings.

### Rows that list several tickers

News, filings, and financial statements list the tickers each row concerns, in `Tickers`. Their
`Symbol` is each ticker you named that the row lists:

- An article about AAPL and MSFT, read with `symbols=["AAPL", "MSFT"]`, is two rows: one with
  `Symbol` AAPL, and one with `Symbol` MSFT.
- Read without `symbols`, `Symbol` is null.

### At most 1,000 option contracts at once

The options WebSocket connection carries at most 1,000 *topics*: one per contract and kind of
data, so a contract's trades and its quotes are two. The cap counts every session that shares
the connection.

- A list or static table of contracts over the cap raises `ValueError`.
- A live watchlist that grows past the cap defers the extra contracts until room frees up. The
  `Deferred` column of `m.streams()` counts them.

<a id="start-and-end"></a>

## `start` and `end`

`history(start, end)` and every list dataset with a time axis take a range: dividends by
ex-date, news by publication time, and so on. Each dataset's reference page names its axis.

Both bounds are dates, or both are instants:

| Kind | Range | Accepted values |
|---|---|---|
| Dates | Whole days in the [market's time zone](concepts.md#time), both ends included: `history("2025-06-02", "2025-06-02")` is all of June 2 | Text such as `"2025-06-02"`, a Python `date`, or a Deephaven `LocalDate` |
| Instants | Exact times, the start included and the end excluded | A timezone-aware Python `datetime`, a `pandas.Timestamp` or `numpy.datetime64`, a Deephaven `Instant` or `ZonedDateTime` (such as `dh_now()`), or Deephaven time text such as `"2025-06-02T09:30 ET"` |

A `datetime` without a time zone is refused. Values taken from another table work directly:
see [the busiest minute](recipes.md#busiest-minute).

An open `end`:

- in `history()` reads through now; for dates, through today in the market's time zone (the
  current session for futures);
- on a list dataset is not sent, so `m.stocks.dividends("AAPL", start="2025-01-01")` includes
  announced dividends still to come.

A calendar-date axis, such as an ex-dividend date, takes dates only.

<a id="as-of"></a>

## `as_of`

A dataset that describes something as it stood on a past day takes `as_of`, a date: ticker
details, option and futures contracts, and futures products. Left out, it means today.

```python
apple_2020 = m.reference.ticker_details("AAPL", as_of="2020-01-02").fetch()
```

`apple_2020` has one row: Apple's details as of January 2, 2020.

<a id="sort-order-limit"></a>

## `sort`, `order`, and `limit`

### `sort` and `order`

List datasets take:

- **`sort`**: the field or fields to order by, named as Massive names them, such as
  `"ex_dividend_date"`. Where the endpoint allows it, several fields separated by commas, each
  with an optional `.asc` or `.desc`: `"ticker,ex_dividend_date.desc"`.
- **`order`**: `dm.Order.ASC` or `dm.Order.DESC`.

Each dataset accepts only the fields and forms its endpoint takes, listed on its reference page.
A dataset whose endpoint cannot sort has neither argument.

### `limit`

`limit` caps the rows Massive returns for the whole request: across every symbol you name and
every page. A symbol that comes first can use up the whole cap, so use `limit` to look at a
sample, and read without it for complete data on each symbol.

`limit` keeps the first rows in Massive's order, so ask for newest first when you want the
latest:

```python
latest_news = m.reference.news(
    "AAPL", sort="published_utc", order=dm.Order.DESC, limit=50
).poll(60)
```

`latest_news` holds Apple's 50 newest articles, read again every minute.

Exceptions:

| Dataset | `limit` caps |
|---|---|
| The indicators; `reference.tickers` and `stocks.ipos` with tickers named | Each symbol's request |
| `reference.news` | Each ticker's request; its `sort` and `order` apply per ticker too |
| `options.contracts` with contracts named | Nothing: each contract is one row |
| News, filings, financial statements | The rows Massive returns. Each row then appears once per named ticker it lists, so the table can hold more rows than `limit`. |

Each dataset's `help()` says when its `limit` is per symbol. A `fetch()` that follows a
[watchlist](watchlists.md) sends `limit` on each symbol's request.

A news or filings `poll()` needs a `start` or a `limit`: without either, every cycle would read
the whole archive again.

<a id="filters"></a>

## `filters`

Any other parameter Massive documents for a list dataset goes in `filters`: a mapping from the
parameter's name, with its dots written as underscores, to a value.

```python
big_strikes = m.options.contracts(underlying="SPY", filters={"strike_price_gte": 600}).fetch()
```

`big_strikes` lists SPY's option contracts with a strike of 600 or more.

- Each dataset's reference page lists the keys it accepts.
- A key the endpoint does not take, or one with its own argument (`start`, `symbols`,
  `adjusted`), raises `ValueError` naming the valid keys.
- A date or time filter takes ISO-8601 text, or any value [`start` and `end`](#start-and-end)
  take.

<a id="adjusted"></a>

## `adjusted`

Stock and option bars and daily open-close summaries take `adjusted`:

- `True`, the default: split-adjusted prices.
- `False`: prices as traded.

Only REST serves adjusted bars; flat files and live bars are as traded. See
[split adjustment](history.md#split-adjustment).

<a id="timespan-and-multiplier"></a>

## `timespan` and `multiplier`

Every `bars()` call names its bar length:

- **`timespan`**, required: a `dm.Timespan` member, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`,
  `MONTH`, `QUARTER`, or `YEAR`. Futures take `SESSION` in place of `DAY`.
- **`multiplier`**: the number of units per bar. The default is 1.

One-second and one-minute bars with `multiplier=1` also stream; every other length is history
only. Text is refused: write `dm.Timespan.DAY`, or convert with `dm.Timespan("day")`.

<a id="venues"></a>

## `venues`

Futures trades, quotes, and bars take `venues`: the exchanges whose flat files `history()`
reads. The members of `dm.FuturesVenue` are `CME`, `CBOT`, `NYMEX`, and `COMEX`, and the
default is all four. Massive sells futures flat files per exchange, so pass the ones your plan
holds:

```python
es = m.futures.trades("ESZ6", venues={dm.FuturesVenue.CME}).history("2026-10-01", "2026-10-02")
```

Streams and REST reads ignore `venues`.

<a id="verb-arguments"></a>

## Verb arguments

| Verb | Arguments |
|---|---|
| `poll(interval, *, mode=dm.PollMode.AUTO)` | `interval`: seconds, or a `timedelta`, of at least 1 second. `mode`: see [poll modes](../../README.md#table-shapes). |
| `ring(capacity)` | `capacity`: the number of rows to keep |
| `last_by(by="Symbol")` | `by`: the key column or columns |
| `history(start, end=None, *, source=dm.HistorySource.AUTO, reread=dm.Reread.ALWAYS, wait=True)` | `start`, `end`: see [`start` and `end`](#start-and-end). `source`: see [where history comes from](history.md#history-sources). `wait=False` reads in the background: see [long reads](concepts.md#live-tables). `reread`: below. |
| `fetch(*, wait=True)` | `wait`: as for `history()` |

### `reread`: a symbol that returns to a watchlist

`reread` matters only when `symbols` is a live table. It says whether a symbol you delete and
add back is downloaded again:

| Value | A returning symbol | Cost |
|---|---|---|
| `dm.Reread.ALWAYS`, the default | Downloaded again: Massive's current answer. It has no rows until the download lands. | A request per return |
| `dm.Reread.NEVER` | The earlier download, with no request | Memory: every symbol the table ever held stays in memory |

- A symbol whose read failed is always read again.
- Massive corrects past data (late cancellations, backfills, splits), so an earlier download can
  be out of date. Choose `NEVER` only when stale rows are acceptable.
- `m.symbol_reads()` shows when each symbol's rows were downloaded, in `ReadAt`.
- With `wait=True`, the rows of the symbols in the table at the call stay in memory until you
  release the result, under either value.

A `fetch()` that follows a watchlist always reads a returning symbol again, as Massive answers
at that moment.
