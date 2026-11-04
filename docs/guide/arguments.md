# Arguments, verbs, and columns

Datasets share their arguments, verbs, and key columns, and each one means the same on every
dataset. A dataset's `help()`, such as `help(m.stocks.dividends)`, gives each shared argument one
line, with a link to its whole rule here. Each dataset's page in the
[dataset reference](../datasets/README.md) lists its own columns, its plans, and the values its
`sort` and `filters` accept. Run every example in the console of the Deephaven IDE.

<a id="verb-reference"></a>

## Pick a verb

A dataset call such as `m.stocks.trades("AAPL")` describes the data and sends nothing to
Massive. A verb on it, such as `.last_by()`, reads the data and returns a
[Deephaven table](https://deephaven.io/core/docs/conceptual/table-types/). Each verb reads from
one of Massive's three sources:

- REST is Massive's request API, which answers each request with rows.
- WebSocket is Massive's streaming API, which sends each trade, quote, or bar as it happens. A
  verb that reads it opens a stream.
- Flat files are Massive's daily files of a whole market's trades, quotes, or bars, on paid
  plans.

A static table never changes after you get it. A live table changes as new data arrives, and
Deephaven applies each batch of changes in an update cycle, about once a second. Each verb
returns one kind of table:

| Verb | What you get | Source |
|---|---|---|
| `fetch()` | A static table of every row the request covers, read once | REST |
| `poll(seconds)` | A live table that repeats the request every `seconds` | REST |
| `history(start, end)` | A static table of the past rows in a range | REST or flat files |
| `blink()` | A live table of only the rows that arrived in the latest update cycle | WebSocket |
| `append_only()` | A live table of every row since the call | WebSocket |
| `ring(n)` | A live table of the latest `n` rows | WebSocket |
| `last_by()` | A live table of the latest row for each symbol | WebSocket |

`fetch()` and `history()` return a live table instead when you give them a live table of
symbols, as [Watchlists](watchlists.md) explains.

Calling a verb a dataset doesn't have raises `AttributeError`, which lists the verbs the dataset
has. Where another dataset has the verb you wanted, the error names that dataset. For example,
`m.stocks.trades("AAPL").fetch()` points you to `m.stocks.last_trade("AAPL").fetch()`.

<a id="choose-a-live-table"></a>

## Pick a live table

The live verbs differ in which rows they keep, and so in how much memory they use. Pick the one
that matches what you want to see:

| You want | Use |
|---|---|
| A running total or average over a stream | `blink()` and an aggregation |
| Every row of a session, to analyze afterwards | `append_only()`, whose memory keeps growing |
| A rolling window of recent rows | `ring(n)`, whose memory stays fixed |
| The current state of each symbol, as it trades | `last_by()` |
| The current state of each symbol, without a stream | `snapshots(...).poll(n)`, where your plan includes snapshots |
| A growing feed of news or filings | `poll(n)` with a `start` |

A [blink table](https://deephaven.io/core/docs/conceptual/table-types/#specialization-3-blink)
holds only the rows added in the current update cycle. An aggregation over it still counts every
row the blink table has delivered since you created the aggregation. A `sum_by` of `Size` over a
`blink()` of trades is each symbol's total volume since the call, and it uses almost no memory.

`last_by()` starts empty and fills in as each symbol trades or quotes. To key it by other
columns, pass a column name or a list, such as `last_by(["Symbol", "Exchange"])`. To give every
symbol a row before it trades, see
[Show every symbol before it trades](watchlists.md#seed-a-watchlist-board).

Live tables of one query share one stream from Massive. A `last_by()` and an `append_only()` of
the same trades count once against your plan's connection.

<a id="poll-modes"></a>

## Pick a poll mode

A `poll(seconds)` table repeats its request every `seconds`, a number or a `timedelta` of at
least 1 second. A shorter interval raises `ValueError`. For anything faster than once a second,
use a WebSocket verb such as `last_by()`.

The poll's `mode` decides how each new answer changes the table:

| Mode | The table | Use it for |
|---|---|---|
| `dm.PollMode.REPLACE` | One row per key, usually `Symbol`. A key the latest answer leaves out is deleted. | The current state, such as the day's top gainers |
| `dm.PollMode.UPSERT` | One row per key. Rows are added and updated, never deleted. | A list that grows and changes, such as upcoming IPOs |
| `dm.PollMode.APPEND` | A row for each new item, never changed or deleted | A feed, such as news articles |
| `dm.PollMode.AUTO`, the default | The dataset's own mode: `APPEND` for news and filings, `UPSERT` for IPOs, and `REPLACE` for the rest | Most polls |

Each dataset's reference page names its default mode, and `mode=` changes it. This poll keeps
every new snapshot of two stocks, not only the latest. Snapshots run 15 minutes behind on Stocks
Starter and Developer, so on those plans the session accepts delayed data:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))
path = m.stocks.snapshots(["AAPL", "MSFT"]).poll(30, mode=dm.PollMode.APPEND)
```

`path` gains a row for each stock whenever its snapshot changes. Over a trading day it becomes a
record of every state the two stocks passed through. On Stocks Advanced, where snapshots are
real-time, leave out the `MassiveConfig`.

<a id="verb-arguments"></a>

## Pass verb arguments

`poll()`, `ring()`, `last_by()`, and `fetch()` each take one or two arguments:

| Verb | Argument | Meaning |
|---|---|---|
| `poll(interval, *, mode=dm.PollMode.AUTO)` | `interval` | Seconds, or a `timedelta`, of at least 1 second |
| | `mode` | How each answer changes the table (see [Pick a poll mode](#poll-modes)) |
| `ring(capacity)` | `capacity` | The number of rows to keep |
| `last_by(by="Symbol")` | `by` | The key column or columns |
| `fetch(*, wait=True)` | `wait` | `True` waits for the rows. `False` returns at once and fills the table in the background. |

`history(start, end=None, *, source=dm.HistorySource.AUTO, reread=dm.Reread.ALWAYS, wait=True)`
takes four kinds of argument:

| Argument | Meaning | Details |
|---|---|---|
| `start`, `end` | The range to read | [Set a range](#start-and-end) |
| `source` | REST, flat files, or the library's choice | [Where history comes from](history.md#history-sources) |
| `wait` | `True` waits for the rows. `False` returns at once and fills the table in the background. | [Run a long read in the background](live-tables.md#long-reads) |
| `reread` | Whether a symbol that returns to a watchlist is downloaded again | [Read a returning symbol](#reread) |

<a id="symbols"></a>

## Name the symbols

A symbol is a Massive ticker, spelled exactly as Massive spells it. A list dataset returns records
about companies and markets, such as news articles, dividends, or option contracts, and its
symbols are optional. The first argument of most datasets names the symbols to read, in any of
these forms:

| Form | Example | Reads |
|---|---|---|
| One symbol | `"AAPL"` | That symbol |
| A list | `["AAPL", "MSFT"]` | Those symbols |
| A table with a `Symbol` column | `watchlist` | The table's rows. A live table is followed as you add and delete rows (see [Watchlists](watchlists.md)). |
| `dm.ALL` | `dm.ALL` | Every symbol Massive has for the dataset, such as the whole US market's daily bars. Each dataset's reference page says whether it takes `dm.ALL`. |
| `None`, or left out | `m.reference.news(limit=20)` | Every record, with no filter by symbol. Only list datasets take it. |

### Spell symbols as Massive does

Each asset class has its own form of symbol:

| Asset class | Example |
|---|---|
| Stock | `AAPL`, `BRK.B` |
| Option | `O:SPY271217C00700000` |
| Index | `I:SPX` |
| Forex | `C:EURUSD` |
| Crypto | `X:BTCUSD` |
| Future | `ESZ6` |

A wrongly spelled symbol in a string or a list raises `ValueError` at the call. A wrongly
spelled row of a table is skipped instead (see
[Find the rows the library skipped](watchlists.md#skipped-rows)).

An option symbol is its OCC code. In `O:SPY271217C00700000`, `SPY` is the underlying, `271217`
is the expiration date (2027-12-17), and `C` marks a call (`P` marks a put). The last eight
digits are the strike times 1,000, so `00700000` is a strike of 700.

### Convert other spellings

The converters turn other spellings into Massive's. They work on every plan, because they don't
call Massive:

```python
import deephaven_massive as dm

eurusd = dm.canonical_symbol_forex("EUR/USD")
berkshire = dm.canonical_symbol_stock_yahoo("BRK-B")
```

`eurusd` is `"C:EURUSD"`, and `berkshire` is `"BRK.B"`. `help(dm.canonical_symbol)` lists every
converter, including the ones you can use inside Deephaven query strings.

`dm.canonicalize_symbols` converts a whole table. On a live table, it keeps converting rows as
they arrive:

```python
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

pairs = new_table([string_col("Symbol", ["EUR/USD", "GBP-USD"])])
converted = dm.canonicalize_symbols(pairs, dm.AssetClass.FOREX)
```

`converted` has the same rows, with `Symbol` values `C:EURUSD` and `C:GBPUSD`. A row the
converter can't read gets a null `Symbol`, which every dataset ignores, and the table keeps
updating. The log gets one WARNING for each bad value, up to 20 for each table. Pass
`on_error=dm.OnError.RAISE` to fail on the first bad value.

### Read records that list several symbols

News, filings, and financial statements list the symbols each record concerns in their `Tickers`
column. Each record appears once for each symbol you named that it lists. An article about AAPL
and MSFT, read with `["AAPL", "MSFT"]`, is two rows, one with `Symbol` AAPL and one with
`Symbol` MSFT. Read without `symbols`, the article is one row, and its `Symbol` is null.

<a id="option-contract-limit"></a>

### Stream at most 1,000 option contracts

The options WebSocket connection carries at most 1,000 topics. A topic is one contract and one
kind of data, so a contract's trades and its quotes count as two. The limit counts every session
of the server that shares the connection.

A list or a static table that goes over the limit raises `ValueError`. A live watchlist that
grows past it holds the extra contracts back until room frees up, and the `Deferred` column of
`m.streams()` counts the contracts waiting.

<a id="start-and-end"></a>

## Set a range

`history(start, end)` takes a range. List datasets with a time axis take the same range through
`start` and `end`, such as dividends by ex-date or news by publication time. Each dataset's
reference page names its axis. Both bounds are dates, or both are instants:

| Kind | Range | Accepted values |
|---|---|---|
| Dates | Whole days in the [market's time zone](#time), both ends included. `history("2025-06-02", "2025-06-02")` is all of June 2. | Text such as `"2025-06-02"`, a Python `date`, or a Deephaven `LocalDate` |
| Instants | Exact times, the start included and the end excluded | A Python `datetime` with a time zone, a `pandas.Timestamp`, a `numpy.datetime64`, a Deephaven `Instant` or `ZonedDateTime` (such as `dh_now()`), or Deephaven time text such as `"2025-06-02T09:30 ET"` |

A `datetime` without a time zone raises `ValueError`. A calendar-date axis, such as an
ex-dividend date, takes dates only.

Instants taken from another table work directly as bounds. To read every trade inside one bar,
for example, pass the bar's `TimestampStart` and `TimestampEnd`, as
[How to read every trade of a bar](history.md#busiest-minute) shows.

When you leave `end` out, `history()` reads through now. With dates, that's through today in
the market's time zone, or through the current session for futures. A list dataset sends no end
to Massive, so `m.stocks.dividends("AAPL", start="2025-01-01")` includes dividends that are
announced but not yet paid.

<a id="reread"></a>

### Read a returning symbol

When you pass `history()` a live table of symbols, a [watchlist](watchlists.md), `reread`
decides what happens to a symbol you delete and then add back:

| Value | A returning symbol | Cost |
|---|---|---|
| `dm.Reread.ALWAYS`, the default | Downloaded again, and empty until the download lands | One request each time it returns |
| `dm.Reread.NEVER` | Gets its earlier rows back at once, with no request | Memory, because every symbol the table ever held keeps its rows |

Under either value, a symbol whose read failed is downloaded again when it returns. A `fetch()`
that follows a watchlist has no `reread` argument, and always downloads a returning symbol
again.

Choose `NEVER` only when out-of-date rows are acceptable. Massive corrects past data for late
cancellations, backfills, and splits, so an earlier download can differ from today's.
`m.symbol_reads()` shows when each symbol's rows were downloaded, in its `ReadAt` column.

A deleted symbol's rows leave the table at once under either value. With `wait=True`, though,
the rows first read for the symbols on the watchlist at the call stay in memory until you
release the table, even under `ALWAYS`.

<a id="time"></a>

## Know which time zone a date uses

Tables and arguments use two kinds of time. An instant is an exact moment, such as a trade's
`Timestamp`, and it's a Deephaven `Instant`. A date is a calendar day in the market's own time
zone, written as `yyyy-MM-dd` text. The asset class decides the time zone:

| Asset class | A date is the day in |
|---|---|
| Stocks, options, indices | New York |
| Forex, crypto | UTC |
| Futures | The trading session that ends on that date. Most sessions open at 17:00 Chicago time the evening before and stop trading at 16:00 that day. |

A stock's `Date` and a crypto pair's `Date` for the same calendar day cover different hours. The
crypto day starts at midnight UTC, four or five hours before New York's. To compare the same
hours across asset classes, build your figures from minute bars over one window you choose.

<a id="as-of"></a>

## Read as of a past date

A dataset that describes something as it stood on a past day takes `as_of`, a date. Ticker
details, the ticker list, option and futures contracts, and futures products take it. Leave it
out and you get today's state.

This reads Apple's details as they stood at the start of 2020. It needs Stocks Starter or
higher, because Stocks Basic keeps two years of ticker details:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
apple_2020 = m.reference.ticker_details("AAPL", as_of="2020-01-02").fetch()
```

`apple_2020` has one row, with Apple's name, listing details, and share counts as of January 2,
2020.

<a id="sort-order-limit"></a>

## Sort and limit the rows

List datasets take `sort` and `order`. `sort` names the field or fields to order by, as Massive
names them, such as `"ex_dividend_date"`. Where the endpoint allows it, `sort` takes several
fields separated by commas, each with an optional `.asc` or `.desc`, as in
`"ticker,ex_dividend_date.desc"`. `order` is `dm.Order.ASC` or `dm.Order.DESC`.

Each dataset takes only the sort fields its endpoint accepts, which its reference page lists. A
dataset whose endpoint can't sort has no `sort` argument. The indicators take `order` and `limit`
without `sort`.

`limit` caps the rows Massive returns for the whole request, across every symbol you name. A
symbol that comes first can use up the whole cap, so `limit` suits a sample. Read without it
when you need every row for each symbol. `limit` keeps the first rows in Massive's order, so ask
for newest first when you want the latest.

This poll keeps Apple's 50 newest articles, on any Stocks plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
latest_news = m.reference.news(
    "AAPL", sort="published_utc", order=dm.Order.DESC, limit=50
).poll(60)
```

`latest_news` holds the 50 articles, with their titles, publishers, and publication times, and
reads them again every minute.

A news or filings `poll()` needs a `start` or a `limit`. Without either, every poll would read
the whole archive.

A few datasets and reads apply `limit` to something else:

| Dataset or read | What `limit` caps |
|---|---|
| The indicators, and `reference.tickers` and `stocks.ipos` with symbols named | Each symbol's request |
| `reference.news` | Each symbol's request. Its `sort` and `order` apply to each symbol too. |
| `options.contracts` with contracts named | Nothing, because each contract is one row |
| Benzinga news, financial statements, and filings | The records Massive returns. Each record then appears once for each named symbol it lists, so the table can hold more rows than `limit`. |
| A `fetch()` that follows a watchlist | Each symbol's request |

Each dataset's `help()` says when its `limit` applies to each symbol.

<a id="filters"></a>

## Filter with Massive's parameters

Any other parameter Massive documents for a list dataset goes in `filters`, a mapping from the
parameter's name to its value. Write each dot in the name as `_`, so Massive's
`strike_price.gte` becomes `strike_price_gte`. Each dataset's reference page lists the keys it
accepts.

This lists SPY's option contracts with a strike of 600 or more, on any Options plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
big_strikes = m.options.contracts(underlying="SPY", filters={"strike_price_gte": 600}).fetch()
```

`big_strikes` has one row for each contract, with its symbol, expiration date, strike, and type.

A key the endpoint doesn't take raises `ValueError` that names the valid keys. So does a key that
has its own argument, such as `start`, `symbols`, or `adjusted`. A date or time filter takes
ISO-8601 text, or any value that [Set a range](#start-and-end) lists.

<a id="adjusted"></a>

## Choose split-adjusted prices

The bars, daily open-close summaries, and previous-day bars of stocks and options take
`adjusted`, and so do the four indicators. `True`, the default, gives split-adjusted prices, restated in terms of later stock
splits. `False` gives prices as traded. Only REST serves adjusted bars, while flat files and live
bars are always as traded. For what adjustment does to a price series, see
[Split adjustment](history.md#split-adjustment).

<a id="timespan-and-multiplier"></a>

## Set the bar length

Every `bars()` call names its bar length with two arguments:

- `timespan`, required, is a `dm.Timespan` member: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`,
  `MONTH`, `QUARTER`, or `YEAR`. Futures take `SESSION` in place of `DAY`.
- `multiplier` is the number of those units in each bar. The default is 1.

One-second and one-minute bars with `multiplier=1` also stream, and every other length is
history only. Text such as `"day"` raises `TypeError`. Write `dm.Timespan.DAY`, or convert text
with `dm.Timespan("day")`.

<a id="venues"></a>

## Choose futures venues

Futures trades, quotes, and bars take `venues`, the exchanges whose flat files `history()`
reads. The members of `dm.FuturesVenue` are `CME`, `CBOT`, `NYMEX`, and `COMEX`, and the
default is all four. Massive sells futures flat files by exchange, so pass the ones your plan
holds. Streams and REST reads ignore `venues`.

A read of one symbol uses REST unless you ask for flat files, so pass `source` along with
`venues`. This reads the December 2026 E-mini S&P 500 contract's trades on October 1 and 2,
2026, from CME's flat files. It needs Futures Developer or higher, with your
[S3 keys](install.md#s3-keys) set:

```python skip-test
# Futures trades from CME's flat files: needs your S3 keys.
import deephaven_massive as dm

m = dm.MassiveSession()
es = m.futures.trades("ESZ6", venues={dm.FuturesVenue.CME}).history(
    "2026-10-01", "2026-10-02", source=dm.HistorySource.FLAT_FILES
)
```

`es` has one row for each trade of `ESZ6` in the sessions of October 1 and 2, 2026, with its
time, price, and size.

<a id="unreadable-data"></a>

## Handle data the library can't read

You get every row the library can read, and the table keeps running when Massive sends something
it can't. A problem that affects every row, such as a rejected API key, fails the table instead.
Every table handles these two problems as follows:

| What Massive sent | What you see |
|---|---|
| A value that doesn't fit its column | A null in that cell, one WARNING in the log for each dataset and field, and a `?` entry in the table's `SchemaDrift` column in [`m.streams()`](troubleshooting.md#diagnostics) |
| A row without a symbol or a time | No row, a line in the log, and a count of the dropped rows in `SchemaDrift` |

A problem that affects only some symbols has its own `dm.MassiveConfig` setting:

| Setting | The problem | What you get by default |
|---|---|---|
| `noncanonical_symbols` | A row of your symbol table isn't spelled as Massive spells the ticker | The row is skipped, with one WARNING |
| `unlisted_symbols` | A crypto pair isn't on Massive's list of crypto tickers | The pair is skipped, with one WARNING |
| `unavailable_symbols` | Massive has no data for a symbol, or a poll can't read it with your arguments | The symbol is left out, with one WARNING |
| `unreadable_symbols` | Massive sent a symbol in a spelling the library can't read | The row is kept with a null `Symbol` and Massive's text in `RawSymbol`, with one ERROR. Report it, because it's a gap in the library. |

Each setting takes one of three values:

| Value | What happens |
|---|---|
| `dm.OnError.LOG`, the default | As in the table, with one log line for each distinct value |
| `dm.OnError.IGNORE` | As in the table, with log lines at DEBUG only |
| `dm.OnError.RAISE` | The read raises, or the live table stops and shows the error. Under `unreadable_symbols`, the table has no `RawSymbol` column. |

This session fails a read that receives a symbol it can't read, where the default keeps the row.
It works on every plan:

```python
import deephaven_massive as dm

strict = dm.MassiveSession(dm.MassiveConfig(unreadable_symbols=dm.OnError.RAISE))
```

`strict` reads like any other session until Massive sends a symbol it can't read. Then a read
through it raises, or its live table stops, with an error that names the spelling.

<a id="columns"></a>

## See the columns every table shares

These columns have the same name and meaning in every table, so tables from different datasets
join on them without renaming:

| Column | What it holds |
|---|---|
| `Symbol` | The symbol, spelled exactly as Massive spells its ticker, such as `AAPL` or `X:BTCUSD` |
| `Timestamp` | When a trade or quote happened, as a Deephaven `Instant` |
| `TimestampStart`, `TimestampEnd` | A bar's window, in place of `Timestamp` (see [How bar times work](history.md#bars)) |
| `Date` | The trading day, as `yyyy-MM-dd` text |
| `TimestampInsert` | When the library received the row |

Every calendar date in every table is `yyyy-MM-dd` text, so a date from one table joins to a
date from any other. Tables of one kind from two asset classes, such as stock and forex quotes,
can still have different columns, and a few columns share a name but not a type. The
[dataset reference](../datasets/README.md) shows how to stack such tables.
