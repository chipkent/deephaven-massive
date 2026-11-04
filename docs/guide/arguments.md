# Arguments, verbs, and columns

Every dataset is called the same way, so one set of rules covers them all. An argument such as
`symbols` or `limit` means the same thing on every dataset that takes it. Each verb returns the
same kind of table on every dataset, and every table names its shared columns the same way. Look here for the
full rule when a dataset's `help()`, such as `help(m.stocks.dividends)`, sends you, or when a
call doesn't do what you expected. Each dataset's page in the
[dataset reference](../datasets/README.md) lists its columns, plans, and the values its `sort`
and `filters` accept.

<a id="verb-reference"></a>

## The verbs

A dataset call such as `m.stocks.trades("AAPL")` describes the data and sends nothing to
Massive. A verb on it, such as `.last_by()`, reads the data and returns a
[Deephaven table](https://deephaven.io/core/docs/conceptual/table-types/). Each verb reads from
one of Massive's three sources:

- REST is Massive's request API, which answers each request with rows.
- WebSocket is Massive's streaming API, which sends each trade, quote, or bar as it happens.
- Flat files are Massive's daily files of a whole market's trades, quotes, or bars, on paid
  plans.

This table lists every verb, the table it returns, and where its rows come from:

| Verb | What you get | Source |
|---|---|---|
| `fetch()` | A static table of every row the request covers, read once | REST |
| `poll(seconds)` | A live table that repeats the request every `seconds` | REST |
| `history(start, end)` | A static table of the past rows in a range | REST or flat files |
| `blink()` | A live table of only the rows that arrived in the latest update cycle | WebSocket |
| `append_only()` | A live table of every row since the call | WebSocket |
| `ring(n)` | A live table of the latest `n` rows | WebSocket |
| `last_by()` | A live table of the latest row for each symbol | WebSocket |

A static table never changes after you get it. A live table changes as new data arrives, and
Deephaven applies each batch of changes in an update cycle, about once a second. `fetch()` and
`history()` return a live table instead when you give them a live table of symbols, as
[watchlists](watchlists.md) explains.

Each dataset has only the verbs Massive can serve it with. If you call one it doesn't have,
Python raises `AttributeError`, which lists the verbs the dataset has and, where one exists,
another dataset that has the verb you wanted. For example, `m.stocks.trades("AAPL").fetch()`
points you to `m.stocks.last_trade("AAPL").fetch()`.

<a id="choose-a-live-table"></a>

## Which live table to use

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
`blink()` of trades is therefore each symbol's total volume since the call, and it uses almost
no memory.

`last_by()` starts empty and fills in as each symbol trades or quotes. To key it by other
columns, pass a column name or a list, such as `last_by(["Symbol", "Exchange"])`. To show a row
for every symbol before it trades, see
[show every symbol before it trades](watchlists.md#seed-a-watchlist-board). Several live tables
of one query share one stream from Massive, so a `last_by()` and an `append_only()` of the same
trades cost one subscription.

<a id="poll-modes"></a>

## Poll modes

A `poll(seconds)` table repeats its request every `seconds`, a number or a `timedelta` of at
least 1 second. A shorter interval raises `ValueError`, because it's almost always a typo, such
as `poll(0.01)` for `poll(10)`. For anything faster than once a second, use a stream.

The poll's `mode` decides how each new answer changes the table:

| Mode | The table | Use it for |
|---|---|---|
| `dm.PollMode.REPLACE` | One row per key, usually `Symbol`. A key the latest answer leaves out is deleted. | The current state, such as the day's top gainers |
| `dm.PollMode.UPSERT` | One row per key. Rows are added and updated, never deleted. | A list that grows and changes, such as upcoming IPOs |
| `dm.PollMode.APPEND` | A row for each new item, never changed or deleted | A feed, such as news articles |
| `dm.PollMode.AUTO`, the default | The dataset's own mode: `APPEND` for news and filings, `UPSERT` for IPOs, and `REPLACE` for the rest | Most polls |

Each dataset's reference page names its own mode, and `mode=` changes what the table means. This
poll records each new snapshot of two stocks, instead of only the latest, and needs a Stocks
Starter plan or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
path = m.stocks.snapshots(["AAPL", "MSFT"]).poll(30, mode=dm.PollMode.APPEND)
```

`path` gains a row for each stock whenever its snapshot changes, so over a day it becomes a
history of both stocks.

<a id="verb-arguments"></a>

## Verb arguments

Each verb takes these arguments:

| Verb | Arguments |
|---|---|
| `poll(interval, *, mode=dm.PollMode.AUTO)` | `interval`: seconds, or a `timedelta`, of at least 1 second. `mode`: see [poll modes](#poll-modes). |
| `ring(capacity)` | `capacity`: the number of rows to keep |
| `last_by(by="Symbol")` | `by`: the key column or columns |
| `history(start, end=None, *, source=dm.HistorySource.AUTO, reread=dm.Reread.ALWAYS, wait=True)` | `start`, `end`: see [start and end](#start-and-end). `source`: see [where history comes from](history.md#history-sources). `wait=False` reads in the background (see [long reads](live-tables.md#live-tables)). `reread`: see the next section. |
| `fetch(*, wait=True)` | `wait`: as for `history()` |

### A symbol that returns to a watchlist

The `reread` argument of `history()` matters only when `symbols` is a live table. It says
whether a symbol you delete and add back is downloaded again:

| Value | A returning symbol | Cost |
|---|---|---|
| `dm.Reread.ALWAYS`, the default | Downloaded again, so you get Massive's current answer. It has no rows until the download lands. | One request each time it returns |
| `dm.Reread.NEVER` | Gets its earlier rows back, with no request | Memory, because every symbol the table ever held stays in it |

A symbol whose read failed is always read again. Massive corrects past data, such as late
cancellations, backfills, and splits, so an earlier download can be out of date. Choose `NEVER`
only when stale rows are acceptable. `m.symbol_reads()` shows when each symbol's rows were
downloaded, in `ReadAt`.

Under either value, with `wait=True`, the rows of the symbols in the table at the call stay in
memory until you release the result. A `fetch()` that follows a watchlist has no `reread`, and
always reads a returning symbol again.

<a id="symbols"></a>

## Symbols

The first argument of most datasets names the instruments to read. It takes any of these forms:

| Form | Example | Reads |
|---|---|---|
| One symbol | `"AAPL"` | That instrument |
| A list | `["AAPL", "MSFT"]` | Those instruments |
| A table with a `Symbol` column | `watchlist` | The table's rows. A live table is followed as you add and delete rows (see [watchlists](watchlists.md)). |
| `dm.ALL` | `dm.ALL` | Every instrument, where the dataset can serve it |
| `None`, or left out | | On list datasets only, every row, with no filter by ticker |

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
spelled row of a table is skipped instead (see [rows the library skips](watchlists.md#skipped-rows)).

An option symbol is its OCC code. In `O:SPY271217C00700000`, `SPY` is the underlying, `271217`
is the expiration date (2027-12-17), and `C` marks a call (`P` marks a put). The last eight
digits are the strike times 1,000, so `00700000` is a strike of 700.

### Convert other spellings

The converters turn other spellings into Massive's. They need no plan, because they don't call
Massive:

```python
import deephaven_massive as dm

eurusd = dm.canonical_symbol_forex("EUR/USD")
berkshire = dm.canonical_symbol_stock_yahoo("BRK-B")
```

`eurusd` is `"C:EURUSD"`, and `berkshire` is `"BRK.B"`. `help(dm.canonical_symbol)` lists every
converter, including the ones you can use inside Deephaven query strings.

`dm.canonicalize_symbols` converts a whole table, and keeps converting rows as they arrive in a
live one:

```python
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

pairs = new_table([string_col("Symbol", ["EUR/USD", "GBP-USD"])])
converted = dm.canonicalize_symbols(pairs, dm.AssetClass.FOREX)
```

`converted` has the same rows, with `Symbol` values `C:EURUSD` and `C:GBPUSD`. A row it can't
convert gets a null `Symbol`, which every dataset ignores, and the table keeps updating. Each bad
value is logged once at WARNING, up to 20 for each table. Pass `on_error=dm.OnError.RAISE` to
fail instead.

### Rows that list several tickers

News, filings, and financial statements list the tickers each row concerns in their `Tickers`
column. Their `Symbol` is each ticker you named that the row lists. An article about AAPL and
MSFT, read with `["AAPL", "MSFT"]`, is two rows, one with `Symbol` AAPL and one with `Symbol`
MSFT. Read without `symbols`, its `Symbol` is null.

### Option contracts: at most 1,000 at once

The options WebSocket connection carries at most 1,000 topics. A topic is one contract and one
kind of data, so a contract's trades and its quotes count as two. The cap counts every session of
the server that shares the connection.

A list or a static table of contracts over the cap raises `ValueError`. A live watchlist that
grows past the cap holds the extra contracts back until room frees up, and the `Deferred` column
of `m.streams()` counts them.

<a id="start-and-end"></a>

## Start and end

`history(start, end)` takes a range, and so does every list dataset with a time axis, such as
dividends by ex-date or news by publication time. Each dataset's reference page names its axis.
Both bounds are dates, or both are instants:

| Kind | Range | Accepted values |
|---|---|---|
| Dates | Whole days in the [market's time zone](#time), both ends included. `history("2025-06-02", "2025-06-02")` is all of June 2. | Text such as `"2025-06-02"`, a Python `date`, or a Deephaven `LocalDate` |
| Instants | Exact times, the start included and the end excluded | A Python `datetime` with a time zone, a `pandas.Timestamp`, a `numpy.datetime64`, a Deephaven `Instant` or `ZonedDateTime` (such as `dh_now()`), or Deephaven time text such as `"2025-06-02T09:30 ET"` |

A `datetime` without a time zone is refused. Instants taken from another table work directly,
as in [read every trade of the busiest minute](history.md#busiest-minute).

When you leave `end` out, `history()` reads through now. With dates, that means through today in
the market's time zone, or through the current session for futures. A list dataset sends no end
at all, so `m.stocks.dividends("AAPL", start="2025-01-01")` includes announced dividends still to
come. A calendar-date axis, such as an ex-dividend date, takes dates only.

<a id="time"></a>

## Dates, instants, and the market day

Tables and arguments use two kinds of time. An instant is an exact moment, such as a trade's
`Timestamp`, and it's a Deephaven `Instant`. A date is a calendar day in the market's own time
zone, written as `yyyy-MM-dd` text. Which time zone that is depends on the asset class:

| Asset class | A date is the day in |
|---|---|
| Stocks, options, indices | New York |
| Forex, crypto | UTC |
| Futures | The trading session that closes at 17:00 Chicago time on that date. It opens at 17:00 Chicago time the evening before, or earlier after a holiday. |

A stock's `Date` and a crypto pair's `Date` for the same calendar day cover different hours. The
crypto day starts at midnight UTC, four or five hours before New York's. To compare the same
hours across asset classes, build your figures from minute bars over one window you choose.

<a id="as-of"></a>

## As of a past date

A dataset that describes something as it stood on a past day takes `as_of`, a date. Ticker
details, the ticker list, option and futures contracts, and futures products take it, and each
means today when you leave it out. This reads Apple's details as they stood at the start of
2020, on any Stocks plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
apple_2020 = m.reference.ticker_details("AAPL", as_of="2020-01-02").fetch()
```

`apple_2020` has one row, Apple's details as of January 2, 2020.

<a id="sort-order-limit"></a>

## Sort, order, and limit

List datasets take `sort` and `order`. `sort` names the field or fields to order by, as Massive
names them, such as `"ex_dividend_date"`. Where the endpoint allows it, `sort` takes several
fields separated by commas, each with an optional `.asc` or `.desc`, as in
`"ticker,ex_dividend_date.desc"`. `order` is `dm.Order.ASC` or `dm.Order.DESC`. Each dataset
accepts only the fields and forms its endpoint takes, which its reference page lists, and a
dataset whose endpoint can't sort has neither argument.

`limit` caps the rows Massive returns for the whole request, across every symbol you name and
every page. A symbol that comes first can use up the whole cap. Use `limit` to look at a
sample, and read without it when you need complete data for each symbol. `limit` keeps the first
rows in Massive's order, so ask for newest first when you want the latest. This poll keeps
Apple's 50 newest articles, on any Stocks plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
latest_news = m.reference.news(
    "AAPL", sort="published_utc", order=dm.Order.DESC, limit=50
).poll(60)
```

`latest_news` holds the 50 articles and reads them again every minute.

A few datasets apply `limit` differently:

| Dataset | What `limit` caps |
|---|---|
| The indicators, and `reference.tickers` and `stocks.ipos` with tickers named | Each symbol's request |
| `reference.news` | Each ticker's request. Its `sort` and `order` apply to each ticker too. |
| `options.contracts` with contracts named | Nothing, because each contract is one row |
| Benzinga news, financial statements, and filings | The rows Massive returns. Each row then appears once for each named ticker it lists, so the table can hold more rows than `limit`. |

Each dataset's `help()` says when its `limit` is per symbol. A `fetch()` that follows a
[watchlist](watchlists.md) sends `limit` with each symbol's request. A news or filings `poll()`
needs a `start` or a `limit`, because without either, every cycle would read the whole archive
again.

<a id="filters"></a>

## Filters

Any other parameter Massive documents for a list dataset goes in `filters`, a mapping from the
parameter's name to its value. Write each dot in the name as `_`, so Massive's
`strike_price.gte` becomes `strike_price_gte`. This lists SPY's option contracts with a strike of
600 or more, on any Options plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
big_strikes = m.options.contracts(underlying="SPY", filters={"strike_price_gte": 600}).fetch()
```

`big_strikes` has one row for each contract. Each dataset's reference page lists the keys it
accepts. A key the endpoint doesn't take raises `ValueError` that names the valid keys, and so
does a key that has its own argument, such as `start`, `symbols`, or `adjusted`. A date or time
filter takes ISO-8601 text, or any value that [start and end](#start-and-end) take.

<a id="adjusted"></a>

## Split adjustment

Stock and option bars, daily open-close summaries, previous-day bars, and the indicators take
`adjusted`. `True`, the default, gives split-adjusted prices, and `False` gives prices as
traded. Only REST serves adjusted bars, while flat files and live bars are always as traded. For
what adjustment does to a price series, see [split adjustment](history.md#split-adjustment).

<a id="timespan-and-multiplier"></a>

## Bar length

Every `bars()` call names its bar length with two arguments:

- `timespan`, required, is a `dm.Timespan` member: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`,
  `MONTH`, `QUARTER`, or `YEAR`. Futures take `SESSION` in place of `DAY`.
- `multiplier` is the number of those units in each bar. The default is 1.

One-second and one-minute bars with `multiplier=1` also stream, and every other length is
history only. Text is refused, so write `dm.Timespan.DAY`, or convert text with
`dm.Timespan("day")`.

<a id="venues"></a>

## Futures venues

Futures trades, quotes, and bars take `venues`, the exchanges whose flat files `history()`
reads. The members of `dm.FuturesVenue` are `CME`, `CBOT`, `NYMEX`, and `COMEX`, and the
default is all four. Massive sells futures flat files by exchange, so pass the ones your plan
holds. This reads two days of the December 2026 E-mini S&P 500 contract's trades from CME's
files, on a Futures Developer plan or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
es = m.futures.trades("ESZ6", venues={dm.FuturesVenue.CME}).history("2026-10-01", "2026-10-02")
```

`es` holds every trade of `ESZ6` in the sessions of October 1 and 2, 2026. Streams and REST reads
ignore `venues`.

<a id="columns"></a>

## Columns every table shares

Every table names the same things the same way, so tables from different datasets join without
renaming:

| Column | What it holds |
|---|---|
| `Symbol` | The instrument, spelled exactly as Massive spells its ticker, such as `AAPL` or `X:BTCUSD` |
| `Timestamp` | When a trade or quote happened, as a Deephaven `Instant` |
| `TimestampStart`, `TimestampEnd` | A bar's window, in place of `Timestamp` (see [bars](history.md#bars)) |
| `Date` | The trading day, as `yyyy-MM-dd` text |
| `TimestampInsert` | When the library received the row |

Every calendar date in every table is `yyyy-MM-dd` text, so a date from one table joins to a
date from any other. Tables of one kind from two asset classes, such as stock and forex quotes,
can still have different columns, and a few columns share a name but not a type. The
[dataset reference](../datasets/README.md) shows how to stack such tables.

### Data the library can't read

You get every row the library can read, and the table keeps running when Massive sends
something it can't. Each case shows up in its own way:

| What Massive sent | What you see |
|---|---|
| A value that doesn't fit its column | A null in that cell, one WARNING in the log for each dataset and field, and a `?` entry in the table's `SchemaDrift` column in [`m.streams()`](troubleshooting.md#diagnostics) |
| A row without a symbol or a time | No row, and a line in the log |

A problem that affects only some symbols has its own `dm.MassiveConfig` setting, which decides
what happens to the symbol:

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
| `dm.OnError.IGNORE` | The same, with log lines at DEBUG only |
| `dm.OnError.RAISE` | The read raises, or the live table stops and shows the error. Under `unreadable_symbols`, the table has no `RawSymbol` column. |

This session fails a read that receives a symbol it can't read, instead of keeping the row:

```python
import deephaven_massive as dm

strict = dm.MassiveSession(dm.MassiveConfig(unreadable_symbols=dm.OnError.RAISE))
```

A problem that affects every row, such as a rejected API key, fails the table whatever these
settings say.
