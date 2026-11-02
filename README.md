# deephaven-massive

`deephaven-massive` turns [Massive.com](https://massive.com) (formerly Polygon.io) market data
into [Deephaven](https://deephaven.io) tables. A stream of trades becomes a table that updates
as each trade arrives; ten years of daily prices become a table you can sort, join, and plot
like any other. The library runs inside a Deephaven server, and you call it from the Deephaven
console or from a script the server runs.

```python
import deephaven_massive as dm

m = dm.MassiveSession()

# Apple's daily bars for one week, read once. Works on every plan, the free one included.
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")

# The latest minute bar of each symbol, updated as each minute closes. Needs a paid Stocks plan.
latest = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).last_by()
```

Live data is real-time unless you ask for delayed data; on a delayed plan, such as Stocks
Starter, the live line raises an error that names the setting to add. See
[real-time or delayed](#real-time-or-delayed).

This README is the whole user guide. The [dataset reference](docs/datasets/README.md) lists
every dataset with its arguments, its columns, and what each Massive plan includes.

<a id="install"></a>

## Install

The repository is private for now and the library is not on PyPI: outside testers get access
and a version from the owner, and follow the [tester guide](docs/tester-guide.md), which walks
through every step. With access to the repository, you need Python 3.11 or later, Java 17 or
later (check with `java -version`; any current JDK works, such as one from
[Adoptium](https://adoptium.net)), and a Massive account:

```bash
python3 -m venv ~/dh-massive && source ~/dh-massive/bin/activate
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git"
export MASSIVE_API_KEY="your-api-key"    # from the keys page of Massive's dashboard
deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=choose-a-password"
```

Open <http://localhost:10000/?psk=choose-a-password> and use the **Console** panel; every example
here runs there. (Without `-Dauthentication.psk`, the server makes up a password and prints a
link that carries it in its startup log.) Set the key in the shell before you start the server,
never in the console: every user of the server can read the console's variables.

**Flat files (optional).** Massive's paid plans include flat files: one compressed file per day
holding every trade, quote, or bar of a whole market, the fast way to read a lot of history.
They live in Amazon S3 and have their own keys, issued beside the API key. Set both or neither:

```bash
export MASSIVE_S3_ACCESS_KEY_ID="your-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

**Settings in code.** Every setting can also be passed to `dm.MassiveConfig`;
`help(dm.MassiveConfig)` lists them all. Two matter from the start. `rest_basic_subscriptions`
names the subscriptions you hold on Massive's free Basic plan, which allows five REST requests a
minute, so the library paces its requests from the first one. `delayed_subscriptions` names
those whose live data you accept late: each one your Starter or Developer plan holds (see
[real-time or delayed](#real-time-or-delayed)). To keep settings in one file that every
notebook, console, and container reads, see [settings file](#settings-file).

```python
import deephaven_massive as dm

# Stocks on Basic; options on Starter, which streams delayed data.
config = dm.MassiveConfig(
    rest_basic_subscriptions={dm.Subscription.STOCKS},
    delayed_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

**One session is enough.** Create one session at the top of your script and use it for
everything. Massive's individual plans allow one WebSocket connection per asset class at a time.
Sessions of one server with the same key and the same connection settings share their
connections; sessions with different settings, such as another `max_outage`, compete for that
one connection.

<a id="first-tables"></a>

## First tables

Run these in the console, one after another. Each line names a dataset, gives it symbols, and
calls a *verb* that makes the table:

```python
import deephaven_massive as dm
from deephaven.time import dh_today, to_j_time_zone

m = dm.MassiveSession()

# Today in New York, the market's day, whatever the server's own time zone.
today = dh_today(to_j_time_zone("America/New_York"))

# History: Apple's daily bars for one week, a static table.
bars = m.stocks.bars("AAPL", dm.Timespan.DAY)
week = bars.history("2025-06-02", "2025-06-06")

# What each column of those bars means: Name, Type, Description, SuppliedBy.
columns = bars.columns()

# Live: the latest minute bar of each symbol (needs a paid Stocks plan).
latest = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).last_by()

# Live on any plan: re-read today's Apple headlines every five minutes.
headlines = m.reference.news("AAPL", start=today).poll(300)
```

Click a table's name to open it. `week` has one row per trading day; `latest` changes as each
minute closes (during market hours); `headlines` grows as articles appear. Hover over a column
header in the web UI to read what the column means.

Crypto trades around the clock, so it is the easiest market to watch at night or at weekends:
`m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).last_by()` (a paid Currencies plan).

<a id="how-it-works"></a>

## How it works

Every table comes from one pattern:

```text
m.<namespace>.<dataset>(<arguments>).<verb>()
```

- **The dataset call** (`m.stocks.trades(["AAPL"])`) builds a *query*: a description of the
  data. It checks its arguments at once and sends nothing, so you can build, store, and reuse
  queries freely.
- **The verb** (`.last_by()`) talks to Massive and returns a Deephaven table. Which verbs a
  query has depends on how Massive serves the dataset; tab completion shows them.
- **`columns()`** describes the dataset's columns without asking Massive anything, and
  `repr(query)` shows the call that built it.

Massive delivers data three ways, which this guide calls *transports*: **REST**, web requests
that return a page of rows at a time; **WebSocket**, a connection that streams each trade or
quote as it happens; and **flat files**, Massive's daily bulk files.

<a id="verbs"></a>

### Verbs

| Verb | Table | Transport |
|---|---|---|
| `fetch()` | static: every row the request covers, read once; live when it follows a live symbol table | REST |
| `poll(seconds)` | live: the same request again every few seconds | REST |
| `blink()` | live: only the rows of the latest update cycle | WebSocket |
| `append_only()` | live: every row since the call | WebSocket |
| `ring(n)` | live: the latest `n` rows | WebSocket |
| `last_by()` | live: the latest row per symbol (or per the columns you name) | WebSocket |
| `history(start, end)` | static: past rows in a range; live when it follows a live symbol table | REST or flat files |

A verb a dataset cannot serve does not exist on its query. Calling one raises `AttributeError`
naming the verbs the query has and, where one exists, the dataset that has the verb you wanted:
`m.stocks.trades("AAPL").fetch()` points you to `m.stocks.last_trade("AAPL").fetch()`.

<a id="namespaces"></a>

### Namespaces

| Namespace | What it holds |
|---|---|
| `m.stocks` | US stocks: trades, quotes, bars, snapshots, corporate actions, short interest |
| `m.stocks.financials` | company financial statements and ratios |
| `m.stocks.filings` | SEC filings: 10-K sections, 8-K text, insider and institutional holdings |
| `m.options` | US options: trades, quotes, bars, chains, contracts |
| `m.futures` | CME futures: trades, quotes, bars, contracts, schedules |
| `m.indices` | index values and bars |
| `m.forex` | currency pairs: quotes, bars, conversion |
| `m.crypto` | crypto pairs: trades, quotes, bars |
| `m.reference` | tickers, news, exchanges, condition codes, market status and holidays |
| `m.indicators` | SMA, EMA, RSI, and MACD of any asset class |
| `m.economy` | Federal Reserve series: treasury yields, inflation, labor market |
| `m.partners.benzinga` | Benzinga analyst ratings, earnings, guidance, and news |
| `m.partners.etf_global` | ETF Global constituents, fund flows, and analytics |
| `m.partners.fable` | Fable consumer-spending data |
| `m.partners.tmx` | TMX corporate events |

`m.snapshots(...)` is a method of the session itself, not a namespace: the current state of any
mix of tickers across asset classes, such as `m.snapshots(["AAPL", "O:SPY271217C00700000"])`.

`dm.datasets()` lists every dataset in one static table, with no session or key needed; filter
it to find what you want:

```python
catalog = dm.datasets()
streaming = catalog.where("Arrays.asList(Verbs).contains(`blink`)")
```

`help(m.stocks.trades)` describes one dataset. Its page in the
[dataset reference](docs/datasets/README.md) adds the columns and what each plan includes.

<a id="how-do-i"></a>

## How do I...

| Task | Code |
|---|---|
| read daily bars | `m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-01-02", "2025-06-30")` |
| read every stock's bars for a day | `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2025-06-02", "2025-06-02")` |
| stream minute bars | `m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).append_only()` |
| keep the current state of symbols without a stream | `m.stocks.snapshots(["AAPL", "MSFT"]).poll(15)`, where your plan includes snapshots |
| follow a watchlist I can edit | pass a table with a `Symbol` column: see [`symbols`](#symbols) |
| read the history of a watchlist I can edit | the recipes in [history of a watchlist](#history-of-a-watchlist) |
| backfill history, then continue live | the recipe in [backfill, then live](#backfill-then-live) |
| start a live board with values before the first trade | the recipe in [seed a live board](#seed-a-live-board) |
| read an option chain with its greeks | `m.options.chain("SPY").fetch()`: see [option chain](#option-chain) |
| read a long range without freezing the console | `history(..., wait=False)`: see [live tables](#live-tables) |
| list a company's dividends | `m.stocks.dividends("AAPL", start="2025-01-01").fetch()` |
| follow news as it appears | `m.reference.news("AAPL", start=today).poll(300)`, with `today` as [first tables](#first-tables) sets it |
| stream the contracts of a futures product | `m.futures.trades(m.futures.contracts(product_code="ES").fetch()).last_by()` |
| find a ticker by name | `m.reference.tickers(search="Nvidia").fetch()` |
| see what a column means | `query.columns()`, or hover over the column header |
| see what the library is doing | `m.streams()`, `m.connections()`, `m.rate_limits()`: see [diagnostics](#diagnostics) |
| stop a live table now | `m.release(table)`: see [live tables](#live-tables) |
| read the library's log | `dm.enable_debug_logging()`: see [troubleshooting](#troubleshooting) |

<a id="recipes"></a>

## Recipes

<a id="backfill-then-live"></a>

### Backfill, then live

A table that starts with history and continues with the stream: today's minute bars so far,
then each new one as it closes. The function opens the stream first, so no bar falls between
the two, and keeps one row per minute:

```python
from deephaven import merge


def history_and_live(symbol, start):
    """Adjusted minute bars from `start`, then live ones; call it again after a split."""
    bars = m.stocks.bars(symbol, dm.Timespan.MINUTE)
    live = bars.append_only()
    past = bars.history(start)
    return merge([past, live]).last_by(["Symbol", "TimestampStart"])
```

A minute both hold shows the stream's bar. Massive recomputes REST bars with trades reported
late, so a minute's REST bar can differ slightly from the live one. Build the table again after
a split: see [split adjustment](#split-adjustment).

<a id="seed-a-live-board"></a>

### Seed a live board

`last_by()` starts empty and fills in as each symbol trades, so a quiet symbol stays missing.
To start every row at once, put a snapshot under the stream: the snapshot's row shows until the
symbol's first trade replaces it. Both tables need the same columns, so take the few you show:

```python
from deephaven import merge

watch = ["AAPL", "MSFT", "NVDA"]
seed = (
    m.stocks.snapshots(watch)
    .fetch()
    .view(["Symbol", "Price = LastTradePrice", "Timestamp = LastTradeSipTimestamp"])
)
trades = m.stocks.trades(watch).last_by().view(["Symbol", "Price", "Timestamp"])
board = merge([seed, trades]).last_by("Symbol")
```

The snapshot needs a Stocks plan that includes snapshots, and real-time data like the stream
(see [real-time or delayed](#real-time-or-delayed)).

<a id="option-chain"></a>

### Option chain with greeks

`m.options.chain(underlying)` returns one row per contract of the underlying: its details, last
quote and trade, implied volatility, greeks, and open interest. Filter it by expiration or
strike, and `poll()` it to keep it current:

```python
from deephaven.time import dh_today, to_j_time_zone

calls = m.options.chain(
    "SPY",
    contract_type=dm.ContractType.CALL,
    filters={"expiration_date_gte": dh_today(to_j_time_zone("America/New_York"))},
    sort="expiration_date",
    limit=500,
)
greeks = calls.fetch().view(
    [
        "Symbol",
        "DetailsExpirationDate",
        "DetailsStrikePrice",
        "ImpliedVolatility",
        "GreeksDelta",
        "GreeksGamma",
        "GreeksTheta",
        "GreeksVega",
        "OpenInterest",
    ]
)
```

Massive leaves the greeks out for some contracts, such as those deep in the money; their
columns are null then. On Options Starter or Developer the chain is delayed (see
[real-time or delayed](#real-time-or-delayed)).

<a id="history-of-a-watchlist"></a>

### History of a watchlist

Pass a live watchlist to `history()` and the table follows it: each symbol you add is read over
the same range, and each one you delete leaves (see [`symbols`](#symbols)). Two other ways fit
other needs. To read the watchlist's symbols once, as they stand now, pass `snapshot()` of it.
To follow it with no request per symbol, read every symbol once and filter by the watchlist:
adding or deleting a symbol then costs nothing. That works where the dataset serves `dm.ALL`
and the whole market fits in memory: daily bars, reference lists, or one day of minute bars,
but not a year of minute bars or any range of trades and quotes. A data index on `Symbol` keeps
each change of the watchlist fast:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col
from deephaven.experimental.data_index import data_index

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))

# Read once: the watchlist's symbols now, never again.
fixed = m.stocks.bars(names.snapshot(), dm.Timespan.DAY).history("2025-06-02", "2025-06-06")

# Every stock's daily bars, read once and filtered by the live watchlist.
every = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
data_index(every, ["Symbol"])
mine = every.where_in(names, "Symbol")
```

## Concepts

<a id="table-shapes"></a>

### Table shapes

A **static** table never changes once you have it: `fetch()` and `history()` return one,
unless they follow a live symbol table (see [`symbols`](#symbols)). A
**live** table changes while you watch it. The live verbs differ in what they keep:

| You want | Use |
|---|---|
| a running total or average over a stream | `blink()` and an aggregation |
| every row of a session, to analyze afterwards | `append_only()` (memory grows) |
| a rolling window of recent rows | `ring(n)` (fixed memory) |
| the current state of each symbol, as it trades | `last_by()` |
| the current state of each symbol, without a stream | `snapshots(...).poll(n)`, where your plan includes snapshots |
| a growing feed of news or filings | `poll(n)` on the feed, with a `start` |
| a reference list, read once | `fetch()` |
| a fixed range of the past | `history(start, end)` |

A blink table holds only the rows that arrived in the latest update cycle, about once a second;
Deephaven's aggregations over it count every row that ever arrived, so
`trades.view(["Symbol", "Size"]).sum_by("Symbol")` is the total volume since the call. Opening
two shapes of one query, or the same shape twice, subscribes once: the tables share one stream.

`last_by()` starts empty and fills in as each symbol ticks; it never asks REST for a starting
value. Pass a column name or a list to key by something else, such as
`last_by(["Symbol", "Exchange"])`.

`poll(seconds)` combines its cycles by a `mode`. `dm.PollMode.REPLACE` keeps one row per key
(usually `Symbol`) and drops keys the latest cycle did not return. `UPSERT` keeps one row per
key and never drops one. `APPEND` adds rows it has not seen and never changes or removes one, so
the table grows into a feed. The default, `dm.PollMode.AUTO`, uses each dataset's own mode,
which its reference page names: `APPEND` for news and filings, `UPSERT` for IPOs, `REPLACE`
otherwise. Pass `mode=` to change what the table means; `APPEND` on a snapshot records every new
state of each symbol:

```python
path = m.stocks.snapshots(["AAPL", "MSFT"]).poll(30, mode=dm.PollMode.APPEND)
```

Every table follows the same column conventions. `Symbol` holds Massive's exact ticker.
`Timestamp` is when a trade or quote happened; a bar has `TimestampStart` and `TimestampEnd`
instead. `Date` is the trading day as `yyyy-MM-dd` text, and every other calendar date is text
in the same form, so a date from one table joins to a date from any other. `TimestampInsert` is
when the library received the row.

When Massive sends something odd, the library serves what it can. A value that does not fit its
column is null, with one WARNING per dataset and field (and a `?` in a live table's
`SchemaDrift`). A symbol the library cannot read leaves `Symbol` null and keeps Massive's text
in `RawSymbol`; `dm.MassiveConfig(unreadable_symbols=dm.OnError.RAISE)` fails the read instead.
A row missing its symbol or time is dropped and logged.

Tables of one kind from two asset classes, such as stock and forex quotes, share their common
columns' names and types but can differ in which columns they have. To stack them, keep the
shared columns:

```python
from deephaven import merge

stock = m.stocks.bars("SPY", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
fx = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
common = [c for c in stock.column_names if c in fx.column_names]
both = merge([t.view(common) for t in (stock, fx)])
```

<a id="time"></a>

### Time: dates, instants, and the market day

Two kinds of time appear in every table and every argument.

- An **instant** is an exact moment, such as a trade's `Timestamp`. Instants are Deephaven
  `Instant` values.
- A **date** is a calendar day in the market's own time zone, stored as `yyyy-MM-dd` text. The
  market day is New York time for stocks, options, and indices, and UTC for forex and crypto.
  For futures, a date names a trading session, which opens at 17:00 Chicago time the evening
  before (earlier after a holiday) and closes at 17:00 Chicago time on that date.

So a stock's `Date` and a crypto pair's `Date` for the same calendar day cover different hours:
the crypto day starts four or five hours earlier. To compare the same hours across classes,
build the figures from minute bars over one window of your choosing.

<a id="bars"></a>

### Bars

A bar summarizes a window: open, high, low, close, volume, the volume-weighted average price,
and the trade count. These rules hold for every `bars()` dataset.

- **The window.** `TimestampStart` is when the window opens and `TimestampEnd` the first instant
  after it. An intraday bar (second, minute, hour) spans its own length. A bar of a day or
  longer runs from 00:00 of its first day to 00:00 after its last, in the market's time zone;
  an index bar ends an hour later, at 01:00 New York time.
- **Weeks and longer.** Massive starts a week on Sunday and names it by that Sunday. A stock or
  option week covers Monday to Friday, and its month, quarter, or year leaves out the weekend
  days at either end. Indices, forex, and crypto, which can trade at weekends, cover whole
  calendar weeks (Sunday to Saturday), months, quarters, and years. A bar of several days spans
  that many calendar days, weekends included, as Massive counts them: a 2-day stock bar stamped
  on a Friday holds Friday's trading alone.
- **Futures bars of a session or longer.** Massive groups whole sessions into blocks of
  calendar days in UTC, and a session belongs to the day it opens, the evening before its date.
  `TimestampStart` is 00:00 UTC on the block's first day and `TimestampEnd` 17:00 Chicago time
  on the bar's last session, Massive's `session_end_date`. So a month bar holds the sessions
  that open in that month: September 2026's runs from the session of September 2 to that of
  October 1. An n-session bar covers n calendar days, so over a weekend it holds fewer than n
  sessions. With dates, `history()` returns a bar only when all its sessions lie in the range,
  so a month bar needs the range to reach the session that opens on the month's last day:

  | Call | Returns |
  |---|---|
  | `m.futures.bars("ESZ6", dm.Timespan.MONTH).history("2026-09-01", "2026-10-01")` | September's bar: `TimestampStart` 2026-09-01 00:00 UTC, `TimestampEnd` 2026-10-01 17:00 Chicago time. The same call ending `"2026-09-30"` returns no bar. |
- **Join on the end.** A bar's values are final only once its window has ended, so join other
  data on `TimestampEnd`: `trades.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"])` gives
  each trade the latest bar that had closed by then. Minute bars of every class end on whole
  minutes, so a stock's and an index's 10:31 bars join exactly on `TimestampEnd`. A row read
  before its window ended (today's bar from `fetch()`, an indicator's latest value) has
  `TimestampInsert` before `TimestampEnd` and can still change:
  `t.where("TimestampEnd <= TimestampInsert")` keeps only the rows read after their window
  ended. Two cases need care. A futures bar of several sessions or longer, read before its
  last session, passes once its latest session so far has ended, though later sessions can
  still join it. A `poll(mode=dm.PollMode.APPEND)` table of indicators keeps the first
  unfinished value it sees and never receives the finished one, so the filter drops that
  period for good: `poll()` in its default mode holds the updated value.
- **`Date`** is the bar's trading day. It is null for a bar that spans more than one date: a
  bar of several days, a week or longer, or an intraday bar across midnight.
- **Which bars `history()` returns.** Only bars whose whole window lies inside the range and has
  already ended. Today's daily bar arrives tomorrow; the current week's or month's bar never
  appears; monthly bars from March 5 to March 20 return nothing, because no whole month fits.
  Pass dates, not instants, for daily bars: instants keep only whole calendar days inside them.
- **Odd stamps.** Massive stamps the same bar differently on different sources, such as a daily
  stock bar at 00:00 on one and at the 16:00 close on another; the library moves each known
  stamp to the start of its day, so every source gives the same rows. A bar stamped at a time it
  does not expect, such as a daily bar at 14:30, could cover that day or the next, so it is
  served as Massive stamped it: `TimestampStart` is the stamp, `TimestampEnd` one period later
  (no earlier than the end of any such period that holds the stamp), and `Date` is empty, with
  one WARNING for each time of day such stamps take. `history()` returns such a bar only when the range holds that whole window, so it may need a
  range one period longer. Please report the WARNING on the
  [issues page](https://github.com/chipkent/deephaven-massive/issues). A crypto
  `daily_open_close` row that Massive marks as not UTC keeps its `Date`, but its window is the
  widest that date has in any time zone, from 14 hours before UTC midnight to 12 hours after the
  next.

<a id="history-sources"></a>

### Where history comes from

`history()` reads from one of two sources. The default picks one and uses the other only for
what the first cannot serve, so a call returns your data whenever either source has it.

- **REST** serves explicit symbols on any dataset with a REST history endpoint. For `dm.ALL` it
  serves only the daily bars of stocks, forex, and crypto, through Massive's grouped daily
  endpoint, one request per day; options and indices have none.
- **Flat files** serve a read when you have set S3 keys and the dataset has files for the
  request. Massive usually publishes a day's files by about 11:00 New York time the next day,
  but the time varies: Friday's can arrive late on Saturday morning. Bars come in files only as
  1-minute and 1-day bars (and 1-session bars for futures), and only as traded
  (`adjusted=False`).

The default, `source=dm.HistorySource.AUTO`, picks flat files when only they can serve the read,
or when both can and the read has many symbols (50 or more, the `flat_file_symbol_threshold`
setting) or `dm.ALL`; otherwise REST. A `history()` that follows a live symbol table ignores
the count: it reads REST whenever REST can serve the read, so adding one symbol never re-reads a
year of flat files. Pass `source=dm.HistorySource.REST` or `FLAT_FILES` to choose. The two
sources sometimes fill different columns (REST trades have `DecimalSize`; the
files do not); when `AUTO`'s choice leaves columns empty that the other source fills, the
library logs a WARNING naming them and the `source=` that fills them. Each result's description,
shown in the web UI, names its source and why.

When a weekday's flat file is missing (not out yet, or never published):

- **`AUTO`, and REST can serve the read:** that day is read from REST, so you get the whole
  range; a WARNING and the table's description name the days. If your plan lacks the files, or
  S3 rejects your S3 keys, `AUTO` reads REST instead, with a WARNING.
- **REST cannot serve the read (such as trades of `dm.ALL`), or `source=FLAT_FILES`:** a day
  with a later day's file is taken for a holiday, and a WARNING and the table's description
  name it, since Massive's file may simply be missing; otherwise the call fails. With
  `FLAT_FILES`, files your plan lacks are an error too.

Split-adjusted bars come only from REST, one symbol at a time (every symbol at once only for
1-day bars), so split-adjusted minute bars of `dm.ALL` have no source: pass `adjusted=False`
for the as-traded files, or name the symbols.

<a id="split-adjustment"></a>

### Split adjustment

When a company splits its stock 10 for 1, each share becomes ten shares worth a tenth of the
price, and a chart of raw prices drops 90% overnight although nothing happened to the company.
Massive can restate older prices in the split's terms so the series is continuous: a
*split-adjusted* price.

- Massive adjusts stock and option bars for splits (forward and reverse) when asked, and never
  for cash dividends. Index, forex, crypto, and futures bars have nothing to adjust.
- Flat files and live WebSocket bars are as traded and carry no adjustment factor.
- `bars()` and `daily_open_close()` of stocks and options take `adjusted=True` by default. Only
  REST serves adjusted bars, and the library checks Massive's `adjusted` echo on every page, so
  one `history()` never mixes the two bases. `adjusted=False` reads REST or flat files.

These examples reach back to June 2024, further than Stocks Basic's two years of history; on
Basic, try them with a split inside your window.

```python
# Split-adjusted daily bars, the default. NVDA split 10 for 1 on 2024-06-10,
# so its 2024-06-07 close reads about 120.9.
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")

# As traded: the 2024-06-07 close reads about 1208.
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
```

To adjust as-traded bars yourself, use the splits dataset's cumulative
`HistoricalAdjustmentFactor`. Massive's rule: for a price on date D, take the first split whose
`ExecutionDate` is after D, multiply the price by its factor, and divide the volume by it. A
reverse as-of join finds that split, and a null factor means no later split:

```python
splits = m.stocks.splits("NVDA").fetch()
mine = traded.raj(
    splits, on=["Symbol", "Date<ExecutionDate"], joins=["Factor=HistoricalAdjustmentFactor"]
).update(
    [
        "Factor = isNull(Factor) ? 1.0 : Factor",
        "AdjustedClose = Close * Factor",
        "AdjustedVolume = Volume / Factor",
    ]
)
```

Adjust daily bars and aggregate them afterwards, since a longer bar's `Date` is null. The recipe
applies stock dividends (`AdjustmentType` `stock_dividend`) too; Massive does not document
whether its own adjustment does, so the two can differ around one.

**History and live together.** Live bars are as traded, which is also the adjusted basis until
the next split, so adjusted history and live bars join without a seam: see
[backfill, then live](#backfill-then-live). On a split's execution date every price moves to
the new basis, so a table that runs across a split, or joins adjusted history to live bars,
shows a step there: build it again after a split. A long download that runs while Massive
applies a split overnight can return pages on both bases, which nothing can detect: download
again after the split. The indicators use Massive's own basis, adjusted.

<a id="live-tables"></a>

### Live table lifetime

A live table runs for as long as it, or any table derived from it, is in use. You never have to
keep the session or the query around. Static tables hold nothing open.

**Stop one table now with `m.release(table)`.** Deleting a variable stops its stream only when
Deephaven next reclaims memory, which can take a while on a quiet server. `m.release()` ends
the library's hold at once: if nothing else uses the stream (another table of the same query, a
table derived from it, or a panel open in the web UI), the library unsubscribes or stops the
poll; otherwise `m.streams()` shows it `RELEASED` until the last user goes. It accepts the live
tables the session's verbs returned, background downloads, and the diagnostics tables;
releasing twice does nothing, and a table you derived yourself is refused with `ValueError`.
A `history()` or `fetch()` that follows a live symbol table is a live table too: it keeps
reading the symbols you add while it is in use, and releasing it stops its reads.

**Stop everything with `m.close()`.** Every stream, poll, and download of the session stops, and
every live table of the session, with everything derived from it, **fails** with
`MassiveSession #1 was closed; this table no longer updates`, so no table looks live while it
is not. After `close()`, verbs and the diagnostics tables raise `MassiveError`; building queries
still works.
A failed table keeps its rows, but Deephaven builds nothing new on it, so take a static copy:

```python
trades = m.stocks.trades("AAPL").append_only()
# ... later ...
m.close()
kept = trades.snapshot()
```

A script that only reads static tables can let a `with` block close the session:
`with dm.MassiveSession() as m:`. A session nothing references any more is closed on its own,
with an INFO line in the log.

**A long feed outage fails live tables.** When the connection to Massive drops, the library
reconnects on its own. Meanwhile each live table keeps its last rows, and `m.streams()` notes
the outage in `Error`. A table still without data `max_outage` seconds after the drop (300 by
default) **fails**, since the rows sent meanwhile are missing. A `poll()` table fails the same
way once its cycles have failed, or a late cycle has received nothing, for that long. Call the
verb again for a new table; `max_outage=0` fails at the first drop:

```python
m = dm.MassiveSession(dm.MassiveConfig(max_outage=0))
```

A connected feed can also go quiet: when nothing arrives for 5 minutes during the market's
regular hours, `Error` says so, but the table keeps running, since a quiet symbol or a market
holiday looks the same.

**The console holds the lock live tables need.** Deephaven runs each console command while
holding the lock every live table needs to update, so a `history()` or `fetch()` that downloads
for ten minutes pauses every live table on the server for ten minutes; the library logs a
WARNING when this happens. Pass `wait=False` to a long read: it returns at once with an empty
live table of the right columns, and when the download finishes, the whole result appears in one
update and never changes again. `m.streams()` shows it as `LOADING`, then `DONE` or `FAILED`.
Until then the table is empty, so take `year.snapshot()` or convert it only after it has filled.
A background read also waits out up to ten minutes of rate-limit pauses, while a read inside a
console command gives up after 90 seconds of them. Releasing the table, or `close()`, stops the
download.

```python
m = dm.MassiveSession()
year = m.stocks.trades(["AAPL", "MSFT"]).history("2025-01-02", "2025-09-30", wait=False)
```

**Never call the library from a table listener.** A listener runs on Deephaven's update thread,
and every verb refuses that thread with `ValueError`. A symbol table does the same job without a
listener: see [`symbols`](#symbols).

**Filter one live table by another with a join, not `where_in`.** Between two of the session's
live tables, `where_in` and `where_not_in` can crash the server when both fail in one update
cycle, as they do when the session closes. Use a join instead:

```python
watch = m.stocks.movers(dm.Direction.GAINERS).poll(60)
quotes = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).last_by()
marks = watch.select_distinct("Symbol").update("Watched = true")
watched = quotes.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")
```

<a id="plans-and-errors"></a>

### Plans and errors

Massive sells one subscription per asset class: Stocks, Options, Indices, Currencies (forex and
crypto together), and Futures, each from the free Basic plan up to paid plans. One account can
hold any mix and upgrade mid-month. Reference data, economic data, and news come with every
account; partner datasets are sold separately. The library does not model plans: it reads what
you ask for, and when something is out of reach, it says so. Two settings tell it what it cannot
learn on its own: `rest_basic_subscriptions` and `delayed_subscriptions` (see
[install](#install)).

What a plan changes, in the library's terms:

- **Transports.** Basic plans read REST only. Paid plans add WebSocket streams, and flat files
  with S3 keys.
- **Pace.** A Basic subscription allows five REST requests a minute; the library spaces its
  requests to fit, so a large download on Basic is slow but completes.
- **Freshness.** Massive's stock, option, index, and futures plans range from end-of-day data
  on Basic, through delayed, to real time (see [real-time or delayed](#real-time-or-delayed)).
- **Kinds of data and history.** Higher plans add more kinds of data, such as streamed trades and
  quotes, and reach further back: Stocks Basic keeps two years of history, while Indices Basic
  keeps all of it. Each dataset's page in the [reference](docs/datasets/README.md) has Massive's
  plan table: which plans include it, how fresh, and how far back.

Every refusal is a `dm.EntitlementError`, and a rejected key a `dm.AuthenticationError`, both
kinds of `dm.MassiveError`. A static verb raises it. A live table fails with it: the web UI
shows the error, `m.streams()` shows `FAILED` with the message, and only that table stops. Each
message names what was refused, Massive's own words, and, where the library has one, another
way to get the data. Each error carries `status` (the HTTP status, such as 403 or 429),
`request_id` (Massive's id, which its support can look up), and `cause`:
`dm.ErrorCause.EXPECTED` (a refusal, a limit, a network failure, a closed session),
`USER_DEFECT` (your code or input; the message names the fix), or `LIBRARY_DEFECT` (a bug;
please report it).

```python
try:
    old = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2004-01-05", "2004-01-30")
except dm.EntitlementError as error:
    print(f"not on this plan ({error.status}, request {error.request_id}): {error}")
```

A mistake in the call itself, such as a symbol not in Massive's form (`aapl`, `EUR/USD`), text
with stray spaces, or text where an enum belongs, raises `ValueError` or `TypeError` before
anything is sent.

<a id="real-time-or-delayed"></a>

### Real-time or delayed

Massive streams stocks, options, indices, and futures from two feeds: a real-time one, and a
delayed one that runs 15 minutes behind (10 for futures). Starter and Developer plans are
delayed; Advanced and Business plans are real-time. You choose the feed for each subscription
with `MassiveConfig(delayed_subscriptions=...)`, and the library never chooses for you: a
stream never moves from one feed to the other.

- **Not listed, the default: real-time.** When Massive shows that your plan's data is delayed,
  the stream verb raises `dm.EntitlementError` with what Massive reported and the exact line
  to add. A `fetch()` or `poll()` of current data (snapshots, last trades and quotes, movers,
  the option chain) fails the same way when Massive labels its answer `DELAYED`, naming the
  tickers, so you never get delayed prices you did not ask for. In
  [`m.snapshots()`](docs/datasets/snapshots.md), which mixes classes, each row is held to its
  own class's subscription.
- **Listed: delayed.** The subscription's streams read the delayed feed, even when your plan is
  real-time. REST cannot ask for delayed data, so `fetch()` and
  `poll()` return whatever your plan serves, and the table's description says so.
- Forex and crypto are real-time on every plan, so `dm.Subscription.CURRENCIES` cannot be
  listed; fair-market-value streams (Business plans) are real-time only.
- Past data is never checked, so this setting does not affect it: `history()`, and the
  `fetch()` and `poll()` of the previous day's bar, a day's open and close, dividends, splits,
  and the other datasets that are not the market's current state. Massive labels some of them
  `DELAYED` on Basic and Starter plans, but their rows are final. An indicator is past data
  over a past range, but its latest value comes from the latest bars: its `poll()`, and a
  `fetch()` with no `end` or an `end` of today or later, are checked like current data.
- `m.streams()` shows each live table's `Recency`, and each live table's description, shown in
  the web UI, says what it knows of it. Nothing is shown as real-time without proof: a stream
  is real-time by its feed, and a `poll()` table only once Massive labels its data real-time;
  until then it shows `UNKNOWN`.

```python
# The default: every subscription real-time. On a delayed plan, a stream raises an error
# that names the setting to add.
real_time = dm.MassiveSession()

# Stocks in real time; options from Massive's delayed feed, 15 minutes behind.
mixed = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS}))
```

<a id="settings-file"></a>

### Settings file

Settings you use everywhere can live in one YAML file instead of in each script. The file sets
any of `dm.MassiveConfig`'s settings by name; a setting it leaves out keeps its default.

```yaml
# ~/massive/settings.yaml: settings for deephaven-massive.
# The API key and the S3 keys never go here: set MASSIVE_API_KEY (and, for flat files,
# MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY) in the environment.

# Subscriptions whose live data you accept from Massive's delayed feed (Starter, Developer).
delayed_subscriptions: [options]

# Subscriptions you hold on Massive's free Basic plan: five REST requests a minute.
rest_basic_subscriptions: [stocks]

# Seconds a live table rides out a dropped connection before it stops.
max_outage: 600

# How many REST requests or flat-file reads run at once.
max_concurrency: 4

# A row of a symbol table that is not Massive's exact ticker: raise, log, or ignore.
noncanonical_symbols: raise

# A crypto pair of a symbol table that Massive's crypto ticker list does not hold, in a live
# stream: raise, log, or ignore.
unlisted_symbols: log

# A symbol a poll cannot read from Massive (no data, or an argument its endpoint does not
# take): log (leave it out and keep polling), ignore (the same, quietly), or raise (stop).
unavailable_symbols: log

# Pin the WebSocket host of an asset class; the library chooses when you leave this out.
ws_hosts:
  options: delayed.massive.com
```

Read it with `dm.MassiveConfig.from_file`. Code wins over the file, and the file wins over the
defaults, so override one setting with `dataclasses.replace`. There are three ways to name the
file:

- **A notebook or script** passes the path:
  `dm.MassiveConfig.from_file("~/massive/settings.yaml")`.
- **The Deephaven console** reads the path from `DEEPHAVEN_MASSIVE_CONFIG_FILE`, set in the shell
  that starts the server, before you start it:
  `export DEEPHAVEN_MASSIVE_CONFIG_FILE=~/massive/settings.yaml`. Then
  `dm.MassiveConfig.from_file()` needs no path.
- **Docker** mounts the file read-only and sets the same variable to the path inside the
  container: `-v "$HOME/massive/settings.yaml:/massive/settings.yaml:ro"` and
  `-e DEEPHAVEN_MASSIVE_CONFIG_FILE=/massive/settings.yaml` on the `docker run` line.

```python
import dataclasses

# A notebook or script names the file.
config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
# A console or container started with DEEPHAVEN_MASSIVE_CONFIG_FILE needs no path.
config = dm.MassiveConfig.from_file()
# Code overrides the file.
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=2))
```

Nothing reads the file unless you call `from_file`: `dm.MassiveConfig()` and
`dm.MassiveSession()` use only the defaults and the environment's keys. The file is read
strictly, so a typo fails at once instead of being ignored. Every value is text until its
setting converts it, so a ticker such as `ON` or `NO` stays a ticker. Words are taken exactly
as shown (`raise`, not `Raise`), an unknown setting names the closest one, and a setting with
no value after its colon is refused: leave it out to get its default. Every error names the
file, the line, and the setting, except an error about an environment variable, which names the
variable. YAML's tags (`!!python/...`), anchors and aliases
(`&name`, `*name`), repeated keys, and several documents in one file are refused. The log reads
the file's name and the settings it set at INFO.

<a id="diagnostics"></a>

### Diagnostics

Four live tables show what the library is doing. Start with `m.streams()` when a table looks
wrong.

- **`m.streams()`**: one row per stream, poll, and background download. `State` is `RUNNING`,
  `RELEASED` (you hold no table from it, but a derived table or an open panel still uses it),
  `FAILED`, `CLOSED`, or for a download `LOADING` and `DONE`. `Error` says why it stopped, or
  warns without stopping it, joined with `;`: a watchlist row in the wrong spelling; options
  contracts waiting for room (`over capacity: <n>/1000; <k> deferred`); a subscription Massive
  has not confirmed (`no acknowledgement from Massive for ...`; the library re-sends it); a
  `dm.ALL` stream's symbols whose rows are being dropped; a connection that is down or quiet;
  a poll's symbols left out of its latest cycle (`no rows this cycle for ...`: Massive has no
  data for them, or their endpoint does not take an argument you passed). `Gaps` keeps every
  span over which a live table's rows may be missing (an outage it recovered from, a run of
  failed poll cycles), after the `Error` note has cleared. `Query` shows the call that opened
  it, `Recency` whether its data is real-time (`REALTIME`), delayed (`DELAYED`), or not yet
  shown to be either (`UNKNOWN`), `RowsPublished` counts rows delivered, and `SchemaDrift`
  lists fields Massive added (`WebSocket:zz+`), stopped sending (`REST:size-`), sent with a
  value that does not fit (`REST:exchange?`), or sent as a whole number too large for its
  decimal column, which holds it rounded (`WebSocket:s~`), and counts the rows dropped because
  their symbol or time cannot be read (`REST:3 rows dropped`). 100 such streamed rows in a
  row, with none delivered, fail the streams, since Massive has most likely changed its format.
  A `history()` or `fetch()` that follows a live symbol table has a row too: `LOADING` while a
  symbol is still being read, then `RUNNING`, with `Loading` counting the symbols left.
- **`m.connections()`**: one row per WebSocket connection, with its `Host`, `Recency`
  (`REALTIME`, `DELAYED`, `UNKNOWN`), `State` (`CONNECTED`, `RECONNECTING`, `WAITING_SLOT` when
  another program uses your key's connection, `FAILED`), subscriptions confirmed and pending,
  and `LastError`; and one row per decision about which server to stream from.
- **`m.rate_limits()`**: one row per REST subscription, plus `other` for reference data. `State`
  is `READY`, `PACED` (five a minute), or `PAUSED` (Massive refused a request for coming too
  fast, until `PausedUntil`). Programs elsewhere that use your key count against the same limit
  without appearing here.
- **`m.symbol_reads()`**: one row per symbol of each `history()` or `fetch()` that follows a live
  symbol table. `State` is `QUEUED`, `LOADING`, `RETRYING` (a passing failure, read again at
  `NextAttemptAt`), `DONE`, `FAILED` (`Error` says why), `REMOVED`, `STOPPED` (you released the
  table), or `CLOSED`. `ReadAt` is when the rows you see were downloaded, and `Rows` counts them.
  The symbols that failed: ``m.symbol_reads().where("State == `FAILED`")``.

```python
streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
```

<a id="arguments"></a>

## Arguments

Each argument below means the same on every dataset that takes it. A dataset's `help()` gives
only what differs for it, and its [reference page](docs/datasets/README.md) lists the values its
`sort` and `filters` accept.

<a id="symbols"></a>

### `symbols`

The first argument of most datasets. It takes:

| Form | Example | Meaning |
|---|---|---|
| one ticker | `"AAPL"` | that instrument |
| a list | `["AAPL", "MSFT"]` | those instruments |
| a table with a `Symbol` column | `watchlist` | its rows; a live table is followed as it changes |
| `dm.ALL` | `dm.ALL` | every instrument, where the dataset can serve it |
| `None` | (left out) | on list datasets only: no ticker filter |

Every ticker must be Massive's exact spelling: `AAPL`, `BRK.B`, `O:SPY271217C00700000`,
`I:SPX`, `C:EURUSD`, `X:BTCUSD`, `ESZ6`. A wrong one in a string or list raises `ValueError` at
once. To accept other spellings, convert first: `dm.canonical_symbol_forex("EUR/USD")` returns
`"C:EURUSD"`, `dm.canonical_symbol_stock_yahoo("BRK-B")` returns `"BRK.B"`, and
`dm.canonicalize_symbols(table, dm.AssetClass.FOREX)` converts a whole table as rows arrive.
The converters also work inside query strings; `help(dm.canonical_symbol)` lists them.

**A live table is followed.** Add a row and a stream subscribes to it; delete one and it
unsubscribes. `poll()` reads the table at every cycle. `fetch()` and `history()` return a live
table: an added symbol's rows, over the same range, appear whole once they are read, and a
deleted symbol's rows leave at once. The range is fixed when you call: an open `end` means
through that moment. A static table, such as one from `new_table` or `snapshot()`, is a fixed
list, read once. An input table makes
a watchlist you can edit in code or by typing in the web UI:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
board = m.stocks.quotes(watchlist).last_by()

watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))
watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
```

Delete rows by passing a static table of the keys, as above: a live view of the watchlist
itself can hang the server. Any live table with a `Symbol` column is a watchlist, so one dataset
can choose another's symbols. This streams the trades of the day's top gainers, re-checked every
minute, and the trades of every E-mini S&P 500 futures contract:

```python
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
hot = m.stocks.trades(gainers).last_by()

contracts = m.futures.contracts(product_code="ES").fetch()
es = m.futures.trades(contracts).last_by()
```

**Stream a watchlist as one table.** Put many symbols in one stream rather than opening one
stream per symbol. Each stream costs the server about 0.2 ms at every flush that brings it rows,
whatever its size, and flushes come 20 times a second: 300 single-symbol streams of busy symbols
need about 60 ms a flush, more than the 50 ms between flushes, so every table falls behind. One
stream of the same 300 symbols pays that cost once. To see one symbol, filter the shared table:

```python
board = m.stocks.quotes(watchlist).last_by()
aapl = board.where("Symbol == `AAPL`")
```

A `history()` or `fetch()` that follows a watchlist reads each symbol on its own, so one bad
symbol never stops the table: Massive's "no data" gives that symbol no rows, a passing failure
(a rate limit, a dropped connection) is retried after 1, 5, and 15 minutes, and a refusal marks
that symbol failed in `m.symbol_reads()`; delete and re-add its row to read it again. Only a
rejected API key, a closed session, a failed watchlist, or a library bug fails the whole table.
With `wait=True`, the default, the call returns once the watchlist's current symbols are read,
and raises only if every one of them failed. A symbol that comes back after you deleted it is
downloaded again, unless you choose otherwise (`reread`, in
[verb arguments](#verb-arguments)). `limit` then caps each symbol's rows. The same holds for
`underlying=` of `m.options.chain` and `m.options.contracts`, which take a table with a
`Symbol` column, and for futures `product_code=`, which takes one with a `ProductCode` column.

A large live result costs memory and time downstream: every table you derive from it updates
when a symbol lands or leaves, and sorting tens of millions of rows takes seconds each time.
Derive from `snapshot()` of it when you need a fixed copy.

In a live table, `last_by()` by `Symbol` (the default) drops the row of a symbol that leaves
the watchlist, and shows nothing for a symbol added back until it trades again; with keys that
span symbols, such as `last_by("Exchange")`, a group keeps its row after the symbol behind it
leaves. `append_only()` and `ring()` keep the rows a removed symbol already produced. A
watchlist row that is not Massive's spelling is skipped, logged once, and listed in the
stream's `Error` in `m.streams()`; fix the row and it subscribes. A watchlist whose every row is
bad is refused. To fail instead, set `dm.MassiveConfig(noncanonical_symbols=dm.OnError.RAISE)`.
A crypto pair that Massive's list of crypto tickers does not hold, in the watchlist at the call
or added later, is skipped and listed the same way, and the stream keeps its other pairs; a
skipped pair stays skipped until its row is deleted and added again. If the list cannot be
read when a pair is added, it is tried twice more over about ten seconds first, and the `Error`
says so. To fail instead, set `dm.MassiveConfig(unlisted_symbols=dm.OnError.RAISE)`. A
futures contract a watchlist holds subscribes once its trading schedule has loaded (up to 15
seconds), so its first rows carry a `Date`.

The options connection carries at most 1,000 topics, counted across every session that shares
it, and each stream of a contract is one (its trades and its quotes are two). A list or static
table of contracts over that cap raises `ValueError`; a live watchlist that grows past it defers
the extra contracts, counted in `m.streams()`'s `Deferred` column, until room frees up.

A schedule is a watchlist too: a symbol table that is empty outside the hours you care about
stops the stream, and makes a poll send no requests, outside them. This one is live only during
New York's regular session (570 and 960 are 09:30 and 16:00 as minutes of the day; weekdays only,
no holidays):

```python
from deephaven import time_table

symbols = new_table([string_col("Symbol", ["AAPL", "MSFT", "NVDA"])])
session_open = (
    time_table("PT1m")
    .tail(1)
    .update_view(
        [
            "Minute = minuteOfDay(Timestamp, timeZone(`America/New_York`), false)",
            "Day = dayOfWeekValue(Timestamp, timeZone(`America/New_York`))",
        ]
    )
    .where(["Day <= 5", "Minute >= 570", "Minute < 960"])
)
scheduled = symbols.join(session_open, on=[], joins=[]).view(["Symbol"])
trades = m.stocks.trades(scheduled).last_by()
```

<a id="start-and-end"></a>

### `start` and `end`

`history(start, end)` and every list dataset with a time axis (dividends by ex-date, news by
publication time, and so on; each dataset's reference page names its axis) take a range. Both
bounds are one kind, never mixed:

- **Dates** are whole days in the market's time zone, both ends included:
  `history("2025-06-02", "2025-06-02")` is all of June 2. Text such as `"2025-06-02"`, a Python
  `date`, or a Deephaven `LocalDate`.
- **Instants** are exact times, the start included and the end excluded. A timezone-aware Python
  `datetime`, a `pandas.Timestamp` or `numpy.datetime64`, Deephaven's `Instant` or
  `ZonedDateTime` (such as `dh_now()`), or Deephaven time text such as `"2025-06-02T09:30 ET"`.
  A naive `datetime` is refused.

Values taken from another table work directly. This finds the busiest minute of a day and reads
every trade inside it:

```python
minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
for row in minutes.sort_descending("Volume").head(1).iter_dict():
    inside = m.stocks.trades("AAPL").history(row["TimestampStart"], row["TimestampEnd"])
```

An open `end` reads through now in `history()`: through today, in the market's time zone, for
dates (the current session for futures). On a list dataset an open bound is not sent, so
`m.stocks.dividends("AAPL", start="2025-01-01")` includes announced dividends still to come. A
calendar-date axis, such as an ex-dividend date, takes dates only.

<a id="as-of"></a>

### `as_of`

A dataset that describes something as it stood on a past day, such as ticker details, option
and futures contracts, or futures products, takes `as_of`, a date. Left out, it means today:
`m.reference.ticker_details("AAPL", as_of="2020-01-02").fetch()`.

<a id="sort-order-limit"></a>

### `sort`, `order`, and `limit`

List datasets take `sort`, the field to order by, as Massive names it (such as
`"ex_dividend_date"`), and `order`, `dm.Order.ASC` or `dm.Order.DESC`. Each dataset accepts only
the fields its endpoint documents, listed on its reference page; a dataset whose endpoint cannot
sort has neither argument.

`limit` caps the rows of each symbol you name. Each named symbol gets its own request with its
own cap, so `m.stocks.dividends(["AAPL", "MSFT", "KO"], limit=4)` returns up to four dividends
of each, and a symbol with many rows never crowds out the others. The rows come symbol by
symbol, with `sort` and `order` applied within each. Without named symbols (`symbols` left out,
or `dm.ALL`), `limit` caps the total rows. Where one row lists several tickers (news, financial
statements, insider filings), a row two of your tickers share appears once. One request per
symbol counts against a Basic plan's five requests a minute, so a long list under a `limit`
takes longer there. On `options.contracts` with explicit symbols, each contract is one row, so
`limit` has no effect. `limit` keeps the first rows in Massive's order, so ask for newest first
when you want the latest:
`m.reference.news("AAPL", sort="published_utc", order=dm.Order.DESC, limit=50).poll(60)`. A news
or filings poll needs a `start` or a `limit`: without either, every cycle would re-read the whole
archive.

<a id="filters"></a>

### `filters`

Any further parameter Massive documents for a list dataset goes in `filters`, a mapping from the
parameter's name, with its dots written as underscores, to a value:
`m.options.contracts(underlying="SPY", filters={"strike_price_gte": 600})`. Each dataset's
reference page lists the keys it accepts. A key the endpoint does not take, or one that has its
own argument (`start`, `symbols`, `adjusted`), is refused with `ValueError` naming the valid keys.

<a id="adjusted"></a>

### `adjusted`

`True`, the default, asks for split-adjusted prices on stock and option bars and daily
summaries; `False` for prices as traded. Only REST serves adjusted bars, and live bars are as
traded. See [split adjustment](#split-adjustment).

<a id="timespan-and-multiplier"></a>

### `timespan` and `multiplier`

Every `bars()` call names its bar length: `timespan`, a `dm.Timespan` (`SECOND`, `MINUTE`,
`HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`; futures take `SESSION` in place of `DAY`),
required, and
`multiplier`, the number of units per bar (default 1). One-second and one-minute bars with
`multiplier=1` also stream; every other length is history only. Text is refused: write
`dm.Timespan.DAY`, or convert with `dm.Timespan("day")`.

<a id="venues"></a>

### `venues`

Futures trades, quotes, and bars take `venues`: the exchanges whose flat files `history()`
reads, all four by default. Massive sells futures flat files per exchange, so on such a plan
pass the ones you hold:
`m.futures.trades("ESZ6", venues={dm.FuturesVenue.CME}).history("2026-10-01", "2026-10-02")`.
The members are `CME`, `CBOT`, `NYMEX`, and `COMEX`. Streams and REST reads ignore it.

<a id="verb-arguments"></a>

### Verb arguments

- `poll(interval, *, mode=dm.PollMode.AUTO)`: `interval` in seconds or a `timedelta`; `mode`
  as in [table shapes](#table-shapes).
- `ring(capacity)`: the number of rows to keep.
- `last_by(by="Symbol")`: the key column or columns.
- `history(start, end=None, *, source=dm.HistorySource.AUTO, reread=dm.Reread.ALWAYS,
  wait=True)`: the range as in [`start` and `end`](#start-and-end); `source` as in
  [history sources](#history-sources); `wait=False` reads in the background, as in
  [live tables](#live-tables). `reread` matters only when `symbols` is a live table: it says
  whether a symbol that returns after you deleted it is downloaded again. `dm.Reread.ALWAYS`,
  the default, downloads it again and shows Massive's current answer; it has no rows until the
  download lands. `dm.Reread.NEVER` gives back the earlier download with no request, at the
  cost of keeping every symbol the table ever held in memory. A symbol that failed is always
  read again. Massive corrects past data (late cancellations, backfills, splits), so no rule can
  tell when an earlier download is still right: choose `NEVER` only when stale rows are
  acceptable. Each symbol's rows are as of its `ReadAt` in `m.symbol_reads()`.
- `fetch(*, wait=True)`: `wait` as for `history()`. A `fetch()` that follows a live symbol
  table reads a returning symbol again, as Massive answers at that moment.

<a id="troubleshooting"></a>

## Troubleshooting

Find the message you see, then the fix. When nothing fits, turn on the debug log (the end of
this section) and look at what the library did.

| Message or symptom | Cause and fix |
|---|---|
| `no Massive API key` | The server cannot see `MASSIVE_API_KEY`. Export it in the shell that starts the server, then restart the server. Don't type the key into the console. |
| `MassiveSession must be created inside a Deephaven worker` | The code ran in a plain Python process. Run it in the Deephaven console or a script the server runs. |
| `timespan must be dm.Timespan` | Arguments that pick from a set take an enum member: `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` naming a symbol | Not Massive's spelling. See [`symbols`](#symbols) for the converters. |
| `AttributeError` naming a verb | The dataset cannot serve that verb; the message names the one that can. |
| `... would be delayed (Massive reports ...)` | Your plan's data for that subscription is delayed, and the library serves real-time data unless you ask for delayed. Add the line the message shows, or upgrade the plan. See [real-time or delayed](#real-time-or-delayed). |
| `... refused the subscription to ...` | Your plan does not stream this data: it may include other streams of the class, or read the class over REST only. Stream bars instead of trades, or read `history()` or `poll()` a REST dataset your plan includes; each dataset's [reference page](docs/datasets/README.md) has the plan table. When the message suggests `delayed_subscriptions`, your plan may be delayed: see [real-time or delayed](#real-time-or-delayed). |
| `delayed_subscriptions cannot hold dm.Subscription.CURRENCIES` | Forex and crypto have no delayed feed. Remove it from the set. |
| `no settings file: pass its path ...` | `dm.MassiveConfig.from_file()` was called without a path, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` is not set where the server runs. Pass the path, or set the variable before you start the server. See [settings file](#settings-file). |
| `settings file ... does not exist` | The path is wrong, or, in Docker, the file is not mounted at the path the variable names. Check the `-v` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE` options. |
| `Is a directory` from `from_file()` | The settings file path is a folder: Docker created it because the file did not exist on the host. Create the file on the host first. |
| `settings file ..., line ..., column ...: ...` | The file holds something the library refuses: an unknown setting (the message suggests the closest), a word in the wrong case, a credential, or a YAML feature such as a tag or anchor. Fix that line. |
| `AuthenticationError` on a live table, ending `can also mean the ... plan includes no WebSocket streams` | Massive's live connection gives one answer for a wrong key and, possibly, for a plan without streams of that asset class. If `fetch()` or `history()` works with the same key, the key is right: read the data with `history()` or `poll()`, or check which streams your plan includes on its [reference page](docs/datasets/README.md). |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan does not include this dataset; the message names another where one exists. |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history window. Ask for a later start. |
| `the plan does not include these flat files` | Pass `source=dm.HistorySource.REST`, or leave `source` at `AUTO`, which reads REST when REST can serve the read. |
| `the flat file for <date> is not published yet` | Massive usually publishes by about 11:00 ET the next day, but the time varies. Retry later, or read with `source=dm.HistorySource.REST`. |
| `... poll() needs a start or a limit` | A news or filings poll would re-read the whole archive each cycle. Give the query a `start` or a `limit`. |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed. Take `table.snapshot()` to keep the rows; create a new session for new tables. |
| `Can not listen to failed table` | Deephaven builds nothing on a failed table. Work on `table.snapshot()`. |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table. Pass `wait=False` to long reads. |
| `... which is not a stamp Massive is known to use` | Massive stamped a bar unexpectedly; it is served at its own stamp with no `Date`. See [bars](#bars), and please report it. |
| a WARNING that a read leaves some columns empty | Pass the `source=` it names. See [history sources](#history-sources). |
| `WAITING_SLOT` in `m.connections()` | Another program or computer uses your key's one connection for this class. Stop it; the library connects when the slot frees. |
| `connection down since <time>; no data`, `cycle running since <time>`, or `last good cycle at <time>` in `m.streams()` | The connection dropped, a poll cycle is running late (often paced by rate limits: see `m.rate_limits()`), or a poll cycle failed with the error before the note. The library reconnects and retries on its own; the note clears once data flows, and `Gaps` keeps the span whose rows may be missing. |
| `no rows this cycle for ...` in `m.streams()` | A poll left those symbols out: Massive answered that it has no data for them (`HTTP 404`), or their endpoint does not take an argument you passed (such as `adjusted=False` for a crypto pair). The other symbols keep polling; fix or remove the symbols named. `dm.MassiveConfig(unavailable_symbols=dm.OnError.RAISE)` stops the table instead, and `IGNORE` leaves them out without a WARNING. |
| `... longer than MassiveConfig.max_outage`, from a stream or from `... poll(): ...` | The outage, or the failing or silent poll cycles, outlasted `max_outage`, so the table stopped rather than show stale data. Fix any error it names, then call the verb again; raise `max_outage` to wait longer. See [live table lifetime](#live-tables). |
| `no acknowledgement from Massive for ...` in `m.streams()` | Massive has not confirmed the subscription; the library re-sends it until it does. If the note stays, the rows of those symbols may be missing: report it. |
| `over capacity: <n>/1000; <k> deferred` in `m.streams()` | The options connection is full; the extra contracts subscribe when others leave. Watch fewer contracts. See [`symbols`](#symbols). |
| `rows for ... are being dropped` in `m.streams()`, or `... the last 100 rows of channel ... were dropped` | Massive sent rows the library cannot read: a `dm.ALL` stream warns and keeps running; any other stream fails after 100 such rows in a row. Massive has most likely changed its format: report it with the log. |
| `rate limited: ...; this read ran inside a Deephaven console command` | A Basic subscription's pace held a console read for more than 90 seconds. Pass `wait=False`, which waits up to ten minutes, and list your Basic subscriptions in `rest_basic_subscriptions` so the library paces from the start. |
| `S3 rejected the flat-file credentials` | The S3 keys are wrong. Set `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY` from Massive's dashboard and restart the server. |
| `no data since <time> during ... trading hours` in `m.streams()` | The connection is up but nothing arrived for 5 minutes while the market is open. A holiday or quiet symbols explain it; the table keeps running. |
| a live table stays empty | The market is closed (`m.reference.market_status().fetch()`); a console command is still running; the table reads Massive's delayed feed (`Recency` `DELAYED` in `m.streams()`), which starts 15 minutes behind; or the stream failed (`m.streams()`). |
| a download is slow | Basic plans allow five requests a minute (`m.rate_limits()` shows the pace). Use flat files for many symbols, and `wait=False` for long reads. |
| memory keeps growing | `append_only()` keeps every row. Use `ring(n)`, `last_by()`, or `blink()` with an aggregation. |
| a table keeps running after `del` | Call `m.release(table)`. |
| prices jump at a split | Live bars are as traded; adjusted history is restated. See [split adjustment](#split-adjustment). |
| a symbol returns no rows | No data in that range, or the ticker changed: `m.reference.ticker_events("META").fetch()` lists past names. |

**The log.** The library writes to Deephaven's log, which the web UI shows in its **Log** panel.
INFO reports a decision or progress; nothing to do. WARNING means a live table may be stale or
wrong, or something needs you; the message says which, once when it starts, and an INFO line
says when it recovers. ERROR means a defect: in your code or input (the message names the fix),
or in the library (the message ends "This is a bug in deephaven-massive"). To see everything the
library does:

```python
dm.enable_debug_logging()  # to the Log panel; pass a file path to write a file instead
# ... run the code that fails ...
dm.disable_debug_logging()
```

To capture a problem from the moment the server starts, set `DEEPHAVEN_MASSIVE_LOG=DEBUG`, or
`DEEPHAVEN_MASSIVE_LOG_FILE=/path/to/massive.log` to write the debug log to a file. The log never
holds your API key or S3 secret.

The debug log grows with the number of requests, not with the amount of market data. The
library logs nothing per WebSocket message or row, so a live table costs about 1 KB a minute
for each connection (about 60 KB an hour), however busy the market, plus one line of about
150 bytes per symbol each time a connection subscribes. Every REST request adds about 0.5 KB
and every flat file read 0.5 to 1 KB, so a `fetch()` or `history()` adds that much per page
or file it reads, and a `poll()` adds 0.5 KB per request and 0.2 KB per cycle: `poll(60)` of
one request is about 40 KB an hour, while a `poll(5)` that sends 100 requests a cycle is about
35 MB an hour. The log has no size limit and is never rotated, and a log file is appended to,
not replaced, each time the log is turned on. To keep it small, turn it on just before the
failing code and off after it, use `DEEPHAVEN_MASSIVE_LOG=INFO` for a long run (it drops the
per-request lines), and delete the file between runs.

To report a bug, open an
[issue](https://github.com/chipkent/deephaven-massive/issues), attach the log file (GitHub cuts
an issue's text at 65,536 characters; zip a larger file), and add the output of `m.streams()`,
`m.connections()`, and `m.rate_limits()`, printed after the failure with
`to_string(num_rows=1000)`.

<a id="status"></a>

## Status

Every dataset in the [reference](docs/datasets/README.md) is implemented: live streams, polled
REST tables, and history from REST or the flat files. Each is tested against simulated Massive
servers that answer as Massive's documentation says Massive does, and the live tests have run
against Massive on a mix of paid and free plans. Some questions remain open because those plans
cannot settle them, such as how index daily bars and futures sessions are stamped and parts of
the flat files; on those, the library follows its best reading of Massive's documentation and
warns when what Massive sends differs. Following a live symbol table in `history()` and
`fetch()`, with `reread` and `m.symbol_reads()`, is specified but not built yet: until it is,
both read a live table once, when called. The library is not on PyPI yet. Outside testers start
with the [tester guide](docs/tester-guide.md).

<a id="contributing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up a development checkout, run the
checks, and propose a change. Licensed under the Apache License 2.0; see [LICENSE](LICENSE).
