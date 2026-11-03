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
[real-time or delayed](docs/guide/plans.md#real-time-or-delayed).

This README is the user guide's starting point, and [docs/guide](docs/guide) holds the
reference pages it links. The [dataset reference](docs/datasets/README.md) lists
every dataset with its arguments, its columns, and what each Massive plan includes, and each
dataset's page links Massive's own pages for that dataset.

<a id="install"></a>

## Install

The library runs on macOS and Linux, and on Windows through WSL2, on a computer with at least
8 GB of memory. It is not on PyPI, and while its repository is private you need access to it
(see the [tester guide's access step](docs/tester-guide.md#12-get-access-to-the-code)). Outside
testers get access and a version from the owner, and follow the
[tester guide](docs/tester-guide.md), which walks through every step. With access to the
repository, you need Python 3.11 or later, Java 17 or
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
[real-time or delayed](docs/guide/plans.md#real-time-or-delayed)). To keep settings in one file that every
notebook, console, and container reads, see [settings file](docs/guide/install.md#settings-file).

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

<a id="terms"></a>

## Terms

Words the rest of this guide uses without defining them again:

- **Console**: the **Console** panel of the Deephaven web IDE in your browser, where you type
  Python that runs inside the server. It is not a terminal.
- **Update cycle**: Deephaven applies changes to its live tables together, in cycles about once a
  second. A live table changes between cycles, never during one.
- **Blink, append-only, ring, and `last_by` tables**: the live tables the stream verbs make.
  [Verbs](#verbs) and [table shapes](#table-shapes) define them.
- **As-of join (`aj`)**: a Deephaven join that matches each row of one table with the row of
  another that has the latest time at or before its own, such as each trade with the quote in
  force when it happened.
- **Table operations**: `where`, `update`, `sort`, `agg_by`, the joins, and the rest are
  Deephaven's own; the library returns ordinary Deephaven tables. Deephaven's
  [table operations overview](https://deephaven.io/core/docs/conceptual/table-operations-overview/)
  lists them.
- **Subscription and plan**: Massive sells one *subscription* per asset class (Stocks, Options,
  Indices, Currencies, and Futures), each at a *plan* level: Basic (free), Starter, Developer,
  Advanced, or Business. Basic through Advanced are individual plans, for one person's own use;
  Business plans are for companies. Massive's [pricing page](https://massive.com/pricing) lists
  them, and [plans and errors](docs/guide/plans.md#plans-and-errors) says what each changes in the library.
- **Delayed data**: live data from Massive's delayed feed, which runs 15 minutes behind the
  market (10 for futures). Starter and Developer plans stream it; see
  [real-time or delayed](docs/guide/plans.md#real-time-or-delayed).

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
`O:SPY271217C00700000` is an OCC option symbol: underlying `SPY`, expiring `271217` (2027-12-17),
`C` for a call (`P` for a put), and a strike of 700, written as the strike times 1000 in eight
digits (`00700000`).

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
| follow a watchlist I can edit | pass a table with a `Symbol` column: see [`symbols`](docs/guide/arguments.md#symbols) |
| read the history of a watchlist I can edit | the recipes in [history of a watchlist](#history-of-a-watchlist) |
| backfill history, then continue live | the recipe in [backfill, then live](#backfill-then-live) |
| start a live board with values before the first trade | the recipe in [seed a live board](#seed-a-live-board) |
| read an option chain with its greeks | `m.options.chain("SPY").fetch()`: see [option chain](#option-chain) |
| follow the option chains of a watchlist | pass `m.options.chain` a table of underlyings: see [option chain](#option-chain) |
| read a long range without freezing the console | `history(..., wait=False)`: see [live tables](docs/guide/concepts.md#live-tables) |
| list a company's dividends | `m.stocks.dividends("AAPL", start="2025-01-01").fetch()` |
| follow news as it appears | `m.reference.news("AAPL", start=today).poll(300)`, with `today` as [first tables](#first-tables) sets it |
| stream the contracts of a futures product | `m.futures.trades(m.futures.contracts(product_code="ES", filters={"active": True}).fetch()).last_by()` |
| find a ticker by name | `m.reference.tickers(search="Nvidia").fetch()` |
| see what a column means | `query.columns()`, or hover over the column header |
| see what the library is doing | `m.streams()`, `m.connections()`, `m.rate_limits()`: see [diagnostics](docs/guide/troubleshooting.md#diagnostics) |
| stop a live table now | `m.release(table)`: see [live tables](docs/guide/concepts.md#live-tables) |
| read the library's log | `dm.enable_debug_logging()`: see [troubleshooting](#troubleshooting) |

<a id="recipes"></a>

## Recipes

<a id="backfill-then-live"></a>

### Backfill, then live

A table that starts with history and continues with the stream: today's minute bars so far,
then each new one as it closes. The function opens the stream before it reads history, so the
two overlap, and keeps one row per minute:

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
a split: see [split adjustment](docs/guide/history.md#split-adjustment).

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
(see [real-time or delayed](docs/guide/plans.md#real-time-or-delayed)).

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
[real-time or delayed](docs/guide/plans.md#real-time-or-delayed)).

`underlying` also takes a table of underlyings in a `Symbol` column, and reads each one's chain
as if you had passed it alone. A live table is followed, as a watchlist is (see
[`symbols`](docs/guide/arguments.md#symbols)): `fetch()` adds an underlying's chain once it is read and drops it when its
row leaves, and `poll()` reads the table at every cycle. `m.options.contracts(underlying=...)`
takes the same table:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

underlyings = input_table({"Symbol": dht.string}, key_cols="Symbol")
underlyings.add(new_table([string_col("Symbol", ["SPY", "QQQ"])]))
chains = m.options.chain(underlyings, contract_type=dm.ContractType.CALL).fetch()
```

A polled chain can choose what to stream. This re-reads SPY's nearest expirations every five
minutes, keeps the contracts within 2% of the underlying's price, and streams their quotes: as
the price moves, contracts that come near are subscribed and those that drift away are dropped
(see [`symbols`](docs/guide/arguments.md#symbols)):

```python
from deephaven.time import dh_today, to_j_time_zone

spy_chain = m.options.chain(
    "SPY",
    filters={"expiration_date_gte": dh_today(to_j_time_zone("America/New_York"))},
    sort="expiration_date",
    limit=200,
).poll(300)
near = spy_chain.where(
    "abs(DetailsStrikePrice - UnderlyingAssetPrice) <= 0.02 * UnderlyingAssetPrice"
)
near_quotes = m.options.quotes(near).last_by()
```

`UnderlyingAssetPrice` depends on your Stocks plan; where it is null, no contract passes the
filter. Keep the filter narrow: the options connection carries at most 1,000 topics, one per
contract's quotes, and a live watchlist's contracts past that wait, counted in the `Deferred`
column of `m.streams()`, until room frees up.

<a id="history-of-a-watchlist"></a>

### History of a watchlist

Pass a live watchlist to `history()` and the table follows it: each symbol you add is read over
the same range, and each one you delete leaves (see [`symbols`](docs/guide/arguments.md#symbols)). Two other ways fit
other needs. To read the watchlist's symbols once, as they stand now, pass `snapshot()` of it.
A followed `history()` served from flat files (see
[where history comes from](docs/guide/history.md#history-sources)) downloads every file of the range again
for each symbol you add, since each file holds the whole market. To follow it with no request
or download per symbol, read every symbol once and filter by the watchlist: adding or deleting
a symbol then costs nothing. That works where the dataset serves `dm.ALL`
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
unless they follow a live symbol table (see [`symbols`](docs/guide/arguments.md#symbols)). A
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

`poll(seconds)` re-reads the query every `seconds`, a number or a `timedelta` of at least 1
second: a shorter one, usually a typo such as `poll(0.01)` for `poll(10)`, raises `ValueError`.
Use a stream for anything faster. `poll()` combines its cycles by a `mode`. `dm.PollMode.REPLACE` keeps one row per key
(usually `Symbol`) and drops keys the latest cycle did not return. `UPSERT` keeps one row per
key and never drops one. `APPEND` adds rows it has not seen and never changes or removes one, so
the table grows into a feed. The default, `dm.PollMode.AUTO`, uses each dataset's own mode,
which its reference page names: `APPEND` for news and filings, `UPSERT` for IPOs, `REPLACE`
otherwise. Pass `mode=` to change what the table means; `APPEND` on a snapshot records every new
state of each symbol:

```python
path = m.stocks.snapshots(["AAPL", "MSFT"]).poll(30, mode=dm.PollMode.APPEND)
```

Every table names its columns the same way, and the library serves what it can when
Massive sends something odd: see [columns](docs/guide/concepts.md#columns).

**Time.** An instant, such as a trade's `Timestamp`, is an exact moment; a date is a
`yyyy-MM-dd` calendar day in the market's own time zone (New York for stocks, UTC for crypto):
see [dates, instants, and the market day](docs/guide/concepts.md#time).

**Bars.** A bar summarizes a window: open, high, low, close, volume, the volume-weighted
average price, and the trade count. Its window, its stamps, and which bars `history()`
returns: see [bars](docs/guide/history.md#bars).

**Where history comes from.** `history()` reads from REST or Massive's flat files and picks
one for you: see [history sources](docs/guide/history.md#history-sources).

**Split adjustment.** Stock and option bars are split-adjusted by default; live bars are as
traded: see [split adjustment](docs/guide/history.md#split-adjustment).

**Live table lifetime.** A live table runs while it is in use; `m.release(table)` stops one
and `m.close()` stops them all: see [live table lifetime](docs/guide/concepts.md#live-tables).

**Plans and errors.** The free Basic plan reads REST only, at five requests a minute; paid
plans add live streams and flat files. A request your plan does not cover raises
`dm.EntitlementError`: see [what your plan allows](docs/guide/plans.md#plans-and-errors).

**Real-time or delayed.** Live data is real-time unless you list the subscription in
`delayed_subscriptions`: see [real-time or delayed](docs/guide/plans.md#real-time-or-delayed).

**Settings file.** One YAML file can hold the settings every notebook, console, and
container reads: see [settings file](docs/guide/install.md#settings-file).

**Diagnostics.** `m.streams()`, `m.connections()`, `m.rate_limits()`, and
`m.symbol_reads()` show what the library is doing: see [diagnostics](docs/guide/troubleshooting.md#diagnostics).

<a id="arguments"></a>

## Arguments

Each argument means the same on every dataset that takes it; its full rules are in
[arguments](docs/guide/arguments.md).

<a id="symbols-and-watchlists"></a>

### Symbols and watchlists

`symbols`, the first argument of most datasets, takes one ticker, a list, a table with a
`Symbol` column, or `dm.ALL`, in Massive's exact spelling: see [`symbols`](docs/guide/arguments.md#symbols).

**A live table is followed.** Add a row and a stream subscribes to it; delete one and it
unsubscribes. `poll()` reads the table at every cycle. `fetch()` and `history()` return a live
table: an added symbol's rows appear whole once they are read, and a deleted symbol's rows leave
at once. `history()` reads every symbol over the same range, fixed when you call: an open `end`
means through that moment. `fetch()` has no range: each symbol's rows are Massive's answer when
that symbol was read, so a snapshot of a symbol added at 14:00 is the 14:00 snapshot. A static
table, such as one from `new_table` or `snapshot()`, is a fixed list, read once. An input table
makes a watchlist you can edit in code or by typing in the web UI:

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

In the web UI, open the `watchlist` panel. To add a symbol, click the empty row at the bottom,
type it, press **Enter**, and click **Commit**: rows you type are not added until you commit
them. To delete one, select its row, right-click, and choose **Delete Selected Rows**.

More on watchlists: [watchlists](docs/guide/watchlists.md).

<a id="troubleshooting"></a>

## Troubleshooting

Find the message you see, then the fix. When nothing fits, turn on the debug log
([the log](docs/guide/troubleshooting.md#the-log)) and look at what the library did.

| Message or symptom | Cause and fix |
|---|---|
| `no Massive API key` | The server cannot see `MASSIVE_API_KEY`. Export it in the shell that starts the server, then restart the server. Don't type the key into the console. |
| `MassiveSession must be created inside a Deephaven server` | The code ran in a plain Python process. Run it in the Deephaven console or a script the server runs. |
| `timespan must be dm.Timespan` | Arguments that pick from a set take an enum member: `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` naming a symbol | Not Massive's spelling. See [`symbols`](docs/guide/arguments.md#symbols) for the converters. |
| `AttributeError` naming a verb | The dataset cannot serve that verb; the message names the one that can. |
| `... would be delayed (Massive reports ...)` | Your plan's data for that subscription is delayed, and the library serves real-time data unless you ask for delayed. Add the line the message shows, or upgrade the plan. See [real-time or delayed](docs/guide/plans.md#real-time-or-delayed). |
| `... refused the subscription to ...` | Your plan does not stream this data: it may include other streams of the class, or read the class over REST only. Stream bars instead of trades, or read `history()` or `poll()` a REST dataset your plan includes; each dataset's [reference page](docs/datasets/README.md) has the plan table. When the message suggests `delayed_subscriptions`, your plan may be delayed: see [real-time or delayed](docs/guide/plans.md#real-time-or-delayed). |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan does not include this dataset; the message names another where one exists. |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history window. Ask for a later start. |
| a live table stays empty | The market is closed (`m.reference.market_status().fetch()`); a console command is still running; the table reads Massive's delayed feed (`Recency` `DELAYED` in `m.streams()`), which starts 15 minutes behind; or the stream failed (`m.streams()`). |
| a download is slow | Basic plans allow five requests a minute (`m.rate_limits()` shows the pace). Use flat files for many symbols, and `wait=False` for long reads. |
| memory keeps growing | `append_only()` keeps every row. Use `ring(n)`, `last_by()`, or `blink()` with an aggregation. |
| a table keeps running after `del` | Call `m.release(table)`. |

The full table, the diagnostics tables, the log, and how to report a bug are in
[troubleshooting](docs/guide/troubleshooting.md).

<a id="status"></a>

## Status

Every dataset in the [reference](docs/datasets/README.md) is implemented: live streams, polled REST
tables, and history from REST or the flat files. Each is tested against simulated Massive servers
that answer as Massive's documentation says Massive does, and the live tests have run against
Massive on a mix of paid and free plans. Some questions remain open because those plans cannot
settle them, such as how index daily bars and futures sessions are stamped and parts of the flat
files; on those, the library follows its best reading of Massive's documentation and warns when what
Massive sends differs. `history()` and `fetch()` follow a live symbol table, with
`m.symbol_reads()`, and `fetch()` follows a live table passed as `underlying=` or
`product_code=`. The library is not on PyPI yet. Outside testers start with the
[tester guide](docs/tester-guide.md).

<a id="contributing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up a development checkout, run the
checks, and propose a change. Licensed under the Apache License 2.0; see [LICENSE](LICENSE).
