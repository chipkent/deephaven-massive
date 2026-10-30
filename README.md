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
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2024-06-03", "2024-06-07")

# The latest trade of each symbol, updated as trades arrive. Needs a paid Stocks plan.
latest = m.stocks.trades(["AAPL", "MSFT"]).last_by()
```

This README is the whole user guide. The [dataset reference](docs/datasets/README.md) lists
every dataset with its arguments, its columns, and what each Massive plan includes.

<a id="install"></a>

## Install

You need Python 3.11 or later, Java 17 or later (check with `java -version`; any current JDK
works, such as one from [Adoptium](https://adoptium.net)), and a Massive account. The library
is not on PyPI yet, so it installs from GitHub:

```bash
python3 -m venv ~/dh-massive && source ~/dh-massive/bin/activate
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git"
export MASSIVE_API_KEY="your-api-key"    # from the keys page of Massive's dashboard
deephaven server --port 10000
```

Open <http://localhost:10000/ide> and use the **Console** panel; every example here runs there.
Set the key in the shell that starts the server, before you start it. Avoid typing the key into
the console itself: every user of a Deephaven server can read the console's variables.

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
minute, so the library paces its requests from the first one instead of learning the limit from
Massive's first refusal. `max_concurrency` (default 8) is how many requests or file reads run at
once.

```python
import deephaven_massive as dm

config = dm.MassiveConfig(rest_basic_subscriptions={dm.Subscription.STOCKS})
m = dm.MassiveSession(config)
```

**One session is enough.** Create one session at the top of your script and use it for
everything. Massive's individual plans allow one WebSocket connection per asset class at a time.
Sessions of one server that use the same key share their connections, so a second session does
no harm, but it buys nothing.

<a id="first-tables"></a>

## First tables

Run these in the console, one after another. Each line names a dataset, gives it symbols, and
calls a *verb* that makes the table:

```python
import deephaven_massive as dm
from deephaven.time import dh_today

m = dm.MassiveSession()

# History: Apple's daily bars for one week, a static table.
bars = m.stocks.bars("AAPL", dm.Timespan.DAY)
week = bars.history("2024-06-03", "2024-06-07")

# What each column of those bars means: Name, Type, Description, SuppliedBy.
columns = bars.columns()

# Live: the latest trade of each symbol (a paid Stocks plan streams trades).
latest = m.stocks.trades(["AAPL", "MSFT"]).last_by()

# Live on any plan: re-read today's Apple headlines every five minutes.
headlines = m.reference.news("AAPL", start=dh_today()).poll(300)
```

Click a table's name to open it. `week` has one row per trading day; `latest` changes as AAPL
and MSFT trade (during market hours); `headlines` grows as articles appear. Hover over a column
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
| `fetch()` | static: every row the request covers, read once | REST |
| `poll(seconds)` | live: the same request again every few seconds | REST |
| `blink()` | live: only the rows of the latest update cycle | WebSocket |
| `append_only()` | live: every row since the call | WebSocket |
| `ring(n)` | live: the latest `n` rows | WebSocket |
| `last_by()` | live: the latest row per symbol (or per the columns you name) | WebSocket |
| `history(start, end)` | static: past rows in a range | REST or flat files |

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
| `m.snapshots` | the current state of any mix of tickers, across asset classes |

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
| read daily bars | `m.stocks.bars("AAPL", dm.Timespan.DAY).history("2024-01-02", "2024-06-28")` |
| read every stock's bars for a day | `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2024-06-03", "2024-06-03")` |
| stream minute bars | `m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).append_only()` |
| keep the current state of symbols, on any plan | `m.stocks.snapshots(["AAPL", "MSFT"]).poll(15)` |
| follow a watchlist I can edit | pass a table with a `Symbol` column: see [`symbols`](#symbols) |
| backfill history, then continue live | the recipe in [split adjustment](#split-adjustment) |
| read a long range without freezing the console | `history(..., wait=False)`: see [live tables](#live-tables) |
| list a company's dividends | `m.stocks.dividends("AAPL", start="2024-01-01").fetch()` |
| follow news as it appears | `m.reference.news("AAPL", start=dh_today()).poll(300)` |
| stream the contracts of a futures product | `m.futures.trades(m.futures.contracts(product_code="ES").fetch()).last_by()` |
| find a ticker by name | `m.reference.tickers(search="Nvidia").fetch()` |
| see what a column means | `query.columns()`, or hover over the column header |
| see what the library is doing | `m.streams()`, `m.connections()`, `m.rate_limits()`: see [diagnostics](#diagnostics) |
| stop a live table now | `m.release(table)`: see [live tables](#live-tables) |
| read the library's log | `dm.enable_debug_logging()`: see [troubleshooting](#troubleshooting) |

## Concepts

<a id="table-shapes"></a>

### Table shapes

A **static** table never changes once you have it: `fetch()` and `history()` return one. A
**live** table changes while you watch it. The live verbs differ in what they keep:

| You want | Use |
|---|---|
| a running total or average over a stream | `blink()` and an aggregation |
| every row of a session, to analyze afterwards | `append_only()` (memory grows) |
| a rolling window of recent rows | `ring(n)` (fixed memory) |
| the current state of each symbol, as it trades | `last_by()` |
| the current state of each symbol, on any plan | `snapshots(...).poll(n)` |
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

Tables of one kind from two asset classes, such as stock and forex quotes, share their common
columns' names and types but can differ in which columns they have. To stack them, keep the
shared columns:

```python
from deephaven import merge

stock = m.stocks.bars("SPY", dm.Timespan.MINUTE).history("2024-06-03", "2024-06-03")
fx = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).history("2024-06-03", "2024-06-03")
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
  on a Friday holds Friday's trading alone. A futures bar covers its trading sessions.
- **Join on the end.** A bar's values are final only once its window has ended, so join other
  data on `TimestampEnd`: `trades.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"])` gives
  each trade the latest bar that had closed by then. Minute bars of every class end on whole
  minutes, so a stock's and an index's 10:31 bars join exactly on `TimestampEnd`.
- **`Date`** is the bar's trading day. It is null for a bar that spans more than one date: a
  bar of several days, a week or longer, or an intraday bar across midnight.
- **Which bars `history()` returns.** Only bars whose whole window lies inside the range and has
  already ended. Today's daily bar arrives tomorrow; the current week's or month's bar never
  appears; monthly bars from March 5 to March 20 return nothing, because no whole month fits.
  Pass dates, not instants, for daily bars: instants keep only whole calendar days inside them.
- **Every bar Massive sends is served.** Massive stamps the same bar differently on different
  sources, such as a daily stock bar at 00:00 on one and at the 16:00 close on another, and the
  library moves each known stamp to the start of its day, so every source gives the same rows.
  If Massive stamps a bar at a time the library does not expect, such as a daily bar at 14:30 or
  a week bar on a Wednesday, the bar is still served, labelled by the day its stamp falls on,
  and the library logs one WARNING per dataset and timespan in a session naming the stamp. Such
  a bar's `Date`, `TimestampStart`, and `TimestampEnd` may cover other days than the bar really
  does: check it before you rely on its dates, and report the WARNING on the
  [issues page](https://github.com/chipkent/deephaven-massive/issues). A crypto
  `daily_open_close` row that Massive marks as not UTC is treated the same way: served with the
  UTC day's window, with a WARNING.

<a id="history-sources"></a>

### Where history comes from

`history()` reads from one of two sources, and one call never mixes them.

- **REST** serves explicit symbols on any dataset with a REST history endpoint. For `dm.ALL` it
  serves only daily bars, through Massive's grouped daily endpoint, one request per day.
- **Flat files** serve a read when you have set S3 keys, the dataset has files for the request,
  and every day's file has been published: a day's file appears the next morning, from about
  11:00 New York time. Bars come in files only as 1-minute and 1-day bars (and 1-session bars for
  futures), and only as traded (`adjusted=False`).

The default, `source=dm.HistorySource.AUTO`, picks flat files when only they can serve the read,
or when both can and the read has many symbols (50 or more, the `flat_file_symbol_threshold`
setting) or `dm.ALL`; otherwise REST. Pass `source=dm.HistorySource.REST` or `FLAT_FILES` to
choose. The two sources sometimes fill different columns (REST trades have `DecimalSize`; the
files do not); when `AUTO`'s choice leaves columns empty that the other source fills, the
library logs a WARNING naming them and the `source=` that fills them. Each result's description,
shown in the web UI, names its source and why.

Flat-file reads skip a missing weekend file silently and a missing weekday file (a holiday) with
a WARNING. A history read pages through everything in its range, so it takes no `limit`.

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
the next split, so adjusted history and live bars join without a seam. On a split's execution
date every price moves to the new basis, so a table that runs across a split, or joins adjusted
history to live bars, shows a step there: build it again after a split. The recipe below
backfills minute bars, then continues live; it opens the stream first, so no bar falls between
the two:

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
late, so a minute's REST bar can differ slightly from the live one. A long download that runs
while Massive applies a split overnight can return pages on both bases, which nothing can
detect: download again after the split. The indicators use Massive's own basis, adjusted.

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

**Stop everything with `m.close()`.** Every stream, poll, and download of the session stops, and
every live table of the session, with everything derived from it, **fails** with
`MassiveSession #1 was closed; this table no longer updates`. Failing them, rather than leaving
them frozen, means no table ever looks live while it is not, so nobody acts on stale data. After
`close()`, verbs and the diagnostics tables raise `MassiveError`; building queries still works.
A failed table keeps its rows, but Deephaven builds nothing new on it, so take a static copy:

```python
trades = m.stocks.trades("AAPL").append_only()
# ... later ...
m.close()
kept = trades.snapshot()
```

A script that only reads static tables can let a `with` block close the session:
`with dm.MassiveSession() as m:`. A session nothing references any more is closed on its own,
with a WARNING.

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
year = m.stocks.trades(["AAPL", "MSFT"]).history("2023-10-02", "2024-09-30", wait=False)
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
account; partner datasets are sold separately. The same code runs on every plan: the library
never asks which plan you hold, and when something is out of reach, it says so.

What a plan changes, in the library's terms:

- **Transports.** Basic plans read REST only. Paid plans add WebSocket streams, and flat files
  with S3 keys.
- **Pace.** A Basic subscription allows five REST requests a minute; the library spaces its
  requests to fit, so a large download on Basic is slow but completes.
- **Freshness.** Lower stock, option, and index plans see the market 15 minutes late;
  `m.connections()` shows `Recency` `DELAYED`. A delayed stream works exactly like a real-time
  one.
- **Kinds of data and history.** Higher plans add trades, then quotes; every Basic plan keeps two
  years of history. Each dataset's page in the [reference](docs/datasets/README.md) has Massive's
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

A mistake in the call itself, such as a misspelled symbol or text where an enum belongs, raises
`ValueError` or `TypeError` before anything is sent.

<a id="diagnostics"></a>

### Diagnostics

Three live tables show what the library is doing. Start with `m.streams()` when a table looks
wrong.

- **`m.streams()`**: one row per stream, poll, and background download. `State` is `RUNNING`,
  `RELEASED` (you hold no table from it, but a derived table or an open panel still uses it),
  `FAILED`, `CLOSED`, or for a download `LOADING` and `DONE`. `Error` says why it stopped, or
  warns without stopping it (a watchlist row in the wrong spelling, options contracts waiting
  for room). `Query` shows the call that opened it, `RowsPublished` counts rows delivered, and
  `SchemaDrift` lists fields Massive added (`WebSocket:zz+`) or stopped sending (`REST:size-`).
- **`m.connections()`**: one row per WebSocket connection, with its `Host`, `Recency`
  (`REALTIME`, `DELAYED`, `UNKNOWN`), `State` (`CONNECTED`, `RECONNECTING`, `WAITING_SLOT` when
  another program uses your key's connection, `FAILED`), subscriptions confirmed and pending,
  and `LastError`; and one row per decision about which server to stream from.
- **`m.rate_limits()`**: one row per REST subscription, plus `other` for reference data. `State`
  is `READY`, `PACED` (five a minute), or `PAUSED` (Massive refused a request for coming too
  fast, until `PausedUntil`). Programs elsewhere that use your key count against the same limit
  without appearing here.

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

Every ticker must be Massive's exact spelling: `AAPL`, `BRK.B`, `O:SPY251219C00650000`,
`I:SPX`, `C:EURUSD`, `X:BTCUSD`, `ESZ6`. A wrong one in a string or list raises `ValueError` at
once. To accept other spellings, convert first: `dm.canonical_symbol_forex("EUR/USD")` returns
`"C:EURUSD"`, `dm.canonical_symbol_stock_yahoo("BRK-B")` returns `"BRK.B"`, and
`dm.canonicalize_symbols(table, dm.AssetClass.FOREX)` converts a whole table as rows arrive.
The converters also work inside query strings; `help(dm.canonical_symbol)` lists them.

**A live table is followed.** Add a row and a stream subscribes to it; delete one and it
unsubscribes. `poll()` reads the table at every cycle; `fetch()` and `history()` read it once,
when called. A static table, such as one from `new_table`, is a fixed list. An input table makes
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

In a live table, `last_by()` drops the row of a symbol that leaves the watchlist, while
`append_only()` and `ring()` keep the rows it already produced. A watchlist row that is not
Massive's spelling is skipped, logged once, and listed in the stream's `Error` in
`m.streams()`; fix the row and it subscribes. A watchlist whose every row is bad is refused. To
fail instead, set `dm.MassiveConfig(noncanonical_symbols=dm.OnError.RAISE)`. One WebSocket
connection carries at most 1,000 options contracts; more wait their turn, counted in
`m.streams()`'s `Deferred` column.

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
  `history("2024-06-03", "2024-06-03")` is all of June 3. Text such as `"2024-06-03"`, a Python
  `date`, or a Deephaven `LocalDate`.
- **Instants** are exact times, the start included and the end excluded. A timezone-aware Python
  `datetime`, a `pandas.Timestamp` or `numpy.datetime64`, Deephaven's `Instant` or
  `ZonedDateTime` (such as `dh_now()`), or Deephaven time text such as `"2024-06-03T09:30 ET"`.
  A naive `datetime` is refused.

Values taken from another table work directly. This finds the busiest minute of a day and reads
every trade inside it:

```python
minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2024-06-03", "2024-06-03")
for row in minutes.sort_descending("Volume").head(1).iter_dict():
    inside = m.stocks.trades("AAPL").history(row["TimestampStart"], row["TimestampEnd"])
```

An open `end` reads through now in `history()`: through today, in the market's time zone, for
dates (the current session for futures). On a list dataset an open bound is not sent, so
`m.stocks.dividends("AAPL", start="2024-01-01")` includes announced dividends still to come. A
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

`limit` caps the rows: the total across every symbol and page, so a symbol that comes first can
use up the whole cap. Some datasets cap each request instead, because each request names one
symbol: the indicators, `reference.tickers` and `stocks.ipos` when tickers are named, and
`reference.news`, whose `sort` and `order` apply to each ticker's request too. On
`options.contracts` with explicit symbols, each contract is one row, so `limit` has no effect.
Each dataset's `help()` says when its `limit` is per symbol. `limit` keeps the first rows in
Massive's order, so ask for newest first when you want the latest:
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
`HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`; `SESSION` for futures), required, and
`multiplier`, the number of units per bar (default 1). One-second and one-minute bars with
`multiplier=1` also stream; every other length is history only. Text is refused: write
`dm.Timespan.DAY`, or convert with `dm.Timespan("day")`.

<a id="venues"></a>

### `venues`

Futures datasets read every exchange by default. On a plan sold per exchange, pass the ones you
hold: `m.futures.trades("ESZ6", venues={dm.FuturesVenue.CME})`. The members are `CME`, `CBOT`,
`NYMEX`, and `COMEX`.

<a id="verb-arguments"></a>

### Verb arguments

- `poll(interval, *, mode=dm.PollMode.AUTO)`: `interval` in seconds or a `timedelta`; `mode`
  as in [table shapes](#table-shapes).
- `ring(capacity)`: the number of rows to keep.
- `last_by(by="Symbol")`: the key column or columns.
- `history(start, end=None, *, source=dm.HistorySource.AUTO, wait=True)`: the range as in
  [`start` and `end`](#start-and-end); `source` as in [history sources](#history-sources);
  `wait=False` reads in the background, as in [live tables](#live-tables).
- `fetch(*, wait=True)`: `wait` as for `history()`.

<a id="troubleshooting"></a>

## Troubleshooting

Find the message you see, then the fix. When nothing fits, turn on the debug log (the end of
this section) and look at what the library did.

| Message or symptom | Cause and fix |
|---|---|
| `no Massive API key` | The server cannot see `MASSIVE_API_KEY`. Export it in the shell that starts the server and restart, or pass `dm.MassiveConfig(api_key=...)`. |
| `MassiveSession must be created inside a Deephaven worker` | The code ran in a plain Python process. Run it in the Deephaven console or a script the server runs. |
| `timespan must be dm.Timespan` | Arguments that pick from a set take an enum member: `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` naming a symbol | Not Massive's spelling. See [`symbols`](#symbols) for the converters. |
| `AttributeError` naming a verb | The dataset cannot serve that verb; the message names the one that can. |
| `... plan has no WebSocket access` | Your plan for that class reads REST only. Poll a REST dataset, such as `m.stocks.snapshots(symbols).poll(15)`, or read `history()`. |
| `... refused the subscription to ...` | Your plan includes some streams of the class but not this one. Stream bars instead of trades, or use the REST counterpart. |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan does not include this dataset; the message names another where one exists. |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history window. Ask for a later start. |
| `the plan does not include these flat files` | Pass `source=dm.HistorySource.REST`; the message says when REST can serve the read. |
| `... is not yet published (flat files land ~11:00 ET next day)` | Read that day with `source=dm.HistorySource.REST`, or wait. |
| `... poll() needs a start or a limit` | A news or filings poll would re-read the whole archive each cycle. Give the query a `start` or a `limit`. |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed. Take `table.snapshot()` to keep the rows; create a new session for new tables. |
| `Can not listen to failed table` | Deephaven builds nothing on a failed table. Work on `table.snapshot()`. |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table. Pass `wait=False` to long reads. |
| `... which is not a stamp Massive is known to use` | Massive stamped a bar unexpectedly; it is served, but check its dates. See [bars](#bars), and please report it. |
| a WARNING that a read leaves some columns empty | Pass the `source=` it names. See [history sources](#history-sources). |
| `WAITING_SLOT` in `m.connections()` | Another program or computer uses your key's one connection for this class. Stop it; the library connects when the slot frees. |
| a live table stays empty | The market is closed (`m.reference.market_status().fetch()`); a console command is still running; your plan is 15 minutes delayed; or the stream failed (`m.streams()`). |
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
holds your API key or S3 secret. To report a bug, open an
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
warns when what Massive sends differs. [`docs/live-tests.md`](docs/live-tests.md) lists them.
The library is not on PyPI yet. Outside testers start with the
[tester guide](docs/tester-guide.md).

<a id="contributing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up a development checkout, run the
checks, and propose a change. Licensed under the Apache License 2.0; see [LICENSE](LICENSE).
