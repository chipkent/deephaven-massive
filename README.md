# deephaven-massive

<!-- The owner supplies the hero image in both variants, at docs/images/hero-light.png and docs/images/hero-dark.png. -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/hero-dark.png">
  <img alt="Screenshot of the Deephaven IDE with a live board of minute bars for five stocks, updating as each minute ends." src="docs/images/hero-light.png">
</picture>

![Python 3.11 or later](https://img.shields.io/badge/python-3.11%2B-blue)
![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status: release candidate](https://img.shields.io/badge/status-release%20candidate-orange)

Read years of market history and watch the market live, from one Python query.

Testing a trading idea often means two programs, one for the past and one for the live feed,
each with its own columns. With deephaven-massive, one query, a line of Python that names the
data you want from [Massive](https://massive.com) (formerly Polygon.io), does both jobs. Call
`history()` on it for the past, or `last_by()` for a live table that keeps each stock's newest
row. A live table updates as data arrives, and both tables have the same columns. You get the
same pair of tables from every dataset Massive offers both as history and as a stream, such as
trades and quotes.

You work with the tables in [Deephaven](https://deephaven.io), a free real-time query engine
that represents both static and streaming data as tables. It runs on your own computer, and you
type Python into its console in your browser. Every filter, join, or chart you build on a live
table updates with it.

Once you've installed the library, this example runs as pasted in the Deephaven console on
Massive's Stocks Advanced plan, which streams in real time. If you're on the free plan, start
with [Example 1](#whole-market-day) instead. The query `bars` names the minute bars of Apple and
Microsoft, and two calls read it, `history()` for one week and `last_by()` for a live table:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

bars = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE)
week = bars.history("2026-10-26", "2026-10-30")
latest = bars.last_by()
```

`week` is a static table of every minute bar the two stocks had that week. `latest` has one row
per stock, and each row is replaced as the next minute ends. It stays empty until the first bar
arrives, so run it while the market trades.

Stock bars are a small part of what you get:

- The library's 112 datasets cover stocks, options, CME Group futures, indices, forex, and crypto. Beyond
  prices, you get company financials, SEC filings, news, and economic data such as Treasury
  yields and inflation. The [dataset index](docs/datasets/README.md) links a page for each, with
  its columns and the plans that include it.
- A live table can follow a watchlist. Pass a table of symbols where a list would go, and the
  live table adds each symbol you add to the watchlist and drops each one you delete.
- On Massive's free Stocks Basic plan, you can read two years of daily and minute stock bars,
  plus news and economic data.
- Live tables stream in real time on Advanced and Business plans, and on Currencies Starter.
  Stocks, options, and indices on Starter and Developer plans stream 15 minutes late, and
  futures on those plans 10 minutes late. To accept that delayed feed, list the subscription in
  one setting, `delayed_subscriptions`.

To begin, [install the library](#install), then work through [ten examples](#learn-by-example).

<a id="install"></a>

## Install

Setup takes about 15 minutes. At the end, you have a Deephaven server running on your computer
with the library installed, and a first table in your browser.

<a id="check-what-you-need"></a>

### Check what you need

The server runs on macOS, Linux, and Windows 10 or 11 through WSL 2. Your computer needs at
least 8 GB of memory. [Platform notes](docs/guide/install.md#platforms) cover each operating
system in more detail.

Get these four ready before you start. The Check column shows how to confirm each one:

| What | Why | Check | Get it |
|---|---|---|---|
| Python 3.11 or later | Runs the library and Deephaven's Python API | `python3 --version` | [python.org](https://www.python.org/downloads/) or your package manager |
| Java 17 or later | Deephaven's engine runs on Java | `java -version` | [Adoptium](https://adoptium.net) |
| Git and the GitHub CLI | The library installs from its GitHub repository until it's on PyPI | `git --version` and `gh --version` | [git-scm.com](https://git-scm.com) and [cli.github.com](https://cli.github.com) |
| A Massive account | Massive gives you the API key every example uses | Sign in at [massive.com](https://massive.com) and open the API Keys page | Sign up at [massive.com](https://massive.com). The free Stocks Basic plan is enough to start. |

On Windows, WSL 2's clock can jump by 10 to 20 seconds and stall live tables. Apply one of
Deephaven's [clock-drift fixes](https://deephaven.io/core/docs/reference/community-questions/wsl2-clock-drift/)
before you start.

### 1. Sign in to GitHub

The install command reads the library from GitHub, so git needs your GitHub sign-in. Sign in
with the GitHub CLI, then let git use the same sign-in:

```bash
gh auth login
gh auth setup-git
```

To check, run `gh auth status`. It prints a line that says you're logged in to github.com.

### 2. Point the server at Java

Deephaven's server finds Java through the `JAVA_HOME` environment variable. Set it in the
terminal you'll use for the rest of the steps. On macOS, run:

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

On Linux, and on Windows inside WSL 2, run this instead:

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

Either way, `echo $JAVA_HOME` now prints the path to your Java install.

To set `JAVA_HOME` in every new terminal, add the `export` line to `~/.zshrc` on macOS or
`~/.bashrc` on Linux.

### 3. Create a Python environment

A virtual environment keeps Deephaven and the library apart from your other Python projects.
Create one and activate it:

```bash
python3 -m venv ~/dh-massive
source ~/dh-massive/bin/activate
```

Your prompt now starts with `(dh-massive)`. If you use uv, create the environment with uv
instead:

```bash
uv venv ~/dh-massive
source ~/dh-massive/bin/activate
```

### 4. Install Deephaven and the library

The first command looks up the newest release of the library, and the second installs it with
Deephaven's server:

```bash
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

If you use uv, install with `uv pip` instead:

```bash
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
uv pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

To check the install, run `pip show deephaven-massive`, or `uv pip show deephaven-massive` if you
used uv. It prints the version you installed.

### 5. Set your API key

The library reads your API key from the `MASSIVE_API_KEY` environment variable. The server sees
only the variables set in its terminal before it starts, so set the key in the terminal you'll
start the server from. Copy the key from the API Keys page of Massive's dashboard, then run this
line with your key in place of `your-api-key`:

```bash
export MASSIVE_API_KEY="your-api-key"
```

A key kept in the environment never lands in a script or notebook you save or share. To set it
in every new terminal, add the same line to `~/.zshrc` on macOS or `~/.bashrc` on Linux.

### 6. Start the Deephaven server

Start the server with 4 GB of memory:

```bash
deephaven server --jvm-args "-Xmx4g"
```

After about a minute, the terminal prints a line that starts with `Deephaven is running at`,
followed by the server's address. Your browser opens the Deephaven IDE at that address, already
signed in. Keep this terminal open, because closing it stops the server. To stop the server
yourself, press Control-C there. On a Mac with an Apple M2 chip, add
`-Dprocess.info.system-info.enabled=false` inside `--jvm-args`.

### 7. Make your first table

Your first table holds a week of Apple's daily bars, and it works on every Stocks plan, the free
Stocks Basic plan included. The code opens a session, which reads your key from
`MASSIVE_API_KEY`, then reads the bars. Click in the IDE's **Console** panel, where Python runs
inside the server. Then paste the code and press Enter:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
apple = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
```

The `apple` table opens in a new panel, with five rows, one for each trading day. Its columns
include `Date`, `Open`, `High`, `Low`, `Close`, and `Volume`.

<a id="fix-a-step"></a>

### Fix a step that fails

Most setup problems come from a terminal that's missing a variable or a sign-in. Find what you
see:

| You see | Why | What to do |
|---|---|---|
| `java: command not found` | Java isn't installed or isn't on your `PATH` | Install Java 17 or later from [Adoptium](https://adoptium.net), open a new terminal, and run `java -version` |
| The server stops at startup with an error about Java | `JAVA_HOME` isn't set in this terminal | Set it as in step 2 and start the server again |
| `Repository not found`, or a password prompt, during the install | Git can't use your GitHub sign-in | Run `gh auth login` and `gh auth setup-git`, then install again |
| A traceback in the console ending in `AuthenticationError: no Massive API key: ...` | The server started before `MASSIVE_API_KEY` was set | Stop the server with Control-C, set the key in that terminal, and start the server again |
| `RuntimeError: The Deephaven Server has not been initialized` | The code ran in plain Python, outside the server | Paste it into the **Console** panel of the Deephaven IDE |

[Troubleshooting](docs/guide/troubleshooting.md) lists every other message and says how to turn
on the library's log.

<a id="configuration"></a>

## Configuration

You need two environment variables to start: `MASSIVE_API_KEY`, your Massive key, and
`JAVA_HOME`, which Deephaven's server reads. [Install](#install) sets both, and everything else
here is optional. Environment variables hold your keys and anything that applies to the whole
server, and settings in code change how one session behaves.

<a id="environment-variables"></a>

### Environment variables

Set these in the terminal that starts the server, before you start it. A running server doesn't
see a change, so restart it after you set one. These are the variables the library and its
server read:

| Variable | Default | What it does |
|---|---|---|
| `MASSIVE_API_KEY` | None; required | Your Massive API key. Creating a session without it raises `dm.AuthenticationError`. |
| `MASSIVE_S3_ACCESS_KEY_ID` | Unset | The access key for Massive's flat files, which are compressed files that each hold a whole market's trades, quotes, or bars for one day. Massive shows it beside your API key on paid plans. |
| `MASSIVE_S3_SECRET_ACCESS_KEY` | Unset | The secret key that goes with the access key. Set both keys or neither. |
| `DEEPHAVEN_MASSIVE_CONFIG_FILE` | Unset | The path of a [settings file](docs/guide/install.md#settings-file), which `dm.MassiveConfig.from_file()` reads when you call it with no path. |
| `DEEPHAVEN_MASSIVE_LOG` | Unset, so only warnings and errors reach the server's terminal | Turns on the library's log at a level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. |
| `DEEPHAVEN_MASSIVE_LOG_FILE` | Unset, so the log goes to the server's terminal | A file to append the log to. Set alone, it turns the log on at `DEBUG`. |
| `HTTPS_PROXY`, `NO_PROXY` | Your system's proxy settings on macOS and Windows; otherwise none | The proxy for every connection to Massive, and the hosts that skip it. |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | The `certifi` certificate bundle | The certificate authorities to trust instead, such as your company's own. |
| `JAVA_HOME` | None; [Install](#install) step 2 sets it | The Java installation the server runs on. Deephaven's server reads it, not the library. |

The log variables take effect only when the server starts. To turn the log on in a console
that's already running, call `dm.enable_debug_logging()` instead.

With both S3 keys set, `history()` reads flat files for a request of 50 or more symbols, or
`dm.ALL`. It does so only when the files hold the data you asked for. Without the keys, it reads
from Massive's REST API, the web API that `fetch()` and `poll()` also use.
[S3 keys for flat files](docs/guide/install.md#s3-keys) says where to find the keys.

<a id="settings-in-code"></a>

### Settings in code

A session needs no settings unless one of your subscriptions is on the free Basic plan or
streams late. Massive sells one subscription per market (Stocks, Options, Indices, Currencies,
and Futures), each sold as a plan such as Basic or Starter. Two settings tell the session which
subscriptions are on the free plan and which stream late:

| Setting | Set it when | What changes | Example |
|---|---|---|---|
| `rest_basic_subscriptions` | A subscription is on Massive's free Basic plan, which allows five requests a minute | The session keeps that subscription to five requests a minute, starting with its first request. Without the setting, it slows down only after Massive refuses a request. | `{dm.Subscription.STOCKS}` |
| `delayed_subscriptions` | Your Starter or Developer plan streams a subscription late: 15 minutes, or 10 for futures | Live tables read Massive's delayed feed. Without the setting, opening a live table or fetching current data, such as a snapshot, raises `dm.EntitlementError`, and its message names the line to add. | `{dm.Subscription.STOCKS}` |

Pass the settings to `dm.MassiveConfig`, and pass that to `dm.MassiveSession`. This session is
for an account with Stocks on the Basic plan and Options on the Starter plan:

```python
import deephaven_massive as dm

config = dm.MassiveConfig(
    rest_basic_subscriptions={dm.Subscription.STOCKS},
    delayed_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

`help(dm.MassiveConfig)` in the console lists every other setting and its default. To share
settings among consoles, scripts, and containers, put them in a
[settings file](docs/guide/install.md#settings-file).

<a id="learn-by-example"></a>

## Learn the library in ten examples

Each example adds one idea to the ones before it, and each one runs as pasted into a fresh
console. To run one, paste it into the **Console** panel of the Deephaven IDE and press Enter.
Examples 1 to 6 run on any Stocks plan, the free Stocks Basic plan included. Examples 1 to 5
need no settings. Example 6 adds one, `rest_basic_subscriptions`, which paces your requests to
the free plan's limit of five a minute. Examples 7 to 10 need a paid plan, which each one names
before its code.

<a id="whole-market-day"></a>

### 1. Rank every US stock by one day's trading, on the free plan

You get the daily bar of every US-listed stock for October 30, 2026, in one table. It runs on
the free Stocks Basic plan with no settings.

The code reads that day's bar for every stock, then ranks the stocks by dollars traded and by
their gain from open to close:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

day = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2026-10-30", "2026-10-30")

busiest = (
    day.update_view("DollarVolume = Vwap * Volume")
    .sort_descending("DollarVolume")
    .head(20)
)
risers = (
    day.where("Volume >= 1000000")
    .update_view("ChangePct = 100 * (Close - Open) / Open")
    .sort_descending("ChangePct")
    .head(20)
)
```

Three tables open. `day` has one row per stock, and its columns include `Symbol`, `Date`,
`Open`, `High`, `Low`, `Close`, `Volume`, and `Vwap`, the volume-weighted average price.
`busiest` holds the 20 stocks with the most dollars traded, largest first. `risers` holds the 20
stocks that rose most from open to close, among those that traded at least a million shares.

To rank another day, change both dates. Stocks Basic reaches back two years.

`m.stocks.bars(dm.ALL, dm.Timespan.DAY)` names the data you want, the daily bars of every
US-listed stock, since `dm.ALL` stands for every symbol. On its own, it reads nothing.
`history()` reads that data from Massive and returns a Deephaven table.
`update_view`, `where`, `sort_descending`, and `head` are Deephaven's own
[table operations](https://deephaven.io/core/docs/conceptual/table-operations-overview/), and
they work on every table the library returns. The [stock bars page](docs/datasets/stocks.bars.md)
lists every column.

<a id="compare-stocks"></a>

### 2. Compare a few stocks over a year

You'll get a three-row scorecard with each stock's first and last close of the year, its high
and low, and its return.

`agg_by` reduces each stock's year of daily bars to one row:

```python
import deephaven_massive as dm
from deephaven import agg

m = dm.MassiveSession()

year = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    "2025-11-03", "2026-10-30"
)
scorecard = year.agg_by(
    [
        agg.sorted_first("Date", "Start = Close"),
        agg.sorted_last("Date", "End = Close"),
        agg.max_("High"),
        agg.min_("Low"),
    ],
    by="Symbol",
).update_view("ReturnPct = 100 * (End / Start - 1)")
```

`year` has one row per stock for each trading day in the range. `scorecard` has three rows, one
per stock, with `Start`, `End`, `High`, `Low`, and `ReturnPct`.

The first argument, `symbols`, takes one ticker, a list, or `dm.ALL`. Its full rules are in
[`symbols`](docs/guide/arguments.md#symbols).

Dates are `yyyy-MM-dd` text, and the range includes both ends. Because `Date` holds the same
text, it sorts by day, which is how `sorted_first` finds the year's first close. For the rules of
a range, see [`start` and `end`](docs/guide/arguments.md#start-and-end).

<a id="how-it-works"></a>

### 3. Find the dataset you need

Before you read any data, you can ask the library which dataset holds it, what a company's
ticker is, and what each column means. `dm.datasets()` and `columns()` answer without a request
to Massive. The ticker search sends one.

Search the catalog for dividends, look up Coca-Cola's ticker, and describe the dividend columns:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

catalog = dm.datasets()
about_dividends = catalog.where("Summary.toLowerCase().contains(`dividend`)")
coke = m.reference.tickers(search="Coca-Cola").fetch()
dividend_columns = m.stocks.dividends("KO").columns()
```

`catalog` has one row per dataset, and its columns include `Dataset`, `Call`, `Summary`,
`Verbs`, and `Documentation`, a link to the dataset's page. `about_dividends` keeps the few
datasets whose summary mentions dividends. `coke` lists the tickers whose name matches, among
its columns `Symbol`, `Name`, and `PrimaryExchange`. `dividend_columns` has one row for each
column of the dividends dataset, with its name, its type, and a description.

`day`, `year`, and `coke` all came from one pattern:
`m.<namespace>.<dataset>(<arguments>).<verb>()`. A namespace, such as `m.stocks` or
`m.reference`, groups the datasets of one market or one kind of data. Calling a dataset makes a
query, and a verb, such as `history()` or `fetch()`, reads the data and returns a table. If you
call a verb the dataset doesn't have, Python raises `AttributeError` and lists the verbs it does
have. The [verb reference](docs/guide/arguments.md#verb-reference) describes each verb.

<a id="price-dividends"></a>

### 4. Price each dividend against the stock

You get each dividend KO, PEP, and JNJ paid over a year, priced against the stock's close on
its ex-dividend date.

`natural_join` matches each dividend to the daily bar of the same symbol and date:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

payers = ["KO", "PEP", "JNJ"]
dividends = m.stocks.dividends(payers, start="2025-11-03", end="2026-10-30").fetch()
closes = m.stocks.bars(payers, dm.Timespan.DAY).history("2025-11-03", "2026-10-30")

yields = (
    dividends.natural_join(
        closes, on=["Symbol", "ExDividendDate = Date"], joins="CloseOnExDate = Close"
    )
    .update_view("AnnualYieldPct = 100 * CashAmount * Frequency / CloseOnExDate")
    .view(["Symbol", "ExDividendDate", "CashAmount", "CloseOnExDate", "AnnualYieldPct"])
)
```

`yields` has one row per dividend whose ex-dividend date falls in the range. Each row shows the cash amount, the close on the ex-dividend date, and the yield that a
year of such payments implies. `Frequency` is the number of payments a year.

Every date column in every table is `yyyy-MM-dd` text, so a dividend's `ExDividendDate` joins
straight onto a bar's `Date`. `fetch()` reads a dataset's rows once. For dividends, `start` and
`end` select the rows by ex-dividend date.

<a id="follow-the-news"></a>

### 5. Follow the news as it breaks

You get the latest articles about Apple, Nvidia, and Tesla in a table that adds each new article
within five minutes of Massive listing it.

`poll(300)` repeats the request for each stock's ten newest articles every five minutes:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

headlines = m.reference.news(
    ["AAPL", "NVDA", "TSLA"], sort="published_utc", order=dm.Order.DESC, limit=10
).poll(300)
newest_first = headlines.view(
    ["PublishedUtc", "Symbol", "Title", "PublisherName"]
).sort_descending("PublishedUtc")
```

`newest_first` starts with up to ten articles for each stock, newest at the top. Articles
already in the table stay as new ones arrive.

`headlines` is your first live table. `poll(seconds)` repeats a `fetch()` on a timer. For news,
each poll adds only the articles not already in the table, and `newest_first` updates with it
because it's built on `headlines`.

`headlines` keeps polling for as long as it, or a table built on it such as `newest_first`, is
open in the IDE or held in a variable. To stop a live table at once, call `m.release(headlines)`.
[Live tables](docs/guide/live-tables.md#live-tables) says when else a live table stops, and
[poll modes](docs/guide/arguments.md#poll-modes) says how polls of other datasets combine their
reads.

<a id="what-your-plan-allows"></a>

### 6. Learn what your plan allows

You find out whether your plan reaches back to 2004. The session is paced for the free plan.

Massive sells one subscription per market (Stocks, Options, Indices, Currencies, and Futures),
each sold as a plan, from the free Basic plan up to paid plans such as Stocks Starter. When you
ask for something your plan doesn't include, Massive refuses. The library then raises
`dm.EntitlementError`, or it stops the live table and shows the error there.

On Stocks Basic, create the session with `rest_basic_subscriptions`, which spaces its Stocks
requests to five a minute. On a paid Stocks plan, leave the setting out. The `try` block asks for
Apple's daily bars from January 2004 and prints the refusal if your plan doesn't reach that far:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions={dm.Subscription.STOCKS}))

try:
    old = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2004-01-05", "2004-01-30")
except dm.EntitlementError as error:
    print(error)
```

On Stocks Basic, Starter, or Developer, the console prints a message that names the refused
data and gives Massive's reason. On Stocks Advanced or Business, `old` holds the bars.

The biggest differences are between the free Basic plan and the paid ones:

| | Basic (free) | Paid plans |
|---|---|---|
| How you read | `fetch()`, `poll()`, and `history()` | Those, plus live streams and flat files |
| Pace | Five requests a minute for each subscription | No limit |
| Freshness of stocks, options, and indices | End of day | 15 minutes late on Starter and Developer, real time on Advanced and Business |
| History of stock bars | Two years | Five years on Starter, ten on Developer, and all of Massive's history on Advanced |

Each dataset's page has Massive's plan table, such as the one for
[stock bars](docs/datasets/stocks.bars.md#plan-access). [Plans](docs/guide/plans.md) covers
the rest, futures and currencies included.

<a id="table-shapes"></a>

### 7. Watch trades arrive, four ways

Bitcoin and Ether trades arrive in four live tables, each keeping a different part of one
stream, plus a running summary. Crypto trades around the clock, so you can run this at any
hour. It needs a Currencies Starter plan or higher, which streams crypto in real time.

Each verb on the `crypto` query makes a different kind of live table, and all four read one
stream from Massive:

```python
import deephaven_massive as dm
from deephaven import agg

m = dm.MassiveSession()

crypto = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"])
last = crypto.last_by()
recent = crypto.ring(50)
tape = crypto.append_only()
ticks = crypto.blink()

since_start = ticks.agg_by(
    [agg.count_("Trades"), agg.sum_("Volume = Size"), agg.weighted_avg("Size", "Vwap = Price")],
    by="Symbol",
)
```

All five tables change about once a second, because Deephaven applies new rows in batches
called update cycles.

Each trade's columns include `Symbol`, `Timestamp`, `Price`, `Size`, and `Exchange`. The tables
differ in which trades they keep:

| Table | Verb | Keeps |
|---|---|---|
| `last` | `last_by()` | One row per pair, holding its latest trade |
| `recent` | `ring(50)` | The latest 50 trades |
| `tape` | `append_only()` | Every trade since you ran the code, so it keeps growing |
| `ticks` | `blink()` | Only the trades of the latest update cycle |
| `since_start` | An aggregation of `ticks` | One row per pair, with its trade count, volume, and VWAP since you ran the code |

An aggregation of a blink table counts every trade that passes through it, though the blink
table holds each trade for only one cycle. That's how `since_start` keeps running totals
without storing the trades. To choose a live table for a job, see
[choose a live table](docs/guide/arguments.md#choose-a-live-table).

<a id="seed-a-live-board"></a>
<a id="live-board"></a>

### 8. Build a live board against yesterday's close

You'll get a board of five stocks, each row showing the latest minute's close and its change
since yesterday's close.

It needs a Stocks Starter plan or higher. Starter and Developer stream 15 minutes late, so the
session accepts delayed data with `delayed_subscriptions`. Without that setting, the library
refuses the live table on those plans with an error that names the line to add. On Advanced or
Business, which stream in real time, leave the setting out.

`natural_join` puts yesterday's close beside each stock's newest minute bar:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
latest = m.stocks.bars(symbols, dm.Timespan.MINUTE).last_by()
yesterday = m.stocks.previous_day_bar(symbols).fetch()

board = (
    latest.natural_join(yesterday, on="Symbol", joins="PrevClose = Close")
    .update_view("ChangePct = 100 * (Close / PrevClose - 1)")
    .view(["Symbol", "TimestampEnd", "Close", "PrevClose", "ChangePct"])
)
```

`board` has one row per stock. While the market trades, each row's `Close`, `ChangePct`, and
`TimestampEnd` change as each minute ends. A stock's row appears with its first bar after you
run the code. A board you create overnight stays empty until the first bar of the next session
arrives. On Starter and Developer, that's 15 minutes after the bar's minute ends.

A live table joins a static one the same way two static tables join. `latest` updates as bars
arrive, and `fetch()` read `yesterday` once.

To give every stock a row before its first bar, see
[show every symbol before it trades](docs/guide/watchlists.md#seed-a-watchlist-board). For how
each plan's feed works, see [real-time or delayed](docs/guide/plans.md#real-time-or-delayed).

<a id="drive-with-a-watchlist"></a>

### 9. Drive the board from a watchlist

The board in this example follows a table you edit, so you can change its stocks while it runs.
It needs a Stocks Starter plan or higher, with `delayed_subscriptions` on Starter and
Developer, as in [Example 8](#live-board).

The watchlist is a Deephaven input table, a table you can edit in code or by hand, keyed by its
`Symbol` column. The code makes the watchlist, builds a board from it, then adds NVDA and
deletes MSFT:

```python
import deephaven_massive as dm
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
board = m.stocks.bars(watchlist, dm.Timespan.MINUTE).last_by()

watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))
watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
```

`watchlist` holds AAPL and NVDA, and `board` shows the latest minute bar of each once it trades.
Adding NVDA started streaming it. Deleting MSFT stopped its stream and dropped its row.

Wherever a dataset takes `symbols`, it also takes a table with a `Symbol` column, and it follows
a live one as it changes. [Watchlists](docs/guide/watchlists.md) says what each verb does as a
watchlist changes.

To add a symbol to `watchlist` by hand in the Deephaven IDE:

1. Open the `watchlist` panel.
2. Type the symbol in the empty row at the bottom.
3. Press Enter.
4. Click **Commit**. The row joins the watchlist only when you commit it.

<a id="backfill-then-live"></a>

### 10. Start with today's history, then continue live

You'll get one table of SPY's minute bars for today, with every minute so far from history and
each new minute added as it ends. It needs a Stocks Starter plan or higher, with
`delayed_subscriptions` on Starter and Developer, as in [Example 8](#live-board).

The code opens SPY's live stream of minute bars, reads today's history, and merges the two:

```python
import deephaven_massive as dm
from deephaven import merge
from deephaven.time import dh_today, to_j_time_zone

m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

today = dh_today(to_j_time_zone("America/New_York"))
bars = m.stocks.bars("SPY", dm.Timespan.MINUTE)
live = bars.append_only()
past = bars.history(today)
spy = merge([past, live]).last_by(["Symbol", "TimestampStart"])
```

`spy` has one row per minute of SPY's day, oldest first. Each minute that ends while the table is
open adds a row at the bottom. `dh_today` gives New York's date as `yyyy-MM-dd` text, and
`history()` with no end date reads through now.

The code opens the stream before it reads history, so no minute falls between the two. Where
both have the same minute, `last_by` keeps the live bar.

History is split-adjusted, and live bars are as traded. After a split, build the table again.
[Split adjustment](docs/guide/history.md#split-adjustment) has the details.

<a id="recipes"></a>

## Recipes

Use these recipes to stream SPY's options near the money as SPY moves, or to read the history of
a watchlist you keep editing. For three other common jobs, start from an example:

- To screen the whole market, see [Example 1](#whole-market-day).
- For a live board, see [Example 8](#live-board) and [Example 9](#drive-with-a-watchlist).
- For history that continues live, see [Example 10](#backfill-then-live).

<a id="option-chain"></a>

### Stream the options near the money

You get the latest quotes of SPY's options within 2% of SPY's price, and the set of streamed
contracts follows the price. Streaming option quotes needs Options Advanced, or Options Business
with Massive's Expansion add-on. Both stream in real time, so you don't need to add options to
`delayed_subscriptions`.

The code re-reads SPY's chain for the next seven days of expirations every five minutes, keeps
the contracts near the money, and streams their quotes:

```python
import deephaven_massive as dm
from datetime import date, timedelta
from deephaven.time import dh_today, to_j_time_zone

m = dm.MassiveSession()

today = date.fromisoformat(dh_today(to_j_time_zone("America/New_York")))
week_out = today + timedelta(days=7)
spy_chain = m.options.chain(
    "SPY",
    filters={
        "expiration_date_gte": today.isoformat(),
        "expiration_date_lte": week_out.isoformat(),
    },
).poll(300)
near = spy_chain.where(
    "abs(DetailsStrikePrice - UnderlyingAssetPrice) <= 0.02 * UnderlyingAssetPrice"
)
near_quotes = m.options.quotes(near).last_by()
```

`spy_chain` has one row for each SPY contract that expires in the next seven days. Its columns
include each contract's strike, implied volatility, greeks, and open interest. Massive leaves the
greek columns null for some contracts, such as those deep in the money. `near_quotes` has the
latest quote of each contract within 2% of SPY's price, and it fills as quotes arrive while
options trade.

The filter compares each strike with the chain's `UnderlyingAssetPrice`, which Massive fills
only as far as your Stocks plan allows. Where that price is null, no contract passes, and
`near_quotes` stays empty.

`m.options.quotes(near)` takes a live table of contracts as its symbols, and the stream follows
`near`. Each five-minute read of the chain updates `near`. The stream then subscribes to the
contracts that came within 2% and drops the ones that drifted out.

One options connection streams at most 1,000 contracts. Contracts past the cap wait until
others leave, so keep the filter narrow enough that `near` stays under 1,000 rows.

<a id="history-of-a-watchlist"></a>

### Read the history of a watchlist

You can read the history of the symbols on a watchlist three ways. They differ in what happens
when you edit the watchlist:

| Way | When you edit the watchlist | Cost |
|---|---|---|
| Pass the watchlist itself | Each symbol you add is read over the same range, and the rows of each one you delete leave the table | One read per symbol you add |
| Pass `watchlist.snapshot()` | Nothing changes, because the table holds the symbols on the list when you called it | One read |
| Read every symbol, then filter by the watchlist | Rows appear and leave at once, with no new read | One read of the whole market |

All three run on any Stocks plan, the free one included. The code builds a watchlist, then reads
its daily bars each way:

```python
import deephaven_massive as dm
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col
from deephaven.experimental.data_index import data_index

m = dm.MassiveSession()

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))

followed = m.stocks.bars(watchlist, dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
fixed = m.stocks.bars(watchlist.snapshot(), dm.Timespan.DAY).history("2026-10-26", "2026-10-30")

every = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
data_index(every, ["Symbol"])
mine = every.where_in(watchlist, "Symbol")
```

`followed`, `fixed`, and `mine` each start with the week's daily bars of AAPL and MSFT. Add a
symbol, as in `watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))`, and `followed`
gains its bars once they're read, `mine` gains them at once, and `fixed` stays as it was.
`m.symbol_reads()` shows the state of each symbol's read in `followed`. The `data_index` line
indexes `every` by symbol, so each edit of the watchlist finds its rows without scanning the
whole market.

Reading every symbol works when the whole market fits in the server's memory, the 4 GB that
Install's step 6 gives it. `every.size` shows how many rows a read holds. Daily bars fit on any
plan, and so does one day of minute bars. Reading minute bars for every symbol needs the flat
files, which start at Stocks Starter, and `adjusted=False`. A year of minute bars, or any range
of trades or quotes, doesn't fit.

To see how `followed` handles each edit and each symbol that fails to read, see
[`history()` and `fetch()` of a watchlist](docs/guide/watchlists.md#followed-reads).

<a id="troubleshooting"></a>

## Fix common errors

Most errors after setup come from what your plan includes or from how a call is written. Find
the message you see:

| You see | Why | What to do |
|---|---|---|
| An `EntitlementError` that says `... would be delayed (Massive reports ...)` | Massive sends this data late on your plan, and the session accepts delayed data only for the subscriptions you list | Add the setting the message shows, as in [settings in code](#settings-in-code) |
| `... refused the subscription to ...` | Your plan doesn't stream this data | Check the dataset's plan table in the [dataset index](docs/datasets/README.md). Stream a dataset your plan includes. If the plan table lists REST for this dataset, read it with `poll()` or `history()` instead. |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history begins | Start inside your plan's history, which the dataset's plan table gives, such as two years of bars on Stocks Basic |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan doesn't include this dataset | Read the message, which names another dataset where one exists |
| `ValueError` naming a symbol | The symbol isn't spelled as Massive spells it, such as `aapl` for `AAPL` | Use Massive's spelling. See [`symbols`](docs/guide/arguments.md#symbols). |
| `timespan must be dm.Timespan` | This argument takes one of the `dm.Timespan` constants, not a string | Write `dm.Timespan.DAY`, not `"day"` |
| `AttributeError` naming a verb | The dataset doesn't have that verb | Use a verb the message lists |
| A live table stays empty | The market is closed, your plan is delayed and the market opened less than 15 minutes ago, or the stream failed | Run `m.streams()`, which shows each live table's state and error |
| A read is slow on the free plan | The Basic plan allows five requests a minute | Run `m.rate_limits()` to see the pace, or read fewer symbols |
| A table keeps running after `del` | `del` stops a live table only once garbage collection reclaims it, which can take minutes | Call `m.release(table)`. The table stops at once unless a table you built from it, or an open panel, still uses it. |

[Troubleshooting](docs/guide/troubleshooting.md) lists every other message and says how to report
a bug.

<a id="where-next"></a>

## Where to go next

| Page | Go here when you want to |
|---|---|
| [Install](docs/guide/install.md) | Run Deephaven in Docker or Jupyter instead of from a terminal |
| [Watchlists](docs/guide/watchlists.md) | Edit a watchlist, or stream only during market hours |
| [History](docs/guide/history.md) | Choose where history comes from, or adjust bars for splits yourself |
| [Live tables](docs/guide/live-tables.md) | Stop a live table, or learn what it does when the connection drops |
| [Plans](docs/guide/plans.md) | Check what your plan includes |
| [Arguments](docs/guide/arguments.md) | Look up the rules of an argument or a verb, or choose a live table |
| [Troubleshooting](docs/guide/troubleshooting.md) | Find an error message or report a bug |
| [Dataset index](docs/datasets/README.md) | Find a dataset's columns and Massive's plan table for it |
| [Deephaven's crash course](https://deephaven.io/core/docs/getting-started/crash-course/overview/) | Learn Deephaven's filters, joins, aggregations, and plots |

<a id="status"></a>

## Status

The library is in release-candidate testing and isn't on PyPI yet. If you're testing a release
candidate, the [tester guide](docs/tester-guide.md) walks you through every step and says what to
report.

<a id="contributing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up a development checkout, run the
checks, and propose a change. The library is licensed under the
[Apache License 2.0](LICENSE).
