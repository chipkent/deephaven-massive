# deephaven-massive

<!-- Placeholder images: the owner's hero screenshot replaces docs/images/hero-light.png and docs/images/hero-dark.png. -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/hero-dark.png">
  <img alt="Placeholder for the hero screenshot: the project name on a plain background." src="docs/images/hero-light.png">
</picture>

![Python 3.11 or later](https://img.shields.io/badge/python-3.11%2B-blue)
![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)

Market data from [Massive](https://massive.com) as live and historical
[Deephaven](https://deephaven.io) tables, for traders and quants who work in Python.

Testing a trading idea usually starts with data plumbing: one program for history, another for
the live feed, and a streaming system before you see a live number. Here, one query serves both
the history and the live table, with the same columns.

- Read a year of bars, or every US stock on one day, in one line.
- Watch each stock's change since the previous close update with every minute bar.
- Spend your time on trading ideas, with live feeds and watchlist-driven tables already built.

Every US stock's bar for one day, on Massive's free plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
day = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2026-10-30", "2026-10-30")
# day: a row per stock, with its open, high, low, close, and volume
```

[Install the library](#install), then start the [tutorial](#tutorial) with a week of Apple's
prices.

<a id="install"></a>

## Install

You install Deephaven's server and the library into one Python environment, then start the
server from it.

<a id="prerequisites"></a>

### Prerequisites

You need:

| What | Check it with | Get it from |
|---|---|---|
| Python 3.11 or later | `python3 --version` | [python.org](https://www.python.org/downloads/) |
| Java 17 or later, with `JAVA_HOME` set | `java -version` and `echo $JAVA_HOME` | Deephaven's [pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/) |
| Git and the GitHub CLI | `git --version` and `gh --version` | [git-scm.com](https://git-scm.com) and [cli.github.com](https://cli.github.com) |
| A Massive account | Sign in at [massive.com](https://massive.com) | [Massive's plans](https://massive.com/pricing) |

<a id="sign-in-to-github"></a>

### Sign in to GitHub

The library installs from its GitHub repository. Sign in with the GitHub CLI, and let git use
the same sign-in:

```bash
gh auth login
gh auth setup-git
```

<a id="install-the-library"></a>

### Install the library

Create a virtual environment and install Deephaven's server, its chart and dashboard plugins,
and the library's newest release. Use pip or uv.

With pip:

```bash
python3 -m venv ~/dh-massive
source ~/dh-massive/bin/activate
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
pip install deephaven-server deephaven-plugin-ui deephaven-plugin-plotly-express \
  "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

With [uv](https://docs.astral.sh/uv/getting-started/installation/):

```bash
uv venv --python ">=3.11" ~/dh-massive
source ~/dh-massive/bin/activate
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
uv pip install deephaven-server deephaven-plugin-ui deephaven-plugin-plotly-express \
  "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

<a id="set-your-api-keys"></a>

### Set your API keys

Add your keys to your shell's startup file: `~/.zshrc` on macOS, `~/.bashrc` on Linux. Copy each
key from Massive's [keys page](https://massive.com/dashboard/keys) over its placeholder. The two
S3 keys are for Massive's flat files. Leave them out if your plan doesn't include flat files:

```bash
export MASSIVE_API_KEY="your-api-key"
export MASSIVE_S3_ACCESS_KEY_ID="your-s3-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-s3-secret-access-key"
```

Every terminal you open from now on has the keys.

<a id="start-the-server"></a>

### Start the server

Open a new terminal, activate the environment, and start Deephaven's server with 4 GB of memory:

```bash
source ~/dh-massive/bin/activate
deephaven server --jvm-args "-Xmx4g"
```

The server prints a line that starts with `Deephaven is running at` and opens the Deephaven IDE
in your browser. Keep the terminal open while you work, and press Control-C there to stop the
server. Your first table is the tutorial's [first example](#read-daily-bars). If a step fails,
[Troubleshooting](docs/troubleshooting.md#setup-messages) lists each setup message and its fix.

<a id="configuration"></a>

## Configuration

Examples 1 to 7 need only the API key you set in Install. Everything else here is optional.

<a id="environment-variables"></a>

### Environment variables

Set these in your shell's startup file, as Install does for the keys. After you set one, stop
the server and start it from a new terminal, as [Start the server](#start-the-server) does.

| Variable | What you get | Default |
|---|---|---|
| `MASSIVE_API_KEY` | Access to Massive. Every session needs it. | None |
| `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY` | `history()` from Massive's flat files, on plans that include them, for trades, quotes, index values, and as-traded (`adjusted=False`) 1-minute and daily bars. A read uses the files for `dm.ALL`, for 50 or more symbols, or when REST can't serve it. Bars with the default `adjusted=True` always come from REST. Set both or neither. | Unset |
| `DEEPHAVEN_MASSIVE_CONFIG_FILE` | Your settings file, read by `dm.MassiveConfig.from_file()` called with no path | Unset |
| `DEEPHAVEN_MASSIVE_LOG` | The library's log at the level you name: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` | Warnings and errors only, in the server's terminal |
| `DEEPHAVEN_MASSIVE_LOG_FILE` | The log in this file instead of the terminal. Set alone, it logs at `DEBUG`. | Unset |
| `HTTPS_PROXY`, `NO_PROXY` | A proxy for every connection to Massive, and the hosts that skip it | Your system's proxy on macOS; none on Linux |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | The certificate authorities to trust, such as your company's own | The `certifi` bundle |

[The log](docs/troubleshooting.md#the-log) in Troubleshooting explains how to read the log and
attach it to a bug report.

<a id="massive-config"></a>

### The MassiveConfig class

`dm.MassiveSession()` creates a session, your connection to Massive. `dm.MassiveConfig` holds its
settings, and `dm.MassiveSession(config)` uses them. A session created without one takes the
defaults. Each Massive subscription, such as Stocks or
Options, has its own plan. Two settings fit a session to your plans:

| Setting | Set it when | What changes |
|---|---|---|
| `delayed_subscriptions` | A subscription's plan is delayed, as Stocks Starter and Developer are | Live tables and snapshots accept Massive's delayed feed. |
| `rest_basic_subscriptions` | A subscription's plan is Basic, the free plan | Its requests wait their turn at Basic's five a minute. Without the setting, the session slows to that rate after Massive's first refusal. |

This session is for an account with Stocks Starter and Options Basic:

```python
import deephaven_massive as dm

config = dm.MassiveConfig(
    delayed_subscriptions={dm.Subscription.STOCKS},
    rest_basic_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

`dm.MassiveConfig.from_file()` reads the same settings from a YAML file, as the
[plans and settings](#what-your-plan-allows) example shows. `help(dm.MassiveConfig)` lists every
setting and its default.

<a id="tutorial"></a>

## Tutorial

Paste each example into the **Console** panel of the Deephaven IDE and run it. Examples 1 to 7
run on Stocks Basic, Massive's free plan. Examples 8 to 13 need Stocks Starter or higher, and
Examples 9 to 13 read the settings file that Example 8 saves.

| # | Example | Plan |
|---|---|---|
| 1 | [Queries: read a week of Apple's daily bars](#read-daily-bars) | Stocks Basic |
| 2 | [Symbol lists: rank every US stock by one day's dollar volume](#whole-market-day) | Stocks Basic |
| 3 | [Date ranges: compare three stocks' price returns over a year](#compare-stocks) | Stocks Basic |
| 4 | [Charts: plot a stock's daily closes for a year](#chart-closes) | Stocks Basic |
| 5 | [Live tables: poll for the latest news headlines](#follow-the-news) | Stocks Basic |
| 6 | [Stopping live tables: stop the news feed and keep a snapshot](#stop-live-tables) | Stocks Basic |
| 7 | [Loading data for new symbols without restarting: add and remove stocks from a live table of previous closes](#drive-with-a-watchlist) | Stocks Basic |
| 8 | [Plans and settings: load your settings from a file and read a delayed market snapshot](#what-your-plan-allows) | Stocks Starter or higher |
| 9 | [Streaming: stream minute bars for the stocks on your list](#stream-minute-bars) | Stocks Starter or higher |
| 10 | [Live calculations: track each stock's change since the previous close](#change-since-close) | Stocks Starter or higher |
| 11 | [Table types: keep the latest bar, the last 30 minutes, or the whole stream since you started](#table-shapes) | Stocks Starter or higher |
| 12 | [Backfill: start with today's minute bars, then continue live](#backfill-then-live) | Stocks Starter or higher |
| 13 | [Dashboards: show a live chart and table together with deephaven.ui](#dashboard) | Stocks Starter or higher |

<a id="read-daily-bars"></a>

### Queries: read a week of Apple's daily bars

Read Apple's daily bars for the last week of October 2026, with Stocks Basic:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
apple = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
```

`apple` has five rows, one per trading day, with columns such as `Date`, `Open`, `High`, `Low`,
`Close`, and `Volume`.

`m` is the session, your connection to Massive. Every dataset works the same way through it.
`m.stocks.bars(...)` is a query, which names the data and reads nothing. `history()` and the
query's other methods read the data and return a Deephaven table, so Deephaven's
[table operations](https://deephaven.io/core/docs/conceptual/table-operations-overview/) work on
it. The
[dataset index](docs/datasets/README.md) lists each dataset's arguments and columns.

Next, one query for every US stock.

<a id="whole-market-day"></a>

### Symbol lists: rank every US stock by one day's dollar volume

Find the stocks that traded the most dollars on October 30, 2026, with Stocks Basic. Read that
day's bar for every US stock, then rank them in Deephaven:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
day = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2026-10-30", "2026-10-30")
busiest = (
    day.update_view("DollarVolume = Vwap * Volume")
    .sort_descending("DollarVolume")
    .head(20)
)
```

`day` has a row for every stock that traded that day, and `busiest` keeps the leaders.

A dataset's `symbols` argument takes one symbol as text or several as a list. Spell each symbol
as Massive does. Daily bars also take `dm.ALL` for every symbol, and each dataset's page says
whether it does.

Next, a list of three stocks over a longer range.

<a id="compare-stocks"></a>

### Date ranges: compare three stocks' price returns over a year

Compare the price returns of Apple, Microsoft, and Nvidia over a year from their daily bars,
with Stocks Basic:

```python
import deephaven_massive as dm
from deephaven import agg

m = dm.MassiveSession()
year = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    "2025-11-03", "2026-10-30"
)
returns = year.agg_by(
    [agg.sorted_first("Date", "Start = Close"), agg.sorted_last("Date", "End = Close")],
    by="Symbol",
).update_view("ReturnPct = 100 * (End / Start - 1)")
```

`returns` gives each stock its first close, its last close, and `ReturnPct`.

Dates are `yyyy-MM-dd` text, and a range includes both of its dates. For stocks, a date means
the trading day in New York. Every `Date` column holds the same text, which sorts by day and
joins across tables.

Next, the same kind of table as a chart.

<a id="chart-closes"></a>

### Charts: plot a stock's daily closes for a year

Draw Nvidia's daily closes for a year on Stocks Basic, charted with
[Deephaven Express](https://deephaven.io/core/plotly/docs/):

```python
import deephaven.plot.express as dx
import deephaven_massive as dm

m = dm.MassiveSession()
nvda = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2025-11-03", "2026-10-30")
closes = dx.line(nvda, x="Date", y="Close")
```

`closes` is a line chart of Nvidia's close on each trading day of the year. Deephaven Express
charts any Deephaven table, so every table in this tutorial can be a chart, live ones included.

Next, a table that keeps changing.

<a id="follow-the-news"></a>

### Live tables: poll for the latest news headlines

Follow the news on Apple, Nvidia, and Tesla with Stocks Basic. Ask Massive for each stock's
newest headlines every five minutes:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
headlines = m.reference.news(["AAPL", "NVDA", "TSLA"], limit=10).poll(300)
latest = headlines.view(["PublishedUtc", "Symbol", "Title"]).sort_descending("PublishedUtc")
```

`latest` starts with up to ten articles per stock, newest first. Massive updates news hourly, so
a new article arrives at the first poll after Massive lists it.

`poll()` returns a live table. At each poll it asks Massive again, and a news table adds only
the articles it hasn't seen. `latest` updates with it.

The poll keeps running until you stop it, which the next example does.

<a id="stop-live-tables"></a>

### Stopping live tables: stop the news feed and keep a snapshot

Stop Example 5's news poll when you're done with it, still on Stocks Basic. In the console where
it runs, close its session and keep a copy of the headlines:

```python continues
m.close()
kept = latest.snapshot()
```

`headlines` and `latest` stop updating and show an error saying the session was closed, and
they keep their rows. `kept` is a static copy that you can filter and join like any table.

`m.close()` stops every live table of the session, and a closed session reads nothing more. To
stop one table and keep the session, call `m.release(table)`. The table then stops once no panel
or other table uses it.

Next, a live table whose stocks you change while it runs.

<a id="drive-with-a-watchlist"></a>

### Loading data for new symbols without restarting: add and remove stocks from a live table of previous closes

Keep a live table of previous closes that follows your list of stocks through the day, with
Stocks Basic:

```python
import deephaven_massive as dm
from deephaven import input_table, new_table

m = dm.MassiveSession()
stocks = new_table({"Symbol": ["AAPL", "MSFT", "AMZN", "GOOGL"]})
watchlist = input_table(init_table=stocks, key_cols="Symbol")
closes = m.stocks.previous_day_bar(watchlist).poll(60)
```

`closes` has a row for each stock with its previous trading day's bar. Each poll sends one
request per stock, so four stocks polled once a minute stay within Basic's five requests a
minute.

`watchlist` is a Deephaven [input table](https://deephaven.io/core/docs/how-to-guides/input-tables/),
which you edit by hand in its panel or in code. By hand, type a symbol into a new row of the
`watchlist` panel, press **Enter**, and click **Commit**. In code, run this in the same console
to add Nvidia and remove Microsoft:

```python continues
watchlist.add(new_table({"Symbol": ["NVDA"]}))
watchlist.delete(new_table({"Symbol": ["MSFT"]}))
```

At the next poll, Nvidia's row arrives and Microsoft's leaves, and the table keeps running.

Wherever a dataset takes symbols, it also takes a table with a `Symbol` column, and it follows a
live one as it changes.

Next, settings for the paid plans.

<a id="what-your-plan-allows"></a>

### Plans and settings: load your settings from a file and read a delayed market snapshot

Read a snapshot of three stocks with Stocks Starter or higher. First, save your plan's setting in
a settings file. Starter and Developer are 15 minutes late, so their file accepts delayed data.
Advanced and Business are real-time. In a new terminal, run the commands for your plan.

Starter or Developer:

```bash
mkdir -p ~/massive
echo "delayed_subscriptions: [stocks]" > ~/massive/settings.yaml
```

Advanced or Business:

```bash
mkdir -p ~/massive
echo "delayed_subscriptions: []" > ~/massive/settings.yaml
```

Then load the file and read the snapshot:

```python
import deephaven_massive as dm

config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
m = dm.MassiveSession(config)
snapshot = m.stocks.snapshots(["AAPL", "MSFT", "NVDA"]).fetch()
```

`snapshot` has one row per stock, with columns such as `LastTradePrice`, `DayClose`, and
`TodaysChangePerc`. On Starter or Developer without the setting, the same call raises
`dm.EntitlementError`.

Settings belong to `dm.MassiveConfig`, and a file keeps them in one place for every console and
script. Your keys stay in the environment, never in the file.
[The MassiveConfig class](#massive-config) lists the settings.

Next, a live table that streams instead of polling.

<a id="stream-minute-bars"></a>

### Streaming: stream minute bars for the stocks on your list

Watch the latest minute bar of each stock on your list as it arrives, with Stocks Starter or
higher:

```python
import deephaven_massive as dm
from deephaven import input_table, new_table

m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
stocks = new_table({"Symbol": ["AAPL", "MSFT", "NVDA"]})
watchlist = input_table(init_table=stocks, key_cols="Symbol")
latest = m.stocks.bars(watchlist, dm.Timespan.MINUTE).last_by()
```

`latest` gets a row for each stock with its first minute bar, and the row changes as each minute
ends.

`last_by()` reads Massive's stream instead of polling. The stream follows the watchlist. Adding
a stock subscribes to it, and deleting one unsubscribes it and removes its row.

Massive sends a minute bar only for a minute with trades. Stocks trade in pre-market, regular,
and after-hours sessions, so a table you start overnight stays empty until pre-market trading.
Crypto trades around the clock, and on Currencies Starter or higher,
`m.crypto.bars("X:BTCUSD", dm.Timespan.MINUTE).last_by()` streams at any hour.

Next, a calculation on the live bars.

<a id="change-since-close"></a>

### Live calculations: track each stock's change since the previous close

Find how far each stock has moved today, with Stocks Starter or higher. Join the live minute
bars to the previous closes, and compute the change:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
symbols = ["AAPL", "MSFT", "NVDA"]
closes = m.stocks.previous_day_bar(symbols).fetch()
latest = m.stocks.bars(symbols, dm.Timespan.MINUTE).last_by()
changes = (
    closes.view(["Symbol", "PrevClose = Close"])
    .natural_join(latest, on="Symbol", joins="Close")
    .update_view("ChangePct = 100 * (Close / PrevClose - 1)")
)
```

`changes` has a row for each stock with a previous close. Its `Close` and `ChangePct` fill in
with the stock's first minute bar and change with every bar after it.

Deephaven recomputes `changes` whenever `latest` changes. Every table you build on a live table
stays live, and the code is the same as for static tables.

Next, other ways to keep a stream's rows.

<a id="table-shapes"></a>

### Table types: keep the latest bar, the last 30 minutes, or the whole stream since you started

Stream SPY's minute bars into three tables that keep different rows, with Stocks Starter or
higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
bars = m.stocks.bars("SPY", dm.Timespan.MINUTE)
newest = bars.last_by()
last_30 = bars.ring(30)
since_start = bars.append_only()
```

| Table | Method | Keeps |
|---|---|---|
| `newest` | `last_by()` | One row per symbol: its newest bar |
| `last_30` | `ring(30)` | The newest 30 bars: for a stock that trades every minute, the last 30 minutes |
| `since_start` | `append_only()` | Every bar since you ran the code |

All three share one stream from Massive, so pick the method by the rows you need to keep.

`since_start` begins when you run the code. Next, fill in the bars before that.

<a id="backfill-then-live"></a>

### Backfill: start with today's minute bars, then continue live

Start with every minute bar SPY has had today, then add each new one as it ends, with Stocks
Starter or higher:

```python
import deephaven_massive as dm
from deephaven import merge
from deephaven.time import dh_today, to_j_time_zone

m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
bars = m.stocks.bars("SPY", dm.Timespan.MINUTE)
live = bars.append_only()
past = bars.history(dh_today(to_j_time_zone("America/New_York")))
spy = merge([past, live]).last_by(["Symbol", "TimestampStart"])
```

`spy` holds SPY's minute bars for the day so far, and it grows as each minute ends. On a day the
market is closed, it stays empty.

To backfill a live table, merge its `history()` with it and keep one row per bar with `last_by`.
Put the live table last, so its bar wins where both have one. `history()` with only a start date
reads through now.

Next, a live chart and a live table on one screen.

<a id="dashboard"></a>

### Dashboards: show a live chart and table together with deephaven.ui

Put a live chart and a live table side by side with
[deephaven.ui](https://deephaven.io/core/ui/docs/), on Stocks Starter or higher. The table is
Example 10's change table:

```python
import deephaven.plot.express as dx
import deephaven_massive as dm
from deephaven import ui

m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
symbols = ["AAPL", "MSFT", "NVDA"]
closes = m.stocks.previous_day_bar(symbols).fetch()
bars = m.stocks.bars(symbols, dm.Timespan.MINUTE)
changes = (
    closes.view(["Symbol", "PrevClose = Close"])
    .natural_join(bars.last_by(), on="Symbol", joins="Close")
    .update_view("ChangePct = 100 * (Close / PrevClose - 1)")
)
chart = dx.line(bars.append_only(), x="TimestampEnd", y="Close", by="Symbol")
dashboard = ui.dashboard(ui.row(ui.panel(chart, title="Closes"), ui.panel(changes, title="Change")))
```

`dashboard` opens with two panels side by side: a line per stock that grows each minute, and
the change table.

A dashboard arranges panels in rows and columns, and each panel holds any table or chart.
Deephaven's guide to
[creating dashboards](https://deephaven.io/core/ui/docs/creating-layouts/creating-dashboards/)
shows more layouts.

<a id="next-steps"></a>

## Next steps

- Call `help()` on any object for its full documentation, such as `help(m.stocks.bars)`.
- The [dataset index](docs/datasets/README.md) has a page for each of the 112 datasets, with its
  arguments, its columns, and the plans that include it. `dm.datasets()` lists them in the
  console.
- [Troubleshooting](docs/troubleshooting.md) gives each error message and its fix.

<a id="contributing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to set up a development checkout, run the
checks, and propose a change.
