# Tester guide

You test a release of `deephaven-massive` before its users do. You install it as the README
says, run its examples, and leave a test program running against Massive for five trading days.
Everything you find goes on an issue in this repository.

Shell commands run in a terminal, and Python code runs in the **Console** panel of the
Deephaven IDE. Each step says which.

<a id="the-test"></a>

## Plan the test

The test has five parts:

| Part | What you do | What you send |
|---|---|---|
| [Set up the test](#set-up) | Install the library and the test tools, and check your setup | Nothing |
| [Run the examples](#run-the-examples) | Run every example with one command, then run the README tutorial and the examples by hand | Your notes |
| [Run the live checks](#live-run) | Start one program that checks the library against Massive, and check on it daily | The results zip |
| [Report a problem](#report-a-problem) | Report each problem when it happens | A bug-report zip |
| [Finish the test](#finish) | Send your results and clean up | Your notes and the results zip |

A free Massive account is enough to start, and each paid plan your key holds adds the checks
that plan allows.

The live checks need a computer that stays on and awake for the whole test, with 20 GB of free
disk.

Keep notes as you go. In a terminal, make the test folder:

```bash
mkdir -p ~/massive-test
```

Then save this checklist as `~/massive-test/checklist.md` in any text editor. Tick each box as
you finish it, and next to each item, note anything that failed, surprised you, or was unclear:

```markdown
## Set up
- [ ] Installed the library from the README, and noted the release tag
- [ ] Opened my results issue
- [ ] Cloned the test tools, and `tester.py check` says `ready: yes`

## Examples
- [ ] Ran `tester.py examples`, and noted each failure
- [ ] Ran the README tutorial's examples that my plan allows
- [ ] Ran examples 1 to 25 by hand, with the watchlist edits of 20, 21, and 23

## Live checks
- [ ] `live_run.py check` says `ready: yes`
- [ ] Started the run, stopped it with Control-C, and resumed it
- [ ] Checked on the run daily (dates: )
- [ ] Covered five trading days, and on a paid plan one stream day (date: )

## Reports
- [ ] Sent each problem with its bug-report zip
```

<a id="set-up"></a>

## Set up the test

Run the Deephaven server from a Python environment, as the README installs it, or in Docker.
Each step that differs shows both.

<a id="check-what-you-need"></a>

### Prerequisites

You need the README's [prerequisites](../README.md#prerequisites), and uv to run the test tools:

| What | Check it with | Get it from |
|---|---|---|
| uv | `uv --version` | [uv's install guide](https://docs.astral.sh/uv/getting-started/installation/) |
| Docker, only to run the server in Docker | `docker info` | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |

On Windows, run every command inside a WSL 2 terminal, and apply the WSL 2 clock fix that
Deephaven's [pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/)
gives.

<a id="install-the-library"></a>

### Install the library and set your keys

Follow the README's [Install](../README.md#install) section, from
[Sign in to GitHub](../README.md#sign-in-to-github) through
[Set your API keys](../README.md#set-your-api-keys), and stop before the README starts the
server:

- Python environment: do every step, and keep the environment the README names, `~/dh-massive`.
- Docker: skip [Install the library](../README.md#install-the-library). The image you build in
  [Check the setup](#check-the-setup) holds the library.

Your keys are now in your shell's startup file, so every terminal you open has them.

<a id="open-your-issue"></a>

### Open your results issue

Open an [issue](https://github.com/chipkent/deephaven-massive/issues/new) titled
`Test results: <your name>`. You post your checklist and results there, and the maintainer asks
for anything else in its comments.

<a id="copy-the-test-tools"></a>

### Clone the test tools

The test tools come with the code of the release you install. In a new terminal, clone the code
at the newest release tag:

```bash
cd ~/massive-test
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
echo $TAG
git clone --branch "$TAG" https://github.com/chipkent/deephaven-massive.git
cd ~/massive-test/deephaven-massive
uv sync
```

`echo` prints the tag, the version you test. Note it in your checklist. Git says you're in
"detached HEAD" state, which is normal for a tag. Then uv installs the tools.

Leave this clone on its tag until the test ends. The live checks refuse to resume on another
commit.

<a id="check-the-setup"></a>

### Check the setup

The setup check confirms that the server's environment and the test tools hold the same
release, and that Massive accepts your key. In a terminal, run the check for your setup.

Python environment:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/tester.py check
```

Docker, which first builds the image from the clone:

```bash
cd ~/massive-test/deephaven-massive
git archive HEAD | docker build -f docs/tester-docker/Dockerfile --build-arg TESTER_COMMIT=$(git rev-parse HEAD) -t deephaven-massive-test -
uv run python scripts/tester.py check --docker-image deephaven-massive-test
```

The check prints one line per item, each starting `OK`, `WARN`, or `FAIL`, and ends with
`ready: yes`. A `FAIL` line names what's wrong. Fix it, and run the check again. Note each `WARN`
line in your checklist. The check also saves its lines in `~/massive-test/setup-check.txt` for
your bug reports.

<a id="run-the-examples"></a>

## Run the examples

A program runs the 25 test examples first, and saves what each one did. Then, in the Deephaven
IDE, you run the README tutorial and the test examples by hand, and judge what each one shows.

Each test example tries one part of the library. Most run on Basic, Massive's free plan:

| # | Example | Plan |
|---|---|---|
| 1 | Open a session with a debug log | Any plan |
| 2 | Find datasets and their columns | Any plan |
| 3 | Read two weeks of daily bars | Any plan |
| 4 | Summarize bars with table operations | Any plan |
| 5 | Read reference data | Any plan |
| 6 | Poll the market's status | Any plan |
| 7 | Exceed Basic's request limit | Any plan |
| 8 | Read history in the background | Any plan |
| 9 | Compare two stocks' returns over a year | Any plan |
| 10 | Ask for data Basic doesn't include | Any plan |
| 11 | Convert symbols from other sources | Any plan |
| 12 | Check the debug log for your key | Any plan |
| 13 | Chart a moving average | Any plan |
| 14 | Compare company size with return | Any plan |
| 15 | Compute a dividend's yield | Any plan |
| 16 | Chart the 10-year to 2-year Treasury spread | Any plan |
| 17 | Stream live minute bars | Stocks Starter or higher |
| 18 | Backfill today's minute bars | Stocks Starter or higher |
| 19 | Stream crypto trades | Currencies Starter or higher |
| 20 | Edit watchlists that tables follow | Stocks Basic and Futures Basic |
| 21 | Stream a crypto watchlist | Currencies Starter or higher |
| 22 | Stream the trades of the top gainers | Stocks Developer, Advanced, or Business + Expansion |
| 23 | Read history that follows a watchlist | Any plan |
| 24 | Save the diagnostics tables | Any plan |
| 25 | Close a session | Any plan |

Run the paid examples on the free plan too. Massive refuses each one, and checking how the
library reports a refusal is part of the test.

<a id="run-every-example"></a>

### Run every example with one command

The example runner starts its own Deephaven server. Run it before you start yours, because the
two would share your key's request limit. In a terminal, run the examples:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/tester.py examples
```

The runner writes a settings file for your plans, which examples 17, 18, and 22 read. It prints a line per example as it runs,
then `no example failed`, or `failed:` and the examples' names. Each example's output is in
`~/massive-test/examples-output/`, and `summary.txt` there gives each one's verdict:

| Verdict | What you do |
|---|---|
| `passed` | Nothing |
| `refused by your plan` | Nothing, unless your plan includes that example's data |
| `failed` | Report it, as [Report a problem](#report-a-problem) shows |

<a id="start-the-server"></a>

### Start the server

In a new terminal, start the server as the README's
[Start the server](../README.md#start-the-server) step does, saving its output for your bug
reports.

Python environment:

```bash
source ~/dh-massive/bin/activate
deephaven server --jvm-args "-Xmx4g" 2>&1 | tee ~/massive-test/server.log
```

The server prints a line that starts `Deephaven is running at` and opens the Deephaven IDE in
your browser.

Docker:

```bash
mkdir -p ~/massive
docker run --rm --name massive-test -p 10000:10000 \
  -e MASSIVE_API_KEY -e MASSIVE_S3_ACCESS_KEY_ID -e MASSIVE_S3_SECRET_ACCESS_KEY \
  -v "$HOME/massive-test:/root/massive-test" -v "$HOME/massive:/root/massive" \
  -e START_OPTS="-Xmx4g -Dauthentication.psk=massive-test" \
  deephaven-massive-test 2>&1 | tee ~/massive-test/server.log
```

The server prints `Server started on port 10000`. Open
<http://localhost:10000/?psk=massive-test> in your browser, which signs you in. The server runs
as `root`, so the two `-v` options give it your `~/massive-test` and `~/massive` folders at the
same `~` paths. To check, run this in a second terminal, and you should see `checklist.md`:

```bash
docker exec massive-test ls /root/massive-test
```

Keep this terminal open while you test, and press Control-C there to stop the server. If the
server fails to start, find its message in [Troubleshooting](troubleshooting.md#setup-messages).

<a id="run-the-tutorial"></a>

### Run the README tutorial

Run each example of the README's [Tutorial](../README.md#tutorial) that your plan allows,
pasted as it is into the console. The text after each example says what you should see. In your
checklist, note each place where what you see differs from that text.

Whatever your plan, save the settings file of the README's
[settings example](../README.md#what-your-plan-allows), because test examples 17, 18, and 22
read it. On the free plan, save the Starter or Developer file.

<a id="run-by-hand"></a>

### Run the test examples by hand

Paste each example below into the console, as a whole block, and press Enter. A table the
example assigns to a variable opens in a panel with that name. You can run any example again on
its own, except 12 and 24, which read the log and the session that earlier examples made.

Market data changes every day, so your numbers differ from the ones here. Judge each result by
its columns, its rough row count, and the kind of message. In your checklist, note any example
that fails or shows something other than its text says. Examples 13 to 16 are short analyses of
the kind you'd write yourself, so try them with your own symbols and dates too.

Examples 20, 21, and 23 make a watchlist that you edit by hand in its panel, or in code, as the
README's [watchlist example](../README.md#drive-with-a-watchlist) shows.

#### 1. Open a session with a debug log

Turn on the library's debug log, and open a session on your key. Every bug report includes this
log, `~/massive-test/massive-debug.log`:

```python
import os

import deephaven_massive as dm

dm.enable_debug_logging(os.path.expanduser("~/massive-test/massive-debug.log"))

m = dm.MassiveSession()
print(dm.__version__)
print(m)
```

You should see the library's version without its leading `v`, such as `0.1.0rc1`, then a line
such as `<MassiveSession #12 open>`. The number counts the sessions this server has opened.

The examples open their sessions as the README's do. On Basic, a session slows to five
requests a minute after Massive's first rate-limit error, as the README's
[MassiveConfig](../README.md#massive-config) table says, and example 7 checks it.

#### 2. Find datasets and their columns

List every dataset, and the columns of a query, without a request to Massive:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

catalog = dm.datasets()
bars = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY)
print(bars)
bar_columns = bars.columns()
```

You should see the line `m.stocks.bars(['AAPL', 'MSFT', 'NVDA'], dm.Timespan.DAY)` and two
panels. `catalog` has a row per dataset, with the methods that read it, and `bar_columns` has a
row per column of daily bars, with its description.

#### 3. Read two weeks of daily bars

It reads three stocks, with one request to Massive for each:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday, wait=True
)
print(daily.size)
```

`wait=True` makes `history()` wait for the rows, so `daily.size` counts them. You should see a
number from 24 to 30, and a `daily` panel with eight to ten bars per stock, oldest first. Its
`Date` column holds each trading day as text, such as `2026-10-30`. Hover over the `Close`
header to see the column's description.

#### 4. Summarize bars with table operations

Add a column, keep one stock's rows, and summarize each stock, with Deephaven's table
operations:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession()

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday
)
ranges = daily.update("Range = High - Low").view(["Symbol", "Date", "Close", "Range"])
nvda = ranges.where("Symbol == `NVDA`")
per_symbol = daily.agg_by(
    [agg.avg("AverageClose = Close"), agg.max_("HighestHigh = High"), agg.count_("Days")],
    by=["Symbol"],
)
```

You should see `daily`, and three panels built from it with table operations, which send
Massive no requests. In `per_symbol`, each stock's `Days` equals its number of rows in
`daily`.

#### 5. Read reference data

Read three companies' details, the market's status, the coming market holidays, and two weeks
of Treasury yields:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

details = m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
companies = details.view(["Symbol", "Name", "PrimaryExchange", "ListDate", "MarketCap"])

market_now = m.reference.market_status().fetch()
holidays = m.reference.market_holidays().fetch()
two_weeks_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=14)
yields = m.economy.treasury_yields(start=two_weeks_ago).fetch()
```

You should see these panels:

| Panel | What it holds |
|---|---|
| `companies` | A row per company: its name, its exchange, its listing date, and its market value in dollars |
| `market_now` | One row, whose `Market` reads `open`, `closed`, or `extended-hours` |
| `holidays` | The coming market holidays |
| `yields` | US Treasury yields, a row per day |

#### 6. Poll the market's status

Poll the market's status every 60 seconds, and open three of the session's diagnostics tables:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
```

You should see `live_status`, whose `ServerTime` changes about once a minute, and these
diagnostics tables:

| Panel | What you should see |
|---|---|
| `rate_limits` | A row per subscription, plus `other` |
| `streams` | A row for `live_status`, with `State` `RUNNING`: the table is updating |
| `connections` | No streaming connection: a poll doesn't stream |

#### 7. Exceed Basic's request limit

Poll six stocks every 30 seconds, more than Basic allows, then release the table:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

# 12 Stocks requests a minute: six stocks every 30 seconds, against a limit of 5.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
rate_limits = m.rate_limits()
streams = m.streams()

# The table keeps updating while its panel is open, and stops once you close the panel.
m.release(paced)
```

Watch `rate_limits` and `paced` for two minutes. On Basic, once Massive refuses a request, the
`stocks` row of `rate_limits` shows `RequestsPerMinute` 5 and `LimitSource` `LEARNED`, which
means the library learned the limit from Massive's refusal. The row reads
`PACED` while requests wait for a free slot. `paced` gets its six rows either way. On a paid
Stocks plan, nothing is refused.

Then close the `paced` panel. The table stops, and the `stocks` row returns to `READY`.
`m.release()` lets the library stop a table once no panel or other table uses it.

#### 8. Read history in the background

Start reading a year of two stocks. `history()` returns at once and fills the table when the
read is done:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago)
streams = m.streams()
```

You should see an empty `year` panel. Its row in `streams` reads `LOADING` while the read runs,
then `DONE`, and the rows arrive together.

#### 9. Compare two stocks' returns over a year

The code computes AAPL's and MSFT's daily returns, growth, and volatility, and charts the growth:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg
from deephaven.plot.figure import Figure
from deephaven.updateby import delta

import deephaven_massive as dm

m = dm.MassiveSession()

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
pair = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago)
returns = (
    pair.sort(["Symbol", "Date"])
    .update_by(ops=[delta(cols=["Change = Close"])], by=["Symbol"])
    .update(["Return = Change / (Close - Change)"])
)
first_close = pair.sort(["Symbol", "Date"]).first_by(["Symbol"]).view(
    ["Symbol", "FirstClose = Close"]
)
growth = returns.natural_join(first_close, on=["Symbol"], joins=["FirstClose"]).update(
    ["Growth = Close / FirstClose"]
)
summary_year = growth.agg_by(
    [
        agg.first("FirstClose = Close"),
        agg.last("LastClose = Close"),
        agg.std("DailyVolatility = Return"),
        agg.count_("TradingDays"),
    ],
    by=["Symbol"],
).update(
    [
        "TotalReturn = LastClose / FirstClose - 1",
        "AnnualVolatility = DailyVolatility * Math.sqrt(252)",
    ]
)

growth_chart = (
    Figure()
    .plot_xy(series_name="AAPL", t=growth.where("Symbol == `AAPL`"), x="TimestampStart", y="Growth")
    .plot_xy(series_name="MSFT", t=growth.where("Symbol == `MSFT`"), x="TimestampStart", y="Growth")
    .show()
)
```

`summary_year` has a row per stock: `TotalReturn` is the year's price change as a fraction, so
0.15 means 15%, and `AnnualVolatility` is the annualized standard deviation of daily returns.
`growth_chart` draws two lines that start at 1.0.

#### 10. Ask for data Basic doesn't include

Ask Basic for the latest trade, live minute bars, and bars from 2015. Basic includes none of
them, and the code prints how Massive refuses each. `wait=True` makes `fetch()` and `history()`
wait for Massive's answer, so a refusal raises in the call:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
streams = m.streams()
connections = m.connections()

try:
    m.stocks.last_trade(["AAPL"]).fetch(wait=True)
    print("the latest trade: no error, so your plan includes it")
except dm.MassiveError as error:
    print(f"the latest trade: {type(error).__name__}: {error}")

try:
    live_minute_bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    print("live minute bars: opened; Massive's answer shows in the streams panel")
except dm.MassiveError as error:
    print(f"live minute bars: {type(error).__name__}: {error}")

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history("2015-01-05", "2015-01-09", wait=True)
    print("bars from 2015: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"bars from 2015: {type(error).__name__}: {error}")
```

On the free plan, you should see three lines:

| Line | What it says |
|---|---|
| The latest trade | `EntitlementError`, quoting Massive's refusal |
| Live minute bars | That they opened. Seconds later, their `stocks.bars` row in `streams` reads `FAILED`. |
| Bars from 2015 | `EntitlementError`: your plan's history doesn't reach 2015 |

A line says `no error` on a paid plan that includes that data. An `EntitlementError` that names
`delayed_subscriptions` means your plan is delayed, and the README's settings example shows the
setting.

For each message, including the `Error` in `streams`, note whether it told you what went wrong
and what to do. Note any that blamed your key when the cause was your plan. For the live minute
bars, note whether their `Error` quotes Massive's refusal of `AM.AAPL` or says Massive rejected
the key, or whether `connections` reconnects again and again instead.

#### 11. Convert symbols from other sources

The functions convert one symbol at a time or a whole table, and need no session:

```python
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS))

yahoo_symbols = new_table([string_col("Symbol", ["BRK-B", "AAPL", "VOD.L"])])
converted = dm.canonicalize_symbols(
    yahoo_symbols, dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
)
```

You should see `C:EURUSD`, `X:BTCUSD`, `I:SPX`, `BRK.B`, and `BRK-B`, each on its own line: with
no convention named, a stock ticker comes back as written. `converted` holds `BRK.B`, `AAPL`, and
a null `Symbol` for `VOD.L`: Yahoo's dot marks a listing outside the US, and Massive's marks a
share class.
The debug log records a `WARNING` for `VOD.L`.

#### 12. Check the debug log for your key

Count the debug log's lines, its warnings and errors, and the copies of your key in it:

```python
import os

with open(os.path.expanduser("~/massive-test/massive-debug.log"), encoding="utf-8") as log_file:
    text = log_file.read()
lines = text.splitlines()
print(len(lines), "lines")
print(sum(" WARNING " in line or " ERROR " in line for line in lines), "warnings and errors")
print(text.count(os.environ["MASSIVE_API_KEY"]), "copies of the key")
```

You should see `0 copies of the key`. Any other count is a serious problem. Report it, and
don't send the log. The warnings count is above 0, because example 11 logs a warning for the
value it couldn't convert.

#### 13. Chart a moving average

It charts NVDA's daily closes over six months, with their 20-day average:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_avg_tick

import deephaven_massive as dm

m = dm.MassiveSession()

six_months_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=182)
nvda_daily = (
    m.stocks.bars("NVDA", dm.Timespan.DAY)
    .history(six_months_ago)
    .update_by(ops=[rolling_avg_tick(cols=["Average20 = Close"], rev_ticks=20)], by=["Symbol"])
)
nvda_chart = (
    Figure()
    .plot_xy(series_name="Close", t=nvda_daily, x="TimestampStart", y="Close")
    .plot_xy(series_name="20-day average", t=nvda_daily, x="TimestampStart", y="Average20")
    .show()
)
```

You should see `nvda_daily` with an `Average20` column, and `nvda_chart`, whose average line is
smoother than the closes.

#### 14. Compare company size with return

Join each company's market value to its return over three months, on the `Symbol` column:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession()

symbols = ["AAPL", "JPM", "XOM"]
quarter_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=91)
closes = m.stocks.bars(symbols, dm.Timespan.DAY).history(quarter_ago)
returns_3m = (
    closes.sort(["Symbol", "Date"])
    .agg_by([agg.first("FirstClose = Close"), agg.last("LastClose = Close")], by=["Symbol"])
    .update("Return3m = LastClose / FirstClose - 1")
)
facts = m.reference.ticker_details(symbols).fetch().view(["Symbol", "Name", "MarketCap"])
size_and_return = facts.natural_join(returns_3m, on=["Symbol"], joins=["Return3m"]).update(
    "MarketCapBillions = MarketCap / 1e9"
)
```

You should see `size_and_return` with a row per company, its market value from reference data
beside its return from bars.

#### 15. Compute a dividend's yield

Divide each Apple dividend of the past year by the close on the trading day before the stock
went ex-dividend:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
year_ago = yesterday - timedelta(days=364)
aapl_daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history(year_ago, yesterday)
dividends = (
    m.stocks.dividends("AAPL", start=year_ago, end=yesterday)
    .fetch()
    .view(["Symbol", "ExDividendDate", "CashAmount"])
)
dividend_yield = dividends.aj(
    aapl_daily, on=["Symbol", "ExDividendDate > Date"], joins=["PriceDate = Date", "Close"]
).update("PaymentYieldPercent = 100 * CashAmount / Close")
```

You should see `dividend_yield`, with a row per dividend, each with its `PriceDate` and
`PaymentYieldPercent`.

#### 16. Chart the 10-year to 2-year Treasury spread

The spread is the 10-year yield minus the 2-year yield, over two years:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure

import deephaven_massive as dm

m = dm.MassiveSession()

two_years_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=730)
curve = (
    m.economy.treasury_yields(start=two_years_ago)
    .fetch()
    .update(["Spread = Yield10Year - Yield2Year", "Day = parseInstant(Date + `T00:00:00 ET`)"])
)
curve_chart = Figure().plot_xy(series_name="10y - 2y", t=curve, x="Day", y="Spread").show()
```

You should see `curve` with a `Spread` column, and `curve_chart`, whose line falls below zero
wherever the curve was inverted.

#### 17. Stream live minute bars

Keep the latest minute bar of five stocks, with a paid Stocks plan. The session loads the
settings file you saved for the README tutorial:

```python
import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))

latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
streams = m.streams()
```

While the stocks trade, `latest_bars` has a row per stock, replaced as each minute ends. After
trading stops, it keeps each stock's last bar, and a table you start while the market is closed
stays empty until the stocks trade.

#### 18. Backfill today's minute bars

Start with AAPL's minute bars from earlier today, and add each new minute as it ends, with the
plan and settings file of example 17:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
# Open the stream before reading history, so no minute falls between the two.
aapl_live = aapl_minutes.append_only()
aapl_past = aapl_minutes.history(session_day)
aapl_today = merge([aapl_past, aapl_live]).last_by(["Symbol", "TimestampStart"])
```

You should see `aapl_today` with a row for each minute AAPL has traded today, and a new row as
each minute ends. Where history and the stream both have a minute, the table keeps the stream's
bar. On a day AAPL doesn't trade, the table is empty.

#### 19. Stream crypto trades

Stream Bitcoin and Ether trades, and count them by the minute, with a paid Currencies plan.
Crypto trades around the clock, so this example works at any hour:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

crypto_trades = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
crypto_latest = crypto_trades.last_by("Symbol")
trades_per_minute = crypto_trades.update("Minute = lowerBin(Timestamp, MINUTE)").count_by(
    "Trades", by=["Symbol", "Minute"]
)
```

You should see `crypto_latest` with a row per pair, its latest trade, and `trades_per_minute`,
which gains a row per pair each minute.

#### 20. Edit watchlists that tables follow

Make two watchlists, and three tables that follow them. `watched_bars` polls each stock's
previous day bar every minute, `watched_dividends` reads each stock's dividends, and
`watched_contracts` reads up to 10 contracts of each futures product:

```python
from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
reads = m.symbol_reads()
streams = m.streams()

# One Stocks request per stock for the dividends, then one per stock a minute for the bars.
stocks = new_table({"Symbol": ["JPM", "XOM", "TSLA"]})
stock_watchlist = input_table(init_table=stocks, key_cols="Symbol")
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
watched_dividends = m.stocks.dividends(stock_watchlist).fetch()

# One Futures request for each product you add.
products = new_table({"ProductCode": ["ES"]})
product_watchlist = input_table(init_table=products, key_cols="ProductCode")
watched_contracts = m.futures.contracts(product_code=product_watchlist, limit=10).fetch()
```

You should see `stock_watchlist` with three stocks, `watched_bars` with a row per stock, and
`watched_dividends` with each stock's dividends. `product_watchlist` has one row, and
`watched_contracts` has up to 10 ES contracts. Then edit the watchlists, in their panels or in
code. These lines, run in the same console, make the first and fourth edits:

```python
stock_watchlist.add(new_table({"Symbol": ["AAPL"]}))
product_watchlist.add(new_table({"ProductCode": ["NQ"]}))
```

1. In `stock_watchlist`, add `AAPL`. AAPL's dividends join `watched_dividends`, and an `AAPL`
   row joins `watched_bars` at the next poll.
2. Delete `TSLA`. TSLA's dividends leave `watched_dividends` at once, and its row leaves
   `watched_bars` at the next poll.
3. In `reads`, the rows of `watched_dividends` show `DONE`, read, for JPM, XOM, and AAPL, and
   `REMOVED` for TSLA, which left the watchlist.
4. In `product_watchlist`, add `NQ`. Up to 10 NQ contracts join `watched_contracts` together.
5. Delete `NQ`. Every NQ contract leaves `watched_contracts` at once, and the ES contracts stay.

#### 21. Stream a crypto watchlist

You need a paid Currencies plan, and it works at any hour:

```python
from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
streams = m.streams()

pairs = new_table({"Symbol": ["X:ETHUSD", "X:LINKUSD"]})
crypto_watchlist = input_table(init_table=pairs, key_cols="Symbol")
latest_trades = m.crypto.trades(crypto_watchlist).last_by()
```

You should see `crypto_watchlist` with two pairs, and `latest_trades` with a row per pair after
its first trade. Then edit the watchlist, and watch `latest_trades` and the `crypto.trades` row
of `streams`:

1. Add `X:SOLUSD`. An `X:SOLUSD` row joins `latest_trades` with its first trade, and the
   stream's `Symbols` lists it.
2. Delete `X:SOLUSD`. Its row leaves `latest_trades` at once, and `Symbols` drops it.
3. Add `btcusd`, in lower case. No row appears, and the stream's `Error` names `btcusd` among the
   pairs Massive's crypto ticker list doesn't hold. The other pairs keep streaming.
4. Delete `btcusd` and add `X:BTCUSD`. The `Error` clears, and an `X:BTCUSD` row arrives with
   its first trade.
5. Add `X:ZZZZUSD`, a pair Massive doesn't list. No row appears, and the `Error` names it. Delete
   it, and the `Error` clears.

#### 22. Stream the trades of the top gainers

Poll the day's top gainers every minute, and stream the trades of whichever stocks are on that
list. You need a Stocks plan that streams trades: Developer, Advanced, or Business +
Expansion:

```python
import deephaven_massive as dm

# Load the settings file that the README's settings example saves for your plan.
m = dm.MassiveSession(dm.MassiveConfig.from_file("~/massive/settings.yaml"))
streams = m.streams()

gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
gainer_trades = m.stocks.trades(gainers).last_by()
```

While the market is open, you should see `gainers` with up to 20 rows, and `gainer_trades` with
a row per gainer after its first trade. When a stock joins or leaves the gainers at a poll, it
joins or leaves `gainer_trades` and the `Symbols` of the `stocks.trades` row in `streams`.

#### 23. Read history that follows a watchlist

Each stock you add costs one Stocks request, for a month of its daily bars:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import input_table, new_table

import deephaven_massive as dm

m = dm.MassiveSession()
reads = m.symbol_reads()

stocks = new_table({"Symbol": ["AAPL", "MSFT"]})
watchlist = input_table(init_table=stocks, key_cols="Symbol")
month_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=30)
followed = m.stocks.bars(watchlist, dm.Timespan.DAY).history(month_ago)
```

You should see `followed` with about 20 bars each of AAPL and MSFT, and `reads` with a `DONE`
row for each. Then edit `watchlist`:

1. Add `NVDA`. NVDA's bars join `followed` in one update. In `reads`, NVDA goes from `QUEUED`,
   waiting, to `LOADING`, being read, to `DONE`, and its `Rows` equals its bars in `followed`.
2. Delete `NVDA`. Its bars leave `followed` in one update, and its row in `reads` shows
   `REMOVED`.
3. Add `NVDA` again. It's read again, with a newer `ReadAt` in `reads`.

<a id="24-save-the-diagnostics"></a>

#### 24. Save the diagnostics tables

Save the four diagnostics tables of the session in `m` as files, for a bug report. `m` holds the
session the last example opened:

```python
import os

from deephaven.pandas import to_pandas

diagnostics = os.path.expanduser("~/massive-test/diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {
    "streams": m.streams(),
    "connections": m.connections(),
    "rate_limits": m.rate_limits(),
    "symbol_reads": m.symbol_reads(),
}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
```

You should see `saved to` and the folder, such as `/Users/you/massive-test/diagnostics`.

#### 25. Close a session

Close a session, and check that it refuses new work. Then read a week of bars in a `with`
block, which closes its session when the block ends. Closing a session stops its reads, so the
read in the block passes `wait=True` to finish first:

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession()
closing_status = m.reference.market_status().poll(60)
m.close()
print(m)

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history(yesterday - timedelta(days=6), yesterday)
except dm.MassiveError as error:
    print(f"MassiveError: {error}")

# A with block closes its session when the block ends.
with dm.MassiveSession() as m2:
    last_week = m2.stocks.bars("NVDA", dm.Timespan.DAY).history(
        yesterday - timedelta(days=6), yesterday, wait=True
    )
print(m2, last_week.size)
```

You should see three lines like these, with your own session numbers and a row count from 3 to
5:

```text
<MassiveSession #7 closed>
MassiveError: MassiveSession #7 is closed
<MassiveSession #8 closed> 5
```

`closing_status` shows an error saying the session was closed. The other examples' sessions
stay open, and their tables keep updating.

<a id="live-run"></a>

## Run the live checks

`scripts/live_run.py` runs every check your plans allow, at set New York times, and records
Massive's answers for the maintainer. You start it once and leave it running for at least five
trading days. It checks your plans when it starts and each trading morning, so you can change
plans during the test.

Keep your key free of other programs while the run goes: a live check can't stream while another
program holds the key's connection. Two kinds of check keep the key busy for many hours:

| Check | When, in New York time | Which plans |
|---|---|---|
| A stream day: a trading day of live data | A plan check at 03:15, then streams from 03:45 to 20:30, on a day the program picks | Any paid plan |
| A stream night: Bitcoin, Ether, and the ES and NQ futures overnight | 16:30 to 09:30 the next morning, on an evening before a full trading day | A paid Currencies or Futures plan |

<a id="check-the-computer"></a>

### Check the computer

Stop every other use of your key first, the Deephaven server included. Then, in a terminal, run
the program's check, which writes nothing:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/live_run.py check
```

You should see a line per item of the computer, each starting `OK`, `WARN`, or `FAIL`, then your
plan for each subscription, and `ready: yes`. A `FAIL` line names what's wrong. The disk fails
below 20 GB free, and the memory warns below 8 GB.

The program keeps the computer awake while it runs, but closing a laptop's lid still puts it to
sleep. On Windows, also turn off sleep in Windows' power settings.

<a id="start-the-run"></a>

### Start the run

In the same terminal, start the run:

```bash
uv run python scripts/live_run.py run --out ~/massive-live --any-time
```

`--any-time` starts the checks that need no market hours at once, instead of at 11:30 New York
time. It counts only the first time you give it for a results folder.

You should see `results folder:` and the folder, then a line as each group of checks starts and
ends, each line led by the New York time. When nothing is due, the program packs a zip of the
results so far, prints `idle: nothing more to run until` and the time, and waits. Leave it
running.

<a id="stop-and-resume"></a>

### Stop and resume the run

The run must survive a stop, and this step checks that it does. After the first groups have
started, press Control-C once. You should see `interrupted; to resume, run:` and the resume
command, which restarts the run after any stop. Run it now, from the same folder:

```bash
uv run python scripts/live_run.py run --out ~/massive-live
```

You should see `resuming`, and every check that passed stays passed.

| What happened | What you do |
|---|---|
| Control-C, a closed terminal, a crash, or a restart of the computer | In a new terminal, go to `~/massive-test/deephaven-massive` and run the resume command. |
| You changed your plan | Nothing. The next morning's plan check sees it. To make the run see it at once, stop and resume it. |
| You got S3 keys for the first time | Add them to your startup file as the README's [Set your API keys](../README.md#set-your-api-keys) shows, then stop the run with Control-C and resume it from `~/massive-test/deephaven-massive` in a new terminal. |
| A new release comes out | Nothing. Keep testing the release you installed, unless a comment on your issue asks you to switch. |

<a id="look-in-once-a-day"></a>

### Check the run daily

Check on the run once a day, in a new terminal. The `status` command changes nothing:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/live_run.py status --out ~/massive-live
```

You should see `latest probe:` with your plans, the next stream day and night, every check with
its status, and a table of counts per group. Most statuses need nothing from you:

| Status | What you do |
|---|---|
| `passed` or `not run` | Nothing |
| `skipped:` and a reason, or `not started (optional)` | Nothing. Your plan lacks that check, or its market is closed, and a check skipped for the moment runs again. |
| `refused` | Nothing. Massive refused data its documentation says your plan includes, and the maintainer reads it in the results. |
| `failed` or `error` | Nothing. A failed check runs once more by itself, and the maintainer reads both attempts in the results. |

Note each date you checked, and the date of the stream day, in your checklist.

<a id="pack-the-results"></a>

### Pack the results

Pack the results when a comment on your issue asks, and when you finish. In the run's terminal,
press Control-C, then pack:

```bash
uv run python scripts/live_run.py pack --out ~/massive-live
```

You should see `wrote` and the zip's path, `~/massive-live/live-results-<date>.zip`. Each zip
holds everything so far, so send only the newest. If `pack` finds a key in a file, it writes no
zip and names the files. Put that list of names, without the files, on your issue. Then resume
the run, unless you're finishing.

<a id="upgrade-check"></a>

### Run the upgrade check when asked

The upgrade check tests that the library stops limiting a subscription to Basic's five requests
a minute once you upgrade it. Run it only when a comment on your issue asks:

1. Stop the run with Control-C, before you upgrade.
2. In the same terminal, start the check with the subscription you'll upgrade: `stocks`,
   `options`, `indices`, `currencies`, or `futures`. For Stocks:

   ```bash
   uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live
   ```

3. When you see `Now upgrade stocks on the dashboard, then press Enter`, upgrade the
   subscription on Massive's dashboard, and press Enter. You should see `upgrade check: passed`,
   or `failed` with a reason.
4. Resume the run.

<a id="live-run-problems"></a>

### Fix a live run that fails

| You see | What you do |
|---|---|
| `the API key was rejected` | Copy the key from Massive's dashboard into your startup file again, then stop the run with Control-C and resume it from `~/massive-test/deephaven-massive` in a new terminal. |
| `a run or an upgrade check is using ...` | Another `run` is already going. Use it, or stop it first. |
| The plan check shows a lower plan than you bought | Stop and resume the run, which checks again. If the plan is still lower, say so on your issue. |
| A gap in a stream day or night, in the zip's `REPORT.md` | Keep the computer awake with the lid open. When a stream day's or night's gaps add up to more than 30 minutes, it fails, and the program plans one more. |
| Anything else | Post the `status` output on your issue, and attach the newest zip. |

<a id="report-a-problem"></a>

## Report a problem

Report each problem when it happens:

- a step that fails, hangs, or shows something other than the step says
- an error message you can't understand or act on
- an instruction that was unclear
- a console command that seems to hang
- a `WARNING` in the debug log about a bar stamped at an unexpected time

<a id="which-errors"></a>

### Decide whether to report an error

The type of an error says whether the library, your plan, or the call caused it:

| Error | What it means | Report it? |
|---|---|---|
| A message that says `This is a bug in deephaven-massive` | A bug in the library | Yes |
| `EntitlementError` | Massive refused the request, and the message quotes it | If the message was unclear, or your plan includes the data |
| `AuthenticationError` | The key is missing, or Massive rejected it or your S3 keys | If the message was unclear |
| Another `MassiveError` | A condition the message explains, such as a closed session | If the message didn't say what happened or what to do |
| `ValueError` or `TypeError` | The call was wrong, and the message says how to fix it | If the message didn't help you fix it |
| Any other error, a Java error, a hang, or a crash | A failure the library didn't catch | Yes |

<a id="make-the-report"></a>

### Make the report

A report is an issue with a zip of your logs, your keys removed:

1. In the console where the problem happened, run [example 24](#24-save-the-diagnostics) at
   once, to save the diagnostics tables. If your session isn't named `m`, change `m` in it to
   your session's name. Take a screenshot of anything on screen that looks wrong.
2. In a text editor, fill in this template, and save it as `~/massive-test/bug-report.md`:

   ```markdown
   # <one line: what went wrong>

   ## What I did
   The code, or the example's number, and the steps before it.

   ## What I expected

   ## What happened
   The full error text the console shows, or what the table showed.

   ## When
   The date, time, and time zone.

   ## My setup
   - Plan of each subscription:
   - Server: Python environment or Docker
   ```

3. In a terminal, build the zip:

   ```bash
   cd ~/massive-test/deephaven-massive
   uv run python scripts/tester.py bug-report
   ```

   You should see `wrote` and the zip's path, `~/massive-test/bug-report-<time>.zip`. The zip
   shows each of your keys as `***`. If a key still shows up in a file, the command writes
   nothing and names the files. Put that list of names, without the files, on the issue.
4. Open an [issue](https://github.com/chipkent/deephaven-massive/issues/new), paste your report,
   and attach the zip and the screenshots.

The zip holds your report, the debug log, the server's output, and what the setup check, the
example runner, and example 24 saved. It also describes your computer, naming the keys you set
but never their values.

<a id="finish"></a>

## Finish the test

The test ends when the live checks have covered five trading days and, if your key holds a paid
plan, one stream day run to its end at 20:30. Then send your results and clean up:

1. Stop the live run with Control-C in its terminal, and stop any Deephaven server the same way.
2. In the live run's terminal, [pack the results](#pack-the-results).
3. On your results issue, paste your checklist, and attach the newest `live-results-<date>.zip`
   and any bug-report zips you haven't sent.
4. Once the maintainer closes your results issue, delete the test's folders in a terminal:

   ```bash
   rm -rf ~/massive-test ~/massive-live
   ```

   Run it again with `sudo` in front if `rm` reports a permission error. With Docker, also
   delete the image with `docker rmi deephaven-massive-test`.
