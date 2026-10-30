# Tester guide

You are among the first people outside the project to use `deephaven-massive`. This guide
takes you through every step: getting the code, installing it, using it, leaving a long test
running against Massive's servers, and sending back what you found. You need no earlier
knowledge of Deephaven, Massive, git, or Python environments. Every step says what to type,
what you should see, and what to do if you see something else.

## The brief

**What you do, and for how long.**

| Phase | What you do | Your time | Computer time |
|---|---|---|---|
| 1. Set up | Install the tools, get the code twice (once for the console, once for the live run), check both | 45 to 90 minutes | the same |
| 2. Console exercises | Paste 22 short examples into Deephaven's web console and judge what they show | 90 minutes | the same |
| 3. Live run | Start one program that runs every check your Massive plan allows | 30 minutes to start, then 10 minutes a day | days to weeks, unattended |
| 4. Send back | Pack the results and send them, with your notes | 15 minutes | a few minutes |

**What you send back.**

1. The live run's zip, `~/massive-live/live-results-<date>.zip`, made by
   `uv run python scripts/live_run.py pack --out ~/massive-live` (section 3.7).
2. One bug report zip, `~/massive-test/bug-report-<time>.zip`, for each problem you report,
   made by `uv run python scripts/tester.py bug-report` (part 4).
3. Your filled-in checklist and notes (the checklist below).

Send them as a GitHub issue on `chipkent/deephaven-massive`, with the zips attached, or by
email to the owner, at the address in your invitation, if GitHub refuses the upload.

**Known issue: do not report it.** A fix is on the way: closing a session (`m.close()`) can
crash the Deephaven server when one live table takes its symbols from another live table. No
exercise does this. If you try it on your own and the server exits, restart it and carry on.

**Waits on the free plan.** The free plan allows 5 requests a minute per subscription, so
some commands pause while the library waits for a free slot. No live-run check waits more
than about a minute on a free plan; a check that would need longer needs a paid plan and is
skipped without one. A console command that waits more than a few minutes is worth
reporting.

**Everything else is worth reporting**: a step that fails, hangs, or shows something other
than this guide says; an error message you cannot understand or act on; a step whose
instructions were unclear. "The instructions were unclear" is a finding, not a failure on
your part.

## Quick start

For when you have done it once and want the commands. Every command runs in a terminal on
macOS or Linux; `<TESTER_VERSION>` is the version the owner gave you, such as `v0.1.0rc1`.

```bash
# Phase 1: set up (sections 1.1 to 1.7)
mkdir -p ~/massive-test && cd ~/massive-test
git clone --branch <TESTER_VERSION> https://github.com/chipkent/deephaven-massive.git
cd ~/massive-test/deephaven-massive && uv sync
uv venv --seed --python 3.12 ~/massive-test/dh-venv
~/massive-test/dh-venv/bin/pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@<TESTER_VERSION>"
printf 'Massive API key: '; read -rs MASSIVE_API_KEY; echo; export MASSIVE_API_KEY
uv run python scripts/tester.py check

# Phase 2: the console (section 1.8, then part 2)
~/massive-test/dh-venv/bin/deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=massive-test" 2>&1 | tee ~/massive-test/server.log
# open http://localhost:10000/?psk=massive-test and paste the examples

# Phase 3: the live run (part 3), from ~/massive-test/deephaven-massive
uv run python scripts/live_run.py check
uv run python scripts/live_run.py run --out ~/massive-live --any-time
uv run python scripts/live_run.py status --out ~/massive-live
uv run python scripts/live_run.py pack --out ~/massive-live

# A problem (part 4)
uv run python scripts/tester.py bug-report
```

## Checklist

Copy this list into a file, `~/massive-test/checklist.md`, and tick each box as you go. Next
to each item, note anything that surprised you.

```markdown
## Setup
- [ ] 1.1 Tools installed: git, GitHub CLI, Java 17 or later, uv
- [ ] 1.2 Repository access works (git ls-remote prints a commit)
- [ ] 1.3 Massive key made and set; S3 keys too, if your plan has them
- [ ] 1.4 Source checkout of <TESTER_VERSION> made, uv sync done
- [ ] 1.5 Console install made in ~/massive-test/dh-venv
- [ ] 1.6 tester.py check says ready: yes
- [ ] 1.7 Deephaven web IDE open
## Console exercises
- [ ] 01 session          - [ ] 02 datasets         - [ ] 03 daily bars
- [ ] 04 first queries    - [ ] 05 reference data   - [ ] 06 refreshing tables
- [ ] 07 pacing           - [ ] 08 release          - [ ] 09 background download
- [ ] 10 year analysis    - [ ] 11 plan refusals    - [ ] 12 symbols
- [ ] 13 debug log        - [ ] 14 to 17 try your own (free plan)
- [ ] 18 to 20 try your own (paid plans only)       - [ ] 21 save diagnostics
- [ ] 22 finish           - [ ] tester.py examples ran all of them at once
## Live run
- [ ] live_run.py check says ready: yes
- [ ] first run started with --any-time
- [ ] Control-C and resume tested
- [ ] status read once a day; dates noted
- [ ] pack done; zip sent
## Reports
- [ ] each problem sent with its bug report zip
```

---

# Part 1: Set up

## 1.0 Words this guide uses

- **Terminal**: the program where you type commands. On macOS it is the Terminal app, in
  Applications, Utilities. Type a command and press Return to run it.
- **Massive** ([massive.com](https://massive.com), formerly Polygon.io) sells market data:
  stock, option, index, futures, currency, and crypto prices. It sells one *subscription* per
  market (Stocks, Options, Indices, Currencies, Futures), each on a *plan*: the free
  **Basic** plan, or a paid one (Starter, Developer, Advanced, Business). Basic allows **5
  requests a minute per subscription**, has no live data, and keeps two years of history.
- **API key**: the password that lets a program use your Massive account.
- **Deephaven** ([deephaven.io](https://deephaven.io)) is a data engine for tables that
  change. You run it as a *server* on your computer and use it through a web page, the *web
  IDE*, whose *console* runs Python code you paste.
- **Table**: like a spreadsheet, with named columns. A *static* table never changes; a
  *refreshing* table updates while you watch.
- **The library**, `deephaven-massive`, runs inside Deephaven and turns Massive's data into
  tables. A *session* (`m = dm.MassiveSession()`) is your connection to Massive.
- **git** copies the project's code from GitHub; a *tag*, such as `v0.1.0rc1`, names one fixed
  version.
- **Virtual environment**: a private folder of Python packages, so this test cannot disturb
  anything else on your computer.
- **uv**: the tool the project uses to run its own programs in the right environment.

## 1.1 Install the tools

You need git, the GitHub CLI (`gh`), Java 17 or later, and `uv`, plus about 25 GB of free
disk and 8 GB of memory. `uv` brings its own Python, so you need not install one.

**macOS.**

1. Install Homebrew, the macOS package manager, if `brew --version` says "command not found":
   follow the one command on [brew.sh](https://brew.sh), then open a new terminal.
2. Install the tools:

   ```bash
   brew install git gh uv
   brew install --cask temurin@21
   ```

**Linux (Ubuntu or Debian).**

```bash
sudo apt update
sudo apt install -y git gh openjdk-21-jdk curl
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If `apt` has no `gh`, follow [cli.github.com](https://cli.github.com/). After installing `uv`,
open a new terminal.

**Windows.** Install WSL2 with Ubuntu ([Microsoft's
guide](https://learn.microsoft.com/windows/wsl/install)), open the Ubuntu terminal, and
follow the Linux steps there. Do everything in this guide inside that Ubuntu terminal.

**Check.** Run each line; each should print a version:

```bash
git --version
gh --version
java -version
uv --version
```

You should see Java 17 or later (21 is ideal). If `java -version`
prints "No Java runtime present" or a version below 17, Java is missing: repeat the install
step, open a new terminal, and check again. **Record** each version in your checklist.

## 1.2 Get access to the code

The code is in a private GitHub repository, `chipkent/deephaven-massive`. The owner has
invited your GitHub account.

1. Accept the invitation: open <https://github.com/notifications>, or the invitation email,
   and click **Accept**. Then open <https://github.com/chipkent/deephaven-massive>: you should
   see the project's files.
2. Log in from the terminal, and let git use that login:

   ```bash
   gh auth login
   gh auth setup-git
   ```

   Choose `GitHub.com`, then `HTTPS`, answer `Yes` to "Authenticate Git with your GitHub
   credentials?", and log in with a web browser.
3. Check:

   ```bash
   git ls-remote https://github.com/chipkent/deephaven-massive.git <TESTER_VERSION>
   ```

   You should see one line: a 40-character commit id and the tag's name. If git asks for a
   password or says `Repository not found`, repeat steps 1 and 2.

## 1.3 Get your Massive keys

1. Create an account at <https://massive.com>. The free plan is enough for parts 1 and 2;
   the owner tells you whether to buy anything for part 3.
2. On the dashboard, open **API Keys** and copy your key.
3. **Keep the key private**: never paste it into a report, a screenshot, or a chat.

Programs read the key from an *environment variable*, a setting the terminal passes to every
program it starts. Set it without leaving it in your shell's history: run this line, paste
the key (nothing appears), and press Return.

```bash
printf 'Massive API key: '; read -rs MASSIVE_API_KEY; echo; export MASSIVE_API_KEY
```

Check it without printing it: `echo "${#MASSIVE_API_KEY} characters"` should print a number
above 20. **A variable lives only in the terminal where you set it**: set it again in every
new terminal you use for this test.

**Flat-file keys, only on a paid plan.** Paid plans include *flat files*, Massive's bulk daily
files, which need a second pair of keys shown on the same dashboard page: an *S3 access key
id* and an *S3 secret access key*. If your dashboard shows them, set them the same way:

```bash
printf 'S3 access key id: '; read -rs MASSIVE_S3_ACCESS_KEY_ID; echo; export MASSIVE_S3_ACCESS_KEY_ID
printf 'S3 secret access key: '; read -rs MASSIVE_S3_SECRET_ACCESS_KEY; echo; export MASSIVE_S3_SECRET_ACCESS_KEY
```

## 1.4 Setup one: the source checkout

You need two copies of the same version. The first is the project's source, which runs the
setup check, the live run, and the bug reports. Replace `<TESTER_VERSION>` with the owner's
tag in each command.

```bash
mkdir -p ~/massive-test
cd ~/massive-test
git clone --branch <TESTER_VERSION> https://github.com/chipkent/deephaven-massive.git
cd ~/massive-test/deephaven-massive
uv sync
```

You should see `Cloning into 'deephaven-massive'...`, a note that you are in "detached HEAD"
state (that is expected for a tag), and then `uv` installing packages, which takes a few
minutes the first time. Every later command marked "from the checkout" runs in
`~/massive-test/deephaven-massive` and starts with `uv run`; never run `python` there without
`uv run`.

**Do not change this checkout during the test.** The live run records the commit it started
on and refuses to resume on another one: it stops with "... was started on commit ...: to
resume, run git checkout ...", so every result in one folder comes from one version.

## 1.5 Setup two: the console install

The second copy is the library installed the way a user installs it, into the Python that runs
the Deephaven server. It goes in a virtual environment in `~/massive-test/dh-venv`, which `uv`
makes with its own copy of Python 3.12 (downloaded the first time), so the Python your computer
came with does not matter:

```bash
uv venv --seed --python 3.12 ~/massive-test/dh-venv
~/massive-test/dh-venv/bin/pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@<TESTER_VERSION>"
```

This downloads about 400 MB and takes a few minutes. It should end with a line that starts
`Successfully installed` and names `deephaven-server-42` and `deephaven-massive-0.1.0`.
**Record** how long it took and every line that says `ERROR` or `WARNING`.

If you would rather run Deephaven in Docker, use [appendix A](#appendix-a-the-docker-console-setup)
instead of this section.

## 1.6 Check both setups

From the checkout, with your key set:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/tester.py check
```

It changes nothing. It checks Java, `uv`, the checkout's version, the console install, that
both copies hold the same commit, your free disk and memory, and your key, which it tries with
a few requests to Massive. You should see one line per item, each starting `OK`, `WARN`, or
`FAIL`, such as:

```text
OK   java: Java 21
OK   uv: uv 0.9.5
OK   commit: 1a2b3c..., tree clean
OK   version: the checkout is on v0.1.0rc1
OK   console: deephaven-massive 0.1.0, deephaven-core 42.7, in /Users/you/massive-test/dh-venv/bin/python
OK   same commit: both are 1a2b3c...
OK   key: Massive accepted the API key
OK   plan stocks: basic, history from 2024-10-30
...
ready: yes
```

A `FAIL` line says how to fix it; fix it and run the check again. A `WARN` line is worth noting
but does not stop you. With Docker, add `--docker-image deephaven-massive-test`.

## 1.7 Start Deephaven and open the web IDE

In a terminal where your key is set:

```bash
~/massive-test/dh-venv/bin/deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=massive-test" 2>&1 | tee ~/massive-test/server.log
```

`-Xmx4g` gives the server 4 GB of memory; `-Dauthentication.psk=massive-test` sets the web
page's password to `massive-test`; `tee` keeps a copy of the server's output for bug reports.
The first start takes up to a minute. You should see:

```text
Server started on port 10000
Deephaven is running at http://localhost:10000/?psk=massive-test with authentication type io.deephaven.authentication.psk.PskAuthenticationHandler
Press Control-C to exit
```

Leave this terminal open: Control-C stops the server. Open a second terminal for anything
else, and set your key there too.

Open <http://localhost:10000/?psk=massive-test> in your browser. You should see the web IDE
with a **Console** panel. If the page does not load, check that the terminal shows "running
at"; if another program uses port 10000, use `--port 10010` and open
`http://localhost:10010/?psk=massive-test`.

---

# Part 2: Console exercises

## 2.0 How the console works

- Paste a whole code block into the input box at the bottom of the **Console** panel and press
  **Enter**. **Shift+Enter** adds a line without running.
- Printed text appears above the input box. Each table you assign to a variable opens in a
  panel named after it; if one does not, click its name in the console output.
- Hover over a column header to see what the column means.
- Run the examples in order, in one console: each uses variables from the ones before.
- Refreshing tables update between commands, not while one is still running.
- **Errors** appear in red, starting `Error in Python interpreter`. The line that matters is
  the last line of the Python part, just before the first line that starts with `at `: it
  names the error's type and its message.

Each example is also a file in the checkout, under
`~/massive-test/deephaven-massive/docs/tester-examples/`, named in its heading. Instead of
pasting, you can run a file in the console:

```python
exec(open(os.path.expanduser("~/massive-test/deephaven-massive/docs/tester-examples/03_daily_bars.py")).read())
```

**All at once.** After you have done the exercises by hand, run them all with one command, from
the checkout, in a terminal with your key set. It starts its own Deephaven server, runs every
example in order as the console would, and saves what each printed, raised, and made in
`~/massive-test/examples-output/`, with a one-line verdict per example in `summary.txt`:
`passed`, `refused by your plan` (expected for the paid-plan examples on a free key), or
`failed` (report it). Close the web console's session first (example 22), since both use the
same key's limits.

```bash
uv run python scripts/tester.py examples
```

Market data changes daily, so judge the *shape* of each result (the columns, roughly how many
rows, the kind of message), not exact numbers. For each example, record pass or fail, how
long it took, and anything unclear.

## 2.1 The exercises

### 1. Start a session (`01_start_a_session.py`)

```python
# Turns on the debug log and opens a session on your Massive key. You should see the
# library's version, such as 0.1.0, then <MassiveSession #1 open>.
import os

import deephaven_massive as dm

FOLDER = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
os.makedirs(FOLDER, exist_ok=True)
LOG = os.path.join(FOLDER, "massive-debug.log")
dm.enable_debug_logging(LOG)

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
print(dm.__version__)
print(m)
```

**You should see** `0.1.0` and `<MassiveSession #1 open>`. Creating the session sends nothing
to Massive. `rest_basic_subscriptions=set(dm.Subscription)` tells the library that all five
of your subscriptions are on the free Basic plan, so it spaces your requests to 5 a minute per
subscription from the start rather than waiting for Massive to refuse the sixth; on a paid
plan, leave out the subscriptions you pay for. The debug log, `~/massive-test/massive-debug.log`,
records every decision and request of the library, never your key.

**If not:** an error ending `AuthenticationError: no Massive API key ...` means the server
did not get your key: stop it (Control-C), set the key in that terminal (1.3), and start it
again (1.7).

### 2. List the datasets and describe a query (`02_explore_datasets.py`)

```python
# Lists every dataset, then describes the columns of one query without sending a request.
# You should see one line describing the query, and two new tables: catalog, one row per
# dataset, and bar_columns, one row per column of daily stock bars.
catalog = dm.datasets()

bars = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY)
print(bars)
bar_columns = bars.columns()
```

**You should see** this line, and two panels:

```text
<HistoryQuery stocks.bars symbols=('AAPL', 'MSFT', 'NVDA') timespan=Timespan.DAY adjusted=True>
```

`catalog` lists every dataset, what it holds, its *verbs* (the methods that fetch data, such
as `history` and `fetch`), and a link to its columns. `bars` is a *query*: a description of
the data, which sends nothing. `bar_columns` lists its 20 columns with a description of each.

### 3. Download two weeks of daily bars (`03_daily_bars.py`)

A *bar* summarizes trading over a stretch of time: the open, high, low, and close prices and
the volume. A daily bar covers one trading day.

```python
# Downloads two weeks of daily price bars for three stocks: three requests to Massive.
# You should see a number between 24 and 30, and a new table named daily.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

today = datetime.now(ZoneInfo("America/New_York")).date()
last_day = today - timedelta(days=1)
first_day = last_day - timedelta(days=13)

daily = bars.history(first_day, last_day)
print(daily.size)
```

**You should see** a number between 24 and 30, and a panel named `daily`: eight to ten bars
per stock (weekends and holidays have none), oldest first. `Date` is the trading day as text;
the prices are split-adjusted. Hover over the `Close` header: a box describes the column.
This makes three requests and takes a few seconds.

### 4. Your first Deephaven queries (`04_first_queries.py`)

```python
# Adds a column, keeps some rows, and summarizes by stock, with Deephaven's table operations.
# You should see three new tables: ranges, nvda (NVDA's rows only), and per_symbol (one row
# per stock).
from deephaven import agg

ranges = daily.update("Range = High - Low").view(["Symbol", "Date", "Close", "Range"])
nvda = ranges.where("Symbol == `NVDA`")
per_symbol = daily.agg_by(
    [agg.avg("AverageClose = Close"), agg.max_("HighestHigh = High"), agg.count_("Days")],
    by=["Symbol"],
)
```

**You should see** three panels: `ranges`, `nvda` with only NVDA's rows, and `per_symbol` with
one row per stock. Text inside a query is written between back-quotes, as in `` `NVDA` ``.

### 5. Reference data (`05_reference_data.py`)

```python
# Reads facts about companies and markets rather than prices. You should see four new
# tables: companies (three rows), market_now (one row), holidays, and yields (one row per
# business day).
details = m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
companies = details.view(["Symbol", "Name", "PrimaryExchange", "ListDate", "MarketCap"])

market_now = m.reference.market_status().fetch()
holidays = m.reference.market_holidays().fetch()
yields = m.economy.treasury_yields(start=first_day).fetch()
```

**You should see** `companies` with three rows (each company's name, its exchange, `XNAS` for
Nasdaq, its listing date, and its market value in dollars), `market_now` with one row
(`Market` reads `open`, `closed`, or `extended-hours`), `holidays`, and `yields` with US
Treasury yields in percent. Reference and economy requests count against no subscription, so
they do not slow your stock requests.

### 6. Tables that refresh themselves (`06_refreshing_tables.py`)

```python
# Makes a table that asks Massive again every minute, and opens the three diagnostics
# tables. You should see four new tables: live_status, which changes about once a minute,
# and rate_limits, streams, and connections.
live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
```

**You should see** four panels. `live_status` asks Massive again every 60 seconds: its
`ServerTime` changes about once a minute. The other three are the *diagnostics tables*:
`rate_limits` has one row per subscription plus `other`, and for `stocks` shows
`RequestsPerMinute` 5 and `LimitSource` `DECLARED`; `streams` has one row per refreshing table
or background download, here `live_status` as `RUNNING`; `connections` lists live connections
to Massive, none yet.

### 7. Watch the free plan's pace (`07_pacing.py`)

```python
# Asks for more than the free plan allows: the previous day's bar of six stocks every 30
# seconds, 12 Stocks requests a minute against a limit of 5. You should see a new table,
# paced, fill within about a minute, and the stocks row of rate_limits switch between READY
# and PACED.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
```

Watch `rate_limits` and `paced` for two minutes without running anything else. **You should
see** the `stocks` row switch between `READY` and `PACED` (waiting for a free slot), and
`paced` get its six rows within about a minute. The debug log gains a `WARNING` that the
table's requests are paced at 5 a minute.

### 8. Stop a refreshing table (`08_release_a_table.py`)

```python
# Stops the table of the previous example. You should see its row in streams change to
# RELEASED within a few seconds, and the stocks row of rate_limits settle back to READY.
m.release(paced)
```

**You should see** `paced`'s row in `streams` change to `RELEASED` within a few seconds.

### 9. Download in the background (`09_background_download.py`)

A long download normally holds the console until it finishes. With `wait=False` it returns at
once and fills the table when the download is done.

```python
# Starts a one-year download of two stocks in the background, so the console stays free.
# You should see a new table, year, with no rows at first; its row in streams reads
# LOADING, then DONE, and the rows appear all at once.
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(
    today - timedelta(days=365), wait=False
)
```

**You should see** an empty `year` panel, and in `streams` a row for it whose `State` reads
`LOADING`, then `DONE`, at which point the rows appear all at once.

### 10. A first analysis (`10_year_analysis.py`)

Run this once `year`'s row reads `DONE`.

```python
# Compares the year of the two stocks: daily returns, growth, volatility, correlation, a
# chart, and a saved file. Run it once year's row in streams reads DONE. You should see a
# correlation between 0 and 1, new tables (pair, returns, growth, summary_year, and
# side_by_side), a chart named growth_chart, and the file growth.parquet in your test folder.
from deephaven import parquet
from deephaven.pandas import to_pandas
from deephaven.plot.figure import Figure
from deephaven.updateby import delta

pair = year.snapshot()
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

side_by_side = (
    returns.where("Symbol == `AAPL`")
    .view(["Date", "AAPL = Return"])
    .natural_join(
        returns.where("Symbol == `MSFT`").view(["Date", "MSFT = Return"]),
        on=["Date"],
        joins=["MSFT"],
    )
    .where(["!isNull(AAPL)", "!isNull(MSFT)"])
)
frame = to_pandas(side_by_side)
print(round(frame["AAPL"].corr(frame["MSFT"]), 2))

growth_chart = (
    Figure()
    .plot_xy(series_name="AAPL", t=growth.where("Symbol == `AAPL`"), x="TimestampStart", y="Growth")
    .plot_xy(series_name="MSFT", t=growth.where("Symbol == `MSFT`"), x="TimestampStart", y="Growth")
    .show()
)

parquet.write(growth, os.path.join(FOLDER, "growth.parquet"))
```

**You should see** a number between 0 and 1, usually between 0.4 and 0.8: how closely the two
stocks' daily moves agreed. `summary_year` has two rows; `TotalReturn` is the year's price
change as a fraction (0.15 is 15%), and `AnnualVolatility`, typically between 0.15 and 0.45,
is how much the price swings. `growth_chart` shows two lines that both start at 1.0. The file
`~/massive-test/growth.parquet` holds the `growth` table.

### 11. What the free plan refuses (`11_plan_refusals.py`)

```python
# Asks for three things the free Basic plan does not include, and prints how each was
# refused. You should see three lines, each starting with what was asked, then
# EntitlementError and the reason.
try:
    m.stocks.last_trade(["AAPL"]).fetch()
    print("the latest trade: no error, so your plan includes it")
except dm.MassiveError as error:
    print(f"the latest trade: {type(error).__name__}: {error}")

try:
    m.stocks.bars(["AAPL"]).append_only()
    print("live minute bars: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"live minute bars: {type(error).__name__}: {error}")

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history("2015-01-05", "2015-01-09")
    print("bars from 2015: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"bars from 2015: {type(error).__name__}: {error}")
```

**You should see** three lines, each with `EntitlementError`: a refusal because of your plan.
The first quotes Massive's refusal of the latest trade; the second says the stocks plan has no
WebSocket access, Massive's live connection; the third says your plan does not reach back to
2015. On a paid plan, some lines say "no error" instead. **Record** whether each message told
you what went wrong and what you could do, and whether any suggested your key was wrong when
it was really your plan.

### 12. Convert symbols (`12_symbols.py`)

The library takes each symbol exactly as Massive writes it: `AAPL`, `BRK.B`, `C:EURUSD` for the
euro against the dollar, `X:BTCUSD` for Bitcoin, `I:SPX` for the S&P 500. Other sources write
symbols differently, and these functions convert them.

```python
# Converts symbols written in other conventions into Massive's, one at a time and as a
# table. You should see C:EURUSD, X:BTCUSD, I:SPX, and BRK.B, then a ValueError that names
# the conventions to choose from, and a new table, converted, whose bad row is empty.
from deephaven import new_table
from deephaven.column import string_col

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))

try:
    dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS)
except ValueError as error:
    print(f"ValueError: {error}")

watchlist = new_table([string_col("Symbol", ["BRK-B", "AAPL", "not a ticker"])])
converted = dm.canonicalize_symbols(
    watchlist,
    dm.AssetClass.STOCKS,
    convention=dm.SymbolConvention.YAHOO,
    on_error=dm.OnError.LOG,
)
```

**You should see** `C:EURUSD`, `X:BTCUSD`, `I:SPX`, `BRK.B`, then a line starting
`ValueError: 'BRK-B' is not a Massive stock ticker: it uses another convention's separators`
that names the conventions to choose from. `converted` holds `BRK.B`, `AAPL`, and an empty
row: with `dm.OnError.LOG` a symbol that cannot be converted becomes empty and is logged,
instead of stopping everything (`dm.OnError.RAISE`) or passing silently (`dm.OnError.IGNORE`).

### 13. Check the debug log (`13_check_the_log.py`)

```python
# Counts the debug log's lines, its warnings and errors, and any copy of your key. You
# should see hundreds of lines, a few warnings and errors, and 0 copies of the key.
with open(LOG, encoding="utf-8") as log_file:
    text = log_file.read()
lines = text.splitlines()
print(len(lines), "lines")
print(sum(" WARNING " in line or " ERROR " in line for line in lines), "warnings and errors")
print(text.count(os.environ["MASSIVE_API_KEY"]), "copies of the key")
```

**You should see** a number of lines, a few warnings and errors (from examples 7 and 11), and
`0 copies of the key`. **Any other number of copies is a serious problem: report it, and do
not send the log.**

## 2.2 Try your own

These show what the library is for. Examples 14 to 17 work on the free plan; 18 to 20 need a
paid plan's live data, and on a free key each ends with an `EntitlementError`, which is
expected. Change the symbols and dates, and combine them with what you learned above.

### 14. A moving average (`14_moving_average.py`)

```python
# Charts NVDA's daily close over six months with its 20-day moving average: one Stocks
# request. You should see a new table, nvda_daily, and a chart, nvda_chart, with two lines.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_avg_tick

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

### 15. Company size beside return (`15_size_and_return.py`)

```python
# Joins company facts to prices: each company's market value beside its return over the
# last three months. Three Stocks requests and three reference requests. You should see a
# new table, size_and_return, with one row per company.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

### 16. Dividend yield (`16_dividend_yield.py`)

```python
# Finds each Apple dividend of the past year and the closing price on the trading day
# before it, to give each payment's yield: two requests. You should see a new table,
# dividend_yield, with about four rows.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
aapl_daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history(year_ago)
dividends = (
    m.stocks.dividends("AAPL")
    .fetch()
    .where(f"ExDividendDate >= `{year_ago}`")
    .view(["Symbol", "ExDividendDate", "CashAmount"])
)
dividend_yield = dividends.aj(
    aapl_daily, on=["Symbol", "ExDividendDate > Date"], joins=["PriceDate = Date", "Close"]
).update("YieldPercent = 100 * CashAmount / Close")
```

### 17. The yield curve (`17_yield_curve.py`)

```python
# Charts the gap between the 10-year and 2-year US Treasury yields over two years: one
# request, counted against no subscription. You should see a new table, curve, and a chart,
# curve_chart, whose line falls below zero when the curve is inverted.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure

two_years_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=730)
curve = (
    m.economy.treasury_yields(start=two_years_ago)
    .fetch()
    .update(["Spread = Yield10Year - Yield2Year", "Day = parseInstant(Date + `T00:00:00 ET`)"])
)
curve_chart = Figure().plot_xy(series_name="10y - 2y", t=curve, x="Day", y="Spread").show()
```

### 18. Live minute bars, paid plans (`18_live_minute_bars.py`)

```python
# Needs a paid Stocks plan (Starter or higher). Keeps the latest minute bar of five stocks,
# updated live. You should see a new table, latest_bars, with one row per stock while the
# market is open; on a free key you see an EntitlementError instead.
latest_bars = m.stocks.bars(["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]).last_by()
```

### 19. History and live in one table, paid plans (`19_history_plus_live.py`)

```python
# Needs a paid Stocks plan (Starter or higher). Joins today's minute bars from history with
# the live ones that follow, into one table that keeps growing. You should see a new table,
# aapl_today, with a row per minute since the open, and a new row each minute.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
aapl_today = merge([aapl_minutes.history(session_day), aapl_minutes.append_only()])
```

### 20. Live crypto, paid plans (`20_live_crypto.py`)

```python
# Needs a paid Currencies plan (Starter or higher). Streams Bitcoin and Ether trades and
# counts them by the minute. You should see two new tables: crypto_latest, one row per coin
# with its latest trade, and trades_per_minute, which grows each minute. Crypto trades
# around the clock, so this works at any hour.
crypto_trades = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
crypto_latest = crypto_trades.last_by("Symbol")
trades_per_minute = crypto_trades.update("Minute = lowerBin(Timestamp, MINUTE)").count_by(
    "Trades", by=["Symbol", "Minute"]
)
```

## 2.3 Save the diagnostics, and finish

### 21. Save the diagnostics (`21_save_diagnostics.py`)

Run this whenever something goes wrong, before anything else, and before you close the session:
the bug report (part 4) collects these files.

```python
# Saves the session's diagnostics tables as files for a bug report. Run it right after a
# problem, before you close the session. You should see the folder the files went to.
import os

from deephaven.pandas import to_pandas

FOLDER = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
diagnostics = os.path.join(FOLDER, "diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {"streams": m.streams(), "connections": m.connections(), "rate_limits": m.rate_limits()}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
```

### 22. Close the session (`22_finish.py`)

```python
# Closes the session, shows that a closed session refuses new work, and downloads once more
# in a with block, which closes its session by itself. You should see
# <MassiveSession #1 closed>, a MassiveError that says the session is closed, then
# <MassiveSession #2 closed> and a number between 3 and 5.
m.close()
print(m)

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history(first_day, last_day)
except dm.MassiveError as error:
    print(f"MassiveError: {error}")

config = dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription))
with dm.MassiveSession(config) as m2:
    last_week = m2.stocks.bars("NVDA", dm.Timespan.DAY).history(
        last_day - timedelta(days=6), last_day
    )
print(m2, last_week.size)
```

**You should see** `<MassiveSession #1 closed>`, a line `MassiveError: MassiveSession #1 is
closed`, then `<MassiveSession #2 closed>` and a number between 3 and 5. Closing stops every
refreshing table of the session: `live_status` now shows an error that says the session was
closed, and keeps its rows. Static tables such as `daily` are unaffected.

Stop the server with Control-C in its terminal when you are done with the console.

---

# Part 3: The live run

## 3.1 What it is

The library's own tests run against imitation servers. Some questions only Massive's real
servers can answer: what a timestamp's unit is, where a day's bar starts, what Massive sends
when a plan lacks some data, whether the library keeps up with a full trading day of live
data. The *live-run program*, `scripts/live_run.py`, runs every such check your key allows,
records Massive's answers, and adapts when your plan changes. You start it once and leave it
running for days or weeks.

- **It finds out your plan by itself.** It *probes* the key (asks Massive what each
  subscription allows) when it starts and every trading morning. Buy, upgrade, or cancel at
  any time: it notices at the next probe, or at once if you restart it.
- **A skip is not a failure.** A check your plan cannot run is recorded as
  `skipped: needs <plan>`, such as `skipped: needs Stocks Starter`. A check fails only when
  something goes wrong that your plan should allow.
- **Waits on the free plan.** No check waits more than about a minute on a free plan; checks
  that need longer are skipped as needing a paid plan.
- **It runs by New York time.** Each trading day: a probe at 08:00, the opening check at 09:28,
  the main checks from 11:30 (after Massive publishes the previous day's files), and the
  connection-limit checks from 16:15. When your plan allows live data, it plans a *stream day*:
  03:45 to 20:30 New York time, streaming a full trading day, with the probe at 03:15; the next
  morning it measures throughput from 09:15 to 11:00.

## 3.2 Before you start

1. Use a computer that can stay on and awake, on a stable network, with 20 GB free.
2. In one terminal, go to the checkout and set your keys (1.3), S3 keys included if you have
   them:

   ```bash
   cd ~/massive-test/deephaven-massive
   ```

3. **Stop every other use of your key**: the Deephaven server of part 2, and any other
   program. Live checks fail or wait when something else holds the key's connection.
4. Check:

   ```bash
   uv run python scripts/live_run.py check
   ```

   It writes nothing. **You should see** one line per item of the machine (Java, `uv`, the
   commit, your keys, the clock, the disk, the memory, keep-awake), then your plan per
   subscription, the flat files your S3 keys reach, and `ready: yes`. A `FAIL` line says how to
   fix it.

## 3.3 The first run

```bash
uv run python scripts/live_run.py run --out ~/massive-live --any-time
```

`--any-time` runs at once the checks that need no market hours, instead of waiting for 11:30.
It works once per results folder; later starts follow the timetable. Every line starts with the
New York time. **You should see** `results folder: /Users/you/massive-live`, your
subscriptions, then a line as each group of checks starts and ends, with how long its requests
may take on the free plan, such as:

```text
10:02 EST start evidence
10:02 EST evidence: 74 checks; on Basic, up to 40 minutes of requests
10:49 EST end evidence
```

When it has nothing more to do until a set time, it prints a line such as
`16:20 EST idle: nothing more to run until Tue 08:00 EST (probe)`, writes a zip of everything
so far, and sleeps. Leave it running: it keeps the computer awake while it runs (closing a laptop's
lid still puts it to sleep).

**Test the resume now.** About ten minutes in, press Control-C once. Within a minute it
prints:

```text
10:12 EST interrupted; to resume, run: uv run python scripts/live_run.py run --out /Users/you/massive-live
```

Run that same command (without `--any-time`). It prints `resuming` and carries on where it
stopped: checks that passed stay passed.

## 3.4 Look in once a day

In a second terminal, from the checkout:

```bash
uv run python scripts/live_run.py status --out ~/massive-live
```

It changes nothing. **You should see** the latest probe and your subscriptions, the next
stream day if one is planned, every check grouped with when its group runs next, each as
`passed`, `failed`, `error`, `refused`, `skipped: needs ...`, or `not run`, and a summary
table:

```text
group | passed | failed | error | refused | skipped | not run
evidence | 52 | 0 | 0 | 0 | 22 | 0
```

`refused` means Massive refused something its own documentation says your plan includes:
that is recorded for the owner, and needs nothing from you. Write to the owner if a check
fails twice.

## 3.5 Stops, restarts, and changes

- **Control-C, a closed terminal, a crash, or a lost network**: run the same `run` command
  again; it resumes.
- **A reboot**: open a terminal, `cd ~/massive-test/deephaven-massive`, set your keys again
  (1.3), and run the same command.
- **A plan change**: do nothing; to make it notice at once, press Control-C and run the same
  command. When you first get S3 keys, it prints a reminder to set them: stop it, set them,
  and start it again.
- **Another commit**: it refuses to resume a folder on a different version. Keep the checkout
  at `<TESTER_VERSION>`; if the owner sends a new version, start a new folder, such as
  `--out ~/massive-live-2`.

## 3.6 While it runs

You are free most of the time. Good uses of it:

- the try-your-own examples (2.2), with a new Deephaven server, but **not while a stream day
  or a connection-limit check runs**, since both use your key's live connection; `status`
  shows when those are planned;
- reading the user guide in `docs/getting-started/` and the column references in
  `docs/datasets/` of the checkout, and noting anything unclear;
- writing up anything from part 2 you have not reported yet.

## 3.7 Pack and send

Whenever the owner asks, and when you stop for good: press Control-C, then, in the same
terminal (it must hold exactly the keys the run used):

```bash
uv run python scripts/live_run.py pack --out ~/massive-live
```

**You should see** `wrote /Users/you/massive-live/live-results-<date>.zip`. Each zip holds
everything so far, so only the newest is kept. Before writing it, `pack` scans every file for
your keys and refuses if it finds one, naming the files; send the owner that list instead. If
it says to set exactly the secrets the run used, set the same keys as when you ran `run`. The
zip holds Massive's market data: send it only to the owner.

## 3.8 The upgrade check, only when the owner asks

It tests that after you upgrade a subscription from the free plan, the library stops limiting
it to 5 requests a minute. Before upgrading, stop the run (Control-C) and run, naming the
subscription (`stocks`, `options`, `indices`, `currencies`, or `futures`):

```bash
uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live
```

After a few minutes it prints `Now upgrade stocks on the dashboard, then press Enter`.
Upgrade, press Enter, and wait up to 15 minutes for `upgrade check: passed`, or `failed` with
a reason. Pressing Enter without upgrading is a rehearsal: it ends
`upgrade check: failed: tier stayed Basic (recorded as a rehearsal)`. Then start `run` again.

## 3.9 When something goes wrong

- **`the API key was rejected`**: copy the key again from the dashboard and set it (1.3).
- **`a run or an upgrade check is using ...`**: another `run` is already going; use it, or
  stop it first.
- **The probe shows a lower plan than you bought**: a purchase can take minutes to reach the
  key; wait ten minutes, then restart the run.
- **A stream day shows a gap**: the computer slept. Keep it awake and the lid open; a gap of
  more than 30 minutes makes the program plan another stream day.
- **Anything else**: send the owner the `status` output and the zip.

---

# Part 4: Reporting a problem

## 4.1 A library bug, or your plan?

The error's type tells you:

| Error | What it means | Report it? |
|---|---|---|
| `EntitlementError` | Massive refused because of your plan: the message names the plan or quotes Massive's refusal | Only if the message was unclear, or your plan should include the data |
| `AuthenticationError` | The key is missing or wrong | Only if the message was unclear |
| `ValueError` or `TypeError` | The call itself was wrong, such as a misspelled symbol; the message says how to fix it | If the message did not help you fix it |
| `MassiveError`, any other error, a Java error, a hang, or a crash | Something went wrong in the library or Massive | **Yes** |

In `tester.py examples`, a refusal shows as `refused by your plan` and anything else as
`failed`. In the live run, a refusal of something your plan should include shows as
`refused`, recorded for the owner.

## 4.2 Make the report

1. **Right away**, in the console, run example 21 (`21_save_diagnostics.py`) to save the
   diagnostics tables. Take a screenshot of anything on screen that looks wrong.
2. Write `~/massive-test/bug-report.md` from the template below.
3. From the checkout, with your key set, bundle everything:

   ```bash
   uv run python scripts/tester.py bug-report
   ```

   It collects your report, the debug log, the server's output, the diagnostics, the
   examples' output, and a description of your machine (operating system, Python, Java,
   versions, which keys are set, never their values), removes your keys from them, scans the
   result for any copy that survived, and writes `~/massive-test/bug-report-<time>.zip`. If it
   finds a key, it writes nothing and names the files instead: send the owner that list.
4. Open an issue at <https://github.com/chipkent/deephaven-massive/issues/new>, paste your
   report, and attach the zip and the screenshots. If GitHub will not take the upload, email
   them to the owner.

## 4.3 The template

```markdown
# <one line: what went wrong>

## What I did
The exact code, pasted (or the example's file name), and the steps before it.

## What I expected

## What happened
The full error text, from `Error in Python interpreter` to the end, or what the table
showed.

## When
Date, time, and time zone (such as 2026-10-30 14:15 America/Chicago).

## My setup
- Plan of each subscription (Basic, Starter, ...):
- Console setup: virtual environment, or Docker
- Version (<TESTER_VERSION>):

## Attachments
- bug-report-<time>.zip
- screenshots, for anything shown on screen
```

---

# Part 5: Finish and clean up

1. Pack and send the live run's zip (3.7) and any open reports.
2. Stop the live run (Control-C) and any Deephaven server.
3. Keep `~/massive-test` and `~/massive-live` until the owner confirms receipt; then delete
   them:

   ```bash
   rm -rf ~/massive-test ~/massive-live
   ```

4. Cancel any paid plan as the owner says; plans renew monthly until cancelled.

---

# Appendix A: The Docker console setup

Instead of the virtual environment of 1.5, you can run the console's Deephaven server in
Docker, from Deephaven's own image, `ghcr.io/deephaven/server`, with the library installed
from your checkout. The checkout (1.4) is still needed. Install Docker Desktop
([docker.com](https://www.docker.com/products/docker-desktop/)), give it at least 6 GB of
memory in its settings, and check that `docker info` prints no error.

1. Build the image from the checkout:

   ```bash
   cd ~/massive-test/deephaven-massive
   git archive HEAD | docker build -f docs/tester-docker/Dockerfile --build-arg TESTER_COMMIT=$(git rev-parse HEAD) -t deephaven-massive-test -
   ```

   The first build downloads Deephaven's image, about 1.3 GB. The image records the commit it
   was built from, which `tester.py check --docker-image deephaven-massive-test` compares with
   the checkout's.

2. Start it, in a terminal where your key is set:

   ```bash
   docker run --rm --name massive-test -p 10000:10000 \
     -e MASSIVE_API_KEY \
     -e MASSIVE_TEST_FOLDER=/massive-test \
     -e START_OPTS="-Xmx4g -Dauthentication.psk=massive-test" \
     -v "$HOME/massive-test:/massive-test" \
     deephaven-massive-test 2>&1 | tee ~/massive-test/server.log
   ```

   `-e MASSIVE_API_KEY` passes your key in without showing it; `MASSIVE_TEST_FOLDER` and `-v`
   make the examples write their files to `~/massive-test` on your computer. After about 30
   seconds you should see
   `Connect automatically to Web UI with http://localhost:10000/?psk=massive-test`. Open that
   address and continue at part 2; paste the examples, since the container cannot see the
   checkout's files.

3. Check the key reached the container, from a second terminal:
   `docker exec massive-test sh -c 'echo "${#MASSIVE_API_KEY} characters"'` should print the
   same count as in 1.3.

Control-C stops the container and removes it; `docker rmi deephaven-massive-test` removes the
image. On Linux, files the container wrote belong to `root`: delete them with `sudo rm`.
