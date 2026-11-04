# Tester guide

You're one of the first people outside the project to use `deephaven-massive`, and your job is
to find what goes wrong before the release. You install the library, try its examples, and
leave it running against Massive's servers for a week. You don't need to know Deephaven,
Massive, git, or Python before you start. The project's owner, who invited you, reads every
report you send.

## Plan your test

The test runs for at least five trading days. The live run, one program that checks the
library against Massive's servers, does most of that work on its own. One of the days must be
a full stream day, when it streams market data from 03:15 to 20:30 New York time.

Keep the computer on and awake the whole time, with 20 GB of free disk. During a stream day, and
an overnight stream night if your plan includes Currencies or Futures, the live run needs your
Massive key to itself. A free Massive account is enough to start, and the owner tells you
whether to buy a paid plan for the live run.

You spend about four hours on the first day, then about 10 minutes a day:

| Part | What you do | Your time |
|---|---|---|
| [Set up](#set-up) | Install the tools, get the code, and check your setup | 45 to 90 minutes |
| [Run the examples](#run-the-examples) | Run the 25 examples with one command, then paste each into the console of the Deephaven IDE and judge what it shows | About 2 hours |
| [Leave the live run going](#leave-the-live-run-going) | Start one program that runs every check your Massive plan allows, and look in once a day | 30 minutes, then 10 minutes a day |
| [Report a problem](#report-a-problem) | Write up each problem as it happens | 15 minutes a problem |
| [Finish the test](#finish) | Pack the results, send them, and clean up | 15 minutes |

You send each file on an issue in the project's GitHub repository. You send a bug report,
`~/massive-test/bug-report-<time>.zip`, as each problem happens. At the end, you send two more
things:

- the live run's results, `~/massive-live/live-results-<date>.zip`
- your checklist, with your notes

Make the test folder with `mkdir -p ~/massive-test`. Then paste this checklist into
`~/massive-test/checklist.md` with any text editor, and tick each box as you go. Next to each
item, note anything that surprised you or took longer than it should:

```markdown
## Set up
- [ ] Installed the tools, and noted their versions
- [ ] Signed in to GitHub, and reached the repository with git
- [ ] Set the Massive API key, and the S3 keys for flat files if your dashboard shows them
- [ ] Copied the code, and noted the release tag
- [ ] Installed Deephaven and the library
- [ ] Ran `tester.py check`, which says `ready: yes`
- [ ] Opened the Deephaven IDE

## Examples
- [ ] Ran `tester.py examples`, and noted any failures
- [ ] Ran examples 1 to 12 by hand
- [ ] Noted how Massive answered the free key's live minute bars in example 10
- [ ] Ran examples 13 to 19 by hand
- [ ] Ran examples 20 to 23 by hand, with their watchlist steps
- [ ] Ran examples 24 and 25 by hand

## Live run
- [ ] Ran `live_run.py check`, which says `ready: yes`
- [ ] Started the first run, and tested Control-C and the resume
- [ ] Read the status once a day, and noted the dates
- [ ] Covered five trading days or more, one of them a full stream day
- [ ] Packed and sent the results

## Reports
- [ ] Sent each problem with its bug-report zip
```

<a id="set-up"></a>

## Set up

Setup takes 45 to 90 minutes, most of it spent waiting for downloads. You'll install the same
release twice. The checkout is a copy of the code, and from it you run the setup check, the
live run, and bug reports. The Deephaven environment is a separate Python environment that
runs Deephaven, with the library installed the way a user installs it. At the end, your
browser shows the Deephaven IDE, ready for the examples.

### Check what you need

You need a computer that runs macOS or Linux, or Windows with WSL 2, with 8 GB of memory and
25 GB of free disk. The test also needs four tools, each with a command that prints its
version:

| What | Why | Check | Get it |
|---|---|---|---|
| git | Copies the code from GitHub | `git --version` | [git-scm.com](https://git-scm.com/downloads) |
| GitHub CLI | Signs you in to GitHub and finds the newest release | `gh --version` | [cli.github.com](https://cli.github.com/) |
| Java 17 or later, with `JAVA_HOME` set | Runs the Deephaven server | `java -version` and `echo $JAVA_HOME` | [Deephaven's install guide](https://deephaven.io/core/docs/getting-started/pip-install/) |
| uv | Runs the project's programs, and installs Python when your computer lacks it | `uv --version` | [uv's install guide](https://docs.astral.sh/uv/getting-started/installation/) |

Deephaven's install guide covers Java on each platform, setting `JAVA_HOME`, and Windows
through WSL 2. On Windows, run every command of the test inside the Ubuntu terminal of WSL 2.
Note each version in your checklist.

<a id="12-get-access-to-the-code"></a>

### 1. Sign in to GitHub

The code is on GitHub, at <https://github.com/chipkent/deephaven-massive>. Open that page, and
you should see the project's files. Then sign in from the terminal, and let git use that
sign-in:

```bash
gh auth login
gh auth setup-git
```

When `gh` asks, choose `GitHub.com`, then `HTTPS`, answer `Yes` to "Authenticate Git with
your GitHub credentials?", and sign in with a web browser. To check that git can reach the
repository, run this command:

```bash
git ls-remote https://github.com/chipkent/deephaven-massive.git HEAD
```

You should see one line, with a 40-character commit id and then `HEAD`.

<a id="2-set-your-massive-key"></a>

### 2. Set your Massive API key

Create an account at <https://massive.com>. The free plan is enough for setup and for every
example, though a few examples show their full result only on a paid plan. The owner tells you
whether to buy a plan for the live run. On Massive's dashboard, open **API Keys** and copy your
API key. Keep it private, and never paste it into a report, a screenshot, or a chat.

Run this line, paste the key when it asks, and press Return. The key doesn't appear as you
paste it, and it stays out of your shell's history:

```bash
printf 'Massive API key: '; read -rs MASSIVE_API_KEY; echo; export MASSIVE_API_KEY
```

To check the key without printing it, run `echo "${#MASSIVE_API_KEY} characters"`. You
should see the length of the key you copied, such as `32 characters`. The key lives only in
the terminal where you set it, so set it again in every new terminal you use for the test.

Paid plans also come with S3 keys, which let the library read Massive's daily flat files. If
your dashboard's keys page shows them, set them the same way:

```bash
printf 'S3 access key id: '; read -rs MASSIVE_S3_ACCESS_KEY_ID; echo; export MASSIVE_S3_ACCESS_KEY_ID
printf 'S3 secret access key: '; read -rs MASSIVE_S3_SECRET_ACCESS_KEY; echo; export MASSIVE_S3_SECRET_ACCESS_KEY
```

<a id="3-copy-the-code"></a>

### 3. Copy the code at the release tag

The checkout holds the code of the release you test, named by its tag. Do this step and the
next in the same terminal, because both use the tag that this command finds:

```bash
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
echo $TAG
```

You should see a tag such as `v0.1.0rc1` (an example). If the owner named another release,
set it instead, as in `TAG=v0.1.0rc1`. Note the tag in your checklist, then copy the code:

```bash
mkdir -p ~/massive-test
cd ~/massive-test
git clone --branch "$TAG" https://github.com/chipkent/deephaven-massive.git
cd ~/massive-test/deephaven-massive
uv sync
```

You should see `Cloning into 'deephaven-massive'...` and a message that you're in "detached
HEAD" state, which is normal for a tag. Then uv installs packages for a few minutes. From now
on, start every program you run in the checkout with `uv run`.

Leave the checkout as it is until the test ends. The live run records the commit it started
on, and it refuses to resume on another one.

<a id="4-install-deephaven"></a>

### 4. Install Deephaven and the library

To run Deephaven in Docker instead, skip steps 4 to 6 and follow
[Run Deephaven in Docker](#run-deephaven-in-docker).

The Deephaven environment, `~/massive-test/dh-venv`, holds the Deephaven server and the
library. The first of the two commands below makes it with Python 3.12, which uv installs if
your computer lacks it. In the same terminal as step 3, so `$TAG` is still set, run these two
commands:

```bash
uv venv --seed --python 3.12 ~/massive-test/dh-venv
~/massive-test/dh-venv/bin/pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

After a few minutes of downloads, pip prints a line that starts `Successfully installed` and
names `deephaven-server` and `deephaven-massive`. The library's version is the tag without its
leading `v`, such as `0.1.0rc1`. Note how long the install took, and copy every line that
says `ERROR` or `WARNING` into your checklist.

To install with uv instead of pip, run this in place of the `pip` line. uv prints a line such
as `Installed 40 packages`, then one line per package, such as
`+ deephaven-massive==0.1.0rc1` (an example):

```bash
uv pip install --python ~/massive-test/dh-venv deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

<a id="5-run-the-setup-check"></a>

### 5. Run the setup check

The setup check confirms that the checkout and the Deephaven environment hold the same
release, and that Massive accepts your key. Run it from the checkout, in a terminal where your
key is set:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/tester.py check
```

You should see one line per item, each starting `OK`, `WARN`, or `FAIL`, and `ready: yes` at
the end. The check also saves its lines in `~/massive-test/setup-check.txt`, for your bug
reports. Here are some of the lines, with your versions, commit, and dates in place of these:

```text
OK   java: Java 17
OK   uv: uv 0.9.5
OK   commit: 1a2b3c..., tree clean
...
OK   version: the checkout is on v0.1.0rc1
OK   console: deephaven-massive 0.1.0rc1, deephaven-core 42.7, in /Users/you/massive-test/dh-venv/bin/python
OK   same commit: both are 1a2b3c...
OK   key: Massive accepted the API key
OK   plan stocks: basic, history from 2024-10-30
...
ready: yes
```

A `WARN` line needs only a note in your checklist. A `FAIL` line says what to fix, so fix it
and run the check again.

<a id="6-start-deephaven"></a>

### 6. Start Deephaven

Start the server in a terminal where your key is set. The server reads the key when it
starts, so a key you set later doesn't reach it:

```bash
~/massive-test/dh-venv/bin/deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=massive-test" 2>&1 | tee ~/massive-test/server.log
```

The command gives the server 4 GB of memory, sets the Deephaven IDE's password to
`massive-test`, and keeps a copy of the server's output in `~/massive-test/server.log` for
bug reports. Within a minute, you should see these lines:

```text
Deephaven is running at http://localhost:10000/?psk=massive-test with authentication type io.deephaven.authentication.psk.PskAuthenticationHandler
Press Control-C to exit
```

Your browser opens the Deephaven IDE, already signed in. If it doesn't, open
<http://localhost:10000/?psk=massive-test>. You should see the Deephaven IDE, with a
**Console** panel where you'll paste the examples.

Leave this terminal open, because Control-C in it stops the server. Open a second terminal for
anything else, and set your key there too.

### Fix a step that fails

Most setup problems come from a sign-in that didn't reach git, or from a key set in a
different terminal from the one that runs the command:

| You see | Why | What to do |
|---|---|---|
| git asks for a password, or says `Repository not found` | git can't use your GitHub sign-in | Run `gh auth login` and `gh auth setup-git` again, as in step 1, then repeat the command |
| `refused: MASSIVE_API_KEY is not set in this terminal` | The key isn't set in the terminal that ran the command | Set it as in step 2, then run the command again |
| `java -version` says "No Java runtime present", or `echo $JAVA_HOME` prints an empty line | Java isn't installed, or this terminal started before you set `JAVA_HOME` | Follow [Deephaven's install guide](https://deephaven.io/core/docs/getting-started/pip-install/), then open a new terminal |
| `java -version` shows a version below 17 | An older Java comes first | Point `JAVA_HOME` at Java 17 or later, as Deephaven's install guide shows, and open a new terminal |
| The server stops at once and prints a Java error | Your platform needs a setting that Deephaven's install guide names, as some Apple M2 Macs do | Find the error in [Deephaven's install guide](https://deephaven.io/core/docs/getting-started/pip-install/), and add the setting it gives inside the `--jvm-args` quotes |
| The server stops with `Address already in use` | Another program holds port 10000 | Add `--port 10010` instead, and open `http://localhost:10010/?psk=massive-test` |

Report any other error, as [Report a problem](#report-a-problem) shows.

<a id="run-the-examples"></a>

## Run the examples

The 25 examples try the library the way a user would, from reading history to live tables
that follow a watchlist. You'll run them twice. First, one
command runs them all and saves what each did, which tells you quickly whether anything fails
on your key. Then you paste each one into the Deephaven IDE and judge what it shows, which is
the part only a person can check.

Massive sells one subscription per asset class, such as Stocks or Currencies, and each is on a
plan, from the free Basic plan up. Most examples work on Basic, and a few need a paid plan:

| Examples | Plan |
|---|---|
| 1 to 16, 20, and 23 to 25 | Any plan, Basic included |
| 17 and 18 | Stocks Starter or higher |
| 22 | Stocks Developer or higher |
| 19 and 21 | Currencies Starter or higher |

Run the paid examples on a free key too. Their live tables open, and a few seconds later
Massive refuses them. Each refusal shows as a `FAILED` row, quoting Massive's answer, in
`streams`, a diagnostics table that example 6 introduces. Seeing that refusal is part of the
test.

### Run every example with one command

The example runner starts its own Deephaven server, runs every example in order, and saves
what each one printed and made. Run it before you paste the examples by hand, because the
console's examples use the same key's request limits.

1. If the Deephaven server of step 6 is running, stop it with Control-C in its terminal.
2. From the checkout, in a terminal where your key is set, run the examples:

   ```bash
   cd ~/massive-test/deephaven-massive
   uv run python scripts/tester.py examples
   ```

   The runner first asks Massive which plans your key holds, and adjusts each example's settings
   to them. It takes 15 minutes or more. You should see one line per example as it runs, then
   `no example failed` or the names of the examples that failed.
3. Read the verdicts in `~/massive-test/examples-output/summary.txt`. Each example has its
   own file there with everything it printed, and its tables are under `tables/`:

   | Verdict | What you do |
   |---|---|
   | `passed` | Nothing. A paid example passes on a free key once its live table opens. |
   | `refused by your plan` | Nothing, unless your plan should include this example's data. |
   | `failed` | Report it, as [Report a problem](#report-a-problem) shows. |

4. Start the Deephaven server again, as in [step 6](#6-start-deephaven).

### Run the examples by hand

Every Python block in this part goes into the **Console** panel of the Deephaven IDE. Paste a
whole block into the input box at the bottom of the panel, and press Enter. Shift+Enter adds a
line without running it. Each example opens its own session, so it runs as pasted, and you
can run any one of them again on its own.

Each table an example assigns to a variable opens in a panel named after the variable. If one
doesn't open, click its name in the console's output.

When an example fails, the console prints the error in red, starting
`Error in Python interpreter`. Its last Python line, right before the first line that starts
with `at `, names the error and says what went wrong.

Market data changes every day, so your numbers will differ from the ones here. Judge each
result by its shape, such as its columns, roughly how many rows it has, and the kind of
message. For each example, note in your checklist whether it worked, how long it took, and
anything unclear.

Instead of pasting, you can run an example from its file in the checkout. Paste this helper
into the console once:

```python
import glob
import os


def run(n):
    pattern = f"~/massive-test/deephaven-massive/docs/tester-examples/{n:02d}_*.py"
    (path,) = glob.glob(os.path.expanduser(pattern))
    exec(compile(open(path).read(), path, "exec"), globals())
```

Then run an example by its number, as in `run(1)` for the first. If you restart the console,
paste the helper again. The helper doesn't work in the Docker setup, whose server can't see
the checkout, so paste the examples there.

Three examples make a watchlist, a table of symbols you edit in its panel. A table that
follows the watchlist reads or streams each symbol you add, and drops each one you delete:

- To add a row, click the empty row at the bottom of the panel, type the symbol, and press
  Enter. Then click **Commit** below the table. Nothing is added until you commit.
- To delete a row, right-click it, and choose **Delete Selected Rows**.

If your IDE shows other button or menu names, note them in your checklist.

#### 1. Open a session

The first example turns on the library's debug log and opens a session, the library's
connection to Massive. The debug log, `~/massive-test/massive-debug.log`, records every
request the library sends and every decision it makes, and your bug reports include it. This
code prints the library's version and the session:

```python
# Turn on the debug log, and open a session on your Massive key.
import os

import deephaven_massive as dm

# tester.py and the Docker setup name the test folder in MASSIVE_TEST_FOLDER.
folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
os.makedirs(folder, exist_ok=True)
dm.enable_debug_logging(os.path.join(folder, "massive-debug.log"))

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
print(dm.__version__)
print(m)
```

You should see the version without its leading `v`, such as `0.1.0rc1`, and then
`<MassiveSession #1 open>`. The number counts the sessions this console has opened, and most
examples open one more.

The setting `rest_basic_subscriptions` tells the library that each subscription is on Basic.
The library then spaces your requests to Basic's limit of 5 a minute from the first one. Every
free-plan example opens its session with this setting. On a paid key, they still run as
written, and they wait between requests.

If you see an error that ends `AuthenticationError: no Massive API key ...`, the server didn't
get your key. Stop the server with Control-C, set the key in that terminal, and start the
server again, as in [step 6](#6-start-deephaven).

#### 2. List the datasets

The library reads more than 100 of Massive's datasets, and `dm.datasets()` lists them. Next, the
code describes the columns of one query without sending Massive a request:

```python
# List every dataset, and describe the columns of daily stock bars without a request.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

catalog = dm.datasets()
bars = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY)
print(bars)
bar_columns = bars.columns()
```

You should see the line `m.stocks.bars(['AAPL', 'MSFT', 'NVDA'], dm.Timespan.DAY)` and two
panels. The `catalog` panel has one row per dataset, with what it holds and a link to its
columns. It also lists the verbs that read the dataset, which are the methods that return a
table, such as `history` and `fetch`. The `bar_columns` panel has one row
per column of daily bars, with a description of each.

#### 3. Download two weeks of daily bars

Two weeks of daily bars for three stocks take three requests to Massive. This block downloads
them and prints how many rows arrived:

```python
# Download two weeks of daily bars for three stocks: three requests to Massive.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
daily = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY).history(
    yesterday - timedelta(days=13), yesterday
)
print(daily.size)
```

After a few seconds, you should see a number between 24 and 30 and a panel named `daily`. It
has eight to ten bars per stock, oldest first, with none for weekends and holidays. Its `Date`
column holds each trading day as text, such as `2026-10-30`. Hover over the `Close` header,
and you should see a description of the column. If you see something else, note it.

#### 4. Summarize the bars with Deephaven

Deephaven's table operations work on these bars directly. Here you read the same bars, add a
column, keep one stock's rows, and summarize each stock:

```python
# Add a column, keep some rows, and summarize by stock, with Deephaven's table operations.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

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

You should see three more panels besides `daily`. The `ranges` panel adds each day's
high-to-low range, `nvda` holds only NVDA's rows, and `per_symbol` has one row per stock, with
its average close, highest high, and number of days.

#### 5. Read reference data

Reference data describes companies and markets: here, three companies' details, the market's
status, the coming holidays, and two weeks of Treasury yields:

```python
# Read facts about companies and markets: company details, market hours, Treasury yields.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

details = m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
companies = details.view(["Symbol", "Name", "PrimaryExchange", "ListDate", "MarketCap"])

market_now = m.reference.market_status().fetch()
holidays = m.reference.market_holidays().fetch()
two_weeks_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=14)
yields = m.economy.treasury_yields(start=two_weeks_ago).fetch()
```

You should see four new panels besides `details`:

| Panel | What it holds |
|---|---|
| `companies` | Three rows: each company's name, its exchange (`XNAS` is Nasdaq), its listing date, and its market value in dollars |
| `market_now` | One row, whose `Market` reads `open`, `closed`, or `extended-hours` |
| `holidays` | The coming market holidays |
| `yields` | US Treasury yields in percent, one row per business day |

#### 6. Make a table that updates itself

A live table updates as new data arrives. Here you make one that asks Massive for the market's
status every 60 seconds, and open the session's three diagnostics tables:

```python
# Make a table that asks Massive again every minute, and open the diagnostics tables.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

live_status = m.reference.market_status().poll(60)

rate_limits = m.rate_limits()
streams = m.streams()
connections = m.connections()
```

You should see `live_status`, whose `ServerTime` changes about once a minute. The diagnostics
tables show what this session is doing:

| Panel | What you should see |
|---|---|
| `rate_limits` | One row per subscription, plus `other`. The `stocks` row shows `RequestsPerMinute` 5 and `LimitSource` `DECLARED`. |
| `streams` | One row per live table or background download of this session: here `live_status`, with `State` `RUNNING` |
| `connections` | The session's live connections to Massive: none yet |

#### 7. Watch Basic's limit

Six stocks read every 30 seconds make 12 Stocks requests a minute, against Basic's limit of 5.
This block asks for exactly that, and its last line lets the library stop the table once
nothing else uses it:

```python
# Ask for more than Basic allows, then let the library stop the table.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

# 12 Stocks requests a minute: six stocks every 30 seconds, against a limit of 5.
paced = m.stocks.previous_day_bar(["AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"]).poll(30)
rate_limits = m.rate_limits()
streams = m.streams()

# The table keeps updating while its panel is open, and stops once you close the panel.
m.release(paced)
```

Watch `rate_limits` and `paced` for two minutes without running anything else. You should
see the `stocks` row of `rate_limits` switch between `READY` and `PACED`, which means a request
is waiting for a free slot. The `paced` panel should get its six rows within about a minute,
and the debug log gets a `WARNING` that the table's requests are paced at 5 a minute.

Then close the `paced` panel. Within a minute, the `stocks` row of `rate_limits` should settle
at `READY`. Until you close it, the panel keeps the table updating, even though its row in
`streams` already reads `RELEASED`.

#### 8. Download in the background

A long download holds the console until it finishes. With `wait=False`, the call returns at
once, and the table fills when the download is done. Here a year of two stocks downloads that
way:

```python
# Start a one-year download of two stocks in the background, so the console stays free.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago, wait=False)
streams = m.streams()
```

You should see an empty `year` panel at first. In `streams`, its row's `State` reads
`LOADING`, then `DONE`, and the rows appear all at once.

#### 9. Analyze a year of two stocks

A year of daily bars is enough to compare two stocks' returns, growth, and volatility, and how
closely they moved together. Reading the same year, this block also draws a chart and saves a
file:

```python
# Compare a year of two stocks: daily returns, growth, volatility, correlation, a chart, a file.
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg, parquet
from deephaven.pandas import to_pandas
from deephaven.plot.figure import Figure
from deephaven.updateby import delta

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

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

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
parquet.write(growth, os.path.join(folder, "growth.parquet"))
```

You should see a printed correlation between -1 and 1, which measures how closely the two
stocks' daily moves agreed. Expect a positive value for these two. The `summary_year` panel
has two rows: `TotalReturn` is the year's price change as a fraction, so 0.15 means 15%, and
`AnnualVolatility` measures how much the price swings. The `growth_chart` panel draws two
lines that both start at 1.0, and the file `~/massive-test/growth.parquet` holds the `growth`
table.

#### 10. See what Basic refuses

Basic doesn't include the latest trade, live minute bars, or bars from 2015. Ask for each, and
print how Massive refused it:

```python
# Ask for three things the free Basic plan doesn't include, and print how each was refused.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
streams = m.streams()

try:
    m.stocks.last_trade(["AAPL"]).fetch()
    print("the latest trade: no error, so your plan includes it")
except dm.MassiveError as error:
    print(f"the latest trade: {type(error).__name__}: {error}")

try:
    live_minute_bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    print("live minute bars: opened; Massive's answer shows in the streams panel")
except dm.MassiveError as error:
    print(f"live minute bars: {type(error).__name__}: {error}")

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history("2015-01-05", "2015-01-09")
    print("bars from 2015: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"bars from 2015: {type(error).__name__}: {error}")
```

On a free key, you should see three lines:

| Line | What it says |
|---|---|
| The latest trade | `EntitlementError`, quoting Massive's refusal |
| Live minute bars | That they opened. Seconds later, their `stocks.bars` row in `streams` turns `FAILED`, with an `Error` that quotes Massive and says Basic has no live data. |
| Bars from 2015 | `EntitlementError`: your plan doesn't reach back to 2015 |

On Stocks Advanced, the first line says "no error". On Starter and Business, which don't
include the latest trade, it's an `EntitlementError`. On Developer, it's an `EntitlementError`
that names `delayed_subscriptions`, because this session doesn't accept delayed data. On
Starter and Developer, the second line says the same. That's expected, and example 17 shows the
setting. On Advanced or Business, the third line says "no error", and the live minute bars
keep streaming.

Note in your checklist whether each message, the `Error` in `streams` included, told you what
went wrong and what you could do about it. Note any message that suggested your key was wrong
when the cause was your plan. Nobody has yet seen what Massive does with a free key's live
minute bars, so note which of these you see:

| Massive | What you see |
|---|---|
| Rejected the key | The `Error` says Massive rejected the API key. |
| Refused the subscription | The `Error` quotes a refusal of `AM.AAPL`. |
| Closed the connection | Open `m.connections()`: it shows the connection reconnecting, again and again. |

#### 11. Convert symbols from other sources

The library takes each symbol exactly as Massive writes it, such as `BRK.B`, `C:EURUSD` for
the euro against the dollar, `X:BTCUSD` for Bitcoin, and `I:SPX` for the S&P 500. Other
sources write symbols differently, and these functions convert them, one symbol at a time or a
whole table at once. They need no session:

```python
# Convert symbols written in other conventions into Massive's, one at a time and as a table.
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))

try:
    dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS)
except ValueError as error:
    print(f"ValueError: {error}")

yahoo_symbols = new_table([string_col("Symbol", ["BRK-B", "AAPL", "not a ticker"])])
converted = dm.canonicalize_symbols(
    yahoo_symbols, dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
)
```

You should see `C:EURUSD`, `X:BTCUSD`, `I:SPX`, and `BRK.B`, each on its own line. Next comes
a line that starts `ValueError: 'BRK-B' is not a Massive stock ticker: it uses another
convention's separators`, which names the conventions to choose from. The `converted` panel
holds `BRK.B`, `AAPL`, and a row whose `Symbol` cell is blank. A symbol the function can't
convert becomes null, and the debug log records a WARNING for it.

#### 12. Check the debug log

The debug log must never hold your key. Count the log's lines, its warnings and errors, and any
copy of your key:

```python
# Count the debug log's lines, its warnings and errors, and any copy of your key.
import os

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
with open(os.path.join(folder, "massive-debug.log"), encoding="utf-8") as log_file:
    text = log_file.read()
lines = text.splitlines()
print(len(lines), "lines")
print(sum(" WARNING " in line or " ERROR " in line for line in lines), "warnings and errors")
print(text.count(os.environ["MASSIVE_API_KEY"]), "copies of the key")
```

You should see `0 copies of the key`. Any other number of copies is a serious problem. Report
it, and don't send the log. You should also see a few warnings and errors, from examples 7,
10, and 11 among others. The line count runs to hundreds, or more if the example runner wrote
to the same log.

#### 13. Chart a moving average

Examples 13 to 16 are short analyses of the kind you'd write yourself: a moving average,
company size against return, a dividend's yield, and the yield curve. Rerun them with your own
symbols and dates. The first charts NVDA's daily close over six months with its 20-day moving
average:

```python
# Chart NVDA's daily close over six months with its 20-day moving average: one request.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_avg_tick

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

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

You should see the `nvda_daily` panel, with an `Average20` column, and the `nvda_chart`
panel, with two lines.

#### 14. Compare company size with return

Company facts join to prices on the `Symbol` column, which puts each company's market value
beside its return over the last three months:

```python
# Put each company's market value beside its return over the last three months.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

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

You should see the `size_and_return` panel, with one row per company and its `Return3m` and
`MarketCapBillions`.

#### 15. Work out a dividend's yield

A dividend's yield is its cash amount divided by the price. Find each Apple dividend of the
past year, and divide it by the close on the trading day before the stock went ex-dividend:

```python
# Find each Apple dividend of the past year, and its yield on the close the day before.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

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

You should see the `dividend_yield` panel, with one row per payment, about four for a year of
Apple, each with its `PriceDate` and `PaymentYieldPercent`.

#### 16. Chart the yield curve

The gap between the 10-year and 2-year US Treasury yields is the yield curve's slope, charted
here over two years:

```python
# Chart the gap between the 10-year and 2-year US Treasury yields over two years.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

two_years_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=730)
curve = (
    m.economy.treasury_yields(start=two_years_ago)
    .fetch()
    .update(["Spread = Yield10Year - Yield2Year", "Day = parseInstant(Date + `T00:00:00 ET`)"])
)
curve_chart = Figure().plot_xy(series_name="10y - 2y", t=curve, x="Day", y="Spread").show()
```

You should see the `curve` panel, with a `Spread` column, and the `curve_chart` panel. Its
line falls below zero wherever the curve was inverted.

#### 17. Stream live minute bars

A `last_by` table keeps the latest minute bar of each stock, replaced as each minute ends. To
run it, you need a paid Stocks plan. Stocks Starter and Developer stream data 15 minutes late,
so the session accepts delayed data. On Stocks Advanced or Business, which stream in real time,
delete the `delayed_subscriptions` setting before you run it:

```python
# Keep the latest minute bar of five stocks, updated live. Needs a paid Stocks plan.
import deephaven_massive as dm

# Stocks Starter and Developer stream 15 minutes late, so this session accepts delayed
# data. On Stocks Advanced or Business, which stream in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

latest_bars = m.stocks.bars(
    ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"], dm.Timespan.MINUTE
).last_by()
streams = m.streams()
```

While the stocks trade, you should see the `latest_bars` panel with one row per stock, each
row replaced as its next minute ends. Once trading stops, the table keeps each stock's last
bar. If you run it while the market is closed, it stays empty until the stocks next trade. On
a free key, the table opens, and a few seconds later its row in `streams` turns `FAILED` with
Massive's answer.

#### 18. Join today's history to live bars

One table can hold AAPL's minute bars for all of today. The minutes already past come
from history, and each new minute is added as it ends. To run it, you need a paid Stocks plan
and the same setting as example 17:

```python
# Join today's minute bars from history with the live ones that follow. Needs a paid Stocks plan.
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import merge

import deephaven_massive as dm

# Stocks Starter and Developer stream 15 minutes late, so this session accepts delayed
# data. On Stocks Advanced or Business, which stream in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))

aapl_minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE)
session_day = datetime.now(ZoneInfo("America/New_York")).date()
# Open the stream before reading history, so no minute falls between the two.
aapl_live = aapl_minutes.append_only()
aapl_past = aapl_minutes.history(session_day)
aapl_today = merge([aapl_past, aapl_live]).last_by(["Symbol", "TimestampStart"])
```

You should see the `aapl_today` panel, with a row for each minute AAPL has traded today and a
new row as each minute ends. It holds one row per minute, and where history and the stream
both have a minute, it keeps the stream's bar. On a day AAPL hasn't traded, such as a weekend, the
table is empty.

#### 19. Stream crypto trades

Crypto trades around the clock, so this stream of Bitcoin and Ether trades works at any hour.
To run it, you need a paid Currencies plan. It counts the trades by the minute:

```python
# Stream Bitcoin and Ether trades, and count them by the minute. Needs a paid Currencies plan.
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

crypto_trades = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
crypto_latest = crypto_trades.last_by("Symbol")
trades_per_minute = crypto_trades.update("Minute = lowerBin(Timestamp, MINUTE)").count_by(
    "Trades", by=["Symbol", "Minute"]
)
```

You should see `crypto_latest`, with one row per coin holding its latest trade, and
`trades_per_minute`, which gains a row per coin each minute.

#### 20. Follow a watchlist

Two watchlists drive three tables here. One table polls the previous day's bar of each stock
on the stock watchlist every minute, which is three Stocks requests a minute, within Basic's
five. Another reads those stocks' dividends, and a third reads the contracts of each futures
product on the product watchlist:

```python
# Make two watchlists you can edit, and tables that follow them. Free plan.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
reads = m.symbol_reads()
streams = m.streams()

# Three Stocks requests a minute, within Basic's five.
stock_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
stock_watchlist.add(new_table([string_col("Symbol", ["JPM", "XOM", "TSLA"])]))
watched_bars = m.stocks.previous_day_bar(stock_watchlist).poll(60)
watched_dividends = m.stocks.dividends(stock_watchlist).fetch()

# One Futures request for each product you add.
product_watchlist = input_table({"ProductCode": dht.string}, key_cols="ProductCode")
product_watchlist.add(new_table([string_col("ProductCode", ["ES"])]))
watched_contracts = m.futures.contracts(product_code=product_watchlist, limit=10).fetch()
```

You should see `stock_watchlist` with three rows, `watched_bars` with one row per stock
within about a minute, and `watched_dividends` with each stock's dividends. You should also
see `product_watchlist` with one row, and `watched_contracts` with up to 10 ES contracts. Then
edit the watchlists:

1. In `stock_watchlist`, add `AAPL`. Within about a minute, you should see an `AAPL` row in
   `watched_bars`, and AAPL's dividends join `watched_dividends`.
2. Delete `TSLA`. You should see TSLA's dividends leave `watched_dividends` at once, and its
   row leave `watched_bars` at the next read.
3. Look at `streams`. You should see `SymbolCount` 3 in the `stocks.previous_day_bar` row, the
   number of symbols its latest read covered, and `Kind` `read` in the `stocks.dividends` row.
4. Look at `reads`. For `watched_dividends`, you should see `DONE` for JPM, XOM, and AAPL,
   and `REMOVED` for TSLA. The `ES` row belongs to `watched_contracts`.
5. In `product_watchlist`, add `NQ`. Within about a minute, you should see up to 10 NQ
   contracts land in `watched_contracts` together, and an `NQ` row in `reads` whose `State` is
   `DONE`.
6. Delete `NQ`. You should see every NQ contract leave `watched_contracts` at once, while the
   ES contracts stay.

#### 21. Stream a crypto watchlist

A stream can follow a watchlist too, keeping each pair's latest trade. To run it, you need a
paid Currencies plan, and it works at any hour:

```python
# Stream the trades of a watchlist of crypto pairs. Needs a paid Currencies plan.
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
streams = m.streams()

crypto_watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
crypto_watchlist.add(new_table([string_col("Symbol", ["X:ETHUSD", "X:LINKUSD"])]))
crypto_board = m.crypto.trades(crypto_watchlist).last_by()
```

You should see `crypto_watchlist` with two rows, and `crypto_board` with one row per pair
after its first trade, usually within seconds. Then edit the watchlist, watching
`crypto_board` and the `crypto.trades` row of `streams`:

1. Add `X:SOLUSD`. You should see an `X:SOLUSD` row in `crypto_board` after its first trade,
   and `X:SOLUSD` in the stream's `Symbols`, with `SymbolCount` 3.
2. Delete `X:SOLUSD`. You should see its row leave `crypto_board` at once, and `Symbols` drop
   it, with `SymbolCount` 2.
3. Add `X:SOLUSD` again. You should see no `X:SOLUSD` row until its next trade, then a row
   whose `Timestamp` is later than when you added it. The old row never comes back.
4. Add `btcusd`, in lower case. You should see no row for it, and the stream's `Error` names
   `btcusd`, which isn't Massive's spelling, while the other pairs keep streaming. Delete the
   `btcusd` row and add `X:BTCUSD`. You should see the `Error` clear, and an `X:BTCUSD` row in
   `crypto_board` after its first trade.
5. Add `X:ZZZZUSD`, a pair Massive doesn't list. You should see no row for it, and the
   stream's `Error` names `X:ZZZZUSD`, while the other pairs keep streaming. Delete it, and
   you should see the `Error` clear.

#### 22. Stream the trades of the day's gainers

One live table can choose another's symbols. Here the day's top gainers, read again every
minute, choose whose trades to stream. To run it, you need Stocks Developer or higher, the
plans that include trades:

```python
# Stream the trades of the day's top gainers. Needs Stocks Developer or higher.
import deephaven_massive as dm

# Stocks Developer streams 15 minutes late, so this session accepts delayed data. On
# Stocks Advanced or Business, which stream in real time, delete that setting.
m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))
streams = m.streams()

gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
gainer_trades = m.stocks.trades(gainers).last_by()
```

While the market is open, you should see `gainers` with up to 20 rows, and `gainer_trades`
with one row per gainer after its first trade. The `stocks.trades` row of `streams` lists the
gainers in its `Symbols`. When a stock joins or leaves the gainers at a minute's read, it joins
or leaves `Symbols` and `gainer_trades` too.

#### 23. Read history that follows a watchlist

History can follow a watchlist as well. Each stock you add is read over the same month, with
one Stocks request, and each one you delete leaves the table:

```python
# Read a month of daily bars that follows a watchlist you can edit. Free plan.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
reads = m.symbol_reads()

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
month_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=30)
followed = m.stocks.bars(names, dm.Timespan.DAY).history(month_ago)
```

You should see `names` with two rows, `followed` with about 20 bars each of AAPL and MSFT, and
`reads` with a `DONE` row for each. Then edit `names`:

1. Add `NVDA`. A few seconds later, you should see NVDA's bars join `followed`, all in one
   update. In `reads`, NVDA goes from `QUEUED` to `LOADING` to `DONE`, and its `Rows` equals
   its number of bars in `followed`.
2. Delete `NVDA`. You should see its bars leave `followed` in one update, and its row in
   `reads` become `REMOVED`.
3. Add `NVDA` again. You should see it downloaded again, with a newer `ReadAt` in `reads`.

<a id="24-save-the-diagnostics"></a>

#### 24. Save the diagnostics

When something goes wrong, run this example first, in the same console. It saves the
diagnostics tables of the session in `m`, which is the session the last example opened, and
your bug report includes them:

```python
# Save the diagnostics tables of the session in m, the one the last example opened.
import os

from deephaven.pandas import to_pandas

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
diagnostics = os.path.join(folder, "diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {"streams": m.streams(), "connections": m.connections(), "rate_limits": m.rate_limits()}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
```

You should see `saved to` and the folder, such as `/Users/you/massive-test/diagnostics`.

#### 25. Close a session

Closing a session stops all its live tables. This block closes one and shows that a closed
session refuses new work. Then it reads a week of bars in a `with` block, which closes its
session when the block ends:

```python
# Close a session, and see that a closed session refuses new work.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
closing_status = m.reference.market_status().poll(60)
m.close()
print(m)

yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history(yesterday - timedelta(days=6), yesterday)
except dm.MassiveError as error:
    print(f"MassiveError: {error}")

# A with block closes its session when the block ends.
config = dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription))
with dm.MassiveSession(config) as m2:
    last_week = m2.stocks.bars("NVDA", dm.Timespan.DAY).history(
        yesterday - timedelta(days=6), yesterday
    )
print(m2, last_week.size)
```

You should see three lines like these, with your own session numbers and a row count between
3 and 5:

```text
<MassiveSession #7 closed>
MassiveError: MassiveSession #7 is closed
<MassiveSession #8 closed> 5
```

The `closing_status` panel shows an error that says the session was closed, and it keeps its
rows. The other examples' sessions are still open, and their tables carry on. When you're
done with the console, stop the server with Control-C in its terminal.

<a id="leave-the-live-run-going"></a>

## Leave the live run going

The live run checks the library against Massive's real servers for at least five trading
days, and it needs about 10 minutes of your time a day. One program, `scripts/live_run.py`,
runs every check your Massive plan allows at set times of the New York day, and records
Massive's answers for the owner. You start it once and leave it running.

When it starts, and again each trading morning, the program asks Massive which plan your key
holds for each subscription, such as Stocks Starter, or Stocks Basic if you're on the free
plan. This
check is the probe. Because of it, you can buy, upgrade, or cancel a plan at any time, and the
program runs the checks your new plan allows.

Two kinds of check keep the computer busy for many hours, and they need it on and awake:

| Check | When, in New York time | Which plans |
|---|---|---|
| A stream day: a whole trading day of live data | 03:15 to 20:30 on a day the program picks | Any paid plan, since the free Basic plans don't stream |
| A stream night: Bitcoin, Ether, and the main stock-index futures overnight | 16:30 to 09:30 the next morning, on a Sunday to Thursday evening | A paid Currencies or Futures plan |

The `status` command of [step 4](#4-look-in-once-a-day) shows when the next stream day and
night are planned. At least one full stream day, from its start to its end, must fall within
your test.

### 1. Prepare the computer

Use a computer that can stay on and awake for the whole test, at least seven calendar days,
on a stable network with 20 GB of free disk. The program keeps the computer awake while it
runs, but closing a laptop's lid still puts it to sleep. On Windows, set
**Settings > System > Power > Sleep** to **Never**.

Stop every other use of your key first, including the Deephaven server you used for the
examples. A live check fails or waits when another program holds the key's connection.

Then open a terminal, and set your keys as in [step 2 of Set up](#2-set-your-massive-key),
including the S3 keys if you have them. From the checkout, run the program's check, which
writes nothing:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/live_run.py check
```

You should see one line for each part of the machine, then your plan for each subscription,
the flat files your S3 keys reach, and `ready: yes`. Here are some of the lines, with your own
values in place of these:

```text
OK   java: Java 17
OK   disk: 212 GB free
OK   keep-awake: caffeinate
...
ready: yes
```

A `WARN` line is advice. A `FAIL` line says how to fix it.

### 2. Start the first run

Start the program with `--any-time`. It runs the checks that need no market hours right away.
Without it, they wait until 11:30 New York time, when each trading day's regular checks
begin. The option counts only on a results folder's first start. Later starts run each check
at its set New York time:

```bash
uv run python scripts/live_run.py run --out ~/massive-live --any-time
```

You should see a line that names the results folder, such as
`10:01 EST results folder: /Users/you/massive-live`, and your subscriptions. Every line starts
with the New York time. Then a line appears as each group of checks starts and ends, such as
these:

```text
10:02 EST start evidence
10:02 EST evidence: 74 checks; on Basic, up to 40 minutes of requests
10:49 EST end evidence
```

When nothing is due until a set time, the program writes a zip of everything so far, prints
`packed live-results-<date>.zip`, then prints a line such as
`16:20 EST idle: nothing more to run until Tue 08:00 EST (probe)`, and sleeps. Leave it
running.

### 3. Stop and resume the run

The program can stop and resume at any time, and this step checks that it does. About ten
minutes after the first start, press Control-C once. Within a minute, you should see a line
like this:

```text
10:12 EST interrupted; to resume, run: uv run python scripts/live_run.py run --out /Users/you/massive-live
```

Run that same command, without `--any-time`. You should see `resuming`, and the run carries
on where it stopped. Checks that passed stay passed.

<a id="4-look-in-once-a-day"></a>

### 4. Look in once a day

Once a day, check on the run from a second terminal in the checkout. The `status` command
changes nothing:

```bash
uv run python scripts/live_run.py status --out ~/massive-live
```

You should see the latest probe and your subscriptions, the next stream day, and the next
stream night if one is planned, such as
`next stream night: Sun 2026-11-08 16:30 EST to Mon 09:30 EST`. Every check follows, grouped,
with when its group runs next, and then a summary table:

```text
group | passed | failed | error | refused | skipped | not run
evidence | 52 | 0 | 0 | 0 | 22 | 0
```

Most statuses need nothing from you:

| Status | What you do |
|---|---|
| `passed` or `not run` | Nothing |
| `skipped: needs <plan>`, such as `skipped: needs Stocks Starter` | Nothing. Your plan doesn't include that check, and a skip isn't a failure. |
| `refused` | Nothing. Massive refused something its own documentation says your plan includes, and the owner reads it in the results. |
| `failed` or `error` | Nothing the first time, because a failed check runs once more. Write to the owner if a check fails twice. |

Note in your checklist each date you looked in, and the date of the full stream day.

Between check-ins, you can run the examples again on a new Deephaven server, but only while the
run's last line says `idle`, and stop before the time that line names. Several checks besides
the stream day and night need your key's live connection.

While the run goes, read `README.md` and the dataset pages in `docs/datasets/`, and note
anything unclear for your report.

<a id="5-pack-and-send-the-results"></a>

### 5. Pack and send the results

Pack the results whenever the owner asks, and when you stop for good. To pack, press Control-C
and run this in the same terminal, which must hold exactly the keys the run used:

```bash
uv run python scripts/live_run.py pack --out ~/massive-live
```

You should see `wrote /Users/you/massive-live/live-results-<date>.zip`. Each zip holds
everything so far, so send only the newest, on a GitHub issue as
[Finish the test](#finish) describes. If `pack` refuses, its message says why:

| Message | What you do |
|---|---|
| It names files in which it found a key, and writes no zip | Send the owner that list instead. |
| It says to set exactly the secrets the run used | Set the same keys as when you ran `run`, and pack again. |

Unless you're stopping for good, start the run again with the same `run` command.

### Restart after a stop

The same `run` command resumes the run after any stop:

| What happened | What you do |
|---|---|
| Control-C, a closed terminal, a crash, or a lost network | Run the same `run` command again. |
| The computer restarted | Open a terminal, set your keys again, go to the checkout, and run the same command. |
| You changed your plan | Nothing, because the next morning's probe notices. To make it notice at once, press Control-C and run the same command. |
| You got S3 keys for the first time | The program prints a reminder. Stop it, set the keys, and start it again. |
| The owner sends a new release | Copy the code of the new release, and start a new results folder, such as `--out ~/massive-live-2`. The program refuses to resume a folder on another commit. |

### Run the upgrade check when the owner asks

The upgrade check tests that once you upgrade a subscription from the free plan, the library
stops limiting it to 5 requests a minute. Run it only when the owner asks:

1. Stop the run with Control-C, before you upgrade.
2. Start the check with the name of the subscription you'll upgrade, which is `stocks`,
   `options`, `indices`, `currencies`, or `futures`. For stocks, run this command:

   ```bash
   uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live
   ```

   After a few minutes, you should see `Now upgrade stocks on the dashboard, then press Enter`.
3. Upgrade the subscription on Massive's dashboard, and press Enter. Within 15 minutes, you
   should see `upgrade check: passed`, or `failed` with a reason.
4. Start the run again, with the same `run` command.

Pressing Enter without upgrading makes the check a rehearsal, and it ends
`upgrade check: failed: tier stayed Basic (recorded as a rehearsal)`.

### Fix a live run that fails

Most live-run problems come from the key, from another program using it, or from a computer
that slept:

| You see | What you do |
|---|---|
| `the API key was rejected` | Copy the key again from the dashboard, and set it again. |
| `a run or an upgrade check is using ...` | Another `run` is already going. Use it, or stop it first. |
| The probe shows a lower plan than you bought | A purchase can take minutes to reach the key. Wait ten minutes, then restart the run. |
| A gap in a stream day or night, in the zip's `REPORT.md` | Keep the computer awake with the lid open. After a first gap of more than 30 minutes, the program plans one more stream day or night. |
| A stream night fails because an hour its market was open brought no data | Nothing. The owner reads it in the zip. |
| Anything else | Send the owner the `status` output and the newest zip. |

<a id="report-a-problem"></a>

## Report a problem

When anything goes wrong, report it at once. You save the diagnostics, fill in a short
template, and run one command that zips your logs with your keys removed. These count as
problems:

- a step that fails, hangs, or shows something other than the step says
- an error message you can't understand or act on
- instructions that were unclear, which is a finding, not a failure on your part
- a console command that hasn't returned after 5 minutes, unless its example says it takes
  longer
- a `WARNING` in the debug log about a bar stamped at an unexpected time, such as a daily bar
  at 14:30

### Decide whether to report an error

An error message tells you whether the library, your plan, or your code caused it. A library
bug's message ends with the sentence "This is a bug in deephaven-massive", followed by how to
report it. Other errors depend on their type:

| Error | What it means | Report it? |
|---|---|---|
| A message containing "This is a bug in deephaven-massive" | A bug in the library | Yes |
| `EntitlementError` | Massive refused the request. The message names the subscription and the refused request, and quotes Massive. | Only if the message was unclear, or your plan should include the data |
| `AuthenticationError` | The key is missing, or Massive rejected it | Only if the message was unclear |
| Any other `MassiveError` | A condition the message explains, such as a closed session or a lost connection | Only if the message didn't tell you what happened or what to do |
| `ValueError` or `TypeError` | The call itself was wrong, such as a misspelled symbol, and the message says how to fix it | Only if the message didn't help you fix it |
| Any other error, a Java error, a hang, or a crash | Something failed that the library didn't catch | Yes |

An example the runner marks `refused by your plan` is a refusal by Massive. Report it only if
your plan should include that data. Report every example the runner marks `failed`.

The live run marks `refused` anything Massive refused although your plan should include it.
The owner reviews those, so you don't report them.

### Make the report

The command `scripts/tester.py bug-report` zips everything the owner needs into one report
zip, with your keys removed. Before you run it, save what the zip needs:

1. Right away, in the console where the problem happened, run
   [example 24](#24-save-the-diagnostics) to save the diagnostics tables. It saves the
   diagnostics of the session named `m`. If your session has another name, change `m` in the
   example to that name. Take a screenshot of anything on screen that looks wrong.
2. Copy this template into `~/massive-test/bug-report.md`, and fill in each part:

   ```markdown
   # <one line: what went wrong>

   ## What I did
   The exact code, pasted (or the example's number), and the steps before it.

   ## What I expected

   ## What happened
   The full error text, from `Error in Python interpreter` to the end, or what the table
   showed.

   ## When
   Date, time, and time zone (such as 2026-10-30 14:15 America/Chicago).

   ## My setup
   - Plan of each subscription (Basic, Starter, ...):
   - Console setup: virtual environment, or Docker

   ## Attachments
   - bug-report-<time>.zip
   - screenshots, for anything shown on screen
   ```

3. If your setup changed since you last ran the setup check, run it again, as in
   [step 5 of Set up](#5-run-the-setup-check).
4. From the checkout, in a terminal where your key is set, build the report zip:

   ```bash
   uv run python scripts/tester.py bug-report
   ```

   You should see it write `~/massive-test/bug-report-<time>.zip`. If the command finds a key
   in any file, it writes nothing and names the files instead. Send the owner that list.
5. Open an issue at <https://github.com/chipkent/deephaven-massive/issues/new>, paste your
   report, and attach the report zip and the screenshots.

The report zip holds these files, each with your keys replaced by `***`:

- `bug-report.md`, your report
- `setup-check.txt`, what the setup check printed last
- `massive-debug.log`, the library's debug log
- `server.log`, the server's output
- `diagnostics/`, the tables example 24 saved
- `examples-output/`, what the example runner saved
- `environment.json`, a description of your machine, which names the keys you set but never
  their values

<a id="finish"></a>

## Finish the test

The test ends once the live run has covered five trading days, one of them a full stream day.
If the dates in your checklist fall short, leave the run going and come back later. When they do,
send your results and clean up:

1. Stop the live run with Control-C in its terminal, and stop any Deephaven server with
   Control-C in its own terminal.
2. In the live run's terminal, with the same keys still set, pack each results folder as in
   [step 5 of Leave the live run going](#5-pack-and-send-the-results). For a second folder,
   use `--out ~/massive-live-2`.
3. Open an issue at <https://github.com/chipkent/deephaven-massive/issues/new>, and attach the
   newest `live-results-<date>.zip` from each folder, your checklist, and any bug-report zips
   you haven't sent.
4. Keep `~/massive-test` and every live-run folder, such as `~/massive-live` and
   `~/massive-live-2`, until the owner confirms they have your files. Then delete them:

   ```bash
   rm -rf ~/massive-test ~/massive-live ~/massive-live-2
   ```

   The command prints nothing. If you ran Deephaven in Docker, also remove its image, as
   [Run Deephaven in Docker](#run-deephaven-in-docker) shows.
5. When the owner says the test is over, cancel any paid plan on Massive's dashboard. A plan
   renews at the end of each billing period until you cancel it.

<a id="run-deephaven-in-docker"></a>

## Run Deephaven in Docker

To run the Deephaven IDE from Docker instead of from a second Python environment, follow
these steps. They replace steps 4 to 6 of Set up and end with the Deephaven IDE open in your
browser. You still need Java and [the checkout](#3-copy-the-code), because the setup check and
the example runner use the checkout's own Deephaven.

A Docker image is a saved starting point for a small Linux system, and a container is that
system running on your computer. The image here is Deephaven's own `ghcr.io/deephaven/server`,
with the library added from your checkout.

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/), and give it at
   least 6 GB of memory. On macOS and Linux, set the memory in Docker Desktop's settings. On
   Windows, set it in WSL's `.wslconfig` file, as
   [Microsoft's WSL settings guide](https://learn.microsoft.com/windows/wsl/wsl-config) shows.
   Then turn on Docker Desktop's WSL integration for Ubuntu, under **Settings > Resources >
   WSL integration**. Run `docker info`, and you should see no error.
2. Build the image from the checkout. The first build downloads Deephaven's image, which is
   over 1 GB:

   ```bash
   cd ~/massive-test/deephaven-massive
   git archive HEAD | docker build -f docs/tester-docker/Dockerfile --build-arg TESTER_COMMIT=$(git rev-parse HEAD) -t deephaven-massive-test -
   ```

   The build takes a few minutes, and its last lines name `deephaven-massive-test`. The image
   records the commit it was built from, so the setup check can compare it with the checkout.
3. In a terminal where your key is set, check the setup against the image:

   ```bash
   uv run python scripts/tester.py check --docker-image deephaven-massive-test
   ```

   You should see `ready: yes` at the end, as in [step 5 of Set up](#5-run-the-setup-check).
4. In a terminal where your keys are set, start the server:

   ```bash
   docker run --rm --name massive-test -p 10000:10000 \
     -e MASSIVE_API_KEY \
     -e MASSIVE_S3_ACCESS_KEY_ID \
     -e MASSIVE_S3_SECRET_ACCESS_KEY \
     -e MASSIVE_TEST_FOLDER=/massive-test \
     -e START_OPTS="-Xmx4g -Dauthentication.psk=massive-test" \
     -v "$HOME/massive-test:/massive-test" \
     deephaven-massive-test 2>&1 | tee ~/massive-test/server.log
   ```

   The three key options pass your keys into the container without showing them. Docker
   skips a variable you haven't set, so the same command works without S3 keys. The
   `MASSIVE_TEST_FOLDER` and `-v` options make the examples write their files to
   `~/massive-test` on your computer. `START_OPTS` gives the server 4 GB of memory and the
   password `massive-test`.
5. Within a minute, the server's output shows `Server started on port 10000`. Then open
   <http://localhost:10000/?psk=massive-test>, because Docker doesn't open a browser for you.
   You should see the Deephaven IDE.
6. From a second terminal, check that the key reached the container:

   ```bash
   docker exec massive-test sh -c 'echo "${#MASSIVE_API_KEY} characters"'
   ```

   You should see the same count as in [step 2 of Set up](#2-set-your-massive-key).

Continue at [Run the examples](#run-the-examples). Where it says to stop or start the server
of step 6, use step 4 here instead. When you're done:

- To stop the server, press Control-C in its terminal. Docker also deletes the container.
- To delete the image, run `docker rmi deephaven-massive-test`.
- On Linux with Docker Engine rather than Docker Desktop, `root` owns the files the container
  wrote, so delete them with `sudo rm`.
