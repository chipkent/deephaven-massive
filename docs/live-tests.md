# Live integration tests

The live tests call Massive's real servers with your keys. They answer questions Massive's
documentation leaves open, such as a timestamp's unit, where a day's bar starts, or how a
ticker is spelled. Each test records what Massive sent, and its docstring says what each answer
means for the library. Run them before a release, and whenever a check of the live-run program
fails.

Testers run the same checks through the [live-run program](#live-run-program), as the
[tester guide](tester-guide.md#live-run) shows.

<a id="run-the-tests"></a>

## Run the tests

The live tests are the files `tests/**/test_*_integration.py`. Run them in a clone set up as the
tester guide's [Set up the test](tester-guide.md#set-up) leaves it, in a terminal at the clone's
root. They carry these pytest markers:

| Marker | What it runs |
|---|---|
| `integration` | The evidence tests, each answering one open question |
| `integration_smoke` | Every dataset and method once, checked automatically |
| `integration_rate_limits` | The three rate-limit tests, which go over Basic's 5 requests a minute on purpose |
| `integration_limits` | The four connection-limit tests, which go over Massive's subscription and connection limits on purpose |

Every live test carries `integration`, and `-m integration` leaves out the tests that also
carry another of these markers.

<a id="variables"></a>

### Set your keys

Set `MASSIVE_API_KEY`, and the two S3 keys if your plans include flat files, as the README's
[Set your API keys](../README.md#set-your-api-keys) step shows. Flat files come with paid plans,
as Massive's [pricing](https://massive.com/pricing) shows. Two more variables are optional:

| Variable | What you get | Without it |
|---|---|---|
| `MASSIVE_DELAYED_API_KEY` | Two checks of a delayed Stocks key. Set it to a key on Stocks Starter or Developer when your main key's Stocks plan is neither; otherwise the main key serves. | Those checks skip |
| `MASSIVE_LIVE_PLANS` | Your plans, declared instead of asked of Massive ([Declare plans the probe can't see](#declare-the-plans)) | The run asks Massive |

<a id="run-order"></a>

### Run the commands in order

The connection-limit tests disturb the key's connections, so they run last and alone:

1. Stop every other program that uses the key. A stream test on a connection another program
   uses either waits for a slot or disconnects that program.
2. Run the evidence, smoke, and rate-limit tests. A stream test checks rows only while its
   market is open, so a weekday run in market hours covers more. The rate-limit tests need no
   open market, so run them in the same command, or, during market hours, at another time.

   In one command:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   Or the rate-limit tests apart, the second command later. They need at least one asset class
   still on Basic:

   ```bash
   uv run pytest -m "integration or integration_smoke" 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_rate_limits 2>&1 | tee -a live-checks.log
   ```

   On Basic, each wait of 5 seconds or more prints a `live pacing:` line that names the wait
   and the requests left.

3. Run the connection-limit tests, one command after the other:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

   The first command sends wide WebSocket subscriptions, such as one frame of 1,000 crypto
   topics and the crypto wildcard. The second takes a second connection slot with the same key,
   and records whether Massive drops the older connection or refuses the newer one.

Each command ends with the sections of [Read the end of a run](#read-the-end-of-a-run).

<a id="plans"></a>

## Let the run find your plans

You usually don't tell the tests your plans. Massive sells a plan for each asset class, and your
key can hold any mix. So the first live test of each run asks Massive which plan your key holds
in each asset class. This check, the probe, decides which later tests run and which skip.

The plans open the `open questions` section of the run's summary, in a line that starts
`plans:`. The run saves the probe's answer in `.live-plans.json` at the repository root. The
file holds a digest of each key, never the key. For the next hour, every run with the same API
key and S3 access key ID reuses the answer, and prints a `live plans:` line that says when it
was probed. To probe again at once, after an upgrade, delete the file.

<a id="declare-the-plans"></a>

### Declare plans the probe can't see

Declare your plans in `MASSIVE_LIVE_PLANS` in any of these cases:

| Case | What the probe does | What you declare |
|---|---|---|
| A snapshot comes back empty | It stops, and the run asks you to declare the plans | Every paid plan you hold |
| The run starts between 03:30 and 04:30 New York time, when Massive clears and refills its snapshots | It stops and asks you to declare the plans, unless it saved an answer in the last hour | Every paid plan you hold, or wait until 04:30 |
| Stocks or Options Business with the trades and quotes expansion | It reports the base Business plan, which streams no trades, quotes, LULD bands, or per-second bars | Every paid plan you hold, with `business_expansion` for that asset class |
| Indices Business | It reports Advanced, because no request tells the two apart | Every paid plan you hold, with `indices=business` |
| Futures Business | It reports Advanced, because no request tells the two apart | Every paid plan you hold, with `futures=business`. Massive sells it per venue. |

List each paid asset class as `<class>=<plan>`, separated by commas, and add `s3` when both S3
keys reach the flat files. Every asset class you leave out is on Basic. A declaration overrides
the probe and the saved answer, and the `plans:` line lists the declared plans. Each asset class
has its own plan names:

| Asset class | Plans you can declare |
|---|---|
| `stocks`, `options` | `starter`, `developer`, `advanced`, `business`, `business_expansion` |
| `futures` | `starter`, `developer`, `advanced`, `business` |
| `indices` | `starter`, `advanced`, `business` |
| `currencies` | `starter`, `business` |

Run this line before the tests to declare Starter on Stocks, Options, and Currencies, and flat
files:

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

<a id="reading-the-results"></a>

## Read the results

A failure is a fault in Massive or the library. A test fails when:

- a request it needs fails or returns nothing
- a stream delivers no row while rows are due
- Massive refuses a request although your key holds the test's plans
- a `verifies` test gets an answer other than the one the library assumes

A skip names its reason. A test skips when:

- your key lacks a plan it needs, and the skip names the plan with its price, such as
  "needs Currencies Starter ($49/month)"
- `MASSIVE_API_KEY`, the S3 keys, or the delayed key isn't set, or `CI` is set
- the day it asks for is older than your plan keeps
- the market isn't open, or the test can't tell its answer apart from a network drop

Most evidence tests carry one of two markers, which name the
[open questions](dev/verification-items.md) they answer by number:

- A `verifies` test asserts the answer the library assumes, so it fails the day Massive
  changes that answer.
- A `settles` test records evidence for you to read, and passes whatever the answer.

<a id="refusal-skips"></a>

### Refusals that skip

Massive's refusal "Your plan doesn't include this data timeframe" skips the test as "outside
the plan's history" when the day is older than the plan keeps. The same refusal for a day the
plan keeps fails the test, because it may be a library bug.

Any other refusal fails a test whose plans your key holds, and the run records the refusal. It
means your plans, probed or declared, don't match what Massive serves. Check
`MASSIVE_LIVE_PLANS` first, then open an issue with the refusal.

A smoke test, or any other test that declares no plans, skips as "not entitled: needs <plan>"
when Massive refuses it. The smoke coverage counts these skips apart from failures. The smoke
test for stock imbalances always skips, because imbalances arrive only around the opening and
closing auctions.

<a id="stream-passes"></a>

### When a stream test passes

A stream test passes only when rows reached its table, never on Massive's acknowledgement
alone. Stock trades and quotes are due from 04:00 New York time. Every other stock, option, and
index stream is due from 09:30, and a delayed plan's streams are due once the session has been
open for the plan's delay. When rows aren't due, the test skips after the acknowledgement, with
a reason such as "not exercised: market closed (acknowledged)".

The LULD check runs only in the regular session, when the bands change.

The `/prev` check shows a short day's stamp only on the trading day after a half day, and its
evidence marks that day `half day: yes`.

<a id="read-the-end-of-a-run"></a>

### Read the end of a run

The evidence goes to its own file at the repository root, `live-evidence-<UTC time>.log`, one
per command, written line by line, so a run cut short keeps everything it recorded. Before
pytest's short summary, the run prints up to four sections:

| Section | What it holds |
|---|---|
| `smoke coverage` | The smoke tests that passed, passed with drift, weren't entitled, were skipped, and failed. Drift means the test saw a library WARNING or a `SchemaDrift`, and each one is listed. |
| `open questions` | The plans line, then the questions the run touched, each by number and title, in groups: `verified X of Y`, `ran, evidence to read, X of Y`, one group per plan some questions need, with its price, and the groups `not verified (failed or skipped)` and `not verified (partly run)` |
| `live debug log` | The path of the run's debug log |
| `live evidence` | The evidence file's lines again |

<a id="older-history"></a>

### Tests that need older history

Four tests read fixed dates, which a plan that doesn't keep the date can't run. Every other
test picks days back from the run's own day, within the two years the free plans keep of
Stocks, Options, Currencies, and Futures. The free plans keep all of Indices.

| Test | Fixed date | Plans that keep it | Without one |
|---|---|---|---|
| The option split check | NVDA's 10-for-1 split of 2024-06-10 | Options Developer or Advanced | It skips, naming Options Developer |
| The deliverable check's adjusted option roots | `AAPL7` (2014-06-10), `DELL1` (2021-11-15), `FF1` (2024-04-15) | The Options plans whose history reaches each day | It records each root it can't read as outside the plan's history |
| `tests/tables/test_followed_read_integration.py` | The week of 2025-06-02 to 2025-06-06 | Every Stocks plan; Basic only until June 2027 | It fails, because its read never finishes |
| The `responses` group of the [live-run program](#live-run-program) | NVDA's daily bars from 2024-06-03 to 2024-06-14 | The Stocks plans whose history reaches the day | It skips the example, naming the Stocks plan it needs |

<a id="debug-log"></a>

### Read the debug log

Every pytest run that selects a live test writes the library's DEBUG log to
`live-debug-logs/<UTC time>-<process id>/debug.log`, which git ignores, and prints its path
under `live debug log`. With `DEEPHAVEN_MASSIVE_LOG_FILE` set, the run logs to that file
instead, at the level `DEEPHAVEN_MASSIVE_LOG` names.

The library keeps the API key and the S3 keys out of every line, and the run scans the log for
them anyway. If it finds one, the same section says where. Don't share that log. A secret in it
is a library bug to fix before the next run.

<a id="checks-to-read-by-hand"></a>

## Read five checks by hand

Five checks record an answer the library's design depends on, and four of them pass whatever
Massive answers, so a pass says only that they ran. Read each one's evidence, compare it with
the answer the library assumes, and open an issue when they differ:

| Check | Run | Plans | The question | The answer the library assumes |
|---|---|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | The first [connection-limit command](#run-order) | Options Advanced | Does Massive's cap of 1,000 per connection count contracts or topics? | Topics |
| `test_an_index_minute_file_holds_its_own_new_york_day` | The [evidence tests](#run-order) | Indices Starter and `s3` | Does each index minute file hold only its own New York day? | Yes. This check asserts it, and fails otherwise. |
| `test_index_day_windows_and_files_across_many_indices` | The evidence tests | Indices Starter and `s3` | Which day, New York or Chicago, do the index day bars from REST and the daily files cover, and do the files match REST? | The Chicago day: an index day bar ends at 01:00 New York time |
| `test_futures_bar_buckets_against_the_bars_inside_them` | The evidence tests | Futures Starter | Does each futures bar of a session or longer hold the whole sessions that start in its UTC days? | Yes |
| A quiet connection | The stream day of the [live-run program](#live-run-program) | Any plan that streams stocks | What does Massive send on a connection that has no data? | None. The check records it. |

<a id="read-the-options-cap"></a>

### Read the options cap

The options check subscribes 1,000 far-dated SPY contracts on two channels, 2,000 topics. If
Massive acknowledges all 2,000, the cap counts contracts. If Massive refuses the topics past
1,000, or closes the socket after a status that says why, the cap counts topics. The check skips
as inconclusive when Massive refuses topics for want of a plan. It also skips when the socket
reconnects without saying why, which a network drop also does, so run the first
connection-limit command again.

<a id="read-the-index-checks"></a>

### Read the index checks

`history()` trims each index minute file to its New York day before it joins the REST rows. If
a file held the Chicago day instead, index ranges would lose or repeat an hour at each
midnight. The minute-file check reads `I:AAVE100`, an around-the-clock index, from the most
recent Monday's file. It records the file's rows outside the New York day, and records minutes
that REST has and the file lacks as gaps in Massive's file, not failures. To run it alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
```

The many-indices check reads about ten indices over four or five recent days, and ends with
three verdict lines:

- Line (a) counts the indices whose REST day bars cover the Chicago day, out of those whose New
  York and Chicago days differ.
- Line (b) splits the indices into regular-hours and around-the-clock groups. For each group,
  it names the indices whose day file covers another day than REST's 1-day bar. It also names
  those whose minute file has gaps or prices that differ from REST.
- Line (c) says whether `I:AAVE100` is the only around-the-clock index whose day file matches
  neither day, or whose minute prices differ from REST.

To run it alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k many_indices 2>&1 | tee -a live-checks.log
```

<a id="read-the-futures-bar-check"></a>

### Read the futures bar check

Massive puts each futures session in the bar of the UTC day it opens. The futures check reads
the front contract of ES, corn, live cattle, and Micro Bitcoin over the most recent whole month,
and prints `VERDICT` lines per contract. The library's model of futures bars holds when:

- every bar holds the whole sessions that start in its UTC days
- 2-session bars follow a fixed grid, rather than starting at the date you ask from
- a 2-month bar comes back the same whether you ask from before its period or from a month
  into it

The lines also show where each 1-session bar starts against its first minute bar. They show
which weekday 1-week bars start on, and whether any session opens on a Friday evening or a
Saturday. To run it alone:

```bash
uv run pytest -m integration tests/namespaces/test_futures_integration.py -k bar_buckets 2>&1 | tee -a live-checks.log
```

<a id="read-a-quiet-connection"></a>

### Read the quiet-connection check

The library warns after 5 minutes without a frame from Massive. The stream day's
[debug log](#debug-log) shows what Massive sends while no data flows. From 03:45 to 04:00 New
York time, the stocks connection holds its subscriptions and gets no data, because stocks trade
from 04:00. Record that connection's `status` lines (`wss://.../stocks status ...`) from those
15 minutes, and the longest stretch without one.

Also record every `no frame for ... during ... trading hours` WARNING from the rest of the day,
with its time and connection. It fires only in a market's regular hours, 09:30 to 16:00 for
stocks. Open an issue with both records.

<a id="user-doc-checks"></a>

## Check the user docs by hand

These README claims need a run with a key, and no live test asserts them. Run each example as
pasted, in the console of a server started as the README's [Install](../README.md#install)
section starts it, and open an issue for each claim the run contradicts:

| README example | Plan | What to record |
|---|---|---|
| Every example | The plan it names | Whether the tables, charts, and panels match the text after the example |
| [1 to 7](../README.md#read-daily-bars), one after another | Stocks Basic | Whether every table fills when an example sends more than five requests in a minute |
| [5, news](../README.md#follow-the-news) | Stocks Basic | Whether `latest` starts with up to ten articles per stock, newest first |
| [6, stopping](../README.md#stop-live-tables) | Stocks Basic | Whether `headlines` and `latest` show the closed-session error and keep their rows, and `kept` holds the same rows |
| [7, watchlist](../README.md#drive-with-a-watchlist) | Stocks Basic | Whether Nvidia's row arrives and Microsoft's leaves at the next poll, with no other change |
| [8, settings file](../README.md#what-your-plan-allows) | Stocks Starter or higher | Whether the snapshot returns rows with your plan's file, and on Starter or Developer, raises `dm.EntitlementError` without it |
| [9, streaming](../README.md#stream-minute-bars) | Stocks Starter or higher | Whether minute bars arrive in pre-market and after-hours trading, and `latest` stays empty overnight. On Currencies Starter, whether the crypto line fills at any hour. |
| [10, change since close](../README.md#change-since-close) | Stocks Starter or higher | Whether each row shows `PrevClose` before the stock's first minute bar, then `Close` and `ChangePct` with each bar |
| [12, backfill](../README.md#backfill-then-live) | Stocks Starter or higher | Whether `spy` has one row per minute of the day so far, with no gap or repeat where history meets the stream |
| [4 and 13, charts](../README.md#chart-closes) | The plan each names | Whether the chart of `Date` text draws a date axis, and the dashboard opens with both panels and its chart grows each minute |

<a id="live-run-program"></a>

## Run the live-run program

`scripts/live_run.py` records every live check a key allows, unattended, on a fixed New York
timetable. A full pass takes at least three trading days. The first is a stream day, a whole
trading day of live streams from 03:45 to 20:30, then the day after it, and then an ordinary
day. A key that streams
crypto or futures also needs a stream night, of those streams from 16:30 to 09:30. The program
asks Massive for your plans when it starts and each trading morning, and reruns the checks a new
or raised plan allows.

In a terminal at the clone's root, with your keys set:

```bash
uv run python scripts/live_run.py check                       # the computer and the key; writes nothing
uv run python scripts/live_run.py run --out ~/massive-live    # runs until Control-C; add --any-time on a first run
uv run python scripts/live_run.py status --out ~/massive-live # from a second terminal
uv run python scripts/live_run.py pack --out ~/massive-live   # after Control-C; writes live-results-<date>.zip
```

- `check` prints an `OK`, `WARN`, or `FAIL` line for each part of the computer, then each
  subscription's plan and how far back its history goes, then `ready: yes` or `ready: no`.
- `run` with `--any-time` starts the groups that need no market hours at once, such as
  `rate-limits`, `evidence`, `smoke`, and `responses`. The option counts only the first time you
  give it for a results folder.
- `status` prints the latest plan check, each check's status and reason, and a count per group.
- `pack` refuses unless your terminal holds the same keys the run used, and refuses to zip a
  folder that holds a key.

Run `upgrade-check` only when the owner asks. It asks you to upgrade a plan while it runs, and
records whether the library stops pacing that subscription. The
[tester guide](tester-guide.md#live-run) walks through a run step by step.

<a id="live-run-groups"></a>

### Group timetable

The program runs its checks in groups, each at a set New York time:

| Group | What it runs | When (New York time) |
|---|---|---|
| `rate-limits` | `-m integration_rate_limits` | From 11:30 |
| `evidence` | `-m integration -k "not luld"` | From 11:30, after the previous day's flat files |
| `luld` | `-m integration -k luld` | 09:28, for the opening bands, except on a stream day and the day after |
| `smoke` | `-m integration_smoke` | From 11:30 |
| `responses` | The documentation's examples, each in its own session, with every raw reply recorded | From 11:30 |
| `limits-ws` | The WebSocket connection-limit tests | From 16:15 |
| `limits-second-connection` | The second-connection test, which runs last with nothing else open | From 11:30, after the groups before it |
| `stream-day` | A full trading day of streams for every asset class the key can stream | 03:45 to 20:30 on a planned day |
| `throughput` | Every symbol's busiest streams, measured over an hour | 09:15 to 11:00 the next day |
| `stream-day-reconcile` | The streamed minute bars against REST history | From 11:30 the next day |
| `stream-night` | Crypto and futures streams through one night | 16:30 on a planned evening to 09:30 the next morning |
| `stream-night-reconcile` | The night's minute bars against REST history | From 11:30 the morning after |
| `half-day` | The bar-stamp tests | On the trading day after a half day |

Each start of a group is one attempt, with its own folder, `<group>/<date>-<n>/`, in the results
folder, holding the attempt's debug log. A stream day or night keeps one folder for all its
starts. While it waits for the next group, `run` packs a fresh zip whenever a group has finished
since the last one.

<a id="live-run-reruns"></a>

### Reruns

The program reruns a check by itself, depending on its last result:

| Last result | When it runs again |
|---|---|
| Passed | Only when a subscription's plan rises |
| Failed | Once more |
| Skipped for a missing subscription, or refused | When the subscriptions change |
| Skipped for the moment, such as a closed market | Up to three attempts in all |

To run every check of a group again, failed ones included, pass its name to `run --redo`, such
as `--redo luld,smoke`.

<a id="stop-and-resume-the-program"></a>

### Stop and resume the program

Control-C, a closed terminal, and `kill` all stop the program the same way, and the same `run`
command resumes it with every result kept. It refuses to resume a results folder on a different
commit, so pulling `main` between starts blocks the resume.

<a id="stream-night"></a>

### Stream nights

A stream night covers the hours only 24-hour markets trade, after a stream day ends at 20:30.
When the key streams crypto or futures, the program plans one on the first suitable evening
before a full trading day. It streams the trades, quotes, and minute bars of `X:BTCUSD` and
`X:ETHUSD`, and the minute bars of the front ES and NQ contracts. From Futures Developer up, it
streams their trades and quotes too.

The night checks the two date boundaries in those hours. Crypto's `Date` changes at 00:00 UTC,
and a futures row's `Date` changes to the next session at the 16:00 Chicago close. The night
records separate results for crypto and futures, and each fails when:

- one of its streams failed
- its gaps passed 30 minutes in all
- the recorder couldn't write a batch of rows
- an hour its market was open brought no row
- a row near a boundary carries the wrong `Date`

The next day, `stream-night-reconcile` compares the night's minute bars with REST history, and
records a failure for crypto or futures if any bar's `Date` differs. The files `hours.csv`,
`boundaries.csv`, and `summary.json` hold the night's evidence, and `REPORT.md` in each zip has
a section per night.

<a id="secrets"></a>

### Secrets in the results

The program reads the keys only from the environment, and never writes them. Its output and the
text files it writes pass through a filter that replaces each secret with `***`. Before it zips
anything, `pack` scans every file of the results folder for each secret, plain, URL-encoded,
and base64, and refuses if it finds one. It also checks your keys against the salted
fingerprints `run` stored.
