# Live integration tests

The live tests call Massive's real servers with your keys, and record what Massive sends. This
is the owner's runbook for running them with pytest and reading what they record. Testers run
the same tests through the live-run program, and send its results zip on an issue.

<a id="run-the-tests"></a>

## Run the tests

Run the live tests, the files `tests/**/test_*_integration.py`, in a clone set up as the tester
guide's [Set up the test](tester-guide.md#set-up) leaves it. Run each command in a terminal at
the clone's root. Each pytest marker selects one set of tests:

| Marker | What it runs |
|---|---|
| `integration` | The evidence tests, most of which answer an open question |
| `integration_smoke` | Every dataset and method once, checked automatically |
| `integration_rate_limits` | The three rate-limit tests, which go over Basic's 5 requests a minute on purpose |
| `integration_limits` | The four connection-limit tests, which go over Massive's subscription and connection limits on purpose |

`-m integration` leaves out the tests that also carry another of these markers.

<a id="variables"></a>

### Set your keys

Set `MASSIVE_API_KEY`, and the two S3 keys if your plans include flat files, as the README's
[Set your API keys](../README.md#set-your-api-keys) step shows. Two more variables are
optional:

| Variable | What you get | Without it |
|---|---|---|
| `MASSIVE_DELAYED_API_KEY` | Two checks of a delayed Stocks key. With your main key on Stocks Developer, set it to a key on Stocks Basic or Starter. With your main key on Stocks Advanced or higher, set it to a key on Stocks Starter. On Stocks Basic or Starter, leave it unset. | The checks it can't serve skip |
| `MASSIVE_LIVE_PLANS` | Your plans, declared instead of asked of Massive ([Declare your plans](#declare-the-plans)) | The run asks Massive |

<a id="run-order"></a>

### Run the commands in order

The connection-limit tests run last and alone, because they disturb the key's connections:

1. Stop every other program that uses the key.
2. Run the evidence, smoke, and rate-limit tests. A weekday run in market hours covers more,
   because a stream test checks rows only while its market is open. The rate-limit tests run
   only while one asset class is still on Basic:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

3. Run the connection-limit tests, one command after the other:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

Every command adds its output to `live-checks.log`, the file to attach to an issue.

<a id="plans"></a>

## Let the run find your plans

The first live test of each run asks Massive which plan your key holds in each asset class.
This check, the probe, decides which tests run and which skip. Its answer opens the
`open questions` section of the summary, in a line that starts `plans:`.

The run saves the answer in `.live-plans.json` at the repository root and reuses it for an
hour. To probe again at once, after an upgrade, delete the file.

<a id="declare-the-plans"></a>

### Declare your plans

Declare your plans in `MASSIVE_LIVE_PLANS` when the probe can't tell them:

| Case | What you declare |
|---|---|
| The run stops and asks you to | Every paid plan you hold. Between 03:30 and 04:30 New York time, you can wait until 04:30 instead. |
| Stocks or Options Business with the trades and quotes expansion | Every paid plan you hold, with `business_expansion` for that asset class |
| Indices Business | Every paid plan you hold, with `indices=business` |
| Futures Business | Every paid plan you hold, with `futures=business` |

List each paid asset class as `<class>=<plan>`, separated by commas, and add `s3` when both S3
keys reach the flat files. An asset class you leave out is on Basic. The plan names are:

| Asset class | Plans you can declare |
|---|---|
| `stocks`, `options` | `starter`, `developer`, `advanced`, `business`, `business_expansion` |
| `futures` | `starter`, `developer`, `advanced`, `business` |
| `indices` | `starter`, `advanced`, `business` |
| `currencies` | `starter`, `business` |

Add the line to your shell's startup file, beside your keys, and open a new terminal. Remove it
when your plans change, because it replaces the probe. This one
declares Starter on Stocks, Options, and Currencies, and flat files:

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

<a id="reading-the-results"></a>

## Read the results

A test fails when:

- a request it needs fails or returns nothing
- a stream delivers no row while rows are due
- Massive refuses a request although your plans, probed or declared, include the plan the test
  needs
- a `verifies` test gets an answer other than the one the library assumes

A refusal of data your plans include means your plans don't match what Massive serves. Check
`MASSIVE_LIVE_PLANS` first, then open an issue with the refusal. For any other failure, open an
issue with `live-checks.log`.

A skip prints its reason, and needs nothing from you. A test skips when:

- your key lacks a plan it needs, and the skip names the plan with its price, such as
  "needs Currencies Starter ($49/month)"
- `MASSIVE_API_KEY`, the S3 keys, or the delayed key isn't set, or `CI` is set
- the day it asks for is older than your plan keeps
- the market isn't open, or the test can't tell its answer from a network drop

A pass of a `settles` test still leaves evidence to read, which the summary lists under
`ran, evidence to read`. A `verifies` test asserts its answer instead, so it fails the day
Massive changes it. Both name the
[open questions](dev/verification-items.md) they answer by number.

<a id="read-the-end-of-a-run"></a>

### Read the end of a run

Each command writes its evidence to `live-evidence-<UTC time>.log` at the repository root, line
by line, so a run cut short keeps what it recorded. Before pytest's short summary, the run
prints up to four sections:

| Section | What it holds |
|---|---|
| `smoke coverage` | The smoke tests that passed, passed with drift, weren't entitled, were skipped, and failed. Drift is a library WARNING or a `SchemaDrift`, each listed. |
| `open questions` | The `plans:` line, then the questions the run touched, in groups: verified, ran with evidence to read, needing a plan you lack, and not verified |
| `live debug log` | The path of the run's debug log |
| `live evidence` | The evidence file's lines again |

<a id="older-history"></a>

### Tests that need older history

Four tests read fixed dates. Every other test reads recent days, within the two years the free
plans keep:

| Test | Fixed date | Without a plan that keeps it |
|---|---|---|
| The option split check | NVDA's split of 2024-06-10 | It skips, naming Options Developer |
| The deliverable check's adjusted option roots | `AAPL7` (2014-06-10), `DELL1` (2021-11-15), `FF1` (2024-04-15) | It records each root it can't read as outside the plan's history |
| `tests/tables/test_followed_read_integration.py` | The week of 2025-06-02 | It fails on Stocks Basic from June 2027, because its read fails |
| The `responses` group of the [live-run program](#live-run-program) | NVDA's daily bars from 2024-06-03 to 2024-06-14 | It skips the example, naming the Stocks plan it needs |

<a id="debug-log"></a>

### Read the debug log

The run writes the library's DEBUG log to `live-debug-logs/<UTC time>-<process id>/debug.log`,
and prints its path under `live debug log`. With `DEEPHAVEN_MASSIVE_LOG_FILE` set, it writes to
that file instead, at the level `DEEPHAVEN_MASSIVE_LOG` names.

The run scans the log for your keys, unless you set `DEEPHAVEN_MASSIVE_LOG_FILE`. If it finds
one, the same section says where. Don't share that log, and open an issue that names the file.

<a id="checks-to-read-by-hand"></a>

## Read five checks by hand

Four of these checks pass whatever Massive answers, so read each one's evidence:

| Check | Plans | What to read | Open an issue when |
|---|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | Options Advanced | Whether Massive acknowledges all 2,000 topics of 1,000 contracts | Massive acknowledges them all, since the library assumes the cap counts topics |
| `test_an_index_minute_file_holds_its_own_new_york_day` | Indices Starter and `s3` | The minutes REST has that the file lacks | Any minute is missing |
| `test_index_day_windows_and_files_across_many_indices` | Indices Starter and `s3` | Its verdict lines (a), (b), and (c) | Always: index day bars end at 01:00 New York time until this answer is read |
| `test_futures_bar_buckets_against_the_bars_inside_them` | Futures Starter | Its `VERDICT` lines | A line differs from the library's model: each bar holds the whole sessions that open in its UTC days |
| A quiet connection, in the [`stream-day` group](#live-run-groups) of a tester's results | Any plan that streams stocks | In that group's debug log, the stocks connection's `status` lines from 03:45 to 04:00 New York time, and every `no frame for ... during ... trading hours` WARNING | Always, with the longest gap between `status` lines |

The options check skips when Massive refuses the topics for want of a plan. It also skips as
inconclusive when the socket reconnects without saying why. Then run the first connection-limit
command again.

To run one of the other checks alone, use `-k` with a word of its name:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k many_indices 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_futures_integration.py -k bar_buckets 2>&1 | tee -a live-checks.log
```

<a id="user-doc-checks"></a>

## Check the user docs by hand

These claims in the user docs need a run with a key, and no live test asserts them. Run each
README example as pasted, in the console of a server started as the README's
[Install](../README.md#install) section starts it. Open an issue for each claim the run
contradicts:

| Where | Plan | What to record |
|---|---|---|
| Every README example | The plan it names | Whether the tables, charts, and panels match the text after the example |
| [README 1 to 7](../README.md#read-daily-bars), one after another | Stocks Basic | Whether every table fills when the examples, run back to back, send more than five requests in a minute |
| [README 5, news](../README.md#follow-the-news) | Stocks Basic | Whether `latest` starts with up to ten articles per stock, newest first |
| [README 6, stopping](../README.md#stop-live-tables) | Stocks Basic | Whether `headlines` and `latest` show the closed-session error and keep their rows, and `kept` holds the same rows |
| [README 7, watchlist](../README.md#drive-with-a-watchlist) | Stocks Basic | Whether Nvidia's row arrives and Microsoft's leaves at the next poll, with no other change |
| [README 8, settings file](../README.md#what-your-plan-allows) | Stocks Starter or higher | Whether the snapshot returns rows with your plan's file, and on Starter or Developer, raises `dm.EntitlementError` without it |
| [README 9, streaming](../README.md#stream-minute-bars) | Stocks Starter or higher | Whether minute bars arrive in pre-market and after-hours trading, and `latest` stays empty overnight. On Currencies Starter, whether the crypto line fills at any hour. |
| [README 10, change since close](../README.md#change-since-close) | Stocks Starter or higher | Whether each row shows `PrevClose` before the stock's first minute bar, then `Close` and `ChangePct` with each bar |
| [README 12, backfill](../README.md#backfill-then-live) | Stocks Starter or higher | Whether `spy` has one row per traded minute of the day so far, with no gap or repeat where history meets the stream |
| [README 4 and 13, charts](../README.md#chart-closes) | The plan each names | Whether Example 4's chart draws Nvidia's closes for the year, and the dashboard opens with both panels and its chart grows each minute |
| [`GreeksGamma`](datasets/options.snapshots.md) of `m.options.snapshots` | Options Starter or higher, with `delayed_subscriptions` listing options on Starter or Developer | For one contract, enter the underlying's price, the strike, the days to expiration, and `ImpliedVolatility` into a standard Black-Scholes calculator, which gives gamma per $1 move of the underlying. Record whether `GreeksGamma` matches that, or a hundredth of it, per $0.01 move as Massive's description says. |

<a id="live-run-program"></a>

<a id="live-run-groups"></a>

## Read a tester's results

Testers run the live tests through `scripts/live_run.py`, as the
[tester guide](tester-guide.md#live-run) shows, and send its `live-results-<date>.zip` on their
results issue. A comment on that issue can ask them to pack the results again, to rerun a group
as [Reruns](#live-run-reruns) shows, or to run the [upgrade check](tester-guide.md#upgrade-check).

The program runs the tests in groups, and the zip holds a folder for each attempt of a group,
`<group>/<date>-<n>/`, with its debug log:

| Group | What it runs |
|---|---|
| `rate-limits` | `-m integration_rate_limits` |
| `evidence` | `-m integration -k "not luld"` |
| `luld` | `-m integration -k luld`, at 09:28 New York time for the opening bands |
| `smoke` | `-m integration_smoke` |
| `responses` | A fixed set of library calls, with every raw reply recorded |
| `limits-ws` | The WebSocket subscription-limit tests |
| `limits-second-connection` | The second-connection test |
| `stream-day` | Streams for every asset class the key can stream, from 03:45 to 20:30 New York time |
| `throughput` | Every symbol's busiest streams, over an hour |
| `stream-day-reconcile` | The stream day's minute bars against REST history |
| `stream-night` | Crypto and futures streams, from 16:30 to 09:30 New York time |
| `stream-night-reconcile` | The night's minute bars against REST history |
| `half-day` | The bar-stamp tests, on the trading day after a half day |

<a id="live-run-reruns"></a>

### Reruns

The program reruns a check by itself:

| Last result | When it runs again |
|---|---|
| Passed | When a subscription's plan rises |
| Failed | Once more |
| Skipped for a missing subscription, or refused | When the subscriptions change |
| Skipped for the moment, such as a closed market | Up to three attempts in all |

To have every check of a group run again, failed ones included, ask the tester to stop the run
and resume it with `--redo` and the group:

```bash
uv run python scripts/live_run.py run --out ~/massive-live --redo luld,smoke
```
