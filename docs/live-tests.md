# Live integration tests

The live tests call Massive's real servers with your keys, and record what Massive sends. This
is the owner's runbook for running them with pytest and reading what they record. Testers run
them, and further checks, through the live-run program, `scripts/live_run.py`, and send its
results zip on an issue.

<a id="run-the-tests"></a>

## Run the tests

Run the live tests, the files `tests/**/test_*_integration.py`, in a checkout set up as
[CONTRIBUTING.md](../CONTRIBUTING.md#set-up) says. Run each command in a terminal at the
checkout's root. Each pytest marker selects one set of tests:

| Marker | What it runs |
|---|---|
| `integration` | The question tests, most of which record or check Massive's answer to an open question, a behavior the library depends on that Massive's docs leave open ([the list](dev/verification-items/README.md)) |
| `integration_smoke` | Every dataset and method once, checked automatically |
| `integration_rate_limits` | The three rate-limit tests, which go over Basic's 5 requests a minute on purpose |
| `integration_limits` | The four connection-limit tests, which go over Massive's subscription and connection limits on purpose |
| `integration_all_symbols` | The test that streams every futures trade for 10 minutes, taking over your key's futures connection the whole time |

`-m integration` leaves out the tests that also carry another of these markers.

<a id="variables"></a>

### Set your keys

Set `MASSIVE_API_KEY`, and the two S3 keys if your plans include flat files, as the README's
[Set your API keys](../README.md#set-your-api-keys) step shows. Two more variables are
optional:

| Variable | What you get | Without it |
|---|---|---|
| `MASSIVE_DELAYED_API_KEY` | Three checks of a delayed Stocks key. With your main key on Stocks Developer, set it to a key on Stocks Basic or Starter. With your main key on Stocks Advanced or higher, set it to a key on Stocks Starter. On Stocks Basic or Starter, leave it unset. | The checks it can't serve skip |
| `MASSIVE_LIVE_PLANS` | Your plans, declared instead of asked of Massive ([Declare your plans](#declare-the-plans)) | The run asks Massive |

<a id="run-order"></a>

### Run the commands in order

The connection-limit tests run last and alone, because they disturb the key's connections:

1. Stop every other program that uses the key.
2. Run the question, smoke, and rate-limit tests:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   A weekday run in market hours covers more, because a stream test checks rows only while its
   market is open. The rate-limit tests run only while one asset class is still on Basic.

   The command also runs every README example as written and checks it against its text. Its
   streaming examples check the most from 09:50 to 15:55 New York time.

3. Run the connection-limit tests, one command after the other:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

4. When you want it, run the all-symbols test while futures trade. It streams for 10 minutes
   and skips at once while the futures market is closed:

   ```bash
   uv run pytest -m integration_all_symbols tests/ingest/test_futures_calendar_integration.py 2>&1 | tee -a live-checks.log
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

| Case | Declare it as |
|---|---|
| The run stops and asks you to | Your plans. Between 03:30 and 04:30 New York time, you can wait until 04:30 instead. |
| Stocks or Options Business + Expansion | `business_expansion` for that asset class |
| Indices Business | `indices=business` |
| Futures Business | `futures=business` |

A declaration lists every paid plan you hold, not only the one the probe misses.
List each paid asset class as `<class>=<plan>`, separated by commas, and add `s3` when both S3
keys reach the flat files. An asset class you leave out is on Basic. The plan names are:

| Asset class | Plans you can declare |
|---|---|
| `stocks`, `options` | `starter`, `developer`, `advanced`, `business`, `business_expansion` |
| `futures` | `starter`, `developer`, `advanced`, `business` |
| `indices` | `starter`, `advanced`, `business` |
| `currencies` | `starter`, `business` |

This line declares Starter on Stocks, Options, and Currencies, and flat files. Add yours to your
shell's startup file, beside your keys, and open a new terminal. Remove it when your plans
change, because it replaces the probe:

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
- a test that asserts the answer the library assumes gets another answer

A refusal of data your plans include means your plans don't match what Massive serves. Check
`MASSIVE_LIVE_PLANS` first, then open an issue with the refusal. For any other failure, open an
issue with `live-checks.log`.

A skip prints its reason. A test skips when:

- your key lacks a plan it needs, and the skip names the plan with its price, such as
  "needs Currencies Starter ($49/month)"
- `MASSIVE_API_KEY`, the S3 keys, or the delayed key isn't set, or `CI` is set
- the day it asks for is older than your plan keeps
- the market isn't open, or the test can't tell its answer from a network drop

A pass can still leave evidence to read. A `settles` test records Massive's answer for you to
read, and a `verifies` test asserts the answer the library assumes. The summary lists a question under `ran, evidence to read` once its tests all
pass and none of them asserts its answer. Both kinds name their questions by number.

<a id="read-the-end-of-a-run"></a>

### Read the end of a run

Each command writes its evidence to `live-evidence-<UTC time>.log` at the repository root, line
by line, so a run cut short keeps what it recorded. Before pytest's short summary, the run
prints up to four sections:

| Section | What it holds |
|---|---|
| `smoke coverage` | The smoke tests that passed, passed with drift, weren't entitled, were skipped, and failed. Drift is a library WARNING or a row of `m.problems()`, each listed. |
| `open questions` | The `plans:` line, then the questions the run touched, in groups: verified, ran with evidence to read, needing a plan you lack, and not verified |
| `live debug log` | The path of the run's debug log |
| `live evidence` | The evidence file's lines again |

<a id="older-history"></a>

### Tests that need older history

Three tests, and the live-run program's `responses` group, read fixed dates. Every other test
reads recent days, within the two years the free plans keep:

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

## Read six checks by hand

Read each one's evidence. The index minute-file check fails when its file disagrees with the New
York day or with REST, and the others pass whatever Massive answers:

| Check | Plans | What to read | Open an issue when |
|---|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | Options Advanced | Whether Massive acknowledges all 2,000 topics of 1,000 contracts | Massive acknowledges them all, since the library assumes the cap counts topics |
| `test_an_index_minute_file_holds_its_own_new_york_day` | Indices Starter and `s3` | The minutes REST has that the file lacks | Any minute is missing |
| `test_index_day_windows_and_files_across_many_indices` | Indices Starter and `s3` | Its verdict lines (a), (b), and (c) | Always: index day bars end at 01:00 New York time until this answer is read |
| `test_futures_bar_buckets_against_the_bars_inside_them` | Futures Starter | Its `VERDICT` lines | A line differs from the library's model: each bar holds the whole sessions that open in its UTC days |
| `test_snapshot_gamma_against_black_scholes_per_dollar_and_per_cent` | Options Starter | Each call's ratio of `GreeksGamma` to Black-Scholes gamma, and the unit it matches | A unit isn't per $0.01, as Massive's description says |
| A quiet connection, in the [`stream-day` group](#live-run-groups) of a tester's results | Any plan that streams stocks | In that group's debug log, the stocks connection's `status` lines from 03:45 to 04:00 New York time, and every `no frame for ... during ... trading hours` WARNING | Always, with the longest gap between `status` lines |

The options check skips when Massive refuses the topics for want of a plan. It also skips as
inconclusive when the socket reconnects without saying why, and then needs the first
connection-limit command run again.

To run one of the other checks alone, use `-k` with a word of its name:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k many_indices 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_futures_integration.py -k bar_buckets 2>&1 | tee -a live-checks.log
```

<a id="other-plans"></a>

## Tests that run only on other plans

These tests skip unless the run's plans match theirs:

| Test | Runs only with |
|---|---|
| `test_the_free_examples_fill_on_stocks_basic` | The main key on Stocks Basic |
| `test_tester_example_10_fails_the_live_minute_bars_on_basic` | The main key on Stocks Basic |
| `test_the_settings_example_without_its_setting_is_refused_on_a_delayed_plan` | A delayed Stocks plan: the main key on Starter or Developer, or `MASSIVE_DELAYED_API_KEY` |
| `test_admin_messages_on_a_plan_without_them_name_the_plans_that_have_them` | The main key on Options Starter, Developer, or Business |

<a id="index-hosts"></a>

## Check the index hosts by hand

No live test covers this check. Run it in the console of a server started as the README's
[Install](../README.md#install) section starts it, and open an issue for each claim it
contradicts:

| Where | Plan | What to record |
|---|---|---|
| [`m.indices.bars`](datasets/indices.bars.md) and [`m.indices.values`](datasets/indices.values.md) of `I:SPX`: `m.indices.bars("I:SPX", dm.Timespan.MINUTE).last_by()` and `m.indices.values("I:SPX").last_by()` | Indices Starter, then Advanced or Business | Massive documents `delayed.massive.com` and `socket.massive.com` for index bars, but only `business.massive.com` for index values, and the library allows that host only for real-time streams. On Starter, set `delayed_subscriptions={dm.Subscription.INDICES}`, and expect a bar each minute in market hours. Then add `websocket_hosts={dm.AssetClass.INDICES: "business.massive.com"}`, and record whether the library refuses it, with the exact message. Expect a refusal; open an issue with the message, since Starter then has no documented host for index values. On Advanced or Business, set nothing for indices and expect bars; then set that host and expect values. Open an issue for any table that stays empty in market hours. |

<a id="live-run-program"></a>

<a id="live-run-groups"></a>

## Read a tester's results

Testers run the live tests through `scripts/live_run.py`, as the
[tester guide](tester-guide.md#live-run) shows, and send its `live-results-<date>.zip` on their
results issue. Open the zip's `REPORT.md` first. It gives the tester's environment, the plans
probed over time, a table of each group's checks, each open question the checks answer with its
status, and the checks still unverified.
`checks.csv` has one line per check. A comment on that issue can ask them to pack the results again, to rerun a group
as [Reruns](#live-run-reruns) shows, or to run the [upgrade check](tester-guide.md#upgrade-check).

Each attempt of a group has a folder in the zip, `<group>/<date>-<n>/`, with its debug log. The
groups run these tests:

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
cd ~/massive-test/deephaven-massive
uv run python scripts/live_run.py run --out ~/massive-live --redo luld,smoke
```
