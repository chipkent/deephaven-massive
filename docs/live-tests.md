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
| `integration_rate_limits` | The three rate-limit tests, which send a subscription more requests a minute than Massive allows, on purpose |
| `integration_limits` | The four connection-limit tests, which go over Massive's subscription and connection limits on purpose |
| `integration_all_symbols` | The test that streams every futures trade for 10 minutes, taking over your key's futures connection the whole time |

`-m integration` leaves out the tests that also carry another of these markers.

<a id="variables"></a>

### Set your keys

Set `MASSIVE_API_KEY`, and the two S3 keys if your key reaches the flat files, as the README's
[Set your API keys](../README.md#set-your-api-keys) step shows.

Then point `DEEPHAVEN_MASSIVE_CONFIG_FILE` at your settings file, the one your own sessions
load ([settings file](configuration.md)). The run takes three settings from it and leaves every
other setting at its default:

| Setting | List in it | Without it |
|---|---|---|
| `delayed_subscriptions` | Each subscription Massive serves your key delayed | Every stream and snapshot test of those subscriptions skips, `not listed as delayed` |
| `rest_requests_per_minute` | Each subscription whose REST requests Massive limits by the minute, with its limit, so its requests are paced from the first | Each one's first refused request pauses its requests while the library learns the limit, and the rate-limit tests skip |
| `websocket_hosts` | A WebSocket host you pin for an asset class | The library picks the host |

Add the variable to your shell's startup file, beside your keys, and open a new terminal:

```bash
export DEEPHAVEN_MASSIVE_CONFIG_FILE="$HOME/massive/settings.yaml"
```

A settings file the library refuses stops the run at the first test that loads it, with the
library's message.

<a id="run-order"></a>

### Run the commands in order

Run the connection-limit tests after the others, and alone, because they disturb the key's
connections:

1. Stop every other program that uses the key.
2. Run the question, smoke, and rate-limit tests:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   The command also runs every README example as written and checks it against its text. Run it
   on a weekday from 09:30 to 16:00 New York time, because stream tests check rows only while
   their market trades.

   The rate-limit tests use up the request limit of one subscription in
   `rest_requests_per_minute`. They use indices if the file lists it, else futures, then options,
   stocks, and currencies. The ticker-lookup test uses indices, else currencies, else stocks.
   They skip when the file lists none of them.

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

<a id="reading-the-results"></a>

## Read the results

Every live test sends its requests whatever your key holds, and Massive's answer decides what
happens. A test skips, and prints its reason, when:

- Massive refuses a request. The reason starts `refused:` and quotes the status, the request,
  and Massive's text. A refusal of an old date also names the earliest day the
  request asked for.
- Massive labels the data delayed and your settings file doesn't list its subscription in
  `delayed_subscriptions`. The reason starts `not listed as delayed:` and names the setting to
  add.
- A stream of a subscription your settings file lists as delayed delivers no row while its
  market is open. The delayed feed's lag is Massive's, so the test can't tell when a row is due.
- Your key isn't the kind of key the test's question is about. Four tests ask what Massive
  does with a key whose stocks data it labels delayed, or real-time, and run on your key: each
  first reads how Massive labels your stocks data, and skips, quoting Massive's label, when it
  isn't the one the question needs. `test_what_massive_says_to_a_delayed_key_on_the_real_time_feed`
  also skips when Massive streams your key from the real-time feed with no error, naming the
  rows it sent. When Massive refuses that stream, the test passes, and its evidence quotes the
  refusal, the answer its question asks for.
- `MASSIVE_API_KEY` or the S3 keys aren't set, or `CI` is set.
- The market isn't open, or the test can't tell its answer from a network drop.

A test fails when:

- a request fails, other than by Massive's refusal, or returns nothing
- a stream on a real-time feed delivers no row while rows are due
- a test that asserts the answer the library assumes gets another answer

Open an issue with `live-checks.log` for each failure. Read each refusal too, and open an issue
for a refusal of data your Massive dashboard shows your key holds.

A pass can still leave evidence to read. A `settles` test records Massive's answer for you to
read, and a `verifies` test asserts the answer the library assumes. The summary lists a
question under `ran, evidence to read` once its tests all pass and none of them asserts its
answer. Both kinds name their questions by number.

<a id="pacing"></a>

### Pace a limited subscription

A subscription Massive limits costs time. At five requests a minute, a test that sends a
subscription 24 requests runs for about five minutes. While a request waits, a line counts
down the requests left.

<a id="read-the-end-of-a-run"></a>

### Read the end of a run

Each command writes its evidence to `live-evidence-<UTC time>.log` at the repository root, line
by line, so a run cut short keeps what it recorded. Before pytest's short summary, the run
prints up to four sections:

| Section | What it holds |
|---|---|
| `smoke coverage` | The smoke tests that passed, passed with drift, were refused, were skipped, and failed. Refusals are grouped by subscription and call, with Massive's text once. Drift is a library WARNING or a `SchemaDrift`, each listed. |
| `open questions` | The questions the run touched, in groups: `verified`, `ran, evidence to read`, `refused by Massive`, `not listed as delayed`, and `not verified`. Under each refused or unlisted question, each distinct reason follows, Massive's refusal or the library's message quoted. |
| `live debug log` | The path of the run's debug log |
| `live evidence` | The evidence file's lines again |

<a id="older-history"></a>

### Tests that need older history

A check that reads a fixed date older than your key's history, such as NVDA's split of
2024-06-10, quotes Massive's refusal and the earliest day it asked for.

<a id="debug-log"></a>

### Read the debug log

The run writes the library's DEBUG log to `live-debug-logs/<UTC time>-<process id>/debug.log`,
and prints its path under `live debug log`. With `DEEPHAVEN_MASSIVE_LOG_FILE` set, it writes to
that file instead, at the level `DEEPHAVEN_MASSIVE_LOG` names.

The run scans the log for your keys, unless you set `DEEPHAVEN_MASSIVE_LOG_FILE`. If it finds
one, the same section says where. Don't share that log, and open an issue that names the file.

<a id="checks-to-read-by-hand"></a>

## Read six checks by hand

A pass doesn't settle these checks. Read each one's evidence, and open an issue as its row
says:

| Check | Data it reads | What to read | Open an issue when |
|---|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | Option quote and trade streams | Whether Massive acknowledges all 2,000 topics of 1,000 contracts | Massive acknowledges them all, since the library assumes the cap counts topics |
| `test_an_index_minute_file_holds_its_own_new_york_day` | Index minute bars, over REST and in the flat files | The minutes REST has that the file lacks | Any minute is missing |
| `test_index_day_windows_and_files_across_many_indices` | Index day and minute bars, over REST and in the flat files | Its verdict lines (a), (b), and (c) | Always. The library ends index day bars at 01:00 New York time, and its verdict lines say whether that's right. |
| `test_futures_bar_buckets_against_the_bars_inside_them` | Futures bars | Its `VERDICT` lines | A line differs from the library's model: each bar holds the whole sessions that open in its UTC days |
| `test_snapshot_gamma_against_black_scholes_per_dollar_and_per_cent` | Option chain snapshots | Each call's ratio of `GreeksGamma` to Black-Scholes gamma, and the unit it matches | A call matches a unit other than per $0.01, the unit Massive's description gives |
| A quiet connection, in the [`stream-day` group](#live-run-groups) of a tester's results | Stock streams | In that group's debug log, the stocks connection's `status` lines from 03:45 to 04:00 New York time, and every `no frame for ... during ... trading hours` WARNING | Always, with the longest gap between `status` lines |

The options check skips when Massive refuses the topics, quoting the refusal. It also skips as
inconclusive when the socket reconnects without saying why. Then run the first
connection-limit command again.

The quiet-connection check comes only from a tester's results. To run one of the others alone,
use `-k` with a word of its name:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k many_indices 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_futures_integration.py -k bar_buckets 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_options_integration.py -k gamma 2>&1 | tee -a live-checks.log
uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py -k options_cap 2>&1 | tee -a live-checks.log
```

<a id="index-hosts"></a>

## Check the index hosts by hand

No live test covers this check. Run it in the console of a server started as the README's
[Install](../README.md#install) section starts it, and open an issue for each claim it
contradicts:

| Where | What to record |
|---|---|
| [`m.indices.bars`](datasets/indices.bars.md) and [`m.indices.values`](datasets/indices.values.md) of `I:SPX`: `m.indices.bars("I:SPX", dm.Timespan.MINUTE).last_by()` and `m.indices.values("I:SPX").last_by()` | Massive documents `delayed.massive.com` and `socket.massive.com` for index bars, but only `business.massive.com` for index values, and the library allows that host only for real-time streams. On a key Massive serves index data delayed, set `delayed_subscriptions={dm.Subscription.INDICES}`, and expect a bar each minute in market hours. Then add `websocket_hosts={dm.AssetClass.INDICES: "business.massive.com"}`, and record whether the library refuses it, with the exact message. Expect a refusal; open an issue with the message, since such a key then has no documented host for index values. On a key Massive serves index data in real time, set nothing for indices and expect bars; then set that host and expect values. Open an issue for any table that stays empty in market hours. |

<a id="live-run-program"></a>

<a id="live-run-groups"></a>

## Read a tester's results

Testers run the live tests through `scripts/live_run.py`, as the
[tester guide](tester-guide.md#live-run) shows, and send its `live-results-<date>.zip` on their
results issue. Open the zip's `REPORT.md` first. It gives:

- the tester's environment, and the probes over time
- a table of each group's checks
- what Massive refused, by day, with its text
- each open question the checks answer, with its status
- the checks still unverified

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
| `stream-day` | Streams for every asset class, from 03:45 to 20:30 New York time |
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
| Passed | Not again |
| Failed | Once more |
| Refused by Massive, or skipped as not listed as delayed | At its group's first attempt each day, and that evening once another check of its subscription stops being refused |
| Skipped for the moment, such as a closed market | Up to three attempts in all |

To have every check of a group run again, failed ones included, ask the tester to stop the run
and resume it with `--redo` and the group:

```bash
cd ~/massive-test/deephaven-massive
uv run python scripts/live_run.py run --out ~/massive-live --redo luld,smoke
```
