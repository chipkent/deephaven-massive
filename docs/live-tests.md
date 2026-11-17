# Live integration tests

The live tests call Massive's real servers with your keys, and record what Massive sends. This
is the owner's runbook for running them with pytest and reading what they record. Testers run
them, and further checks, through the live-run program, `scripts/live_run.py`, and send its
results zip on an issue.

<a id="run-the-tests"></a>

## Run the tests

The live tests are the files `tests/**/test_*_integration.py`. Run them in a checkout set up as
[CONTRIBUTING.md](../CONTRIBUTING.md#set-up) says, with each command in a terminal at the
checkout's root.

<a id="variables"></a>

### Set your keys and settings file

Set your keys as the README's [Set your API keys](../README.md#set-your-api-keys) step shows.
Without the S3 keys, the flat-file tests skip.

Then point the run at your [settings file](configuration.md). Add this line to your shell's
startup file, beside your keys, and open a new terminal:

```bash
export DEEPHAVEN_MASSIVE_CONFIG_FILE="$HOME/massive/settings.yaml"
```

The run takes three settings from the file and leaves every other setting at its default:

| Setting | What to set | Without it |
|---|---|---|
| `delayed_subscriptions` | Each subscription Massive serves your key delayed | Every stream and snapshot test of those subscriptions skips, `not listed as delayed` |
| `rest_requests_per_minute` | Each subscription Massive limits by the minute, and its limit | The rate-limit tests skip. Other tests pause at a subscription's first refused request while the library learns its limit. |
| `websocket_hosts` | A host for an asset class, when Massive serves it there, such as `indices: business.massive.com` for index values ([`websocket_hosts`](configuration.md#setting-details)) | The library picks the host |

A settings file the library refuses stops the run at the first test that loads it, with the
library's message.

<a id="run-order"></a>

### Run the three commands in order

Stop every other program that uses the key, then run these in order. Each command adds its
output to `live-checks.log`, the file to attach to an issue.

1. While stocks trade, from 09:30 to 16:00 New York time on a weekday, run the main tests:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   It names all three markers, because `-m integration` alone leaves out every test that also
   carries another marker:

   | Marker | What it runs |
   |---|---|
   | `integration` | The open-question tests, and every README example as written. An open question is a behavior the library relies on that Massive's docs leave open ([the list](dev/verification-items/README.md)). |
   | `integration_smoke` | Every dataset and method once |
   | `integration_rate_limits` | Three tests that go past a subscription's request limit on purpose |

   A test of a subscription Massive limits waits its turn, and prints the requests it has left.

2. Run the WebSocket limit tests alone, one command after the other, because they disturb the
   key's connections:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

3. To check that every futures contract Massive streams is on its contracts list, run the
   all-symbols test while futures trade. It streams every futures trade for 10 minutes, and
   takes over the key's futures connection meanwhile:

   ```bash
   uv run pytest -m integration_all_symbols tests/ingest/test_futures_calendar_integration.py 2>&1 | tee -a live-checks.log
   ```

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

## Read the checks a pass doesn't settle

Five tests record what Massive does for you to judge. Find each one's lines under
`live evidence` at the end of the run, or in `live-evidence-<UTC time>.log`
([Read the end of a run](#read-the-end-of-a-run)):

| Test | The question | Open an issue when |
|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | Does Massive's cap of 1,000 option contracts per connection count contracts, or channel subscriptions? A contract's trades and quotes are two channel subscriptions, and the library assumes the cap counts these. The test subscribes both for 1,000 contracts. | Massive acknowledges all 2,000 channel subscriptions. |
| `test_an_index_minute_file_holds_its_own_new_york_day` | Does an index minute file hold every minute that REST has for its New York day? | Its evidence lists any minute the file lacks. |
| `test_index_day_windows_and_files_across_many_indices` | Is the library right to end index day bars at 01:00 New York time? | Always. Attach its verdict lines (a), (b), and (c). |
| `test_futures_bar_buckets_against_the_bars_inside_them` | Does each futures bar of a session or longer hold the whole sessions that open in its UTC days? | Always. Attach its `VERDICT` lines. |
| `test_snapshot_gamma_against_black_scholes_per_dollar_and_per_cent` | Is `GreeksGamma` per $0.01, as Massive's description says? It compares each call with Black-Scholes gamma. | A call matches another unit. |

To run one alone, use `-k` with part of its name:

```bash
uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py -k options_cap 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k many_indices 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_futures_integration.py -k bar_buckets 2>&1 | tee -a live-checks.log
uv run pytest -m integration tests/namespaces/test_options_integration.py -k gamma 2>&1 | tee -a live-checks.log
```

Run the options test again when it skips as inconclusive. That skip means Massive refused
some of its channel subscriptions, or the socket reconnected without saying why.

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
| `limits-ws` | The WebSocket limit tests |
| `limits-second-connection` | The second-connection test |
| `stream-day` | Streams for every asset class, from 03:45 to 20:30 New York time |
| `throughput` | Every symbol's busiest streams, over an hour |
| `stream-day-reconcile` | The stream day's minute bars against REST history |
| `stream-night` | Crypto and futures streams, from 16:30 to 09:30 New York time |
| `stream-night-reconcile` | The night's minute bars against REST history |
| `half-day` | The bar-stamp tests, on the trading day after a half day |

The `stream-day` group also records how long a healthy stocks connection goes without a frame
while no stock trades. In its debug log, read the stocks
connection's `status` lines from 03:45 to 04:00 New York time. Open an issue with the longest
gap between them, and with every `no frame for ... during ... trading hours` WARNING of the day.

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
