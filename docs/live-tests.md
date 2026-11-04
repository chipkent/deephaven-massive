# Live integration tests

The live tests, `tests/**/test_*_integration.py`, call Massive's real servers. They answer
questions Massive's documentation leaves open, such as a timestamp's unit, where a day's bar
starts, or how a ticker is spelled. Each test records what Massive sent, and its docstring says
what each answer means for the library. Rerun them whenever you suspect Massive has changed. Only the
owner runs them, with the owner's own keys. Testers run the same checks through the
[live-run program](#live-run-program), as [the tester guide](tester-guide.md) describes.

<a id="run-the-tests"></a>

## Run the tests

A full run takes about 40 minutes on the free plan, and it needs the key to itself. Run every
command from the repository root, in a terminal that holds the [variables](#variables). The
live tests carry these pytest markers:

| Marker | What it runs |
|---|---|
| `integration` | The evidence tests, each answering one question |
| `integration_smoke` | Every dataset and verb once, checked automatically |
| `integration_rate_limits` | The three tests that go over a Basic subscription's 5 requests a minute on purpose |
| `integration_limits` | The four tests that go over Massive's subscription and connection limits on purpose |

Every live test carries `integration`, and `-m integration` leaves out the tests that also
carry one of the other three markers. A plain `pytest` run deselects every live test, so
`./scripts/check.sh` and CI never run one. Every live test also skips without
`MASSIVE_API_KEY`, and whenever `CI` is set.

<a id="variables"></a>

### Set the variables

The tests read the keys and their options from the environment:

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | Every test | Every test skips |
| `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY` | The flat-file tests | They skip |
| `MASSIVE_DELAYED_API_KEY` | Two checks of a delayed Stocks plan, when the main key's Stocks plan streams in real time | They skip |
| `MASSIVE_LIVE_PLANS` | Optional: declares the plans instead of probing them ([Declare the plans](#declare-the-plans)) | The run probes |
| `DEEPHAVEN_MASSIVE_LOG_FILE` and `DEEPHAVEN_MASSIVE_LOG` | Optional: the debug log's file and level ([The debug log](#debug-log)) | The run picks a file, and the level is DEBUG |

The S3 keys read Massive's flat files, and Massive's dashboard shows them beside the API key
([S14]). Each asset class's files come with its Starter plan or above. The trades files need
Developer, and the stocks and options quotes files need Advanced ([S1]).

The delayed key answers two questions that a real-time main key can't. The first is what
Massive says when a delayed key subscribes on the real-time feed. The second is whether past
stock data reads on the default configuration when Massive labels it `DELAYED`.

<a id="run-order"></a>

### Run them in order

Run the tests in three steps, so that the limit tests, which disturb the key's connections, run
last and alone:

1. Stop every other program that uses the key. A stream test on a connection another program
   uses either waits for a slot or disconnects that program.
2. Run the main command. It works at any hour, but a stream proves its rows only while its
   market is open, so a run in market hours checks more than a run on a weekend:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   It probes your plans, runs the rate-limit tests first, then every evidence test and the
   smoke tests. Whatever your plans, the market's hours, or a plan's history leaves out skips
   with its reason. You should see a path to an evidence file once the first test records a
   line.
3. Run the limit tests, one command after the other:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

   The first command sends wide WebSocket subscriptions, such as one frame of 1,000 crypto
   topics, 1,000 far-dated options contracts, and the crypto wildcard. The second takes a second
   connection slot with the same key. If Massive drops the older connection instead of
   refusing the newer one, the two sessions can drop each other until the test ends.

The rate-limit tests need no open market. To keep market hours for the streams, run them
apart, at any other time, and leave them out of the market-hours run:

```bash
uv run pytest -m integration_rate_limits 2>&1 | tee -a live-checks.log
uv run pytest -m "integration or integration_smoke" 2>&1 | tee -a live-checks.log
```

They need at least one asset class on the free plan, Indices if you can, and they take about
six minutes, mostly pauses.

### Plan the time a run takes

How long a run takes depends on your plans and on the market's hours:

| Plans | Main command |
|---|---|
| Every class on the free plan | About 40 minutes, most of it waiting for the free plan's 5 requests a minute |
| Paid plans, markets open | About 30 minutes for the smoke tests, because each stream waits for its rows |
| Paid plans, markets closed | Under ten minutes for the smoke tests |

A real-time Stocks key adds up to 17 minutes when its delayed-feed check runs in the first
quarter hour of the stock session. On the free plan, each wait of 5 seconds or more prints a
line such as `live pacing: options: waiting 47 s for a request slot`, so a paced run never
looks hung.

<a id="plans"></a>

## Tell the tests your plans

Massive sells one subscription per asset class: Stocks, Options, Indices, Currencies (forex
and crypto), and Futures. Each is on its own plan, and the paid plans include flat files
([S1], [S3]). A key can hold any mix and upgrade during the month, so the first live test of a
run asks Massive what the key holds. The run prints the plans it found in a `live plans:` line,
and they head the evidence and the summary at the end.

The answer is kept for an hour in `.live-plans.json` at the repository root, and runs within
that hour reuse it. Git ignores the file, and it names the keys only by a digest. To probe
again at once, after an upgrade, delete the file.

<a id="declare-the-plans"></a>

### Declare the plans

Declare the plans in `MASSIVE_LIVE_PLANS` when the probe can't tell them apart, or to skip the
probe. A declaration overrides the probe and the cache. List each paid class as
`<class>=<plan>`, separated by commas, and add `s3` when the S3 keys reach the flat files.
Every class you leave out is on Basic, the free plan:

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

The plans are `starter`, `developer`, `advanced`, and `business`, and for stocks and options
also `business_expansion`. Declare them in these cases:

| Case | What the probe does |
|---|---|
| A snapshot comes back empty | It stops, and the run asks you to declare the plans. |
| The run starts between 03:30 and 04:30 New York time | It doesn't run, because Massive clears and refills its snapshots then. |
| Stocks or Options Business with the expansion | It reads the base Business plan, which streams no trades, quotes, LULD bands, or per-second bars. Declare `business_expansion` to run the tests that need them. |
| Indices Business | It reads Advanced, because no request tells the two apart. |
| Futures Business | It reads Advanced. Massive sells it per venue, so a test of a venue the plan lacks is refused. |

<a id="reading-the-results"></a>

## Read the results

Each test ends in one of three outcomes, and a skip always names what was missing:

| Outcome | When |
|---|---|
| Fails | A request it needs fails or returns nothing; a stream delivers no row while rows are due; Massive refuses it although the key holds its plans; or, for a test marked `verifies`, Massive's answer is no longer the one the library assumes |
| Skips | A plan, a key, the S3 keys, history beyond the plan's window, or an open market is missing. A skip for a plan names it with its price, such as "needs Currencies Starter ($49/month)". |
| Passes | Everything else. The evidence still needs reading: each test's docstring says what its answer means. |

A test marked `verifies` asserts the answer the library assumes, so it fails the day Massive
changes. A test marked `settles` records evidence for you to read. Both name the open questions
they answer, by their numbers in [the verification items](dev/verification-items.md).

### Know when a refusal is a skip

Massive's refusal "Your plan doesn't include this data timeframe" skips the test as "outside
the plan's history" when the day is older than the plan keeps. The free plans keep two years
of Stocks, Options, Currencies, and Futures, and all of Indices. The same refusal for a day the
plan does keep fails the test, because it points to a library bug. Any other refusal of a test
whose plans the key holds fails, and the refusal is recorded, because either the probe or
Massive's plan table is wrong. The smoke tests report a refused dataset as "not entitled: needs <plan>",
and count it apart from failures.

### Know when a stream test passes

A stream test passes only when rows reached its table, never on Massive's acknowledgement
alone. Stock trades and quotes are due from 04:00 New York time, in the extended hours. Every
other stock, option, and index stream is due in the regular session, from 09:30. A delayed plan
is due once the session has been open as long as the plan's delay. When rows aren't due, the
test skips after the acknowledgement, as "not exercised: market closed (acknowledged)" or the
like.

Two checks need particular days. The LULD check needs the regular session, the only time the
bands change. The `/prev` check shows a short day's stamp only on the trading day after a half
day, and its evidence marks that day `half day: yes`. Stock imbalances arrive only around the
auctions, so their smoke case always skips.

### Read the end of a run

The evidence goes to its own file at the repository root, `live-evidence-<UTC time>.log`.
Each line is written as the test records it, so a run cut short keeps everything it recorded. The same lines close the
run's terminal output, under "live evidence". After them come three summaries:

- The summary says "verified X of Y" for the asserted questions whose every test passed, and
  "ran" for the evidence-only questions whose every test passed. Each remaining group follows,
  under the plan it needs and its price.
- The smoke tests' coverage counts the cases that passed, passed with drift, weren't
  entitled, were skipped, and failed. Drift means the case saw a library WARNING or a
  `SchemaDrift`, and each one is listed.
- The open questions follow, each group with its count, and each question's number beside its
  title.

### Watch the fixed dates

Most tests choose their days from the run's own day, so they stay inside the shortest free
history of two years. Four read fixed dates, and a plan that doesn't keep the date can't run
them:

| Test | Fixed date | When the plan doesn't keep it |
|---|---|---|
| The option split check | NVDA's 10-for-1 split of 2024-06-10 | It needs Options Developer, which keeps four years. |
| The deliverable check's adjusted option roots | `AAPL7` (2014-06-10), `DELL1` (2021-11-15), `FF1` (2024-04-15) | Each root is read only when the Options plan keeps its day, and otherwise recorded as outside the plan's history. |
| `tests/tables/test_followed_read_integration.py` | The week of 2025-06-02 to 2025-06-06 | Every plan keeps it until June 2027. |
| The live-run `responses` group | NVDA's daily bars from 2024-06-03 to 2024-06-14 | The example is skipped, naming the Stocks plan that keeps the day. |

<a id="debug-log"></a>

### Read the debug log

Every pytest run that selects a live test writes the library's DEBUG log, so you can diagnose
a failure without another live run. The run logs to
`live-debug-logs/<UTC time>-<process id>/debug.log`, which git ignores, and prints the path in
its last lines, under "live debug log". Send that file with a report of any failure.

The library keeps the API key and the S3 keys out of every line, and the run checks anyway. If
it finds a secret in the folder, its last lines say so and name where. Don't send that log, and
send the owner the list instead. When `DEEPHAVEN_MASSIVE_LOG_FILE` is set, the run logs to that
file instead, and `DEEPHAVEN_MASSIVE_LOG` still chooses the level.

<a id="checks-to-read-by-hand"></a>

## Read five checks by hand

Five checks answer questions the library's design depends on. Make sure each one ran, and read
its evidence yourself:

| Check | Where it runs, and what it needs | The question |
|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | The first limit-test command; Options Advanced | Does Massive's cap of 1,000 contracts per connection count contracts or topics? The library counts topics. |
| `test_an_index_minute_file_holds_its_own_new_york_day` | The main run; Indices Starter and `s3` | Does each index minute file hold only its own New York day? |
| `test_index_day_windows_and_files_across_many_indices` | The main run; Indices Starter and `s3`; about 110 requests | Which day, New York or Chicago, each index day source covers, and whether the day and minute files match REST |
| `test_futures_bar_buckets_against_the_bars_inside_them` | The main run; Futures Starter; about 40 requests | Where each futures session bar and longer bar falls, for ES, corn, live cattle, and Micro Bitcoin |
| A quiet connection | The live-run program's stream day | Does Massive send frames on a connection that has no data? |

The options cap is inconclusive when Massive refuses the topics for the plan, or when the
socket reconnects without a status that says why, which a network drop also causes. Run it
again then.

The index minute file check matters because `history()` trims each index file to its New York
day before it joins the REST rows. A failure means index ranges lose or repeat an hour at each
midnight. Minutes that REST has and the file lacks are recorded, not failed, because they're a
gap in Massive's file. To run it alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
```

The many-indices check answers in its last three evidence lines. The first says whether REST
day bars cover the Chicago day for most indices that trade outside the stock session. The
second lists, for regular-hours and around-the-clock indices apart, which day files cover
another window than the range bar, and which minute files have gaps or other prices. The third
says whether the wrong values of `I:AAVE100` are an isolated case.

The futures check prints `VERDICT` lines per contract over the most recent whole month. They
say where each 1-session bar starts against its first minute bar, and whether any session opens
on a Friday evening or a Saturday. They also cover the 2-session grid and the day 1-week bars
start on. The last ones say whether each longer bar holds exactly the sessions that start in
its UTC days, and whether a 2-month bar read from inside its bucket is clipped at `{from}`. To run the last two checks
alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py tests/namespaces/test_futures_integration.py -k "many_indices or bar_buckets" 2>&1 | tee -a live-checks.log
```

The quiet-connection answer comes from the stream day's `debug.log`. A connection with
acknowledged subscriptions logs `no frame for ... during ... trading hours` at WARNING after 5
minutes without a frame in its market's regular hours. Whether 5 minutes is right depends on
whether Massive sends a status frame while no data flows. From 03:45 to 04:00 New York time,
the stocks connection holds its subscriptions and gets no data, because stocks trade from
04:00. Record that connection's `status` lines (`wss://.../stocks status ...`) in those 15
minutes and the longest stretch without one, and report every `no frame for` WARNING with its
time and connection. The threshold changes only on that evidence.

<a id="plan-prices"></a>

## Choose the plans to buy

Each plan settles a set of open questions, and the end of each run lists the questions still
open under the plan that settles them. Prices are Massive's monthly list prices as of
2026-11 ([S1], [S2]). Massive offers no free trial, and a cancelled plan keeps its access to
the end of the month ([S4], [S5]):

| Plans | Per month | What they add |
|---|---|---|
| Every class on Basic | $0 | The reference, filings, and insider keys; bar alignment; futures sessions; the crypto minute rebuild; split and dividend bases; the rate-limit questions |
| Currencies Starter | $49 | The crypto and forex day cuts and files; the REST timestamp units; the WebSocket protocol checks; the watchlist follow |
| Options Starter | $29 | Option chain recency labels; underlying spellings; adjusted deliverables; option bar stamps |
| Stocks Starter | $29 | What Massive says to a delayed key on the real-time feed; AAPL's bar stamps on every source. With `s3`, the flat files' publication time and weekend answers. |
| Currencies, Options, and Stocks Starter | $107 | Every priority question |
| The same and Futures Starter | $136 | Most of the open questions, adding the futures files, snapshots, streams, and bar buckets |
| Indices Starter | $49 | The index values file, `I:SPX` everywhere, index bar stamps, and, with `s3`, the index files' days |
| Options Developer | $79 | Whether `adjusted=true` adjusts option bars for the underlying's split |
| Stocks Advanced; Options Advanced | $199 each | The LULD unit; whether a real-time Stocks key is served on the delayed feed; the options topic cap |
| Business plans | $999 and up | The FMV unit (Currencies Business, $999) and the Business host (Stocks Business, $2,499) ([S2], [S8]). Futures Business covers only its own venue's tests. |
| Partner datasets | $49 to $99 each | Benzinga adds one check, that every consensus-ratings row names its ticker ([S9]) |

<a id="live-run-program"></a>

## Run the live-run program

The live-run program, `scripts/live_run.py`, runs every check above unattended for days, on a
tester's machine or your own. It probes the key when it starts and each trading morning, runs
each group of checks at a set New York time, and adapts when the subscriptions change. You can
stop and start it at any time. Run it from the repository root, in a terminal that holds the
[variables](#variables):

```bash
uv run python scripts/live_run.py check                       # the machine and the key; writes nothing
uv run python scripts/live_run.py run --out ~/massive-live    # add --any-time for a first run now
uv run python scripts/live_run.py status --out ~/massive-live
uv run python scripts/live_run.py pack --out ~/massive-live   # writes live-results-<date>.zip
uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live   # only when asked
```

To make every check of some groups run again, failed ones included, pass their names to
`run --redo`, separated by commas. The groups, and when each runs:

| Group | What it runs | When (New York time) |
|---|---|---|
| `rate-limits` | `-m integration_rate_limits` | From 11:30 |
| `evidence` | `-m integration -k "not luld"` | From 11:30, after the previous day's flat files |
| `luld` | `-m integration -k luld` | 09:28, for the opening bands |
| `smoke` | `-m integration_smoke` | From 11:30 |
| `responses` | The documentation's examples, each in its own session, with every raw reply recorded | From 11:30 |
| `limits-ws`, `limits-second-connection` | The two limit-test commands, the second last and alone | From 16:15 |
| `stream-day` | A full trading day of streams for every class the key can stream | 03:45 to 20:30 on a planned day |
| `throughput` | Every symbol's busiest streams, measured over an hour | 09:15 to 11:00 the next day |
| `stream-day-reconcile` | The streamed minute bars against REST history | From 11:30 the next day |
| `stream-night` | Crypto and futures streams through one night | 16:30 on a planned evening to 09:30 the next morning |
| `stream-night-reconcile` | The night's minute bars against REST history | From 11:30 the morning after |
| `half-day` | The bar-stamp tests | On the trading day after a half day |

Each group runs in a child process, with its debug log in its attempt's folder,
`<group>/<date>-<n>/debug.log`. A check runs again on its own in three cases:

| Check | Runs again |
|---|---|
| Passed | Only when a subscription's plan rises |
| Failed | Once more |
| Skipped for a missing subscription | When the subscriptions change |

Stream checks that passed before their streams had to deliver rows count as unproven, and run
again by themselves. Control-C, a closed terminal, and `kill` all stop the program the same
way, and the same `run` command resumes it, keeping every result recorded so far.
`--any-time` takes effect once per results folder.

### Read a stream night

A stream night covers the hours only 24-hour markets trade, which a stream day, ending at
20:30, never reaches. When the key streams crypto or futures, the program plans one on the
first suitable Sunday to Thursday evening before a full trading day. It runs from 16:30 New
York time to 09:30 the next morning. It streams the trades, quotes, and minute bars of
`X:BTCUSD` and `X:ETHUSD`, and the minute bars of the front ES and NQ contracts. From Futures
Developer up, it streams their trades and quotes too.

The night checks the two date boundaries in those hours. Crypto's `Date` changes at 00:00 UTC,
and a futures row's `Date` changes to the next session at the 16:00 Chicago close. A night's
unit fails in four cases:

- one of its streams failed
- its gaps passed 30 minutes in all
- an hour its market was open brought no row
- a row near a boundary carries the wrong `Date`

On the next day, `stream-night-reconcile` compares the night's minute bars with REST history,
and a bar whose `Date` differs fails its unit. The files `hours.csv`, `boundaries.csv`, and
`summary.json` hold the night's evidence, and `REPORT.md` in each zip has a section per night.

### Keep the secrets out of the results

The program reads the keys only from the environment, and it never writes them. Every line it
prints or writes passes through a filter that replaces each secret with `***`. Before it zips
anything, `pack` scans every file of the results folder for each secret, plain, URL-encoded,
and base64, and refuses if it finds one. It also refuses unless the terminal holds exactly the
secrets the run used, which `run` records as salted fingerprints, so a scan for the wrong
values can't pass by mistake. The program's code is in `tests/_live/live_run/`, and
`scripts/live_run.py` is its entry point.

<a id="licensed-data"></a>

## Keep licensed data private

The evidence files, `live-checks.log`, and the live-run zips hold raw licensed rows, such as
quotes and partner data from Benzinga and ETF Global. Git ignores the evidence files and the
log. Attach them only where the audience may see that data, and remove those rows before you
share them anywhere public.

## Sources

- [S1] https://massive.com/pricing
- [S2] https://massive.com/business
- [S3] https://massive.com/knowledge-base/article/what-are-the-different-massive-subscriptions-i-can-use
- [S4] https://massive.com/knowledge-base/article/does-massive-offer-free-trials
- [S5] https://massive.com/knowledge-base/article/how-can-i-downgrade-or-cancel-my-massive-account
- [S8] https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value
- [S9] https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse
- [S14] https://massive.com/docs/flat-files/quickstart.md
