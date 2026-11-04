# Live integration tests

The live tests, `tests/**/test_*_integration.py`, call Massive's real servers. They answer
questions Massive's documentation leaves open, such as a timestamp's unit, where a day's bar
starts, or how a ticker is spelled. Each test records what Massive sent, and its docstring says
what each answer means for the library. You run them with your own keys, whenever you suspect
Massive has changed something. Testers run the same checks through the
[live-run program](#live-run-program), as [the tester guide](tester-guide.md) describes.

<a id="run-the-tests"></a>

## Run the tests

A run takes three commands, in a set order: about 40 minutes on Basic for the main command,
plus the limit tests. Run every command from the repository root, in a terminal that holds the
[variables](#variables). The live tests carry these pytest markers:

| Marker | What it runs |
|---|---|
| `integration` | The evidence tests, each answering one question |
| `integration_smoke` | Every dataset and verb once, checked automatically |
| `integration_rate_limits` | The three tests that go over Basic's 5 requests a minute on purpose (Basic is Massive's free plan, and the tests and their output use that name) |
| `integration_limits` | The four tests that go over Massive's subscription and connection limits on purpose |

Every live test carries `integration`, and `-m integration` leaves out the tests that also carry
one of the other three markers. A plain `pytest` run deselects every live test, so
`./scripts/check.sh` and CI never run one. Every live test also skips without `MASSIVE_API_KEY`,
and whenever `CI` is set.

<a id="variables"></a>

### Set the variables

The tests read the keys and their options from the environment:

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | Every test | Every test skips |
| `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY` | The flat-file tests | They skip |
| `MASSIVE_DELAYED_API_KEY` | The check of what Massive says to a delayed key on the real-time feed, when your Stocks plan is Advanced or above, and the check of past stock data labeled `DELAYED`, when it's Developer or above | They skip |
| `MASSIVE_LIVE_PLANS` | Optional: declares the plans instead of probing them ([Declare plans the probe can't see](#declare-the-plans)) | The run probes |
| `DEEPHAVEN_MASSIVE_LOG_FILE` and `DEEPHAVEN_MASSIVE_LOG` | Optional: the debug log's file and level ([Read the debug log](#debug-log)) | The run picks a file, and the level is DEBUG |

The flat-file tests read Massive's flat files with the S3 keys. Massive's dashboard shows those
keys beside the API key, as the
[flat-files quickstart](https://massive.com/docs/flat-files/quickstart.md) says. Each
asset class's files come with its Starter plan or above. The trades files need Developer, and
the stocks and options quotes files need Advanced ([pricing](https://massive.com/pricing)).

The delayed key answers two questions that a real-time main key can't. The first is what
Massive says when a delayed key subscribes on the real-time feed. The second is whether past
stock data reads on the default configuration when Massive labels it `DELAYED`.

### Plan the time a run takes

How long a run takes depends on your plans and on the market's hours. These times are
estimates from earlier runs:

| Part | Time |
|---|---|
| The main command, every class on Basic | About 40 minutes, most of it waiting for Basic's 5 requests a minute |
| The smoke tests on paid plans, markets open | About 30 minutes, because each stream test waits for its rows |
| The smoke tests on paid plans, markets closed | Under ten minutes |
| The rate-limit tests | About six minutes, most of it pauses |
| A real-time Stocks key's delayed-feed check, in the first quarter hour of the stock session | Up to 17 more minutes |

The repository records no measured time for the whole main command on paid plans, or for the
limit tests.

<a id="run-order"></a>

### Run the main command, then the limit tests

The limit tests disturb the key's connections, so they run last and alone:

1. Stop every other program that uses the key. A stream test on a connection another program
   uses either waits for a slot or disconnects that program.
2. Run the main command. You can run it at any hour. A stream test checks rows only while its
   market is open, so a weekday run in market hours covers more:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   It probes your plans, runs the rate-limit tests first, then every evidence test and the smoke
   tests. A test skips, and prints why, when your plans don't include its data, its market is
   closed, or your plan's history doesn't reach the dates it reads. On Basic, each wait of 5
   seconds or more prints a line such as `live pacing: options: waiting 47 s for a request slot
   (paced at 5 requests a minute); 3 of this test's 5 requests left`. When the first test
   records a line, pytest prints `live evidence: writing <repo>/live-evidence-<UTC time>.log`.
   The `tee -a` appends each command's output to `live-checks.log`, so the three commands leave
   one log.

   The rate-limit tests need no open market. If you start during market hours, run them at
   another time, and use `-m "integration or integration_smoke"` in the main command. Run
   apart, they need at least one asset class still on Basic, and they use Indices first, then
   Currencies, then Stocks:

   ```bash
   uv run pytest -m integration_rate_limits 2>&1 | tee -a live-checks.log
   ```

3. Run the limit tests, one command after the other:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

   The first command sends wide WebSocket subscriptions, such as one frame of 1,000 crypto
   topics, 1,000 far-dated options contracts, and the crypto wildcard. The second takes a
   second connection slot with the same key. If Massive drops the older connection instead of
   refusing the newer one, the two sessions can drop each other until the test ends.

Each command ends with the sections that [Read the end of a run](#read-the-end-of-a-run)
describes, and writes its own `live-evidence-<UTC time>.log`.

<a id="plans"></a>

## Let the run find your plans

You usually don't tell the tests your plans. Massive sells one subscription per asset class
(Stocks, Options, Indices, Currencies for forex and crypto, and Futures), and each has its own
plan. Your key can hold any mix, and you can upgrade during the month. So the first live test
of each run asks Massive which plan your key holds in each class. This check, the probe, sets
which later tests run and which skip.

The plans head the summary at the end of the run, in a line that starts `plans:`. The evidence
file records the same line under `live plans`
([Read the end of a run](#read-the-end-of-a-run)).

The run saves the probe's answer in `.live-plans.json` at the repository root. For the next
hour, every run with the same API key and S3 access key ID reuses it. Such a run prints a line
that starts `live plans:`, saying when the plans were probed. Git ignores the file. It holds a
digest of each key, never the key itself. To probe again at once, after an upgrade, delete the
file.

<a id="declare-the-plans"></a>

### Declare plans the probe can't see

Declare your plans in `MASSIVE_LIVE_PLANS` in any of these cases:

| Case | What the probe does | What you declare |
|---|---|---|
| A snapshot comes back empty | It stops, and the run asks you to declare the plans | Every paid plan you hold |
| The run starts between 03:30 and 04:30 New York time, when Massive clears and refills its snapshots | It stops, and the run asks you to declare the plans, unless it saved an answer in the last hour | Every paid plan you hold, or wait until 04:30 |
| Stocks or Options Business with the trades and quotes expansion | It reports the base Business plan, which streams no trades, quotes, LULD bands, or per-second bars | Every paid plan you hold, with `business_expansion` for that class |
| Indices Business | It reports Advanced, because no request tells the two apart | Every paid plan you hold, with `indices=business` |
| Futures Business | It reports Advanced, because no request tells the two apart | Every paid plan you hold, with `futures=business`. Massive sells it per venue, and refuses a test of a venue your plan lacks. |

List each paid class as `<class>=<plan>`, separated by commas, and add `s3` when your S3 keys
(both `MASSIVE_S3_*` variables) reach the flat files. Every class you leave out is on Basic.
A declaration overrides both the probe and the saved answer. Each class has its own plan
names:

| Class | Plans you can declare |
|---|---|
| `stocks`, `options` | `starter`, `developer`, `advanced`, `business`, `business_expansion` |
| `futures` | `starter`, `developer`, `advanced`, `business` |
| `indices` | `starter`, `advanced`, `business` |
| `currencies` | `starter`, `business` |

This line declares Starter for Stocks, Options, and Currencies, Basic for Indices and Futures,
and flat files:

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

With it set, the run probes nothing, and the summary's first line lists the declared plans.

<a id="reading-the-results"></a>

## Read the results

Treat a failure as a fault in Massive or the library. Your plans or the clock explain every
skip, and a pass still leaves evidence for you to read. Most evidence tests carry one of two
markers:

- A test marked `verifies` asserts the answer the library assumes, so it fails the day Massive
  changes that answer.
- A test marked `settles` records evidence for you to read, and passes whatever the answer.

Both name, by number, the [open questions](dev/verification-items.md) they answer. A test
fails when:

- a request it needs fails or returns nothing
- a stream delivers no row while rows are due
- Massive refuses a request although your key holds the test's plans
- a `verifies` test gets an answer other than the one the library assumes

A test skips when:

- your key lacks a plan it needs, and the skip names the plan with its price, such as
  "needs Currencies Starter ($49/month)"
- `MASSIVE_API_KEY` or the S3 keys aren't set
- the day it asks for is older than your plan keeps
- the market isn't open

Any other test passes, and its docstring says what its evidence means.

### Know when a refusal is a skip

Massive's refusal "Your plan doesn't include this data timeframe" skips the test as "outside
the plan's history" when the day is older than the plan keeps. The free plans keep two years
of Stocks, Options, Currencies, and Futures, and all of Indices. The same refusal for a day the
plan does keep fails the test, because it may be a library bug.

Any other refusal fails a test whose plans your key holds, and the run records the refusal. Such
a refusal means the probe misread your plans, or Massive's pricing page is wrong about what the
plan includes.

A smoke case, or any other test that declares no plans, skips as "not entitled: needs <plan>"
when Massive refuses it. The smoke coverage counts these skips apart from failures. The smoke
case for stock imbalances always skips, because imbalances arrive only around the opening and
closing auctions.

### Know when a stream test passes

A stream test passes only when rows reached its table, never on Massive's acknowledgement
alone. Stock trades and quotes are due from 04:00 New York time, in the extended hours. Every
other stock, option, and index stream is due in the regular session, from 09:30. A delayed plan
is due once the session has been open as long as the plan's delay. When rows aren't due, the
test skips after the acknowledgement, with a reason that starts "not exercised:", such as
"not exercised: market closed (acknowledged)".

The LULD check needs the regular session, the only time the bands change. The `/prev` check
shows a short day's stamp only on the trading day after a half day, and its evidence marks that
day `half day: yes`.

<a id="read-the-end-of-a-run"></a>

### Read the end of a run

The evidence goes to its own file at the repository root, `live-evidence-<UTC time>.log`. The
run writes each line the moment a test records it, so a run cut short keeps everything
recorded up to then. Before pytest's own short summary, the run prints up to four sections,
in this order:

| Section | What it holds |
|---|---|
| `smoke coverage` | The smoke cases that passed, passed with drift, weren't entitled, were skipped, and failed. Drift means the case saw a library WARNING or a `SchemaDrift`, and each one is listed. |
| `open questions` | The plans line, then the questions the run touched, in groups: `verified X of Y`, `ran, evidence to read, X of Y`, one group for each plan some questions need, with its price, and the groups `not verified (failed or skipped)` and `not verified (partly run)` |
| `live debug log` | The path of the run's debug log |
| `live evidence` | The evidence file's lines again |

Each question appears under its group on a line of its own, its number beside its one-line
title.

### Know which tests need older history

Four tests read fixed dates, and a plan that doesn't keep the date can't run them. Every other
test picks its days back from the run's own day, inside the two years every free plan keeps:

| Test | Fixed date | Plans that keep it | Without one |
|---|---|---|---|
| The option split check | NVDA's 10-for-1 split of 2024-06-10 | Options Developer (four years) or Advanced (all history) | It skips, naming Options Developer |
| The deliverable check's adjusted option roots | `AAPL7` (2014-06-10), `DELL1` (2021-11-15), `FF1` (2024-04-15) | The Options plans whose history reaches each day | It records each root it can't read as outside the plan's history |
| `tests/tables/test_followed_read_integration.py` | The week of 2025-06-02 to 2025-06-06 | Every Stocks plan; Basic only until June 2027 | It skips as outside the plan's history |
| The `responses` group of the [live-run program](#live-run-program) | NVDA's daily bars from 2024-06-03 to 2024-06-14 | The Stocks plans whose history reaches the day | It skips the example, naming the Stocks plan it needs |

<a id="debug-log"></a>

### Read the debug log

Every pytest run that selects a live test writes the library's DEBUG log, so you can diagnose
a failure without another live run. The run logs to
`live-debug-logs/<UTC time>-<process id>/debug.log`, which git ignores, and prints the path
under `live debug log`.

The library keeps the API key and the S3 keys out of every line, and the run scans the log for
them anyway. If it finds a secret, the same section says so and names where. Don't share that
log, because a secret in it is a library bug to fix before the next run.

When `DEEPHAVEN_MASSIVE_LOG_FILE` is set, the run logs to that file instead, and
`DEEPHAVEN_MASSIVE_LOG` still chooses the level.

<a id="checks-to-read-by-hand"></a>

## Read five checks by hand

Five checks record a question the library's design depends on, and four of them pass whatever
Massive answers. A green result from one of those tells you only that it ran. Read each check's
[evidence](#reading-the-results), compare it with the answer the library assumes, and open an
issue when they differ:

| Check | Run | Plans | The question | The answer the library assumes |
|---|---|---|---|---|
| `test_the_options_cap_counts_contracts_or_topics` | The first [limit-test command](#run-order) | Options Advanced | Does Massive's cap of 1,000 per connection count contracts or topics? | Topics |
| `test_an_index_minute_file_holds_its_own_new_york_day` | The [main command](#run-order) | Indices Starter and `s3` | Does each index minute file hold only its own New York day? | Yes. This check asserts it, and fails otherwise. |
| `test_index_day_windows_and_files_across_many_indices` | The main command, with about 110 requests | Indices Starter and `s3` | Which day, New York or Chicago, do the index day bars from REST and the daily files cover, and do the files match REST? | No assumption. The check records which day each source covers. |
| `test_futures_bar_buckets_against_the_bars_inside_them` | The main command, with about 40 requests | Futures Starter | Does each futures bar of a session or longer hold the whole sessions that start in its UTC days? | Yes |
| A quiet connection | The stream day of the [live-run program](#live-run-program) | Any plan that streams stocks | Does Massive send status frames on a connection that has no data? | Yes, within 5 minutes |

### Read the options cap

The options check subscribes 1,000 far-dated SPY contracts on two channels, 2,000 topics. If
Massive acknowledges all 2,000, the cap counts contracts, and the library could count
contracts as well. If
Massive refuses topics past 1,000, or closes the socket after a status that says why, the cap
counts topics. The check skips as inconclusive when Massive refuses topics for want of a plan.
It also skips when the socket reconnects with no status that says why, because a network drop
looks the same. In either case, run the first limit-test command again.

### Read the index checks

The index minute file matters because `history()` trims each index file to its New York day
before it joins the REST rows. If a file held the Chicago day instead, index ranges would lose
or repeat an hour at each midnight. The check reads `I:AAVE100`, which updates around the
clock, from the most recent Monday's file. Minutes that REST has and the file lacks are
recorded, not failed, because they're a gap in Massive's file. To run this check alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
```

The run prints the path of its evidence file, and the check's lines record the file's rows
outside the New York day. The answer the library expects is that there are none.

The many-indices check reads about ten indices over four or five recent days, and ends with
three verdict lines:

- Line (a) counts the indices whose REST day bars cover the Chicago day, out of those whose New
  York and Chicago days differ.
- Line (b) splits the indices into regular-hours and around-the-clock groups. For each group,
  it names the indices whose day file covers a different day than REST's 1-day bar, and those
  whose minute file has gaps or prices that differ from REST.
- Line (c) says whether `I:AAVE100` is the only around-the-clock index whose day file matches
  neither day, or whose minute prices differ from REST.

### Read the futures bar check

Each futures bar of a session or longer covers a bucket of whole UTC days, and Massive puts each
session in the bucket of the UTC day it opens. The futures check reads the front contract of ES,
corn, live cattle, and Micro Bitcoin over the most recent whole month, and prints `VERDICT`
lines per contract. The library's model of futures bars holds when three things are true:

- every bar holds the whole sessions that start in its UTC days
- 2-session bars follow a fixed grid, rather than starting at the date you ask from
- a 2-month bar comes back the same whether you ask from before its period or from a month
  into it

Any other answer shows where the model breaks. The lines also show where each 1-session bar
starts against its first minute bar, which weekday 1-week bars start on, and whether any
session opens on a Friday evening or a Saturday.

This command runs the many-indices and futures checks alone. Their verdicts are the lines that
start `(a)`, `(b)`, `(c)`, and `VERDICT`:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py tests/namespaces/test_futures_integration.py -k "many_indices or bar_buckets" 2>&1 | tee -a live-checks.log
```

### Read a quiet connection

The library warns after 5 minutes without a frame, and that threshold is right only if Massive
sends status frames while no data flows. The stream day's [debug log](#debug-log) answers
this. From 03:45
to 04:00 New York time, the stocks connection holds its subscriptions and gets no data, because
stocks trade from 04:00. Record that connection's `status` lines
(`wss://.../stocks status ...`) from those 15 minutes, and the longest stretch without one.

Separately, report every `no frame for ... during ... trading hours` WARNING from the rest of
the day, with its time and connection. The warning fires only in a market's regular hours,
09:30 to 16:00 for stocks, so it never appears in the early window. Open an issue with both
records, because the 5-minute threshold is set from them.

<a id="user-doc-checks"></a>

## Check the user docs by hand

The README and the troubleshooting page make claims that only a keyed run can confirm, and no
live test asserts them. Run the example the claim belongs to, as pasted, in the Deephaven
console of a server started as the README's Install section starts it. Then open an issue for
each claim the run contradicts, with what you saw:

| Claim | Where | Plan | What to record |
|---|---|---|---|
| ORLY's 15-for-1 split | README, [Adjust prices for splits](../README.md#split-adjustment) | Stocks Basic | `splits` has a row with `ExecutionDate` 2025-06-10 and `HistoricalAdjustmentFactor` near 1/15 (0.0667). `traded` closes near $1,350 before June 10 and `adjusted` near $90, and `by_hand`'s `AdjustedClose` matches `adjusted`'s `Close`. |
| ORLY's `AdjustmentType` | The same recipe | Stocks Basic | Whether Massive labels the split `forward_split` or `stock_dividend`. O'Reilly paid it as a stock dividend of 14 shares per share. |
| Stock dividends in the cumulative factor | The same recipe | Stocks Basic | For a stock with a `stock_dividend` row and a later split, whether reading the splits without the `stock_dividend` rows changes the later split's `HistoricalAdjustmentFactor`, or only which row each bar joins. |
| Stocks Basic reaches June 2025 | The split recipe, and the debug-log example in troubleshooting | Stocks Basic | Both reads return rows rather than `Your plan doesn't include this data timeframe`. |
| Free-plan timing | README Examples 1 to 5, and [Read the history of a watchlist](../README.md#history-of-a-watchlist) | Stocks Basic | How long each takes to fill its tables. The README says up to a minute. |
| `UnderlyingAssetPrice` | README, [Stream the options near the money](../README.md#option-chain) | Each Stocks plan you hold, with any Options plan | Whether `spy_chain`'s `UnderlyingAssetPrice` is filled or null on each Stocks plan. |
| Releasing a poll | README Example 5 | Stocks Basic | After `m.release(headlines)`, closing both panels, and `del newest_first`, whether the poll's `State` in `m.streams()` leaves `RUNNING` within a few minutes. |
| Minute bars outside regular hours | README hook and Example 8 | Stocks Starter or higher | Whether live minute bars arrive in pre-market and after-hours trading, and whether `board` shows each stock's row with null `TimestampEnd`, `Close`, and `ChangePct` until its first bar. |
| Memory in 4 GB | README, [Read the history of a watchlist](../README.md#history-of-a-watchlist) | Stocks Basic for daily bars; Stocks Starter with S3 keys for minute bars | With `-Xmx4g`, whether a year of every stock's daily bars, and one day of every stock's minute bars with `adjusted=False`, each load without running out of memory. Record `market.size`. |
| What you see | Every README example | The plan each example names | The tables that open, their row counts, and their first columns, against the text after each example. |
| Many sessions on one key | Every README and troubleshooting example, run in one console | The plans the examples name | Whether `m.connections()` ever shows `WAITING_SLOT` once a dozen or more sessions, some with different settings, have opened live tables on the same key. |

<a id="plan-prices"></a>

## What each plan settles

Each plan settles the open questions listed for it, and
[the end of each run](#read-the-end-of-a-run) lists the questions still open under the plan
that settles them. Prices are Massive's monthly list prices as of November 2026
([S1], [S2]):

| Plans | Per month | What they settle |
|---|---|---|
| Every class on Basic | $0 | The questions in [What Basic settles](#what-basic-settles) |
| Currencies Starter | $49 | The questions in [What Currencies Starter settles](#what-currencies-starter-settles) |
| Options Starter | $29 | The questions in [What Options Starter settles](#what-options-starter-settles) |
| Stocks Starter | $29 | The questions in [What Stocks Starter settles](#what-stocks-starter-settles) |
| Futures Starter | $29 | The questions in [What Futures Starter settles](#what-futures-starter-settles) |
| Indices Starter | $49 | The questions in [What Indices Starter settles](#what-indices-starter-settles) |
| Options Developer | $79 | Whether `adjusted=true` adjusts option bars for NVDA's 10-for-1 split of June 2024 |
| Stocks Advanced; Options Advanced | $199 each | The unit of the LULD bands; whether Massive serves a real-time Stocks key on its delayed feed when `delayed_subscriptions` lists stocks; the options topic cap |
| Business plans | $999 and up | The unit of fair market value (Currencies Business, $999), and the WebSocket host of the base Business plan (Stocks Business, $2,499) ([S2], [S8]). Futures Business, sold per venue, covers only its own venue's tests, and other Business questions have fixture tests only. |
| Partner datasets | $49 to $99 each | Benzinga, ETF Global, TMX, and consumer spending cost $99 a month each, and NOI costs $49 ([S9]). The Benzinga add-on adds one check, that every consensus-ratings row names its ticker, and that test skips without it. |

Currencies, Options, and Stocks Starter together cost $107 a month, and adding Futures Starter
makes $136. Economy data and each class's reference data are free on Basic.

Massive offers no free trial ([S4]). A plan you cancel keeps its access to the end of the
month ([S5]).

<a id="what-basic-settles"></a>

### What Basic settles

A key with every class on Basic settles these questions:

- Which keys Massive sends for ticker events, inactive tickers, exchanges, and conditions.
- Which keys the filings without a ticker carry, and which the insider activity and holdings
  carry.
- How week, month, and multi-unit bars align.
- Where futures sessions start and end, and how the holiday session looks, read from minute
  bars.
- How futures sessions are labeled, around the 17:00 Chicago roll and on Good Friday.
- Whether crypto minute bars rebuild the day bar, and where Massive's REST answers cut the
  crypto and forex day.
- Whether an index path with `I:` in it works, read with `I:NDX`, and where stock and index
  bars of a half day are stamped.
- Which basis the split data uses, checked on the year's latest forward split, the latest
  special cash dividend, and stock dividends, and whether every page echoes the `adjusted`
  value it was sent.
- Which statuses an unknown ticker gets, and where the daily indicators are stamped.
- Whether past stock data (the previous day's bar, a day's open and close) reads on the
  default configuration, though Massive labels it `DELAYED`.
- How Massive writes futures spread tickers on every venue, whether it writes a futures
  contract's year with one digit or two, and which currencies the crypto ticker list holds.
- Whether `fetch()` of futures contracts follows a live table of product codes, so a code you
  add lands and a code you delete leaves.
- How the rate limits behave:
  - what a 429 refusal says
  - which requests count against which limit
  - whether a ticker lookup filtered by market counts against its class
  - how a spent minute frees its requests
  - whether a learned limit outlives the key's sessions

<a id="what-currencies-starter-settles"></a>

### What Currencies Starter settles

- Where Massive cuts the crypto and forex day, and whether the forex and crypto file pairs
  match it.
- Whether forex weekend day files exist.
- Which unit the REST timestamps use.
- How forex topics are spelled.
- Whether unsubscribing works, how large a subscribe frame can be, what happens when a second
  connection takes the slot, and how the wildcard subscription behaves.
- Whether a crypto stream follows its watchlist.
- How Massive's raw WebSocket frames look, and, with `s3`, the matching file rows.

<a id="what-options-starter-settles"></a>

### What Options Starter settles

- Whether a delayed plan's option chain says `DELAYED`, in its envelope status or in
  `last_trade.timeframe`. Nobody has confirmed the answer yet. Only a Starter or Developer key
  can settle it, and on a real-time plan the test skips. `fetch()` and `poll()` refuse the
  chain unless `delayed_subscriptions` lists options.
- How the chain and the snapshots spell index and `BRK.B` underlyings.
- What a contract adjusted for a split delivers. The oldest option roots are checked only on a
  plan whose history reaches their day.
- Where SPY's and SPXW's 1-day, half-day minute, and week bars are stamped on every source,
  with `s3` for the day file.
- Whether `fetch()` of the option chain follows a live table of underlyings, so an underlying
  you add lands and one you delete leaves.

<a id="what-stocks-starter-settles"></a>

### What Stocks Starter settles

- What Massive says when a delayed key subscribes on the real-time feed.
- Where AAPL's 1-day, week, and month bars are stamped on every source.
- With `s3`, when Massive publishes each day's flat file, and what the split stock's day file
  holds, as traded.
- With `s3`, whether a weekend flat-file key answers 404 `NoSuchKey`. Any other answer makes
  the library raise on every flat-file range that holds a weekend.

<a id="what-futures-starter-settles"></a>

### What Futures Starter settles

- Which sessions the futures files hold.
- Whether a delayed futures plan's REST answers say `DELAYED`, in the snapshot's `timeframe`
  fields and envelope status. It's the only futures evidence of a delayed plan the library
  reads, for streams and for `fetch()` and `poll()` alike. Nobody has confirmed the answer
  yet. Only a Starter or Developer key can settle it, and on a real-time plan the test skips.
- Where the snapshot puts the ticker.
- Which dates streamed futures rows carry, and where one-second bars end.
- How the futures bars of ES, corn, live cattle, and Micro Bitcoin fall, as
  [Read the futures bar check](#read-the-futures-bar-check) describes.

<a id="what-indices-starter-settles"></a>

### What Indices Starter settles

- Which unit the index values file uses.
- Whether `I:SPX` works on every source.
- Where the 1-day, week, month, quarter, and year bars of `I:SPX` and `I:AAVE100` are stamped,
  and which day and week each covers, with `s3` for the day file.
- With `s3`, the answers of the [index checks](#read-the-index-checks).
- Possibly the index value stream. Massive's documentation says Starter includes it, and the
  pricing page says Advanced.

<a id="live-run-program"></a>

## Run the live-run program

To record every live check a key allows without sitting at the terminal, run
`scripts/live_run.py`. It follows a fixed New York timetable on your machine or a tester's,
and `pack` turns its results into one zip you can read. A full pass takes at least three
trading days: a stream day, the day after it, and one ordinary day. It also takes a stream
night when the key streams crypto or futures.

The program probes the key when it starts and each trading morning. When the probe finds a plan
added or raised, the program reruns the checks that plan allows. Run it from the repository
root, in a terminal that holds the [variables](#variables), starting with `check`:

```bash
uv run python scripts/live_run.py check                       # the machine and the key; writes nothing
uv run python scripts/live_run.py run --out ~/massive-live    # add --any-time for a first run now
uv run python scripts/live_run.py status --out ~/massive-live
uv run python scripts/live_run.py pack --out ~/massive-live   # writes live-results-<date>.zip
uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live   # attended
```

The `check` command prints an `OK`, `WARN`, or `FAIL` line for each part of the machine, then
each subscription's plan and how far back its history goes, then `ready: yes` or `ready: no`.
The `status` command prints the latest probe, then each check's status and reason, then a
count per group. The `upgrade-check` command is attended: run it while the subscription is
still on Basic, upgrade when it tells you to, and it records whether the library stops pacing
that subscription.

On a first run, `--any-time` starts the
groups that need no market hours (`rate-limits`, `evidence`, `smoke`, and `responses`) at
once. It counts only the first time you give it for a results folder, and after that the
program follows the timetable.

### Know when each group runs

The program runs its checks in groups, each at a set New York time:

| Group | What it runs | When (New York time) |
|---|---|---|
| `rate-limits` | `-m integration_rate_limits` | From 11:30 |
| `evidence` | `-m integration -k "not luld"` | From 11:30, after the previous day's flat files |
| `luld` | `-m integration -k luld` | 09:28, for the opening bands, except on the day after a stream day |
| `smoke` | `-m integration_smoke` | From 11:30 |
| `responses` | The documentation's examples, each in its own session, with every raw reply recorded | From 11:30 |
| `limits-ws` | The WebSocket limit tests | From 16:15 |
| `limits-second-connection` | The second-connection test, which runs last with nothing else open | From 11:30, after the groups before it |
| `stream-day` | A full trading day of streams for every class the key can stream | 03:45 to 20:30 on a planned day |
| `throughput` | Every symbol's busiest streams, measured over an hour | 09:15 to 11:00 the next day |
| `stream-day-reconcile` | The streamed minute bars against REST history | From 11:30 the next day |
| `stream-night` | Crypto and futures streams through one night | 16:30 on a planned evening to 09:30 the next morning |
| `stream-night-reconcile` | The night's minute bars against REST history | From 11:30 the morning after |
| `half-day` | The bar-stamp tests | On the trading day after a half day |

Each time a group starts is one attempt, with its own folder in the results folder,
`<group>/<date>-<n>/`, which holds the attempt's debug log. While it waits for the next group,
`run` packs a fresh zip on its own when a group has finished since the last zip.

### Know when a check runs again

The program reruns a check by itself, depending on its last result:

| Last result | When it runs again |
|---|---|
| Passed | Only when a subscription's plan rises |
| Passed under an older rule that didn't require stream rows | Until it passes under the current rule |
| Failed | Once more |
| Skipped for a missing subscription | When the subscriptions change |
| Skipped for the moment, such as a closed market | Up to three attempts in all |

To make every check of a group run again, failed ones included, pass its name to `run --redo`,
such as `--redo luld,smoke`.

### Stop and resume the program

Control-C, a closed terminal, and `kill` all stop the program the same way. The same `run`
command resumes it, and keeps every result recorded so far. It refuses to resume a results
folder on a different commit, so pulling `main` between starts blocks the resume.

### Know what a stream night tests

A stream night covers the hours only 24-hour markets trade, which a stream day, ending at
20:30, never reaches. When the key streams crypto or futures, the program plans one on the
first suitable Sunday to Thursday evening before a full trading day. It runs from 16:30 New
York time to 09:30 the next morning. It streams the trades, quotes, and minute bars of
`X:BTCUSD` and `X:ETHUSD`, and the minute bars of the front ES and NQ contracts. From Futures
Developer up, it streams their trades and quotes too.

The night checks the two date boundaries in those hours. Crypto's `Date` changes at 00:00 UTC,
and a futures row's `Date` changes to the next session at the 16:00 Chicago close. The night
records separate results for crypto and futures, and each fails in five cases:

- one of its streams failed
- its gaps passed 30 minutes in all
- the recorder couldn't write a batch of rows
- an hour its market was open brought no row
- a row near a boundary carries the wrong `Date`

On the next day, `stream-night-reconcile` compares the night's minute bars with REST history,
and the night's crypto or futures result fails if any bar's `Date` differs. The files
`hours.csv`, `boundaries.csv`, and `summary.json` hold the night's evidence, and `REPORT.md` in
each zip has a section per night.

### Keep the secrets out of the results

The program reads the keys only from the environment, and never writes them. Its output and
the text files it writes pass through a filter that replaces each secret with `***`. Before it
zips anything, `pack` scans every file of the results folder for each secret, plain,
URL-encoded, and base64, and refuses if it finds one. `pack` also refuses unless your terminal
holds the same keys the run used. It compares them with the salted fingerprints `run`
stored.
The program's code is in `tests/_live/live_run/`, and `scripts/live_run.py` is its entry point.

## Sources

- [S1] https://massive.com/pricing
- [S2] https://massive.com/business
- [S4] https://massive.com/knowledge-base/article/does-massive-offer-free-trials
- [S5] https://massive.com/knowledge-base/article/how-can-i-downgrade-or-cancel-my-massive-account
- [S8] https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value
- [S9] https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse
