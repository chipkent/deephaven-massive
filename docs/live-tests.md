# Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive's real servers. They settle
questions Massive's documentation leaves open, such as a timestamp's unit, where a day bar is
cut, or how a ticker is spelled, and you rerun them whenever Massive may have changed. Each
test records what Massive sent, and its docstring says what each answer means for the
library.

This page is the runbook: how to run the tests safely, within the plans your key holds, and
how to read what they record. Testers on their own machines use the
[live-run program](#the-live-run-program) through [the tester guide](tester-guide.md).

## Quick start

From the repository root, with every other client of the key stopped:

```bash
export MASSIVE_API_KEY=...                       # required
export MASSIVE_S3_ACCESS_KEY_ID=...              # optional: the flat files
export MASSIVE_S3_SECRET_ACCESS_KEY=...
uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
```

[Run order](#run-order) explains each command; [Reading the results](#reading-the-results)
says what to look at afterwards.

## Test markers

| Marker | What it runs | Command |
|---|---|---|
| `integration` | The evidence tests, one question each | `uv run pytest -m integration` |
| `integration_smoke` | Every dataset and verb once, checked automatically | `uv run pytest -m integration_smoke` |
| `integration_limits` | The four tests that go over Massive's subscription and connection limits on purpose | `uv run pytest -m integration_limits` |
| `integration_rate_limits` | The three tests that go over a Basic subscription's 5 requests a minute on purpose | `uv run pytest -m integration_rate_limits` |

- Every live test carries `integration`. The smoke, limit, and rate-limit tests also carry
  their own marker, and `-m integration` leaves them out.
- The default run deselects every live test, so `./scripts/check.sh` and CI never run one.
- Every live test skips without `MASSIVE_API_KEY`, and whenever `CI` is set.
- `tests/conftest.py` enforces the markers.

## Variables

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | Every test | Every test skips |
| `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY` | The flat-file tests ([S3 keys](#s3-keys)) | They skip |
| `MASSIVE_DELAYED_API_KEY` | Two checks: what Massive says when a delayed key subscribes on the real-time feed, when Stocks is not on Starter or Developer (then the main key serves); and whether past stock data reads on the default configuration, when Massive labels the main key's past data `OK` | They skip |
| `MASSIVE_LIVE_PLANS` | Optional: declares the plans instead of probing them ([Declare the plans](#declare-the-plans)) | The run probes |
| `DEEPHAVEN_MASSIVE_LOG_FILE`, `DEEPHAVEN_MASSIVE_LOG` | Optional: the debug log's file and level ([The debug log](#the-debug-log-of-a-pytest-run)) | The run picks a file; the level is DEBUG |

### S3 keys

The flat files are an S3 bucket, `flatfiles` at `https://files.massive.com`, with keys of
their own: an access key id and a secret access key, separate from the API key. Massive's
dashboard issues them on its keys page, beside the API key ([S14]).

| Files | Plan needed |
|---|---|
| Each class's files | That class's Starter plan or above |
| Trades files | Developer |
| Stocks and options quotes files | Advanced |

Export both variables, and the probe finds which files they reach.

## How the tests learn your plans

Massive sells one subscription per asset class (Stocks, Options, Indices, Currencies for forex
and crypto, Futures), each on its own plan, and the S3 flat files come with the paid plans
([S1], [S3]). You can hold any mix, and upgrade during the month, so the tests ask Massive
what the key holds.

### The probe

The first live test of a run probes each class with a few cheap requests:

| What it asks | What an answer means |
|---|---|
| A snapshot of one liquid ticker (AAPL, SPY's option chain, `I:NDX`, `C:EURUSD`, the ES contracts) | Starter or above; refused means Basic |
| That ticker's trades, then its quotes (stocks, and the chain's contract with the most volume that day) | Developer (trades), Advanced (quotes). An empty answer counts as access, since a plan without it refuses with a 403. |
| The front ES contract's trades, and its recency label in the snapshot (futures) | Developer or above (trades). A `REAL-TIME` label means Advanced, since Developer is delayed. The front contract is the one with the most volume that session among those carrying a label. |
| A non-null `fmv` in the snapshot (stocks, options, currencies) | Business. For stocks and options it is always the base Business plan: its REST trades and quotes answer too (at end-of-day recency), so only a declaration names the trades and quotes expansion. |
| A real-time indices snapshot | Advanced |
| One file of every flat-file prefix | Which flat files the S3 keys reach: only a file that opens counts |

What the probe cannot tell:

- **An empty snapshot.** A snapshot counts only with at least one row. An empty one proves
  nothing, so the probe stops and the run asks you to declare the plans.
- **03:30 to 04:30 New York time.** Massive clears and refills its snapshots then, and a
  Business plan's `fmv` can be missing, so the probe does not run.
- **Stocks or options Business.** It is not an Advanced plan: without the expansion it
  streams no trades, quotes, LULD, or per-second bars, so a test that needs Advanced skips on
  it. Declare `business_expansion` to run those tests on a key with the expansion.
- **Indices Business** leaves no mark a request can see, and reads as Advanced.
- **Futures Business** is sold per venue (CME, CBOT, NYMEX, COMEX) and reads as Advanced. A
  test of a venue the plan lacks is refused.

### The cache

The probe's answer is kept for an hour in `.live-plans.json` in the repository root. Git
ignores it, and it names the key and the S3 access key id only by a digest.

- Runs within the hour reuse it, and print a `live plans:` line that says so and when the
  plans were probed.
- To probe again at once, after an upgrade, delete the file.
- The plans head the evidence (under "live plans") and the summary at the end of the run.

### Declare the plans

To skip the probe, set `MASSIVE_LIVE_PLANS`, comma-separated:

- `<class>=<plan>` for each paid class: `starter`, `developer`, `advanced`, `business`, or,
  for stocks and options, `business_expansion`;
- `s3` when the S3 keys reach the flat files.

Every class you leave out is Basic, the free plan. A declaration overrides the probe and the
cache.

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

### Delayed plans

The live sessions list every class on a delayed plan in
`MassiveConfig.delayed_subscriptions`, since the library refuses delayed data for any class
not listed there. The delayed plans are Stocks, Options, and Futures Starter and Developer, and
Indices Starter. Their streams use Massive's delayed feed, and their REST reads are not
checked.

## How a test decides to skip

Each test declares the plans it needs, such as `@pytest.mark.needs("currencies:starter", "s3")`.

| Case | Outcome |
|---|---|
| The key lacks the plans | Skips before any request, naming the plan and its monthly price, such as "needs Currencies Starter ($49/month)" |
| A test of what a delayed plan receives declares `<class>:delayed`, such as `futures:delayed` | Only Starter or Developer (Starter for indices) meets it, since a real-time plan cannot answer it |
| Refused as "Your plan doesn't include this data timeframe", for a day before the first day the plan keeps | Skips as "outside the plan's history", by the history windows Massive documents per plan (two years on every Basic plan) |
| The same refusal, for a day the plan keeps | Fails: it may be a library bug |
| Any other refusal of a test whose plans the key holds | Fails, and the refusal is recorded: either the probe or the documented plan table is wrong |
| A refused dataset in the smoke tests | Reported as "not entitled: needs <plan>", and counted apart from failures |

## The free plan

Basic is REST only, without WebSocket access or flat files ([S3], [S7]).

### Pacing

Basic allows 5 REST requests a minute per class; paid plans have no limit.

- **The library paces itself.** The classes on Basic become the live sessions'
  `MassiveConfig.rest_basic_subscriptions`. Every live test shares one rate governor per key
  for the whole run, on the real clock, so every REST request to a Basic class, the library's
  and the tests' raw ones, waits for its class's place within 5 a minute.
- **No test waits more than about a minute.** A test that may read a class on Basic sends that
  class at most 5 requests, and declares the count with `@pytest.mark.requests(n)`. A unit
  test fails when a live test declares more than 5 for a class it may read on Basic, and a
  live run skips such a test while its class is on Basic: "needs a paid <class> plan: N
  requests would take about M minutes on Basic".
- **A test reads less on Basic.** It reads fewer underlyings, symbols, or pages, or leaves out
  a part that repeats what another request shows. For example, the underlying spellings read
  only the contracts of `SPX`, `I:SPX`, `BRK.B`, and `BRKB`; the split and cash-dividend checks
  try only the newest event, and the stock-dividend check at most three. Each such test's
  docstring says what it leaves out, and its evidence has a line that says so.
- **A paced run never looks hung.** Each wait of 5 s or more prints one line, such as
  `live pacing: options: waiting 47 s for a request slot (paced at 5 requests a minute); 3 of
  this test's 5 requests left`.
- **Timeouts.** A test's timeout grows with its request count. A live run lifts the 120 s
  limit after which faulthandler would end the process; each test's own timeout bounds it.

### How long a run takes

The main command of [Run order](#run-order) runs the rate-limit tests, the evidence tests, and
the smoke tests.

- **Every class on Basic: about 40 minutes.** The classes are paced side by side, so stocks
  sets the pace:

  | Part | Requests declared, at most | Time at 5 a minute |
  |---|---|---|
  | Evidence tests (the [Every class on Basic](#every-class-on-basic) list) | About 74 to stocks (reference data included), 32 to currencies, 31 to futures, 8 to options, 7 to indices | Up to about 15 minutes |
  | Smoke tests | 88 to stocks, 36 to currencies, 24 each to options and futures, 14 to indices; its stream cases skip before any request, since Basic does not stream | Up to about 18 minutes |
  | Rate-limit tests | | About six minutes of pauses between them |

  A run takes less when a test sends fewer requests than it declares.
- **Paid plans.** Nothing waits for its pace, but each stream case waits for its rows while
  its market is open (up to 75 s for a minute bar). The smoke tests take about half an hour in
  market hours, and under ten minutes when the markets are closed. The delayed-feed check of a
  real-time Stocks key can add up to 17 minutes in the first quarter hour of a stock session.

### Indices on Basic

Indices Basic excludes SPX, DJI, RUT, and VIX ([S6]). With Indices on Basic, the index tests
use `I:NDX` instead of `I:SPX`.

### The rate-limit tests

The three tests carry `integration_rate_limits`, so `-m integration` leaves them out.

- They need at least one class left on Basic; Indices is preferred.
- They draw a few HTTP 429 refusals on purpose, so each sends its class more than 5 requests.
- The main command runs them first, each after a minute's pause, so earlier requests no
  longer count.

| Test | What it records |
|---|---|
| The refusal | The 429's text, and which requests outside every subscription Massive counts |
| The market filter | Whether a ticker lookup filtered by a class's market (Indices, Currencies, or Stocks) is charged to that class |
| The learned limit | Whether a session learns the limit from the refusal, and a later session of the key forgets it; about three minutes |

They need no open market. To keep market hours for the streams, run them apart, at any other
time, and leave them out of the market-hours run:

```bash
uv run pytest -m integration_rate_limits 2>&1 | tee -a live-checks.log
uv run pytest -m "integration or integration_smoke" 2>&1 | tee -a live-checks.log   # in market hours
```

## Run order

1. **Prepare.** Stop every other client of the key, and export the [variables](#variables).
2. **The main run**, at any hour, on any day:

   ```bash
   uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
   ```

   It probes the plans, runs the rate-limit tests first, then every evidence test and the
   smoke tests. Whatever the key lacks, the market's hours, or the plan's history leaves out
   skips with its reason. Nothing needs a person to time it, but a stream proves its rows only
   while its market is open, so a run in market hours checks more than one on a weekend. Two
   checks answer more on some days:

   - the LULD check needs the regular session for rows;
   - `/prev` shows a short-day stamp only on the trading day after a half day (its evidence
     marks that day `half day: yes`).

3. **The limit tests**, after the main run, apart, with no other client of the key. They go
   over Massive's limits on purpose:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

   - The first command runs the wide WebSocket tests: one frame of 1,000 crypto topics, 1,000
     far-dated options contracts on two channels, and `XT.*` with `XT.BTC-USD` for 30 s.
   - The second runs, last and alone, the second-connection test, which takes a second
     connection slot with the same key. If Massive drops the older connection instead of
     refusing the newer, the two sessions can drop each other until the test ends.

Each run writes its evidence to a file of its own in the repository root,
`live-evidence-<UTC time>.log`, and prints its path when it records the first line. Every test
appends what it observes as it goes, flushed line by line, so a run cut short keeps everything
recorded before. The same lines close the run's terminal output, under "live evidence".

## Checks to read by hand

Four checks answer questions the library's design leans on. Make sure each ran, and read its
evidence.

### The options cap

`test_the_options_cap_counts_contracts_or_topics`: a limit test, so it runs only in the first
`integration_limits` command; it needs Options Advanced.

- **The question:** does Massive's 1,000-contract cap per connection count contracts or
  topics? The library counts topics.
- **Inconclusive** when Massive refuses topics for the plan, or when the socket reconnects
  with no status that says why, which a network drop causes too: run it again.

### The index minute file's day

`test_an_index_minute_file_holds_its_own_new_york_day`: in the main run; it needs Indices
Starter and `s3`.

- **The question:** does `us_indices/minute_aggs_v1` hold only its own New York day?
  `history()` trims each index file to that day before joining REST rows, so a failure means
  index ranges lose or repeat an hour at each midnight.
- Minutes REST has and the file lacks are recorded, not failed: they are a gap in Massive's
  file, not a wrong day.

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py -k minute_file 2>&1 | tee -a live-checks.log
```

### Index day windows across many indices

`test_index_day_windows_and_files_across_many_indices`: in the main run; it needs Indices
Starter and `s3`. It sends about 110 REST requests and reads two flat files a day.

- **The question:** which day, New York or Chicago, each index day source covers, and whether
  the index day and minute files match REST, for about ten indices over four or five recent
  days.
- **Its last three evidence lines answer:**
  1. whether REST day bars cover the Chicago day for most indices that update outside the
     stock session;
  2. for regular-hours and around-the-clock indices apart: which day files cover another
     window than the range bar, which match no window, and which minute files have gaps or
     other prices;
  3. whether `I:AAVE100`'s wrong values (a day file in no window, or other minute prices) are
     an isolated case.

### Futures bar buckets

`test_futures_bar_buckets_against_the_bars_inside_them`: in the main run; it needs Futures
Starter. It sends about 40 REST requests, and its `VERDICT` lines answer each question per
contract, for ES, corn, live cattle, and Micro Bitcoin over the most recent whole month:

- where each 1-session bar's `window_start` falls against its first minute bar;
- whether any session opens on a Friday evening or a Saturday (the UTC days 1-session bars
  start on, and their labels' weekdays);
- whether 2-session bars follow a fixed grid;
- the day 1-week bars start on;
- whether each longer bar holds exactly the whole sessions that start in its UTC days;
- whether a 2-month bar read from inside its bucket is clipped at `{from}`.

To run the last two alone:

```bash
uv run pytest -m integration tests/namespaces/test_indices_integration.py tests/namespaces/test_futures_integration.py -k "many_indices or bar_buckets" 2>&1 | tee -a live-checks.log
```

### A quiet connection

One answer comes from the stream day's `debug.log` (a [live-run](#the-live-run-program)
group) rather than a test: **does Massive send frames on a quiet connection?**

- A connection with acknowledged subscriptions logs a WARNING after 5 minutes without any
  frame during its market's regular hours: `no frame for ... during ... trading hours`.
- Whether that threshold is right depends on whether Massive sends a status frame while no
  data flows.
- From 03:45 to 04:00 New York time the stocks connection holds its subscriptions and gets no
  data, since stocks trade from 04:00. Record that connection's `status` lines
  (`wss://.../stocks status ...`) in those 15 minutes, and the longest stretch without one.
- Report every `no frame for` WARNING with its time and connection.

The threshold and its gate change only on that evidence.

## Dates and markets

### Which days the tests read

No test reads a fixed date. Each computes its days from the run's New York day, with the US
exchanges' holiday rules, so every read stays inside a Basic plan's two years of history:

| Test | Days it reads |
|---|---|
| Common choices | The most recent Monday that traded and whose files are published; the most recent calendar week without a holiday; the most recent whole month; the most recent half day (the Friday after Thanksgiving, July 3, or December 24) whose week has ended |
| The many-index check | Four or five days: the three trading days before the run's, the most recent Monday, and the trading day after the most recent holiday |
| The futures tests | The contract that was front on their day (for the bucket check, at the end of the most recent whole month) |
| The holiday-session checks | The most recent Martin Luther King Day and Good Friday |
| The split and dividend checks | The year's most recent forward split and special cash dividend, found through the library's own `stocks.splits` and `stocks.dividends`, choosing common stocks with no later split |
| The bar-stamp checks | The most recent half day whose week has ended, and the full trading day before it; the month and quarter reads are the two whole ones before the run's; the index year read runs from January 1 to the run's day |

Exceptions and edge cases:

- **The option split check** reads NVDA's 10-for-1 split of 2024-06-10, whose thin OTC
  neighbors list no options, and needs Options Developer, which keeps four years.
- **The deliverable check's three adjusted option roots** (from 2014, 2021, and 2024) are each
  read only when the key's Options plan keeps their day, by the documented history windows;
  otherwise each is recorded as outside the plan's history.
- **A December 24 half day** stays the latest until the next Thanksgiving week ends, so the
  bar-stamp reads can be about eleven months old: still inside every plan's two years.
- **Before the year's first trading day** there is no year bar yet, and the index year read is
  recorded as skipped.

### When a stream test passes

A stream test passes only when rows reached its table. It never passes on the acknowledgement
alone.

1. It asserts that Massive acknowledged its topics, which Massive does whether or not the
   market is open.
2. It reads the market's status: `/v1/marketstatus/now`, or for futures ES's `market_event`
   in `/futures/v1/market-status`.
3. It requires rows while the market is open for the stream's kind of rows:

   | Stream | Rows required |
   |---|---|
   | Stock trades and quotes | In the extended hours too: status `extended-hours`, from 04:00 New York time |
   | Every other stock, option, and index stream | In the regular session, from 09:30 |
   | On a delayed plan | Once the session has been open as long as the plan's delay: 15 minutes on Stocks, Options, and Indices Starter (and Stocks and Options Developer); 10 minutes on Futures Starter and Developer, counted for futures from the `timestamp` of its market status |
   | A minute-bar stream | It waits until right after the next minute boundary, up to 75 s |

4. When rows are not required, it skips after the acknowledgement: "not exercised: market
   closed (acknowledged)" or the like.

Special cases:

- **LULD** needs the regular session, the only time bands change; the smoke tests stream the
  LULD bands of every ticker for the same reason.
- **Stock imbalances** arrive only around the auctions, so their smoke case always skips
  after the acknowledgement.
- **A skip for the market's hours** holds only for that moment: the live-run program runs the
  check again on a later attempt, and a whole stream day fails a unit whose streams delivered
  no row once its session had opened.
- **Unproven passes run again.** In a results folder, stream checks that passed before
  streams had to deliver rows (the smoke stream cases, the delayed-feed check, and the
  stream-day units) count as unproven, and run again by themselves.

### Protocol and watchlist checks

- **The wire-protocol checks** that belong to no asset class (the unsubscribe, the
  1,000-topic frame, the wildcard) stream crypto, which trades all the time. On a key that
  streams stocks but no currencies, they stream stocks instead, on the feed of the key's
  Stocks plan, while stock bars are due.
- **The reconnect check** closes the session's streaming socket from under it, and requires
  the crypto stream to reconnect, return to `RUNNING`, and deliver new trades. It records any
  `WAITING_SLOT` on the way.
- **The `AUTO` check** reads AAPL's minute bars from the previous trading day through today on
  `AUTO` and on REST, before today's flat file exists. It requires the same minutes from
  both, no day missing or doubled, and records `AUTO`'s warning that it filled days from REST.
  It needs Stocks Starter and the S3 keys.
- **The watchlist check**, `test_a_crypto_stream_follows_its_watchlist` in
  `tests/ingest/test_ws_integration.py`, streams crypto trades from an input table, adds
  `X:ETHUSD`, deletes it, and adds it again. It requires a row after each add; the row and the
  stream's `Symbols` entry gone after the delete; and no row from before the re-add. It needs
  Currencies Starter; each step waits at most 60 s.

### Delayed-plan labels

- **The past-data check** records the envelope status Massive gives AAPL's previous day bar,
  and counts only when that status is `DELAYED`. On a key that gets `OK`, it uses
  `MASSIVE_DELAYED_API_KEY` when set, and otherwise skips: "needs a Basic or Starter stocks
  key".
- **The unlisted-delayed check.** Because the live sessions list every delayed class in
  `delayed_subscriptions`, one check opens a session with the key alone, as a user who lists
  nothing would. It requires the first delayed class's stream (or, for options, the chain) to
  be refused with a message that names the remedy.

## What each plan settles

Each test names the open questions it answers. A test marked `verifies` asserts the answer the
library assumes, so it fails the day Massive changes; a test marked `settles` records evidence
that a person reads.

Prices are monthly list prices from massive.com/pricing, as of 2026-11 ([S1], [S2]). Massive
offers no free trial, and a cancelled plan keeps its access to the month's end ([S4], [S5]).

| Plans | Per month | Settles |
|---|---|---|
| Every class on Basic | $0 | [The list below](#every-class-on-basic) |
| Currencies Starter | $49 | Adds the crypto and forex day cuts and file pairs; whether forex weekend day files exist; the REST timestamp units; the forex topic spelling; the unsubscribe, frame-size, slot, and wildcard checks; the watchlist follow; the raw frame forms (with `s3` for the files) |
| Options Starter | $29 | [Options Starter](#options-starter) |
| Stocks Starter | $29 | Adds what Massive says when a delayed key subscribes on the real-time feed, and the stamps of AAPL's 1-day, week, and month bars on every source. With `s3`, also the flat-file publication time, the split stock's as-traded day file, and whether a weekend flat-file key answers 404 `NoSuchKey` through files.massive.com (if it answers anything else, the library raises on every flat-file range that holds a weekend) |
| **Currencies, Options, and Stocks Starter** | **$107** | Every priority question |
| The same and Futures Starter | $136 | [Futures Starter](#futures-starter): most of the open questions |
| Indices Starter | $49 | [Indices Starter](#indices-starter) |
| Options Developer | $79 | Adds option bars across NVDA's 2024 split under both bases: whether `adjusted=true` adjusts option bars for the underlying's split |
| Stocks Advanced; Options Advanced | $199 each | The LULD unit; whether Massive serves a real-time Stocks key on its delayed feed (`delayed_subscriptions` listing stocks); the options topic cap |
| Business plans | $999 and up | The FMV unit (Currencies Business, $999) and the base Business host (Stocks Business, $2,499) ([S2], [S8]); otherwise covered by fixture tests only. Futures Business is sold per venue, and covers only the tests of its venue. |

### Every class on Basic

- The reference keys: ticker events, inactive tickers, exchanges, conditions.
- The filings without a ticker; the insider activity and holdings keys.
- Week, month, and multi-unit bar alignment.
- The futures sessions, labels, and holiday session (from minute bars); the futures session
  labels around the 17:00 Chicago roll and Good Friday.
- The crypto minute rebuild; the REST sides of the crypto and forex day cuts.
- The encoded index path (with `I:NDX`); the stock and index half-day stamps.
- The split basis: the year's latest forward split, the adjusted echo on every page, the
  latest special cash dividend, stock dividends.
- An unknown ticker's statuses; the daily indicator stamps.
- Whether past stock data (the previous day's bar, a day's open and close) reads on the
  default configuration, though Massive labels it `DELAYED`.
- The futures spread ticker forms of every venue; how Massive writes a futures contract's year
  (one digit or two); the crypto ticker list's currencies.
- Whether `fetch()` of futures contracts follows a live table of product codes: a code added
  lands, a code deleted leaves.
- The rate-limit questions: the 429 text, the rate buckets, whether a market-filtered ticker
  lookup is charged to its class, how a spent minute frees its requests, and whether a
  learned limit outlives the key's sessions.

### Options Starter

- Whether a delayed plan's option chain says `DELAYED`, in its envelope status or
  `last_trade.timeframe`. `fetch()` and `poll()` refuse it unless options are listed in
  `delayed_subscriptions`. **UNVERIFIED**, and only on Starter or Developer: a real-time plan
  skips it.
- The index and `BRK.B` underlying spellings across the chain and snapshots.
- The adjusted deliverables; the oldest roots only on a plan that keeps their day.
- The stamps of SPY and SPXW 1-day, half-day minute, and week bars on every source (with
  `s3` for the day file).
- Whether `fetch()` of the option chain follows a live table of underlyings: an underlying
  added lands, one deleted leaves.

### Futures Starter

- The futures files' sessions.
- The snapshot's `timeframe` fields and envelope status: whether a delayed futures plan's REST
  answers say `DELAYED`. It is the only futures evidence of a delayed plan the library reads,
  for streams and for `fetch()` and `poll()` alike. **UNVERIFIED**, and only on Starter or
  Developer: a real-time plan skips it.
- The snapshot's ticker location.
- The streamed futures dates and one-second bar ends.
- The futures bar buckets of ES, corn, live cattle, and Micro Bitcoin: where each session and
  longer bar falls, the 2-session grid, whether any session opens on a Friday evening or a
  Saturday, and clipping at `{from}`.

### Indices Starter

- The index values file unit.
- `I:SPX` everywhere.
- The index 1-day, week, month, quarter, and year bar stamps, and which day and week they
  cover (`I:SPX` and `I:AAVE100` rebuilt from minute bars; with `s3` for the day file).
- With `s3`: whether an index minute file holds exactly its New York day (`I:AAVE100`, which
  updates around the clock, against its REST minutes), and the day windows and file agreement
  of about ten indices over recent days.
- Possibly the index value stream: the documentation says Starter, the pricing page Advanced.

### Partner datasets

- Benzinga, ETF Global, TMX, and consumer spending are $99 a month each; NOI is a $49
  expansion ([S9]). Economy data and each class's reference data are free on Basic.
- The Benzinga add-on adds one check, that every consensus-ratings row names its ticker;
  without the add-on it skips.

### Answers Massive already states

Massive's knowledge base states some answers, which the live tests confirm ([S10] to [S13]):

- a missing flat file answers 404 `NoSuchKey`;
- one subscribe frame of 120,003 entries is accepted;
- a connection carries about 1,000 options contracts;
- grouped daily stamps the end of its window;
- the crypto and forex files spell `X:BTC-USD` and `C:EUR-USD`.

Each documentation page names its plan in its "Plan Access" table ([S15]).

## Safety

- **Streams.** Every stream test subscribes to one symbol per asset class, except the LULD
  check, which streams the bands of every ticker (`LULD.*`) so that some arrive within seconds
  in an open session. Each test releases its stream as soon as the first rows arrive and
  closes its session at the end, so no cluster holds more than one of the run's connections.
  The limit tests are the exception.
- **REST.** Some checks query several symbols, because the question is about the differences
  between them:
  - nine options underlyings, and five contract roots;
  - four stock ticker types, and three partner pages;
  - nine futures products' schedules, every futures combo product, and the combo contracts of
    four venues;
  - the crypto ticker list (at most 5 pages on Basic);
  - the inactive tickers from `A` to `AC`, and every exchange and condition;
  - a month of four filings datasets.
- **Other clients.** Stop every other client of the key before you run: a stream test on a
  cluster another client uses either waits for a connection slot or disconnects that client.
- **Licensed data.** The evidence files and `live-checks.log` are git-ignored. They hold raw
  licensed rows (quotes, and partner data such as Benzinga and ETF Global): attach them only
  where the audience may see that data, and redact those rows before you share them anywhere
  public.

## Reading the results

| Outcome | When |
|---|---|
| Fails | A request it needs fails or returns nothing; a stream delivers no row while rows are due; it is refused although the key holds its plans; or, for a test marked `verifies`, Massive's answer is no longer the one the library assumes |
| Skips | Something was missing, and the skip names it: a plan, a key, S3 keys, history beyond the plan's window, or a market that is closed or within its feed's delay, so no row was due ("not exercised") |
| Passes | Everything else is evidence: read each test's docstring for what it means |

The end of a run:

- **The summary** says "verified X of Y" for the asserted questions whose every test passed,
  and "ran" for the evidence-only questions whose every test passed (their evidence still
  needs reading). Each remaining group follows, under the plan it needs and its price.
- **The smoke tests' coverage**: passed (with the count that passed with drift, a library
  WARNING or a `SchemaDrift` the case saw, each listed), not entitled, otherwise skipped, and
  failed. The not-entitled and skipped cases are grouped by the plan they need or their
  reason, with a count per group.
- **The open questions**, each group with its count, and each question's number beside its
  one-line title.

## The debug log of a pytest run

Every pytest run that selects a live test also writes the library's DEBUG log, so you can
diagnose a failure without another live run.

- **Where:** once the run's tests are chosen, and before any opens a session, the run creates
  `live-debug-logs/<UTC time>-<process id>/` and logs to `debug.log` inside it. Git ignores
  the folder.
- **Its path** is in the last lines of the run, under "live debug log". Send that file with a
  report of any failure.
- **No log:** a run of unit tests only writes none, and a run whose live tests opened no
  session (each skipped for want of a key, say) removes its empty folder.
- **No secrets.** The library keeps the API key and the S3 pair out of every line, and the run
  checks anyway: it scans the folder for each secret set in its environment. If it finds one,
  the same last lines say so and name where. Do not send that log; send the owner the list.
- **Your own file:** a run whose environment sets `DEEPHAVEN_MASSIVE_LOG_FILE` logs to that
  file and changes nothing. The live-run program sets it for each of its children, so their
  logs stay in their attempt's folder. `DEEPHAVEN_MASSIVE_LOG` still chooses the level when
  set.

## The live-run program

`scripts/live_run.py` runs everything above unattended, for days, on a tester's machine. It
records what you need to diagnose a problem afterwards without another live run.

- It probes what the key can do at each start and each trading morning.
- It runs every group of checks at a fixed New York time.
- It adapts by itself when the subscriptions change.
- You can stop and start it at any time.

A tester follows [the tester guide](tester-guide.md), which walks through it step by step. Run
it from the repository root, in the terminal that holds the [variables](#variables):

```bash
uv run python scripts/live_run.py check                       # the machine and the key; writes nothing
uv run python scripts/live_run.py run --out ~/massive-live    # add --any-time for a first run now
uv run python scripts/live_run.py status --out ~/massive-live
uv run python scripts/live_run.py pack --out ~/massive-live   # writes live-results-<date>.zip
uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live   # only when the owner asks
```

`run --redo GROUP,...` makes every check of the named groups run again, failed ones included.

### Groups

| Group | What it runs | When (New York time) |
|---|---|---|
| `rate-limits` | `-m integration_rate_limits` | From 11:30 |
| `evidence` | `-m integration -k "not luld"` | From 11:30, after the previous day's flat files |
| `luld` | `-m integration -k luld` | 09:28, for the opening bands |
| `smoke` | `-m integration_smoke` | From 11:30 |
| `responses` | The documentation's examples, each in its own session, every raw reply recorded | From 11:30 |
| `limits-ws`, `limits-second-connection` | The two limit-test commands of [Run order](#run-order), the second last and alone | From 16:15 |
| `stream-day` | A full trading day of streams for every class the key can stream | 03:45 to 20:30 on a planned day |
| `throughput` | Every symbol's busiest streams, measured over an hour | 09:15 to 11:00 the next day |
| `stream-day-reconcile` | The streamed minute bars against REST history | From 11:30 the next day |
| `stream-night` | Crypto and futures streams through one night | 16:30 on a planned evening to 09:30 the next morning |
| `stream-night-reconcile` | The night's minute bars against REST history, overall and at each boundary | From 11:30 the morning after |
| `half-day` | The bar-stamp tests | On the trading day after a half day |

### How the groups run

- **Children.** Each group runs in a child process, with:
  - the probed plans in `MASSIVE_LIVE_PLANS`;
  - the library's debug log in the attempt's folder, `<group>/<date>-<n>/debug.log`;
  - a pytest plugin (`tests/_live/live_run/outcomes.py`) that records each test's outcome as
    it runs, so a crash loses nothing.
- **Raw replies** come from the wire package's reply-capture hook, which hands over every REST
  body and WebSocket frame already cleaned of the key.
- **Reruns.**

  | Check | Runs again |
  |---|---|
  | Passed | Only when a subscription's plan rises |
  | Failed | Once more |
  | Skipped for a missing subscription | When the subscriptions change |

- **Stop and resume.** Control-C, a closed terminal, and a `kill` all stop the program the
  same way: the running child is stopped, the state saved, and the same `run` command
  resumes. A resume first stops any child an earlier run left behind, and keeps every result
  that child recorded. `--any-time` takes effect once per results folder; a resume finishes
  its groups without it.

### The stream night

A stream day stops at 20:30, so it never holds a stream through the hours only 24-hour markets
trade. Those hours hold two date boundaries:

- crypto's `Date` is the UTC date, which changes at 00:00 UTC;
- a futures row's `Date` is its trading session, which the library takes from Massive's
  futures schedules. It changes when the evening's session closes at 16:00 Chicago time: a
  row in the pause before the 17:00 open already belongs to the next session.

**What it streams.** When the key streams crypto (a paid Currencies plan) or futures, the
program plans a stream night, from 16:30 New York time to 09:30 the next morning:

- crypto: the trades, quotes, and minute bars of `X:BTCUSD` and `X:ETHUSD`;
- futures: the minute bars of the front ES and NQ contracts, and from Futures Developer up
  their trades and quotes.

**When.**

- A night is planned for the first evening, after the probe that finds its checks due, whose
  next morning is a full trading day: a Sunday evening, when futures open for the week, or a
  Monday to Thursday evening that is itself a full trading day.
- Holidays and early closes come from the exchanges' fixed rules, and from the closures
  Massive's market status lists as upcoming, which each probe reads.
- A night never shares a day with a stream day.
- That evening no other group starts from 16:15, and one still running is stopped then.
- No other group starts from 16:30 until 09:00 the next morning, even when the night's child
  has stopped for good.
- A child that dies is started again, up to five times, until 09:00. A night whose child has
  used its five starts is over: the next probe, even the morning's before 09:30, plans the
  next night.

**What it writes.** The night writes what a stream day writes (rows by hour, per-minute counts,
latency, samples, gaps), and at its end:

| File | What it holds |
|---|---|
| `hours.csv` | The rows of each stream per UTC hour |
| `boundaries.csv` | The rows within 90 minutes before and after the 16:00 Chicago close (futures) and 00:00 UTC (crypto), with their `Date` values. Futures rows must move from the evening's session to the morning's, the pause's included; on a Sunday nothing trades before the close, so only the side after is judged. Crypto rows must move from one UTC date to the next. |
| `summary.json` | The gaps, the most reconnects of each connection, and the quiet hours |

**When a unit fails.**

- One of its streams failed.
- The night's gaps passed 30 minutes.
- An hour its market was open went without a row. Futures pause from 16:00 to 17:00 Chicago
  time; crypto never closes.
- A row near a boundary carries the wrong `Date`.

**The next day,** `stream-night-reconcile` compares the night's minute bars with REST history
of both dates, kept to the night's hours, over the whole night and within 90 minutes on each
side of each boundary.

- A futures bar whose `Date` differs from history's fails its unit.
- A crypto bar, on either side, whose `Date` is not the UTC date of its start fails its unit.
- Each unit is compared with the night it passed in, so a reconciliation that runs late,
  after the next night, still reads the right night: one night per run, the oldest first.

`status` shows the next stream night, and `REPORT.md` in each zip has a section per night.

### Secrets

The program reads the secrets only from the environment, and never writes them.

- **Every line it prints or writes** passes through a filter that replaces with `***` each
  secret's value (plain or URL-encoded), a proxy URL's password, and any `apiKey=`,
  `Authorization`, or `X-Amz-` value.
- **`pack` scans** every file of the results folder for each secret, plain, URL-encoded, and
  base64, and refuses to zip a folder in which it finds one.
- **`pack` refuses** unless the terminal holds exactly the secrets the run used. `run` records
  a salted fingerprint of each one, never its value, so a scan for other values cannot pass
  by mistake.

The program's logic lives in `tests/_live/live_run/` (`cli.py` and the modules it names),
unit-tested against the fakes; the script is its entry point.

## Sources

- [S1] https://massive.com/pricing
- [S2] https://massive.com/business
- [S3] https://massive.com/knowledge-base/article/what-are-the-different-massive-subscriptions-i-can-use
- [S4] https://massive.com/knowledge-base/article/does-massive-offer-free-trials
- [S5] https://massive.com/knowledge-base/article/how-can-i-downgrade-or-cancel-my-massive-account
- [S6] https://massive.com/knowledge-base/article/are-all-indices-available-through-the-basic-subscription-tier
- [S7] https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets
- [S8] https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value
- [S9] https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse
- [S10] https://massive.com/knowledge-base/article/what-time-are-massives-flat-files-ready-each-day
- [S11] https://massive.com/knowledge-base/article/why-doesnt-a-bar-from-the-rest-api-match-the-flat-files
- [S12] https://massive.com/knowledge-base/article/how-many-massive-websocket-connections-can-i-use-at-one-time
- [S13] https://massive.com/knowledge-base/article/how-many-tickers-can-you-subscribe-to-on-a-single-massive-websocket-connection
- [S14] https://massive.com/docs/flat-files/quickstart.md
- [S15] the "Plan Access" table of each documentation page, vendored under `vendor/massive/`
