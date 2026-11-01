# Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself. They settle questions the
vendor's documentation leaves open (a timestamp's unit, where a day bar is cut, how a ticker
is spelled), and they are rerun whenever Massive may have changed. Each test records what
Massive sent; its docstring says what the evidence shows and what each answer means for the
library. This page says how to run them, safely and within the plans you hold.

## Tiers

| Marker | What it runs | Command |
|---|---|---|
| `integration` | the evidence tests, one question each | `uv run pytest -m integration` |
| `integration_smoke` | every dataset and verb once, checked automatically | `uv run pytest -m integration_smoke` |
| `integration_limits` | the four tests that deliberately exceed Massive's subscription and connection limits | `uv run pytest -m integration_limits` |
| `integration_rate_limits` | the three tests that deliberately exceed a Basic class's 5 requests a minute | `uv run pytest -m integration_rate_limits` |

Every live test carries `integration`; the smoke, limit, and rate-limit tests also carry their
own marker, and `-m integration` leaves them out. The default run deselects all of them, so
`./scripts/check.sh` and CI never select a live test, and every live test skips without
`MASSIVE_API_KEY` and whenever `CI` is set. `tests/conftest.py` enforces the markers.

## How the tests know your plans

Massive sells one subscription per asset class (Stocks, Options, Indices, Currencies for forex
and crypto, Futures), each on its own plan, and the S3 flat files ride on the paid plans
([S1], [S3]). You buy any mix and can upgrade during the month, so the tests ask Massive what
the key holds instead of taking your word for it. The first live test of a run probes, with
a few cheap requests per class:

| What it asks | What an answer means |
|---|---|
| a snapshot of one liquid ticker (AAPL, SPY's option chain, `I:NDX`, `C:EURUSD`, one ES contract) | Starter or above; refused means Basic |
| that ticker's trades, then its quotes (stocks, and the chain's contract with the most volume that day) | Developer (trades), Advanced (quotes); an empty answer counts as access, since the plan refuses with a 403 |
| that contract's trades (futures) | Developer or above; Developer and Advanced both include quotes, so futures never read as Advanced |
| a non-null `fmv` in the snapshot (stocks, options, currencies) | Business; for stocks and options always the base Business plan, since its REST trades and quotes answer too (at end-of-day recency), so only a declaration names the trades and quotes expansion |
| a real-time indices snapshot | Advanced |
| one file of every flat-file prefix | which flat files the S3 credentials reach: only a file that opens counts |

A snapshot counts only with at least one row, since every probe asks for a liquid ticker. An
empty snapshot proves nothing either way, so the probe stops and the run asks you to declare
the plans instead. For the same reason the probe does not run from 03:30 to 04:30 New York
time, while Massive clears and refills its snapshots and a Business plan's `fmv` can be
missing. A stocks or options Business plan is not an Advanced plan: without the
expansion it streams no trades, quotes, LULD, or per-second bars, so a test that needs Advanced
skips on it; declare `business_expansion` to run those tests on a key with the expansion. Indices Business leaves no mark a request can see and reads as Advanced. Futures
Business is sold per venue (CME, CBOT, NYMEX, COMEX) and reads as Developer; a test of a venue
the plan lacks is refused. The answer is kept in `.live-plans.json` in the repository root (git-ignored; it names the key and
the S3 access key id only by a digest) until the New York day ends, so later runs that day do
not probe again. Delete the file to probe again at once, after an upgrade. The plans head the
evidence (under "live plans") and the summary at the end of the run. The live sessions list
every class on a delayed plan (Stocks, Options, and Futures Starter and Developer; Indices
Starter) in `MassiveConfig.delayed_subscriptions`, since the library refuses delayed data for
any other: their streams use Massive's delayed feed, and their REST reads are not checked.

To skip the probe, declare the plans in `MASSIVE_LIVE_PLANS`, comma-separated:
`<class>=<plan>` for each paid class (`starter`, `developer`, `advanced`, `business`, or, for
stocks and options, `business_expansion`), and `s3` when the S3 credentials reach the flat
files. Every class you leave out is Basic, the
free plan. A declaration overrides the probe and the cache.

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

Each test declares the plans it needs (`@pytest.mark.needs("currencies:starter", "s3")`). A
test whose plans the key lacks skips before any request, naming the plan and its monthly
price, such as "needs Currencies Starter ($49/month)". A request refused because it reaches
further back than the plan's history ("Your plan doesn't include this data timeframe") skips
the test as "outside the plan's history" when the day it asked for is before the first day
the plan keeps, by the history windows Massive documents per plan (two years on every Basic
plan). Refused for a day the plan keeps, it fails, since that may be a library bug. Any other
refusal of a test whose plans the key
holds fails, and the refusal is recorded: either the probe or the documented plan table is
wrong. The smoke tier reports a refused dataset as "not entitled: needs <plan>" and counts it
apart from failures.

Basic is REST only, without WebSocket access or flat files ([S3], [S7]). Two more
consequences:

- **Pacing.** Basic allows 5 REST requests a minute per class; paid plans have no limit.
  The library paces these itself: the classes on Basic become the live sessions'
  `MassiveConfig.rest_basic_subscriptions`, and every live test shares one rate governor per
  key for the whole run, on the real clock, so a REST request to a Basic class (the library's
  own and the tests' raw ones) waits for its class's place within 5 a minute. No test waits
  more than about a minute: a test that may read a class on Basic sends that class at most 5
  requests when it is on Basic, and declares that count with `@pytest.mark.requests(n)`. To
  stay within it, a test reads less on Basic than on a paid plan: fewer underlyings,
  symbols, or pages, or none of a part that repeats what another request shows (the
  underlying spellings read only the contracts of `SPX`, `I:SPX`, `BRK.B`, and `BRKB`; the
  split and dividend checks try only the newest event). Each such test's docstring says what
  it leaves out, and its evidence has a line saying so. A unit test fails when a live test
  that may read a class on Basic declares more than 5, and a live run skips such a test while
  its class is on Basic, with the reason "needs a paid <class> plan: N requests would take
  about M minutes on Basic". Each time a request waits, the run prints one line, such as
  `live pacing: options: waiting 47 s for a request slot (paced at 5 requests a minute); 3 of
  this test's 5 requests left`, so a paced run never looks hung; a wait shorter than 5 s
  prints nothing. A test's timeout also grows
  with its count. A live run lifts the 120 s limit after which faulthandler would end the
  process; each test's own timeout bounds it.
- **How long a run takes.** The command of "Run order" below runs the rate-limit tests, the
  evidence tests, and the smoke tier. With every class on Basic, the evidence tests that run
  (the "Every class on Basic" row below) declare at most about 74 requests to stocks
  (reference data included), 32 to currencies, 29 to futures, 8 to options, and 7 to indices,
  and the smoke tier at most 88 to stocks, 36 to currencies, 24 each to options and futures,
  and 14 to indices; its stream cases skip before any request, since Basic does not stream.
  The classes are paced side by side, so stocks sets the pace: at 5 a minute its requests
  take up to about 15 minutes for the evidence tests and 18 for the smoke tier, and the
  rate-limit tests wait out about six minutes between them, so a free key's run takes about
  40 minutes, less when a test sends fewer requests than it declares. On paid plans nothing
  waits for its pace, but each stream case waits for its rows while its market is open (up
  to 75 s for a minute bar), so the smoke tier takes about half an hour in market hours and
  under ten minutes when the markets are closed, and the delayed-feed check of a real-time
  Stocks key can add up to 17 minutes in the first quarter hour of a stock session.
- **Indices.** Indices Basic excludes SPX, DJI, RUT, and VIX ([S6]): with Indices on Basic,
  the index tests use `I:NDX` instead of `I:SPX`.

The rate-limit tests need at least one class left on Basic (Indices is preferred). They carry
`integration_rate_limits`, so `-m integration` leaves them out; the command of "Run order"
names them, and they run first, each after a minute's pause so earlier requests no longer
count. They need no open market, so to keep market hours for the streams, run them apart at
any other time and leave them out of the market-hours run:

```bash
uv run pytest -m integration_rate_limits 2>&1 | tee -a live-checks.log
uv run pytest -m "integration or integration_smoke" 2>&1 | tee -a live-checks.log   # in market hours
```
They draw a few HTTP 429 refusals on purpose, so each sends its class more than 5 requests:
one records the refusal's text and which requests outside every subscription Massive
counts; one whether a ticker lookup filtered by a class's market (Indices, Currencies, or
Stocks) is charged to that class; and one whether a session learns the limit from the
refusal and a later session of the key forgets it, which takes about three minutes.

## Dates, markets, and corporate actions

No test reads a fixed date. Each computes its days from the run's New York day with the US
exchanges' holiday rules: the most recent Monday that traded and whose files are published,
the most recent calendar week without a holiday, the most recent whole month, and the most
recent half day (the Friday after Thanksgiving, July 3, or December 24) whose week has ended.
The futures tests read the contract that was front on their day, and the holiday-session
checks the most recent Martin Luther King Day and Good Friday. The split and dividend checks
find the year's most recent forward split and special cash dividend through the library's own
`stocks.splits` and `stocks.dividends`, choosing common stocks with no later split. So every
read stays inside a Basic plan's two years of history. The three adjusted option roots of the
deliverable check (from 2014, 2021, and 2024) are the exception: each is read only when the
key's Options plan keeps its day, by the documented history windows, and is otherwise
recorded as outside the plan's history.

A stream test passes only when rows reached its table. It first asserts that Massive
acknowledged its topics, which Massive does whether or not the market is open, then reads the
market's status (`/v1/marketstatus/now`, or for futures ES's `market_event` in
`/futures/v1/market-status`). Rows are required while the market is open for the stream's kind
of rows: stock trades and quotes also in the extended hours (status `extended-hours`, from
04:00 New York time), every other stock, option, and index stream only in the regular session
(from 09:30). On a delayed plan the first rows of a session arrive only after the plan's
delay, so rows are required once the session has been open that long: 15 minutes on Stocks,
Options, and Indices Starter (and Stocks and Options Developer), 10 minutes on Futures Starter
and Developer, counted for futures from the `timestamp` of its market status. A minute bar
stream waits until just past the next minute boundary, up to 75 s. When rows are not required,
the test skips after the acknowledgement, "not exercised: market closed (acknowledged)" or the
like; it never passes on the acknowledgement alone. The LULD check needs the regular session,
the only time bands change, and the smoke tier streams LULD bands of every ticker for the same
reason; stock imbalances arrive only around the auctions, so their smoke case always skips
after the acknowledgement. A skip for the market's hours is a condition of the moment: the
live-run program runs the check again on a later attempt, and a whole stream day fails a unit
whose streams delivered no row once its session had opened. A results folder whose stream
checks passed before streams had to deliver rows (the smoke stream cases, the delayed-feed
check, and the stream-day units) treats those passes as unproven, so they run again by
themselves.

The checks of the wire protocol that belong to no asset class (the unsubscribe, the 1,000-topic
frame, the wildcard) stream crypto, which trades all the time; on a key that streams stocks
but no currencies they stream stocks instead, on the feed of the key's Stocks plan, while stock
bars are due. Two more checks prove rows directly: one closes the session's streaming socket
from under it and requires the crypto stream to reconnect, return to `RUNNING`, and deliver new
trades, recording any `WAITING_SLOT` on the way; one reads AAPL's minute bars from the previous
trading day through today on `AUTO` and on REST, before today's flat file exists, and requires
the same minutes from both, no day missing or doubled, recording `AUTO`'s warning that it
filled days from REST (it needs Stocks Starter and the S3 credentials).

Two checks are about a delayed plan's labels. The past-data check records the envelope status
Massive gives AAPL's previous day bar, and counts only when that status is `DELAYED`: on a key
that gets `OK`, it uses `MASSIVE_DELAYED_API_KEY` when set and otherwise skips, "needs a Basic
or Starter stocks key". And because the live sessions list every delayed class in
`delayed_subscriptions`, one check opens a session with the key alone, as a user who lists
nothing would, and requires the first delayed class's stream (or, for options, the chain) to
be refused with a message that names the remedy.

## S3 credentials

The flat files are an S3 bucket, `flatfiles` at `https://files.massive.com`, with credentials
of their own: an access key id and a secret access key, separate from the API key. Massive's
dashboard issues them on its keys page, beside the API key ([S14]). Each class's files need
that class's Starter plan or above (trades files Developer, stocks and options quotes files
Advanced). Export them, and the probe finds which files they reach:

```bash
export MASSIVE_S3_ACCESS_KEY_ID=...
export MASSIVE_S3_SECRET_ACCESS_KEY=...
```

## What your plan can verify

Each test names the open questions it answers. One marked `verifies` asserts the answer the
library assumes, so it fails the day Massive changes; one marked `settles` records evidence
that a person reads. The summary at the end of a run says "verified X of Y" for the asserted
questions whose every test passed, "ran" for the evidence-only questions whose every test
passed (their evidence still needs reading), then each remaining group under the plan it needs
and its price. Prices are monthly ([S1], [S2]); Massive offers no free trial, and a cancelled
plan keeps its access to the month's end ([S4], [S5]).

| Plans | Per month | What it settles |
|---|---|---|
| Every class on Basic | $0 | the reference keys (ticker events, inactive tickers, exchanges, conditions), the filings without a ticker, the insider activity and holdings keys, week, month, and multi-unit bar alignment, the futures sessions, labels, and holiday session (from minute bars), the crypto minute rebuild, the REST sides of the crypto and forex day cuts, the encoded index path (with `I:NDX`), the stock and index half-day stamps, the split basis (the year's latest forward split, the adjusted echo on every page, the latest special cash dividend, stock dividends), an unknown ticker's statuses, the daily indicator stamps, whether past stock data (the previous day's bar, a day's open and close) reads on the default configuration though Massive labels it `DELAYED`, the futures session labels around the 17:00 Chicago roll and Good Friday; the futures spread ticker forms of every venue, how Massive writes a futures contract's year (one digit or two), the crypto ticker list's currencies; plus the 429 text, the rate buckets, whether a market-filtered ticker lookup is charged to its class, how a spent minute frees its requests, and whether a learned limit outlives the key's sessions |
| Currencies Starter | $49 | adds the crypto and forex day cuts and file pairs, whether forex weekend day files exist, the REST timestamp units, the forex topic spelling, the unsubscribe, frame-size, slot, and wildcard checks, and the raw frame forms (with `s3` for the files) |
| Options Starter | $29 | adds whether a delayed plan's option chain says `DELAYED` (its envelope status or `last_trade.timeframe`, which `fetch()` and `poll()` refuse unless options are listed in `delayed_subscriptions`; **UNVERIFIED**), the index and `BRK.B` underlying spellings across the chain and snapshots, the adjusted deliverables (the oldest roots only on a plan that keeps their day), and the stamps of SPY and SPXW 1-day, half-day minute, and week bars on every source (with `s3` for the day file) |
| Stocks Starter | $29 | adds what Massive says when a delayed key subscribes on the real-time feed, and the stamps of AAPL's 1-day, week, and month bars on every source; with `s3`, also the flat-file publication time, the split stock's as-traded day file, and whether a weekend flat-file key answers 404 `NoSuchKey` through files.massive.com (if it answers anything else, the library raises on every flat-file range that holds a weekend) |
| **Currencies, Options, and Stocks Starter** | **$107** | every priority question |
| the same and Futures Starter | $136 | adds the futures files' sessions, the snapshot's `timeframe` fields and envelope status (whether a delayed futures plan's REST answers say `DELAYED`, the only futures evidence of a delayed plan the library reads, for streams and for `fetch()` and `poll()` alike; **UNVERIFIED**), its ticker location, and the streamed futures dates and one-second bar ends: most of the open questions |
| Indices Starter | $49 | the index values file unit, `I:SPX` everywhere, the index 1-day, week, month, quarter, and year bar stamps and which day and week they cover (`I:SPX` and `I:AAVE100` rebuilt from minute bars; with `s3` for the day file), and possibly the index value stream (the documentation says Starter, the pricing page Advanced) |
| Stocks Advanced; Options Advanced | $199 each | the LULD unit; whether Massive serves a real-time Stocks key on its delayed feed (`delayed_subscriptions` listing stocks); the options topic cap |
| Business plans | $999 and up | the FMV unit (Currencies Business, $999) and the base Business host (Stocks Business, $2,499) ([S2], [S8]); otherwise covered by fixture tests only. Futures Business is sold per venue, and covers only the tests of its venue |

The bar-stamp checks read the most recent half day whose week has ended and the full trading
day before it. A December 24 half day stays the latest until the next Thanksgiving week ends,
so these reads can be about eleven months old, still inside every plan's two years of history. Their month and quarter reads are the two whole ones before the run's, and the
index year read runs from January 1 to the run's day; before the year's first trading day
there is no year bar yet, and the year read is recorded as skipped.

Partner datasets (Benzinga, ETF Global, TMX, consumer spending) are $99 a month each; NOI is
a $49 expansion ([S9]); economy data and each class's reference data are free on Basic.
The Benzinga add-on adds one check, that every consensus-ratings row names its ticker; without
the add-on that check skips.
Massive's knowledge base already states some answers, which the live tests confirm: a missing
flat file answers 404 `NoSuchKey`, one subscribe frame of 120,003 entries is accepted, a
connection carries about 1,000 options contracts, grouped daily stamps the end of its window,
and the crypto and forex files spell `X:BTC-USD` and `C:EUR-USD` ([S10] to [S13]). Each
documentation page names its plan in its "Plan Access" table ([S15]).

## Other variables

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | every test | every test skips |
| `MASSIVE_DELAYED_API_KEY` | what Massive says when a delayed key subscribes on the real-time feed, when Stocks is not on Starter or Developer (then the main key serves); whether past stock data reads on the default configuration, when Massive labels the main key's past data `OK` | it skips |

## Safety

- **Streams.** Every stream test subscribes to one symbol per asset class, except the LULD
  check, which streams the bands of every ticker (`LULD.*`) so that some arrive within seconds
  of an open session. Each releases its stream as soon as the first rows arrive and closes its
  session at the end, so no cluster holds more than one of the run's connections. The
  exceptions are the limit tests.
- **REST.** Some checks query several symbols over REST, because the question is about the
  differences between them: nine options underlyings, five contract roots, four stock ticker
  types and three partner pages, nine futures products' schedules, every futures combo
  product and the combo contracts of four venues, the crypto ticker list (at most 5 pages on
  Basic), the inactive tickers from `A` to `AC`, every exchange and condition, and a month of
  four filings datasets.
- **Other clients.** Stop every other client of the key before you run: a stream test on a
  cluster another client uses either waits for a connection slot or disconnects that client.

## Run order

Each run writes its evidence to a file of its own in the repository root,
`live-evidence-<UTC time>.log`, and prints its path when the first line is recorded. Every
test appends what it observes to that file as it goes, flushed line by line, so a run cut
short keeps everything recorded before; the same lines close each run's terminal output, under
"live evidence". Stop every other client of the key, export the variables above, and run, at
any hour and on any day:

```bash
uv run pytest -m "integration_rate_limits or integration or integration_smoke" 2>&1 | tee -a live-checks.log
```

The run probes the plans, runs the rate-limit tests first, then every evidence test and the
smoke tier. Whatever the key lacks, the market's hours, or the plan's history leaves out
skips with its reason; nothing needs a person to time it, but a stream proves its rows only
while its market is open, so a run in market hours checks more than one on a weekend. Two checks answer more on some days:
the LULD check needs the regular session for rows, and `/prev` shows a short-day stamp only on
the trading day after a half day (its evidence marks that day `half day: yes`).

Only the limit tests need a person: they exceed Massive's limits on purpose, so run them
apart, after the run above, with no other client of the key. First the wide WebSocket tests
(one frame of 1,000 crypto topics, 1,000 far-dated options contracts on two channels, and `XT.*`
with `XT.BTC-USD` for 30 s), then, last and alone, the second-connection test, which takes a
second connection slot with the same key; if Massive drops the older connection instead of
refusing the newer, the two sessions can drop each other until the test ends:

```bash
uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
```

## The live-run program

For a long run on a tester's machine, `scripts/live_run.py` runs everything above unattended,
for days, and records what is needed to diagnose a problem afterwards without another live
run. It probes what the key can do at each start and each trading morning, runs every group of
checks at a fixed New York time, adapts by itself when the subscriptions change, and can be
stopped and started at any time. A tester follows [the tester guide](tester-guide.md), which
walks through it step by step. Run it from the repository root, in the terminal that holds
the variables above:

```bash
uv run python scripts/live_run.py check                       # the machine and the key; writes nothing
uv run python scripts/live_run.py run --out ~/massive-live    # add --any-time for a first run now
uv run python scripts/live_run.py status --out ~/massive-live
uv run python scripts/live_run.py pack --out ~/massive-live   # writes live-results-<date>.zip
```

| Group | What it runs | When (New York time) |
|---|---|---|
| `rate-limits` | `-m integration_rate_limits` | from 11:30 |
| `evidence` | `-m integration -k "not luld"` | from 11:30, after the previous day's flat files |
| `luld` | `-m integration -k luld` | 09:28, for the opening bands |
| `smoke` | `-m integration_smoke` | from 11:30 |
| `responses` | the documentation's examples, each in its own session, every raw reply recorded | from 11:30 |
| `limits-ws`, `limits-second-connection` | the two limit-test commands above, the second last and alone | from 16:15 |
| `stream-day` | a full trading day of streams for every class the key can stream | 03:45 to 20:30 on a planned day |
| `throughput` | every symbol's busiest streams, measured over an hour | 09:15 to 11:00 the next day |
| `stream-day-reconcile` | the streamed minute bars against REST history | from 11:30 the next day |
| `half-day` | the bar-stamp tests | on the trading day after a half day |

Each group runs in a child process, with the probed plans in `MASSIVE_LIVE_PLANS`, the
library's debug log in the attempt's folder (`<group>/<date>-<n>/debug.log`), and a pytest
plugin (`tests/_live/live_run/outcomes.py`) that records each test's outcome as it runs, so a crash
loses nothing. The raw replies come from the wire package's reply-capture hook, which hands
over every REST body and WebSocket frame already cleaned of the key. A check that passed is not
run again until a subscription's tier rises; a check that failed runs once more; a check
skipped for a missing subscription waits for the subscriptions to change.

Control-C, a closed terminal, and a `kill` all stop the program the same way: the running
child is stopped, the state saved, and the same `run` command resumes. A resume first stops any
child an earlier run left behind and keeps every result that child recorded. `--any-time`
takes effect once per results folder; a resume finishes its groups without it.

The program reads the secrets only from the environment and never writes them: every line it
prints or writes passes through a filter that replaces each secret's value (plain or
URL-encoded), a proxy URL's password, and any `apiKey=`, `Authorization`, or `X-Amz-` value
with `***`. `pack` scans every file of the results folder for each secret, plain,
URL-encoded, and base64, and refuses to zip a folder in which it finds one. It also refuses
unless the terminal holds exactly the secrets the run used: `run` records a salted fingerprint
of each one, never its value, so a scan for other values cannot pass by mistake. The
program's logic lives in `tests/_live/live_run/` (`cli.py` and the modules it names),
unit-tested against the fakes; the script is its entry point.

## Reading the results

A test fails when a request it needs fails or returns nothing, when a stream delivers no row
while rows are due, when it is refused although
the key holds its plans, or, for a test marked `verifies`, when Massive's answer is no longer
the one the library assumes. A skip names what was missing: a plan, a key, S3 credentials,
history beyond the plan's window, or a market that is closed or within its feed's delay, so
no row was due ("not exercised"). Everything else is evidence: read each
test's docstring for what it means. The smoke tier ends with its coverage: passed (with the
count that passed with drift: a library WARNING or a `SchemaDrift` the case saw, each listed),
not entitled, otherwise skipped, failed; the not-entitled and skipped cases are grouped by the
plan they need or their reason, with a count per group. The open questions follow, each group
with its count and each question's number beside its one-line title.

One answer comes from `stream-day`'s `debug.log` rather than a test: **does Massive send
frames on a quiet connection?** A connection with acknowledged subscriptions logs a WARNING
after 5 minutes without any frame during its market's regular hours (`no frame for ... during
... trading hours`). Whether that threshold is right depends on whether Massive sends a status
frame while no data flows. From 03:45 to 04:00 New York time the stocks connection holds its
subscriptions and receives no data, since stocks trade from 04:00: note the `status` lines of
that connection (`wss://.../stocks status ...`) in those 15 minutes and the longest stretch
without one, and report every `no frame for` WARNING with its time and connection. The
threshold and its gate change only on that evidence.

The evidence files and `live-checks.log` are git-ignored. They hold raw licensed rows
(quotes, partner data such as Benzinga and ETF Global): attach them only where the audience may
see that data, and redact those rows before sharing them anywhere public.

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
