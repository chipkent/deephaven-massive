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

Every live test carries `integration`; the smoke and limit tests also carry their own marker,
and `-m integration` leaves them out. The default run deselects all of them, so
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
| a snapshot of one liquid ticker (AAPL, one SPY option, `I:NDX`, `C:EURUSD`, one ES contract) | Starter or above; refused means Basic |
| that ticker's trades, then its quotes (stocks and options) | Developer (trades), Advanced (quotes) |
| that contract's trades (futures) | Developer or above; Developer and Advanced both include quotes, so futures never read as Advanced |
| a non-null `fmv` in the snapshot (stocks, options, currencies) | Business; for stocks and options, Business with the trades and quotes expansion when trades or quotes answer too |
| a real-time indices snapshot | Advanced |
| one file of every flat-file prefix | which flat files the S3 credentials reach: only a file that opens counts |

An answer counts only with at least one row, since every probe asks for a liquid ticker. An
empty answer proves nothing either way, so the probe stops and the run asks you to declare
the plans instead. For the same reason the probe does not run from 03:30 to 04:30 New York
time, while Massive clears and refills its snapshots and a Business plan's `fmv` can be
missing. A stocks or options Business plan is not an Advanced plan: without the
expansion it has no trades, quotes, LULD, or per-second streams, so a test that needs Advanced
skips on it. Indices Business leaves no mark a request can see and reads as Advanced. Futures
Business is sold per venue (CME, CBOT, NYMEX, COMEX) and reads as Developer; a test of a venue
the plan lacks is refused. The answer is kept in `.live-plans.json` in the repository root (git-ignored; it names the key and
the S3 access key id only by a digest) until the New York day ends, so later runs that day do
not probe again. Delete the file to probe again at once, after an upgrade. The plans head the
evidence (under "live plans") and the summary at the end of the run.

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
  own and the tests' raw ones) waits for its class's place within 5 a minute. A test that may
  read a class on Basic gets a timeout that grows with its declared request count
  (`@pytest.mark.requests(n)`), so a run on a free key is slow but never refused for its own
  pace. A live run also lifts the 120 s limit after which faulthandler would end the process;
  each test's own timeout bounds it.
- **Indices.** Indices Basic excludes SPX, DJI, RUT, and VIX ([S6]): with Indices on Basic,
  the index tests use `I:NDX` instead of `I:SPX`.

The rate-limit test needs at least one class left on Basic (Indices is preferred), and it
runs first in `-m integration`, after a minute's pause so earlier requests no longer count. It
draws a few HTTP 429 refusals on purpose.

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

A stream test asserts that Massive acknowledged its topics, which Massive does whether or not
the market is open. It waits for rows only while `/v1/marketstatus/now` says the market is
open, and skips, after the acknowledgement, only when that status says closed. The futures
stream reads the E-mini S&P 500's status from `/futures/v1/market-status` instead, and the
LULD check needs the regular session, the only time bands change. In the smoke tier a stream
that passed without a row is counted "acknowledged only".

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
| Every class on Basic | $0 | the reference keys (ticker events, inactive tickers, exchanges, conditions), the filings without a ticker, the insider activity and holdings keys, week, month, and multi-unit bar alignment, the futures sessions, labels, and holiday session (from minute bars), the crypto minute rebuild, the REST sides of the crypto and forex day cuts, the encoded index path (with `I:NDX`), the stock and index half-day stamps, the split basis (the year's latest forward split, the adjusted echo on every page, the latest special cash dividend, stock dividends), an unknown ticker's statuses, the daily indicator stamps, the futures session labels around the 17:00 Chicago roll and Good Friday; plus the 429 text, the rate buckets, and how a spent minute frees its requests |
| Currencies Starter | $49 | adds the crypto and forex day cuts and file pairs, whether forex weekend day files exist, the REST timestamp units, the forex topic spelling, the unsubscribe, frame-size, slot, and wildcard checks, and the raw frame forms (with `s3` for the files) |
| Options Starter | $29 | adds the index and `BRK.B` underlying spellings across the chain and snapshots, the adjusted deliverables (the oldest roots only on a plan that keeps their day), and the stamps of SPY and SPXW 1-day, half-day minute, and week bars on every source (with `s3` for the day file) |
| Stocks Starter | $29 | adds the delayed-key refusal text and the stamps of AAPL's 1-day, week, and month bars on every source; with `s3`, also the flat-file publication time, the split stock's as-traded day file, and whether a weekend flat-file key answers 404 `NoSuchKey` through files.massive.com (if it answers anything else, the library raises on every flat-file range that holds a weekend) |
| **Currencies, Options, and Stocks Starter** | **$107** | every priority question |
| the same and Futures Starter | $136 | adds the futures files' sessions, the snapshot's `timeframe` fields, its ticker location, and the streamed futures dates and one-second bar ends: most of the open questions |
| Indices Starter | $49 | the index values file unit, `I:SPX` everywhere, the index 1-day, week, month, quarter, and year bar stamps and which day and week they cover (`I:SPX` and `I:AAVE100` rebuilt from minute bars; with `s3` for the day file), and possibly the index value stream (the documentation says Starter, the pricing page Advanced) |
| Stocks Advanced; Options Advanced | $199 each | the LULD unit; the options topic cap |
| Business plans | $999 and up | the FMV unit (Currencies Business, $999) and the base Business host (Stocks Business, $2,499) ([S2], [S8]); otherwise covered by fixture tests only. Futures Business is sold per venue, and covers only the tests of its venue |

The bar-stamp checks read the most recent half day whose week has ended and the full trading
day before it. A December 24 half day stays the latest until the next Thanksgiving week ends,
so these reads can be about eleven months old, still inside every plan's two years of history. Their month and quarter reads are the two whole ones before the run's, and the
index year read runs from January 1 to the run's day; before the year's first trading day
there is no year bar yet, and the year read is recorded as skipped.

Partner datasets (Benzinga, ETF Global, TMX, consumer spending) are $99 a month each; NOI is
a $49 expansion ([S9]); economy data and each class's reference data are free on Basic.
Massive's knowledge base already states some answers, which the live tests confirm: a missing
flat file answers 404 `NoSuchKey`, one subscribe frame of 120,003 entries is accepted, a
connection carries about 1,000 options contracts, grouped daily stamps the end of its window,
and the crypto and forex files spell `X:BTC-USD` and `C:EUR-USD` ([S10] to [S13]). Each
documentation page names its plan in its "Plan Access" table ([S15]).

## Other variables

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | every test | every test skips |
| `MASSIVE_DELAYED_API_KEY` | the delayed-key refusal text, when Stocks is not on Starter or Developer (then the main key serves) | it skips |

## Safety

- **Streams.** Every stream test subscribes to one symbol per asset class, except the LULD
  check, which streams the bands of every ticker (`LULD.*`) so that some arrive within seconds
  of an open session. Each releases its stream as soon as the first rows arrive and closes its
  session at the end, so no cluster holds more than one of the run's connections. The
  exceptions are the limit tests.
- **REST.** Some checks query several symbols over REST, because the question is about the
  differences between them: nine options underlyings, five contract roots, four stock ticker
  types and three partner pages, nine futures products' schedules, the inactive tickers from
  `A` to `AC`, every exchange and condition, and a month of four filings datasets.
- **Other clients.** Stop every other client of the key before you run: a stream test on a
  cluster another client uses either waits for a connection slot or disconnects that client.

## Run order

Delete `live-evidence.log` first. Every test appends what it observes to that file as it goes,
flushed line by line, so a run cut short keeps everything recorded before; the same lines close
each run's terminal output, under "live evidence". Stop every other client of the key, export
the variables above, and run, at any hour and on any day:

```bash
uv run pytest -m "integration or integration_smoke" 2>&1 | tee -a live-checks.log
```

The run probes the plans, runs the rate-limit test first, then every evidence test and the
smoke tier. Whatever the key lacks, the market's hours, or the plan's history leaves out
skips with its reason; nothing needs a person to time it. Two checks answer more on some days:
the LULD check needs the regular session for rows, and `/prev` shows a short-day stamp only on
the trading day after a half day (its evidence marks that day `half day: yes`).

Only the limit tests need a person: they exceed Massive's limits on purpose, so run them
apart, after the run above, with no other client of the key. First the wide WebSocket tests
(one frame of 1,000 crypto topics, 501 far-dated options contracts on two channels, and `XT.*`
with `XT.BTC-USD` for 30 s), then, last and alone, the second-connection test, which takes a
second connection slot with the same key; if Massive drops the older connection instead of
refusing the newer, the two sessions can drop each other until the test ends:

```bash
uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
```

## Reading the results

A test fails when a request it needs fails or returns nothing, when it is refused although
the key holds its plans, or, for a test marked `verifies`, when Massive's answer is no longer
the one the library assumes. A skip names what was missing: a plan, a key, S3 credentials,
history beyond the plan's window, or an open market. Everything else is evidence: read each test's docstring for what it
means. The smoke tier ends with its coverage: passed, not entitled, otherwise skipped, failed.

`live-evidence.log` and `live-checks.log` are git-ignored. They hold raw licensed rows
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
