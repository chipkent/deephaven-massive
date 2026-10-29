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

## Declaring your plans

Massive sells one subscription per asset class (Stocks, Options, Indices, Currencies for forex
and crypto, Futures), each on its own plan, and the S3 flat files ride on the paid plans
([S1], [S3]). Declare what the key holds in `MASSIVE_LIVE_PLANS`, comma-separated:
`<class>=<plan>` for each paid class (`starter`, `developer`, `advanced`, or `business`), and
`s3` when the S3 credentials are set. Every class you leave out is Basic, the free plan.

```bash
export MASSIVE_LIVE_PLANS="currencies=starter,options=starter,stocks=starter,s3"
```

Each test declares the plans it needs (`@pytest.mark.needs("currencies:starter", "s3")`). A
test whose plans are not declared skips before any request, naming the plan and its monthly
price, such as "needs Currencies Starter ($49/month)". A test whose plans are declared but is
refused anyway fails, and the refusal is recorded: either the declaration or the documented
plan table is wrong. The smoke tier, which keeps no per-dataset plan table, reports a refused
dataset as "not entitled: needs <plan>" and counts it apart from failures.

Basic is REST only, without WebSocket access or flat files ([S3], [S7]). Two more
consequences:

- **Pacing.** Basic allows 5 REST requests a minute per class; paid plans have no limit.
  Every REST request of a live test to a class declared Basic (the library's own and the
  tests' raw ones) waits for a place in that class's run-wide budget of 5 a minute, and the
  test's timeout grows with its declared request count (`@pytest.mark.requests(n)`), so a run
  on a free key is slow but never refused for its own pace. A live run also lifts the 120 s
  limit after which faulthandler would end the process; each test's own timeout bounds it.
- **Indices.** Indices Basic excludes SPX, DJI, RUT, and VIX ([S6]): with Indices on Basic,
  the index tests use `I:NDX` instead of `I:SPX`.

The rate-limit tests need at least one class left on Basic (Indices is preferred), and they
run first in `-m integration`, while that class's minute is unspent. They draw a few HTTP 429
refusals on purpose.

## S3 credentials

The flat files are an S3 bucket, `flatfiles` at `https://files.massive.com`, with credentials
of their own: an access key id and a secret access key, separate from the API key. Massive's
dashboard issues them on its keys page, beside the API key ([S14]). Each class's files need
that class's Starter plan or above (trades files Developer, stocks and options quotes files
Advanced). Export them:

```bash
export MASSIVE_S3_ACCESS_KEY_ID=...
export MASSIVE_S3_SECRET_ACCESS_KEY=...
```

and add `s3` to `MASSIVE_LIVE_PLANS`.

## What your plan can verify

The open questions each test settles (`@pytest.mark.settles`) are summed up at the end of a
run: "verified X of Y", then each remaining group under the plan it needs and its price.
Prices are monthly ([S1], [S2]); Massive offers no free trial, and a cancelled plan keeps its
access to the month's end ([S4], [S5]).

| Plans | Per month | What it settles |
|---|---|---|
| Every class on Basic | $0 | about 12 questions: the reference keys (ticker events, inactive tickers, exchanges, conditions), the filings without a ticker, the insider activity and holdings keys, week, month, and multi-unit bar alignment, the futures sessions, labels, and holiday session (from minute bars), the crypto minute rebuild, the REST sides of the crypto and forex day cuts, the encoded index path (with `I:NDX`), the stock and index half-day stamps, the split basis (NVDA around its 2024 split, the adjusted echo on every page, COST's dividend, stock dividends), an unknown ticker's statuses, the daily indicator stamps, the futures session labels around the 17:00 Chicago roll and Good Friday; plus the 429 text and the rate buckets |
| Currencies Starter | $49 | adds the crypto and forex day cuts and file pairs, whether forex weekend day files exist, the REST timestamp units, the forex topic spelling, the unsubscribe, frame-size, slot, and wildcard checks, and the raw frame forms (with `s3` for the files) |
| Options Starter | $29 | adds the index and `BRK.B` underlying spellings across the chain and snapshots, the adjusted deliverables (with full contracts history), and the option half-day stamps |
| Stocks Starter | $29 | adds the delayed-key refusal text, the flat-file publication time, and that a weekend flat-file key answers 404 `NoSuchKey` through files.massive.com and NVDA's as-traded day file (with `s3`; if it answers anything else, the library raises on every flat-file range that holds a weekend) |
| **Currencies, Options, and Stocks Starter** | **$107** | every priority question |
| the same and Futures Starter | $136 | adds the futures files' sessions, the snapshot's `timeframe` fields, its ticker location, and the streamed futures dates and one-second bar ends: most of the open questions |
| Indices Starter | $49 | the index values file unit, `I:SPX` everywhere, and possibly the index value stream (the documentation says Starter, the pricing page Advanced) |
| Stocks Advanced; Options Advanced | $199 each | the LULD unit; the options topic cap |
| Business plans | $999 and up | the FMV unit (Currencies Business, $999) and the base Business host (Stocks Business, $2,499) ([S2], [S8]); otherwise covered by fixture tests only |

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
| `MASSIVE_DELAYED_API_KEY` | the delayed-key refusal text, when Stocks is not declared Starter or Developer (then the main key serves) | it skips |

## Safety

- **Streams.** Every stream test subscribes to one symbol per asset class, releases its stream
  as soon as the first rows arrive, and closes its session at the end, so no cluster holds more
  than one of the run's connections. The exceptions are the limit tests.
- **REST.** Some checks query several symbols over REST, because the question is about the
  differences between them: nine options underlyings, five contract roots, four stock ticker
  types and three partner pages, nine futures products' schedules, the inactive tickers from
  `A` to `AC`, every exchange and condition, and a month of four filings datasets.
- **Other clients.** Stop every other client of the key before you run: a stream test on a
  cluster another client uses either waits for a connection slot or disconnects that client.

## Run order

Delete `live-evidence.log` first. Every test appends what it observes to that file as it goes,
flushed line by line, so a run cut short keeps everything recorded before; the same lines close
each run's terminal output, under "live evidence". Run these steps on a US market weekday that
is not a holiday, with the variables above exported:

1. **At the 09:30 New York open**, the LULD check (Stocks Advanced), while bands are published
   for every symbol:

   ```bash
   uv run pytest -m integration -k luld 2>&1 | tee -a live-checks.log
   ```

2. **After 11:30 New York time**, when yesterday's flat files are published, the evidence
   tests; the rate-limit tests run first:

   ```bash
   uv run pytest -m integration -k "not luld" 2>&1 | tee -a live-checks.log
   ```

3. **The smoke tier**, unattended:

   ```bash
   uv run pytest -m integration_smoke 2>&1 | tee -a live-checks.log
   ```

4. **The wide WebSocket tests**: one frame of 1,000 crypto topics, 501 far-dated options
   contracts on two channels, and `XT.*` with `XT.BTC-USD` for 30 s:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   ```

5. **Last and alone**, the second-connection test. It takes a second connection slot with the
   same key on purpose; if Massive drops the older connection instead of refusing the newer,
   the two sessions can drop each other until the test ends:

   ```bash
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

## Reading the results

A test fails when a request it needs fails or returns nothing, when it is refused although
its plans are declared, or, where the check is a plain yes or no (a timestamp's unit, an
encoded path, a missing file), when the answer is no. A skip names what was missing: a plan, a
key, or S3 credentials. Everything else is evidence: read each test's docstring for what it
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
