# Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself. They settle questions the
vendor's documentation leaves open (a timestamp's unit, where a day bar is cut, how a ticker
is spelled), and they are rerun whenever Massive may have changed. Each test records what
Massive sent; its docstring says what the evidence shows and what each answer means for the
library. This page says how to run them safely.

## Safety

- **Never by default.** Every live test carries the `integration` marker, which the default
  run deselects, so `./scripts/check.sh` and CI never select them. Each one also skips without
  `MASSIVE_API_KEY`, and always when `CI` is set, so a key added to CI later still never
  reaches Massive.
- **Limit tests are separate.** Seven tests deliberately exceed Massive's subscription and
  connection limits. They also carry `integration_limits`: `-m integration` leaves them out,
  and only `-m integration_limits` runs them. `tests/conftest.py` enforces both markers.
- **Streams.** Every stream test subscribes to one symbol per asset class, releases its stream
  as soon as the first rows arrive, and closes its session at the end, so no cluster holds more
  than one of the run's connections. The exceptions are the limit tests.
- **REST.** Some checks query several symbols over REST, because the question is about the
  differences between them:
  - the options underlyings `SPX`, `I:SPX`, `SPXW`, `NDX`, `NDXP`, `VIX`, `VIXW`, `BRK.B`, and
    `BRKB` (one page each of the contracts reference and the chain);
  - the contract roots `DELL1`, `FF1`, `AAPL7`, `AAPL`, and `SPX` (up to 15 pages each);
  - the stock ticker types `WARRANT`, `UNIT`, `RIGHT`, and `PFD`, and one page of three
    partner datasets;
  - the March 2026 schedules of nine futures products;
  - the inactive tickers from `A` to `AC`, every exchange and condition, and a month of four
    filings datasets.
- **Other clients.** Stop every other client of the key before you run: a stream test on a
  cluster another client uses either waits for a connection slot or disconnects that client.
- **Timeouts.** Each test is bounded at 100 s, below the 120 s after which the process is
  stopped. Stream tests wait at most 60 s for their first frames.

## Environment

| Variable | Needed by | Without it |
|---|---|---|
| `MASSIVE_API_KEY` | every test | every test skips |
| `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY` | the flat-file tests and the flat-file halves of the day-bar checks | they skip |
| `MASSIVE_BUSINESS_API_KEY` | the host check of a base Business key | it skips |
| `MASSIVE_DELAYED_API_KEY` | the text of the delayed-key refusal | it skips |
| `MASSIVE_LIVE_BASIC_SUBSCRIPTION` | set it when the account holds a subscription on the free Basic tier (`stocks`, `options`, `indices`, `currencies`, or `futures`) | the rate-limit tests skip |

A subscription on Basic answers only 5 REST requests per minute. With
`MASSIVE_LIVE_BASIC_SUBSCRIPTION` set, every test that reads that subscription's data over REST
skips, because its answers would be refusals; the rate-limit tests use it on purpose. The FMV
check needs a Business entitlement on `MASSIVE_API_KEY` itself; without it, it records the
refusal and skips.

The fixed dates (2026-01-20, the weeks of 2026-02-23 to 2026-03-22, the first quarter of 2026,
2026-06-01) lie inside a Basic plan's two years of history until 2028. Three option roots need
older history: `DELL1` (2021), `FF1` (2024), and `AAPL7` (2014). When a date leaves the plan's
window, its check returns no rows; the evidence then says so, and the date should move.

## Run order

Delete `live-evidence.log` first. Every test appends what it observes to that file as it goes,
flushed line by line, so a run cut short keeps everything recorded before; the same lines close
each run's terminal output, under "live evidence". Run these steps on a US market weekday that
is not a holiday, with the variables above exported:

1. **At the 09:30 New York open**, the LULD check, while bands are published for every symbol
   (later, one symbol's bands change only on a large move):

   ```bash
   uv run pytest -m integration -k luld 2>&1 | tee -a live-checks.log
   ```

2. **After 11:30 New York time**, when yesterday's flat files are published, the main suite:

   ```bash
   uv run pytest -m integration -k "not luld" 2>&1 | tee -a live-checks.log
   ```

3. **The wide WebSocket tests**: one frame of 1,000 crypto topics, 501 far-dated options
   contracts on two channels, and `XT.*` with `XT.BTC-USD` for 30 s:

   ```bash
   uv run pytest -m integration_limits tests/massive/ws/test_client_integration.py 2>&1 | tee -a live-checks.log
   ```

4. **The rate-limit tests**, with `MASSIVE_LIVE_BASIC_SUBSCRIPTION` set. They provoke a few
   HTTP 429 refusals on that subscription, each after letting a minute pass:

   ```bash
   uv run pytest -m integration_limits tests/massive/rest/test_client_integration.py 2>&1 | tee -a live-checks.log
   ```

5. **Last and alone**, the second-connection test. It takes a second connection slot with the
   same key on purpose; if Massive drops the older connection instead of refusing the newer,
   the two sessions can drop each other until the test ends:

   ```bash
   uv run pytest -m integration_limits tests/ingest/test_ws_integration.py 2>&1 | tee -a live-checks.log
   ```

Without a key, `uv run pytest -m integration` skips its 46 tests and `-m integration_limits`
its 7.

## Reading the results

A test fails when a request it needs fails or returns nothing, or, where the check is a plain
yes or no (a timestamp's unit, an encoded path, a missing file), when the answer is no. A skip
names what was missing. Everything else is evidence: read each test's docstring for what it
means. `live-evidence.log` and `live-checks.log` are git-ignored. They hold raw licensed rows
(quotes, partner data such as Benzinga and ETF Global): attach them only where the audience may
see that data, and redact those rows before sharing them anywhere public.
