# Soak tests

The soak tests run the library for a long time under heavy load, against the fake Massive
servers in `tests/_fakes/`, and fail on the faults that only show up that way: a row lost or
delivered twice, a hang, or memory or threads that keep growing. They never reach Massive and
need no key, so anyone can run them, on the standard build and on the free-threaded one.

A plain run (`./scripts/check.sh`, CI, `uv run pytest`) leaves them out: they carry the `soak`
marker, which the default `-m` expression deselects.

## What they do

Each test runs for `--soak-seconds` seconds (20 by default) in rounds of at least two
seconds. A fake feed sends stocks trades for 40 symbols as fast as the fake server writes
them, in frames of 100, and numbers each symbol's trades 1, 2, 3, and so on. Each stream's
table is summed per symbol (row count, lowest and highest number, the sum of the numbers, and
the sum of their squares modulo a prime), which proves that it holds an unbroken run of
numbers, each exactly once, while keeping one row of state per symbol however long the run.

| Test | Load | What must hold |
|---|---|---|
| `tests/ingest/test_ws.py::test_soak_every_stream_receives_every_row_of_a_fast_feed` | 51 streams on one connection: one per symbol, ten of three symbols, one of every symbol | every stream ends with every trade of its symbols, each once; no table fails |
| `tests/ingest/test_ws.py::test_soak_streams_opened_and_released_under_load_lose_and_repeat_no_row` | 8 threads open streams of one to three symbols, read them, and release them, nonstop | each short stream holds an unbroken run of each symbol, each trade once; a long stream of every symbol gets every trade; afterwards only its topic stays subscribed |
| `tests/ingest/test_ws.py::test_soak_a_stream_follows_a_churning_watchlist_under_load` | a thread adds and deletes watchlist symbols every 10 to 50 ms | no trade twice within a round; once the watchlist settles, exactly its topics are subscribed and exactly its symbols' trades arrive |
| `tests/ingest/test_ws.py::test_soak_sessions_close_under_load_without_disturbing_a_shared_connection` | each round a second session on the same connection is closed by 4 threads at once while 4 more threads open and release its streams | every close returns; a racing query method succeeds or says the session is closed; every table of the closed session fails; the first session's stream of every symbol still gets every trade once |
| `tests/tables/test_liveness.py::test_soak_released_and_collected_results_free_their_streams` | 8 threads open blink, append-only, last-by, and ring results and let them go, half by `m.release`, half by dropping them for the garbage collector | the session holds only a handful of results and streams after each round, none at the end, and no topic stays subscribed |

After every round the `soak` fixture (`tests/conftest.py`) collects garbage on both sides and
measures the JVM's used heap, the objects Python's collector tracks, and the threads running.
The test fails when, from the end of the first round to the end of the run, the heap grows by
256 MiB or more, Python's objects by 50,000 or more, or the threads by 64 or more. The usual
end-of-test check then fails a test that leaves a session, a library thread, or a rate-governor
lease behind.

Each test also has a time limit of its run plus 300 seconds. A test that hangs is stopped
there, and the report shows every thread's stack, so a deadlock names its threads.

## Running them

A quick run, about two minutes on a laptop:

```bash
uv run pytest -m soak
```

One test, for five minutes:

```bash
uv run pytest -m soak --soak-seconds=300 \
  "tests/ingest/test_ws.py::test_soak_every_stream_receives_every_row_of_a_fast_feed"
```

On the free-threaded build, which keeps its own environment:

```bash
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -m soak
```

Run the soak tests in one process. Under `-n`, several soak tests would share the machine's
cores and each would measure the others' load.

Once its time is up, each test writes a summary line to stderr, before its last checks, such
as:

```text
soak tests/ingest/test_ws.py::test_soak_every_stream_receives_every_row_of_a_fast_feed: 30 rounds in 60 s; JVM heap 114 -> 129 MiB; Python objects 183841 -> 183155; threads 7 -> at most 7; rows=1720000 rows_per_second=28667
```

The rate is the fake feed's. The feed sends each frame only once the previous one is written
to the connection, so a client that falls behind slows it down; the rate is a rough measure
of what the library keeps up with on that machine, not a target.

## Overnight

Give each test about 45 minutes, run the standard build and then the free-threaded one, and
keep the output. With five tests that is close to four hours per build. On a Mac,
`caffeinate -i` keeps the machine awake while the command runs:

```bash
export SOAK_LOGS=~/massive-soak-$(date +%F)
mkdir -p "$SOAK_LOGS"
caffeinate -i bash -c '
  uv run pytest -m soak --soak-seconds=2700 \
    --junitxml="$SOAK_LOGS/standard.xml" 2>&1 | tee "$SOAK_LOGS/standard.log"
  PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t \
    uv run --python 3.14t pytest -m soak --soak-seconds=2700 \
    --junitxml="$SOAK_LOGS/free-threaded.xml" 2>&1 | tee "$SOAK_LOGS/free-threaded.log"
'
```

The logs go outside the repository, so nothing of the night can end up in a commit. To keep
the library's own log as well, add `DEEPHAVEN_MASSIVE_LOG=INFO
DEEPHAVEN_MASSIVE_LOG_FILE="$SOAK_LOGS/library.log"` to each command. `DEBUG` logs every stream
opened and released, and the churn tests open dozens a second, so keep it for a rerun of the
one test that failed.

In the morning, read the summary lines and the result of each test:

- **A lost or repeated row** fails with the stream's per-symbol summary (`Rows`, `Lo`, `Hi`,
  `Sum`, `Squares`), or with the symbols whose counts differ from what the feed sent.
- **A hang** fails with "Timeout" and every thread's stack. Look for two library threads each
  waiting on a lock the other holds, or a thread waiting on the update graph while holding a
  library lock.
- **Growth** fails with the first round's figure and the run's. Rerun the one test with
  `--soak-seconds` long enough to see the trend, and compare the summary lines: a leak
  grows with the work done (`rows`, `streams_opened`, `results_opened`), not with time.
- **A JVM crash** leaves an `hs_err_pid*.log` file, and a fatal JVM error a
  `jvm_fatal_pid*.log` file, in the repository root; either is a bug to investigate.
