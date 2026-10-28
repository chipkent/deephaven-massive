"""Test-suite configuration: the embedded Deephaven JVM and the shared fixtures.

The embedded server starts in `pytest_configure`, before collection, so test modules can import
`deephaven` at module level, exactly as the library does. This module itself is imported
before `pytest_configure` runs, so it reaches `deephaven` only through `importlib` inside
fixtures, never through a module-level import.

Test modules never import from this file; they request its fixtures by name. Fakes from
`tests/_fakes/` are handed to tests through thin fixtures defined here.

The live integration tests (`tests/**/test_*_integration.py`) get their own fixtures here too:
the `integration` marker check, the key skip, live sessions and wire clients, raw flat-file
and REST readers, and the `evidence` recorder whose lines close the terminal summary.
"""

from __future__ import annotations

import atexit
import contextlib
import gzip
import importlib
import os
import sys
import threading
import time
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pyarrow as pa
import pytest
from deephaven_server import Server

from deephaven_massive._massive import (
    FlatFileClient,
    ReadColumn,
    ReadPlan,
    RestClient,
    Transport,
    object_key,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterator, Mapping

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive import MassiveConfig, MassiveSession
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.stream import FakeStreamTransport
    from tests._fakes.ws import FakeMassiveServer

_DATA_DIR = Path(__file__).parent / "data"

_CYCLE_MILLIS = 25
"""The update-graph cycle of the test JVM, in milliseconds (Deephaven's default is 1000)."""

_JVM_ARGS = [
    "-Xmx2g",
    # A short update-graph cycle keeps ticking-table tests fast. pytest_configure checks that
    # the JVM applied it: an unknown property is silently ignored.
    f"-DPeriodicUpdateGraph.targetCycleDurationMillis={_CYCLE_MILLIS}",
    # A JVM crash writes its hs_err log to the repository root, where CI collects it, instead
    # of the working directory or, if that is not writable, the temporary directory.
    f"-XX:ErrorFile={Path(__file__).resolve().parent.parent}/hs_err_pid%p.log",
]


# `trylast` matters: pytest's faulthandler plugin installs its SIGSEGV handler in its own
# `pytest_configure`. The JVM must install its signal handlers after that, chaining to the
# faulthandler, because the JVM raises SIGSEGV on purpose (implicit null checks, safepoints).
# Started first, the JVM's handlers would be replaced and the first such signal would kill
# the process.
@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    """Start the embedded Deephaven server once per process, before collection.

    Then check that the update graph runs the 25 ms cycle the tests are written for.

    Args:
        config: The pytest configuration (unused; required by the hook signature).

    Raises:
        pytest.UsageError: If the JVM did not apply the cycle property.
    """
    # Port 0 asks the OS for an ephemeral port, so two test runs can share a machine.
    Server(port=0, jvm_args=_JVM_ARGS).start()
    atexit.register(_final_jvm_exit)
    jpy = importlib.import_module("jpy")
    execution_context = importlib.import_module("deephaven.execution_context")
    periodic = jpy.get_type("io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph")
    j_update_graph = execution_context.get_exec_ctx().update_graph.j_update_graph
    cycle = int(jpy.cast(j_update_graph, periodic).getTargetCycleDurationMillis())
    if cycle != _CYCLE_MILLIS:
        raise pytest.UsageError(f"update-graph cycle is {cycle} ms, expected {_CYCLE_MILLIS} ms")


# --- JVM shutdown workaround -------------------------------------------------------------------
# Leaving the process normally after the embedded JVM has run is not safe, for two reasons:
#
# 1. On macOS with Python 3.11+, interpreter shutdown can segfault (exit code 139) while C++
#    static destructors in jpy and the JVM are finalized. The `atexit` handler below avoids
#    it: once pytest has returned, it flushes stdio and leaves through `os._exit` with
#    pytest's own exit status, skipping the finalizers. `atexit` handlers run last-in,
#    first-out, so this one, registered after the server's, runs first.
# 2. pytest's faulthandler plugin calls `faulthandler.disable()` in its `pytest_unconfigure`.
#    That restores the SIGSEGV, SIGBUS, SIGFPE, and SIGILL handlers faulthandler saved before
#    the JVM started, which are the defaults, and so removes the JVM's handlers. The JVM keeps
#    running (the update graph cycles every 25 ms), and its next deliberate SIGSEGV (an armed
#    safepoint poll or an implicit null check) would kill the process silently with exit code
#    139 (138, SIGBUS, on macOS) during the rest of pytest's teardown or before the `atexit`
#    handler runs. `pytest_sessionfinish` below unregisters the faulthandler plugin once the
#    session has finished, so its `pytest_unconfigure` never runs and the JVM's handlers stay
#    installed until `os._exit`. The rest of pytest's teardown runs as usual.
#
# Do not remove either part without reproducing both crashes.

_SESSIONS: list[pytest.Session] = []


def pytest_sessionstart(session: pytest.Session) -> None:
    """Remember the session so the exit handler can read its final exit status.

    Args:
        session: The pytest session that is starting.
    """
    _SESSIONS.append(session)


@pytest.hookimpl(wrapper=True, trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> Generator[None, object, object]:
    """Keep the JVM's signal handlers installed until the process exits.

    After every other `pytest_sessionfinish` implementation has run, or raised, this
    unregisters pytest's faulthandler plugin, so its `pytest_unconfigure` never calls
    `faulthandler.disable()`, which would remove the JVM's handlers. Faulthandler itself stays
    enabled, chained behind the JVM's handlers as it was during the tests. Unregistering in
    `pytest_unconfigure` would be too late: pytest calls the implementations it had when the
    hook started.

    Args:
        session: The pytest session that is finishing.

    Yields:
        Control to the other `pytest_sessionfinish` implementations.

    Returns:
        Their result, unchanged.
    """
    try:
        return (yield)
    finally:
        session.config.pluginmanager.unregister(name="faulthandler")


def _final_jvm_exit() -> None:
    """Flush stdio and exit with pytest's exit status, skipping JVM and jpy finalizers.

    The status is the session's, or `INTERNAL_ERROR` if no session started. If an exception
    escaped pytest (the interpreter has printed it and set `sys.last_value`), a status of 0
    becomes `INTERNAL_ERROR`, because `os._exit` replaces the interpreter's own exit code 1.
    """
    status = int(_SESSIONS[-1].exitstatus) if _SESSIONS else int(pytest.ExitCode.INTERNAL_ERROR)
    if status == 0 and getattr(sys, "last_value", None) is not None:
        status = int(pytest.ExitCode.INTERNAL_ERROR)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)


# --- End of JVM shutdown workaround ------------------------------------------------------------


@pytest.fixture(scope="session")
def dh_exec_ctx() -> ExecutionContext:
    """Return the embedded server's `ExecutionContext`.

    Returns:
        The execution context of the main thread's script session.
    """
    execution_context = importlib.import_module("deephaven.execution_context")
    ctx: ExecutionContext = execution_context.get_exec_ctx()
    return ctx


@pytest.fixture
def dh_to_pylist() -> Callable[[Table], list[dict[str, Any]]]:
    """Return a function that snapshots a Deephaven table into a list of row dictionaries.

    Returns:
        A function mapping a table to one `{column: value}` dictionary per row.
    """
    dh_arrow = importlib.import_module("deephaven.arrow")

    def to_pylist(table: Table) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = dh_arrow.to_arrow(table).to_pylist()
        return rows

    return to_pylist


_ROWS_WHEN_TIMEOUT = 15.0
"""Seconds `rows_when` waits for a live table's rows to match."""


@pytest.fixture
def rows_when(
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]:
    """Return a function that waits until a live table's rows satisfy a predicate.

    Each check reads one consistent snapshot of the table (`dh_to_pylist`) and hands the rows
    to the predicate, so the wait never decides on a size read in the middle of an update cycle
    and then reads rows from another state. The function returns the matching rows, and fails
    the test after 15 seconds with the last rows it saw.

    Returns:
        A function `(table, predicate) -> rows`.
    """

    def wait(
        table: Table, predicate: Callable[[list[dict[str, Any]]], bool]
    ) -> list[dict[str, Any]]:
        pause = threading.Event()  # never set; only paces the polling
        deadline = time.monotonic() + _ROWS_WHEN_TIMEOUT
        while True:
            rows = dh_to_pylist(table)
            if predicate(rows):
                return rows
            if time.monotonic() >= deadline:
                raise AssertionError(f"the table's rows never matched; last seen: {rows!r}")
            pause.wait(0.02)

    return wait


@pytest.fixture
def load_fixture() -> Callable[[str, str], bytes]:
    """Return a function that reads a payload fixture from `tests/data/<owner>/<name>`.

    `owner` is the mirrored test's path under `tests/` without the `test_` prefix and `.py`
    (`"datasets/symbols"` for `tests/datasets/test_symbols.py`), or a test package's path
    (`"massive/rest"`), so each owner's fixtures stay together.

    Returns:
        A function mapping `(owner, name)` to the fixture file's bytes.
    """

    def load(owner: str, name: str) -> bytes:
        return (_DATA_DIR / owner / name).read_bytes()

    return load


@pytest.fixture
def make_csv_gz() -> Callable[[str], bytes]:
    """Return a function that gzip-compresses CSV text, as a flat file is stored in S3.

    Returns:
        A function mapping CSV text to the bytes of a `.csv.gz` file.
    """

    def compress(csv_text: str) -> bytes:
        return gzip.compress(csv_text.encode())

    return compress


@pytest.fixture
def fake_ws_server() -> Iterator[FakeMassiveServer]:
    """Yield a fake Massive WebSocket server (`tests/_fakes/ws.py`), stopped at teardown.

    Yields:
        The running server; its `url()` is the endpoint to connect to.
    """
    ws = importlib.import_module("tests._fakes.ws")
    server: FakeMassiveServer = ws.FakeMassiveServer()
    yield server
    server.stop()


@pytest.fixture
def fake_rest() -> Iterator[FakeRestServer]:
    """Yield a started `FakeRestServer` (`tests/_fakes/rest.py`) on 127.0.0.1, closed at teardown.

    Yields:
        The server; script it with `reply` and read `requests` afterwards.
    """
    rest = importlib.import_module("tests._fakes.rest")
    server: FakeRestServer = rest.FakeRestServer()
    server.start()
    yield server
    server.close()


# --- Session fixtures --------------------------------------------------------------------------


@pytest.fixture
def massive_session() -> Iterator[MassiveSession]:
    """Yield a `MassiveSession` with a test key, closed at teardown.

    Yields:
        An open session on the embedded server's execution context.
    """
    dm = importlib.import_module("deephaven_massive")
    session: MassiveSession = dm.MassiveSession(dm.MassiveConfig(api_key="test-key"))
    yield session
    session.close()


@pytest.fixture(autouse=True)
def _fresh_stream_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Number each test's streams from 1, so tests can name them by id.

    Stream ids are process-unique in the library (a module-level counter in
    `_tables/core.py`); each test gets a fresh counter.
    """
    core = importlib.import_module("deephaven_massive._tables.core")
    itertools = importlib.import_module("itertools")
    monkeypatch.setattr(core, "_stream_numbers", itertools.count(1))


@pytest.fixture
def fake_streams(
    massive_session: MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> FakeStreamTransport:
    """Install a `FakeStreamTransport` (`tests/_fakes/stream.py`) as the stream factory hook.

    Returns:
        The fake, for inspecting opened streams and shutdown callbacks.
    """
    stream = importlib.import_module("tests._fakes.stream")
    fake: FakeStreamTransport = stream.FakeStreamTransport(massive_session._core)
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: fake)
    return fake


@pytest.fixture
def drain(massive_session: MassiveSession) -> Callable[[], None]:
    """Return a function that waits until the session's background work has caught up.

    It waits for every reaper job queued so far (result releases), then for every diagnostics
    row queued so far, so a test can read `m.streams()` deterministically.

    Returns:
        The waiting function; it fails the test after 10 seconds.
    """

    def wait() -> None:
        core = massive_session._core
        released = threading.Event()
        core._reaper.submit(released.set)
        assert released.wait(10.0)
        assert core.diagnostics.barrier().wait(30.0)

    return wait


@pytest.fixture
def stream_rows(
    massive_session: MassiveSession,
    drain: Callable[[], None],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[], list[dict[str, Any]]]:
    """Return a function that drains the session and reads `m.streams()`, sorted by stream.

    Returns:
        A function returning one dictionary per `m.streams()` row, ordered by `StreamId`.
    """

    def rows() -> list[dict[str, Any]]:
        drain()
        return sorted(dh_to_pylist(massive_session.streams()), key=lambda r: r["StreamId"])

    return rows


_AWS_IDENTITY_VARIABLES = (
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
"""The environment variables through which boto3 finds a profile or credentials of its own."""


@pytest.fixture
def no_aws_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Hide the machine's AWS settings from boto3 for the test, so every machine sees the same.

    The AWS configuration and credentials files are an empty file, the variables that name a
    profile or credentials are unset, and the instance metadata service is never asked. A test
    may set its own on top.

    Args:
        monkeypatch: Undoes the environment changes after the test.
        tmp_path: Holds the empty AWS configuration file.
    """
    empty = tmp_path / "aws-empty"
    empty.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    for name in _AWS_IDENTITY_VARIABLES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def fake_s3(monkeypatch: pytest.MonkeyPatch, no_aws_settings: None) -> FakeS3:
    """Answer every flat-file S3 client the library creates from a `FakeS3` (`tests/_fakes/s3.py`).

    The library's own client factory still runs (credentials, endpoint, signing configuration);
    the fake then answers the client's requests inside botocore, so nothing leaves the process.
    The machine's AWS settings are hidden for the test (`no_aws_settings`).

    Args:
        monkeypatch: Undoes the wrapping of boto3's client factory after the test.
        no_aws_settings: Hides the machine's AWS settings.

    Returns:
        The fake, for storing objects and inspecting requests.
    """
    s3 = importlib.import_module("tests._fakes.s3")
    fake: FakeS3 = s3.FakeS3()
    fake.install(monkeypatch)
    return fake


# --- Live integration tests --------------------------------------------------------------------
# Live tests live in `tests/**/test_*_integration.py` and carry the `integration` marker, which
# `addopts` deselects by default; `uv run pytest -m integration` runs them. Each one skips
# without `MASSIVE_API_KEY`, so CI, which has no key, never reaches Massive even when it selects
# them. What each test observed is recorded with the `evidence` fixture and printed in a
# "live evidence" section at the end of the run, for passed, failed, and skipped tests alike.

_INTEGRATION = "integration"
_INTEGRATION_FILE_SUFFIX = "_integration.py"

_EVIDENCE: Final = pytest.StashKey[list[tuple[str, str]]]()
"""The config stash key of the evidence lines recorded so far, as (test id, line) pairs."""

_EVIDENCE_LOCK = threading.Lock()
"""Guards the evidence list, so a line may be recorded from any thread."""

_FLAT_FILE_BLOCK = 1 << 20
"""Bytes of decompressed CSV per block when a live test reads a flat file."""


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Require the `integration` marker on exactly the tests of the live test files.

    Runs before the `-m` deselection, so the check covers every collected test, also those a
    default run deselects. A live test without the marker would run in CI, and a marked test
    elsewhere would never run by default.

    Args:
        config: The pytest configuration (unused; required by the hook signature).
        items: The collected tests.

    Raises:
        pytest.UsageError: If a test of a `*_integration.py` file lacks the marker, or a test
            of another file has it; the message lists them.
    """
    wrong = [
        item.nodeid
        for item in items
        if item.path.name.endswith(_INTEGRATION_FILE_SUFFIX)
        != (item.get_closest_marker(_INTEGRATION) is not None)
    ]
    if wrong:
        raise pytest.UsageError(
            "the integration marker must be on exactly the tests of *_integration.py files "
            "(pytestmark = pytest.mark.integration): " + ", ".join(wrong)
        )


@pytest.fixture(autouse=True)
def _live_tests_need_a_key(request: pytest.FixtureRequest) -> None:
    """Skip a live test when `MASSIVE_API_KEY` is not set, before the test's own fixtures run.

    Args:
        request: The requesting test, for its markers.
    """
    if request.node.get_closest_marker(_INTEGRATION) is not None:
        _required_env("MASSIVE_API_KEY")


def _required_env(name: str) -> str:
    """Return an environment variable's value, or skip the test when it is unset or blank.

    Args:
        name: The variable, such as `"MASSIVE_API_KEY"`.

    Returns:
        The value, stripped of surrounding whitespace.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.skip(f"{name} is not set")
    return value


@pytest.fixture
def live_env() -> Callable[[str], str]:
    """Return a function that reads a required environment variable or skips the test.

    Live tests that need a second key (a Business or a delayed one) or a named Basic
    subscription call it first.

    Returns:
        A function mapping a variable's name to its stripped value; it skips the test when
        the variable is unset or blank.
    """
    return _required_env


@pytest.fixture
def live_opt_in() -> Callable[[str], None]:
    """Return a function that skips the test unless an opt-in variable is set to `1`.

    The live tests that go beyond one symbol per class (`MASSIVE_LIVE_WIDE`) or one connection
    per cluster (`MASSIVE_LIVE_SECOND_CONNECTION`) run only when the owner opts in.

    Returns:
        A function taking the variable's name; it skips the test unless the value is `1`.
    """

    def require(name: str) -> None:
        if os.environ.get(name, "").strip() != "1":
            pytest.skip(f"{name} is not 1")

    return require


@pytest.fixture
def evidence(request: pytest.FixtureRequest) -> Callable[[str], None]:
    """Return a function that records one line of what a live test observed.

    The lines are printed, grouped by test, in the "live evidence" section of the terminal
    summary, whatever the test's outcome; a skipped test's lines show what was missing. Safe
    to call from any thread.

    Args:
        request: The requesting test, whose id heads its lines.

    Returns:
        A function taking one line of text.
    """
    records = request.config.stash.setdefault(_EVIDENCE, [])
    test_id = request.node.nodeid

    def record(line: str) -> None:
        with _EVIDENCE_LOCK:
            records.append((test_id, line))

    return record


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter, config: pytest.Config
) -> None:
    """Print the evidence the live tests recorded, grouped by test, after the test results.

    Args:
        terminalreporter: The terminal reporter that writes the summary.
        config: The pytest configuration, whose stash holds the evidence.
    """
    stored: list[tuple[str, str]] = config.stash.setdefault(_EVIDENCE, [])
    with _EVIDENCE_LOCK:
        records = list(stored)
    if not records:
        return
    terminalreporter.write_sep("=", "live evidence")
    current = None
    for test_id, line in records:
        if test_id != current:
            terminalreporter.write_line(test_id)
            current = test_id
        terminalreporter.write_line(f"    {line}")


@pytest.fixture
def live_config() -> MassiveConfig:
    """Return a `MassiveConfig` with `MASSIVE_API_KEY`, and the S3 credentials when set.

    Returns:
        The configuration; the test is skipped when `MASSIVE_API_KEY` is not set.
    """
    dm = importlib.import_module("deephaven_massive")
    config: MassiveConfig = dm.MassiveConfig(api_key=_required_env("MASSIVE_API_KEY"))
    return config


@pytest.fixture
def live_s3_config(live_config: MassiveConfig) -> MassiveConfig:
    """Return the live configuration, or skip the test when it has no S3 credentials.

    Args:
        live_config: The live configuration.

    Returns:
        `live_config`, whose `s3_access_key_id` and `s3_secret_access_key` are set.
    """
    if live_config.s3_access_key_id is None:
        pytest.skip("MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY are not set")
    return live_config


@pytest.fixture
def live_session(live_config: MassiveConfig) -> Iterator[MassiveSession]:
    """Yield a `MassiveSession` on the live key, closed at teardown.

    Closing the session closes its connections, so no stream outlives the test.

    Args:
        live_config: The live configuration.

    Yields:
        An open session on the embedded server's execution context.
    """
    dm = importlib.import_module("deephaven_massive")
    session: MassiveSession = dm.MassiveSession(live_config)
    yield session
    session.close()


@pytest.fixture
def live_rest(live_config: MassiveConfig) -> Iterator[RestClient]:
    """Yield the wire REST client on the live key, for raw responses; closed at teardown.

    Live tests read raw rows through it where the evidence is the wire form itself (a stamp
    before the library moves it, a key the library renames).

    Args:
        live_config: The live configuration, for the key and the base URL.

    Yields:
        A `RestClient` with one connection.
    """
    client = RestClient(live_config.api_key, base_url=live_config.rest_base_url, max_concurrency=1)
    yield client
    client.close()


@pytest.fixture
def live_flat_files(live_s3_config: MassiveConfig) -> Iterator[FlatFileClient]:
    """Yield the wire flat-file client on the live S3 credentials; closed at teardown.

    Args:
        live_s3_config: The live configuration with S3 credentials.

    Yields:
        A `FlatFileClient` with one connection.
    """
    assert live_s3_config.s3_access_key_id is not None
    assert live_s3_config.s3_secret_access_key is not None
    client = FlatFileClient(
        access_key_id=live_s3_config.s3_access_key_id,
        secret_access_key=live_s3_config.s3_secret_access_key,
        endpoint_url=live_s3_config.s3_endpoint_url,
        bucket=live_s3_config.s3_bucket,
        max_pool=1,
    )
    yield client
    client.close()


FlatFileRows = tuple[list[dict[str, str | None]], frozenset[str]]
"""The raw rows of a flat file, every value as its CSV text, and its undocumented headers."""


def _read_flat_file(
    client: FlatFileClient,
    key: str,
    prefix: str,
    *,
    max_rows: int | None,
    tickers: list[str] | None,
) -> FlatFileRows | None:
    """Read the raw rows of one flat file: every documented header as its CSV text.

    Nothing is converted (no unit, no type) and futures rows are not checked against their
    session, so the rows show the wire form.

    Args:
        client: The live flat-file client.
        key: The object's key.
        prefix: The generated prefix whose documented headers are read, such as
            `"us_futures_{venue}/session_aggs_v1"`.
        max_rows: The most rows to keep; reading stops after the block that reaches it. None
            reads the whole file.
        tickers: The `ticker` values to keep, as a sorted list, or None for every row.

    Returns:
        The rows and the headers the file has beyond the documented ones, or None when the
        file does not exist.
    """
    headers = [field.wire_key for field in source(Transport.FLAT_FILES, prefix).fields]
    plan = ReadPlan(
        columns=tuple(ReadColumn(header, header, pa.string()) for header in headers),
        symbol_header="ticker",
        symbols=None if tickers is None else pa.array(tickers, pa.string()),
        time_header=headers[0],
        window_ns=None,
        to_ns=lambda values: values,
        session=None,
        block_size=_FLAT_FILE_BLOCK,
        required=frozenset(),
    )
    file = client.open(key)
    if file is None:
        return None
    rows: list[dict[str, str | None]] = []
    extra: frozenset[str] = frozenset()
    with file, contextlib.closing(file.batches(plan)) as batches:
        for batch, drift in batches:
            extra = drift.extra
            rows.extend(batch.to_pylist())
            if max_rows is not None and len(rows) >= max_rows:
                break
    return (rows if max_rows is None else rows[:max_rows]), extra


_HEAD_ROWS = 1000
"""How many rows `flat_file_head` keeps from the start of a file."""


@pytest.fixture
def flat_file_head(
    live_flat_files: FlatFileClient,
) -> Callable[[str, date, str | None], FlatFileRows | None]:
    """Return a function that reads the first rows of one flat file, as raw text.

    Only the first block is downloaded and decoded, so even a quotes file costs little.

    Args:
        live_flat_files: The live flat-file client.

    Returns:
        A function `(prefix, day, venue)`: the generated prefix (such as
        `"global_forex/quotes_v1"`), the file's day, and the venue that fills a futures
        prefix (`"cme"`; None for every other prefix). It returns the first 1,000 rows, every
        value as the file's text, and the file's undocumented headers, or None when the file
        does not exist.
    """

    def read(prefix: str, day: date, venue: str | None) -> FlatFileRows | None:
        key = object_key(prefix, day, venue)
        return _read_flat_file(live_flat_files, key, prefix, max_rows=_HEAD_ROWS, tickers=None)

    return read


@pytest.fixture
def flat_file_tickers(
    live_flat_files: FlatFileClient,
) -> Callable[[str, date, Collection[str] | None], FlatFileRows | None]:
    """Return a function that reads every row of some tickers from one flat file, as raw text.

    Args:
        live_flat_files: The live flat-file client.

    Returns:
        A function `(prefix, day, tickers)`: the generated prefix of a file without a venue
        (such as `"global_crypto/minute_aggs_v1"`), the file's day, and the `ticker` values to
        keep, or None for every row. It returns the rows, every value as the file's text, and
        the file's undocumented headers, or None when the file does not exist.
    """

    def read(prefix: str, day: date, tickers: Collection[str] | None) -> FlatFileRows | None:
        wanted = None if tickers is None else sorted(tickers)
        key = object_key(prefix, day)
        return _read_flat_file(live_flat_files, key, prefix, max_rows=None, tickers=wanted)

    return read


@pytest.fixture
def table_rows() -> Callable[[Table], list[dict[str, Any]]]:
    """Return a function that snapshots a table into row dictionaries, instants as epoch ns.

    Unlike `dh_to_pylist`, every timestamp column becomes an `int` of nanoseconds since the
    epoch, so evidence shows exact values.

    Returns:
        A function mapping a table to one `{column: value}` dictionary per row.
    """
    dh_arrow = importlib.import_module("deephaven.arrow")

    def read(table: Table) -> list[dict[str, Any]]:
        data = dh_arrow.to_arrow(table)
        columns = [
            column.cast(pa.int64()) if pa.types.is_timestamp(column.type) else column
            for column in data.columns
        ]
        rows: list[dict[str, Any]] = pa.table(columns, names=data.column_names).to_pylist()
        return rows

    return read


@pytest.fixture
def stream_report(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[MassiveSession], list[str]]:
    """Return a function that describes a session's connections and streams, one line each.

    Returns:
        A function mapping a session to lines such as `connection stocks/TICK
        socket.massive.com CONNECTED acked=1 ...` and `stream 1 stocks.trades RUNNING ...`.
    """

    def report(session: MassiveSession) -> list[str]:
        lines = [
            f"connection {r['Transport']} {r['AssetClass']}/{r['Family']} host={r['Host']} "
            f"recency={r['Recency']} evidence={r['Evidence']!r} state={r['State']} "
            f"topics={r['Topics']} acked={r['AckedTopics']} pending={r['PendingTopics']} "
            f"error={r['LastError']!r} drift={r['SchemaDrift']!r}"
            for r in table_rows(session.connections())
        ]
        lines.extend(
            f"stream {r['StreamId']} {r['Dataset']} host={r['Host']} state={r['State']} "
            f"rows={r['RowsPublished']} error={r['Error']!r} drift={r['SchemaDrift']!r}"
            for r in table_rows(session.streams())
        )
        return lines

    return report


@pytest.fixture
def wait_rows(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[MassiveSession, Table, float], list[dict[str, Any]]]:
    """Return a function that waits for a live table's first rows.

    Returns:
        A function `(session, table, timeout)`: it returns the table's rows once it holds any,
        once any of the session's streams has `FAILED`, or once `timeout` seconds have passed,
        whichever comes first, so the caller records what happened (a closed market sends
        nothing; a refusal fails the stream).
    """

    def wait(session: MassiveSession, table: Table, timeout: float) -> list[dict[str, Any]]:
        pause = threading.Event()  # never set; only paces the polling
        deadline = time.monotonic() + timeout
        while True:
            rows = table_rows(table)
            if rows or time.monotonic() >= deadline:
                return rows
            if any(r["State"] == "FAILED" for r in table_rows(session.streams())):
                return rows
            pause.wait(0.25)

    return wait


def _instant_text(nanos: int) -> str:
    """Write epoch nanoseconds as a UTC instant and its weekday.

    Args:
        nanos: Nanoseconds since the Unix epoch.

    Returns:
        Text such as `"2025-01-02T00:00:00.000000000Z Thu"`.
    """
    seconds, fraction = divmod(nanos, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{fraction:09d}Z {moment:%a}"


@pytest.fixture
def instant_text() -> Callable[[int], str]:
    """Return a function that writes epoch nanoseconds as a UTC instant and its weekday.

    Returns:
        A function mapping `1735776000000000000` to `"2025-01-02T00:00:00.000000000Z Thu"`.
    """
    return _instant_text


@pytest.fixture
def rest_rows(
    live_rest: RestClient,
) -> Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]:
    """Return a function that reads every raw row of one REST request, following its pages.

    Args:
        live_rest: The live REST client.

    Returns:
        A function `(path, path_params, query_params)`: the generated path template (such as
        `"/v2/aggs/ticker/{cryptoTicker}/prev"`), its placeholder values, and its query
        values, by wire name. It returns the rows as Massive sent them (wire keys, epoch
        integers, nested objects). A failed request raises the wire error.
    """

    def read(
        path: str, path_params: Mapping[str, object], query_params: Mapping[str, object]
    ) -> list[dict[str, Any]]:
        rest_path = source(Transport.REST, path)
        return [
            row
            for page in live_rest.pages(rest_path, path_params, query_params)
            for row in page.rows
        ]

    return read


_MS = 1_000_000
"""Nanoseconds per millisecond."""

_WEEK_MS = 7 * 86_400_000
"""Milliseconds per week."""

_WEEK_RANGE = (date(2025, 2, 24), date(2025, 3, 23))
"""The four calendar weeks, Monday to Sunday, whose week bars the week-start checks read."""


@pytest.fixture
def week_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
) -> Callable[[str, str], list[str]]:
    """Return a function that shows where Massive starts a week bar, from raw REST bars.

    It reads the week bars and the hour bars of one ticker over four calendar weeks
    (2025-02-24 to 2025-03-23) from the aggregates range endpoint, unfiltered. For each week
    bar it reports its stamp, its volume, the first and last hour bar inside the seven days
    from its stamp, and the hour-bar volume of three candidate weeks: from Monday 00:00 UTC,
    from the Sunday before at 00:00 UTC, and from the stamp itself. The candidate whose volume
    equals the week bar's is the bucket Massive uses.

    Args:
        rest_rows: Reads raw REST rows.

    Returns:
        A function `(ticker_param, ticker)`: the path placeholder of the class (such as
        `"stocksTicker"`) and the ticker. It returns the evidence lines.
    """

    def report(ticker_param: str, ticker: str) -> list[str]:
        path = (
            f"/v2/aggs/ticker/{{{ticker_param}}}/range/{{multiplier}}/{{timespan}}/{{from}}/{{to}}"
        )
        first, last = _WEEK_RANGE

        def bars(span: str) -> list[dict[str, Any]]:
            params = {ticker_param: ticker, "multiplier": 1, "timespan": span}
            params |= {"from": first, "to": last}
            return rest_rows(path, params, {"sort": "asc", "limit": 50_000})

        weeks, hours = bars("week"), bars("hour")

        def volume(start_ms: int) -> float:
            return float(sum(h["v"] for h in hours if start_ms <= h["t"] < start_ms + _WEEK_MS))

        lines = [f"{ticker}: {len(weeks)} week bars, {len(hours)} hour bars"]
        for week in weeks:
            stamp = int(week["t"])
            inside = [h for h in hours if stamp <= h["t"] < stamp + _WEEK_MS]
            day = datetime.fromtimestamp(stamp / 1000, UTC).date()
            monday = datetime.combine(day - timedelta(days=day.weekday()), dt_time(), UTC)
            monday_ms = int(monday.timestamp()) * 1000
            lines.append(
                f"week t={_instant_text(stamp * _MS)} v={week['v']} "
                f"first_hour={_instant_text(inside[0]['t'] * _MS) if inside else None} "
                f"last_hour={_instant_text(inside[-1]['t'] * _MS) if inside else None} "
                f"hour_volume_from_monday_utc={volume(monday_ms)} "
                f"from_sunday_utc={volume(monday_ms - 86_400_000)} from_stamp={volume(stamp)}"
            )
        return lines

    return report
