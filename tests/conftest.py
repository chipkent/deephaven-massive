"""Test-suite configuration: the embedded Deephaven JVM and the shared fixtures.

The embedded server starts in `pytest_configure`, before collection, so test modules can import
`deephaven` at module level, exactly as the library does. This module itself is imported
before `pytest_configure` runs, so it reaches `deephaven` only through `importlib` inside
fixtures, never through a module-level import.

Test modules never import from this file; they request its fixtures by name. Fakes from
`tests/_fakes/` are handed to tests through thin fixtures defined here.
"""

from __future__ import annotations

import atexit
import gzip
import importlib
import os
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from deephaven_server import Server

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive import MassiveSession
    from tests._fakes.clock import FakeClock
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


@pytest.fixture(autouse=True)
def rest_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Give each test a registry of REST rate governors of its own, on a fake clock.

    Sessions share one governor per API key through a process-wide registry, so without this
    one test's pauses, learned limits, and held slots would carry over into the next. Every
    governor of the test, and so every session's REST client, waits on the returned clock:
    automatic by default (a wait returns at once and moves virtual time), so no rate-limit or
    retry wait ever sleeps. A test that needs the manual mode replaces the registry itself.

    Returns:
        The clock.
    """
    clock_module = importlib.import_module("tests._fakes.clock")
    functools = importlib.import_module("functools")
    rate_limits = importlib.import_module("deephaven_massive._ingest.rate_limits")
    wire = importlib.import_module("deephaven_massive._massive")
    clock: FakeClock = clock_module.FakeClock()
    create = functools.partial(wire.RequestGovernor, rate_limits.BUCKETS, clock=clock)
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry(create))
    return clock


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
