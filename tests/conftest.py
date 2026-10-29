"""Test-suite configuration: the embedded Deephaven JVM and the shared fixtures.

The embedded server starts in `pytest_configure`, before collection, so test modules can import
`deephaven` at module level, exactly as the library does. This module itself is imported
before `pytest_configure` runs, so it reaches `deephaven` only through `importlib` inside
fixtures, never through a module-level import.

Test modules never import from this file; they request its fixtures by name. Fakes from
`tests/_fakes/` are handed to tests through thin fixtures defined here.

The live integration tests (`tests/**/test_*_integration.py`) get their own fixtures here too:
the marker checks, the key skip, live sessions and wire clients, raw flat-file and REST
readers, and the `evidence` recorder, which writes `live-evidence.log` as it goes and closes
the terminal summary.
"""

from __future__ import annotations

import atexit
import contextlib
import faulthandler
import gc
import gzip
import importlib
import os
import sys
import threading
import time
from collections import Counter, deque
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, cast
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from deephaven_server import Server

from deephaven_massive._errors import EntitlementError
from deephaven_massive._massive import (
    FlatFileClient,
    ReadColumn,
    ReadPlan,
    RestClient,
    Transport,
    WireEntitlementError,
    object_key,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterator, Mapping

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive import MassiveConfig, MassiveSession
    from deephaven_massive._massive import RestPath
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.stream import FakeStreamTransport
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

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


_LIBRARY_THREADS: Final = ("MassiveSession #", "massive-ws ", "massive-flush ", "massive-move ")
"""The name prefixes of the library's threads: a session's reaper, diagnostics writer, and
pollers, and a connection's client, flush, and verdict-move threads."""


@pytest.fixture(autouse=True)
def _check_nothing_outlives_the_test(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test that leaves a session running or alive, or a library thread alive.

    A session is in the live set exactly while it has a running stream or poll or a held
    diagnostics view, so a test that ends with one has leaked a table, or the library has. A
    session the test created that is still alive at the end, in the live set or not, is held
    by something that should have let it go.
    Autouse, and named to sort first (pytest orders autouse fixtures by name), so it is set up
    first and runs after every other fixture's teardown (a closed `massive_session` included).
    When something is left, it collects garbage and waits, event-driven and bounded, for the
    library threads to end and the background work to land, and fails naming what is still
    left. Only what the test itself
    started counts, so one leak fails its own test and not every test after it.
    """
    before = _running()
    yield
    # Every other fixture is torn down, but pytest keeps their values in the item until its
    # teardown ends; drop them, as pytest is about to, so a closed session can be freed.
    funcargs: dict[str, object] | None = getattr(request.node, "funcargs", None)
    if funcargs:
        funcargs.clear()
    deadline = time.monotonic() + 30.0
    pause = threading.Event()  # never set; only paces the bounded wait
    collect = False  # most tests leave nothing: a full collection per test would be slow
    while True:
        _drop_logged_objects(request.node)  # a library thread may still be logging
        if collect:
            gc.collect()
        collect = True
        sessions, alive, groups, threads = _running()
        sessions, alive = sessions - before[0], alive - before[1]
        groups, threads = groups - before[2], threads - before[3]
        if not sessions and not alive and not groups and not threads:
            return
        if time.monotonic() > deadline:
            pytest.fail(
                f"left running after the test: sessions {sorted(sessions)}, alive sessions "
                f"{sorted(alive)}, {len(groups)} connection groups, threads "
                f"{sorted(t.name for t in threads)}"
            )
        for thread in threads:  # wait on the threads themselves, briefly each
            thread.join(0.05)
        if not threads:
            pause.wait(0.05)


def _drop_logged_objects(item: pytest.Item) -> None:
    """Keep only the text of the log records pytest keeps with a test item.

    The records hold a logged exception (in the arguments and the exception info), and through
    its traceback the test's frames, for the whole run; the report has their text already.

    Args:
        item: The test item.
    """
    records_key = importlib.import_module("_pytest.logging").caplog_records_key
    for records in item.stash.get(records_key, {}).values():
        for record in records:
            record.msg, record.args, record.exc_info = record.getMessage(), None, None


def _running() -> tuple[set[str], set[str], set[object], set[threading.Thread]]:
    """Return what of the library runs now: sessions, connection groups, and threads.

    Returns:
        The labels of the sessions in the live set, the labels of every session not yet
        freed, the connection groups, and the alive library threads (`_LIBRARY_THREADS`).
    """
    core = importlib.import_module("deephaven_massive._tables.core")
    session = importlib.import_module("deephaven_massive._session")
    ws = importlib.import_module("deephaven_massive._ingest.ws")
    with core._live_lock:
        live: set[str] = {each.label for each in core._live.values()}
    with session._keys_lock:
        cores = [ref() for ref in session._open_sessions]
    alive = {each.label for each in cores if each is not None}
    groups: set[object] = set(ws._groups.values())
    threads = {
        t for t in threading.enumerate() if t.is_alive() and t.name.startswith(_LIBRARY_THREADS)
    }
    return live, alive, groups, threads


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
def make_ws_session(fake_ws_server: FakeMassiveServer) -> Iterator[Callable[..., WsSession]]:
    """Return a factory of `WsSession`s (`tests/_fakes/ws_session.py`) on the fake server.

    Each session's streams connect, through real WebSocket clients, to the `fake_ws_server`
    fixture's server. The factory takes `WsSession`'s keyword options (`decide`,
    `flush_interval`, `backoff`, and further `MassiveConfig` options); every session it made is
    closed at teardown, so a test may also close one itself.

    Teardown closes every session even when one close raises, then waits, for at most
    `_CLIENT_THREADS_TIMEOUT` seconds, until no client loop thread (`massive-ws …`) or flush
    thread (`massive-flush …`) is alive, and fails the test if one is: a client left running
    reconnects forever, to a stopped server or through whatever proxy a later test sets.

    Yields:
        The factory.
    """
    module = importlib.import_module("tests._fakes.ws_session")
    with contextlib.ExitStack() as sessions:

        def make(**options: object) -> WsSession:
            session: WsSession = module.WsSession(fake_ws_server, **options)
            sessions.callback(session.close)
            return session

        yield make
    deadline = time.monotonic() + _CLIENT_THREADS_TIMEOUT
    pause = threading.Event()  # never set; only paces the polling
    while (left := _client_threads()) and time.monotonic() < deadline:
        pause.wait(0.02)
    assert not left, f"threads outlived their session: {left}"


_CLIENT_THREADS_TIMEOUT: Final = 15.0
"""Seconds `make_ws_session`'s teardown waits for its sessions' threads to end."""


def _client_threads() -> list[str]:
    """Return the names of the live WebSocket client and flush threads, in any session."""
    return [
        t.name
        for t in threading.enumerate()
        if t.name.startswith(("massive-ws ", "massive-flush ")) and t.is_alive()
    ]


@pytest.fixture
def ws_session(make_ws_session: Callable[..., WsSession]) -> WsSession:
    """Return a `WsSession` with the default options: a fixed real-time host, manual flushes.

    Returns:
        The wrapper; its `session` is the `MassiveSession` to build queries from.
    """
    return make_ws_session()


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
# `addopts` deselects by default; `uv run pytest -m integration` runs them. Those that
# deliberately exceed Massive's limits also carry `integration_limits` and run only when `-m`
# names it. Each live test skips without `MASSIVE_API_KEY`, and always under CI. What a test
# observes is recorded with the `evidence` fixture: appended at once to `live-evidence.log` in
# the repository root, flushed line by line, so a run cut short keeps everything recorded
# before, and printed again in a "live evidence" section at the end of the run.
# `docs/live-tests.md` explains how to run them.

_INTEGRATION = "integration"
_LIMITS = "integration_limits"
_SMOKE = "integration_smoke"
_SEPARATE = (_LIMITS, _SMOKE)
"""The live tiers that `-m integration` leaves out: each runs only when `-m` names it."""
_INTEGRATION_FILE_SUFFIX = "_integration.py"

_EVIDENCE: Final = pytest.StashKey[list[tuple[str, str]]]()
"""The config stash key of the evidence lines recorded so far, as (test id, line) pairs."""

_EVIDENCE_FILE = "live-evidence.log"
"""The file, in the repository root, every evidence line is appended to as it is recorded."""

_EVIDENCE_LOCK = threading.Lock()
"""Guards the evidence list and file, so a line may be recorded from any thread."""

_PLANS_VARIABLE = "MASSIVE_LIVE_PLANS"
"""The key's plans, as `stocks=starter,currencies=starter,s3`; unnamed classes are Basic."""

_TIERS: Final = {
    "stocks": ("basic", "starter", "developer", "advanced", "business"),
    "options": ("basic", "starter", "developer", "advanced", "business"),
    "indices": ("basic", "starter", "advanced", "business"),
    "currencies": ("basic", "starter", "business"),
    "futures": ("basic", "starter", "developer", "advanced", "business"),
}
"""Each class's plans, cheapest first (https://massive.com/pricing)."""

_PRICES: Final = {
    ("stocks", "starter"): "$29",
    ("stocks", "developer"): "$79",
    ("stocks", "advanced"): "$199",
    ("stocks", "business"): "$2,499",
    ("options", "starter"): "$29",
    ("options", "developer"): "$79",
    ("options", "advanced"): "$199",
    ("options", "business"): "$1,999",
    ("indices", "starter"): "$49",
    ("indices", "advanced"): "$99",
    ("indices", "business"): "$2,500",
    ("currencies", "starter"): "$49",
    ("currencies", "business"): "$999",
    ("futures", "starter"): "$29",
    ("futures", "developer"): "$79",
    ("futures", "advanced"): "$199",
    ("futures", "business"): "$999 a venue",
}
"""The monthly price of each paid plan (https://massive.com/pricing, https://massive.com/business)."""

_S3 = "s3"
_SOME_BASIC = "some:basic"
"""A requirement met when at least one class is declared Basic: the rate-limit tests need one."""

_BASIC_RATE = 5
"""REST requests a minute Massive allows each class on Basic; paid plans have no limit."""

_PACE_LOCK = threading.Lock()
_PACE_CALLS: dict[str, deque[float]] = {}
"""When each Basic class's recent REST requests started (`time.monotonic()`), run-wide."""

_REQUEST_SECONDS = 13.0
"""Seconds of timeout a live test gets for each REST request it makes to a Basic class."""

_DEFAULT_REQUESTS = 10
"""The REST requests a live test is assumed to make when it declares none (`requests`)."""

_SETTLED: Final = pytest.StashKey[dict[str, tuple[tuple[str, ...], tuple[str, ...]]]]()
"""Each live test's `settles` items and `needs` requirements, by test id, for the summary."""


def _declared_plans() -> tuple[dict[str, str], bool]:
    """Read `MASSIVE_LIVE_PLANS`: each class's declared plan, and whether S3 is declared.

    Returns:
        The plan of every class (Basic unless named) and the S3 flag.

    Raises:
        pytest.UsageError: If an entry names an unknown class or plan.
    """
    plans = dict.fromkeys(_TIERS, "basic")
    s3 = False
    for entry in os.environ.get(_PLANS_VARIABLE, "").lower().replace(" ", "").split(","):
        if not entry:
            continue
        if entry == _S3:
            s3 = True
            continue
        name, _, tier = entry.partition("=")
        if tier not in _TIERS.get(name, ()):
            raise pytest.UsageError(
                f"{_PLANS_VARIABLE}: {entry!r} is not <class>=<plan> or s3; classes and plans: "
                + "; ".join(f"{c}: {', '.join(t)}" for c, t in _TIERS.items())
            )
        plans[name] = tier
    return plans, s3


def _plan_text(requirement: str) -> str:
    """Name a requirement for a person: its plan and monthly price.

    Args:
        requirement: `"<class>:<plan>"`, `"s3"`, or `"some:basic"`.

    Returns:
        Such as `"Currencies Starter ($49/month)"`.
    """
    if requirement == _S3:
        return "S3 flat files (MASSIVE_S3_* credentials, on the class's Starter plan)"
    if requirement == _SOME_BASIC:
        return "at least one class left on Basic"
    name, _, tier = requirement.partition(":")
    price = "free" if tier == "basic" else f"{_PRICES[name, tier]}/month"
    return f"{name.capitalize()} {tier.capitalize()} ({price})"


def _unmet(requirements: tuple[str, ...]) -> list[str]:
    """Return the requirements the declared plans do not meet.

    Args:
        requirements: `"<class>:<plan>"`, `"s3"`, or `"some:basic"` each.

    Returns:
        The unmet ones, in order.
    """
    plans, s3 = _declared_plans()
    unmet = []
    for requirement in requirements:
        if requirement == _S3:
            met = s3
        elif requirement == _SOME_BASIC:
            met = "basic" in plans.values()
        else:
            name, _, tier = requirement.partition(":")
            tiers = _TIERS[name]
            met = tiers.index(plans[name]) >= tiers.index(tier)
        if not met:
            unmet.append(requirement)
    return unmet


def _requirements(item: pytest.Item) -> tuple[str, ...]:
    """Return every requirement a test's `needs` markers declare.

    Args:
        item: The test.

    Returns:
        The requirements, in marker order.
    """
    return tuple(str(arg) for marker in item.iter_markers("needs") for arg in marker.args)


def _basic_classes(item: pytest.Item) -> list[str]:
    """Return the classes a test reads that are declared Basic, whose requests are paced.

    Args:
        item: The test.

    Returns:
        The classes.
    """
    plans, _ = _declared_plans()
    named = {r.partition(":")[0] for r in _requirements(item) if ":" in r}
    return sorted(name for name in named if plans.get(name) == "basic")


def _request_class(template: str, target: str) -> str | None:
    """Return the class whose REST request limit a request counts against.

    A path that names a ticker counts against the ticker's class, by its prefix (`X:` and
    `C:` currencies, `I:` indices, `O:` options; the futures paths futures; any other stocks).
    A path without one is placed by its words. Reference data, economy data, market status,
    partner data, and the unified snapshot belong to no class.

    Args:
        template: The generated path template, such as `"/v2/aggs/ticker/{stocksTicker}/prev"`.
        target: The request's path and query.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or None.
    """
    if template.startswith("/futures/"):
        return "futures"
    parts = template.split("/")
    sent = target.split("?", maxsplit=1)[0].split("/")
    for index, part in enumerate(parts):
        if part.startswith("{") and "icker" in part and index < len(sent):
            ticker = unquote(sent[index])
            prefixes = {"X:": "currencies", "C:": "currencies", "I:": "indices", "O:": "options"}
            return prefixes.get(ticker[:2], "stocks")
    words = (
        ("/options", "options"),
        ("/indices", "indices"),
        ("/crypto", "currencies"),
        ("/forex", "currencies"),
        ("/fx/", "currencies"),
        ("currencies", "currencies"),
        ("/conversion/", "currencies"),
        ("/stocks", "stocks"),
        ("/v2/last/nbbo", "stocks"),
    )
    return next((name for word, name in words if word in template), None)


def _pace(name: str) -> None:
    """Wait until a request to a Basic class fits the class's 5 a minute, then count it.

    The budget is shared by every request of the run (the library's own and the tests' raw
    ones), so a run on a free key never draws a refusal of its own making.

    Args:
        name: The class.
    """
    pause = threading.Event()  # never set; only waits
    while True:
        with _PACE_LOCK:
            calls = _PACE_CALLS.setdefault(name, deque())
            now = time.monotonic()
            while calls and now - calls[0] >= 60.0:
                calls.popleft()
            if len(calls) < _BASIC_RATE:
                calls.append(now)
                return
            wait = 60.0 - (now - calls[0])
        pause.wait(wait)


class _Targeted(Protocol):
    """A REST request as the wire client hands it to one attempt: its path and query."""

    target: str


@pytest.fixture(autouse=True)
def _pace_basic_classes(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pace every REST request of a live test to a class declared Basic.

    Each attempt of the wire REST client (the library's requests and the tests' raw ones)
    waits for a place in its class's budget of 5 a minute (`_pace`); a request of a paid class,
    or of no class, is never delayed. The library's own pacing replaces this once it lands.

    Args:
        request: The requesting test, for its markers.
        monkeypatch: Undoes the wrapping after the test.
    """
    if request.node.get_closest_marker(_INTEGRATION) is None:
        return
    plans, _ = _declared_plans()
    attempt = RestClient._attempt

    def paced(self: RestClient, rest_path: RestPath, sent: _Targeted) -> object:
        name = _request_class(rest_path.key, sent.target)
        if name is not None and plans.get(name) == "basic":
            _pace(name)
        return attempt(self, rest_path, cast("Any", sent))

    monkeypatch.setattr(RestClient, "_attempt", paced)


_FLAT_FILE_BLOCK = 1 << 20
"""Bytes of decompressed CSV per block when a live test reads a flat file."""


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Check the live markers, and keep the limit tests out of every run that does not name them.

    Runs first (`tryfirst`), before the `-m` deselection, so the checks cover every collected
    test, also those a default run deselects; run later, a marked unit test would already be
    deselected and escape the check. A live test without the `integration` marker would run in
    CI, and a marked test elsewhere would never run by default. Two tiers of live tests run
    apart: `integration_limits` (tests that deliberately exceed Massive's subscription and
    connection limits) and `integration_smoke` (the unattended check that every dataset and
    verb works). A test of either must also be a live test, and it is deselected unless the
    `-m` expression names its tier: `-m integration` runs every live test but those, and
    `-m integration_limits` or `-m integration_smoke` runs only that tier.

    Args:
        config: The pytest configuration, for the `-m` expression and the deselection hook.
        items: The collected tests; the limit tests are removed from it unless selected.

    Raises:
        pytest.UsageError: If a test of a `*_integration.py` file lacks the `integration`
            marker, a test of another file has it, or a test has `integration_limits` or
            `integration_smoke` without `integration`; the message lists them.
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
    unpaired = [
        item.nodeid
        for item in items
        if any(item.get_closest_marker(tier) is not None for tier in _SEPARATE)
        and item.get_closest_marker(_INTEGRATION) is None
    ]
    if unpaired:
        raise pytest.UsageError(
            "a test marked integration_limits or integration_smoke must also be marked "
            "integration: " + ", ".join(unpaired)
        )
    expression = config.option.markexpr or ""
    for tier in _SEPARATE:
        if tier in expression:
            continue
        tiered = [item for item in items if item.get_closest_marker(tier) is not None]
        if tiered:
            config.hook.pytest_deselected(items=tiered)
            items[:] = [item for item in items if item.get_closest_marker(tier) is None]
    _arrange_live_tests(config, items)


def _arrange_live_tests(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Order, time, and register the live tests for the run's summary.

    The tests that need a class left on Basic (the rate-limit checks) run first, while their
    class's minute is unspent. A test reading a class declared Basic gets a timeout scaled to
    its REST requests (its `requests` marker, else 10), since each waits for a place in the
    class's 5 a minute. Each test's `settles` items and `needs` are kept for the summary.

    Args:
        config: The pytest configuration, whose stash keeps the items.
        items: The selected tests; reordered in place.
    """
    live = [item for item in items if item.get_closest_marker(_INTEGRATION) is not None]
    if not live:
        return
    items.sort(key=lambda item: _SOME_BASIC not in _requirements(item))
    settled = config.stash.setdefault(_SETTLED, {})
    for item in live:
        settles = tuple(str(a) for m in item.iter_markers("settles") for a in m.args)
        settled[item.nodeid] = (settles, _requirements(item))
        if not _basic_classes(item):
            continue
        marker = item.get_closest_marker("requests")
        count = int(marker.args[0]) if marker is not None else _DEFAULT_REQUESTS
        current = item.get_closest_marker("timeout")
        base = float(current.args[0]) if current is not None and current.args else 100.0
        item.add_marker(pytest.mark.timeout(base + _REQUEST_SECONDS * count), append=False)


@pytest.fixture(autouse=True)
def _live_tests_need_a_key(request: pytest.FixtureRequest) -> None:
    """Skip a live test under CI, without `MASSIVE_API_KEY`, or when the plans fall short.

    It runs before the test's own fixtures, so a skipped live test touches nothing. `CI` is
    checked too, so a key added to CI's secrets later still never reaches Massive from CI. A
    test's `needs` markers name the plans it needs; one the declared plans
    (`MASSIVE_LIVE_PLANS`) do not meet skips, naming the plan and its monthly price.

    Args:
        request: The requesting test, for its markers.
    """
    if request.node.get_closest_marker(_INTEGRATION) is None:
        return
    if os.environ.get("CI", "").strip():
        pytest.skip("live tests never run in CI")
    _required_env("MASSIVE_API_KEY")
    unmet = _unmet(_requirements(request.node))
    if unmet:
        pytest.skip("needs " + "; ".join(_plan_text(r) for r in unmet))


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


_PLANS: Final = {
    "test_crypto_integration.py": "a Currencies plan that includes this data",
    "test_forex_integration.py": "a Currencies plan that includes this data",
    "test_timestamps_integration.py": "a Currencies plan that includes this data",
    "test_stocks_integration.py": "a Stocks plan that includes this data",
    "test_options_integration.py": "an Options plan that includes this data",
    "test_indices_integration.py": "an Indices plan that includes this data",
    "test_futures_integration.py": "a Futures plan that includes this data",
    "test_reference_integration.py": "a plan that includes this reference data",
    "test_flatfiles_integration.py": "flat-file access to this dataset",
    "test_hosts_integration.py": "the plan this check names in its docstring",
    "test_ws_integration.py": "the plan this check names in its docstring",
}
"""The plan a live test module's data needs, named in a skip when the key lacks it."""

_ENTITLEMENT_MARKS: Final = ("HTTP 403", "NOT_AUTHORIZED", "not entitled", "plan may not include")
"""Text that marks a stream's or a poll's error as a missing entitlement."""


def not_entitled_reason(plan: str, error: object) -> str:
    """Return the skip reason, and evidence line, of a request the key's plan does not cover.

    Args:
        plan: The plan the data needs, such as `"a Stocks plan that includes this data"`.
        error: The refusal, as Massive or the library worded it.

    Returns:
        `"not entitled: needs <plan> (<error>)"`.
    """
    return f"not entitled: needs {plan} ({error})"


def is_entitlement_text(text: object) -> bool:
    """Return whether an error's text (a failed stream's or poll's `Error`) is a refusal.

    Args:
        text: The error text, or None.

    Returns:
        True when it holds an HTTP 403, a `NOT_AUTHORIZED` envelope, or the library's wording
        of a refused topic.
    """
    return isinstance(text, str) and any(mark in text for mark in _ENTITLEMENT_MARKS)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None, object, object]:
    """Lift the faulthandler limit from a live test, and decide what a refusal means.

    The 120 s faulthandler limit is cancelled for the call of a live test, whose own timeout,
    scaled to its paced requests, bounds it instead.

    A test that declares its plans (`needs`) runs only when they are declared held, so a
    refusal (an `EntitlementError`, or the wire's `WireEntitlementError`: an HTTP 403 or a
    `NOT_AUTHORIZED` envelope) contradicts the declaration: it is recorded as evidence and
    fails the test. A live test without `needs` (the smoke tier handles its own) skips with
    "not entitled: needs <plan>", the plan taken from the test module. Other tests are
    untouched.

    Args:
        item: The test being run.

    Yields:
        Control to the test call.

    Returns:
        The call's result, unchanged.

    Raises:
        EntitlementError: When it escapes a test that is not a live test, or one whose
            declared plans should have covered the request.
        WireEntitlementError: In the same cases.
    """
    if item.get_closest_marker(_INTEGRATION) is not None:
        # A live test waits on Massive's rate limits and streams, longer than the 120 s after
        # which faulthandler would end the process; its own (scaled) timeout bounds it.
        faulthandler.cancel_dump_traceback_later()
    try:
        return (yield)
    except (EntitlementError, WireEntitlementError) as error:
        if item.get_closest_marker(_INTEGRATION) is None:
            raise
        requirements = _requirements(item)
        if requirements:
            declared = ", ".join(_plan_text(r) for r in requirements)
            _record(item.config, item.nodeid, f"refused although {declared} is declared: {error}")
            raise
        plan = _PLANS.get(item.path.name, "a plan that includes this data")
        pytest.skip(not_entitled_reason(plan, error))


@pytest.fixture
def skip_if_not_entitled(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[MassiveSession, str], None]:
    """Return a function that skips a stream or poll test whose stream was refused.

    A refused stream does not raise: its row in `m.streams()` turns `FAILED` with the refusal
    as its `Error`. Call this after recording the evidence.

    Returns:
        A function `(session, plan)`: it skips the test, naming the plan, when one of the
        session's streams failed with a refusal.
    """

    def check(session: MassiveSession, plan: str) -> None:
        for row in table_rows(session.streams()):
            if row["State"] == "FAILED" and is_entitlement_text(row["Error"]):
                pytest.skip(not_entitled_reason(plan, row["Error"]))

    return check


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
def index_symbol() -> str:
    """Return the index a live test reads: `I:SPX`, or `I:NDX` when Indices is declared Basic.

    Indices Basic excludes SPX, DJI, RUT, and VIX
    (https://massive.com/knowledge-base/article/are-all-indices-available-through-the-basic-subscription-tier).

    Returns:
        The index's ticker.
    """
    plans, _ = _declared_plans()
    return "I:NDX" if plans["indices"] == "basic" else "I:SPX"


@pytest.fixture
def require_plans() -> Callable[[tuple[str, ...]], None]:
    """Return a function that skips a test unless the declared plans meet some requirements.

    For a test whose requirements depend on its parameters (the smoke tier's), where a
    `needs` marker cannot name them.

    Returns:
        A function taking requirements (`"<class>:<plan>"`, `"s3"`); it skips, naming each
        unmet plan and its price.
    """

    def check(requirements: tuple[str, ...]) -> None:
        unmet = _unmet(requirements)
        if unmet:
            pytest.skip("needs " + "; ".join(_plan_text(r) for r in unmet))

    return check


@pytest.fixture
def basic_class() -> str:
    """Return a class declared on Basic, preferring the one a run is least likely to need.

    Returns:
        `"indices"`, `"futures"`, `"options"`, `"stocks"`, or `"currencies"`, the first of those
        declared Basic; the test is skipped when none is.
    """
    plans, _ = _declared_plans()
    basic = [
        n for n in ("indices", "futures", "options", "stocks", "currencies") if plans[n] == "basic"
    ]
    if not basic:
        pytest.skip("needs at least one class left on Basic")
    return basic[0]


@pytest.fixture
def delayed_key(live_config: MassiveConfig) -> str:
    """Return a key with delayed stocks data: `MASSIVE_DELAYED_API_KEY`, or the main key.

    The main key serves when Stocks is declared Starter or Developer, the delayed plans.

    Args:
        live_config: The live configuration, for the main key.

    Returns:
        The key; the test is skipped when there is none.
    """
    key = os.environ.get("MASSIVE_DELAYED_API_KEY", "").strip()
    if key:
        return key
    plans, _ = _declared_plans()
    if plans["stocks"] not in {"starter", "developer"}:
        pytest.skip("needs a delayed Stocks key: MASSIVE_DELAYED_API_KEY, or Stocks Starter")
    return live_config.api_key


@pytest.fixture
def evidence(request: pytest.FixtureRequest) -> Callable[[str], None]:
    """Return a function that records one line of what a live test observed.

    Each line is appended at once to `live-evidence.log` in the repository root, under the
    test's id, and flushed, so nothing recorded is lost if the run is cut short (a timeout
    that ends the process). The lines are printed again, grouped by test, in the "live
    evidence" section of the terminal summary, whatever the test's outcome. Safe to call from
    any thread.

    Args:
        request: The requesting test, whose id heads its lines.

    Returns:
        A function taking one line of text.
    """
    config = request.config
    test_id = request.node.nodeid

    def record(line: str) -> None:
        _record(config, test_id, line)

    return record


def _record(config: pytest.Config, test_id: str, line: str) -> None:
    """Record one evidence line: in the run's list, and appended to `live-evidence.log`.

    Args:
        config: The pytest configuration, whose stash holds the list.
        test_id: The test's id, which heads its lines.
        line: The line.
    """
    records = config.stash.setdefault(_EVIDENCE, [])
    with _EVIDENCE_LOCK:
        heading = not records or records[-1][0] != test_id
        records.append((test_id, line))
        with (config.rootpath / _EVIDENCE_FILE).open("a", encoding="utf-8") as file:
            if heading:
                file.write(f"{datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ} {test_id}\n")
            file.write(f"    {line}\n")
            file.flush()


def _skip_reason(report: pytest.TestReport) -> str:
    """Return a skipped test's reason.

    Args:
        report: The skipped test's report.

    Returns:
        The reason, without pytest's `Skipped: ` prefix.
    """
    longrepr = report.longrepr
    reason = str(longrepr[2]) if isinstance(longrepr, tuple) else str(longrepr)
    return reason.removeprefix("Skipped: ")


def _smoke_coverage(terminalreporter: pytest.TerminalReporter) -> None:
    """Print the smoke tier's coverage: passed, not entitled, otherwise skipped, failed.

    Args:
        terminalreporter: The terminal reporter, whose statistics hold the reports.
    """

    def smoke(outcome: str) -> list[pytest.TestReport]:
        return [
            report
            for report in terminalreporter.stats.get(outcome, [])
            if isinstance(report, pytest.TestReport) and _SMOKE in report.keywords
        ]

    passed = [r for r in smoke("passed") if r.when == "call"]
    skipped = smoke("skipped")
    failed = smoke("failed") + smoke("error")
    if not (passed or skipped or failed):
        return
    refused = [r for r in skipped if _skip_reason(r).startswith("not entitled")]
    other = [r for r in skipped if r not in refused]
    terminalreporter.write_sep("=", "smoke coverage")
    terminalreporter.write_line(
        f"{len(passed)} passed, {len(refused)} not entitled, {len(other)} skipped otherwise, "
        f"{len(failed)} failed"
    )
    for title, reports in (("not entitled", refused), ("skipped", other), ("failed", failed)):
        for report in reports:
            reason = _skip_reason(report) if report.skipped else report.head_line or ""
            terminalreporter.write_line(
                f"{title}: {report.nodeid.split('[')[-1].rstrip(']')}: {reason[:200]}"
            )


def _settled_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    """Print which open questions the run verified, and the plans the rest need.

    An item (a `settles` argument) is verified when every test that settles it passed; one
    whose tests were skipped for a plan is listed under the cheapest plan text it needs.

    Args:
        terminalreporter: The terminal reporter, whose statistics hold the outcomes.
        config: The pytest configuration, whose stash holds each test's items and needs.
    """
    settled = config.stash.setdefault(_SETTLED, {})
    ran = any(terminalreporter.stats.get(kind) for kind in ("passed", "failed", "skipped"))
    keyed = bool(os.environ.get("MASSIVE_API_KEY", "").strip())
    if not (ran and keyed) or not any(items for items, _ in settled.values()):
        return
    outcome: dict[str, str] = {}
    for kind in ("passed", "failed", "skipped", "error"):
        for report in terminalreporter.stats.get(kind, []):
            if isinstance(report, pytest.TestReport) and (
                report.when == "call" or kind != "passed"
            ):
                outcome[report.nodeid] = kind
    verified: list[str] = []
    needing: dict[str, list[str]] = {}
    other: list[str] = []
    ran_items = {i for t, (items, _) in settled.items() if t in outcome for i in items}
    if not ran_items:
        return
    for item in sorted(ran_items, key=_item_order):
        tests = [(t, needs) for t, (items, needs) in settled.items() if item in items]
        tests = [(t, needs) for t, needs in tests if t in outcome]
        results = [outcome.get(t) for t, _ in tests]
        if all(result == "passed" for result in results):
            verified.append(item)
            continue
        unmet = [r for t, needs in tests if outcome.get(t) == "skipped" for r in _unmet(needs)]
        if unmet:
            needing.setdefault(", ".join(_plan_text(r) for r in dict.fromkeys(unmet)), []).append(
                item
            )
        else:
            other.append(item)
    total = len(verified) + sum(len(items) for items in needing.values()) + len(other)
    terminalreporter.write_sep("=", "open questions")
    terminalreporter.write_line(f"verified {len(verified)} of {total}: {', '.join(verified)}")
    for plan, items in needing.items():
        terminalreporter.write_line(f"{len(items)} need {plan}: {', '.join(items)}")
    if other:
        terminalreporter.write_line(
            f"{len(other)} not verified (failed or skipped): {', '.join(other)}"
        )


def _item_order(item: str) -> tuple[int, str]:
    """Sort numbered items first, by number, then named ones.

    Args:
        item: An item, such as `"21"` or `"429 text"`.

    Returns:
        The sort key.
    """
    return (int(item), "") if item.isdigit() else (10_000, item)


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter, config: pytest.Config
) -> None:
    """Print the smoke tier's coverage, then the evidence the live tests recorded, by test.

    Args:
        terminalreporter: The terminal reporter that writes the summary.
        config: The pytest configuration, whose stash holds the evidence.
    """
    _smoke_coverage(terminalreporter)
    _settled_summary(terminalreporter, config)
    stored: list[tuple[str, str]] = config.stash.setdefault(_EVIDENCE, [])
    with _EVIDENCE_LOCK:
        records = list(stored)
    if not records:
        return
    terminalreporter.write_sep("=", f"live evidence (also in {_EVIDENCE_FILE})")
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
    """Return the live configuration, or skip the test when it lacks an S3 credential.

    Args:
        live_config: The live configuration.

    Returns:
        `live_config`, whose `s3_access_key_id` and `s3_secret_access_key` are both set.
    """
    if live_config.s3_access_key_id is None or live_config.s3_secret_access_key is None:
        pytest.skip("MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY are not both set")
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
        live_s3_config: The live configuration with both S3 credentials.

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


def _raw_plan(prefix: str, tickers: list[str] | None) -> ReadPlan:
    """Return a plan that reads every documented header of a prefix as its CSV text.

    Nothing is converted (no unit, no type) and futures rows are not checked against their
    session, so the rows show the wire form.

    Args:
        prefix: The generated prefix, such as `"us_futures_{venue}/session_aggs_v1"`.
        tickers: The `ticker` values to keep, as a sorted list, or None for every row.

    Returns:
        The plan.
    """
    headers = [field.wire_key for field in source(Transport.FLAT_FILES, prefix).fields]
    return ReadPlan(
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


def _read_flat_file(
    client: FlatFileClient,
    key: str,
    prefix: str,
    *,
    max_rows: int | None,
    tickers: list[str] | None,
) -> FlatFileRows | None:
    """Read the raw rows of one flat file: every documented header as its CSV text.

    Args:
        client: The live flat-file client.
        key: The object's key.
        prefix: The generated prefix whose documented headers are read.
        max_rows: The most rows to keep; reading stops after the block that reaches it. None
            reads the whole file.
        tickers: The `ticker` values to keep, as a sorted list, or None for every row.

    Returns:
        The rows and the headers the file has beyond the documented ones, or None when the
        file does not exist. A refusal raises the wire error.
    """
    file = client.open(key)
    if file is None:
        return None
    rows: list[dict[str, str | None]] = []
    extra: frozenset[str] = frozenset()
    with file, contextlib.closing(file.batches(_raw_plan(prefix, tickers))) as batches:
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
        does not exist; a refusal raises the wire error.
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
def flat_file_sessions(
    live_flat_files: FlatFileClient,
) -> Callable[[str, date, str], Counter[str | None] | None]:
    """Return a function that counts the `session_end_date` values of a whole futures file.

    The whole file is read, block by block, and only the counts are kept, so a large minute
    file costs no memory.

    Args:
        live_flat_files: The live flat-file client.

    Returns:
        A function `(prefix, day, venue)`: a futures prefix such as
        `"us_futures_{venue}/minute_aggs_v1"`, the file's day, and the venue. It returns how
        many rows hold each `session_end_date` text, or None when the file does not exist; a
        refusal raises the wire error.
    """

    def count(prefix: str, day: date, venue: str) -> Counter[str | None] | None:
        file = live_flat_files.open(object_key(prefix, day, venue))
        if file is None:
            return None
        counts: Counter[str | None] = Counter()
        with file, contextlib.closing(file.batches(_raw_plan(prefix, None))) as batches:
            for batch, _drift in batches:
                counts.update(batch.column("session_end_date").to_pylist())
        return counts

    return count


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

    Call it before releasing a stream: a release is carried out on the session's reaper
    thread, so a report taken after it may or may not still show the stream's topics.

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
    """Return a function that waits for a live table's first rows, up to a deadline.

    Returns:
        A function `(session, table, deadline)`, the deadline a `time.monotonic()` value: it
        returns the table's rows once it holds any, once any of the session's streams has
        `FAILED`, or once the deadline has passed, whichever comes first, so the caller
        records what happened (a closed market sends nothing; a refusal fails the stream).
        Several waits of one test share one deadline, which keeps the test within its time.
    """

    def wait(session: MassiveSession, table: Table, deadline: float) -> list[dict[str, Any]]:
        pause = threading.Event()  # never set; only paces the polling
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
        pages = live_rest.pages(rest_path, path_params, query_params)
        return [row for page in pages for row in page.rows]

    return read


_MS = 1_000_000
"""Nanoseconds per millisecond."""

_DAY_MS = 86_400_000
"""Milliseconds per day."""

_WEEK_MS = 7 * _DAY_MS
"""Milliseconds per week."""

_WEEK_RANGE = (date(2026, 2, 23), date(2026, 3, 22))
"""The four calendar weeks, Monday to Sunday, whose week bars the week-start checks read."""

_MATCH = 1e-9
"""The relative volume error within which a candidate week is the bar's bucket."""

_NEW_YORK = ZoneInfo("America/New_York")


def _week_candidates(stamp_ms: int) -> list[tuple[str, int]]:
    """Return the candidate starts of the seven days a week bar may cover.

    Args:
        stamp_ms: The bar's stamp, in epoch milliseconds.

    Returns:
        `(label, start_ms)` pairs: 00:00 UTC of each day from three days before the stamp's
        UTC day to three days after, and 17:00 New York time of each of those days (the
        forex trading day's start).
    """
    day = datetime.fromtimestamp(stamp_ms / 1000, UTC).date()
    found = []
    for offset in range(-3, 4):
        start = day + timedelta(days=offset)
        utc = datetime.combine(start, dt_time(), UTC)
        new_york = datetime.combine(start, dt_time(17), _NEW_YORK)
        found.append((f"{start:%a %m-%d} 00:00 UTC", int(utc.timestamp()) * 1000))
        found.append((f"{start:%a %m-%d} 17:00 New York", int(new_york.timestamp()) * 1000))
    return found


@pytest.fixture
def week_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
) -> Callable[[str, str], list[str]]:
    """Return a function that shows where Massive starts a week bar, from raw REST bars.

    It reads the week bars and the hour bars of one ticker over four calendar weeks
    (2026-02-23 to 2026-03-22) from the aggregates range endpoint, unfiltered. For each week
    bar it reports its stamp, its volume, the first and last hour bar of the seven days from
    its stamp, and the candidate seven-day windows whose hour-bar volume comes closest to the
    bar's: windows starting at 00:00 UTC, and at 17:00 New York time, on each day from three
    days before the stamp to three after. A candidate within a relative error of 1e-9 is
    marked `MATCH`: that is the bucket Massive uses, wherever the bar is stamped.

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
            params |= {"from": first - timedelta(days=7), "to": last + timedelta(days=7)}
            return rest_rows(path, params, {"sort": "asc", "limit": 50_000})

        weeks, hours = bars("week"), bars("hour")

        def volume(start_ms: int) -> float:
            return float(sum(h["v"] for h in hours if start_ms <= h["t"] < start_ms + _WEEK_MS))

        lines = [f"{ticker}: {len(weeks)} week bars, {len(hours)} hour bars"]
        for week in weeks:
            stamp = int(week["t"])
            target = float(week["v"])
            inside = [h for h in hours if stamp <= h["t"] < stamp + _WEEK_MS]
            scored = sorted(
                (abs(volume(start) - target) / max(abs(target), 1e-12), label)
                for label, start in _week_candidates(stamp)
            )
            best = ", ".join(
                f"{label} err={error:.2e}{' MATCH' if error <= _MATCH else ''}"
                for error, label in scored[:3]
            )
            span = (
                f"{_instant_text(inside[0]['t'] * _MS)} .. {_instant_text(inside[-1]['t'] * _MS)}"
                if inside
                else "none"
            )
            lines.append(
                f"week t={_instant_text(stamp * _MS)} v={target} hours from the stamp: {span}; "
                f"closest windows: {best}"
            )
        return lines

    return report


_HALF_DAYS = (date(2025, 11, 28), date(2025, 12, 24))
"""US market half days (closing at 13:00 New York time): the day after Thanksgiving 2025 and
Christmas Eve 2025."""


def _wall_clock(stamp_ms: int) -> str:
    """Describe an epoch-millisecond stamp in UTC and New York time, naming a notable hour.

    Args:
        stamp_ms: The stamp.

    Returns:
        Such as `"2025-11-28T18:00:00Z = 13:00 New York (the half-day close)"`.
    """
    utc = datetime.fromtimestamp(stamp_ms / 1000, UTC)
    local = utc.astimezone(_NEW_YORK)
    notable = {
        (13, 0): " (the half-day close)",
        (16, 0): " (the regular close)",
        (0, 0): " (midnight)",
        (9, 30): " (the open)",
    }.get((local.hour, local.minute), "")
    return f"{utc:%Y-%m-%dT%H:%M:%SZ} = {local:%H:%M} New York{notable}"


@pytest.fixture
def half_day_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
) -> Callable[[str, str], list[str]]:
    """Return a function that shows how REST stamps the bars of a half trading day.

    For each half day (2025-11-28, 2025-12-24) it reads one ticker's day bar and its minute
    bars from the aggregates range endpoint, unfiltered, and reports the day bar's stamp and
    the first and last minute bar's, each in UTC and New York time with the notable hour named
    (13:00 the half-day close, 16:00 the regular close, midnight, the 09:30 open).

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
        lines = []
        for day in _HALF_DAYS:
            for span in ("day", "minute"):
                params = {ticker_param: ticker, "multiplier": 1, "timespan": span}
                params |= {"from": day, "to": day}
                bars = rest_rows(path, params, {"sort": "asc", "limit": 50_000})
                if not bars:
                    lines.append(f"{ticker} {day} {span} bars: none")
                elif span == "day":
                    lines.extend(
                        f"{ticker} {day} day bar: t={_wall_clock(int(b['t']))}" for b in bars
                    )
                else:
                    first, last = _wall_clock(int(bars[0]["t"])), _wall_clock(int(bars[-1]["t"]))
                    lines.append(
                        f"{ticker} {day} {len(bars)} minute bars: first t={first}, last t={last}"
                    )
        return lines

    return report
