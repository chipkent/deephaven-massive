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
import itertools
import os
import re
import sys
import threading
import time
import types
import weakref
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, TypeAlias, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from _pytest.mark.expression import Expression
from deephaven_server import Server

from deephaven_massive._errors import EntitlementError, MassiveError
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
    from deephaven_massive._enums import Subscription
    from deephaven_massive._ingest.rate_limits import GovernorRegistry
    from tests._fakes.clock import FakeClock
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


def _is_xdist_controller(config: pytest.Config) -> bool:
    """Tell whether this process hands tests to pytest-xdist workers instead of running them.

    The condition is xdist's own for starting its distributed session: a distribution mode
    other than `no` and at least one worker (`-n 0` leaves none), in a run that is more than
    collection. Under `--collect-only`, `--fixtures`, and `--fixtures-per-test`, xdist starts
    no session and the main process collects the tests itself. Workers carry `workerinput`.

    Args:
        config: The pytest configuration.

    Returns:
        True only in the xdist controller of a distributed run.
    """
    option = config.option
    return (
        getattr(option, "dist", "no") != "no"
        and bool(getattr(option, "tx", None))
        and not option.collectonly
        and not option.showfixtures
        and not option.show_fixtures_per_test
        and not hasattr(config, "workerinput")
    )


def _selects_live_tests(config: pytest.Config) -> bool:
    """Tell whether the run's `-m` expression selects any live test.

    A live test carries `integration` and any combination of the markers only live tests use:
    `integration_limits`, `integration_smoke`, `needs`, `settles`, and `requests`. An `-m`
    expression matches a marker by name whatever its arguments, so `-m needs` selects every
    live test with a `needs` marker. Each combination of those markers is tried; a keyword
    form such as `needs(x=1)` is treated as matching nothing, since their arguments are
    positional.

    Args:
        config: The pytest configuration, for the `-m` expression.

    Returns:
        True if a test with any of those marker sets would be selected.
    """
    expression = Expression.compile(config.option.markexpr or "")
    optional = (_LIMITS, _SMOKE, "needs", "settles", "requests")

    def carrying(markers: frozenset[str]) -> Callable[..., bool]:
        def matcher(name: str, /, **kwargs: str | int | bool | None) -> bool:
            return not kwargs and name in markers

        return matcher

    return any(
        expression.evaluate(carrying(frozenset({_INTEGRATION, *extra})))
        for size in range(len(optional) + 1)
        for extra in itertools.combinations(optional, size)
    )


# `trylast` matters: pytest's faulthandler plugin installs its SIGSEGV handler in its own
# `pytest_configure`. The JVM must install its signal handlers after that, chaining to the
# faulthandler, because the JVM raises SIGSEGV on purpose (implicit null checks, safepoints).
# Started first, the JVM's handlers would be replaced and the first such signal would kill
# the process.
@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    """Start the embedded Deephaven server once per process, before collection.

    Then check that the update graph runs the 25 ms cycle the tests are written for. Under
    pytest-xdist (`-n`), only the worker processes start a server.

    Args:
        config: The pytest configuration, read to tell an xdist controller from a worker.

    Raises:
        pytest.UsageError: If the JVM did not apply the cycle property, or if the live tests
            are asked to run on parallel workers.
    """
    # The xdist controller only hands tests to its workers and reports their results; it never
    # imports a test module or runs a test. Each worker is its own process that runs this hook
    # and starts its own JVM, so a JVM in the controller would cost only startup time and
    # memory.
    if _is_xdist_controller(config):
        # The live tests share Massive's per-key rate limits and write one evidence log and
        # one summary, which only a single process keeps whole.
        if _selects_live_tests(config):
            raise pytest.UsageError("the live tests run in one process: drop -n, or pass -n 0")
        return
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

    The conversion holds the update graph's shared lock, so no update cycle runs while it
    reads a live table. Deephaven's Arrow conversion snapshots the rows consistently, but
    first reads the table's flatness without a lock. On a table Deephaven marks flat (a
    `select_distinct`, for one), that read asserts the row set is flat by comparing its last
    key with its size, two separate reads that a cycle growing the row set can fall between.

    Returns:
        A function mapping a table to one `{column: value}` dictionary per row.
    """
    dh_arrow = importlib.import_module("deephaven.arrow")
    update_graph = importlib.import_module("deephaven.update_graph")

    def to_pylist(table: Table) -> list[dict[str, Any]]:
        with update_graph.shared_lock(table):
            rows: list[dict[str, Any]] = dh_arrow.to_arrow(table).to_pylist()
        return rows

    return to_pylist


@pytest.fixture
def string_objects() -> Callable[[Table, str], int]:
    """Return a function that counts the distinct Java string objects of a column's values.

    Identity is read inside Deephaven, since a string crossing to Python is copied. The formula
    goes straight to Java: the Python `Table.update` copies the calling frame's locals into the
    query scope through jpy, where they would live until the JVM collected them, keeping a
    result (and through it its session) alive.

    Returns:
        A function of a table and a column name, counting the objects among non-null values.
    """
    liveness = importlib.import_module("deephaven.liveness_scope")
    jpy = importlib.import_module("jpy")
    update_graph = importlib.import_module("deephaven.update_graph")
    java_table = jpy.get_type("io.deephaven.engine.table.Table")

    def count(table: Table, column: str) -> int:
        # The derived tables live in a scope of their own, released at once: a refreshing table
        # derived from a live result would keep its stream or poll running.
        scope = liveness.LivenessScope()
        try:
            # Java calls: the Python wrappers would copy this frame's locals.
            with scope.open(), update_graph.shared_lock(table):
                refs = table.j_table.update(f"Ref = java.lang.System.identityHashCode({column})")
                distinct = refs.where(f"!isNull({column})").selectDistinct("Ref")
                return int(jpy.cast(distinct, java_table).size())
        finally:
            scope.release()

    return count


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
        # The fastest level: the reader decompresses any level alike, and the default, 9, spends
        # about 20 times as long on the suite's largest files.
        return gzip.compress(csv_text.encode(), compresslevel=1)

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


_LIBRARY_THREADS: Final = ("massive",)
"""The name prefix of the library's threads: a session's reaper, diagnostics writer, pollers,
call pools, and crypto-pair worker (`massive #N ...`), a connection group's clients, flush,
delayed-start, verdict-move, and futures schedule threads (`massive group #N ...`), and a
client made alone (`massive ws ...`)."""


@pytest.fixture(autouse=True)
def _check_nothing_outlives_the_test(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test that leaves a session running or alive, or a library thread alive.

    A session is in the live set exactly while it has a running stream or poll or a held
    diagnostics view, so a test that ends with one has leaked a table, or the library has. A
    test whose call already failed is not checked: its traceback holds its locals. A
    session that ends without releasing its lease on the shared REST rate governor fails it
    too: the test's registry (`rest_clock`) must hold no more leases than when it began. A
    session the test created that is still alive at the end, in the live set or not, is held
    by something that should have let it go. Only what the test itself started counts, so one
    leak fails its own test and not every test after it.

    Autouse, and named to sort first (pytest orders autouse fixtures by name), so it is set up
    first and runs after every other fixture's teardown (a closed `massive_session` included).
    The library frees an unused session by reference counting alone, so the check runs with
    the cyclic collector off and polls, bounded and in Python, until nothing is left. Unless
    the first look finds nothing, it collects with `gc.DEBUG_SAVEALL`, at most once a second,
    which keeps what it would free in `gc.garbage`: a session a cycle there holds
    (`_held_by_cycles`) is one reference counting never frees, and the test fails with "freed
    only by the cyclic collector", unless the cycle runs through an exception's traceback. A
    test marked `jvm_held`, whose own formula hands its frame's locals to the JVM, gets one JVM
    collection for what is still alive or running after a second.
    """
    before = _running()
    yield
    if request.node.stash.get(_CALL_FAILED, False):
        return  # its traceback holds the test's locals; the failure is already reported
    # Every other fixture is torn down, but pytest keeps their values in the item until its
    # teardown ends; drop them, as pytest is about to, so a closed session can be freed.
    funcargs: dict[str, object] | None = getattr(request.node, "funcargs", None)
    if funcargs:
        funcargs.clear()
    enabled = gc.isenabled()
    gc.disable()
    try:
        cyclic: set[str] = set()
        started = time.monotonic()
        deadline = started + 30.0
        pause = threading.Event()  # never set; only paces the bounded wait
        next_collect = started + 0.04  # after a first look, then at most once a second: slow
        jvm_collected = False
        jvm_held = request.node.get_closest_marker("jvm_held") is not None
        while True:
            _drop_logged_objects(request.node)  # a library thread may still be logging
            if time.monotonic() > next_collect:
                cyclic |= _collect_cycles(before)
                next_collect = time.monotonic() + 1.0
            sessions, alive, groups, threads = _new(before)
            if not (sessions or alive or groups or threads or _new_leases(request.node)):
                break
            if time.monotonic() > deadline:
                pytest.fail(
                    f"left running after the test: {_left(before)}, "
                    f"{_new_leases(request.node)} rate governor leases"
                )
            if jvm_held and not jvm_collected and time.monotonic() > started + 1.0:
                # A formula a test ran (`t.update("...")`) copies its frame's locals into the
                # query scope through jpy, and a session, a result, or a table derived from it
                # among them lives until the JVM collects them: that is Deephaven's, not a leak
                # of the library.
                jpy = importlib.import_module("jpy")
                jpy.get_type("java.lang.System").gc()
                jvm_collected = True
            for thread in threads:  # wait on the threads themselves, briefly each
                thread.join(0.05)
            if not threads:
                pause.wait(0.05)
        if cyclic:
            pytest.fail(f"freed only by the cyclic collector: sessions {sorted(cyclic)}")
    finally:
        if enabled:
            gc.enable()


_Running: TypeAlias = "tuple[set[str], set[str], set[object], set[threading.Thread]]"


def _new_leases(node: pytest.Item) -> int:
    """Count the rate governor leases a test's sessions took and never released.

    Args:
        node: The test, whose stash holds its registry and its leases when it began
            (`rest_clock`).

    Returns:
        The leases held now beyond those held when the test began; 0 without a registry.
    """
    stashed = node.stash.get(_LEASES, None)
    return 0 if stashed is None else stashed[0].leases() - stashed[1]


def _new(before: _Running) -> _Running:
    """Return what of the library runs now that did not run before the test.

    Args:
        before: What ran when the test started.

    Returns:
        The new live sessions, alive sessions, connection groups, and threads.
    """
    sessions, alive, groups, threads = _running()
    return sessions - before[0], alive - before[1], groups - before[2], threads - before[3]


def _left(before: _Running) -> str:
    """Name what a test left running, for a failure message.

    Args:
        before: What ran when the test started.

    Returns:
        The text.
    """
    sessions, alive, groups, threads = _new(before)
    return (
        f"sessions {sorted(sessions)}, alive sessions {sorted(alive)}, {len(groups)} connection "
        f"groups, threads {sorted(t.name for t in threads)}"
    )


def _collect_cycles(before: _Running) -> set[str]:
    """Collect garbage, and name the sessions the test started that only a cycle held.

    Collects with `gc.DEBUG_SAVEALL`, so what the collector would free stays in `gc.garbage`
    to be inspected; then empties it, which frees it. A session in that garbage was reachable
    only through a reference cycle, which reference counting never frees.

    Args:
        before: What ran when the test started.

    Returns:
        The labels of those sessions, except the ones only a traceback's frames held.
    """
    session = importlib.import_module("deephaven_massive._session")
    _, alive, _, _ = _new(before)
    with session._keys_lock:
        cores = [ref() for ref in session._open_sessions]
    wanted = {id(core): core.label for core in cores if core is not None and core.label in alive}
    del cores
    gc.set_debug(gc.DEBUG_SAVEALL)
    try:
        gc.collect()
        garbage = list(gc.garbage)
        gc.garbage.clear()
    finally:
        gc.set_debug(0)
    # A raised exception and the frames of its traceback form a cycle, and a frame holds its
    # locals: a session a failing call had in hand lives until the collector breaks that
    # cycle. That is how Python works, not a leak, so the references out of frames and
    # tracebacks are not followed, and only a session another cycle holds is named.
    cyclic = _held_by_cycles(garbage, follow=_not_a_traceback) & set(wanted)
    del garbage
    return {wanted[key] for key in cyclic}


def _not_a_traceback(item: object) -> bool:
    """Tell whether the references out of an object count: not out of a frame or traceback.

    Args:
        item: A garbage object.

    Returns:
        False for a frame or a traceback.
    """
    return type(item) not in {types.FrameType, types.TracebackType}


_RESTORES_METHODS: Final = pytest.version_tuple < (9, 2)
"""Whether this pytest restores a patched method as an attribute of its own instance."""


def _held_by_cycles(garbage: list[object], *, follow: Callable[[object], bool]) -> set[int]:
    """Return the ids of the garbage that a reference cycle holds.

    Not everything the collector finds is held by a cycle. On a free-threaded build a reference
    another thread dropped waits for its object's creating thread to merge it, and the
    collector merges it first; an object that then has no reference left is garbage too,
    although reference counting alone frees it. So the references among the garbage are
    counted, and what nothing else in the garbage refers to is let go, repeatedly, as
    reference counting would let it go; what remains is held by a cycle.

    pytest before 9.2 restores a patched method by setting it on its own instance: a bound
    method under its own name in the instance's `__dict__`, a cycle of the test tool's. On such
    a pytest that one reference is not counted; any other self-reference is.

    Args:
        garbage: What the collector found unreachable.
        follow: Whether the references out of an object count.

    Returns:
        The ids of the objects reference counting alone would never free.
    """
    ids = {id(item) for item in garbage}
    owners = (
        {id(vars(item)): item for item in garbage if _has_dict(item)} if _RESTORES_METHODS else {}
    )
    holds: dict[int, list[int]] = {}
    held = dict.fromkeys(ids, 0)
    for item in garbage:
        referents: list[object]
        if not follow(item):
            referents = []
        elif id(item) in owners:
            owner = owners[id(item)]
            attributes = cast("dict[str, object]", item).items()
            referents = [v for k, v in attributes if not _restored_method(owner, k, v)]
        else:
            referents = gc.get_referents(item)
        holds[id(item)] = [id(each) for each in referents if id(each) in ids]
        for key in holds[id(item)]:
            held[key] += 1
    free = [key for key, count in held.items() if not count]
    while free:
        for key in holds[free.pop()]:
            held[key] -= 1
            if not held[key]:
                free.append(key)
    return {key for key, count in held.items() if count}


def _restored_method(owner: object, name: str, value: object) -> bool:
    """Tell whether an instance attribute is a method pytest restored onto its own instance.

    Args:
        owner: The instance.
        name: The attribute's name.
        value: Its value.

    Returns:
        True for a bound method of `owner`, under its own name, of the class's own function.
    """
    return (
        type(value) is types.MethodType
        and value.__self__ is owner
        and value.__name__ == name
        and value.__func__ is getattr(type(owner), name, None)
    )


def _has_dict(item: object) -> bool:
    """Tell whether an object has an instance `__dict__`, without touching a dead proxy.

    Args:
        item: The object.

    Returns:
        True if `vars(item)` returns its attributes.
    """
    if issubclass(type(item), (type, types.ModuleType, weakref.ProxyTypes)):
        return False
    try:
        vars(item)
    except TypeError:
        return False
    return True


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
    with ws._groups_lock:
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


@pytest.fixture(autouse=True)
def rest_clock(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Give each test a registry of REST rate governors of its own, on a fake clock.

    Sessions share one governor per API key through a process-wide registry, so without this
    one test's pauses, learned limits, and held slots would carry over into the next. Every
    governor of the test, and so every session's REST client, waits on the returned clock:
    automatic by default (a wait returns at once and moves virtual time), so no rate-limit or
    retry wait ever sleeps. A test that needs the manual mode replaces the registry itself.

    A live test (marked `integration`) talks to Massive, whose limits run in real time and
    count across tests, so it gets the run's one registry on the real clock instead
    (`_live_governors`); the returned fake clock then governs nothing.

    Returns:
        The clock.
    """
    clock_module = importlib.import_module("tests._fakes.clock")
    functools = importlib.import_module("functools")
    rate_limits = importlib.import_module("deephaven_massive._ingest.rate_limits")
    wire = importlib.import_module("deephaven_massive._massive")
    clock: FakeClock = clock_module.FakeClock()
    if request.node.get_closest_marker(_INTEGRATION) is not None:
        registry = _live_governors()
    else:
        create = functools.partial(
            wire.RequestGovernor,
            rate_limits.BUCKETS,
            unpaced=rate_limits.UNPACED,
            clock=clock,
            labels=rate_limits.LABELS,
        )
        registry = rate_limits.GovernorRegistry(create)
    monkeypatch.setattr(rate_limits, "GOVERNORS", registry)
    # The leak check counts the leases the test's sessions leave behind.
    request.node.stash[_LEASES] = (registry, registry.leases())
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
def make_ws_session(fake_ws_server: FakeMassiveServer) -> Iterator[Callable[..., WsSession]]:
    """Return a factory of `WsSession`s (`tests/_fakes/ws_session.py`) on the fake server.

    Each session's streams connect, through real WebSocket clients, to the `fake_ws_server`
    fixture's server. The factory takes `WsSession`'s keyword options (`decide`,
    `flush_interval`, `backoff`, and further `MassiveConfig` options); every session it made is
    closed at teardown, so a test may also close one itself.

    Teardown closes every session even when one close raises, then waits, for at most
    `_CLIENT_THREADS_TIMEOUT` seconds, until no client loop thread (`massive group #<n> ws …`)
    or flush thread (`massive group #<n> flush …`) is alive, and fails the test if one is: a
    client left running reconnects forever, to a stopped server or through whatever proxy a
    later test sets.

    Yields:
        The factory.
    """
    module = importlib.import_module("tests._fakes.ws_session")
    with contextlib.ExitStack() as sessions:

        def make(**options: object) -> WsSession:
            session: WsSession = module.WsSession(fake_ws_server, **options)
            sessions.callback(_drop_session, session)  # last: after the close below
            sessions.callback(session.close)
            return session

        yield make
    deadline = time.monotonic() + _CLIENT_THREADS_TIMEOUT
    pause = threading.Event()  # never set; only paces the polling
    while (left := _client_threads()) and time.monotonic() < deadline:
        pause.wait(0.02)
    assert not left, f"threads outlived their session: {left}"


def _drop_session(harness: WsSession) -> None:
    """Drop a closed wrapper's session and transport, once the test is done with them.

    The wrapper and the transport refer to each other (the transport holds the wrapper's bound
    methods), so without this the session would live until the cyclic collector broke that
    cycle.

    Args:
        harness: The wrapper.
    """
    vars(harness).pop("session", None)
    vars(harness).pop("connections", None)


_CLIENT_THREADS_TIMEOUT: Final = 15.0
"""Seconds `make_ws_session`'s teardown waits for its sessions' threads to end."""

_CLIENT_THREAD: Final = re.compile(r"massive(?: group)?(?: #\d+)? (?:ws|flush) ")
"""The name of a WebSocket client's loop thread or a connection's flush thread."""


def _client_threads() -> list[str]:
    """Return the names of the live WebSocket client and flush threads, in any session."""
    return [t.name for t in threading.enumerate() if _CLIENT_THREAD.match(t.name) and t.is_alive()]


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

_LEASES: Final = pytest.StashKey["tuple[GovernorRegistry, int]"]()
"""A test's rate governor registry and its leases when the test began, for the leak check."""

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


_TICKER_CLASSES: Final = {"X:": "currencies", "C:": "currencies", "I:": "indices", "O:": "options"}
"""The class of a ticker by its prefix; a ticker without one is a stock."""

_PATH_CLASSES: Final = (
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
"""The class of a path without a ticker, by a word in its template."""


def _live_bucket(template: str, path_params: Mapping[str, object]) -> str:
    """Return the rate governor's bucket of a raw live request: the subscription of its data.

    A path that names a ticker is charged to the ticker's class, by its prefix (`X:` and `C:`
    currencies, `I:` indices, `O:` options, any other stocks); the futures paths to futures; a
    path without a ticker by a word of its template. Reference data, economy data, market
    status, partner data, and the unified snapshot belong to no subscription (`other`).

    Args:
        template: The generated path template, such as `"/v2/aggs/ticker/{stocksTicker}/prev"`.
        path_params: The request's placeholder values.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or `"other"`.
    """
    if template.startswith("/futures/"):
        return "futures"
    for name, value in path_params.items():
        if "icker" in name:
            return _TICKER_CLASSES.get(str(value)[:2], "stocks")
    return next((name for word, name in _PATH_CLASSES if word in template), "other")


_LIVE_GOVERNORS: list[GovernorRegistry] = []
"""The run's one registry of live rate governors, on the real clock, created by the first live
test: Massive counts a minute's requests across tests, so the governors must outlive each."""

_LIVE_LOCK = threading.Lock()


def _live_governors() -> GovernorRegistry:
    """Return the run's registry of live rate governors, creating it the first time.

    Returns:
        The registry; its governors wait on the real clock.
    """
    rate_limits = importlib.import_module("deephaven_massive._ingest.rate_limits")
    with _LIVE_LOCK:
        if not _LIVE_GOVERNORS:
            _LIVE_GOVERNORS.append(rate_limits.GovernorRegistry())
        return _LIVE_GOVERNORS[0]


def _basic_subscriptions() -> frozenset[Subscription]:
    """Return the subscriptions `MASSIVE_LIVE_PLANS` leaves on Basic, for the live sessions.

    Returns:
        Each class declared (or left) Basic, as a `dm.Subscription`.
    """
    dm = importlib.import_module("deephaven_massive")
    plans, _ = _declared_plans()
    return frozenset(dm.Subscription(name) for name, plan in plans.items() if plan == "basic")


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


_CALL_FAILED: Final = pytest.StashKey[bool]()
"""Whether a test's call failed, so the leak check does not wait on what its traceback holds."""


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Note a failed call, so the leak check of its teardown returns at once.

    A failed assertion's traceback keeps the test's frame, and with it the test's sessions,
    alive until pytest drops the report; waiting 30 s for them to go would only delay the
    failure already reported.

    Args:
        item: The test.
        call: The phase's result.

    Yields:
        Control to the report's creation.

    Returns:
        The report, unchanged.
    """
    report = yield
    if call.when == "call" and report.failed:
        item.stash[_CALL_FAILED] = True
    return report


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

    An item (a `settles` argument) is verified when every test that settles it ran and
    passed. One that some of its tests never ran for (a `-k` or path selection left them
    out) is "partly run", never verified, since part of its question went unchecked; one
    whose tests were skipped for a plan is listed under the cheapest plan text it needs. A run
    whose paths narrow collection (a `::` test id, or a path other than the configured test
    paths) may not have collected every test of a question, so its passed items are reported
    as "passed in this selection", not verified.

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
    partly: list[str] = []
    ran_items = {i for t, (items, _) in settled.items() if t in outcome for i in items}
    if not ran_items:
        return
    for item in sorted(ran_items, key=_item_order):
        every = [(t, needs) for t, (items, needs) in settled.items() if item in items]
        tests = [(t, needs) for t, needs in every if t in outcome]
        if len(tests) < len(every):
            partly.append(f"{item} ({len(tests)} of {len(every)} tests ran)")
            continue
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
    _write_settled(
        terminalreporter,
        narrowed=_narrowed(config),
        verified=verified,
        needing=needing,
        unverified={"failed or skipped": other, "partly run": partly},
    )


def _narrowed(config: pytest.Config) -> bool:
    """Return whether the run's paths may leave some live tests uncollected.

    Args:
        config: The pytest configuration, for the run's paths and the configured test paths.

    Returns:
        True when a path names a test id (`::`) or is not one of the configured test paths;
        False for a run of the configured paths, which pytest uses when none is given.
    """
    configured = {(config.rootpath / p).resolve() for p in config.getini("testpaths")}
    here = config.invocation_params.dir
    return any("::" in arg or (here / arg).resolve() not in configured for arg in config.args)


def _write_settled(
    terminalreporter: pytest.TerminalReporter,
    *,
    narrowed: bool,
    verified: list[str],
    needing: dict[str, list[str]],
    unverified: dict[str, list[str]],
) -> None:
    """Write the open-questions section of the terminal summary.

    Args:
        terminalreporter: The terminal reporter that writes the summary.
        narrowed: Whether the run's paths may have left some live tests uncollected; its
            passed items are then "passed in this selection", not "verified".
        verified: The items every collected test of which ran and passed.
        needing: The items skipped for a plan, by the plan's text.
        unverified: The other items, by why they are not verified.
    """
    total = (
        len(verified)
        + sum(len(items) for items in needing.values())
        + sum(len(items) for items in unverified.values())
    )
    terminalreporter.write_sep("=", "open questions")
    passed = f"{len(verified)} of {total}: {', '.join(verified)}"
    terminalreporter.write_line(
        f"passed in this selection {passed} (run the full live suite to verify)"
        if narrowed
        else f"verified {passed}"
    )
    for plan, items in needing.items():
        terminalreporter.write_line(f"{len(items)} need {plan}: {', '.join(items)}")
    for why, items in unverified.items():
        if items:
            terminalreporter.write_line(f"{len(items)} not verified ({why}): {', '.join(items)}")


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

    The classes `MASSIVE_LIVE_PLANS` leaves on Basic are its `rest_basic_subscriptions`, so the
    library paces their requests at 5 a minute from the first one.

    Returns:
        The configuration; the test is skipped when `MASSIVE_API_KEY` is not set.
    """
    dm = importlib.import_module("deephaven_massive")
    config: MassiveConfig = dm.MassiveConfig(
        api_key=_required_env("MASSIVE_API_KEY"), rest_basic_subscriptions=_basic_subscriptions()
    )
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
    before the library moves it, a key the library renames). It takes its permits from the
    run's governor of the live key, as the live sessions do, declaring the same Basic
    classes, so a raw request waits its turn like the library's; each call names the bucket
    of its data.

    Args:
        live_config: The live configuration, for the key, the base URL, and the Basic classes.

    Yields:
        A `RestClient` with one connection.
    """
    lease = _live_governors().lease(
        api_key=live_config.api_key,
        base_url=live_config.rest_base_url,
        basic=live_config.rest_basic_subscriptions,
        label="live tests",
        listener=_ignore,
    )
    client = RestClient(
        live_config.api_key,
        base_url=live_config.rest_base_url,
        max_concurrency=1,
        governor=lease.governor,
    )
    yield client
    client.close()
    lease.release()


def _ignore() -> None:
    """Note nothing: the raw client's governor listener (only sessions show diagnostics)."""


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
        pages = live_rest.pages(
            rest_path, path_params, query_params, bucket=_live_bucket(path, path_params)
        )
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
        (1, 0): " (midnight in Chicago)",
        (9, 30): " (the open)",
        (16, 15): " (the index-option close)",
        (13, 15): " (the index-option half-day close)",
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


_SAME_BAR = 1e-6
"""The relative error within which two sources' open, high, low, close, or volume agree; a
flat file's text and a JSON number of the same price can differ in the last digits."""

StampedBar = tuple[int, tuple[float | None, ...]]
"""One source's bar: its stamp in epoch milliseconds and its open, high, low, close, and
volume (None where the source has none, such as an index's volume)."""

StampDays = dict[str, tuple[date, date]]
"""The day-stamp checks' dates, by unit: `"day"` the full and the half trading day; `"week"`,
`"month"`, `"quarter"`, and `"year"` the first and last day to read of that unit's bars."""

_TICKER_PARAMS: Final = {"I:": "indicesTicker", "O:": "optionsTicker"}
"""The range and `/prev` path placeholder of a ticker's class, by its prefix; stocks have none."""

_OHLCV_KEYS: Final = {
    "t": ("o", "h", "l", "c", "v"),
    "window_start": ("open", "high", "low", "close", "volume"),
}
"""The value keys of a raw REST bar (stamped `t`) and of a day flat-file row (`window_start`)."""


def _thanksgiving(year: int) -> date:
    """Return the US Thanksgiving Day of a year: the fourth Thursday of November.

    Args:
        year: The year.

    Returns:
        The date. US exchanges close that day and close early, at 13:00 New York time, on the
        Friday after, by their fixed holiday rules.
    """
    first = date(year, 11, 1)
    return first + timedelta(days=(3 - first.weekday()) % 7 + 21)


def _is_half_day(day: date) -> bool:
    """Return whether US exchanges close early, at 13:00 New York time, on a day.

    By the exchanges' rules: the Friday after Thanksgiving, and July 3 and December 24 when
    they fall Monday to Thursday (on a Friday the next day's holiday moves onto them).

    Args:
        day: The day.

    Returns:
        True for a half trading day.
    """
    if day == _thanksgiving(day.year) + timedelta(days=1):
        return True
    return (day.month, day.day) in {(7, 3), (12, 24)} and day.weekday() < 4


def _months_before(day: date, months: int) -> date:
    """Return the first day of the month `months` before the month of `day`.

    Args:
        day: Any day of the month counted from.
        months: How many months back, 0 for `day`'s own month.

    Returns:
        The first of that month.
    """
    index = day.year * 12 + day.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def _stamp_days(today: date) -> StampDays:
    """Return the day-stamp checks' dates: a recent Thanksgiving week and recent longer periods.

    The half day is the Friday after the most recent Thanksgiving with `half + 3 days <=
    today`: that week's Sunday is over, so every bar it holds, an index week updated around
    the clock included, is complete and its flat files are published. Its days are at most
    about a year and two weeks old, inside every stock and option plan's history; an index
    plan that keeps one year can lack them in the two weeks before the next Thanksgiving week
    ends. The longer periods are anchored to `today` instead, so they are as recent as whole
    periods can be.

    Args:
        today: The run's day, in New York time.

    Returns:
        By unit: `"day"` the Wednesday before Thanksgiving (a full trading day) and the Friday
        after (a half day); `"week"` the Monday of the week before and the Sunday ending
        Thanksgiving week; `"month"` the two whole months before the current month;
        `"quarter"` the two whole quarters before the current quarter; and `"year"` January 1
        of this year to `today`, which holds this year's unfinished bar (none yet before the
        year's first trading day).
    """
    half = _thanksgiving(today.year) + timedelta(days=1)
    if half + timedelta(days=3) > today:
        half = _thanksgiving(today.year - 1) + timedelta(days=1)
    this_month = today.replace(day=1)
    this_quarter = date(today.year, (today.month - 1) // 3 * 3 + 1, 1)
    return {
        "day": (half - timedelta(days=2), half),
        "week": (half - timedelta(days=11), half + timedelta(days=2)),
        "month": (_months_before(today, 2), this_month - timedelta(days=1)),
        "quarter": (_months_before(this_quarter, 6), this_quarter - timedelta(days=1)),
        "year": (date(today.year, 1, 1), today),
    }


@pytest.fixture(scope="session")
def stamp_days() -> StampDays:
    """Return the dates the day-stamp checks read, computed from the run's day.

    Massive's two-year history on Basic plans would push fixed dates out of reach, so the
    checks read the most recent finished Thanksgiving week: its Wednesday is a full trading
    day and its Friday a half day. See `_stamp_days` for every unit.

    Returns:
        The dates, by unit.
    """
    return _stamp_days(datetime.now(_NEW_YORK).date())


def _ticker_param(ticker: str) -> str:
    """Return the range and `/prev` path placeholder of a ticker's class.

    Args:
        ticker: A stock, `O:` option, or `I:` index ticker.

    Returns:
        `"stocksTicker"`, `"optionsTicker"`, or `"indicesTicker"`.
    """
    return _TICKER_PARAMS.get(ticker[:2], "stocksTicker")


def _new_york_day(stamp_ms: int) -> date:
    """Return the New York calendar day of an epoch-millisecond stamp.

    Args:
        stamp_ms: The stamp.

    Returns:
        The day.
    """
    return datetime.fromtimestamp(stamp_ms / 1000, UTC).astimezone(_NEW_YORK).date()


def _stamped(row: Mapping[str, Any]) -> StampedBar:
    """Return a raw REST bar's or day flat-file row's stamp and values.

    Args:
        row: A REST bar as Massive sent it (`t` in ms; `o`, `h`, `l`, `c`, `v`) or a day-file
            row as `flat_file_tickers` reads it (`window_start` in ns; `open`, ... as text).

    Returns:
        The stamp in epoch milliseconds and the open, high, low, close, and volume, None where
        the source has none (the index day file has no volume).
    """
    if "t" in row:
        stamp = int(row["t"])
        keys = _OHLCV_KEYS["t"]
    else:
        stamp = int(str(row["window_start"])) // _MS
        keys = _OHLCV_KEYS["window_start"]
    return stamp, tuple(None if row.get(k) is None else float(str(row[k])) for k in keys)


def _largest_error(left: tuple[float | None, ...], right: tuple[float | None, ...]) -> float | None:
    """Return the largest relative error between two bars' values.

    Open, high, low, and close must be present on both; the volume counts only when both have
    one.

    Args:
        left: One bar's values.
        right: The other's.

    Returns:
        The largest relative error, or None when either lacks an open, high, low, or close.
    """
    errors = []
    for index, (a, b) in enumerate(zip(left, right, strict=False)):
        if a is None or b is None:
            if index < 4:
                return None
            continue
        errors.append(abs(a - b) / max(abs(a), abs(b), 1e-12))
    return max(errors, default=None)


def _match_text(values: tuple[float | None, ...], ranged: list[StampedBar]) -> str:
    """Describe which range bar a bar from another source matches.

    Args:
        values: The other source's values.
        ranged: The range bars it may match.

    Returns:
        `"matches the range bar of <day> (largest relative error <e>)"`, or the closest bar
        when none is within `_SAME_BAR`, or `"no range bar to compare"`.
    """
    scored = [
        (error, _new_york_day(stamp))
        for stamp, other in ranged
        if (error := _largest_error(values, other)) is not None
    ]
    if not scored:
        return "no range bar to compare"
    error, day = min(scored)
    verdict = "matches" if error <= _SAME_BAR else "matches no range bar; closest is"
    return f"{verdict} the range bar of {day} (largest relative error {error:.2e})"


def _read_range(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    ticker: str,
    *,
    span: str,
    bounds: tuple[date, date],
) -> list[dict[str, Any]]:
    """Read one ticker's raw bars of one unit from the aggregates range endpoint, unfiltered.

    Args:
        rest_rows: Reads raw REST rows.
        ticker: The ticker; its prefix selects the class's endpoint.
        span: The unit, such as `"minute"`, `"day"`, or `"week"`.
        bounds: The first and last day, inclusive.

    Returns:
        The bars as Massive sent them, oldest first.
    """
    param = _ticker_param(ticker)
    path = f"/v2/aggs/ticker/{{{param}}}/range/{{multiplier}}/{{timespan}}/{{from}}/{{to}}"
    params = {param: ticker, "multiplier": 1, "timespan": span}
    params |= {"from": bounds[0], "to": bounds[1]}
    return rest_rows(path, params, {"sort": "asc", "limit": 50_000})


def _weekday_after(day: date, step: int) -> date:
    """Return the nearest weekday before (`step=-1`) or after (`step=1`) a day.

    Args:
        day: The day.
        step: -1 or 1.

    Returns:
        The weekday; a holiday on it is not skipped, so a read from it still reaches a
        trading day on the far side of a day off.
    """
    found = day + timedelta(days=step)
    while found.weekday() >= 5:
        found += timedelta(days=step)
    return found


def _week_stamp_days(start: date, end: date) -> tuple[date, date]:
    """Return the New York stamp days that count a week bar as one of the weeks `start`-`end`.

    A week bar may be stamped on its Monday, on the Sunday before, or on the Saturday before
    (00:00 UTC Sunday is 19:00 or 20:00 New York time on Saturday). Counting stamp days from
    the Saturday before `start` to the Friday before `end` keeps all three stamps of every
    week in the span and none of a neighbour's: the week before's latest stamp, its Monday,
    is five days before the first day counted, and the week after's earliest, the Saturday
    before its Monday, is the day before `end`, one day after the last day counted. For the
    weeks 2025-11-17 to 2025-11-30 that is 2025-11-15 (Saturday) to 2025-11-28 (Friday).

    Args:
        start: The Monday of the first week.
        end: The Sunday of the last week.

    Returns:
        The first and last stamp day counted, inclusive.
    """
    return start - timedelta(days=2), end - timedelta(days=2)


def _inside(
    raw: list[dict[str, Any]],
    *,
    ticker: str,
    span: str,
    days: tuple[date, date],
    evidence: Callable[[str], None],
) -> list[dict[str, Any]]:
    """Keep the raw bars whose New York stamp day falls in a span, recording the rest.

    Massive may widen a range to whole periods of its unit (a week from its Sunday), so a
    read can return a period the check did not ask for. Each such bar is recorded as
    "outside the span" with its stamp, and the count line says how many were read and kept.

    Args:
        raw: The bars as Massive sent them.
        ticker: The ticker, for the evidence.
        span: The unit, for the evidence.
        days: The first and last stamp day kept, inclusive.
        evidence: Records one evidence line.

    Returns:
        The bars kept, in their order.
    """
    kept = []
    for bar in raw:
        if days[0] <= _new_york_day(int(bar["t"])) <= days[1]:
            kept.append(bar)
        else:
            evidence(f"{ticker} {span} bar outside the span: {_bar_text(_stamped(bar))}")
    evidence(
        f"{ticker} {span} bars stamped {days[0]} to {days[1]}: {len(kept)} of {len(raw)} read "
        "(any other was outside the span: the range widened to whole periods)"
    )
    return kept


def _bar_text(bar: StampedBar) -> str:
    """Describe a stamped bar: its stamp in UTC and New York time, and its values.

    Args:
        bar: The bar.

    Returns:
        Such as `"t=2025-11-26T05:00:00Z = 00:00 New York (midnight) ohlcv=(...)"`.
    """
    return f"t={_wall_clock(bar[0])} ohlcv={bar[1]}"


@pytest.fixture
def range_bars(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
) -> Callable[[str, str, tuple[date, date]], list[dict[str, Any]]]:
    """Return a function that reads one ticker's raw bars from the aggregates range endpoint.

    Args:
        rest_rows: Reads raw REST rows.

    Returns:
        A function `(ticker, span, bounds)`: the ticker (its prefix selects the class), the
        unit (such as `"minute"`), and the first and last day, inclusive. It returns the bars
        as Massive sent them, unfiltered, oldest first.
    """

    def read(ticker: str, span: str, bounds: tuple[date, date]) -> list[dict[str, Any]]:
        return _read_range(rest_rows, ticker, span=span, bounds=bounds)

    return read


@pytest.fixture
def stamped_bar() -> Callable[[Mapping[str, Any]], StampedBar]:
    """Return a function that turns a raw REST bar or day flat-file row into a stamped bar.

    Returns:
        A function mapping a row to its stamp in epoch milliseconds and its open, high, low,
        close, and volume (see `_stamped`).
    """
    return _stamped


@pytest.fixture
def largest_error() -> Callable[[tuple[float | None, ...], tuple[float | None, ...]], float | None]:
    """Return a function that measures how far apart two bars' values are.

    Returns:
        A function `(left, right)` returning the largest relative error of their open, high,
        low, close, and (when both have one) volume, or None when either lacks a price.
    """
    return _largest_error


@pytest.fixture
def day_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    evidence: Callable[[str], None],
) -> Callable[[str, tuple[date, ...], Mapping[str, Mapping[date, list[StampedBar]]]], list[date]]:
    """Return a function that compares one ticker's 1-day bars across sources.

    It reads the ticker's 1-day bars once from the aggregates range endpoint, unfiltered,
    from the weekday before the first day to the weekday after the last, and reports each
    asked day's range bars with their stamps (UTC and New York time) and values. Beside them
    it reports the same day's bar from every other source the caller read (grouped daily, a
    day flat file, a rebuild from minute bars), with its stamp and which day's range bar it
    matches, with the largest relative error of its open, high, low, close, and volume (a
    volume only when both have one); a match is within 1e-6.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records each evidence line as soon as the report has it.

    Returns:
        A function `(ticker, days, others)`: the ticker (its prefix selects the class), the
        days, and the other sources' bars, by source name and day. It records the evidence
        and returns the days without a range bar.
    """

    def report(
        ticker: str,
        days: tuple[date, ...],
        others: Mapping[str, Mapping[date, list[StampedBar]]],
    ) -> list[date]:
        bounds = (_weekday_after(min(days), -1), _weekday_after(max(days), 1))
        ranged = [
            _stamped(bar) for bar in _read_range(rest_rows, ticker, span="day", bounds=bounds)
        ]
        lines = [f"{ticker} range 1-day bars {bounds[0]} to {bounds[1]}: {len(ranged)}"]
        missing = []
        for day in days:
            own = [bar for bar in ranged if _new_york_day(bar[0]) == day]
            if not own:
                missing.append(day)
                lines.append(f"{ticker} {day} range: no 1-day bar")
            lines.extend(f"{ticker} {day} range 1-day bar: {_bar_text(bar)}" for bar in own)
            for name, by_day in others.items():
                found = by_day.get(day, [])
                if not found:
                    lines.append(f"{ticker} {day} {name}: no bar")
                lines.extend(
                    f"{ticker} {day} {name}: {_bar_text(bar)}; {_match_text(bar[1], ranged)}"
                    for bar in found
                )
        for line in lines:
            evidence(line)
        return missing

    return report


@pytest.fixture
def prev_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    evidence: Callable[[str], None],
) -> Callable[[str], int]:
    """Return a function that compares `/prev` with the range bar of the same day.

    `/prev` serves only the most recent completed trading day, so its stamp is checked on
    whatever day that is when the test runs; a short-day stamp shows only on a run on the
    trading day after a half day. For one ticker it reads the raw `/prev` bar, takes its New
    York calendar day, says whether that day is a half day, reads the range endpoint's 1-day
    bars around it, and reports the stamps (UTC and New York time) and which day's range bar
    `/prev` matches, with the largest relative error.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records each evidence line as soon as the report has it.

    Returns:
        A function taking the ticker (its prefix selects the class). It records the evidence
        and returns how many `/prev` bars it read.
    """

    def report(ticker: str) -> int:
        param = _ticker_param(ticker)
        prevs = rest_rows(f"/v2/aggs/ticker/{{{param}}}/prev", {param: ticker}, {})
        if not prevs:
            evidence(f"{ticker} /prev: no bar")
            return 0
        for prev in prevs:
            bar = _stamped(prev)
            day = _new_york_day(bar[0])
            half = "yes" if _is_half_day(day) else "no"
            evidence(f"{ticker} /prev ({day}, half day: {half}): {_bar_text(bar)}")
            bounds = (_weekday_after(day, -1), _weekday_after(day, 1))
            ranged = [
                _stamped(r) for r in _read_range(rest_rows, ticker, span="day", bounds=bounds)
            ]
            for r in ranged:
                evidence(f"{ticker} range 1-day bar: {_bar_text(r)}")
            evidence(f"{ticker} /prev {_match_text(bar[1], ranged)}")
        return len(prevs)

    return report


@pytest.fixture
def period_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    evidence: Callable[[str], None],
) -> Callable[[str, Mapping[str, tuple[date, date]], Mapping[str, tuple[int, int]]], bool]:
    """Return a function that shows the stamps of one ticker's week or longer bars.

    For each unit asked for it reads the bars over a range of days from the aggregates range
    endpoint, unfiltered, and reports each stamp in UTC and New York time with its values.
    Weeks are read from the Saturday before the first Monday to the last Saturday, and a week
    bar is counted when its New York stamp day falls from that Saturday to the last Friday
    (see `_week_stamp_days`), so a week stamped on its Monday or on the weekend before is
    counted once. Massive may widen a range to whole periods of its unit (a week from its
    Sunday), so each bar stamped outside the span is recorded as such and left out. A
    read the plan refuses (a range beyond the plan's history) is recorded as evidence and
    counts as no bars, so the other units' evidence is kept. Reading it: a bar stamped 00:00
    New York time (01:00 for an index) on its period's first day, or on the Saturday or
    Sunday before a week's Monday, is one the library accepts.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records each evidence line as soon as the report has it.

    Returns:
        A function `(ticker, spans, expected)`: the ticker (its prefix selects the class), the
        first and last day to read of each unit (`"week"`, `"month"`, `"quarter"`, `"year"`),
        and the fewest and most bars expected of each. It records the evidence and returns
        whether every unit returned an expected number of bars.
    """

    def report(
        ticker: str,
        spans: Mapping[str, tuple[date, date]],
        expected: Mapping[str, tuple[int, int]],
    ) -> bool:
        complete = True
        for span, (start, end) in spans.items():
            week = span == "week"
            first = start - timedelta(days=2) if week else start
            last = end - timedelta(days=1) if week else end
            try:
                raw = _read_range(rest_rows, ticker, span=span, bounds=(first, last))
            except WireEntitlementError as error:
                evidence(
                    f"{ticker} {span} bars {first} to {last}: refused, beyond the plan's "
                    f"history? ({error})"
                )
                complete = False
                continue
            inside = _week_stamp_days(start, end) if week else (start, end)
            bars = [
                _stamped(b)
                for b in _inside(raw, ticker=ticker, span=span, days=inside, evidence=evidence)
            ]
            least, most = expected[span]
            complete &= least <= len(bars) <= most
            evidence(f"{ticker} {span} bars counted: {len(bars)} (expected {least} to {most})")
            for bar in bars:
                evidence(f"{ticker} {span} bar: {_bar_text(bar)}")
        return complete

    return report


@pytest.fixture
def week_bars(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    evidence: Callable[[str], None],
) -> Callable[[str, tuple[date, date]], list[dict[str, Any]]]:
    """Return a function that reads one ticker's week bars of whole calendar weeks.

    It reads like `period_bar_report` (from the Saturday before the first Monday to the last
    Saturday), keeps the bars whose New York stamp day falls in `_week_stamp_days`, and
    records each bar outside the span and the count, before returning.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records each evidence line as soon as it is known.

    Returns:
        A function `(ticker, weeks)`: the ticker and the Monday of the first week and the
        Sunday of the last. It returns the raw week bars kept, oldest first.
    """

    def read(ticker: str, weeks: tuple[date, date]) -> list[dict[str, Any]]:
        start, end = weeks
        bounds = (start - timedelta(days=2), end - timedelta(days=1))
        raw = _read_range(rest_rows, ticker, span="week", bounds=bounds)
        return _inside(
            raw, ticker=ticker, span="week", days=_week_stamp_days(start, end), evidence=evidence
        )

    return read


def _new_york_text(nanos: int | None) -> str:
    """Write epoch nanoseconds as New York wall-clock time.

    Args:
        nanos: Nanoseconds since the Unix epoch, or None.

    Returns:
        Such as `"2025-11-28 00:00 EST"`, or `"null"`.
    """
    if nanos is None:
        return "null"
    return f"{datetime.fromtimestamp(nanos / 1e9, UTC).astimezone(_NEW_YORK):%Y-%m-%d %H:%M %Z}"


@pytest.fixture
def library_stamps(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[Callable[[], Table]], str]:
    """Return a function that runs one library bar call and describes the bars' windows.

    A refusal is not an outcome to record: the returned function lets `EntitlementError`
    propagate, so the test fails (its declared plan covers the data) rather than reporting it.

    Args:
        table_rows: Snapshots a table into rows.

    Returns:
        A function taking the call (such as `partial(q.history, start, end)`). It returns
        each row's `Symbol`, `Date`, `TimestampStart`, and `TimestampEnd` (in New York wall
        clock), or the text of the `MassiveError` the call raised.
    """

    def describe(call: Callable[[], Table]) -> str:
        try:
            rows = table_rows(call())
        except EntitlementError:
            raise
        except MassiveError as error:
            return f"MassiveError: {error}"
        if not rows:
            return "no rows"
        return "; ".join(
            f"{r['Symbol']} Date={r['Date']} TimestampStart={_new_york_text(r['TimestampStart'])}"
            f" TimestampEnd={_new_york_text(r['TimestampEnd'])}"
            for r in rows
        )

    return describe
