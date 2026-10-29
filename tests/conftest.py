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
import dataclasses
import faulthandler
import functools
import gc
import gzip
import hashlib
import importlib
import itertools
import json
import os
import re
import sys
import threading
import time
import types
import urllib.parse
import weakref
from collections import Counter
from datetime import UTC, date, datetime, timedelta, tzinfo
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
    sources,
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
    `integration_limits`, `integration_smoke`, `needs`, `settles`, `verifies`, and `requests`. An
    `-m` expression matches a marker by name whatever its arguments, so `-m needs` selects every
    live test with a `needs` marker. Each combination of those markers is tried; a keyword form such
    as `needs(x=1)` is treated as matching nothing, since their arguments are positional.

    Args:
        config: The pytest configuration, for the `-m` expression.

    Returns:
        True if a test with any of those marker sets would be selected.
    """
    expression = Expression.compile(config.option.markexpr or "")
    optional = (_LIMITS, _SMOKE, "needs", "settles", "verifies", "requests")

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

    Returns:
        A function mapping a table to one `{column: value}` dictionary per row.
    """
    dh_arrow = importlib.import_module("deephaven.arrow")

    def to_pylist(table: Table) -> list[dict[str, Any]]:
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
"""An override of the probed plans, as `stocks=starter,currencies=starter,s3`: when it is set,
nothing is probed, and every class it leaves out is Basic."""

_PLANS_CACHE = ".live-plans.json"
"""The file, in the repository root, that keeps the probed plans until the New York day ends. It
names the key and the S3 credentials only by a digest; deleting it makes the next run probe
again, as an upgrade during the day needs."""

_TIERS: Final = {
    "stocks": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "options": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "indices": ("basic", "starter", "advanced", "business"),
    "currencies": ("basic", "starter", "business"),
    "futures": ("basic", "starter", "developer", "advanced", "business"),
}
"""Each class's plans, cheapest first (https://massive.com/pricing). For stocks and options,
`business` is the base Business plan and `business_expansion` the Business plan with the trades
and quotes expansion."""

_BRANCHES: Final[Mapping[str, Mapping[str, frozenset[str]]]] = {
    name: {
        "business": frozenset({"basic", "starter", "business"}),
        "business_expansion": frozenset(_TIERS[name]) - {"business_expansion"},
    }
    for name in ("stocks", "options")
}
"""The plans a stocks or options Business plan covers besides itself. The base Business plan has
no trades, quotes, LULD, or per-second streams, and so is no Developer or Advanced plan; with
the expansion it covers every individual plan (the plan tables in `vendor/massive/websocket/`)."""

_PRICES: Final = {
    ("stocks", "starter"): "$29",
    ("stocks", "developer"): "$79",
    ("stocks", "advanced"): "$199",
    ("stocks", "business"): "$2,499",
    ("stocks", "business_expansion"): "price on request",
    ("options", "starter"): "$29",
    ("options", "developer"): "$79",
    ("options", "advanced"): "$199",
    ("options", "business"): "$1,999",
    ("options", "business_expansion"): "price on request",
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

_HISTORY_YEARS: Final[Mapping[tuple[str, str], int]] = {
    ("stocks", "basic"): 2,
    ("stocks", "starter"): 5,
    ("stocks", "developer"): 10,
    ("stocks", "business"): 5,
    ("options", "basic"): 2,
    ("options", "starter"): 2,
    ("options", "developer"): 4,
    ("options", "business"): 2,
    ("currencies", "basic"): 2,
    ("futures", "basic"): 2,
    ("futures", "starter"): 2,
    ("futures", "developer"): 5,
}
"""How many years of history each plan keeps, from the "Plan History" tables of the vendored
aggregates documentation; a plan not listed keeps all history. A base Business plan is taken to
keep what the cheapest plan it covers keeps, the conservative reading of an undocumented
window."""

_S3 = "s3"
_SOME_BASIC = "some:basic"
"""A requirement met when at least one class is on Basic: the rate-limit test needs one."""

_REQUEST_SECONDS = 13.0
"""Seconds of timeout a live test gets for each REST request it makes to a Basic class."""

_DEFAULT_REQUESTS = 10
"""The REST requests a live test is assumed to make when it declares none (`requests`)."""

_SETTLED: Final = pytest.StashKey[
    dict[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]]
]()
"""Each live test's `settles` items, `verifies` items, and `needs` requirements, by test id, for
the summary."""

_PLANS_KEY: Final = pytest.StashKey["LivePlans"]()
"""The run's plans, once the first live test has asked for them, for the summary."""

_CACHE_VERSION = 2
"""The layout of the plans cache; a cache of another version is ignored and probed again."""


@dataclasses.dataclass(frozen=True)
class LivePlans:
    """The plans a live run takes the API key and the S3 credentials to hold.

    Created once per run by the `live_plans` fixture: probed from Massive, read back from the
    day's cache, or declared in `MASSIVE_LIVE_PLANS`. Immutable, so any thread may read it.

    Attributes:
        tiers: Each class's plan, one of its `_TIERS`, such as `{"stocks": "starter", ...}`.
        s3: Whether the S3 credentials reach at least one flat-file prefix (or, when declared,
            whether `s3` is declared).
        flat_files: The flat-file prefixes whose file of a recent published Monday the
            credentials opened, sorted; None when declared, which lets every prefix be tried.
        source: How the plans were found, for the evidence: probed, cached, or declared.
    """

    tiers: Mapping[str, str]
    s3: bool
    flat_files: tuple[str, ...] | None
    source: str


def _declared_plans(text: str) -> LivePlans:
    """Read a `MASSIVE_LIVE_PLANS` declaration: each class's plan, and whether S3 is declared.

    Args:
        text: The variable's value, such as `"stocks=starter,currencies=starter,s3"`.

    Returns:
        The plans: every class Basic unless named.

    Raises:
        pytest.UsageError: If an entry names an unknown class or plan.
    """
    tiers = dict.fromkeys(_TIERS, "basic")
    s3 = False
    for entry in text.lower().replace(" ", "").split(","):
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
        tiers[name] = tier
    return LivePlans(tiers=tiers, s3=s3, flat_files=None, source=f"declared in {_PLANS_VARIABLE}")


def _digest(secret: str | None) -> str | None:
    """Return a short digest of a credential, so the plans cache never holds the credential.

    Args:
        secret: The API key or the S3 access key id, or None when unset.

    Returns:
        The first 16 hex digits of its SHA-256 digest, or None.
    """
    return None if not secret else hashlib.sha256(secret.encode()).hexdigest()[:16]


def _cached_plans(path: Path, *, owner: Mapping[str, str | None], today: date) -> LivePlans | None:
    """Read the plans cache, if it was written today, in this layout, for the same credentials.

    Args:
        path: The cache file.
        owner: The digests of the API key and the S3 access key id (`_digest`).
        today: The run's New York day; a cache of another day is stale, since a plan bought or
            cancelled during the month changes what the key can do.

    Returns:
        The cached plans, or None when the file is missing, unreadable, of another layout
        version, stale, written for other credentials, or names an unknown plan.
    """
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
        if stored["version"] != _CACHE_VERSION:
            return None
        if stored["owner"] != dict(owner) or stored["day"] != today.isoformat():
            return None
        tiers = {name: str(stored["tiers"][name]) for name in _TIERS}
        flat_files = tuple(str(prefix) for prefix in stored["flat_files"])
        probed = str(stored["probed"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if any(tiers[name] not in _TIERS[name] for name in _TIERS):
        return None
    return LivePlans(
        tiers=tiers,
        s3=bool(flat_files),
        flat_files=flat_files,
        source=f"cached in {_PLANS_CACHE}, probed {probed}",
    )


def _store_plans(
    path: Path, plans: LivePlans, *, owner: Mapping[str, str | None], today: date
) -> None:
    """Write probed plans to the cache, for the later runs of the same New York day.

    Args:
        path: The cache file, replaced whole.
        plans: The plans just probed.
        owner: The digests of the API key and the S3 access key id (`_digest`).
        today: The run's New York day.
    """
    stored = {
        "version": _CACHE_VERSION,
        "owner": dict(owner),
        "day": today.isoformat(),
        "probed": plans.source.removeprefix("probed "),
        "tiers": dict(plans.tiers),
        "flat_files": list(plans.flat_files or ()),
    }
    path.write_text(json.dumps(stored, indent=2) + "\n", encoding="utf-8")


Rows: TypeAlias = "list[dict[str, Any]]"

Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"
"""A raw REST request: the generated path template, its placeholder values, and its query."""


class PlanProbeError(RuntimeError):
    """A plan probe's answer that proves nothing, so the probe cannot name the plan.

    Raised by the plan probes for an empty answer where a liquid ticker always has rows; the
    `live_plans` fixture lets it fail every live test, whose message says to declare the plans
    in `MASSIVE_LIVE_PLANS` instead.
    """


def _entitled(client: RestClient, request: Request) -> Rows | None:
    """Send one probe request and tell a refusal apart from an answer that proves access.

    A refusal is an HTTP 403, a 200 whose envelope says `NOT_AUTHORIZED`, or a snapshot whose
    every row carries an `error` (Massive answers a ticker the plan excludes with such a row).
    An answer proves access only with at least one row: each probe asks for a liquid ticker,
    which always has some.

    Args:
        client: The probe's REST client.
        request: The request.

    Returns:
        The rows without error rows, or None when the request was refused.

    Raises:
        PlanProbeError: When Massive answered without a row, which proves nothing either way.

    Any other failure propagates: a probe that cannot tell must not guess a plan.
    """
    path, path_params, query = request
    try:
        page = client.get(
            source(Transport.REST, path),
            path_params,
            query,
            bucket=_live_bucket(path, path_params),
        )
    except WireEntitlementError:
        return None
    rows = [row for row in page.rows if row.get("error") is None]
    if page.rows and not rows:
        return None
    if not rows:
        msg = f"{path} {dict(path_params)} {dict(query)} answered without a row"
        raise PlanProbeError(msg)
    return rows


def _has_fmv(rows: Rows) -> bool:
    """Tell whether a snapshot carries the fair market value only Business plans see.

    Args:
        rows: The snapshot's rows.

    Returns:
        True when a row has a non-null `fmv`.
    """
    return any(row.get("fmv") is not None for row in rows)


def _trades_and_quotes_plan(
    client: RestClient, paths: tuple[str, str], params: Mapping[str, object]
) -> tuple[bool, bool]:
    """Ask whether the plan includes a ticker's trades and its quotes.

    Args:
        client: The probe's REST client.
        paths: The class's trades and quotes path templates.
        params: The placeholder values of both, naming one liquid ticker.

    Returns:
        Whether the trades and whether the quotes were answered.
    """
    trades, quotes = paths
    return (
        _entitled(client, (trades, params, {"limit": 1})) is not None,
        _entitled(client, (quotes, params, {"limit": 1})) is not None,
    )


def _equity_plan(snapshot: Rows, trades: bool, quotes: bool) -> str:
    """Name a stocks or options plan from its snapshot and its trades and quotes answers.

    Args:
        snapshot: The answered snapshot's rows.
        trades: Whether trades were answered.
        quotes: Whether quotes were answered.

    Returns:
        `"business_expansion"` or `"business"` when the snapshot carries `fmv` (with or
        without trades and quotes), else `"advanced"` (quotes), `"developer"` (trades), or
        `"starter"`.
    """
    if _has_fmv(snapshot):
        return "business_expansion" if quotes or trades else "business"
    return "advanced" if quotes else "developer" if trades else "starter"


def _stocks_plan(client: RestClient) -> str:
    """Probe the Stocks plan: AAPL's snapshot (Starter), its trades, and its quotes.

    Args:
        client: The probe's REST client.

    Returns:
        The plan (`_equity_plan`), or `"basic"` when the snapshot is refused.
    """
    rows = _entitled(
        client, ("/v2/snapshot/locale/us/markets/stocks/tickers", {}, {"tickers": ["AAPL"]})
    )
    if rows is None:
        return "basic"
    paths = ("/v3/trades/{stockTicker}", "/v3/quotes/{stockTicker}")
    return _equity_plan(rows, *_trades_and_quotes_plan(client, paths, {"stockTicker": "AAPL"}))


def _options_plan(client: RestClient) -> str:
    """Probe the Options plan: one contract of SPY's chain (Starter), its trades, and quotes.

    Args:
        client: The probe's REST client.

    Returns:
        The plan (`_equity_plan`), or `"basic"` when the chain is refused.

    Raises:
        PlanProbeError: When the chain's row names no contract to ask for trades and quotes.
    """
    rows = _entitled(
        client,
        ("/v3/snapshot/options/{underlyingAsset}", {"underlyingAsset": "SPY"}, {"limit": 1}),
    )
    if rows is None:
        return "basic"
    details = [row.get("details") for row in rows]
    contracts = [str(d["ticker"]) for d in details if isinstance(d, dict) and d.get("ticker")]
    if not contracts:
        msg = f"SPY's chain names no contract: {rows}"
        raise PlanProbeError(msg)
    paths = ("/v3/trades/{optionsTicker}", "/v3/quotes/{optionsTicker}")
    answers = _trades_and_quotes_plan(client, paths, {"optionsTicker": contracts[0]})
    return _equity_plan(rows, *answers)


def _indices_plan(client: RestClient) -> str:
    """Probe the Indices plan: the snapshot of `I:NDX` (Starter), real-time (Advanced).

    Indices Business has no mark a request can see, so it reads as Advanced.

    Args:
        client: The probe's REST client.

    Returns:
        The plan.
    """
    rows = _entitled(client, ("/v3/snapshot/indices", {}, {"ticker.any_of": ["I:NDX"]}))
    if rows is None:
        return "basic"
    return "advanced" if any(row.get("timeframe") == "REAL-TIME" for row in rows) else "starter"


def _currencies_plan(client: RestClient) -> str:
    """Probe the Currencies plan: the snapshot of `C:EURUSD` (Starter), with `fmv` (Business).

    Args:
        client: The probe's REST client.

    Returns:
        The plan.
    """
    rows = _entitled(
        client,
        ("/v2/snapshot/locale/global/markets/forex/tickers", {}, {"tickers": ["C:EURUSD"]}),
    )
    if rows is None:
        return "basic"
    return "business" if _has_fmv(rows) else "starter"


def _futures_plan(client: RestClient) -> str:
    """Probe the Futures plan: one ES contract's snapshot (Starter) and its trades (Developer).

    Developer and Advanced both include trades and quotes, and Business per venue leaves no
    mark a request can see, so every plan above Starter reads as Developer: the futures tests
    need nothing above it.

    Args:
        client: The probe's REST client.

    Returns:
        `"basic"`, `"starter"`, or `"developer"`.

    Raises:
        PlanProbeError: When the snapshot's row names no contract to ask for trades.
    """
    rows = _entitled(client, ("/futures/v1/snapshot", {}, {"product_code": "ES", "limit": 1}))
    if rows is None:
        return "basic"
    tickers = [row.get("ticker") or (row.get("details") or {}).get("ticker") for row in rows]
    contracts = [str(ticker) for ticker in tickers if ticker]
    if not contracts:
        msg = f"the ES snapshot names no contract: {rows}"
        raise PlanProbeError(msg)
    trades = ("/futures/v1/trades/{ticker}", {"ticker": contracts[0]}, {"limit": 1})
    return "developer" if _entitled(client, trades) is not None else "starter"


_SNAPSHOT_RESET: Final = (dt_time(3, 30), dt_time(4, 30))
"""New York times between which the probe does not run: Massive clears its snapshots at 03:30
and refills them from about 04:00 (`vendor/massive/rest/stocks/snapshots/`), with a margin."""

_PLAN_PROBES: Final[Mapping[str, Callable[[RestClient], str]]] = {
    "stocks": _stocks_plan,
    "options": _options_plan,
    "indices": _indices_plan,
    "currencies": _currencies_plan,
    "futures": _futures_plan,
}
"""Each class's plan probe: one snapshot of a liquid ticker, then its trades and quotes."""


def _reached_prefixes(config: MassiveConfig, day: date) -> tuple[str, ...]:
    """Open one file of every flat-file prefix, to find the prefixes the credentials reach.

    A prefix is reached only when its file of `day` opens: a 403 is a refusal, and a missing
    file proves nothing, so neither counts. Each file is closed as soon as it opens, so only
    the start of its body is read.

    Args:
        config: The live configuration, for the S3 credentials, endpoint, and bucket.
        day: A recent published Monday, whose files exist; a futures prefix reads `cme`.

    Returns:
        The prefixes reached, sorted; none without both S3 credentials.
    """
    if config.s3_access_key_id is None or config.s3_secret_access_key is None:
        return ()
    client = FlatFileClient(
        access_key_id=config.s3_access_key_id,
        secret_access_key=config.s3_secret_access_key,
        endpoint_url=config.s3_endpoint_url,
        bucket=config.s3_bucket,
        max_pool=1,
    )
    reached = []
    try:
        for prefix in sources(Transport.FLAT_FILES):
            venue = "cme" if "{venue}" in prefix else None
            try:
                file = client.open(object_key(prefix, day, venue))
            except WireEntitlementError:
                continue
            if file is not None:
                file.close()
                reached.append(prefix)
    finally:
        client.close()
    return tuple(sorted(reached))


def _probe_plans(config: MassiveConfig, today: date) -> LivePlans:
    """Find the key's plans by asking Massive, a few requests per class and one per prefix.

    The requests take their permits from the run's governor of the key, every class paced as
    Basic, since none is known yet: no class gets more than five requests, so none waits, and
    the later tests' pacing counts them.

    Args:
        config: The live configuration.
        today: The run's New York day.

    Returns:
        The probed plans.

    Raises:
        PlanProbeError: During Massive's daily snapshot reset (`_SNAPSHOT_RESET`, New York
            time), when a snapshot can lack the `fmv` that marks a Business plan; the run
            asks for a declaration rather than cache a lower plan for the day.
    """
    now = datetime.now(_NEW_YORK).time()
    if _SNAPSHOT_RESET[0] <= now < _SNAPSHOT_RESET[1]:
        msg = (
            f"Massive clears and refills its snapshots from {_SNAPSHOT_RESET[0]:%H:%M} to "
            f"{_SNAPSHOT_RESET[1]:%H:%M} New York time, when a Business plan cannot be seen"
        )
        raise PlanProbeError(msg)
    dm = importlib.import_module("deephaven_massive")
    lease = _live_governors().lease(
        api_key=config.api_key,
        base_url=config.rest_base_url,
        basic=frozenset(dm.Subscription),
        label="live plan probe",
        listener=_ignore,
    )
    client = RestClient(
        config.api_key, base_url=config.rest_base_url, max_concurrency=1, governor=lease.governor
    )
    try:
        tiers = {name: probe(client) for name, probe in _PLAN_PROBES.items()}
    finally:
        client.close()
        lease.release()
    prefixes = _reached_prefixes(config, _recent_monday(today))
    return LivePlans(
        tiers=tiers,
        s3=bool(prefixes),
        flat_files=prefixes,
        source=f"probed {datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ}",
    )


def _plans_text(plans: LivePlans) -> str:
    """Describe the run's plans in one line, for the evidence and the summary.

    Args:
        plans: The plans.

    Returns:
        Such as `"stocks=starter options=basic ...; flat files: us_stocks_sip/... (probed ...)"`.
    """
    tiers = " ".join(f"{name}={tier}" for name, tier in plans.tiers.items())
    if plans.flat_files is None:
        files = "declared" if plans.s3 else "none"
    else:
        files = ", ".join(plans.flat_files) or "none"
    return f"{tiers}; flat files: {files} ({plans.source})"


@pytest.fixture(scope="session")
def live_plans(pytestconfig: pytest.Config) -> LivePlans:
    """Return the plans of the run's key: declared, cached today, or probed now.

    `MASSIVE_LIVE_PLANS`, when set, overrides everything and nothing is probed. Otherwise the
    plans come from `.live-plans.json` in the repository root when it was written today (New
    York time), in this layout, for the same API key and S3 access key id, and else from a
    probe: per class, a snapshot of one liquid ticker (Starter), its trades (Developer) and
    quotes (Advanced), and the snapshot's `fmv` field (Business, for stocks, options, and
    currencies); then one file of every flat-file prefix. The probe writes the cache, which
    holds digests, never the credentials. The plans are recorded as evidence under "live
    plans". Only the live tests request this fixture, after the key check, so a unit run never
    probes.

    Args:
        pytestconfig: The pytest configuration, for the repository root and the stash.

    Returns:
        The plans.

    Raises:
        pytest.UsageError: When a probe's answer proves nothing (`PlanProbeError`): the run
            cannot tell the plans and asks for `MASSIVE_LIVE_PLANS`.
    """
    declared = os.environ.get(_PLANS_VARIABLE, "").strip()
    if declared:
        plans = _declared_plans(declared)
    else:
        dm = importlib.import_module("deephaven_massive")
        config: MassiveConfig = dm.MassiveConfig(api_key=_required_env("MASSIVE_API_KEY"))
        today = _today()
        owner = {"api_key": _digest(config.api_key), "s3": _digest(config.s3_access_key_id)}
        path = pytestconfig.rootpath / _PLANS_CACHE
        cached = _cached_plans(path, owner=owner, today=today)
        if cached is None:
            try:
                plans = _probe_plans(config, today)
            except PlanProbeError as error:
                msg = (
                    f"the plan probe cannot tell the key's plans ({error}); declare them in "
                    f"{_PLANS_VARIABLE}, such as stocks=starter,currencies=starter,s3"
                )
                raise pytest.UsageError(msg) from error
            _store_plans(path, plans, owner=owner, today=today)
        else:
            plans = cached
    pytestconfig.stash[_PLANS_KEY] = plans
    _record(pytestconfig, "live plans", _plans_text(plans))
    return plans


def _meets(name: str, held: str, needed: str) -> bool:
    """Tell whether a class's plan covers another plan of the class.

    Plans are ordered cheapest first, each covering the ones before it, except a stocks or
    options Business plan, which covers only the plans `_BRANCHES` names.

    Args:
        name: The class.
        held: The plan the key holds.
        needed: The plan a test needs.

    Returns:
        True when `held` covers `needed`.
    """
    if held == needed:
        return True
    branch = _BRANCHES.get(name, {}).get(held)
    if branch is not None:
        return needed in branch
    tiers = [tier for tier in _TIERS[name] if not _BRANCHES.get(name, {}).get(tier)]
    if needed not in tiers:
        return False
    return held in tiers and tiers.index(held) >= tiers.index(needed)


def _history_start(plans: LivePlans, name: str, today: date) -> date | None:
    """Return the first day a class's plan keeps, by the documented history windows.

    Args:
        plans: The run's plans.
        name: The class.
        today: The run's day.

    Returns:
        The day `_HISTORY_YEARS` years before `today` (February 29 read as the 28th), or None
        for a plan that keeps all history.
    """
    years = _HISTORY_YEARS.get((name, plans.tiers[name]))
    if years is None:
        return None
    return date(today.year - years, today.month, min(today.day, 28))


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
    price = "free" if tier == "basic" else _PRICES[name, tier]
    shown = tier.replace("_", " with the ").replace("expansion", "trades and quotes expansion")
    month = "" if price in {"free", "price on request"} else "/month"
    return f"{name.capitalize()} {shown.capitalize()} ({price}{month})"


def _unmet(requirements: tuple[str, ...], plans: LivePlans) -> list[str]:
    """Return the requirements the run's plans do not meet.

    Args:
        requirements: `"<class>:<plan>"`, `"s3"`, or `"some:basic"` each.
        plans: The run's plans.

    Returns:
        The unmet ones, in order.
    """
    unmet = []
    for requirement in requirements:
        if requirement == _S3:
            met = plans.s3
        elif requirement == _SOME_BASIC:
            met = "basic" in plans.tiers.values()
        else:
            name, _, tier = requirement.partition(":")
            met = _meets(name, plans.tiers[name], tier)
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


def _may_read_basic(item: pytest.Item) -> bool:
    """Return whether a test may read a class on Basic, whose requests are paced.

    A test that runs on a class's Basic plan (`needs("<class>:basic")`) reads that class
    whatever the key's plan, so its requests may wait for a place in the class's five a minute.
    This needs no plan, so collection never probes.

    Args:
        item: The test.

    Returns:
        True when one of its `needs` requirements is a class's Basic plan.
    """
    return any(
        r.endswith(":basic") and r != _SOME_BASIC and r.partition(":")[0] in _TIERS
        for r in _requirements(item)
    )


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


def _basic_subscriptions(plans: LivePlans) -> frozenset[Subscription]:
    """Return the subscriptions on Basic, for the live sessions.

    Args:
        plans: The run's plans.

    Returns:
        Each class on Basic, as a `dm.Subscription`.
    """
    dm = importlib.import_module("deephaven_massive")
    return frozenset(dm.Subscription(name) for name, tier in plans.tiers.items() if tier == "basic")


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
    class's minute is unspent. A test that may read a class on Basic (`_may_read_basic`) gets
    a timeout scaled to its REST requests (its `requests` marker, else 10), since each may wait
    for a place in the class's 5 a minute; on a paid plan the longer limit is only unused. Each
    test's `settles` and `verifies` items and its `needs` are kept for the summary.

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
        verifies = tuple(str(a) for m in item.iter_markers("verifies") for a in m.args)
        settled[item.nodeid] = (settles, verifies, _requirements(item))
        if not _may_read_basic(item):
            continue
        marker = item.get_closest_marker("requests")
        count = int(marker.args[0]) if marker is not None else _DEFAULT_REQUESTS
        current = item.get_closest_marker("timeout")
        base = float(current.args[0]) if current is not None and current.args else 100.0
        item.add_marker(pytest.mark.timeout(base + _REQUEST_SECONDS * count), append=False)


@pytest.fixture(autouse=True)
def _live_tests_need_a_key(request: pytest.FixtureRequest) -> None:
    """Skip a live test under CI, without `MASSIVE_API_KEY`, or when the plans fall short.

    It runs before the test's own fixtures, so a skipped live test touches nothing but the
    plans probe, which the first live test of a run triggers (`live_plans`). `CI` is checked
    too, so a key added to CI's secrets later still never reaches Massive from CI. A test's
    `needs` markers name the plans it needs; one the run's plans do not meet skips, naming the
    plan and its monthly price.

    Args:
        request: The requesting test, for its markers and the `live_plans` fixture.
    """
    if request.node.get_closest_marker(_INTEGRATION) is None:
        return
    if os.environ.get("CI", "").strip():
        pytest.skip("live tests never run in CI")
    _required_env("MASSIVE_API_KEY")
    plans: LivePlans = request.getfixturevalue("live_plans")
    unmet = _unmet(_requirements(request.node), plans)
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

_TIMEFRAME_PHRASE = "doesn't include this data timeframe"
"""The phrase of Massive's refusal of a request beyond the plan's history, in lower case."""

_REQUEST_TARGET = re.compile(r" from GET (\S+) on ")
"""Where a wire error's message names the request's target (path and query)."""

_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
"""An ISO date in a request's target."""

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


def is_timeframe_refusal(error: BaseException) -> bool:
    """Return whether a refusal says the request reaches beyond the plan's history.

    Massive words it "Your plan doesn't include this data timeframe". The phrase is matched in
    the message, never a single word: a request's target is percent-encoded and holds no
    spaces, so the phrase cannot come from a query string.

    Args:
        error: The refusal.

    Returns:
        True for a refusal of the plan's history window.
    """
    return _TIMEFRAME_PHRASE in str(error).lower().replace("\u2019", "'")


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

    A refusal is an `EntitlementError`, or the wire's `WireEntitlementError` (an HTTP 403 or a
    `NOT_AUTHORIZED` envelope). One that names the timeframe ("Your plan doesn't include this
    data timeframe") skips the test, "outside the plan's history", when the day it asked for
    is before the first day the plan keeps by the documented history windows; refused for a
    day inside the window, it fails, since that may be a library bug (`_history_verdict`).
    Any other refusal of a test that declares its
    plans (`needs`) contradicts them, since it runs only when the plans hold them: it is
    recorded as evidence and fails the test. A live test without `needs` (the smoke tier
    handles its own) skips with "not entitled: needs <plan>", the plan taken from the test
    module. Other tests are untouched.

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
        if is_timeframe_refusal(error):
            reason = _history_verdict(error, item.config.stash.get(_PLANS_KEY, None))
            if reason is not None:
                _record(item.config, item.nodeid, f"{reason} ({error})")
                pytest.skip(f"{reason} ({error})")
            _record(item.config, item.nodeid, f"refused inside the plan's history: {error}")
            raise
        requirements = _requirements(item)
        if requirements:
            held = ", ".join(_plan_text(r) for r in requirements)
            _record(item.config, item.nodeid, f"refused although the plans hold {held}: {error}")
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


def _index_symbol(plans: LivePlans) -> str:
    """Return the index a live request reads: `I:SPX`, or `I:NDX` when Indices is on Basic.

    Indices Basic excludes SPX, DJI, RUT, and VIX
    (https://massive.com/knowledge-base/article/are-all-indices-available-through-the-basic-subscription-tier).

    Args:
        plans: The run's plans.

    Returns:
        The index's ticker.
    """
    return "I:NDX" if plans.tiers["indices"] == "basic" else "I:SPX"


@pytest.fixture
def index_symbol(live_plans: LivePlans) -> str:
    """Return the index a live test reads: `I:SPX`, or `I:NDX` when Indices is on Basic.

    Args:
        live_plans: The run's plans.

    Returns:
        The index's ticker (see `_index_symbol`).
    """
    return _index_symbol(live_plans)


@pytest.fixture
def basic_class(live_plans: LivePlans) -> str:
    """Return a class on Basic, preferring the one a run is least likely to need.

    Args:
        live_plans: The run's plans.

    Returns:
        `"indices"`, `"futures"`, `"options"`, `"stocks"`, or `"currencies"`, the first of those
        on Basic; the test is skipped when none is.
    """
    order = ("indices", "futures", "options", "stocks", "currencies")
    basic = [name for name in order if live_plans.tiers[name] == "basic"]
    if not basic:
        pytest.skip("needs at least one class left on Basic")
    return basic[0]


@pytest.fixture
def delayed_key(live_config: MassiveConfig, live_plans: LivePlans) -> str:
    """Return a key with delayed stocks data: `MASSIVE_DELAYED_API_KEY`, or the main key.

    The main key serves when Stocks is on Starter or Developer, the delayed plans.

    Args:
        live_config: The live configuration, for the main key.
        live_plans: The run's plans.

    Returns:
        The key; the test is skipped when there is none.
    """
    key = os.environ.get("MASSIVE_DELAYED_API_KEY", "").strip()
    if key:
        return key
    if live_plans.tiers["stocks"] not in {"starter", "developer"}:
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

    A stream case that passed without a row (a closed market) is counted apart among the
    passes as "acknowledged only": its pass rests on Massive's acknowledgement alone.

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
    quiet = [r for r in passed if ("stream", "acknowledged only") in r.user_properties]
    terminalreporter.write_sep("=", "smoke coverage")
    terminalreporter.write_line(
        f"{len(passed)} passed ({len(quiet)} of them streams acknowledged only, without a row), "
        f"{len(refused)} not entitled, {len(other)} skipped otherwise, {len(failed)} failed"
    )
    for title, reports in (("not entitled", refused), ("skipped", other), ("failed", failed)):
        for report in reports:
            reason = _skip_reason(report) if report.skipped else report.head_line or ""
            terminalreporter.write_line(
                f"{title}: {report.nodeid.split('[')[-1].rstrip(']')}: {reason[:200]}"
            )


def _settled_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    """Print which open questions the run verified or ran, and the plans the rest need.

    A test names the questions whose answer it asserts (`verifies`: it fails unless Massive
    still behaves as the library assumes) and those it only records evidence for (`settles`:
    a person reads the evidence). An item is verified when every test of it ran and passed and
    at least one of them asserts it; it "ran" when they all passed but none asserts it, so its
    evidence still needs reading. One that some of its tests never ran for (a `-k` or path
    selection left them out) is "partly run", since part of its question went unchecked; one
    whose tests were skipped for a plan is listed under the cheapest plan text it needs. A run
    whose paths narrow collection (a `::` test id, or a path other than the configured test
    paths) may not have collected every test of a question, so its items are reported "in this
    selection". The run's plans head the section.

    Args:
        terminalreporter: The terminal reporter, whose statistics hold the outcomes.
        config: The pytest configuration, whose stash holds each test's items and needs and
            the run's plans.
    """
    settled = config.stash.setdefault(_SETTLED, {})
    ran = any(terminalreporter.stats.get(kind) for kind in ("passed", "failed", "skipped"))
    keyed = bool(os.environ.get("MASSIVE_API_KEY", "").strip())
    if not (ran and keyed) or not any(s or v for s, v, _ in settled.values()):
        return
    outcome: dict[str, str] = {}
    for kind in ("passed", "failed", "skipped", "error"):
        for report in terminalreporter.stats.get(kind, []):
            if isinstance(report, pytest.TestReport) and (
                report.when == "call" or kind != "passed"
            ):
                outcome[report.nodeid] = kind
    plans = config.stash.get(_PLANS_KEY, None)
    groups: dict[str, list[str]] = {"verified": [], "ran": []}
    needing: dict[str, list[str]] = {}
    unverified: dict[str, list[str]] = {"failed or skipped": [], "partly run": []}
    ran_items = {i for t, (s, v, _) in settled.items() if t in outcome for i in (*s, *v)}
    for item in sorted(ran_items, key=_item_order):
        every = [(t, v, n) for t, (s, v, n) in settled.items() if item in s or item in v]
        tests = [(t, v, n) for t, v, n in every if t in outcome]
        if len(tests) < len(every):
            unverified["partly run"].append(f"{item} ({len(tests)} of {len(every)} tests ran)")
            continue
        if all(outcome[t] == "passed" for t, _, _ in tests):
            groups["verified" if any(item in v for _, v, _ in tests) else "ran"].append(item)
            continue
        skipped = [n for t, _, n in tests if outcome[t] == "skipped"]
        unmet = [r for needs in skipped for r in (_unmet(needs, plans) if plans else [])]
        if unmet:
            plan = ", ".join(_plan_text(r) for r in dict.fromkeys(unmet))
            needing.setdefault(plan, []).append(item)
        else:
            unverified["failed or skipped"].append(item)
    terminalreporter.write_sep("=", "open questions")
    if plans is not None:
        terminalreporter.write_line(f"plans: {_plans_text(plans)}")
    _write_settled(
        terminalreporter,
        narrowed=_narrowed(config),
        groups=groups,
        needing=needing,
        unverified=unverified,
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
    groups: dict[str, list[str]],
    needing: dict[str, list[str]],
    unverified: dict[str, list[str]],
) -> None:
    """Write the counts and items of the open-questions section of the terminal summary.

    Args:
        terminalreporter: The terminal reporter that writes the summary.
        narrowed: Whether the run's paths may have left some live tests uncollected; its
            items are then reported "in this selection".
        groups: The items whose every collected test passed: `"verified"`, those a test
            asserts, and `"ran"`, those whose evidence only a person can read.
        needing: The items skipped for a plan, by the plan's text.
        unverified: The other items, by why they are not verified.
    """
    total = sum(len(items) for each in (groups, needing, unverified) for items in each.values())
    selection = " in this selection (run the full live suite to verify)" if narrowed else ""
    verified, evidence = groups["verified"], groups["ran"]
    terminalreporter.write_line(
        f"verified{selection} {len(verified)} of {total}: {', '.join(verified)}"
    )
    terminalreporter.write_line(
        f"ran{selection}, evidence to read, {len(evidence)} of {total}: {', '.join(evidence)}"
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
def live_config(live_plans: LivePlans) -> MassiveConfig:
    """Return a `MassiveConfig` with `MASSIVE_API_KEY`, and the S3 credentials when set.

    The classes on Basic are its `rest_basic_subscriptions`, so the library paces their
    requests at 5 a minute from the first one.

    Args:
        live_plans: The run's plans.

    Returns:
        The configuration; the test is skipped when `MASSIVE_API_KEY` is not set.
    """
    dm = importlib.import_module("deephaven_massive")
    config: MassiveConfig = dm.MassiveConfig(
        api_key=_required_env("MASSIVE_API_KEY"),
        rest_basic_subscriptions=_basic_subscriptions(live_plans),
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


def _reaches(plans: LivePlans, prefix: str) -> bool:
    """Tell whether the S3 credentials reach a flat-file prefix, by the plans probe.

    Args:
        plans: The run's plans.
        prefix: The generated prefix.

    Returns:
        True when the probe opened the prefix's file, or when the plans were declared, which
        names no prefix and so lets each be tried.
    """
    return plans.flat_files is None or prefix in plans.flat_files


def _require_prefix(plans: LivePlans, prefix: str) -> None:
    """Skip the test unless the S3 credentials reach a flat-file prefix (`_reaches`).

    Args:
        plans: The run's plans.
        prefix: The generated prefix.
    """
    if not _reaches(plans, prefix):
        pytest.skip(f"needs the {prefix} flat files: the S3 credentials do not reach them")


@pytest.fixture
def flat_files_reached(live_plans: LivePlans) -> Callable[[str], bool]:
    """Return a function that tells whether the S3 credentials reach a flat-file prefix.

    For a test that reads several prefixes and records each one it cannot reach, instead of
    skipping at the first (`flat_file_head` and its kin skip).

    Args:
        live_plans: The run's plans.

    Returns:
        A function of a generated prefix (see `_reaches`).
    """
    return functools.partial(_reaches, live_plans)


@pytest.fixture
def plan_history_start(live_plans: LivePlans) -> Callable[[str], date | None]:
    """Return a function that names the first day a class's plan keeps.

    Args:
        live_plans: The run's plans.

    Returns:
        A function of a class (`"options"`, ...) returning the day, or None for a plan that
        keeps all history (see `_history_start`).
    """
    return lambda name: _history_start(live_plans, name, _today())


_HEAD_ROWS = 1000
"""How many rows `flat_file_head` keeps from the start of a file."""


@pytest.fixture
def flat_file_head(
    live_flat_files: FlatFileClient, live_plans: LivePlans
) -> Callable[[str, date, str | None], FlatFileRows | None]:
    """Return a function that reads the first rows of one flat file, as raw text.

    Only the first block is downloaded and decoded, so even a quotes file costs little.

    Args:
        live_flat_files: The live flat-file client.
        live_plans: The run's plans, for the prefixes the credentials reach.

    Returns:
        A function `(prefix, day, venue)`: the generated prefix (such as
        `"global_forex/quotes_v1"`), the file's day, and the venue that fills a futures
        prefix (`"cme"`; None for every other prefix). It returns the first 1,000 rows, every
        value as the file's text, and the file's undocumented headers, or None when the file
        does not exist; a refusal raises the wire error. It skips the test for a prefix the
        plans probe did not reach (`_require_prefix`).
    """

    def read(prefix: str, day: date, venue: str | None) -> FlatFileRows | None:
        _require_prefix(live_plans, prefix)
        key = object_key(prefix, day, venue)
        return _read_flat_file(live_flat_files, key, prefix, max_rows=_HEAD_ROWS, tickers=None)

    return read


@pytest.fixture
def flat_file_tickers(
    live_flat_files: FlatFileClient, live_plans: LivePlans
) -> Callable[[str, date, Collection[str] | None], FlatFileRows | None]:
    """Return a function that reads every row of some tickers from one flat file, as raw text.

    It skips the test for a prefix the plans probe did not reach (`_require_prefix`).

    Args:
        live_flat_files: The live flat-file client.
        live_plans: The run's plans, for the prefixes the credentials reach.

    Returns:
        A function `(prefix, day, tickers)`: the generated prefix of a file without a venue
        (such as `"global_crypto/minute_aggs_v1"`), the file's day, and the `ticker` values to
        keep, or None for every row. It returns the rows, every value as the file's text, and
        the file's undocumented headers, or None when the file does not exist.
    """

    def read(prefix: str, day: date, tickers: Collection[str] | None) -> FlatFileRows | None:
        _require_prefix(live_plans, prefix)
        wanted = None if tickers is None else sorted(tickers)
        key = object_key(prefix, day)
        return _read_flat_file(live_flat_files, key, prefix, max_rows=None, tickers=wanted)

    return read


@pytest.fixture
def flat_file_sessions(
    live_flat_files: FlatFileClient, live_plans: LivePlans
) -> Callable[[str, date, str], Counter[str | None] | None]:
    """Return a function that counts the `session_end_date` values of a whole futures file.

    The whole file is read, block by block, and only the counts are kept, so a large minute
    file costs no memory. It skips the test for a prefix the plans probe did not reach
    (`_require_prefix`).

    Args:
        live_flat_files: The live flat-file client.
        live_plans: The run's plans, for the prefixes the credentials reach.

    Returns:
        A function `(prefix, day, venue)`: a futures prefix such as
        `"us_futures_{venue}/minute_aggs_v1"`, the file's day, and the venue. It returns how
        many rows hold each `session_end_date` text, or None when the file does not exist; a
        refusal raises the wire error.
    """

    def count(prefix: str, day: date, venue: str) -> Counter[str | None] | None:
        _require_prefix(live_plans, prefix)
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

_MATCH = 1e-9
"""The relative volume error within which a candidate week is the bar's bucket."""

_NEW_YORK = ZoneInfo("America/New_York")

# --- Live test dates ------------------------------------------------------------------------
# Fixed dates age out of a Basic plan's history and land on holidays, so the live tests compute
# theirs from the run's New York day with the US stock exchanges' fixed holiday rules. A closure
# no rule predicts (a national day of mourning) is not known; a test that reads one finds no
# rows there and says so.


def _easter(year: int) -> date:
    """Return Easter Sunday of a year, by the anonymous Gregorian algorithm.

    Args:
        year: The year.

    Returns:
        The date; Good Friday, an exchange holiday, is two days before.
    """
    a, b, c = year % 19, year // 100, year % 100
    d, e = divmod(b, 4)
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 19 * weekday) // 433
    month, day = divmod(h + weekday - 7 * m + 90, 25)
    return date(year, month, (h + weekday - 7 * m + 33 * month + 19) % 32 or day)


def _nth_weekday(year: int, month: int, weekday: int, *, nth: int) -> date:
    """Return the nth given weekday of a month, or the last one for `nth=-1`.

    Args:
        year: The year.
        month: The month, 1 to 12.
        weekday: Monday is 0.
        nth: 1 for the first, 2 for the second, and so on; -1 for the last.

    Returns:
        The date.
    """
    if nth > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (nth - 1))
    last = _months_before(date(year, month, 1), -1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    """Move a fixed-date holiday that falls on a weekend to the weekday the exchanges close.

    Args:
        day: The holiday's date.

    Returns:
        The Friday before a Saturday, the Monday after a Sunday, or the day itself.
    """
    return day + timedelta(days={5: -1, 6: 1}.get(day.weekday(), 0))


def _market_holidays(year: int) -> frozenset[date]:
    """Return the days the US stock exchanges close in a year, by their fixed rules.

    New Year's Day (not moved back into December when it falls on a Saturday), Martin Luther
    King Jr. Day, Washington's Birthday, Good Friday, Memorial Day, Juneteenth (from 2022),
    Independence Day, Labor Day, Thanksgiving, and Christmas, each fixed-date one observed on
    the nearest weekday.

    Args:
        year: The year.

    Returns:
        The closed days.
    """
    new_year = date(year, 1, 1)
    days = {
        new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year,
        _nth_weekday(year, 1, 0, nth=3),
        _nth_weekday(year, 2, 0, nth=3),
        _easter(year) - timedelta(days=2),
        _nth_weekday(year, 5, 0, nth=-1),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, nth=1),
        _thanksgiving(year),
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        days.add(_observed(date(year, 6, 19)))
    return frozenset(days)


def _is_trading_day(day: date) -> bool:
    """Return whether the US stock exchanges open on a day.

    The weekend rule is the library's own (`dates.is_trading_day` of stocks); the exchange
    holidays, which the library does not keep, come from `_market_holidays`, so it describes
    the stock, option, and index calendar. Crypto trades every day and forex on US holidays:
    the days they share with stocks (the recent Monday) are at most a week older for it. A
    futures contract is chosen by its own roll rules, which read no holiday.

    Args:
        day: The day.

    Returns:
        True for a stock trading day.
    """
    dates = importlib.import_module("deephaven_massive._datasets.dates")
    dm = importlib.import_module("deephaven_massive")
    return dates.is_trading_day(dm.AssetClass.STOCKS, day) and day not in _market_holidays(day.year)


def _trading_day_before(day: date) -> date:
    """Return the last trading day before a day.

    Args:
        day: The day.

    Returns:
        The trading day.
    """
    found = day - timedelta(days=1)
    while not _is_trading_day(found):
        found -= timedelta(days=1)
    return found


def _recent_monday(today: date) -> date:
    """Return the most recent Monday that was a trading day and whose flat files are published.

    It is at least two days before `today`, so its files, published by 11:30 New York time the
    next day, exist whatever the hour.

    Args:
        today: The run's New York day.

    Returns:
        The Monday.
    """
    day = today - timedelta(days=2)
    day -= timedelta(days=day.weekday())
    while not _is_trading_day(day):
        day -= timedelta(days=7)
    return day


def _recent_week(today: date) -> tuple[date, date]:
    """Return the most recent ended calendar week whose five weekdays were all trading days.

    Args:
        today: The run's New York day.

    Returns:
        Its Monday and its Sunday, which is before `today`.
    """
    sunday = today - timedelta(days=(today.weekday() + 1) % 7 or 7)
    while not all(_is_trading_day(sunday - timedelta(days=n)) for n in range(2, 7)):
        sunday -= timedelta(days=7)
    return sunday - timedelta(days=6), sunday


def _recent_month(today: date) -> tuple[date, date]:
    """Return the most recent ended calendar month.

    Args:
        today: The run's New York day.

    Returns:
        Its first and last day.
    """
    last = today.replace(day=1) - timedelta(days=1)
    return last.replace(day=1), last


def _today() -> date:
    """Return the run's day in New York, where the US exchanges keep their calendar.

    Returns:
        The day.
    """
    return datetime.now(_NEW_YORK).date()


@pytest.fixture(scope="session")
def recent_monday() -> date:
    """Return the most recent Monday that was a trading day and whose flat files are published.

    For the checks that read one day of a file or of an endpoint that takes a date: a Monday's
    files also follow a weekend, which the weekend checks read. See `_recent_monday`.

    Returns:
        The Monday.
    """
    return _recent_monday(_today())


@pytest.fixture(scope="session")
def recent_week() -> tuple[date, date]:
    """Return the most recent ended calendar week without an exchange holiday.

    Returns:
        Its Monday and Sunday (see `_recent_week`).
    """
    return _recent_week(_today())


@pytest.fixture(scope="session")
def recent_month() -> tuple[date, date]:
    """Return the most recent ended calendar month.

    Returns:
        Its first and last day.
    """
    return _recent_month(_today())


@pytest.fixture(scope="session")
def previous_trading_day() -> date:
    """Return the last trading day before the run's New York day, holidays skipped.

    Returns:
        The day.
    """
    return _trading_day_before(_today())


_HOLIDAY_RULES: Final[Mapping[str, Callable[[int], date]]] = {
    "martin luther king day": lambda year: _nth_weekday(year, 1, 0, nth=3),
    "good friday": lambda year: _easter(year) - timedelta(days=2),
}
"""The exchange holidays a live test reads around, by name, as a function of the year."""


@pytest.fixture(scope="session")
def recent_holiday() -> Callable[[str], date]:
    """Return a function that finds the most recent occurrence of an exchange holiday.

    The occurrence is at least a week before the run's New York day, so the sessions around
    it are over and published.

    Returns:
        A function taking `"martin luther king day"` or `"good friday"` and returning the date.
    """
    today = _today()

    def find(name: str) -> date:
        rule = _HOLIDAY_RULES[name]
        day = rule(today.year)
        return day if day <= today - timedelta(days=7) else rule(today.year - 1)

    return find


_MONTH_CODES = "FGHJKMNQUVXZ"
"""The futures month codes, January to December."""

_CONTRACT_MONTHS: Final = {"ES": "HMUZ", "ZC": "HKNUZ"}
"""The contract months of the futures products the live tests read: the E-mini S&P 500 (CME)
and corn (CBOT)."""


def _front_contract(product: str, day: date) -> str:
    """Return a product's front contract on a day: the one most of its trading is in.

    Trading leaves a contract on its roll day (`_roll_day`), and the front contract is the
    first listed one whose roll day is after `day`.

    Args:
        product: `"ES"` or `"ZC"`.
        day: The day.

    Returns:
        Such as `"ESZ6"` on 2026-10-29, `"ESH7"` on 2026-12-10, or `"ZCK6"` on 2026-03-02.
    """
    months = [_MONTH_CODES.index(code) + 1 for code in _CONTRACT_MONTHS[product]]
    listed = [(year, month) for year in (day.year, day.year + 1) for month in months]
    year, month = next((y, m) for y, m in listed if _roll_day(product, y, m) > day)
    return f"{product}{_MONTH_CODES[month - 1]}{year % 10}"


@pytest.fixture
def front_contract() -> Callable[[str, date], str]:
    """Return a function that names a futures product's front contract on a day.

    Returns:
        A function `(product, day)`, the product `"ES"` or `"ZC"` (see `_front_contract`).
    """
    return _front_contract


_MARKETS: Final = {
    "stocks": ("market",),
    "options": ("market",),
    "indices": ("indicesGroups", "s_and_p"),
    "fx": ("currencies", "fx"),
    "crypto": ("currencies", "crypto"),
}
"""Where `/v1/marketstatus/now` gives each market's status; options trade the stock session, and
S&P indices are computed in it."""


@pytest.fixture
def market_status(live_rest: RestClient, evidence: Callable[[str], None]) -> Callable[[str], str]:
    """Return a function that reads one market's status from Massive.

    A stream test asserts that Massive acknowledged its topics whatever the hour, and asserts
    rows only while their market is open: it skips only when this status says the market is
    not open. The stocks, options, indices, forex, and crypto statuses come from
    `/v1/marketstatus/now`, which belongs to no subscription; the futures status is the E-mini
    S&P 500's `market_event` from `/futures/v1/market-status?product_code=ES`, which every
    Futures plan includes.

    Args:
        live_rest: The live REST client.
        evidence: Records the status read.

    Returns:
        A function taking `"stocks"`, `"options"`, `"indices"`, `"fx"`, `"crypto"`, or
        `"futures"`, and returning Massive's status: `"open"`, `"closed"`, `"extended-hours"`,
        and so on, and for futures `"open"`, `"pause"`, `"close"`, and so on.
    """

    def status(market: str) -> str:
        if market == "futures":
            rows = live_rest.get(
                source(Transport.REST, "/futures/v1/market-status"),
                {},
                {"product_code": "ES"},
                bucket="futures",
            ).rows
            found = str(rows[0].get("market_event")) if rows else "unknown"
        else:
            page = live_rest.get(
                source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other"
            )
            body: object = page.body
            for step in _MARKETS[market]:
                body = body.get(step) if isinstance(body, dict) else None
            found = str(body)
        evidence(f"market status of {market}: {found}")
        return found

    return status


@pytest.fixture
def wait_acked(
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> Callable[[MassiveSession, int, float], int]:
    """Return a function that waits until a session's topics are acknowledged.

    Massive acknowledges a topic whether or not its market is open, so a stream test asserts
    the acknowledgement, which a closed market cannot hide, before it waits for any row.

    Returns:
        A function `(session, topics, deadline)`, the deadline a `time.monotonic()` value: it
        returns the sum of `AckedTopics` over the session's connections once it reaches
        `topics`, once one of the session's streams has `FAILED`, or once the deadline has
        passed, whichever comes first.
    """

    def wait(session: MassiveSession, topics: int, deadline: float) -> int:
        pause = threading.Event()  # never set; only paces the polling
        while True:
            acked = sum(int(r["AckedTopics"] or 0) for r in table_rows(session.connections()))
            failed = any(r["State"] == "FAILED" for r in table_rows(session.streams()))
            if acked >= topics or failed or time.monotonic() >= deadline:
                return acked
            pause.wait(0.25)

    return wait


def _week_candidates(stamp_ms: int, zone: tzinfo) -> list[tuple[str, int, bool]]:
    """Return the candidate starts of the seven days a week bar may cover.

    Args:
        stamp_ms: The bar's stamp, in epoch milliseconds.
        zone: The zone of the class's day, whose Monday 00:00 the library starts a week at:
            UTC for crypto and forex, New York for stocks.

    Returns:
        `(label, start_ms, library)` triples: 00:00 UTC, 00:00 New York time, and 17:00 New
        York time (the forex trading day's start) of each day from three days before the
        stamp's UTC day to three days after; `library` marks Monday 00:00 in `zone`.
    """
    day = datetime.fromtimestamp(stamp_ms / 1000, UTC).date()
    found = []
    for offset in range(-3, 4):
        start = day + timedelta(days=offset)
        for hour, where, name in (
            (0, UTC, "UTC"),
            (0, _NEW_YORK, "New York"),
            (17, _NEW_YORK, "New York"),
        ):
            moment = datetime.combine(start, dt_time(hour), where)
            library = start.weekday() == 0 and hour == 0 and where == zone
            found.append(
                (f"{start:%a %m-%d} {hour:02d}:00 {name}", int(moment.timestamp()) * 1000, library)
            )
    return found


WeekReport = tuple[list[str], int, list[str]]
"""What `week_bar_report` found: the evidence lines, how many week bars it checked, and the
stamps of those whose closest candidate window is not the library's week."""


@pytest.fixture
def week_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    recent_week: tuple[date, date],
) -> Callable[[str, str, str], WeekReport]:
    """Return a function that shows where Massive starts a week bar, from raw REST bars.

    It reads the week bars and the hour bars of one ticker over four calendar weeks, the
    recent week without a holiday and the three before it, from the aggregates range endpoint,
    unfiltered. For each week bar it reports its stamp, its volume, the first and last hour bar
    of the seven days from its stamp, and the candidate seven-day windows whose hour-bar volume
    comes closest to the bar's: windows starting at 00:00 UTC, and at 17:00 New York time, on
    each day from three days before the stamp to three after. A candidate within a relative
    error of 1e-9 is marked `MATCH`: that is the bucket Massive uses, wherever the bar is
    stamped. A bar of one of the four weeks (stamped from the Saturday before its Monday to
    the Monday) is checked by comparison, not by an absolute tolerance: the window from Monday
    00:00 in the class's day zone, the start the library gives every week, must be the closest
    candidate, ties allowed. For stocks, which do not trade at weekends, every start from
    Friday evening to Monday morning ties, so the check can only catch a week that starts in
    the middle of the week.

    Args:
        rest_rows: Reads raw REST rows.
        recent_week: The last of the four weeks.

    Returns:
        A function `(ticker_param, ticker, zone)`: the path placeholder of the class (such as
        `"stocksTicker"`), the ticker, and the class's day zone, `"UTC"` (crypto, forex) or
        `"New York"` (stocks). It returns the evidence lines, how many bars it checked, and the
        stamps of the checked bars whose closest window is not the library's week.
    """
    first, last = recent_week[0] - timedelta(days=21), recent_week[1]

    def report(ticker_param: str, ticker: str, zone: str) -> WeekReport:
        day_zone = UTC if zone == "UTC" else _NEW_YORK
        path = (
            f"/v2/aggs/ticker/{{{ticker_param}}}/range/{{multiplier}}/{{timespan}}/{{from}}/{{to}}"
        )

        def bars(span: str) -> list[dict[str, Any]]:
            params = {ticker_param: ticker, "multiplier": 1, "timespan": span}
            params |= {"from": first - timedelta(days=7), "to": last + timedelta(days=7)}
            return rest_rows(path, params, {"sort": "asc", "limit": 50_000})

        weeks, hours = bars("week"), bars("hour")

        def volume(start_ms: int) -> float:
            return float(sum(h["v"] for h in hours if start_ms <= h["t"] < start_ms + _WEEK_MS))

        lines = [f"{ticker} {first} to {last}: {len(weeks)} week bars, {len(hours)} hour bars"]
        checked, misses = 0, []
        for week in weeks:
            stamp = int(week["t"])
            target = float(week["v"])
            inside = [h for h in hours if stamp <= h["t"] < stamp + _WEEK_MS]
            scored = sorted(
                (abs(volume(start) - target) / max(abs(target), 1e-12), label, monday)
                for label, start, monday in _week_candidates(stamp, day_zone)
            )
            best = ", ".join(
                f"{label} err={error:.2e}{' MATCH' if error <= _MATCH else ''}"
                for error, label, _ in scored[:3]
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
            stamp_day = datetime.fromtimestamp(stamp / 1000, UTC).date()
            if first - timedelta(days=2) <= stamp_day <= last - timedelta(days=6):
                checked += 1
                closest = scored[0][0]
                if not any(library and error <= closest + _MATCH for error, _, library in scored):
                    misses.append(_instant_text(stamp * _MS))
        return lines, checked, misses

    return report


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
def minute_span_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    evidence: Callable[[str], None],
) -> Callable[[str, date], int]:
    """Return a function that records where one day's minute bars of a ticker start and end.

    On a half day the regular session ends at 13:00 New York time (13:15 for index options),
    so the last minute bar shows whether a source cuts the day there.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records the line.

    Returns:
        A function `(ticker, day)`: the ticker (its prefix selects the class) and the day. It
        reads that day's minute bars from the aggregates range endpoint, records their count
        and the first and last stamp in UTC and New York time, and whether the day is a half
        day, and returns the count.
    """

    def report(ticker: str, day: date) -> int:
        bars = _read_range(rest_rows, ticker, span="minute", bounds=(day, day))
        half = "yes" if _is_half_day(day) else "no"
        if not bars:
            evidence(f"{ticker} {day} (half day: {half}) minute bars: none")
            return 0
        first, last = _wall_clock(int(bars[0]["t"])), _wall_clock(int(bars[-1]["t"]))
        evidence(
            f"{ticker} {day} (half day: {half}) {len(bars)} minute bars: first t={first}, "
            f"last t={last}"
        )
        return len(bars)

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


def _half_days(year: int) -> list[date]:
    """Return the days of a year US exchanges close early, at 13:00 New York time.

    By the exchanges' rules: the Friday after Thanksgiving, and July 3 and December 24 when
    they fall Monday to Thursday (on a Friday the next day's holiday moves onto them).

    Args:
        year: The year.

    Returns:
        The half trading days, in date order.
    """
    found = [date(year, 7, 3), _thanksgiving(year) + timedelta(days=1), date(year, 12, 24)]
    return [day for day in found if day.weekday() < 4 or day.month == 11]


def _is_half_day(day: date) -> bool:
    """Return whether US exchanges close early, at 13:00 New York time, on a day.

    Args:
        day: The day.

    Returns:
        True for a half trading day (see `_half_days`).
    """
    return day in _half_days(day.year)


def _months_before(day: date, months: int) -> date:
    """Return the first day of the month `months` before the month of `day`.

    Args:
        day: Any day of the month counted from.
        months: How many months back, 0 for `day`'s own month, negative for later months.

    Returns:
        The first of that month.
    """
    index = day.year * 12 + day.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def _stamp_days(today: date) -> StampDays:
    """Return the day-stamp checks' dates: a recent half day's week and recent longer periods.

    The half day is the most recent one (the Friday after Thanksgiving, July 3, or December
    24) whose calendar week ended before `today`, so every bar that week holds, an index week
    updated around the clock included, is complete and its flat files are published. With
    three half days a year it is at most about eleven months old (a December 24 half day, read
    until the next Thanksgiving week ends), inside every plan's two years of history.
    The longer periods are anchored to `today` instead, so they are as recent as whole periods
    can be.

    Args:
        today: The run's day, in New York time.

    Returns:
        By unit: `"day"` the trading day before the half day (a full trading day) and the half
        day; `"week"` the Monday of the week before the half day's and the Sunday ending the
        half day's week; `"month"` the two whole months before the current month; `"quarter"`
        the two whole quarters before the current quarter; and `"year"` January 1 of this year
        to `today`, which holds this year's unfinished bar (none yet before the year's first
        trading day).
    """
    half = max(
        day
        for year in (today.year - 1, today.year)
        for day in _half_days(year)
        if day + timedelta(days=6 - day.weekday()) < today
    )
    monday = half - timedelta(days=half.weekday())
    this_month = today.replace(day=1)
    this_quarter = date(today.year, (today.month - 1) // 3 * 3 + 1, 1)
    return {
        "day": (_trading_day_before(half), half),
        "week": (monday - timedelta(days=7), monday + timedelta(days=6)),
        "month": (_months_before(today, 2), this_month - timedelta(days=1)),
        "quarter": (_months_before(this_quarter, 6), this_quarter - timedelta(days=1)),
        "year": (date(today.year, 1, 1), today),
    }


@pytest.fixture(scope="session")
def stamp_days() -> StampDays:
    """Return the dates the day-stamp checks read, computed from the run's day.

    The checks read the most recent half day whose week has ended, and the trading day before
    it, so a full and a short day are compared and both stay inside every plan's history. See
    `_stamp_days` for every unit.

    Returns:
        The dates, by unit.
    """
    return _stamp_days(_today())


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
    endpoint, unfiltered, and reports each stamp in UTC and New York time with its values. Weeks are
    read from the Saturday before the first Monday to the last Saturday, and a week bar is counted
    when its New York stamp day falls from that Saturday to the last Friday (see
    `_week_stamp_days`), so a week stamped on its Monday or on the weekend before is counted once.
    Massive may widen a range to whole periods of its unit (a week from its Sunday), so each bar
    stamped outside the span is recorded as such and left out. A read the plan refuses is recorded
    and the other units are still read; the first refusal is then raised, for the run to judge (a
    day outside the plan's history skips the test). Reading it: a bar stamped 00:00 New York time
    (01:00 for an index) on its period's first day, or on the Saturday or Sunday before a week's
    Monday, is one the library accepts.

    Args:
        rest_rows: Reads raw REST rows.
        evidence: Records each evidence line as soon as the report has it.

    Returns:
        A function `(ticker, spans, expected)`: the ticker (its prefix selects the class), the
        first and last day to read of each unit (`"week"`, `"month"`, `"quarter"`, `"year"`),
        and the fewest and most bars expected of each. It records the evidence and returns
        whether every unit returned an expected number of bars; a refused read raises its
        `WireEntitlementError` once the other units are read.
    """

    def report(
        ticker: str,
        spans: Mapping[str, tuple[date, date]],
        expected: Mapping[str, tuple[int, int]],
    ) -> bool:
        complete = True
        refused: list[WireEntitlementError] = []
        for span, (start, end) in spans.items():
            week = span == "week"
            first = start - timedelta(days=2) if week else start
            last = end - timedelta(days=1) if week else end
            try:
                raw = _read_range(rest_rows, ticker, span=span, bounds=(first, last))
            except WireEntitlementError as error:
                evidence(f"{ticker} {span} bars {first} to {last}: refused ({error})")
                refused.append(error)
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
        if refused:
            raise refused[0]
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


# --- Corporate actions the live tests read ----------------------------------------------------
# A fixed split or dividend ages out of a Basic plan's two years of history, so the tests find
# recent ones through the library, once per run.

_EVENT_DAYS = 365
"""How many days back the search for a recent split or special dividend reaches."""

_EVENT_SETTLED = 3
"""Days an event must lie behind the run's day, so the bars after it exist."""

_EVENT_CANDIDATES = 5
"""How many recent events are kept, newest first; a test takes the first one that has bars."""

_PLAIN_TICKER = re.compile(r"[A-Z]{1,5}")
"""A plain listed ticker: no class suffix, warrant, or unit, which may lack bars. ETFs, leveraged
ones included, have plain tickers too, so a candidate is also checked to be a common stock
(`_common_stocks`)."""

_EVENTS: dict[str, list[Any]] = {}
"""The events found so far in the run, by kind; the first test that needs one fills it."""


@dataclasses.dataclass(frozen=True)
class Split:
    """A recent forward split of a US-listed stock, found through `m.stocks.splits`.

    Attributes:
        symbol: The stock.
        before: The last trading day before the split, the last one on the old basis.
        executed: The execution date, the first trading day on the new basis.
        ratio: `SplitTo / SplitFrom`, such as 10.0 for a 10-for-1 split.
    """

    symbol: str
    before: date
    executed: date
    ratio: float


@dataclasses.dataclass(frozen=True)
class Dividend:
    """A recent special cash dividend of a US-listed stock, found through `m.stocks.dividends`.

    Attributes:
        symbol: The stock.
        ex_date: The ex-dividend date.
        amount: The cash amount per share, as declared.
    """

    symbol: str
    ex_date: date
    amount: float


def _event_window() -> tuple[date, date]:
    """Return the first and last day the event searches read.

    Returns:
        From a year before the run's day to three days before it.
    """
    today = _today()
    return today - timedelta(days=_EVENT_DAYS), today - timedelta(days=_EVENT_SETTLED)


@pytest.fixture
def recent_splits(
    table_rows: Callable[[Table], list[dict[str, Any]]], evidence: Callable[[str], None]
) -> Callable[[MassiveSession], list[Split]]:
    """Return a function that finds the year's most recent forward splits of common stocks.

    A candidate is a forward split with a ratio of at least 2, of a plain ticker, executed on a
    trading day of the last year and at least three days ago, and with no later split of any
    kind: adjusted bars restate the prices before every later split too, so one would hide
    the ratio. The first call of a run reads every split of the year and after it (one REST
    request, `_all_splits`), then checks the newest candidates one by one against the
    reference tickers until five common stocks are found (a request each; an ETF, leveraged
    or not, is left out); later calls reuse them. Each call records them as evidence.

    Args:
        table_rows: Snapshots a table into rows.
        evidence: Records the splits found.

    Returns:
        A function taking the test's session and returning the splits, newest first; empty
        when the year had none.
    """

    def find(session: MassiveSession) -> list[Split]:
        if "splits" not in _EVENTS:
            splits = _all_splits(session, table_rows)
            start, end = _event_window()
            found = []
            for row in splits:
                executed = date.fromisoformat(str(row["ExecutionDate"]))
                ratio = float(row["SplitTo"]) / float(row["SplitFrom"])
                symbol = str(row["Symbol"])
                if not (
                    row["AdjustmentType"] == "forward_split"
                    and start <= executed <= end
                    and ratio >= 2
                    and _PLAIN_TICKER.fullmatch(symbol)
                    and _is_trading_day(executed)
                    and not _split_after(splits, symbol, executed)
                ):
                    continue
                found.append(Split(symbol, _trading_day_before(executed), executed, ratio))
            _EVENTS["splits"] = _common_stocks(session, found, table_rows)
        evidence(f"recent forward splits: {_EVENTS['splits']}")
        return list(_EVENTS["splits"])

    return find


@pytest.fixture
def special_dividends(
    table_rows: Callable[[Table], list[dict[str, Any]]], evidence: Callable[[str], None]
) -> Callable[[MassiveSession], list[Dividend]]:
    """Return a function that finds the year's most recent special cash dividends.

    A candidate is a dividend classed `special`, of a plain ticker, whose ex-dividend date is
    a trading day of the last year at least three days ago, with no split of any kind from a
    week before it on (`_all_splits`): a later split would restate the bars of every day the
    check reads. The first call of a run reads up to 50 such dividends, newest first, through
    the session's `stocks.dividends` (one REST request), then checks them one by one against
    the reference tickers until five common stocks are found; later calls reuse them. Each
    call records them as evidence.

    Args:
        table_rows: Snapshots a table into rows.
        evidence: Records the dividends found.

    Returns:
        A function taking the test's session and returning the dividends, newest first; empty
        when the year had none.
    """

    def find(session: MassiveSession) -> list[Dividend]:
        if "dividends" not in _EVENTS:
            dm = importlib.import_module("deephaven_massive")
            splits = _all_splits(session, table_rows)
            start, end = _event_window()
            query = session.stocks.dividends(
                start=start,
                end=end,
                sort="ex_dividend_date",
                order=dm.Order.DESC,
                limit=50,
                filters={"distribution_type": "special"},
            )
            found = []
            for row in table_rows(query.fetch()):
                ex_date = date.fromisoformat(str(row["ExDividendDate"]))
                symbol = str(row["Symbol"])
                if (
                    _PLAIN_TICKER.fullmatch(symbol)
                    and _is_trading_day(ex_date)
                    and not _split_after(splits, symbol, ex_date - timedelta(days=8))
                ):
                    found.append(Dividend(symbol, ex_date, float(row["CashAmount"] or 0.0)))
            _EVENTS["dividends"] = _common_stocks(session, found, table_rows)
        evidence(f"recent special cash dividends: {_EVENTS['dividends']}")
        return list(_EVENTS["dividends"])

    return find


def _requested(error: BaseException) -> tuple[str | None, date | None]:
    """Read the class and the earliest day a refused REST request asked for.

    The wire error's message reads `"<what> from GET <target> on <host>...: <detail>"`; the
    class comes from the target's path (a ticker's prefix, or a word such as `/futures/`) and
    the day from the ISO dates in the target.

    Args:
        error: The refusal.

    Returns:
        The class (`"stocks"`, `"options"`, ...; None for data of no class) and the earliest
        ISO date the target names (None when it names none).
    """
    found = _REQUEST_TARGET.search(str(error))
    if found is None:
        return None, None
    target = urllib.parse.unquote(found.group(1))
    path = target.partition("?")[0]
    days = [date.fromisoformat(text) for text in _ISO_DATE.findall(target)]
    if path.startswith("/futures/"):
        name: str | None = "futures"
    else:
        prefixed = [
            _TICKER_CLASSES[part[:2]] for part in path.split("/") if part[:2] in _TICKER_CLASSES
        ]
        name = (
            prefixed[0]
            if prefixed
            else next(
                (n for word, n in _PATH_CLASSES if word in path),
                "stocks"
                if "/aggs/ticker/" in path or path.startswith(("/v3/trades/", "/v3/quotes/"))
                else None,
            )
        )
    return name, min(days, default=None)


def _history_verdict(error: BaseException, plans: LivePlans | None) -> str | None:
    """Decide whether a timeframe refusal is outside the plan's documented history.

    A refusal of a day before the first day the plan keeps (`_history_start`) is expected, and
    the test skips. A refusal of a day the plan keeps, of a request that names no day, of data
    of no class, or of a plan that keeps all history contradicts the documented window: it may
    be a library bug, so the test fails.

    Args:
        error: The timeframe refusal.
        plans: The run's plans, or None when no live test has resolved them.

    Returns:
        The skip reason when the refused day is outside the window, else None.
    """
    name, day = _requested(error)
    if plans is None or name is None or day is None or name not in _TIERS:
        return None
    first = _history_start(plans, name, _today())
    if first is None or day >= first:
        return None
    return f"outside the plan's history: {day} is before {first}, the first day {name} keeps"


def _roll_day(product: str, year: int, month: int) -> date:
    """Return the day trading leaves a contract for the next one.

    An E-mini S&P 500 contract expires on its month's third Friday, and the market rolls to
    the next contract eight days before, on the Thursday of the week before. A corn contract's
    holders must take delivery from its first notice day, the last business day of the month
    before its month, so trading leaves it by then.

    Args:
        product: `"ES"` or `"ZC"`.
        year: The contract's year.
        month: The contract's month.

    Returns:
        The roll day; on and after it the next contract is the front one.
    """
    if product == "ES":
        return _nth_weekday(year, month, 4, nth=3) - timedelta(days=8)
    return _trading_day_before(date(year, month, 1))


def _all_splits(
    session: MassiveSession, table_rows: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    """Read every split of every kind from the event window's start on, once per run.

    Args:
        session: The test's session.
        table_rows: Snapshots a table into rows.

    Returns:
        The rows of `stocks.splits`, newest first, announced future ones included.
    """
    if "all splits" not in _EVENTS:
        dm = importlib.import_module("deephaven_massive")
        query = session.stocks.splits(
            start=_event_window()[0], sort="execution_date", order=dm.Order.DESC, limit=5_000
        )
        _EVENTS["all splits"] = table_rows(query.fetch())
    return list(_EVENTS["all splits"])


def _split_after(splits: list[dict[str, Any]], symbol: str, day: date) -> bool:
    """Tell whether a stock has a split of any kind executed after a day.

    Args:
        splits: Every split of the window (`_all_splits`).
        symbol: The stock.
        day: The day.

    Returns:
        True when one of the stock's splits executes after `day`.
    """
    return any(
        row["Symbol"] == symbol and date.fromisoformat(str(row["ExecutionDate"])) > day
        for row in splits
    )


def _common_stocks(
    session: MassiveSession,
    events: list[Any],
    table_rows: Callable[[Table], list[dict[str, Any]]],
) -> list[Any]:
    """Keep the events of common stocks, newest first, up to five.

    Each candidate's ticker is looked up in the reference tickers as a common stock (one
    request each); an ETF, leveraged or not, a fund, or a warrant answers no row and is left
    out.

    Args:
        session: The test's session.
        events: The candidate events, newest first; each has a `symbol`.
        table_rows: Snapshots a table into rows.

    Returns:
        The first five events whose ticker is a common stock.
    """
    dm = importlib.import_module("deephaven_massive")
    kept: list[Any] = []
    for event in events:
        if len(kept) == _EVENT_CANDIDATES:
            break
        query = session.reference.tickers(symbols=event.symbol, ticker_type=dm.TickerType.CS)
        if table_rows(query.fetch()):
            kept.append(event)
    return kept
