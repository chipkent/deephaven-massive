"""Test-suite configuration: the embedded Deephaven JVM and the shared fixtures.

The embedded server starts in `pytest_configure`, before collection, so test modules can import
`deephaven` at module level, exactly as the library does. This module itself is imported
before `pytest_configure` runs, so it reaches `deephaven` only through `importlib` inside
fixtures, never through a module-level import.

Test modules never import from this file; they request its fixtures by name. Fakes from
`tests/_fakes/` are handed to tests through thin fixtures defined here.

The live integration tests (`tests/**/test_*_integration.py`) get their own fixtures here too:
the marker checks, the key skip, the Basic plan's pacing (the skip of a test that would wait
more than a minute, and a terminal line for each wait), live sessions and wire clients, raw
flat-file and REST readers, the run's plans, dates, and corporate actions, and the `evidence`
recorder, which writes `live-evidence.log` as it goes and closes the terminal summary. The
logic behind the plans, pacing, dates, contracts, and corporate actions lives in
`tests/_live/`, where it is unit-tested against the fakes; the fixtures here only call it.

The soak tests (marker `soak`) run the library against the fakes for `MASSIVE_SOAK_SECONDS`
each, under load: a fast fake feed, many streams, and churn. Here they get a time limit scaled
to that run instead of the suite's 60 s and faulthandler's 120 s, and the `soak` fixture, which
paces their rounds and fails one whose memory or threads keep growing. `docs/dev/soak-tests.md`
says how to run them, overnight included.
"""

from __future__ import annotations

import atexit
import contextlib
import faulthandler
import functools
import gc
import gzip
import importlib
import inspect
import os
import re
import sys
import threading
import time
import types
import weakref
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, TypeAlias, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
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
from tests._live import (
    calendar,
    classes,
    contracts,
    events,
    pacing,
    parallel,
    plans,
    probe,
    streams,
    summary,
    ticker_lists,
    week_windows,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterator, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive import MassiveConfig, MassiveSession
    from deephaven_massive._enums import Subscription
    from deephaven_massive._ingest.rate_limits import GovernorRegistry
    from tests._fakes.clock import FakeClock
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.stream import FakeStreamTransport
    from tests._fakes.ws import FakeMassiveServer, TradeFeed
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

    The first part is xdist's own condition for starting its distributed session: a
    distribution mode other than `no` and at least one worker (`-n 0` leaves none), in a run
    that is more than `--collect-only`, under which xdist stays out entirely. Under `--fixtures`
    and `--fixtures-per-test` xdist does start its workers, but pytest lists the fixtures by
    collecting the tests in the main process itself, which imports the test modules and so
    needs a JVM; such a run counts as no controller here. Workers carry `workerinput`.

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
        pytest.UsageError: If the JVM did not apply the cycle property.
    """
    # The xdist controller only hands tests to its workers and reports their results; it never
    # imports a test module or runs a test. Each worker is its own process that runs this hook
    # and starts its own JVM, so a JVM in the controller would cost only startup time and
    # memory.
    if _is_xdist_controller(config):
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
    _record_jvm_fatal_errors()
    _record_unlocked_library_releases()


_UNLOCKED_RELEASES: list[str] = []
"""Each liveness-scope release the library made without the update graph's lock, not yet
reported (`_record_unlocked_library_releases`); guarded by `_UNLOCKED_RELEASES_LOCK`."""

_UNLOCKED_RELEASES_LOCK: Final = threading.Lock()


def _record_unlocked_library_releases() -> None:
    """Record each liveness-scope release the library makes without the update graph's lock.

    Releasing a scope can destroy a refreshing table, and the library releases every scope
    holding the update graph's shared lock (`release_scope`): destroyed between two update
    cycles by a thread without the lock, a blink table with a failure queued can deliver it
    twice, and the JVM exits. This wraps `LivenessScope.release` for the whole run. A release
    called from library code (a module of the `deephaven_massive` package) on a thread that
    holds neither the shared nor the exclusive lock is recorded with its caller's file, line,
    and thread, and `_check_nothing_outlives_the_test` fails the test that made it. Tests and
    fakes release their own scopes as they like. The suite runs every branch of the library,
    so a clean run shows that every release site holds the lock.
    """
    liveness_scope = importlib.import_module("deephaven.liveness_scope")
    locks = importlib.import_module("deephaven.update_graph")
    execution_context = importlib.import_module("deephaven.execution_context")
    update_graph = execution_context.get_exec_ctx().update_graph
    package = importlib.import_module("deephaven_massive")
    library = f"{Path(cast('str', package.__file__)).parent}{os.sep}"
    real = liveness_scope.LivenessScope.release

    def release(scope: object) -> None:
        caller = cast("types.FrameType", cast("types.FrameType", inspect.currentframe()).f_back)
        locked = locks.has_shared_lock(update_graph) or locks.has_exclusive_lock(update_graph)
        if caller.f_code.co_filename.startswith(library) and not locked:
            where = f"{caller.f_code.co_filename}:{caller.f_lineno} ({caller.f_code.co_name})"
            with _UNLOCKED_RELEASES_LOCK:
                _UNLOCKED_RELEASES.append(f"{where} on {threading.current_thread().name}")
        real(scope)

    liveness_scope.LivenessScope.release = release


def _take_unlocked_releases() -> list[str]:
    """Return the unlocked library releases recorded so far, and forget them.

    Returns:
        One line per release: its caller's file, line, and function, and its thread.
    """
    with _UNLOCKED_RELEASES_LOCK:
        taken = list(_UNLOCKED_RELEASES)
        _UNLOCKED_RELEASES.clear()
    return taken


def _fail_unlocked_releases() -> None:
    """Fail the test if the library released a scope without the update graph's lock."""
    unlocked = _take_unlocked_releases()
    if unlocked:
        pytest.fail(
            "the library released a liveness scope without the update graph's lock: "
            + "; ".join(unlocked)
        )


def _record_jvm_fatal_errors() -> None:
    """Record the message of every Deephaven fatal error in a file and on file descriptor 2.

    A fatal error inside the JVM, such as a failed assertion on an update-graph thread, ends
    the process: Deephaven's fatal-error handler prints a message and calls `System.exit(1)`.
    The embedded server forwards Java's standard error to Python's `sys.stderr`, which during a
    test is pytest's capture buffer, so the message dies with the process and the run ends with
    exit code 1, no summary, and no clue. This adds an interceptor to the handler that writes the
    message and the Java stack trace to `jvm_fatal_pid<pid>.log` in the repository root, next to
    the JVM's `hs_err` crash logs (CI uploads both when a job fails), and to file descriptor 2.
    The file works in every capture mode and in pytest-xdist workers; the descriptor reaches the
    job log because CI runs pytest with `--capture=sys`, which leaves it alone. The interceptor
    runs before the handler exits the process, so it never raises: whatever goes wrong while it
    writes is dropped, and the exit still happens.
    """
    jpy = importlib.import_module("jpy")
    jcompat = importlib.import_module("deephaven.jcompat")
    traces = jpy.get_type("org.apache.commons.lang3.exception.ExceptionUtils")
    path = Path(__file__).resolve().parent.parent / f"jvm_fatal_pid{os.getpid()}.log"

    def intercept(message: str, throwable: object, _uncaught: bool) -> None:
        with contextlib.suppress(BaseException):
            trace = traces.getStackTrace(throwable)
            text = f"JVM fatal error, the process exits: {message}\n{trace}"
            data = text.encode(errors="replace")
            with contextlib.suppress(BaseException):
                path.write_bytes(data)
            _write_all(2, data)

    interceptor = jpy.get_type("io.deephaven.util.process.FatalErrorReporter$Interceptor")
    process = jpy.get_type("io.deephaven.util.process.ProcessEnvironment")
    process.getGlobalFatalErrorReporter().addInterceptor(jcompat.j_lambda(intercept, interceptor))


def _write_all(fd: int, data: bytes) -> None:
    """Write all of `data` to a file descriptor, however many writes it takes.

    Args:
        fd: The descriptor.
        data: The bytes.
    """
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_node_collection_finished(ids: Sequence[str]) -> None:
    """End a parallel run that selected a live test, before any test starts.

    pytest-xdist calls this in its controller each time a worker has collected, with the ids
    of the tests that worker selected after `-m`, `-k`, and every other deselection. Each
    worker's report is checked as it arrives, and the controller hands out no test until every
    worker has reported, so a refusal always comes before any test is scheduled. The decision
    rests on the selection, not on the `-m` expression, so no expression can slip a live test
    past it. Without pytest-xdist, or with `-n 0`, the hook is never called.

    A refused run ends through `pytest.exit` with pytest's usage-error status, 4, and a message
    naming how many live tests the worker selected and the first of them.

    Args:
        ids: The ids of the tests the worker selected.
    """
    message = parallel.refusal(ids)
    if message is not None:
        pytest.exit(message, returncode=pytest.ExitCode.USAGE_ERROR)


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
def next_update_cycle(dh_exec_ctx: ExecutionContext) -> Callable[[], None]:
    """Return a function that asks the update graph for a cycle and returns once one has run.

    For a test that needs the live tables to have updated between two steps, such as two
    console commands that must count as two pauses. The function fails the test when no cycle
    runs within 30 seconds; it must not be called while the thread holds the update graph's
    lock, under which no cycle can run.

    Returns:
        The function.
    """

    def run() -> None:
        graph = dh_exec_ctx.update_graph.j_update_graph
        lock = graph.exclusiveLock()
        signal = lock.newCondition()
        first = graph.clock().currentStep()
        # Asked for outside the lock, the signal comes from a terminal notification of the next
        # cycle, which runs under the exclusive lock. Holding the lock, the waiter sees either a
        # finished cycle (the step moved) or one not yet begun, whose signal `awaitNanos` waits
        # for, so a cycle that ends before the wait starts is never missed.
        graph.requestSignal(signal)
        graph.requestRefresh()
        remaining = 30_000_000_000  # nanoseconds
        lock.lock()
        try:
            while graph.clock().currentStep() == first and remaining > 0:
                remaining = signal.awaitNanos(remaining)
        finally:
            lock.unlock()
        if graph.clock().currentStep() == first:
            pytest.fail("no update cycle ran within 30 s")

    return run


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


_CONSOLE_COMMAND_TIMEOUT: Final = 30.0
"""Seconds `console_command` lets one command run before it fails the test as a deadlock."""


@pytest.fixture
def console_command(
    dh_exec_ctx: ExecutionContext, request: pytest.FixtureRequest
) -> Callable[[Callable[[], None]], None]:
    """Return a function that runs code the way the Deephaven console runs a command.

    The console runs every command on one server thread that holds the update graph's exclusive
    lock from the command's first line to its last, so no update cycle runs meanwhile. The
    function runs its argument the same way: on a new daemon thread that enters the embedded
    server's execution context (through a fresh wrapper, as library threads do) and takes the
    Java exclusive lock itself, as the console does.

    A command that raises fails the test: pytest reports the thread's exception with its
    traceback. A command that has not returned after 30 seconds is a deadlock: its thread still
    holds the lock, so no update cycle can run again in this process and every later test would
    hang too. The function then fails the test, naming the deadlock, and stops the run after it
    (`session.shouldstop`, which pytest-xdist passes on to the whole run); the thread is a
    daemon, so it cannot keep the process alive.

    Returns:
        The function; it takes the command and returns once the command has returned.
    """
    execution_context = importlib.import_module("deephaven_massive._tables.execution_context")

    def run(command: Callable[[], None]) -> None:
        returned = threading.Event()

        def body() -> None:
            with execution_context.entered(dh_exec_ctx):
                lock = dh_exec_ctx.update_graph.j_update_graph.exclusiveLock()
                lock.lock()
                try:
                    command()
                finally:
                    lock.unlock()
            returned.set()

        thread = threading.Thread(target=body, name="console command", daemon=True)
        thread.start()
        thread.join(_CONSOLE_COMMAND_TIMEOUT)
        if thread.is_alive():
            deadlock = (
                f"a console command deadlocked: it did not return within "
                f"{_CONSOLE_COMMAND_TIMEOUT:.0f} s, waiting for something that needs the update "
                "graph's lock, which it holds; no update cycle can run in this process again"
            )
            request.session.shouldstop = deadlock
            pytest.fail(deadlock)
        if not returned.is_set():
            pytest.fail("the console command raised; its traceback is reported with this test")

    return run


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


class _Heard:
    """How many failures one table's listener heard.

    Written on an update-graph thread and read on the test thread, under a lock. It refers to
    nothing else, so the listener's callback, which the JVM holds, keeps no table or session
    alive.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def add(self, _error: Exception) -> None:
        with self._lock:
            self._count += 1

    def count(self) -> int:
        with self._lock:
            return self._count


@pytest.fixture
def failures_heard() -> Iterator[Callable[[Table], Callable[[], int]]]:
    """Return a function that counts the failures a live table delivers to a listener.

    Deephaven delivers a table's failure to each of its listeners once; a table that hears of a
    second failure ends the process before any count could show it, so each count a test
    reads is 0 or 1. The function adds a listener to the table and returns a function that
    reads how many failures that listener has heard so far. Every listener is stopped when the
    test ends, before its leak check. Watch only tables that fail by then (a session's close
    fails every live table): the JVM holds a stopped listener until it collects it, and a
    listener on a table that is still running keeps that table, and its session, alive.

    Returns:
        A function mapping a table to the reader of its count.
    """
    table_listener = importlib.import_module("deephaven.table_listener")
    dh_table = importlib.import_module("deephaven.table")
    handles = []

    def watch(table: Table) -> Callable[[], int]:
        heard = _Heard()
        # A wrapper of its own: the JVM holds the listener, and through it the wrapper, until
        # it collects them, and the result's own wrapper would keep its session tracked.
        own = dh_table.Table(j_table=table.j_table)
        handles.append(
            table_listener.listen(own, lambda _update, _replay: None, on_error=heard.add)
        )
        return heard.count

    yield watch
    for handle in handles:
        handle.stop()


@pytest.fixture
def user_tables(
    dh_exec_ctx: ExecutionContext,
) -> Iterator[Callable[[Table, str, str], list[Table]]]:
    """Return a function that derives a user's tables from a live result.

    Each failure of a result passes on to every table a user builds on it, and each kind of
    operation hears of it through its own listener. The function builds four, in a liveness
    scope of the user's own, released (under the update graph's shared lock) when the test
    ends:

    - a `where` that keeps the symbol `AAPL`;
    - a `natural_join` that adds a `Mark` column from a static table keyed on the symbol;
    - an `aj` that adds `Mark` from the static row of the same symbol whose `MarkValue` is at
      most the result's value in the ordering column;
    - a `merge` of two `where` branches of the result, so the failure reaches it twice.

    The tables are built through Java calls: the Python wrappers would copy the calling
    frame's locals into the query scope, where they would live until the JVM collected them,
    keeping the result (and through it its session) alive.

    Returns:
        A function of the result, its symbol column, and an ordering column of `double`
        or `Instant` values, returning the four tables in the order above.
    """
    dh_table = importlib.import_module("deephaven.table")
    column = importlib.import_module("deephaven.column")
    factory = importlib.import_module("deephaven.table_factory")
    liveness = importlib.import_module("deephaven.liveness_scope")
    update_graph = importlib.import_module("deephaven.update_graph")
    jpy = importlib.import_module("jpy")
    table_tools = jpy.get_type("io.deephaven.engine.util.TableTools")
    scope = liveness.LivenessScope()

    def derive(result: Table, symbol: str, ordered: str) -> list[Table]:
        with scope.open(), update_graph.shared_lock(dh_exec_ctx.update_graph):
            marks = factory.new_table(
                [column.string_col(symbol, ["AAPL"]), column.string_col("Mark", ["m"])]
            ).j_table
            kind = result.j_table.getDefinition().getColumn(ordered).getDataType().getName()
            lowest = {"java.time.Instant": "'2020-01-01T00:00:00 ET'", "double": "0.0"}[kind]
            marked = marks.updateView(f"MarkValue = {lowest}")
            source = result.j_table
            tables = [
                source.where(f"{symbol} = `AAPL`"),
                source.naturalJoin(marks, symbol, "Mark"),
                source.aj(marked, f"{symbol}, {ordered} >= MarkValue", "Mark"),
                table_tools.merge([source.where("true"), source.where("false")]),
            ]
            return [dh_table.Table(j_table=t) for t in tables]

    yield derive
    with update_graph.shared_lock(dh_exec_ctx.update_graph):
        scope.release()


_RACE_TIMEOUT: Final = 30.0
"""Seconds `race` lets its calls take before it fails the test."""


@pytest.fixture
def race() -> Callable[..., None]:
    """Return a function that runs calls at the same moment, each on a thread of its own.

    A barrier releases every thread at once, so the calls overlap as closely as threads can
    (a `close()` against a failure, a `release()` against a refused request). The function
    returns once every call has, and raises the first error any of them raised; a call still
    running after 30 seconds fails the test.

    Returns:
        A function taking the calls, each with no arguments.
    """

    def run_together(*calls: Callable[[], object]) -> None:
        start = threading.Barrier(len(calls))

        def run(call: Callable[[], object]) -> None:
            start.wait(_RACE_TIMEOUT)
            call()

        # Not a `with` block: its exit would wait for a call that deadlocked, forever, and the
        # test would never fail. A stuck thread is left behind for the leak check to name.
        pool = ThreadPoolExecutor(len(calls), thread_name_prefix="racer")
        try:
            for future in [pool.submit(run, call) for call in calls]:
                future.result(_RACE_TIMEOUT)
        finally:
            pool.shutdown(wait=False)

    return run_together


_CYCLES_TIMEOUT: Final = 30.0
"""Seconds `cycles_ended` waits for the update graph to run its cycles before failing the test."""


@pytest.fixture
def cycles_ended(dh_exec_ctx: ExecutionContext) -> Callable[[], None]:
    """Return a function that waits until every failure queued so far has been delivered.

    Deephaven delivers a published failure at the start of the next update cycle. The function
    waits until the update graph's logical clock has moved two steps past its value at the
    call, so a cycle has started and finished since, and then takes the shared lock, which
    waits for the running cycle to end: every failure queued before the call has reached every
    listener, and the process has survived it, once the function returns. A clock that stops
    for 30 seconds fails the test.

    Returns:
        The function.
    """
    update_graph = importlib.import_module("deephaven.update_graph")

    def wait() -> None:
        clock = dh_exec_ctx.update_graph.j_update_graph.clock()
        target = clock.currentStep() + 2
        deadline = time.monotonic() + _CYCLES_TIMEOUT
        pause = threading.Event()  # never set; only paces the polling
        while clock.currentStep() < target:
            assert time.monotonic() < deadline, "the update graph ran no cycle in 30 s"
            pause.wait(0.005)
        with update_graph.shared_lock(dh_exec_ctx.update_graph):
            pass

    return wait


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
    (`"datasets/wire_symbols"` for `tests/datasets/test_wire_symbols.py`), or a test package's path
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
def rest_session(fake_rest: FakeRestServer) -> Iterator[MassiveSession]:
    """Yield a `MassiveSession` whose REST requests go to the `fake_rest` server.

    Yields:
        An open session with a test key, closed at teardown; read what it sent with
        `fake_rest.get_queries()`.
    """
    dm = importlib.import_module("deephaven_massive")
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url)
    session: MassiveSession = dm.MassiveSession(config)
    yield session
    session.close()


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
    leak fails its own test and not every test after it. Once every library thread the test
    started has ended, a release of a liveness scope the library made without the update
    graph's lock during the test fails it too (`_record_unlocked_library_releases`).

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
    _take_unlocked_releases()  # one made after the previous test's check is not this test's
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
        _fail_unlocked_releases()
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
    core = importlib.import_module("deephaven_massive._tables.session_core")
    session = importlib.import_module("deephaven_massive._session")
    ws_connection = importlib.import_module("deephaven_massive._ingest.ws_connection")
    with core._live_lock:
        live: set[str] = {each.label for each in core._live.values()}
    with session._keys_lock:
        cores = [ref() for ref in session._open_sessions]
    alive = {each.label for each in cores if each is not None}
    with ws_connection._groups_lock:
        groups: set[object] = set(ws_connection._groups.values())
    threads = {
        t for t in threading.enumerate() if t.is_alive() and t.name.startswith(_LIBRARY_THREADS)
    }
    return live, alive, groups, threads


@pytest.fixture(autouse=True)
def _fresh_stream_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Number each test's streams from 1, so tests can name them by id.

    Stream ids are process-unique in the library (a module-level counter in
    `_tables/session_core.py`); each test gets a fresh counter.
    """
    core = importlib.import_module("deephaven_massive._tables.session_core")
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
            learn_only=rate_limits.LEARN_ONLY,
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


# --- Soak tests ------------------------------------------------------------------------------

SOAK_SECONDS_VARIABLE: Final = "MASSIVE_SOAK_SECONDS"
"""The environment variable that sets how long each soak test runs, in seconds."""

_SOAK_DEFAULT_SECONDS: Final = 20.0
"""How long each soak test runs when `MASSIVE_SOAK_SECONDS` is not set: long enough for a few
rounds, short enough that `-m soak` finishes in a few minutes."""

_SOAK = "soak"
"""The marker of the soak tests."""

_SOAK_MARGIN: Final = 300.0
"""Seconds a soak test may take beyond its run before its timeout ends it: its setup, the
drain of its last rows, and its checks. A test that hangs (a deadlock) is ended there, with
every thread's stack in the report."""

_SOAK_ROUND_SECONDS: Final = 2.0
"""How long one round of a soak test lasts, at least: the resources are sampled between
rounds."""

_SOAK_JVM_GROWTH: Final = 256 * 2**20
"""How far, in bytes, the JVM's used heap after a full collection may rise from the end of a
soak test's first round to the end of its run. The first round warms up the code paths and
the update graph; after it, a run that frees what it makes stays within tens of megabytes,
while a stream or table kept per churn step grows by megabytes a second."""

_SOAK_PYTHON_GROWTH: Final = 50_000
"""How many more Python objects the garbage collector may track at the end of a soak test's
run than at the end of its first round. A run that frees what it makes stays within a few
thousand; a result or row kept per churn step grows by tens of thousands a minute."""

_SOAK_THREAD_GROWTH: Final = 64
"""How many more threads than at the end of its first round a soak test may have running at
any round's end. Every library thread belongs to a session or a connection, so a run whose
streams come and go has a steady count; a thread left per stream, call, or session grows it
without limit."""


def soak_seconds() -> float:
    """Return how long each soak test runs: `MASSIVE_SOAK_SECONDS`, or 20 s when it is not set.

    Returns:
        The seconds, a positive number.

    Raises:
        pytest.UsageError: If the variable is set to anything but a positive number.
    """
    text = os.environ.get(SOAK_SECONDS_VARIABLE, "")
    if not text:
        return _SOAK_DEFAULT_SECONDS
    try:
        seconds = float(text)
    except ValueError:
        seconds = 0.0
    if not seconds > 0:
        raise pytest.UsageError(
            f"{SOAK_SECONDS_VARIABLE} must be a positive number of seconds, not {text!r}"
        )
    return seconds


class Soak:
    """One soak test's run: its rounds, and the bounds on what may grow across them.

    The `soak` fixture creates one per soak test. The test runs its load (a feed, churning
    worker threads) and iterates `rounds()` on its own thread; each round lasts at least
    `_SOAK_ROUND_SECONDS`, and after each one the run samples the JVM's used heap after a full
    collection, the objects Python's collector tracks, and the threads running. Once the run's
    time is up the iteration ends, after checking that none of the three grew past its bound
    (`_SOAK_JVM_GROWTH`, `_SOAK_PYTHON_GROWTH`, `_SOAK_THREAD_GROWTH`) from the end of the first
    round, and writes a one-line summary to stderr with the test's own figures (`note`). A
    test that ends early stops iterating, and nothing is checked. Used by the test's thread
    only.

    Attributes:
        seconds: How long the run lasts (`soak_seconds`).
        running: Set while the run lasts; worker threads loop while it is set, and it is
            cleared when the time is up, before the checks.
    """

    def __init__(self, seconds: float, name: str, report: Callable[[str], None]) -> None:
        """Prepare the run; its clock starts at the first round.

        Args:
            seconds: How long the run lasts.
            name: The test's node id, for the summary.
            report: Writes the summary line where the person running the tests sees it.
        """
        self.seconds = seconds
        self.running = threading.Event()
        self.running.set()
        self._name = name
        self._report = report
        self._notes: dict[str, object] = {}
        self._pause = threading.Event()  # never set; only paces the rounds
        runtime = importlib.import_module("jpy").get_type("java.lang.Runtime")
        self._runtime = runtime.getRuntime()

    def note(self, **figures: object) -> None:
        """Add figures to the run's summary line, such as the rows sent and their rate.

        Args:
            **figures: Each name and its value, shown as `name=value`.
        """
        self._notes.update(figures)

    def rounds(self) -> Iterator[int]:
        """Yield each round's number until the run's time is up, then check the growth bounds.

        The run fails the test once its time is up if the JVM's heap, Python's objects, or the
        threads grew past their bounds; the message gives the first round's figures and the
        run's.

        Yields:
            0, 1, 2, ...: the round about to run. The test does or checks one round's work,
            and the next round starts at least `_SOAK_ROUND_SECONDS` after this one did.
        """
        started = time.monotonic()
        samples: list[tuple[int, int, int]] = []
        round_ = 0
        while True:
            began = time.monotonic()
            yield round_
            self._pause.wait(max(0.0, began + _SOAK_ROUND_SECONDS - time.monotonic()))
            samples.append(self._sample())
            round_ += 1
            if time.monotonic() - started >= self.seconds:
                break
        self.running.clear()
        # The least of the last three samples of each measure, so one sample taken mid-burst
        # does not count as growth.
        first = samples[0]
        last = tuple(min(sample[i] for sample in samples[-3:]) for i in range(3))
        most_threads = max(sample[2] for sample in samples)
        figures = " ".join(f"{name}={value}" for name, value in self._notes.items())
        self._report(
            f"soak {self._name}: {round_} rounds in {time.monotonic() - started:.0f} s; "
            f"JVM heap {first[0] / 2**20:.0f} -> {last[0] / 2**20:.0f} MiB; Python objects "
            f"{first[1]} -> {last[1]}; threads {first[2]} -> at most {most_threads}; {figures}"
        )
        assert last[0] - first[0] < _SOAK_JVM_GROWTH, (
            f"the JVM's heap grew from {first[0]} to {last[0]} bytes over the run"
        )
        assert last[1] - first[1] < _SOAK_PYTHON_GROWTH, (
            f"Python's tracked objects grew from {first[1]} to {last[1]} over the run"
        )
        assert most_threads - first[2] < _SOAK_THREAD_GROWTH, (
            f"the threads grew from {first[2]} to {most_threads} over the run"
        )

    def _sample(self) -> tuple[int, int, int]:
        """Collect both heaps, then measure them and count the threads.

        Returns:
            The JVM's used heap in bytes, the objects Python's collector tracks, and the
            threads running.
        """
        gc.collect()
        importlib.import_module("jpy").get_type("java.lang.System").gc()
        used = self._runtime.totalMemory() - self._runtime.freeMemory()
        return used, len(gc.get_objects()), threading.active_count()


@pytest.fixture
def make_trade_feed(fake_ws_server: FakeMassiveServer) -> Iterator[Callable[..., TradeFeed]]:
    """Return a factory of `TradeFeed`s (`tests/_fakes/ws.py`) on the fake WebSocket server.

    The factory takes the feed's symbols and its keyword options (`rows_per_frame`); a feed it
    made that is still running at teardown is stopped then.

    Yields:
        The factory; call `start()` on the feed it returns.
    """
    module = importlib.import_module("tests._fakes.ws")
    feeds: list[TradeFeed] = []

    def make(symbols: list[str], **options: object) -> TradeFeed:
        feed: TradeFeed = module.TradeFeed(fake_ws_server, symbols, **options)
        feeds.append(feed)
        return feed

    yield make
    for feed in feeds:
        feed.stop()


@pytest.fixture
def soak(request: pytest.FixtureRequest, capsys: pytest.CaptureFixture[str]) -> Soak:
    """Return the run of a soak test, which lasts `MASSIVE_SOAK_SECONDS` (20 s by default).

    Args:
        request: The test, named in the summary.
        capsys: Lets the summary line through to the terminal.

    Returns:
        The run; iterate its `rounds()`.
    """

    def report(line: str) -> None:
        with capsys.disabled():
            sys.stderr.write(f"\n{line}\n")

    return Soak(soak_seconds(), request.node.nodeid, report)


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

_LEASES: Final = pytest.StashKey["tuple[GovernorRegistry, int]"]()
"""A test's rate governor registry and its leases when the test began, for the leak check."""

_EVIDENCE: Final = pytest.StashKey[list[tuple[str, str]]]()
"""The config stash key of the evidence lines recorded so far, as (test id, line) pairs."""

_EVIDENCE_FILE = "live-evidence.log"
"""The file, in the repository root, every evidence line is appended to as it is recorded."""

_EVIDENCE_LOCK = threading.Lock()
"""Guards the evidence list and file, so a line may be recorded from any thread."""


_REQUEST_SECONDS = 13.0
"""Seconds of timeout a live test gets for each REST request it makes to a Basic class."""

_SETTLED: Final = pytest.StashKey[
    dict[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]]
]()
"""Each live test's `settles` items, `verifies` items, and `needs` requirements, by test id, for
the summary."""

_PLANS_KEY: Final = pytest.StashKey["plans.LivePlans"]()
"""The run's plans, once the first live test has asked for them, for the summary."""


def _probe_plans(config: MassiveConfig, today: date) -> plans.LivePlans:
    """Find the key's plans by asking Massive, a few requests per class and one per prefix.

    The requests take their permits from the run's governor of the key, every class paced as
    Basic, since none is known yet: no class gets more than five requests, so none waits, and
    the later tests' pacing counts them. The flat files are opened only with both S3
    credentials.

    Args:
        config: The live configuration.
        today: The run's New York day.

    Returns:
        The probed plans. A `PlanProbeError` of a probe that cannot tell them
        (`probe.probe_tiers`) propagates.
    """
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
        tiers = probe.probe_tiers(client, now=datetime.now(_NEW_YORK).time())
    finally:
        client.close()
        lease.release()
    prefixes: tuple[str, ...] = ()
    if config.s3_access_key_id is not None and config.s3_secret_access_key is not None:
        flat_files = FlatFileClient(
            access_key_id=config.s3_access_key_id,
            secret_access_key=config.s3_secret_access_key,
            endpoint_url=config.s3_endpoint_url,
            bucket=config.s3_bucket,
            max_pool=1,
        )
        try:
            prefixes = probe.reached_prefixes(flat_files, calendar.recent_monday(today))
        finally:
            flat_files.close()
    return plans.LivePlans(
        tiers=tiers,
        s3=bool(prefixes),
        flat_files=prefixes,
        source=f"probed {datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ}",
    )


@pytest.fixture(scope="session")
def live_plans(pytestconfig: pytest.Config) -> plans.LivePlans:
    """Return the plans of the run's key: declared, cached today, or probed now.

    `MASSIVE_LIVE_PLANS`, when set, overrides everything and nothing is probed. Otherwise the
    plans come from `.live-plans.json` in the repository root when it was written today (New
    York time), in this layout, for the same API key and S3 access key id, and else from a
    probe: per class, a snapshot of one liquid ticker (Starter), its trades (Developer) and
    quotes (Advanced), and the snapshot's `fmv` field (Business, for stocks, options, and
    currencies); then one file of every flat-file prefix. The probe writes the cache, which
    holds digests, never the credentials. The plans are recorded as evidence under "live
    plans". Only the live tests request this fixture, so a unit run never probes. Being
    session-scoped, it is set up before any function-scoped fixture of the first live test that
    needs it, the autouse key check included, so it makes the same check first itself: under
    CI, or without `MASSIVE_API_KEY`, the test skips before anything reaches Massive.

    Args:
        pytestconfig: The pytest configuration, for the repository root and the stash.

    Returns:
        The plans.

    Raises:
        pytest.UsageError: When a probe's answer proves nothing (`probe.PlanProbeError`):
            the run cannot tell the plans and asks for `MASSIVE_LIVE_PLANS`.
    """
    api_key = _live_key()
    declared = os.environ.get(plans.PLANS_VARIABLE, "").strip()
    if declared:
        run_plans = plans.declared_plans(declared)
    else:
        dm = importlib.import_module("deephaven_massive")
        config: MassiveConfig = dm.MassiveConfig(api_key=api_key)
        today = calendar.today()
        owner = {
            "api_key": probe.digest(config.api_key),
            "s3": probe.digest(config.s3_access_key_id),
        }
        path = pytestconfig.rootpath / probe.PLANS_CACHE
        cached = probe.cached_plans(path, owner=owner, day=today)
        if cached is None:
            try:
                run_plans = _probe_plans(config, today)
            except probe.PlanProbeError as error:
                msg = (
                    f"the plan probe cannot tell the key's plans ({error}); declare them in "
                    f"{plans.PLANS_VARIABLE}, such as stocks=starter,currencies=starter,s3"
                )
                raise pytest.UsageError(msg) from error
            probe.store_plans(path, run_plans, owner=owner, day=today)
        else:
            run_plans = cached
    pytestconfig.stash[_PLANS_KEY] = run_plans
    _record(pytestconfig, "live plans", plans.plans_text(run_plans))
    return run_plans


def _requirements(item: pytest.Item) -> tuple[str, ...]:
    """Return every requirement a test's `needs` markers declare.

    Args:
        item: The test.

    Returns:
        The requirements, in marker order.
    """
    return tuple(str(arg) for marker in item.iter_markers("needs") for arg in marker.args)


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


def _basic_subscriptions(run_plans: plans.LivePlans) -> frozenset[Subscription]:
    """Return the subscriptions on Basic, for the live sessions.

    Args:
        run_plans: The run's plans.

    Returns:
        Each class on Basic, as a `dm.Subscription`.
    """
    dm = importlib.import_module("deephaven_massive")
    return frozenset(
        dm.Subscription(name) for name, tier in run_plans.tiers.items() if tier == "basic"
    )


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
    `-m integration_limits` or `-m integration_smoke` runs only that tier. Last, every soak test
    gets a time limit of its run (`soak_seconds`) plus `_SOAK_MARGIN`, in place of the suite's
    60 s.

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
        if item.path.name.endswith(parallel.INTEGRATION_FILE_SUFFIX)
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
    limit = soak_seconds() + _SOAK_MARGIN
    for item in items:
        if item.get_closest_marker(_SOAK) is not None:
            item.add_marker(pytest.mark.timeout(limit))


def _arrange_live_tests(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Order, time, and register the live tests for the run's summary.

    The tests that need a class left on Basic (the rate-limit checks) run first, while their
    class's minute is unspent. A test that may read a class on Basic (`pacing.basic_reads`)
    gets a timeout scaled to its REST requests (`pacing.declared_requests`), since each may
    wait for a place in the class's 5 a minute; on a paid plan the longer limit is only unused.
    A test that would send a class on Basic more than 5 requests is skipped once the run knows
    its plans (`_live_tests_need_a_key`), since the plans are probed only when the first live
    test runs. Each test's `settles` and `verifies` items and its `needs` are kept for the
    summary.

    Args:
        config: The pytest configuration, whose stash keeps the items.
        items: The selected tests; reordered in place.
    """
    live = [item for item in items if item.get_closest_marker(_INTEGRATION) is not None]
    if not live:
        return
    items.sort(key=lambda item: plans.SOME_BASIC not in _requirements(item))
    settled = config.stash.setdefault(_SETTLED, {})
    for item in live:
        settles = tuple(str(a) for m in item.iter_markers("settles") for a in m.args)
        verifies = tuple(str(a) for m in item.iter_markers("verifies") for a in m.args)
        settled[item.nodeid] = (settles, verifies, _requirements(item))
        if not pacing.basic_reads(_requirements(item)):
            continue
        count = pacing.declared_requests(item.get_closest_marker("requests"))
        current = item.get_closest_marker("timeout")
        base = float(current.args[0]) if current is not None and current.args else 100.0
        item.add_marker(pytest.mark.timeout(base + _REQUEST_SECONDS * count), append=False)


@pytest.fixture(autouse=True)
def _live_tests_need_a_key(request: pytest.FixtureRequest) -> None:
    """Skip a live test under CI, without a key, short of its plans, or too slow on Basic.

    It runs before the test's own function-scoped fixtures. The session-scoped `live_plans`
    fixture, which probes the key's plans for the first live test of a run, is set up before
    it, and so makes the same check (`_live_key`) first. A test's `needs` markers name the plans
    it needs; one the run's plans do not meet skips, naming the plan and its monthly price. A
    test that reads a class the key holds on Basic and declares more than 5 requests skips too
    (`pacing.skip_reason`), since every request after the fifth would wait for a later minute.
    That check needs the plans, which the run learns only when its first live test starts, so
    it sits here rather than in the collection hook.

    Args:
        request: The requesting test, for its markers and the `live_plans` fixture.
    """
    if request.node.get_closest_marker(_INTEGRATION) is None:
        return
    _live_key()
    run_plans: plans.LivePlans = request.getfixturevalue("live_plans")
    requirements = _requirements(request.node)
    unmet = plans.unmet(requirements, run_plans)
    if unmet:
        pytest.skip("needs " + "; ".join(plans.plan_text(r) for r in unmet))
    on_basic = pacing.classes_on_basic(run_plans.tiers)
    reason = pacing.skip_reason(
        [name for name in pacing.basic_reads(requirements) if name in on_basic],
        pacing.declared_requests(request.node.get_closest_marker("requests")),
    )
    if reason is not None:
        pytest.skip(reason)


@pytest.fixture(autouse=True)
def _live_wait_progress(request: pytest.FixtureRequest, rest_clock: FakeClock) -> Iterator[None]:
    """Print a terminal line each time a request of a live test waits for its pace.

    A live test gets a `pacing.WaitProgress` on the key's rate governor for its duration, which
    prints one line per wait (`_write_progress`), so a paced run never looks hung. It holds a
    lease of the governor meanwhile. It is named to sort after `_live_tests_need_a_key`, so a
    skipped test takes no lease, and it depends on `rest_clock`, which counts the leases a test
    holds when it begins, so that count includes none of this fixture's and the leak check
    sees it released.

    Args:
        request: The requesting test, for its markers, the `live_plans` fixture, and the
            terminal.
        rest_clock: Installs the run's live governors for a live test; its clock is unused.

    Yields:
        Nothing; the test runs in between.
    """
    del rest_clock  # requested only for its order: it is set up before this fixture
    if request.node.get_closest_marker(_INTEGRATION) is None:
        yield
        return
    config = _live_massive_config(request.getfixturevalue("live_plans"))
    lease = _live_governors().lease(
        api_key=config.api_key,
        base_url=config.rest_base_url,
        basic=config.rest_basic_subscriptions,
        label="live test progress",
        listener=_ignore,
    )
    requests = pacing.declared_requests(request.node.get_closest_marker("requests"))
    write = functools.partial(_write_progress, request.config)
    progress = pacing.WaitProgress(lease.governor, requests=requests, write=write)
    lease.governor.subscribe(progress)
    try:
        yield
    finally:
        lease.governor.unsubscribe(progress)
        lease.release()


_PROGRESS_LOCK = threading.Lock()
"""Serializes the progress lines, each of which suspends the output capture while it prints."""


def _write_progress(config: pytest.Config, line: str) -> None:
    """Print one pacing line to the terminal while a live test runs, past the output capture.

    pytest captures a test's output and shows it only for a failure, so the line is written
    with the capture suspended for that one write, as `capsys.disabled()` does.

    Args:
        config: The pytest configuration, for the capture manager and the terminal reporter.
        line: The line, without its newline.
    """
    capture = config.pluginmanager.getplugin("capturemanager")
    reporter = config.pluginmanager.getplugin("terminalreporter")
    with _PROGRESS_LOCK, capture.global_and_fixture_disabled():
        reporter.write_line(f"live pacing: {line}")


def _live_key() -> str:
    """Return the API key a live test may use, or skip the test when it may reach no server.

    Every fixture that sets up a live test calls this before anything reaches Massive: under
    CI (`CI` set to anything but blank) live tests never run, so a key added to CI's secrets
    later still never reaches Massive from CI, and without `MASSIVE_API_KEY` they cannot.

    Returns:
        The key, stripped of surrounding whitespace.
    """
    if os.environ.get("CI", "").strip():
        pytest.skip("live tests never run in CI")
    return _required_env("MASSIVE_API_KEY")


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
    "test_stock_filings_integration.py": "a Stocks plan that includes this data",
    "test_options_integration.py": "an Options plan that includes this data",
    "test_indices_integration.py": "an Indices plan that includes this data",
    "test_futures_integration.py": "a Futures plan that includes this data",
    "test_reference_integration.py": "a plan that includes this reference data",
    "test_flatfiles_integration.py": "flat-file access to this dataset",
    "test_host_decision_integration.py": "the plan this check names in its docstring",
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
    """Lift the faulthandler limit from a live or soak test, and decide what a refusal means.

    The 120 s faulthandler limit is cancelled for the call of a live test, whose own timeout,
    scaled to its paced requests, bounds it instead; and for a soak test's, whose timeout is
    scaled to its run (`pytest_collection_modifyitems`).

    A refusal is an `EntitlementError`, or the wire's `WireEntitlementError` (an HTTP 403 or a
    `NOT_AUTHORIZED` envelope). One that names the timeframe ("Your plan doesn't include this
    data timeframe") skips the test, "outside the plan's history", when the day it asked for
    is before the first day the plan keeps by the documented history windows; refused for a
    day inside the window, it fails, since that may be a library bug (`plans.history_verdict`).
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
    if item.get_closest_marker(_INTEGRATION) is not None or item.get_closest_marker(_SOAK):
        # A live test waits on Massive's rate limits and streams, and a soak test runs for
        # `MASSIVE_SOAK_SECONDS`, either longer than the 120 s after which faulthandler would
        # end the process; each one's own (scaled) timeout bounds it.
        faulthandler.cancel_dump_traceback_later()
    try:
        return (yield)
    except (EntitlementError, WireEntitlementError) as error:
        if item.get_closest_marker(_INTEGRATION) is None:
            raise
        if plans.is_timeframe_refusal(error):
            reason = plans.history_verdict(
                error, item.config.stash.get(_PLANS_KEY, None), calendar.today()
            )
            if reason is not None:
                _record(item.config, item.nodeid, f"{reason} ({error})")
                pytest.skip(f"{reason} ({error})")
            _record(item.config, item.nodeid, f"refused inside the plan's history: {error}")
            raise
        requirements = _requirements(item)
        if requirements:
            held = ", ".join(plans.plan_text(r) for r in requirements)
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


def _index_symbol(run_plans: plans.LivePlans) -> str:
    """Return the index a live request reads: `I:SPX`, or `I:NDX` when Indices is on Basic.

    Indices Basic excludes SPX, DJI, RUT, and VIX
    (https://massive.com/knowledge-base/article/are-all-indices-available-through-the-basic-subscription-tier).

    Args:
        run_plans: The run's plans.

    Returns:
        The index's ticker.
    """
    return "I:NDX" if run_plans.tiers["indices"] == "basic" else "I:SPX"


@pytest.fixture
def index_symbol(live_plans: plans.LivePlans) -> str:
    """Return the index a live test reads: `I:SPX`, or `I:NDX` when Indices is on Basic.

    Args:
        live_plans: The run's plans.

    Returns:
        The index's ticker (see `_index_symbol`).
    """
    return _index_symbol(live_plans)


@pytest.fixture
def basic_class(live_plans: plans.LivePlans) -> str:
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
def market_lookup(live_plans: plans.LivePlans) -> classes.MarketLookup:
    """Return the class on Basic whose market-filtered ticker lookups a rate-limit check tests.

    `/v3/reference/tickers` filters by market only the tickers of indices, currencies (forex
    and crypto), and stocks, so the check needs one of those on Basic (`classes.market_lookup`
    prefers them in that order).

    Args:
        live_plans: The run's plans.

    Returns:
        The class, its markets, and a market of another class; the test is skipped when none
        of the three classes is on Basic.
    """
    lookup = classes.market_lookup(live_plans.tiers)
    if lookup is None:
        pytest.skip("needs Indices, Currencies, or Stocks left on Basic")
    return lookup


@pytest.fixture
def one_request_call(basic_class: str) -> Callable[[MassiveSession], object]:
    """Return a library call of the Basic class that sends exactly one REST request.

    The calls are the live-run upgrade check's (`upgrade_check.CALLS`): the previous day's bar
    of AAPL, of an option contract, of `I:NDX` (which Indices Basic includes), and of
    `C:EURUSD`, and the market status of ES.

    Args:
        basic_class: The class on Basic, such as `"indices"`.

    Returns:
        A function that makes the call in a session and returns its result.
    """
    upgrade_check = importlib.import_module("tests._live.live_run.upgrade_check")
    call: Callable[[MassiveSession], object] = upgrade_check.CALLS[basic_class]
    return call


@pytest.fixture
def own_governors(rest_clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> GovernorRegistry:
    """Give a live test a registry of REST rate governors of its own, on the real clock.

    A live test's sessions share the run's registry (`rest_clock`), whose governors the run
    holds for the whole run, so what a governor learned there outlives every session. A test
    of how a governor forgets needs one only its own sessions hold. Its requests are counted
    by Massive with the run's, but not by the run's governors.

    Args:
        rest_clock: Installs the run's registry first; this fixture replaces it.
        monkeypatch: Installs the registry as the library's `GOVERNORS` until the test ends.

    Returns:
        The registry; its governors wait on the real clock.
    """
    del rest_clock  # requested only for its order: it is set up before this fixture
    rate_limits = importlib.import_module("deephaven_massive._ingest.rate_limits")
    registry: GovernorRegistry = rate_limits.GovernorRegistry()
    monkeypatch.setattr(rate_limits, "GOVERNORS", registry)
    return registry


@pytest.fixture
def futures_spread_reads(
    classes_on_basic: frozenset[str],
) -> Callable[[Sequence[Mapping[str, Any]]], tuple[list[ticker_lists.SpreadRead], list[str]]]:
    """Return a function that chooses the reads of futures combo contracts, one per venue.

    With Futures on Basic it keeps 4 reads, the four exchanges' (CME, CBOT, NYMEX, COMEX), so
    the products read and the contracts reads make 5 requests (`ticker_lists.spread_reads`).

    Args:
        classes_on_basic: The classes on Basic.

    Returns:
        A function taking the combo products and returning the reads and the venues left out.
    """
    reads = 4 if "futures" in classes_on_basic else None
    return functools.partial(ticker_lists.spread_reads, reads=reads)


@pytest.fixture
def futures_spread_report() -> Callable[
    [Sequence[Mapping[str, Any]], Mapping[str, Sequence[Mapping[str, Any]]]], list[str]
]:
    """Return a function that describes the futures spread tickers read, by venue and form.

    Returns:
        A function taking the combo products and the combo contracts by venue, and returning
        evidence lines (`ticker_lists.spread_report`) judged by the library's
        `canonical_symbol_future_spread`.
    """
    dm = importlib.import_module("deephaven_massive")
    return functools.partial(ticker_lists.spread_report, spread=dm.canonical_symbol_future_spread)


@pytest.fixture
def crypto_report() -> Callable[..., list[str]]:
    """Return a function that describes Massive's crypto ticker list and its unsplittable rows.

    Returns:
        `ticker_lists.crypto_report`.
    """
    return ticker_lists.crypto_report


@pytest.fixture
def delayed_key(live_config: MassiveConfig, live_plans: plans.LivePlans) -> str:
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

    Gathers each live test's outcome and hands the sorting to `summary.question_lines`, which
    explains the groups. Nothing is printed for a run without a key or without a live test
    that answers a question.

    Args:
        terminalreporter: The terminal reporter, whose statistics hold the outcomes.
        config: The pytest configuration, whose stash holds each test's items and needs and
            the run's plans.
    """
    settled = config.stash.setdefault(_SETTLED, {})
    ran = any(terminalreporter.stats.get(kind) for kind in ("passed", "failed", "skipped"))
    keyed = bool(os.environ.get("MASSIVE_API_KEY", "").strip())
    if not (ran and keyed):
        return
    outcome: dict[str, str] = {}
    for kind in ("passed", "failed", "skipped", "error"):
        for report in terminalreporter.stats.get(kind, []):
            if isinstance(report, pytest.TestReport) and (
                report.when == "call" or kind != "passed"
            ):
                outcome[report.nodeid] = kind
    lines = summary.question_lines(
        settled, outcome, config.stash.get(_PLANS_KEY, None), narrowed=_narrowed(config)
    )
    if not lines:
        return
    terminalreporter.write_sep("=", "open questions")
    for line in lines:
        terminalreporter.write_line(line)


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
def live_config(live_plans: plans.LivePlans) -> MassiveConfig:
    """Return a `MassiveConfig` with `MASSIVE_API_KEY`, and the S3 credentials when set.

    The classes on Basic are its `rest_basic_subscriptions`, so the library paces their
    requests at 5 a minute from the first one.

    Args:
        live_plans: The run's plans.

    Returns:
        The configuration; the test is skipped when `MASSIVE_API_KEY` is not set.
    """
    return _live_massive_config(live_plans)


def _live_massive_config(run_plans: plans.LivePlans) -> MassiveConfig:
    """Build the live configuration: `MASSIVE_API_KEY`, the S3 credentials, the Basic classes.

    Args:
        run_plans: The run's plans; its classes on Basic are paced from the first request.

    Returns:
        The configuration; the test is skipped when `MASSIVE_API_KEY` is not set.
    """
    dm = importlib.import_module("deephaven_massive")
    config: MassiveConfig = dm.MassiveConfig(
        api_key=_required_env("MASSIVE_API_KEY"),
        rest_basic_subscriptions=_basic_subscriptions(run_plans),
    )
    return config


@pytest.fixture
def classes_on_basic(live_plans: plans.LivePlans) -> frozenset[str]:
    """Return the classes the key holds on Basic, so a live test can read less of them.

    A test that may read a class on Basic sends that class at most 5 requests when it is on
    Basic, and reads more (more underlyings, symbols, or pages) only on a paid plan, which is
    not paced.

    Args:
        live_plans: The run's plans.

    Returns:
        The name of each class on Basic, such as `"options"`.
    """
    return pacing.classes_on_basic(live_plans.tiers)


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


def _require_prefix(run_plans: plans.LivePlans, prefix: str) -> None:
    """Skip the test unless the S3 credentials reach a flat-file prefix (`plans.reaches`).

    Args:
        run_plans: The run's plans.
        prefix: The generated prefix.
    """
    if not plans.reaches(run_plans, prefix):
        pytest.skip(f"needs the {prefix} flat files: the S3 credentials do not reach them")


@pytest.fixture
def flat_files_reached(live_plans: plans.LivePlans) -> Callable[[str], bool]:
    """Return a function that tells whether the S3 credentials reach a flat-file prefix.

    For a test that reads several prefixes and records each one it cannot reach, instead of
    skipping at the first (`flat_file_head` and its kin skip).

    Args:
        live_plans: The run's plans.

    Returns:
        A function of a generated prefix (see `plans.reaches`).
    """
    return functools.partial(plans.reaches, live_plans)


@pytest.fixture
def plan_history_start(live_plans: plans.LivePlans) -> Callable[[str], date | None]:
    """Return a function that names the first day a class's plan keeps.

    Args:
        live_plans: The run's plans.

    Returns:
        A function of a class (`"options"`, ...) returning the day, or None for a plan that
        keeps all history (see `plans.history_start`).
    """
    return lambda name: plans.history_start(live_plans, name, calendar.today())


_HEAD_ROWS = 1000
"""How many rows `flat_file_head` keeps from the start of a file."""


@pytest.fixture
def flat_file_head(
    live_flat_files: FlatFileClient, live_plans: plans.LivePlans
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
    live_flat_files: FlatFileClient, live_plans: plans.LivePlans
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
    live_flat_files: FlatFileClient, live_plans: plans.LivePlans
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
            rest_path, path_params, query_params, bucket=classes.bucket_of(path, path_params)
        )
        return [row for page in pages for row in page.rows]

    return read


_MS = 1_000_000
"""Nanoseconds per millisecond."""

_NEW_YORK = ZoneInfo("America/New_York")

# --- Live test dates ------------------------------------------------------------------------
# Fixed dates age out of a Basic plan's history and land on holidays, so the live tests compute
# theirs from the run's New York day with the US stock exchanges' fixed holiday rules. A closure
# no rule predicts (a national day of mourning) is not known; a test that reads one finds no
# rows there and says so.


@pytest.fixture(scope="session")
def recent_monday() -> date:
    """Return the most recent Monday that was a trading day and whose flat files are published.

    For the checks that read one day of a file or of an endpoint that takes a date: a Monday's
    files also follow a weekend, which the weekend checks read. See `calendar.recent_monday`.

    Returns:
        The Monday.
    """
    return calendar.recent_monday(calendar.today())


@pytest.fixture(scope="session")
def recent_week() -> tuple[date, date]:
    """Return the most recent ended calendar week without an exchange holiday.

    Returns:
        Its Monday and Sunday (see `calendar.recent_week`).
    """
    return calendar.recent_week(calendar.today())


@pytest.fixture(scope="session")
def recent_month() -> tuple[date, date]:
    """Return the most recent ended calendar month.

    Returns:
        Its first and last day.
    """
    return calendar.recent_month(calendar.today())


@pytest.fixture(scope="session")
def previous_trading_day() -> date:
    """Return the last trading day before the run's New York day, holidays skipped.

    Returns:
        The day.
    """
    return calendar.trading_day_before(calendar.today())


@pytest.fixture(scope="session")
def recent_holiday() -> Callable[[str], date]:
    """Return a function that finds the most recent occurrence of an exchange holiday.

    Returns:
        A function taking `"martin luther king day"` or `"good friday"` and returning its most
        recent date a week or more before the run's New York day (`calendar.recent_holiday`).
    """
    today = calendar.today()
    return lambda name: calendar.recent_holiday(name, today)


@pytest.fixture
def front_contract() -> Callable[[str, date], str]:
    """Return a function that names a futures product's front contract on a day.

    Returns:
        A function `(product, day)`, the product `"ES"` or `"ZC"` (see `contracts.front_contract`).
    """
    return contracts.front_contract


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


WeekReport = tuple[list[str], int, list[str]]
"""What `week_bar_report` found: the evidence lines, how many week bars it checked, and the
stamps of those whose closest candidate window is not the library's week."""


@pytest.fixture
def week_bar_report(
    rest_rows: Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]],
    recent_week: tuple[date, date],
) -> Callable[[str, str, str], WeekReport]:
    """Return a function that shows where Massive starts a week bar, from raw REST bars.

    It reads the week bars and the shorter bars of one ticker over four calendar weeks, the
    recent week without a holiday and the three before it, from the aggregates range endpoint,
    unfiltered: hour bars for crypto and forex, and day bars for stocks, whose hour bars do not
    add up to the week bar's volume (about a fifth off in the live check) while the day bars of
    its Monday to Friday do exactly. For each week bar it reports its stamp, its volume, the
    first and last shorter bar of the seven days from its stamp, and the three candidate
    seven-day windows whose volume comes closest to the bar's (`week_windows.score`); a
    candidate within a relative error of 1e-9 is marked `MATCH`: that is the bucket Massive
    uses, wherever the bar is stamped. A bar of the four weeks (`week_windows.is_checked`) is
    checked by comparison: the library's week must be the closest window, ties allowed
    (`week_windows.library_is_closest`).

    Args:
        rest_rows: Reads raw REST rows.
        recent_week: The last of the four weeks.

    Returns:
        A function `(ticker_param, ticker, zone)`: the path placeholder of the class (such as
        `"stocksTicker"`), the ticker, and the class's day zone, `"UTC"` (crypto, forex) or
        `"New York"` (stocks), which also selects day bars instead of hour bars. It returns the
        evidence lines, how many bars it checked, and the stamps of the checked bars whose
        closest window is not the library's week.
    """
    span_of_weeks = (recent_week[0] - timedelta(days=21), recent_week[1])
    first, last = span_of_weeks

    def report(ticker_param: str, ticker: str, zone: str) -> WeekReport:
        day_zone = UTC if zone == "UTC" else _NEW_YORK
        path = (
            f"/v2/aggs/ticker/{{{ticker_param}}}/range/{{multiplier}}/{{timespan}}/{{from}}/{{to}}"
        )

        def bars(span: str) -> list[dict[str, Any]]:
            params = {ticker_param: ticker, "multiplier": 1, "timespan": span}
            params |= {"from": first - timedelta(days=7), "to": last + timedelta(days=7)}
            return rest_rows(path, params, {"sort": "asc", "limit": 50_000})

        unit = "hour" if day_zone is UTC else "day"
        week_rows, shorter = bars("week"), bars(unit)
        counts = f"{len(week_rows)} week bars, {len(shorter)} {unit} bars"
        lines = [f"{ticker} {first} to {last}: {counts}"]
        checked, misses = 0, []
        for week in week_rows:
            stamp = int(week["t"])
            target = float(week["v"])
            inside = [h for h in shorter if stamp <= h["t"] < stamp + week_windows.WEEK_MS]
            scored = week_windows.score(stamp, target, shorter, zone=day_zone)
            best = ", ".join(
                f"{label} err={error:.2e}{' MATCH' if error <= week_windows.MATCH else ''}"
                for error, label, _ in scored[:3]
            )
            span = (
                f"{_instant_text(inside[0]['t'] * _MS)} .. {_instant_text(inside[-1]['t'] * _MS)}"
                if inside
                else "none"
            )
            lines.append(
                f"week t={_instant_text(stamp * _MS)} v={target} {unit} bars from the stamp: "
                f"{span}; "
                f"closest windows: {best}"
            )
            if week_windows.is_checked(stamp, span_of_weeks):
                checked += 1
                if not week_windows.library_is_closest(scored):
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
        half = "yes" if calendar.is_half_day(day) else "no"
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


_TICKER_PARAMS: Final = {"I:": "indicesTicker", "O:": "optionsTicker"}
"""The range and `/prev` path placeholder of a ticker's class, by its prefix; stocks have none."""

_OHLCV_KEYS: Final = {
    "t": ("o", "h", "l", "c", "v"),
    "window_start": ("open", "high", "low", "close", "volume"),
}
"""The value keys of a raw REST bar (stamped `t`) and of a day flat-file row (`window_start`)."""


@pytest.fixture(scope="session")
def stamp_days() -> calendar.StampDays:
    """Return the dates the day-stamp checks read, computed from the run's day.

    The checks read the most recent half day whose week has ended, and the trading day before
    it, so a full and a short day are compared and both stay inside every plan's history. See
    `calendar.stamp_days` for every unit.

    Returns:
        The dates, by unit.
    """
    return calendar.stamp_days(calendar.today())


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
        close, and volume (see `stamped`).
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
            half = "yes" if calendar.is_half_day(day) else "no"
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
    (01:00 for an index) on its period's first day, a week's Sunday, is one the library accepts.

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


_EVENTS: dict[str, list[Any]] = {}
"""The events found so far in the run, by kind; the first test that needs one fills it."""


@pytest.fixture
def recent_splits(
    table_rows: Callable[[Table], list[dict[str, Any]]],
    live_plans: plans.LivePlans,
    evidence: Callable[[str], None],
) -> Callable[[MassiveSession], list[events.Split]]:
    """Return a function that finds the year's most recent forward splits of common stocks.

    The first call of a run reads every split of the year and after it (one REST request,
    `_all_splits`), chooses the candidates (`events.forward_splits`), then checks the newest
    ones against the reference tickers until five common stocks are found (a request each,
    `events.first_common`), or, with stocks on Basic, until one is found among the first two
    (`_common_events`); later calls reuse them. Each call records them as evidence.

    Args:
        table_rows: Snapshots a table into rows.
        live_plans: The run's plans, whose Stocks plan sets how many splits are checked.
        evidence: Records the splits found.

    Returns:
        A function taking the test's session and returning the splits, newest first; empty
        when the year had none.
    """

    def find(session: MassiveSession) -> list[events.Split]:
        if "splits" not in _EVENTS:
            window = events.event_window(calendar.today())
            found = events.forward_splits(_all_splits(session, table_rows), window)
            common = _is_common_stock(session, table_rows)
            _EVENTS["splits"] = _common_events(
                found, lambda s: s.symbol, common, run_plans=live_plans
            )
        evidence(f"recent forward splits: {_EVENTS['splits']}")
        return list(_EVENTS["splits"])

    return find


@pytest.fixture
def special_dividends(
    table_rows: Callable[[Table], list[dict[str, Any]]],
    live_plans: plans.LivePlans,
    evidence: Callable[[str], None],
) -> Callable[[MassiveSession], list[events.Dividend]]:
    """Return a function that finds the year's most recent special cash dividends.

    The first call of a run reads up to 50 dividends of the last year classed `special`, newest
    first, through the session's `stocks.dividends` (one REST request), chooses the candidates
    without a later split (`events.special_dividends`, with `_all_splits`), then checks them
    against the reference tickers until five common stocks are found (`events.first_common`),
    or, with stocks on Basic, until one is found among the first two (`_common_events`); later
    calls reuse them. Each call records them as evidence.

    Args:
        table_rows: Snapshots a table into rows.
        live_plans: The run's plans, whose Stocks plan sets how many dividends are checked.
        evidence: Records the dividends found.

    Returns:
        A function taking the test's session and returning the dividends, newest first; empty
        when the year had none.
    """

    def find(session: MassiveSession) -> list[events.Dividend]:
        if "dividends" not in _EVENTS:
            dm = importlib.import_module("deephaven_massive")
            start, end = events.event_window(calendar.today())
            query = session.stocks.dividends(
                start=start,
                end=end,
                sort="ex_dividend_date",
                order=dm.Order.DESC,
                limit=50,
                filters={"distribution_type": "special"},
            )
            rows = table_rows(query.fetch())
            found = events.special_dividends(rows, _all_splits(session, table_rows))
            common = _is_common_stock(session, table_rows)
            _EVENTS["dividends"] = _common_events(
                found, lambda d: d.symbol, common, run_plans=live_plans
            )
        evidence(f"recent special cash dividends: {_EVENTS['dividends']}")
        return list(_EVENTS["dividends"])

    return find


def _common_events(
    found: Sequence[Any],
    symbol: Callable[[Any], str],
    is_common: Callable[[str], bool],
    *,
    run_plans: plans.LivePlans,
) -> list[Any]:
    """Keep the candidate events of common stocks, fewer with stocks on Basic.

    A test sends a class on Basic at most 5 requests, and each candidate it tries costs two bar
    requests, so with stocks on Basic only the first `events.BASIC_EVENT_CHECKS` candidates are
    checked and `events.BASIC_EVENT_CANDIDATES` kept; on a paid plan, the first
    `events.EVENT_CANDIDATES` common stocks.

    Args:
        found: The candidate events, newest first.
        symbol: Returns an event's ticker.
        is_common: Tells whether a ticker is a common stock (a request each).
        run_plans: The run's plans.

    Returns:
        The events kept, newest first.
    """
    if run_plans.tiers["stocks"] == "basic":
        return events.first_common(
            found[: events.BASIC_EVENT_CHECKS],
            symbol,
            is_common,
            keep=events.BASIC_EVENT_CANDIDATES,
        )
    return events.first_common(found, symbol, is_common)


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
            start=events.event_window(calendar.today())[0],
            sort="execution_date",
            order=dm.Order.DESC,
            limit=5_000,
        )
        _EVENTS["all splits"] = table_rows(query.fetch())
    return list(_EVENTS["all splits"])


def _is_common_stock(
    session: MassiveSession, table_rows: Callable[[Table], list[dict[str, Any]]]
) -> Callable[[str], bool]:
    """Return a function that asks the reference tickers whether a ticker is a common stock.

    Args:
        session: The test's session.
        table_rows: Snapshots a table into rows.

    Returns:
        A function of a ticker: True when `reference.tickers` lists it as a common stock (one
        request); an ETF, leveraged or not, a fund, or a warrant answers no row.
    """
    dm = importlib.import_module("deephaven_massive")

    def check(symbol: str) -> bool:
        query = session.reference.tickers(symbol, ticker_type=dm.TickerType.CS)
        return bool(table_rows(query.fetch()))

    return check


@pytest.fixture
def rows_due(
    live_rest: RestClient, live_plans: plans.LivePlans, evidence: Callable[[str], None]
) -> Callable[[str], tuple[str, bool]]:
    """Return a function that tells whether a delayed stream must have delivered rows by now.

    For indices it reads the S&P indices' status from `/v1/marketstatus/now` and takes the
    session to open at 09:30 New York time; for futures it reads the E-mini S&P 500's
    `market_event` and its `timestamp` from `/futures/v1/market-status`. Rows are due once the
    market has been open longer than the plan's feed delay (`streams.rows_due`).

    Args:
        live_rest: The live REST client.
        live_plans: The run's plans, whose class plan sets the delay.
        evidence: Records the status, the open, and the verdict.

    Returns:
        A function taking `"indices"` or `"futures"` and returning Massive's status and
        whether rows are due.
    """

    def due(name: str) -> tuple[str, bool]:
        now = datetime.now(UTC)
        opened: datetime | None = None
        if name == "futures":
            rows = live_rest.get(
                source(Transport.REST, "/futures/v1/market-status"),
                {},
                {"product_code": "ES"},
                bucket="futures",
            ).rows
            status = str(rows[0].get("market_event")) if rows else "unknown"
            stamp = rows[0].get("timestamp") if rows else None
            if status == "open" and isinstance(stamp, str):
                opened = datetime.fromisoformat(stamp)
        else:
            body = live_rest.get(
                source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other"
            ).body
            groups = body.get("indicesGroups") if isinstance(body, dict) else None
            status = str(groups.get("s_and_p")) if isinstance(groups, dict) else "unknown"
            if status == "open":
                opened = streams.index_session_open(now)
        verdict = streams.rows_due(live_plans, name, opened=opened, now=now)
        evidence(f"{name} status {status}, open since {opened}, rows due: {verdict}")
        return status, verdict

    return due
