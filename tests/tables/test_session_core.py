"""Tests for `SessionCore`, the engine behind every session.

They cover closers, collection, the update-graph guards, the ingest ports, the reports
transports make by stream id (row changes, failures, drift), diagnostics views, dying library
threads, and the close.
"""

from __future__ import annotations

import functools
import gc
import importlib
import logging
import re
import threading
import weakref
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import time_table
from deephaven.dherror import DHError
from deephaven.table_listener import listen
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets import catalog as _catalog
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars
from deephaven_massive._errors import LIBRARY_BUG, NEW_SESSION
from deephaven_massive._massive import DriftReport, Transport
from deephaven_massive._tables import live_set
from deephaven_massive._tables import session_core as _core
from deephaven_massive._tables import transports as _transports
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    Diagnostics,
    NotePart,
    StreamUpdate,
)
from deephaven_massive._tables.execution_context import entered, wait_for_update_cycle
from deephaven_massive._tables.liveness import (
    Reaper,
    ResultRegistry,
    make_result,
    new_scope,
    release_scope,
)
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import SymbolStreamQuery
from tests._fakes.stream import transport_of

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.transports import StreamHandle
    from tests._fakes.stream import FakeStreamTransport

TIMEOUT = 30.0


SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)


def stream_query(
    session: dm.MassiveSession, symbols: dm.SymbolsLike = ("AAPL",)
) -> SymbolStreamQuery:
    return build_query(session._core, SPEC, query_type=SymbolStreamQuery, symbols=symbols)


def label(session: dm.MassiveSession) -> str:
    return session._core.label


# --- Lifecycle: closers, collection, guards, and failures reported to the core ---------------


def test_a_session_collected_unclosed_runs_its_closers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ran: list[str] = []

    def broken() -> None:
        raise RuntimeError("cannot close")

    m = dm.MassiveSession(dm.MassiveConfig(api_key="collected-unclosed"))
    name = label(m)
    m._core.on_close(broken)
    m._core.on_close(lambda: ran.append("closed"))
    reaper = m._core._reaper.thread
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.session_core"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert ran == ["closed"]
    # Collection is how a script ends its session: INFO. A closer that raises is a defect.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.INFO,
            f"{name} was garbage-collected without close(); closing its connections and pollers",
        ),
        (logging.ERROR, f"{name}: a transport failed to close. {LIBRARY_BUG}"),
    ]


def test_a_closed_session_does_not_run_its_closers_again_when_collected(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ran: list[str] = []
    m = dm.MassiveSession(dm.MassiveConfig(api_key="collected-closed"))
    m._core.on_close(lambda: ran.append("closed"))
    reaper = m._core._reaper.thread
    m.close()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert ran == ["closed"]
    assert caplog.records == []


def test_closers_run_and_failures_are_logged(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    ran: list[str] = []

    def failing() -> None:
        raise RuntimeError("cannot close")

    massive_session._core.on_close(failing)
    massive_session._core.on_close(lambda: ran.append("second"))
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.session_core"):
        massive_session.close()
    assert ran == ["second"]
    assert "a transport failed to close" in caplog.text


def test_diagnostics_that_cannot_be_built_stop_the_reaper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Reaper] = []
    real = Reaper

    def spying(
        exec_ctx: ExecutionContext,
        name: str,
        *,
        on_death: Callable[[BaseException], None] | None = None,
    ) -> Reaper:
        reaper = real(exec_ctx, name, on_death=on_death)
        created.append(reaper)
        return reaper

    def no_tables(*_: object, **__: object) -> Diagnostics:
        raise RuntimeError("no second table")

    monkeypatch.setattr(_core, "Reaper", spying)
    monkeypatch.setattr(_core, "Diagnostics", no_tables)
    with pytest.raises(RuntimeError, match="no second table"):
        dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    (reaper,) = created
    reaper.thread.join(TIMEOUT)
    assert not reaper.thread.is_alive()  # stopped: no thread outlives the failed session


def test_background_threads_stop_when_the_session_is_collected() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    reaper = m._core._reaper.thread
    writer = m._core.diagnostics.thread
    del m
    gc.collect()
    reaper.join(TIMEOUT)
    writer.join(TIMEOUT)
    assert not reaper.is_alive()
    assert not writer.is_alive()


def test_a_session_with_released_results_is_collected() -> None:
    # Regression: a background loop that kept its last job or row referenced while waiting
    # for the next one kept the SessionCore alive, so its finalizer never stopped the threads.
    # A second, independent fake is needed here because the `fake_streams` fixture is bound to
    # the fixture session, and monkeypatch would hold this session's core until teardown.
    fakes = importlib.import_module("tests._fakes.stream")
    m = dm.MassiveSession(dm.MassiveConfig(api_key="collected-session"))
    core = m._core
    fake = fakes.FakeStreamTransport(core)
    vars(core)["stream_transport"] = lambda: fake
    # A weak reference, not a finalizer that sets an Event: garbage collection can run the
    # finalizer while this thread holds the Event's internal lock, which would deadlock.
    alive = weakref.ref(core)
    reaper, writer = core._reaper.thread, core.diagnostics.thread
    result = stream_query(m).blink()
    del result, core, m
    gc.collect()
    assert fake.shutdown[1].wait(TIMEOUT)
    # The reaper finishes its job just after the shutdown callback; collect until it lets go.
    pause = threading.Event()  # never set; only paces the bounded wait below
    for _ in range(200):
        gc.collect()
        if alive() is None:
            break
        pause.wait(0.05)
    assert alive() is None
    reaper.join(TIMEOUT)
    writer.join(TIMEOUT)
    assert not reaper.is_alive()
    assert not writer.is_alive()


def test_a_closer_registered_after_close_runs_at_once(
    massive_session: dm.MassiveSession,
) -> None:
    ran: list[str] = []
    massive_session.close()
    massive_session._core.on_close(lambda: ran.append("late"))
    assert ran == ["late"]
    massive_session.close()  # idempotent: the late closer does not run again
    assert ran == ["late"]


def test_the_update_graph_check_precedes_the_closed_check(
    massive_session: dm.MassiveSession,
) -> None:
    q = stream_query(massive_session)
    massive_session.close()
    errors: list[BaseException] = []
    seen = threading.Event()
    # The JVM holds the listener, and so its closure, until it collects it: the closure reaches
    # the session through a list emptied below.
    calls = [q.blink, massive_session.close, massive_session._core.close]

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        for call in calls:
            try:
                call()
            except ValueError as e:
                errors.append(e)
        seen.set()

    scope = new_scope()
    with scope.open():
        handle = listen(time_table("PT0.01S"), on_update)
    assert seen.wait(TIMEOUT)
    handle.stop()
    scope.release()
    assert len(errors) == 3
    calls.clear()
    errors.clear()  # their tracebacks hold the session too


def test_a_long_verb_in_a_console_command_is_reported_once_per_pause(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    console_command: Callable[[Callable[[], None]], None],
    next_update_cycle: Callable[[], None],
) -> None:
    # While a console command holds the update graph's lock, no live table updates: a verb that
    # runs long there says so, once per pause. Every verb counts as long here.
    q = stream_query(massive_session)
    made: list[Table] = []
    monkeypatch.setattr(_core, "PAUSED_TABLES_WARNING_SECONDS", 0.0)
    made.append(q.blink())  # without the lock: nothing is paused, nothing is reported
    assert caplog.messages == []

    def command() -> None:
        made.append(q.append_only())
        made.append(q.ring(5))

    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.session_core"):
        console_command(command)
        paused = [r for r in caplog.records if "inside a Deephaven console command" in r.message]
        # The command's two verbs share one pause: the second logs only at DEBUG.
        assert [r.levelno for r in paused] == [logging.WARNING, logging.DEBUG]
        message = paused[0].getMessage()
        assert message.startswith(
            f"{label(massive_session)}: m.test.trades().append_only() ran for "
        )
        assert "the command ends; leave out wait=True to let fetch() or history() read" in message
        caplog.clear()
        next_update_cycle()  # the live tables updated again: the pause ended
        console_command(lambda: made.append(q.ring(7)))
        paused = [r for r in caplog.records if "inside a Deephaven console command" in r.message]
        assert [r.levelno for r in paused] == [logging.WARNING]  # a new pause warns again
    for result in made:
        massive_session.release(result)
    made.clear()


def test_a_short_verb_in_a_console_command_is_not_reported(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    caplog: pytest.LogCaptureFixture,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    q = stream_query(massive_session)
    made: list[Table] = []
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        console_command(lambda: made.append(q.blink()))
    assert caplog.messages == []
    massive_session.release(made.pop())


def test_calls_holding_the_shared_lock_work(massive_session: dm.MassiveSession) -> None:
    # A thread holding the update graph's shared lock may open and close sessions; close()
    # then waits for neither the diagnostics writer nor an update cycle.
    update_graph = massive_session._core.exec_ctx.update_graph
    writer = massive_session._core.diagnostics.thread
    with shared_lock(update_graph):
        other = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
        other.close()
        massive_session.close()
    assert massive_session._core.closed
    assert other._core.closed
    writer.join(TIMEOUT)
    assert not writer.is_alive()


def test_a_result_that_outlives_its_session_is_still_released() -> None:
    # A result whose release callback does not hold the core (a future poll() table's
    # poller, for example) can outlive the session. The reaper must outlive it too, or the
    # result's scope would never be released once it is collected.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="outliving-poll"))
    core = m._core
    released = threading.Event()
    with entered(core.exec_ctx):
        result, scope = make_result(core.exec_ctx, lambda: time_table("PT1S"))
    core.results.track(result, scope, released.set)  # the callback holds nothing of the core
    alive = weakref.ref(core)
    reaper = core._reaper.thread
    del core, m
    pause = threading.Event()  # never set; only paces the bounded waits below
    for _ in range(200):
        gc.collect()
        if alive() is None:
            break
        pause.wait(0.05)
    assert alive() is None  # the session is gone
    pause.wait(0.2)
    assert reaper.is_alive()  # but its reaper waits for the result
    assert result.is_refreshing
    del result
    gc.collect()
    assert released.wait(TIMEOUT)
    reaper.join(TIMEOUT)
    assert not reaper.is_alive()  # and ends with the last result


def test_an_unused_session_is_freed_without_the_cyclic_collector() -> None:
    # No reference cycle runs through a core: a result record's owner callback holds the
    # core's stream registry only weakly, so reference counting alone frees a session once
    # nothing uses it. The fake is imported directly, not taken from the `fake_streams` fixture:
    # that fixture is bound to the fixture session, and this test must own the only reference
    # to the core it watches.
    fakes = importlib.import_module("tests._fakes.stream")
    gc.collect()
    gc.disable()
    try:
        idle = dm.MassiveSession(dm.MassiveConfig(api_key="unused-core"))
        idle_core = weakref.ref(idle._core)
        del idle
        assert idle_core() is None  # never used: freed at once
        m = dm.MassiveSession(dm.MassiveConfig(api_key="used-core"))
        core = m._core
        fake = fakes.FakeStreamTransport(core)
        vars(core)["stream_transport"] = lambda: fake
        result = stream_query(m).append_only()
        with core.registry_lock:
            record = next(iter(core.results._results.values()))
        callback = record.on_released
        assert isinstance(callback, functools.partial)
        assert isinstance(callback.args[0], weakref.ref)  # it holds the registry weakly
        alive = weakref.ref(core)
        reaper = core._reaper.thread
        del record, callback, core, m
        assert alive() is not None  # the running stream keeps its session
        del result  # the last use of the stream: its session leaves the live set
        assert fake.shutdown[1].wait(TIMEOUT)
        # The reaper drops the last reference, then ends. Wait in Python, not in join(): a
        # free-threaded build merges a reference dropped on another thread into its owner's
        # count only while the owner runs Python code.
        pause = threading.Event()  # never set; only paces the bounded wait
        for _ in range(int(TIMEOUT / 0.02)):
            if alive() is None and not reaper.is_alive():
                break
            pause.wait(0.02)
        assert alive() is None
        assert not reaper.is_alive()
    finally:
        gc.enable()


def test_update_streams_reaches_every_stream_still_registered(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    drain: Callable[[], None],
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    # Shut down while still held: the record stays, so the flush can still report on it.
    fake_streams.publishers[1].publish_failure(RuntimeError("gone"))
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    core.update_streams(
        {
            1: StreamUpdate(rows_published=5, notes={NotePart.QUIET: "slow consumer"}),
            99: StreamUpdate(rows_published=1),  # left the registry: ignored
        }
    )
    columns = ("StreamId", "State", "RowsPublished", "Host", "Error")
    assert [tuple(r[c] for c in columns) for r in stream_rows()] == [
        (1, "RUNNING", 5, "socket.test", "slow consumer")  # an update never fails a stream
    ]
    massive_session.release(result)
    drain()
    assert core.streams._streams == {}
    core.update_streams({1: StreamUpdate(rows_published=6)})  # deleted: nothing changes
    assert [r["RowsPublished"] for r in stream_rows()] == [5]


def test_a_closer_registered_after_close_never_runs_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ran: list[str] = []
    m = dm.MassiveSession(dm.MassiveConfig(api_key="late-closer"))
    reaper = m._core._reaper.thread
    m.close()
    m._core.on_close(lambda: ran.append("late"))
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert ran == ["late"]  # once, at registration; the collection finalizer runs nothing
    assert caplog.records == []


def test_a_failure_after_the_stream_left_the_registry_is_logged(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    caplog: pytest.LogCaptureFixture,
) -> None:
    core = massive_session._core
    result = stream_query(massive_session).blink()
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.session_core"):
        # In the registry: shown in m.streams(), not logged.
        core.fail_stream(1, dm.MassiveError("shown"))
        core.fail_stream(42, dm.MassiveError("late"))  # never registered: only logged
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.DEBUG, f"{core.label}: stream 42 failed after it left the registry: late")
    ]
    massive_session.release(result)


# --- The ingest ports and the catalog --------------------------------------------------------


# Stand-ins compared by identity only.
_REST: _transports.RestTransport = object()  # type: ignore[assignment]  # compared by identity
_STREAMS: _transports.StreamTransport = object()  # type: ignore[assignment]  # by identity
_FLAT_FILES: _transports.FlatFileTransport = object()  # type: ignore[assignment]  # by identity


class _Ports:
    # Ingest ports that record which transport each hook asked for.
    def __init__(self) -> None:
        self.asked: list[str] = []

    def rest(self) -> _transports.RestTransport:
        self.asked.append("rest")
        return _REST

    def streams(self) -> _transports.StreamTransport:
        self.asked.append("streams")
        return _STREAMS

    def flat_files(self) -> _transports.FlatFileTransport:
        self.asked.append("flat_files")
        return _FLAT_FILES

    def read_stop(self) -> threading.Event:
        self.asked.append("read_stop")
        return threading.Event()


def test_the_transport_hooks_ask_the_attached_ports(dh_exec_ctx: ExecutionContext) -> None:
    catalog = _catalog.Catalog((SPEC,))
    core = _core.SessionCore(
        dm.MassiveConfig(api_key="k"), dh_exec_ctx, "core", catalog=catalog, thread_prefix="t"
    )
    try:
        assert core.catalog is catalog
        assert core.thread_prefix == "t"
        with pytest.raises(RuntimeError, match=r"^core has no transports attached$"):
            core.rest_transport()
        ports = _Ports()
        core.attach(ports)
        assert core.rest_transport() is _REST
        assert core.stream_transport() is _STREAMS
        assert core.flat_file_transport() is _FLAT_FILES
        assert not core.read_stop().is_set()
        assert ports.asked == ["rest", "streams", "flat_files", "read_stop"]
        with pytest.raises(RuntimeError, match=r"^core already has its transports attached$"):
            core.attach(_Ports())
        assert core.rest_transport() is _REST
        assert ports.asked == ["rest", "streams", "flat_files", "read_stop", "rest"]
    finally:
        core.close()


# Schema drift of streams.


HALTED = DriftReport(extra=frozenset({"halted"}))


def test_stream_drift_reaches_the_row_and_is_forgotten_when_the_stream_leaves(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    drain: Callable[[], None],
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    core.record_stream_drift(1, Transport.WS, HALTED)
    core.record_stream_drift(1, Transport.WS, HALTED)  # nothing new: the row is not rewritten
    core.record_stream_drift(1, Transport.WS, DriftReport(missing=frozenset({"trfi"})))
    core.record_stream_drift(99, Transport.WS, HALTED)  # not in the registry: ignored, not kept
    assert [(r["StreamId"], r["SchemaDrift"], r["Error"]) for r in stream_rows()] == [
        (1, "WebSocket:halted+, WebSocket:trfi-", None)
    ]
    # Nothing was kept for 99.
    assert core.drift.record(99, "WebSocket", HALTED) == "WebSocket:halted+"
    # Released, then shut down: the record leaves the registry at the shutdown.
    massive_session.release(result)
    drain()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.streams._streams == {}
    # Forgotten, so new again; the last row keeps its text.
    assert core.drift.record(1, "WebSocket", HALTED) == "WebSocket:halted+"
    assert [r["SchemaDrift"] for r in stream_rows()] == ["WebSocket:halted+, WebSocket:trfi-"]


def test_stream_drift_is_forgotten_when_the_last_hold_of_a_shut_down_stream_goes(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    drain: Callable[[], None],
) -> None:
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    core.record_stream_drift(1, Transport.WS, HALTED)
    # Shut down while held: the record stays until the hold is released.
    fake_streams.publishers[1].publish_failure(RuntimeError("gone"))
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.drift.record(1, "WebSocket", HALTED) is None  # still remembered
    massive_session.release(result)
    drain()
    assert core.streams._streams == {}
    assert core.drift.record(1, "WebSocket", HALTED) == "WebSocket:halted+"


def test_stream_drift_recorded_during_the_open_is_kept(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    core = massive_session._core

    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        core.record_stream_drift(stream_id, Transport.WS, HALTED)  # the row is not written yet
        return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    result = stream_query(massive_session).append_only()
    assert [(r["StreamId"], r["SchemaDrift"]) for r in stream_rows()] == [(1, "WebSocket:halted+")]
    massive_session.release(result)


# --- Failing on close, diagnostics views, and dying threads -----------------------------------


class _Fatal(BaseException):
    # Like KeyboardInterrupt: a reaper job that raises it ends the thread.
    pass


def test_a_diagnostics_view_that_cannot_be_built_leaves_no_hold(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = massive_session._core

    def refuse(*_: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(_core, "make_result", refuse)  # fails before a scope exists
    with pytest.raises(KeyboardInterrupt):
        massive_session.streams()
    assert core._views == 0
    monkeypatch.undo()
    monkeypatch.setattr(ResultRegistry, "track", refuse)  # fails after the scope exists
    with pytest.raises(KeyboardInterrupt):
        massive_session.connections()
    assert core._views == 0
    assert id(core) not in live_set._live


def test_a_diagnostics_view_that_races_close_raises_the_closed_error(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The close releases the diagnostics tables while the view is built.
    core = massive_session._core
    build = make_result

    def close_first(exec_ctx: ExecutionContext, factory: Callable[[], Table]) -> object:
        massive_session.close()
        return build(exec_ctx, factory)

    monkeypatch.setattr(_core, "make_result", close_first)
    with pytest.raises(
        dm.MassiveError,
        match=f"^{label(massive_session)} is closed; " + re.escape(NEW_SESSION) + "$",
    ) as caught:
        massive_session.streams()
    assert isinstance(caught.value.__cause__, DHError)
    assert core._views == 0


def test_a_table_that_cannot_be_failed_does_not_spare_the_others(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fail = fake_streams._fail

    def stubborn(stream_id: int, error: dm.MassiveError) -> bool:
        if stream_id == 1:
            raise RuntimeError("cannot fail")
        return fail(stream_id, error)

    monkeypatch.setattr(fake_streams, "_fail", stubborn)  # before the opens bind it
    first = stream_query(massive_session).append_only()
    second = stream_query(massive_session, ["MSFT"]).append_only()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        massive_session.close()
    assert f"{label(massive_session)}: could not fail the table of stream 1" in caplog.text
    assert f"{label(massive_session)} closed; 1 live tables failed, streams 1 [2]" in caplog.text
    assert list(fake_streams.failed) == [2]
    massive_session.release(first)
    massive_session.release(second)


def test_a_dying_reaper_fails_the_sessions_live_tables(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)  # the death still escapes
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    reaper = core._reaper.thread

    def fatal() -> None:
        raise _Fatal

    core.defer(fatal)
    reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert [args.exc_type for args in escaped] == [_Fatal]
    text = fake_streams.failed[1]
    assert text.startswith(f"{label(massive_session)}: its reaper thread died (_Fatal")
    assert text.endswith(f"this table no longer updates; {NEW_SESSION}")
    assert core.diagnostics.wait_until_shown(TIMEOUT)
    (row,) = dh_to_pylist(core.diagnostics.streams)  # no view: nothing could release it
    assert (row["State"], row["Error"]) == ("FAILED", text)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    # Nothing can release the hold any more: a dead reaper leaks its session. Drop the hold
    # here, then the job it left in the dead reaper's queue, so the session can go.
    del result
    core.streams._release_hold(1)
    queued = core._reaper._worker._queue
    while not queued.empty():
        queued.get_nowait()


class _FatalPublisher:
    # A diagnostics publisher whose add raises what the writer cannot carry on after, once the
    # test sets `go`: the test picks when the writer dies.
    def __init__(self) -> None:
        self.go = threading.Event()
        self.failures: list[Exception] = []

    def add(self, table: Table) -> None:
        assert self.go.wait(TIMEOUT)
        raise _Fatal

    def publish_failure(self, error: Exception) -> None:
        self.failures.append(error)


def test_a_diagnostics_writer_dying_after_the_call_fails_the_returned_table(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)  # the death still escapes
    core = massive_session._core
    writer = core.diagnostics.thread
    publisher = _FatalPublisher()
    core.diagnostics._streams.publisher = publisher
    result = stream_query(massive_session).append_only()  # its row waits for `go`
    publisher.go.set()  # the row kills the writer only now, after the call returned
    writer.join(TIMEOUT)
    assert not writer.is_alive()
    assert [args.exc_type for args in escaped] == [_Fatal]
    text = fake_streams.failed[1]
    assert text.startswith(f"{label(massive_session)}: its diagnostics writer thread died (_Fatal")
    assert text.endswith(f"this table no longer updates; {NEW_SESSION}")
    massive_session.release(result)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_a_diagnostics_writer_dying_during_the_call_makes_it_raise(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)  # the death still escapes
    core = massive_session._core
    writer = core.diagnostics.thread
    publisher = _FatalPublisher()
    publisher.go.set()
    core.diagnostics._streams.publisher = publisher
    require_unfailed = core.streams._require_unfailed

    def after_the_death(spec: DatasetSpec, stream_id: int) -> None:
        writer.join(TIMEOUT)  # the stream's opening row has killed the writer by now
        require_unfailed(spec, stream_id)

    monkeypatch.setattr(core.streams, "_require_unfailed", after_the_death)
    with pytest.raises(
        dm.MassiveError,
        match=rf"^test\.trades stream 1 failed: {re.escape(label(massive_session))}: its "
        r"diagnostics writer thread died \(_Fatal\(\)\); this table no longer updates; "
        + re.escape(NEW_SESSION)
        + "$",
    ):
        stream_query(massive_session).append_only()
    assert not writer.is_alive()
    assert [args.exc_type for args in escaped] == [_Fatal]
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the call released its hold


def test_a_dying_diagnostics_writer_fails_the_diagnostics_tables(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A view a user holds fails instead of showing frozen rows as if they were current, a new
    # view is refused with the reason, and the tables' scope goes to the reaper.
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)  # the death still escapes
    core = massive_session._core
    diagnostics = core.diagnostics
    writer = diagnostics.thread
    publisher = _FatalPublisher()
    diagnostics._streams.publisher = publisher
    handed: list[LivenessScope] = []
    hand = diagnostics._release_later

    def handing(scope: LivenessScope) -> None:
        handed.append(scope)
        hand(scope)

    monkeypatch.setattr(diagnostics, "_release_later", handing)
    held = massive_session.connections()
    assert not held.j_table.isFailed()
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.diagnostics"):
        result = stream_query(massive_session).append_only()  # its row waits for `go`
        publisher.go.set()  # the row kills the writer only now, after the call returned
        writer.join(TIMEOUT)
    assert not writer.is_alive()
    assert [args.exc_type for args in escaped] == [_Fatal]
    text = f"{label(massive_session)}: its diagnostics writer thread died (_Fatal()); this table"
    assert fake_streams.failed[1].startswith(text)  # the live table fails as before
    assert [str(error) for error in publisher.failures] == [
        f"{text} no longer updates; {NEW_SESSION}"
    ]
    assert diagnostics._scope in handed
    assert "their scope is handed to the reaper" in caplog.text
    assert wait_for_update_cycle(core.exec_ctx, TIMEOUT)  # the cycle that delivers the failures
    assert held.j_table.isFailed()
    with pytest.raises(
        dm.MassiveError,
        match=rf"^{re.escape(label(massive_session))}: its diagnostics tables failed when their "
        r"writer thread died \(_Fatal\(\)\); see the ERROR logged then; "
        + re.escape(NEW_SESSION)
        + "$",
    ):
        massive_session.connections()
    assert core._views == 1  # the refused view counted nothing
    massive_session.release(held)
    massive_session.release(result)


class _UnfailablePublisher:
    # A diagnostics publisher whose failure raises.
    def add(self, table: Table) -> None:
        raise AssertionError(table)  # the writer is releasing its tables: it adds no rows

    def publish_failure(self, error: Exception) -> None:
        raise RuntimeError(f"cannot fail: {error}")


def test_a_writer_dying_as_it_releases_its_tables_leaves_the_scope_alone(
    dh_exec_ctx: ExecutionContext,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Releasing a scope twice raises: a writer that dies in its own release hands only its
    # tables' failures on, never the scope. A table that cannot fail costs the others nothing.
    monkeypatch.setattr(threading, "excepthook", [].append)  # the death still escapes
    release = Diagnostics._release_tables

    def release_then_die(diagnostics: Diagnostics) -> None:
        release(diagnostics)
        raise _Fatal

    monkeypatch.setattr(Diagnostics, "_release_tables", release_then_die)
    handed: list[LivenessScope] = []
    deaths: list[BaseException] = []
    diagnostics = Diagnostics(
        dh_exec_ctx,
        "dying diagnostics",
        label="TestSession",
        release_later=handed.append,
        on_death=deaths.append,
    )
    diagnostics._connections.publisher = _UnfailablePublisher()
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.diagnostics"):
        diagnostics.release()
    assert [type(death) for death in deaths] == [_Fatal]
    assert diagnostics._scope not in handed
    assert len(handed) == 3  # the three other tables' unused pins: they were destroyed
    assert f"could not fail the connections table. {LIBRARY_BUG}" in caplog.text
    for scope in handed:
        release_scope(dh_exec_ctx, scope)


def test_the_reaper_outlives_the_diagnostics_writer_of_a_collected_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A writer that dies hands its work to the reaper: when a session is collected, the reaper
    # waits for the writer to end before it retires, so that work is still done.
    monkeypatch.setattr(threading, "excepthook", [].append)  # the death still escapes
    gate = threading.Event()
    release = Diagnostics._release_tables

    def held_then_die(diagnostics: Diagnostics) -> None:
        assert gate.wait(TIMEOUT)
        release(diagnostics)
        raise _Fatal

    ran: list[str] = []
    real_release_on = _core._release_on

    def release_on(reaper: Reaper, exec_ctx: ExecutionContext, scope: LivenessScope) -> None:
        real_release_on(reaper, exec_ctx, scope)
        reaper.submit(lambda: ran.append("handed work"))

    monkeypatch.setattr(Diagnostics, "_release_tables", held_then_die)
    monkeypatch.setattr(_core, "_release_on", release_on)
    m = dm.MassiveSession(dm.MassiveConfig(api_key="outlived"))
    reaper, writer = m._core._reaper.thread, m._core.diagnostics.thread
    del m
    gc.collect()
    gate.set()  # the writer releases its tables and dies: its failures go to the reaper
    writer.join(TIMEOUT)
    reaper.join(TIMEOUT)
    assert not writer.is_alive()
    assert not reaper.is_alive()
    assert ran  # the reaper ran the dead writer's work before it ended


def test_a_thread_of_a_freed_session_dies_quietly() -> None:
    def gone() -> None:
        return None  # the core was collected

    _core.SessionCore._thread_died(gone, "reaper", _Fatal())


def test_an_exception_escaping_a_verb_is_logged_at_debug_and_raised(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core

    def refuse() -> None:
        raise ValueError("start must come before end")

    with (
        caplog.at_level(logging.DEBUG, logger=_core.__name__),
        pytest.raises(ValueError, match=r"^start must come before end$"),
    ):
        core.run_verb("m.stocks.trades('AAPL').history()", refuse)
    massive_session.close()
    with (
        caplog.at_level(logging.DEBUG, logger=_core.__name__),
        pytest.raises(dm.MassiveError, match=r"is closed; " + re.escape(NEW_SESSION) + "$"),
    ):
        core.run_verb("m.stocks.trades('AAPL').fetch()", refuse)
    # The guard's refusal too: every exception that escapes a verb.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.DEBUG,
            "m.stocks.trades('AAPL').history() raised ValueError: start must come before end",
        ),
        (
            logging.DEBUG,
            f"m.stocks.trades('AAPL').fetch() raised SessionClosedError: {core.label} is closed; "
            + NEW_SESSION,
        ),
    ]


def test_rows_written_while_close_runs_leave_one_row_per_key(
    massive_session: dm.MassiveSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # Library threads keep writing connection rows while close() releases the diagnostics (on
    # a free-threaded build too): nothing deadlocks, and the held view shows one row per key.
    view = massive_session.connections()
    core = massive_session._core
    families = (ChannelFamily.TICK, ChannelFamily.FMV)
    start = threading.Barrier(len(families) + 1)

    def write(family: ChannelFamily) -> None:
        start.wait(TIMEOUT)
        for reconnects in range(200):
            core.diagnostics.write(
                ConnectionRow(
                    transport=Transport.WS,
                    asset_class=AssetClass.STOCKS,
                    family=family,
                    reconnects=reconnects,
                )
            )

    threads = [threading.Thread(target=write, args=(family,)) for family in families]
    for thread in threads:
        thread.start()
    start.wait(TIMEOUT)
    massive_session.close()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    rows = dh_to_pylist(view)
    keys = [(r["Transport"], r["AssetClass"], r["Family"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert all(0 <= r["Reconnects"] < 200 for r in rows)
    del view


def test_an_unexpected_exception_escaping_a_verb_is_raised_as_a_library_bug(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    core = massive_session._core
    broken = RuntimeError("the index broke")

    def body() -> None:
        raise broken

    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.session_core"),
        pytest.raises(dm.MassiveError, match=r"^m\.x\(\)\.fetch\(\) failed: RuntimeError") as bug,
    ):
        core.run_verb("m.x().fetch()", body)
    assert (bug.value.source, bug.value.is_bug, bug.value.is_retryable) == (
        dm.ErrorSource.LIBRARY,
        True,
        False,
    )
    assert bug.value.__cause__ is broken
    (record,) = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.exc_info is not None  # logged with its traceback


@pytest.mark.parametrize(
    "error",
    [ValueError("bad start"), TypeError("not a bool"), KeyboardInterrupt()],
    ids=["value", "type", "interrupt"],
)
def test_a_wrong_argument_or_an_interrupt_escapes_a_verb_unchanged(
    massive_session: dm.MassiveSession, error: BaseException
) -> None:
    def body() -> None:
        raise error

    with pytest.raises(type(error)) as raised:
        massive_session._core.run_verb("m.x().fetch()", body)
    assert raised.value is error
    error.__traceback__ = None  # the parameter outlives the test; its traceback holds the core
