"""Tests for `SessionCore`, the engine behind every session.

They cover opening streams, the record lifetime, closers, collection, the update-graph guards,
failures reported to the core, and the interrupt windows of a verb's bookkeeping.
"""

from __future__ import annotations

import contextlib
import dataclasses
import gc
import importlib
import inspect
import itertools
import logging
import sys
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, input_table, new_table, time_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table
from deephaven.table_listener import listen
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets import catalog as _catalog
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._massive import DriftReport
from deephaven_massive._namespaces.query_building import build_query
from deephaven_massive._query import StreamQuery
from deephaven_massive._tables import session_core as _core
from deephaven_massive._tables import transports as _transports
from deephaven_massive._tables.columns import describe
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    Diagnostics,
    StreamRow,
    StreamUpdate,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import Reaper, ResultRegistry, make_result, new_scope
from deephaven_massive._tables.transports import StreamHandle
from deephaven_massive._types import override
from tests._fakes.stream import transport_of

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    import functools
    from collections.abc import Callable, Iterator
    from types import FrameType

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.request import StreamKey
    from deephaven_massive._tables.liveness import ResultRecord
    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.stream import FakeStreamTransport

TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, StreamQuery)
    return q


def label(session: dm.MassiveSession) -> str:
    return session._core.label


# --- Opening a stream: registered first, one open per key ------------------------------------


class _Gated:
    # A stream transport whose first open blocks until `gate` is set and then, with
    # `fail_first`, raises that exception instead of opening.

    def __init__(
        self, fake: FakeStreamTransport, *, fail_first: BaseException | None = None
    ) -> None:
        self.fake = fake
        self.fail_first = fail_first
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.calls = 0

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            assert self.gate.wait(TIMEOUT)
            if self.fail_first is not None:
                raise self.fail_first
        return self.fake.open_stream(spec, request, channel, stream_id=stream_id)


def _when_waiting(
    core: _core.SessionCore, monkeypatch: pytest.MonkeyPatch, then: Callable[[], None]
) -> threading.Thread:
    # Once a verb has found the stream still opening (it is about to wait), run `then` on a
    # helper thread.
    waiting = threading.Event()
    claim = core._claim

    def spying(
        spec: DatasetSpec,
        request: Request,
        key: StreamKey,
        *,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(spec, request, key, may_wait=may_wait, holds=holds)
        if isinstance(found, threading.Event):
            waiting.set()
        return found

    monkeypatch.setattr(core, "_claim", spying)

    def run() -> None:
        assert waiting.wait(TIMEOUT)
        then()

    helper = threading.Thread(target=run)
    helper.start()
    return helper


def test_concurrent_verbs_on_one_key_share_one_open(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    core = massive_session._core
    gated = _Gated(fake_streams)
    monkeypatch.setattr(core, "stream_transport", lambda: gated)
    q = stream_query(massive_session)
    with ThreadPoolExecutor(1) as pool:
        opener = pool.submit(q.blink)
        assert gated.entered.wait(TIMEOUT)
        helper = _when_waiting(core, monkeypatch, gated.gate.set)
        waiter = q.append_only()  # waits for the open, then reuses the stream
        first = opener.result(TIMEOUT)
    helper.join(TIMEOUT)
    assert gated.calls == 1
    assert [(r["StreamId"], r["HeldResults"]) for r in stream_rows()] == [(1, 2)]
    massive_session.release(first)
    massive_session.release(waiter)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_a_console_command_never_waits_for_another_threads_open(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
    console_command: Callable[[Callable[[], None]], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Another thread's open that needs the update graph's lock, as starting a watchlist's
    # listener does, cannot finish while a console command holds that lock. A verb in the
    # command therefore takes the key over and opens a stream of its own instead of waiting
    # for that open; later verbs reuse the command's stream, and the other thread's stream
    # serves only its own caller.
    core = massive_session._core
    gated = _Gated(fake_streams)

    def locking(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = gated.open_stream(spec, request, channel, stream_id=stream_id)
        with shared_lock(core.exec_ctx.update_graph):  # as a watchlist's listener starts
            return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(locking))
    q = stream_query(massive_session)
    made: list[Table] = []

    def command() -> None:
        gated.gate.set()  # the open goes on, and then waits for the lock this command holds
        made.append(q.append_only())

    got: dict[str, Table] = {}

    def opening() -> None:
        got["first"] = q.blink()

    def waiting_verb() -> None:  # a thread without the lock waits for the first open
        got["woken"] = q.ring(3)

    # Daemons, never a pool's threads: a regression deadlocks these threads behind the lock the
    # command keeps, and the interpreter would join a pool's threads at exit forever.
    opener = threading.Thread(target=opening, daemon=True)
    waiter = threading.Thread(target=waiting_verb, daemon=True)
    waiting = threading.Event()
    opener.start()
    assert gated.entered.wait(TIMEOUT)
    helper = _when_waiting(core, monkeypatch, waiting.set)
    waiter.start()
    assert waiting.wait(TIMEOUT)
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.session_core"):
        console_command(command)
    # The second row of the key in m.streams() is explained.
    assert caplog.messages == [
        (
            f"{label(massive_session)}: test.trades stream 2 opens alongside stream 1 for AAPL, "
            "which another thread is still opening: this call holds the update graph's lock (a "
            "console command) and cannot wait for that open, so m.streams() shows both; later "
            "calls reuse stream 2"
        )
    ]
    for thread in (opener, waiter, helper):
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    first, woken = got["first"], got["woken"]  # woken: found the new stream once the first opened
    assert gated.calls == 2
    assert [(r["StreamId"], r["HeldResults"]) for r in stream_rows()] == [(1, 1), (2, 2)]
    reused = q.ring(5)  # the key's stream is the command's
    assert gated.calls == 2
    assert [(r["StreamId"], r["HeldResults"]) for r in stream_rows()] == [(1, 1), (2, 3)]
    for result in (first, woken, reused, *made):
        massive_session.release(result)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert fake_streams.shutdown[2].wait(TIMEOUT)
    made.clear()
    got.clear()


@pytest.mark.parametrize("failure", [RuntimeError("refused"), KeyboardInterrupt()])
def test_a_waiter_opens_the_stream_when_the_opener_fails(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
    failure: BaseException,
) -> None:
    core = massive_session._core
    gated = _Gated(fake_streams, fail_first=failure)
    monkeypatch.setattr(core, "stream_transport", lambda: gated)
    q = stream_query(massive_session)
    with ThreadPoolExecutor(1) as pool:
        opener = pool.submit(q.blink)
        assert gated.entered.wait(TIMEOUT)
        helper = _when_waiting(core, monkeypatch, gated.gate.set)
        waiter = q.append_only()  # wakes when the open fails, then opens the stream itself
        assert opener.exception(TIMEOUT) is failure
    helper.join(TIMEOUT)
    assert gated.calls == 2
    # The failed open left nothing behind: no row, no record; the waiter's stream is id 2.
    assert [(r["StreamId"], r["HeldResults"]) for r in stream_rows()] == [(2, 1)]
    assert list(core._streams) == [2]
    massive_session.release(waiter)
    assert fake_streams.shutdown[2].wait(TIMEOUT)
    failure.__traceback__ = None  # the parameter outlives the test; its traceback holds the core


def test_close_during_an_open_wakes_the_waiters(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    gated = _Gated(fake_streams)
    monkeypatch.setattr(core, "stream_transport", lambda: gated)
    q = stream_query(massive_session)

    def close_then_open() -> None:
        core.close()
        gated.gate.set()

    with ThreadPoolExecutor(1) as pool:
        opener = pool.submit(q.blink)
        assert gated.entered.wait(TIMEOUT)
        helper = _when_waiting(core, monkeypatch, close_then_open)
        with pytest.raises(dm.MassiveError, match=r"is closed$"):
            q.append_only()
        error = opener.exception(TIMEOUT)
    helper.join(TIMEOUT)
    assert isinstance(error, dm.MassiveError)
    assert "was closed while a stream was opening" in str(error)
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the opener released the new stream
    assert core._streams == {}


def test_a_failure_during_the_open_is_shown_and_never_reused(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    core = massive_session._core

    # The server's rejection arrives on the WebSocket thread before the open returns.
    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        if stream_id == 1:
            core.fail_stream(stream_id, "not authorized")
        return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    q = stream_query(massive_session)
    # As if the failure had come just after the open: no result, and the hold is released.
    with pytest.raises(dm.MassiveError, match=r"^test\.trades stream 1 failed: not authorized$"):
        q.blink()
    second = q.blink()  # a fresh stream: the failed one is never reused
    rows = sorted(stream_rows(), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["State"], r["Error"], r["HeldResults"]) for r in rows] == [
        (1, "FAILED", "not authorized", 0),
        (2, "RUNNING", None, 1),
    ]
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    massive_session.release(second)
    assert fake_streams.shutdown[2].wait(TIMEOUT)


def test_an_interrupt_before_the_open_starts_leaves_no_opening_record(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A KeyboardInterrupt landing after the claim registered the record and before the open
    # began: the record must be deleted and readied, or every later verb on the key would wait
    # for ever.
    core = massive_session._core
    opened = core._open

    def interrupted(*_args: object, **_kwargs: object) -> StreamHandle:
        raise KeyboardInterrupt

    monkeypatch.setattr(core, "_open", interrupted)
    q = stream_query(massive_session)
    with pytest.raises(KeyboardInterrupt):
        q.blink()
    assert core._streams == {}
    assert core._stream_ids == {}
    monkeypatch.setattr(core, "_open", opened)
    with ThreadPoolExecutor(1) as pool:  # a later verb on the key opens at once
        result = pool.submit(q.blink).result(TIMEOUT)
    assert list(core._streams) == [2]
    massive_session.release(result)


def test_an_interrupt_right_after_a_reuse_claim_leaks_no_hold(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The reuse path: _claim took one more hold on stream 1 and returned its handle, and a
    # KeyboardInterrupt lands before the verb builds its result. That hold must be released.
    core = massive_session._core
    first = stream_query(massive_session).blink()
    claim = core._claim

    def interrupting(
        spec: DatasetSpec,
        request: Request,
        key: StreamKey,
        *,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(spec, request, key, may_wait=may_wait, holds=holds)
        if isinstance(found, StreamHandle):
            raise KeyboardInterrupt
        return found

    monkeypatch.setattr(core, "_claim", interrupting)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    assert core._streams[1].row.held_results == 1  # only the first result's hold
    massive_session.release(first)
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # so its release shuts the stream down
    assert core._streams == {}
    assert core._stream_ids == {}


def test_an_interrupt_before_the_record_is_stored_is_not_masked(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A KeyboardInterrupt landing while _claim builds the OPENING record, before it is
    # stored: the cleanup must find nothing to release, so the interrupt itself propagates
    # (not a KeyError from releasing a record that was never stored).
    core = massive_session._core
    state_class = _core._StreamEntry

    def interrupted(*_args: object, **_kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(_core, "_StreamEntry", interrupted)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    monkeypatch.setattr(_core, "_StreamEntry", state_class)
    assert core._streams == {}
    assert core._stream_ids == {}


def test_an_interrupt_before_the_opened_stream_is_stored_releases_it(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The transport returned the stream; a KeyboardInterrupt lands before _open stores its
    # handle (here, at the exit of `entered`). Nobody else knows the stream, so _open must
    # release its scope itself.
    core = massive_session._core
    armed: list[bool] = []
    real_entered = entered

    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        armed.append(True)
        return handle

    @contextlib.contextmanager
    def interrupting_entered(exec_ctx: ExecutionContext) -> Iterator[ExecutionContext]:
        with real_entered(exec_ctx):
            yield exec_ctx
        if armed:
            armed.clear()
            raise KeyboardInterrupt

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    monkeypatch.setattr(_core, "entered", interrupting_entered)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    assert core._streams == {}
    assert core._stream_ids == {}
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the opened stream was released


def test_an_interrupt_after_the_open_releases_the_stream(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A KeyboardInterrupt inside the open, after the transport returned and the registry was
    # updated: the waiters wake and the opener's hold is released with the stream's scope.
    core = massive_session._core
    write = Diagnostics.write
    interrupted: list[bool] = []

    def interrupting(self: Diagnostics, row: ConnectionRow | StreamRow) -> None:
        if not interrupted:
            interrupted.append(True)
            raise KeyboardInterrupt
        write(self, row)

    monkeypatch.setattr(Diagnostics, "write", interrupting)
    q = stream_query(massive_session)
    with pytest.raises(KeyboardInterrupt):
        q.blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the opener's hold was released
    assert core._streams == {}
    assert core._stream_ids == {}
    with ThreadPoolExecutor(1) as pool:
        result = pool.submit(q.blink).result(TIMEOUT)  # no verb waits on the dead open
    massive_session.release(result)


@pytest.mark.parametrize("report", ["fail", "update"])
def test_an_open_that_raises_leaves_no_row(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
    report: str,
) -> None:
    # The transport reports on the stream (a server rejection, or a flush) and then its open
    # raises: the record is deleted, and no row, stale for ever, is left in m.streams().
    core = massive_session._core

    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        if report == "fail":
            core.fail_stream(stream_id, "not authorized")
        else:
            core.update_streams({stream_id: StreamUpdate(rows_published=3)})
        raise RuntimeError("socket closed")

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    with pytest.raises(RuntimeError, match="socket closed"):
        stream_query(massive_session).blink()
    assert core._streams == {}
    assert stream_rows() == []


def test_an_update_made_during_the_open_is_kept(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # A reactive stream's seed reported by the transport before its open returns.
    core = massive_session._core

    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        core.update_streams({stream_id: StreamUpdate(symbol_count=7, rows_published=3)})
        return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    result = stream_query(massive_session).append_only()
    columns = ("StreamId", "SymbolCount", "RowsPublished", "Host", "State")
    assert [tuple(r[c] for c in columns) for r in stream_rows()] == [
        (1, 7, 3, "socket.test", "RUNNING")
    ]
    massive_session.release(result)


def test_stream_ids_come_from_one_allocator(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    core = massive_session._core
    assert core.next_stream_id() == 1  # the id a poll() row takes
    result = stream_query(massive_session).blink()
    assert list(core._streams) == [2]  # the WebSocket stream never reuses it
    assert core.next_stream_id() == 3
    massive_session.release(result)


def test_close_wakes_a_waiter_before_the_open_finishes(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    gated = _Gated(fake_streams)
    monkeypatch.setattr(core, "stream_transport", lambda: gated)
    q = stream_query(massive_session)
    waiting = threading.Event()
    helper = _when_waiting(core, monkeypatch, waiting.set)
    with ThreadPoolExecutor(2) as pool:
        opener = pool.submit(q.blink)
        assert gated.entered.wait(TIMEOUT)
        waiter = pool.submit(q.append_only)
        assert waiting.wait(TIMEOUT)
        core.close()
        # close() alone must release the waiter: the open is still blocked on the gate.
        with pytest.raises(dm.MassiveError, match="is closed"):
            waiter.result(TIMEOUT)
        gated.gate.set()
        assert isinstance(opener.exception(TIMEOUT), dm.MassiveError)
    helper.join(TIMEOUT)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_close_during_an_open_writes_no_row_for_the_opening_stream(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    gated = _Gated(fake_streams)
    monkeypatch.setattr(core, "stream_transport", lambda: gated)
    keeper = new_scope()  # keeps the diagnostics table readable after close() releases it
    streams = massive_session.streams()
    keeper.manage(streams)
    q = stream_query(massive_session)
    with ThreadPoolExecutor(1) as pool:
        opener = pool.submit(q.blink)
        assert gated.entered.wait(TIMEOUT)
        massive_session.close()
        gated.gate.set()
        assert isinstance(opener.exception(TIMEOUT), dm.MassiveError)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert dh_arrow.to_arrow(streams).to_pylist() == []  # m.streams() has no OPENING state
    keeper.release()


# --- Record lifetime: holds and the shutdown callback, in any order --------------------------


def test_a_failure_that_shuts_the_stream_down_while_held(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    drain: Callable[[], None],
    caplog: pytest.LogCaptureFixture,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    q = stream_query(massive_session)
    first = q.append_only()
    core = massive_session._core
    # publish_failure shuts the blink table down at once, while the result is still held.
    fake_streams.publishers[1].publish_failure(RuntimeError("entitlement"))
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    with core.registry_lock:
        assert core._streams[1].shut_down
        assert core._stream_ids == {}  # the dead stream is never reused
    core.fail_stream(1, "entitlement")  # still shown: the record outlives the shutdown
    second = q.blink()  # a fresh stream, no KeyError
    caplog.set_level(logging.ERROR)
    massive_session.release(first)
    drain()
    assert caplog.records == []  # the reaper job succeeded
    with core.registry_lock:
        assert list(core._streams) == [2]  # the last hold deleted the shut-down record
    rows = sorted(stream_rows(), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["State"], r["Error"], r["HeldResults"]) for r in rows] == [
        (1, "FAILED", "entitlement", 0),
        (2, "RUNNING", None, 1),
    ]
    massive_session.release(second)


_EVENTS = ("release", "fail", "shut_down", "close")


def _spy_stream_rows(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, str]]:
    # Records every stream row written, as (stream id, state), in order.
    written: list[tuple[int, str]] = []
    write = Diagnostics.write

    def spying(self: Diagnostics, row: ConnectionRow | StreamRow) -> None:
        if isinstance(row, StreamRow):
            written.append((row.stream_id, row.state.value))
        write(self, row)

    monkeypatch.setattr(Diagnostics, "write", spying)
    return written


@pytest.mark.parametrize("order", list(itertools.permutations(_EVENTS)))
def test_record_lifetime_in_any_order(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
    order: tuple[str, ...],
) -> None:
    core = massive_session._core
    written = _spy_stream_rows(monkeypatch)
    q = stream_query(massive_session)
    result = q.append_only()
    key = next(iter(core._stream_ids))
    user_scope = new_scope()  # keeps the blink table alive: shutdown happens only on demand
    with user_scope.open():
        derived = result.tail(1)
    # The model: what the registry must hold after each event.
    present, held, shut, mapped, state = True, 1, False, True, "RUNNING"
    for event in order:
        written.clear()
        expect_row = present
        if event == "release":
            massive_session.release(result)
            drain()
            held, mapped = 0, False
            state = "RELEASED" if state == "RUNNING" else state
            present = not shut
            expect_row = True
        elif event == "fail":
            core.fail_stream(1, "boom")
            mapped = False
            expect_row = present and state != "CLOSED"  # a close is final
            state = "FAILED" if expect_row else state
        elif event == "shut_down":
            core.stream_shut_down(1)  # what the blink table's shutdown callback does
            expect_row = False
            mapped = False
            shut = present and held > 0
            present = present and held > 0
        else:
            running = present and not shut and state in {"RUNNING", "RELEASED"}
            core.close()
            state = "CLOSED" if present and state != "FAILED" else state
            if running:  # the close failed the table; its shutdown follows a cycle later
                assert fake_streams.shutdown[1].wait(TIMEOUT)
                mapped, shut, present = False, held > 0, held > 0
        with core.registry_lock:
            assert (1 in core._streams) is present, event
            assert (core._stream_ids.get(key) == 1) is mapped, event
            if present:
                assert core._streams[1].row.state.value == state, event
                assert core._streams[1].shut_down is shut, event
        assert written == ([(1, state)] if expect_row else []), event
    user_scope.release()
    del derived
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core._streams == {}


# --- Interrupt windows inside the verb's own bookkeeping -------------------------------------


def _cleanup_loop_line() -> int:
    # The line of the first `for ... in holds` loop of `_acquire`, the success path's.
    lines, start = inspect.getsourcelines(_core.SessionCore._acquire)
    for offset, text in enumerate(lines):
        if "for stream_id, ready in holds" in text or "for _, ready in holds" in text:
            return start + offset
    raise AssertionError("_acquire has no loop over its holds")


@contextlib.contextmanager
def _interrupt_at_the_cleanup_back_edge() -> Iterator[list[bool]]:
    # Raise KeyboardInterrupt at the loop's backward jump (an eval-breaker checkpoint) once
    # `_acquire` has the handle it is about to return. Restores the previous trace function,
    # so a line-tracing coverage tool keeps working afterwards.
    line = _cleanup_loop_line()
    visits: list[int] = []
    fired: list[bool] = []

    def local(frame: FrameType, event: str, arg: object) -> object:
        if event == "line" and frame.f_lineno == line and not fired:
            visits.append(1)
            got = frame.f_locals.get("handle", frame.f_locals.get("claim"))
            if len(visits) == 2 and isinstance(got, StreamHandle):
                fired.append(True)
                raise KeyboardInterrupt
        return local

    def tracer(frame: FrameType, event: str, arg: object) -> object:
        if frame.f_code is _core.SessionCore._acquire.__code__:
            visits.clear()
            return local
        return None

    previous = sys.gettrace()
    sys.settrace(cast("Any", tracer))
    try:
        yield fired
    finally:
        sys.settrace(previous)


def test_an_interrupt_while_acquire_returns_a_reused_stream_leaks_no_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    core = massive_session._core
    first = stream_query(massive_session).blink()
    with _interrupt_at_the_cleanup_back_edge() as fired, pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    assert fired
    assert core._streams[1].row.held_results == 1  # only the first result's hold
    massive_session.release(first)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_an_interrupt_while_acquire_returns_a_new_stream_leaks_no_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    core = massive_session._core
    with _interrupt_at_the_cleanup_back_edge() as fired, pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    assert fired
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the opener's hold was released
    assert core._streams == {}


def test_an_interrupt_while_a_reactive_key_is_stored_leaves_no_record(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A reactive key holds a SymbolTable box, whose dataclass __hash__ is Python code, so the
    # key store in _claim is an interruptible checkpoint wherever the key's tuple hash is not
    # already cached (CPython before 3.14 caches no tuple hash). The store raises here on
    # every Python, as an interrupt inside that hash would, before anything is stored: the
    # interrupt must leave neither an OPENING record nor a key behind.
    core = massive_session._core
    stored: list[object] = []

    class InterruptedStore(dict[tuple[object, ...], int]):
        @override
        def __setitem__(self, key: tuple[object, ...], value: int) -> None:
            if not stored:  # the first store only: _claim's, before the record is stored
                stored.append(key[2])
                raise KeyboardInterrupt
            super().__setitem__(key, value)

    monkeypatch.setattr(core, "_stream_ids", InterruptedStore())
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session, watchlist).blink()
    assert len(stored) == 1
    assert isinstance(stored[0], SymbolTable)  # a refreshing table keeps the key reactive
    assert core._streams == {}
    assert core._stream_ids == {}
    assert fake_streams.opened == []


def test_an_interrupt_after_make_result_releases_the_result_scope(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The interrupt lands at the exit of `entered`, after make_result returned and before the
    # result is tracked: both the result scope and the hold are this call's to release.
    core = massive_session._core
    real_make = make_result
    real_entered = entered
    armed: list[bool] = []

    def making(build: Callable[[], Table]) -> tuple[Table, LivenessScope]:
        made = real_make(build)
        armed.append(True)
        return made

    @contextlib.contextmanager
    def interrupting_entered(exec_ctx: ExecutionContext) -> Iterator[ExecutionContext]:
        with real_entered(exec_ctx):
            yield exec_ctx
        if armed:
            armed.clear()
            raise KeyboardInterrupt

    monkeypatch.setattr(_core, "make_result", making)
    monkeypatch.setattr(_core, "entered", interrupting_entered)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    monkeypatch.setattr(_core, "entered", real_entered)
    gc.collect()
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # nothing keeps the stream alive
    assert core._streams == {}


# --- Lifecycle: closers, collection, guards, and failures reported to the core ---------------


def test_a_verb_racing_close_does_not_reuse_the_closed_stream(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core
    q = stream_query(massive_session)
    first = q.blink()
    run_verb = core.run_verb

    def guard_then_close(call: str, body: Callable[[], Table]) -> Table:
        # The verb passes the guard, then another thread closes the session.
        def closing() -> Table:
            core.close()
            return body()

        return run_verb(call, closing)

    monkeypatch.setattr(core, "run_verb", guard_then_close)
    with pytest.raises(dm.MassiveError, match=f"^{label(massive_session)} is closed$"):
        q.append_only()
    assert len(fake_streams.opened) == 1
    assert core.results.held() == 1  # no hold was taken on the closed stream
    massive_session.release(first)


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
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        del m
        gc.collect()
        reaper.join(TIMEOUT)
    assert not reaper.is_alive()
    assert ran == ["closed"]
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.WARNING,
            f"{name} was garbage-collected without close(); closing its connections and pollers",
        ),
        (logging.ERROR, f"{name}: a transport failed to close"),
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


def test_a_reaper_that_cannot_start_stops_the_diagnostics_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Diagnostics] = []
    real = Diagnostics

    def spying(
        exec_ctx: ExecutionContext,
        name: str,
        on_death: Callable[[BaseException], None] | None = None,
    ) -> Diagnostics:
        diagnostics = real(exec_ctx, name, on_death=on_death)
        created.append(diagnostics)
        return diagnostics

    def no_thread(exec_ctx: ExecutionContext, name: str, **_: object) -> Reaper:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(_core, "Diagnostics", spying)
    monkeypatch.setattr(_core, "Reaper", no_thread)
    with pytest.raises(RuntimeError, match="can't start new thread"):
        dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    (diagnostics,) = created
    diagnostics.thread.join(TIMEOUT)
    assert not diagnostics.thread.is_alive()  # stopped, with its tables released


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


def test_a_result_keeps_its_session_alive() -> None:
    # A result that outlives every reference to its session and query must still be released
    # when it is collected: the release path (registry, reaper, core) lives as long as it does.
    # An independent fake is needed for the same reason as in the test above.
    fakes = importlib.import_module("tests._fakes.stream")
    m = dm.MassiveSession(dm.MassiveConfig(api_key="outliving-result"))
    core = m._core
    fake = fakes.FakeStreamTransport(core)
    vars(core)["stream_transport"] = lambda: fake
    alive = weakref.ref(core)
    reaper = core._reaper.thread
    result = stream_query(m).append_only()
    del core, m
    gc.collect()
    assert alive() is not None
    assert reaper.is_alive()
    assert not fake.shutdown[1].is_set()
    del result
    gc.collect()
    assert fake.shutdown[1].wait(TIMEOUT)
    pause = threading.Event()  # never set; only paces the bounded wait below
    for _ in range(200):
        gc.collect()
        if alive() is None:
            break
        pause.wait(0.05)
    assert alive() is None
    reaper.join(TIMEOUT)
    assert not reaper.is_alive()


def test_a_failed_stream_is_not_reused(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    q = stream_query(massive_session)
    first = q.blink()
    massive_session._core.fail_stream(1, "not authorized")
    massive_session._core.fail_stream(1, "not authorized")  # its key is already unregistered
    second = q.blink()
    assert len(fake_streams.opened) == 2
    rows = stream_rows()
    assert [(r["StreamId"], r["State"], r["Error"]) for r in rows] == [
        (1, "FAILED", "not authorized"),
        (2, "RUNNING", None),
    ]
    massive_session.release(first)  # the failed stream's key already serves stream 2
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    third = q.append_only()
    assert len(fake_streams.opened) == 2  # still served by stream 2
    massive_session.release(second)
    massive_session.release(third)
    assert fake_streams.shutdown[2].wait(TIMEOUT)


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


def test_a_long_verb_in_a_console_command_is_reported_once(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    # While a console command holds the update graph's lock, no live table updates: a verb that
    # runs long there says so, once per session. Every verb counts as long here.
    q = stream_query(massive_session)
    made: list[Table] = []
    monkeypatch.setattr(_core, "PAUSED_TABLES_WARNING_SECONDS", 0.0)
    made.append(q.blink())  # without the lock: nothing is paused, nothing is reported
    assert caplog.messages == []

    def command() -> None:
        made.append(q.append_only())
        made.append(q.ring(5))

    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        console_command(command)
    (message,) = caplog.messages
    assert message.startswith(f"{label(massive_session)}: test.trades append_only() ran for ")
    assert "live tables do not update until the command ends; run long downloads" in message
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


def test_a_failure_after_the_last_release_is_shown(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # The library released its last result, but a derived table keeps the blink table, and so
    # the publisher, alive; the transport can still report a failure for that stream.
    q = stream_query(massive_session)
    result = q.append_only()
    user_scope = new_scope()
    with user_scope.open():
        derived = result.tail(1)
    massive_session.release(result)
    assert [r["State"] for r in stream_rows()] == ["RELEASED"]
    massive_session._core.fail_stream(1, "not authorized")
    assert [(r["State"], r["Error"], r["HeldResults"]) for r in stream_rows()] == [
        ("FAILED", "not authorized", 0)
    ]
    assert derived.is_refreshing
    again = q.blink()  # the released stream's key was already free: a fresh stream opens
    assert len(fake_streams.opened) == 2
    assert list(fake_streams.shutdown) == [1, 2]  # the new verb opened stream 2
    massive_session.release(again)
    user_scope.release()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    # The stream has shut down and left the registry: a failure now is only logged.
    massive_session._core.fail_stream(1, "later")
    rows = sorted(stream_rows(), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["State"], r["Error"]) for r in rows] == [
        (1, "FAILED", "not authorized"),
        (2, "RELEASED", None),
    ]


def test_an_interrupt_while_tracking_releases_the_scope_and_the_hold(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = massive_session._core

    def interrupted(*_args: object) -> ResultRecord:
        raise KeyboardInterrupt  # delivered on the caller thread right after make_result

    monkeypatch.setattr(ResultRegistry, "track", interrupted)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    # The result scope and the stream hold were both released, so the stream shut down.
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core._streams == {}
    assert core.results.held() == 0


def test_a_result_that_outlives_its_session_is_still_released() -> None:
    # A result whose release callback does not hold the core (a future poll() table's
    # poller, for example) can outlive the session. The reaper must outlive it too, or the
    # result's scope would never be released once it is collected.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="outliving-poll"))
    core = m._core
    released = threading.Event()
    with entered(core.exec_ctx):
        result, scope = make_result(lambda: time_table("PT1S"))
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
    # No reference cycle runs through a core: a result record's owner callback holds it only
    # weakly, so reference counting alone frees a session once nothing uses it. The fake is
    # imported directly, not taken from the `fake_streams` fixture: that fixture is bound to the
    # fixture session, and this test must own the only reference to the core it watches.
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
        callback = cast("functools.partial[None]", record.on_released)
        assert isinstance(callback.args[0], weakref.ref)  # the callback holds the core weakly
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
            1: StreamUpdate(rows_published=5, host="socket.other", error="slow consumer"),
            99: StreamUpdate(rows_published=1),  # left the registry: ignored
        }
    )
    columns = ("StreamId", "State", "RowsPublished", "Host", "Error")
    assert [tuple(r[c] for c in columns) for r in stream_rows()] == [
        (1, "RUNNING", 5, "socket.other", "slow consumer")  # an update never fails a stream
    ]
    massive_session.release(result)
    drain()
    assert core._streams == {}
    core.update_streams({1: StreamUpdate(rows_published=6)})  # deleted: nothing changes
    assert [r["RowsPublished"] for r in stream_rows()] == [5]


def test_a_shutdown_of_an_unknown_stream_is_ignored(massive_session: dm.MassiveSession) -> None:
    # Called on this thread so that coverage sees it on every Python version; a blink table's
    # shutdown callback runs on a Deephaven thread.
    massive_session._core.stream_shut_down(99)
    assert massive_session._core._streams == {}


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
        core.fail_stream(1, "shown")  # in the registry: shown in m.streams(), not logged
        core.fail_stream(42, "late")  # never registered: only logged
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.DEBUG, f"{core.label}: stream 42 failed after it left the registry: late")
    ]
    massive_session.release(result)


def test_shaping_a_stream_that_failed_during_its_open_releases_the_hold(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
) -> None:
    # Every shape, blink() included, raises the MassiveError naming the failure, and the hold
    # is released.
    core = massive_session._core

    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        fake_streams.publishers[stream_id].publish_failure(RuntimeError("entitlement"))
        assert fake_streams.shutdown[stream_id].wait(TIMEOUT)
        core.fail_stream(stream_id, "entitlement")
        return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    for verb in ("blink", "append_only"):
        with pytest.raises(dm.MassiveError, match="failed: entitlement"):
            getattr(stream_query(massive_session), verb)()
    drain()
    assert core.results.held() == 0
    assert core._streams == {}
    assert core._stream_ids == {}


def test_a_failure_while_the_shape_is_derived_is_named(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    # The failure lands between the check and the shape: Deephaven's DHError is wrapped.
    core = massive_session._core
    q = stream_query(massive_session)

    def failing_then_shaping(blink: Table) -> Table:
        fake_streams.publishers[1].publish_failure(RuntimeError("entitlement"))
        assert fake_streams.shutdown[1].wait(TIMEOUT)
        core.fail_stream(1, "entitlement")
        return blink_to_append_only(blink)

    with pytest.raises(dm.MassiveError, match=r"stream 1 failed: entitlement") as raised:
        core.stream_result(SPEC, q._request, channel="T", build=failing_then_shaping)
    assert isinstance(raised.value.__cause__, DHError)
    drain()
    assert core.results.held() == 0


def test_a_failure_while_the_blink_copy_is_described_is_named(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    # `describe` calls Java directly, so a failed blink table surfaces as jpy's RuntimeError.
    core = massive_session._core
    q = stream_query(massive_session)
    schema = core.catalog.dataset_schema(SPEC)

    def failing_then_describing(blink: Table) -> Table:
        fake_streams.publishers[1].publish_failure(RuntimeError("entitlement"))
        assert fake_streams.shutdown[1].wait(TIMEOUT)
        core.fail_stream(1, "entitlement")
        return describe(blink, SPEC, schema)

    with pytest.raises(dm.MassiveError, match=r"stream 1 failed: entitlement") as raised:
        core.stream_result(SPEC, q._request, channel="T", build=failing_then_describing)
    assert type(raised.value.__cause__) is RuntimeError
    drain()
    assert core.results.held() == 0


def test_a_deephaven_error_of_a_live_stream_is_not_wrapped(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    q = stream_query(massive_session)

    def broken(blink: Table) -> Table:
        # A shape Deephaven refuses for a reason of its own: ring() of a negative capacity.
        return ring_table(blink, -1)

    with pytest.raises(DHError):
        massive_session._core.stream_result(SPEC, q._request, channel="T", build=broken)


# --- The ingest ports and the catalog --------------------------------------------------------


_REST = cast("_transports.RestTransport", object())
_STREAMS = cast("_transports.StreamTransport", object())
_FLAT_FILES = cast("_transports.FlatFileTransport", object())


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
        assert ports.asked == ["rest", "streams", "flat_files"]
        with pytest.raises(RuntimeError, match=r"^core already has its transports attached$"):
            core.attach(_Ports())
        assert core.rest_transport() is _REST
        assert ports.asked == ["rest", "streams", "flat_files", "rest"]
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
    core.record_stream_drift(1, "WS", HALTED)
    core.record_stream_drift(1, "WS", HALTED)  # nothing new: the row is not rewritten
    core.record_stream_drift(1, "WS", DriftReport(missing=frozenset({"trfi"})))
    core.record_stream_drift(99, "WS", HALTED)  # not in the registry: ignored, not kept
    assert [(r["StreamId"], r["SchemaDrift"], r["Error"]) for r in stream_rows()] == [
        (1, "WS:halted+, WS:trfi-", None)
    ]
    assert core.drift.record(99, "WS", HALTED) == "WS:halted+"  # nothing was kept for 99
    # Released, then shut down: the record leaves the registry at the shutdown.
    massive_session.release(result)
    drain()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core._streams == {}
    assert core.drift.record(1, "WS", HALTED) == "WS:halted+"  # forgotten, so new again
    assert [r["SchemaDrift"] for r in stream_rows()] == ["WS:halted+, WS:trfi-"]  # last row


def test_stream_drift_is_forgotten_when_the_last_hold_of_a_shut_down_stream_goes(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    drain: Callable[[], None],
) -> None:
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    core.record_stream_drift(1, "WS", HALTED)
    # Shut down while held: the record stays until the hold is released.
    fake_streams.publishers[1].publish_failure(RuntimeError("gone"))
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.drift.record(1, "WS", HALTED) is None  # still remembered
    massive_session.release(result)
    drain()
    assert core._streams == {}
    assert core.drift.record(1, "WS", HALTED) == "WS:halted+"


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
        core.record_stream_drift(stream_id, "WS", HALTED)  # the row is not written yet
        return handle

    monkeypatch.setattr(core, "stream_transport", transport_of(open_stream))
    result = stream_query(massive_session).append_only()
    assert [(r["StreamId"], r["SchemaDrift"]) for r in stream_rows()] == [(1, "WS:halted+")]
    massive_session.release(result)


def test_the_stream_lifecycle_is_logged(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = stream_query(massive_session)
    with caplog.at_level(logging.DEBUG, logger=_core.__name__):
        first = q.blink()
        second = q.append_only()
        massive_session.release(first)
        massive_session.release(second)
        assert fake_streams.shutdown[1].wait(TIMEOUT)  # the last release has run
    assert [m for m in caplog.messages if m.startswith("stream 1")] == [
        "stream 1 opened: test.trades channel T on socket.test/stocks",
        "stream 1 reused: test.trades channel T",
        "stream 1: a hold released, 1 left",
        "stream 1: a hold released, 0 left; its stream scope is released",
    ]


# --- The live set, failing on close, diagnostics views, and dying threads -----------------------


class _Fatal(BaseException):
    # Like KeyboardInterrupt: a reaper job that raises it ends the thread.
    pass


def test_a_running_stream_keeps_its_session_in_the_live_set(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    core = massive_session._core
    assert id(core) not in _core._live  # an idle session is held only by its user
    result = stream_query(massive_session).append_only()
    assert _core._live[id(core)] is core
    massive_session.release(result)
    drain()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    drain()
    assert id(core) not in _core._live
    view = massive_session.streams()  # a held diagnostics view counts too
    assert _core._live[id(core)] is core
    massive_session.release(view)
    drain()
    assert id(core) not in _core._live


def test_a_stream_that_shuts_down_while_opening_stops_its_watchlist_listener(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    drain: Callable[[], None],
) -> None:
    # The shutdown ran before the handle reached the registry, so it had nothing to stop the
    # listener through; storing the handle stops it.
    core = massive_session._core
    stopped = threading.Event()
    open_stream = fake_streams.open_stream

    def shut_down_while_opening(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = open_stream(spec, request, channel, stream_id=stream_id)
        core.stream_shut_down(stream_id)  # what an early shutdown callback does
        return dataclasses.replace(handle, stop_watch=stopped.set)

    monkeypatch.setattr(fake_streams, "open_stream", shut_down_while_opening)
    result = stream_query(massive_session).append_only()  # a shutdown is not a failure
    drain()
    assert stopped.is_set()
    massive_session.release(result)
    drain()
    assert core._streams == {}


def test_a_verb_that_races_close_never_returns_its_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    # The session closes while the result is built: the verb releases it and raises.
    core = massive_session._core
    spec = SPEC

    def build_and_close(blink: Table) -> Table:
        shaped = blink_to_append_only(blink)
        massive_session.close()
        return shaped

    with pytest.raises(dm.MassiveError, match=f"^{label(massive_session)} is closed$"):
        core.stream_result(spec, Request(symbols=("AAPL",)), channel="T", build=build_and_close)
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # failed by the close, then released


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
    assert id(core) not in _core._live


def test_a_diagnostics_view_that_races_close_raises_the_closed_error(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The close releases the diagnostics tables while the view is built.
    core = massive_session._core
    build = make_result

    def close_first(factory: Callable[[], Table]) -> object:
        massive_session.close()
        return build(factory)

    monkeypatch.setattr(_core, "make_result", close_first)
    with pytest.raises(dm.MassiveError, match=f"^{label(massive_session)} is closed$") as caught:
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
    assert f"{label(massive_session)}: could not fail a live table" in caplog.text
    assert f"{label(massive_session)} closed; 1 live tables failed" in caplog.text
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
    assert text.endswith("this table no longer updates")
    assert core.diagnostics.barrier().wait(TIMEOUT)
    (row,) = dh_to_pylist(core.diagnostics.streams)  # no view: nothing could release it
    assert (row["State"], row["Error"]) == ("FAILED", text)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    # Nothing can release the hold any more: a dead reaper leaks its session. Drop the hold
    # here, then the job it left in the dead reaper's queue, so the session can go.
    del result
    core._release_hold(1)
    queued = core._reaper._worker._queue
    while not queued.empty():
        queued.get_nowait()


def test_a_thread_of_a_freed_session_dies_quietly() -> None:
    class _Gone:
        pass

    ref = weakref.ref(_Gone())  # already dead
    _core.SessionCore._thread_died(cast("weakref.ref[SessionCore]", ref), "reaper", _Fatal())
