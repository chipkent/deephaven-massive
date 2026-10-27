"""Tests for `SessionCore`: opening streams, the record lifetime, and interrupt windows."""

from __future__ import annotations

import contextlib
import gc
import inspect
import itertools
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.schema import TRADES
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import StreamQuery
from deephaven_massive._tables import core as _core
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    Diagnostics,
    StreamRow,
    StreamUpdate,
)
from deephaven_massive._tables.liveness import (
    StreamHandle,
    entered,
    make_result,
    new_scope,
)

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable, Iterator
    from types import FrameType

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.request import Request, StreamKey
    from tests._fakes.stream import FakeStreamTransport

TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    query_class=StreamQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
)


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, StreamQuery)
    return q


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
        self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
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
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(spec, request, key, holds=holds)
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

    class FailingDuringOpen:
        # The server's rejection arrives on the WebSocket thread before the open returns.
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
        ) -> StreamHandle:
            handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
            if stream_id == 1:
                core.fail_stream(stream_id, "not authorized")
            return handle

    monkeypatch.setattr(core, "stream_transport", FailingDuringOpen)
    q = stream_query(massive_session)
    first = q.blink()  # the failed table, as if the failure had come just after the open
    second = q.blink()  # a fresh stream: the failed one is never reused
    rows = sorted(stream_rows(), key=lambda r: r["StreamId"])
    assert [(r["StreamId"], r["State"], r["Error"]) for r in rows] == [
        (1, "FAILED", "not authorized"),
        (2, "RUNNING", None),
    ]
    massive_session.release(first)
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
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(spec, request, key, holds=holds)
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
    state_class = _core._StreamState

    def interrupted(*_args: object, **_kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(_core, "_StreamState", interrupted)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    monkeypatch.setattr(_core, "_StreamState", state_class)
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

    class Arming:
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
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

    monkeypatch.setattr(core, "stream_transport", Arming)
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

    class ReportThenRaise:
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
        ) -> StreamHandle:
            if report == "fail":
                core.fail_stream(stream_id, "not authorized")
            else:
                core.update_streams({stream_id: StreamUpdate(rows_published=3)})
            raise RuntimeError("socket closed")

    monkeypatch.setattr(core, "stream_transport", ReportThenRaise)
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

    class UpdatingDuringOpen:
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
        ) -> StreamHandle:
            handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
            core.update_streams({stream_id: StreamUpdate(symbol_count=7, rows_published=3)})
            return handle

    monkeypatch.setattr(core, "stream_transport", UpdatingDuringOpen)
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
    written: list[tuple[int, str]] = []
    write = Diagnostics.write

    def spying(self: Diagnostics, row: ConnectionRow | StreamRow) -> None:
        if isinstance(row, StreamRow):
            written.append((row.stream_id, row.state.value))
        write(self, row)

    monkeypatch.setattr(Diagnostics, "write", spying)
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
            state = "FAILED" if present else state
        elif event == "shut_down":
            core.stream_shut_down(1)  # what the blink table's shutdown callback does
            expect_row = False
            mapped = False
            shut = present and held > 0
            present = present and held > 0
        else:
            core.close()
            state = "CLOSED" if present else state
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


def test_an_interrupt_while_a_reactive_key_is_hashed_leaves_no_record(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Storing a reactive key hashes its SymbolTable box in Python code, an interruptible
    # checkpoint inside _claim's stores on Pythons that do not cache a tuple's hash (3.11,
    # 3.13). The interrupt must leave neither an OPENING record nor a key behind.
    core = massive_session._core
    real_hash = SymbolTable.__hash__
    real_mint = _core._next_stream_id
    minted: list[bool] = []
    fired: list[bool] = []

    def minting() -> int:
        minted.append(True)
        return real_mint()

    def hashing(self: SymbolTable) -> int:
        if minted and not fired:  # the first hash after the id is minted: inside the stores
            fired.append(True)
            raise KeyboardInterrupt
        return real_hash(self)

    monkeypatch.setattr(_core, "_next_stream_id", minting)
    monkeypatch.setattr(SymbolTable, "__hash__", hashing)
    reactive = stream_query(massive_session, new_table([string_col("Symbol", ["AAPL"])]))
    with contextlib.suppress(KeyboardInterrupt):
        massive_session.release(reactive.blink())
    monkeypatch.setattr(SymbolTable, "__hash__", real_hash)
    if not fired:
        # Only 3.14+ caches the tuple's hash; on 3.11 and 3.13 the hash must fire, so a refactor
        # that stops minting through _next_stream_id fails there instead of skipping everywhere.
        assert sys.version_info >= (3, 14)
        pytest.skip("this Python caches the key tuple's hash: no Python code runs in the stores")
    assert core._streams == {}
    assert core._stream_ids == {}


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

    def making(build: Callable[[], Table], *, wrap: bool = False) -> tuple[Table, LivenessScope]:
        made = real_make(build, wrap=wrap)
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
