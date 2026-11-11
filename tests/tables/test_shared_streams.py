"""Tests for `SharedStreams`, a session's WebSocket streams, one per key, shared by its verbs.

They cover opening a stream (registered first, one open per key, the console's take-over),
the record lifetime across holds, failures, shutdowns, and the close, the interrupt windows of
a verb's bookkeeping, building a result over a stream that fails, and the stream's log lines.
"""

from __future__ import annotations

import contextlib
import dataclasses
import gc
import inspect
import itertools
import logging
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table
from deephaven.update_graph import shared_lock

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._namespaces.query_building import build_query
from deephaven_massive._query import QueryCall, SymbolStreamQuery
from deephaven_massive._tables import session_core as _core
from deephaven_massive._tables import shared_streams as _streams
from deephaven_massive._tables.columns import describe
from deephaven_massive._tables.diagnostics import (
    QUERY_SHOWN,
    ConnectionRow,
    Diagnostics,
    StreamRow,
    StreamUpdate,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import (
    ResultRegistry,
    make_result,
    new_scope,
    release_scope,
)
from deephaven_massive._tables.shared_streams import SharedStreams
from deephaven_massive._tables.transports import StreamHandle
from deephaven_massive._types import override
from tests._fakes.stream import transport_of

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable, Iterator
    from types import FrameType

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table
    from deephaven.table_factory import InputTable

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


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> SymbolStreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, SymbolStreamQuery)
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
    claim = core.streams._claim

    def spying(
        owner: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        key: StreamKey,
        query: str,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(owner, spec, request, key=key, query=query, may_wait=may_wait, holds=holds)
        if isinstance(found, threading.Event):
            waiting.set()
        return found

    monkeypatch.setattr(core.streams, "_claim", spying)

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


def test_only_the_verb_that_opened_a_stream_adds_its_held_rows(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    # The opener adds the rows received while the stream opened, once its result exists; a
    # verb that reuses the stream must not add them before the opener's result exists.
    first = stream_query(massive_session).blink()
    second = stream_query(massive_session).append_only()
    assert fake_streams.published_held == [1]
    massive_session.release(first)
    massive_session.release(second)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_a_failed_first_build_still_adds_the_held_rows(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Another verb may have built a result on the stream meanwhile; it must get the rows.
    def broken(_exec_ctx: object, _build: object) -> object:
        raise ValueError("the build broke")

    monkeypatch.setattr(_streams, "make_result", broken)
    with pytest.raises(ValueError, match="the build broke"):
        stream_query(massive_session).blink()
    assert fake_streams.published_held == [1]
    assert fake_streams.shutdown[1].wait(TIMEOUT)


@pytest.mark.parametrize("window", ["while the open is logged", "before the result is built"])
def test_an_interrupt_after_the_open_still_adds_the_held_rows(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    *,
    window: str,
) -> None:
    # Once the stream opens, another verb may reuse it before the opener builds its result.
    # An interrupt in either window must still add the held rows, or they pile up forever and
    # that verb's result shows none of them.
    core = massive_session._core
    if window == "while the open is logged":
        debug = _streams.logger.debug

        def interrupting_debug(message: str, *args: object) -> None:
            if message.startswith("%s: stream %d opened"):
                raise KeyboardInterrupt
            debug(message, *args)

        monkeypatch.setattr(_streams.logger, "debug", interrupting_debug)
    else:  # at the first statement after `_acquire` returns the stream it opened
        acquire = core.streams._acquire
        armed: list[bool] = []
        real_entered = entered

        def arming(
            owner: SessionCore,
            spec: DatasetSpec,
            request: Request,
            *,
            key: StreamKey,
            channel: str,
            query: str,
            verb: str,
        ) -> tuple[StreamHandle, bool]:
            found = acquire(owner, spec, request, key=key, channel=channel, query=query, verb=verb)
            armed.append(True)
            return found

        def interrupting_entered(exec_ctx: ExecutionContext) -> ExecutionContext:
            if armed:
                armed.clear()
                raise KeyboardInterrupt
            return real_entered(exec_ctx)

        monkeypatch.setattr(core.streams, "_acquire", arming)
        monkeypatch.setattr(_streams, "entered", interrupting_entered)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    assert fake_streams.published_held == [1]
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the opener's hold was released too
    assert core.streams._streams == {}


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
    with caplog.at_level(logging.INFO, logger=_streams.__name__):
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
    assert list(core.streams._streams) == [2]
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
    assert core.streams._streams == {}


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
    opened = core.streams._open

    def interrupted(*_args: object, **_kwargs: object) -> StreamHandle:
        raise KeyboardInterrupt

    monkeypatch.setattr(core.streams, "_open", interrupted)
    q = stream_query(massive_session)
    with pytest.raises(KeyboardInterrupt):
        q.blink()
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}
    monkeypatch.setattr(core.streams, "_open", opened)
    with ThreadPoolExecutor(1) as pool:  # a later verb on the key opens at once
        result = pool.submit(q.blink).result(TIMEOUT)
    assert list(core.streams._streams) == [2]
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
    claim = core.streams._claim

    def interrupting(
        owner: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        key: StreamKey,
        query: str,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        found = claim(owner, spec, request, key=key, query=query, may_wait=may_wait, holds=holds)
        if isinstance(found, StreamHandle):
            raise KeyboardInterrupt
        return found

    monkeypatch.setattr(core.streams, "_claim", interrupting)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    assert core.streams._streams[1].row.held_results == 1  # only the first result's hold
    massive_session.release(first)
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # so its release shuts the stream down
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}


def test_an_interrupt_before_the_record_is_stored_is_not_masked(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A KeyboardInterrupt landing while _claim builds the OPENING record, before it is
    # stored: the cleanup must find nothing to release, so the interrupt itself propagates
    # (not a KeyError from releasing a record that was never stored).
    core = massive_session._core
    state_class = _streams._StreamEntry

    def interrupted(*_args: object, **_kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(_streams, "_StreamEntry", interrupted)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    monkeypatch.setattr(_streams, "_StreamEntry", state_class)
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}


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
    monkeypatch.setattr(_streams, "entered", interrupting_entered)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).blink()
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}
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
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}
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
    assert core.streams._streams == {}
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
    release_scope(core.exec_ctx, keeper)


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
        assert core.streams._streams[1].shut_down
        assert core.streams._stream_ids == {}  # the dead stream is never reused
    core.fail_stream(1, "entitlement")  # still shown: the record outlives the shutdown
    second = q.blink()  # a fresh stream, no KeyError
    caplog.set_level(logging.ERROR)
    massive_session.release(first)
    drain()
    assert caplog.records == []  # the reaper job succeeded
    with core.registry_lock:
        assert list(core.streams._streams) == [2]  # the last hold deleted the shut-down record
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
    key = next(iter(core.streams._stream_ids))
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
            core.streams.shut_down(1)  # what the blink table's shutdown callback does
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
            assert (1 in core.streams._streams) is present, event
            assert (core.streams._stream_ids.get(key) == 1) is mapped, event
            if present:
                assert core.streams._streams[1].row.state.value == state, event
                assert core.streams._streams[1].shut_down is shut, event
        assert written == ([(1, state)] if expect_row else []), event
    release_scope(core.exec_ctx, user_scope)
    del derived
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.streams._streams == {}


# --- Interrupt windows inside the verb's own bookkeeping -------------------------------------


def _cleanup_loop_line() -> int:
    # The line of the first `for ... in holds` loop of `_acquire`, the success path's.
    lines, start = inspect.getsourcelines(SharedStreams._acquire_once)
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
        if frame.f_code is SharedStreams._acquire_once.__code__:
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
    assert core.streams._streams[1].row.held_results == 1  # only the first result's hold
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
    assert core.streams._streams == {}


def test_an_interrupt_while_a_reactive_key_is_stored_leaves_no_record(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dh_input_table: Callable[..., InputTable],
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

    monkeypatch.setattr(core.streams, "_stream_ids", InterruptedStore())
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session, watchlist).blink()
    assert len(stored) == 1
    assert isinstance(stored[0], SymbolTable)  # a refreshing table keeps the key reactive
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}
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

    def making(
        exec_ctx: ExecutionContext, build: Callable[[], Table]
    ) -> tuple[Table, LivenessScope]:
        made = real_make(exec_ctx, build)
        armed.append(True)
        return made

    @contextlib.contextmanager
    def interrupting_entered(exec_ctx: ExecutionContext) -> Iterator[ExecutionContext]:
        with real_entered(exec_ctx):
            yield exec_ctx
        if armed:
            armed.clear()
            raise KeyboardInterrupt

    monkeypatch.setattr(_streams, "make_result", making)
    monkeypatch.setattr(_streams, "entered", interrupting_entered)
    with pytest.raises(KeyboardInterrupt):
        stream_query(massive_session).append_only()
    monkeypatch.setattr(_streams, "entered", real_entered)
    gc.collect()
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # nothing keeps the stream alive
    assert core.streams._streams == {}


# --- Results over a stream that fails or closes, and the stream's log lines -----------------


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
    release_scope(massive_session._core.exec_ctx, user_scope)
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
    assert core.streams._streams == {}
    assert core.results.held() == 0


def test_a_shutdown_of_an_unknown_stream_is_ignored(massive_session: dm.MassiveSession) -> None:
    # Called on this thread so that coverage sees it on every Python version; a blink table's
    # shutdown callback runs on a Deephaven thread.
    massive_session._core.streams.shut_down(99)
    assert massive_session._core.streams._streams == {}


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
    assert core.streams._streams == {}
    assert core.streams._stream_ids == {}


def test_a_failure_while_the_shape_is_derived_is_named(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    # The failure lands between the check and the shape: Deephaven's DHError is wrapped.
    core = massive_session._core
    q = stream_query(massive_session)

    def failing_then_shaping(handle: StreamHandle) -> Table:
        blink = handle.blink
        fake_streams.publishers[1].publish_failure(RuntimeError("entitlement"))
        assert fake_streams.shutdown[1].wait(TIMEOUT)
        core.fail_stream(1, "entitlement")
        return blink_to_append_only(blink)

    with pytest.raises(dm.MassiveError, match=r"stream 1 failed: entitlement") as raised:
        core.streams.result(
            SPEC,
            q._request,
            channel="T",
            query="<Query test>",
            verb="blink",
            build=failing_then_shaping,
        )
    assert isinstance(raised.value.__cause__, DHError)
    drain()
    assert core.results.held() == 0


def test_a_close_while_the_shape_is_derived_raises_the_closed_session(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    # The close fails the stream: the verb names the close, not a failure of the stream.
    core = massive_session._core
    q = stream_query(massive_session)

    def closing_then_shaping(handle: StreamHandle) -> Table:
        blink = handle.blink
        core.close()
        assert fake_streams.shutdown[1].wait(TIMEOUT)
        return blink_to_append_only(blink)

    with pytest.raises(dm.MassiveError, match=rf"^{re.escape(core.label)} is closed$") as raised:
        core.streams.result(
            SPEC,
            q._request,
            channel="T",
            query="<Query test>",
            verb="blink",
            build=closing_then_shaping,
        )
    assert isinstance(raised.value.__cause__, DHError)


def test_a_close_the_streams_row_does_not_show_yet_raises_the_closed_session(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The close failed the table, and Deephaven refuses the described copy, before the
    # stream's row reads CLOSED: the verb still names the close, never Deephaven's error.
    core = massive_session._core
    q = stream_query(massive_session)
    monkeypatch.setattr(core.streams, "_failure", lambda _: None)

    def closing_then_describing(handle: StreamHandle) -> Table:
        core.close()
        raise RuntimeError(
            "java.lang.UnsupportedOperationException: Cannot mutate attributes after they have "
            "been published"
        )

    with pytest.raises(dm.MassiveError, match=rf"^{re.escape(core.label)} is closed$") as raised:
        core.streams.result(
            SPEC,
            q._request,
            channel="T",
            query="<Query test>",
            verb="blink",
            build=closing_then_describing,
        )
    assert type(raised.value.__cause__) is RuntimeError


def test_a_failure_while_the_blink_copy_is_described_is_named(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    # `describe` calls Java directly, so a failed blink table surfaces as jpy's RuntimeError.
    core = massive_session._core
    q = stream_query(massive_session)
    schema = core.catalog.dataset_schema(SPEC)

    def failing_then_describing(handle: StreamHandle) -> Table:
        blink = handle.blink
        fake_streams.publishers[1].publish_failure(RuntimeError("entitlement"))
        assert fake_streams.shutdown[1].wait(TIMEOUT)
        core.fail_stream(1, "entitlement")
        return describe(blink, SPEC, schema)

    with pytest.raises(dm.MassiveError, match=r"stream 1 failed: entitlement") as raised:
        core.streams.result(
            SPEC,
            q._request,
            channel="T",
            query="<Query test>",
            verb="blink",
            build=failing_then_describing,
        )
    assert type(raised.value.__cause__) is RuntimeError
    drain()
    assert core.results.held() == 0


def test_a_deephaven_error_of_a_live_stream_is_not_wrapped(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    q = stream_query(massive_session)

    def broken(handle: StreamHandle) -> Table:
        blink = handle.blink
        # A shape Deephaven refuses for a reason of its own: ring() of a negative capacity.
        return ring_table(blink, -1)

    with pytest.raises(DHError):
        massive_session._core.streams.result(
            SPEC, q._request, channel="T", query="<Query test>", verb="blink", build=broken
        )


def test_the_stream_lifecycle_is_logged(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = stream_query(massive_session)
    with caplog.at_level(logging.DEBUG, logger=_streams.__name__):
        first = q.blink()
        second = q.append_only()
        massive_session.release(first)
        massive_session.release(second)
        assert fake_streams.shutdown[1].wait(TIMEOUT)  # the last release has run
    name = label(massive_session)
    assert [m for m in caplog.messages if "stream 1" in m] == [
        (
            f"{name}: stream 1 opened: {q!r}.blink -> channel T on socket.test/stocks "
            "(test host); symbols 1 [AAPL]"
        ),
        f"{name}: stream 1 reused: {q!r}.append_only -> channel T",
        "stream 1: a hold released, 1 left",
        "stream 1: a hold released, 0 left; its stream scope is released",
    ]


def test_a_stream_log_line_cuts_a_long_query(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A query's text can grow with its arguments; the open and reuse lines cut it as the
    # `Query` column does, so one line never carries thousands of characters.
    q = dataclasses.replace(stream_query(massive_session), _call=QueryCall("m." + "x" * 500))
    with caplog.at_level(logging.DEBUG, logger=_streams.__name__):
        first = q.blink()
        second = q.append_only()
        massive_session.release(first)
        massive_session.release(second)
    cut = "m." + "x" * (QUERY_SHOWN - 3) + "…"
    lines = [m for m in caplog.messages if "stream 1 opened" in m or "stream 1 reused" in m]
    assert [cut in line for line in lines] == [True, True]
    assert all(len(line) < QUERY_SHOWN + 200 for line in lines)


@pytest.mark.parametrize(
    ("symbols", "shown"),
    [
        (
            [f"S{n:02d}" for n in range(25)],
            "25 [" + ", ".join(f"S{n:02d}" for n in range(20)) + ", +5 more]",
        ),
        (dm.ALL, "ALL"),
    ],
)
def test_a_stream_open_names_its_symbols_bounded(
    massive_session: dm.MassiveSession,
    *,
    fake_streams: FakeStreamTransport,
    caplog: pytest.LogCaptureFixture,
    symbols: object,
    shown: str,
) -> None:
    q = stream_query(massive_session, symbols)
    with caplog.at_level(logging.DEBUG, logger=_streams.__name__):
        massive_session.release(q.ring(5))
    [opened] = [m for m in caplog.messages if "stream 1 opened" in m]
    assert f"{q!r}.ring -> channel T" in opened
    assert opened.endswith(f"; symbols {shown}")


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
        core.streams.shut_down(stream_id)  # what an early shutdown callback does
        return dataclasses.replace(handle, stop_watch=stopped.set)

    monkeypatch.setattr(fake_streams, "open_stream", shut_down_while_opening)
    result = stream_query(massive_session).append_only()  # a shutdown is not a failure
    drain()
    assert stopped.is_set()
    massive_session.release(result)
    drain()
    assert core.streams._streams == {}


def test_a_verb_that_races_close_never_returns_its_table(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    # The session closes while the result is built: the verb releases it and raises.
    core = massive_session._core
    spec = SPEC

    def build_and_close(handle: StreamHandle) -> Table:
        blink = handle.blink
        shaped = blink_to_append_only(blink)
        massive_session.close()
        return shaped

    with pytest.raises(dm.MassiveError, match=f"^{label(massive_session)} is closed$"):
        core.streams.result(
            spec,
            Request(symbols=("AAPL",)),
            channel="T",
            query="<Query test>",
            verb="blink",
            build=build_and_close,
        )
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # failed by the close, then released
