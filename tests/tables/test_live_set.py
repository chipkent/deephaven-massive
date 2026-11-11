"""Tests for the live set, which keeps a session alive while its own live tables run."""

from __future__ import annotations

import gc
import importlib
import threading
import weakref
from typing import TYPE_CHECKING, cast

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._massive import Transport
from deephaven_massive._tables import live_set
from deephaven_massive._tables.diagnostics import StreamKind, StreamRow, StreamState
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.typehints._queries import SymbolStreamQuery

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._tables.session_core import SessionCore

    # Type only: the instance comes from the `fake_streams` fixture.
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


def test_a_result_keeps_its_session_alive() -> None:
    # A result that outlives every reference to its session and query must still be released
    # when it is collected: the release path (registry, reaper, core) lives as long as it does.
    # A fake of its own: the `fake_streams` fixture is bound to the fixture session, and
    # monkeypatch would hold this session's core until teardown.
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


def test_a_running_stream_keeps_its_session_in_the_live_set(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    core = massive_session._core
    assert id(core) not in live_set._live  # an idle session is held only by its user
    result = stream_query(massive_session).append_only()
    assert live_set._live[id(core)] is core
    massive_session.release(result)
    drain()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    drain()
    assert id(core) not in live_set._live
    view = massive_session.streams()  # a held diagnostics view counts too
    assert live_set._live[id(core)] is core
    massive_session.release(view)
    drain()
    assert id(core) not in live_set._live


ROUNDS = 40
"""How many times each racing thread changes its registry in the concurrency test."""


def _in_live_set(core: SessionCore) -> bool:
    with live_set._live_lock:
        return live_set._live.get(id(core)) is core


def test_the_live_set_follows_every_registry_under_concurrent_changes(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
) -> None:
    # The three things that keep a session alive live in three places (the stream registry,
    # the worker-table registry, and the core's count of views), and each changes on its own
    # thread here. Each change recomputes membership from all three under `registry_lock`, so
    # the session is in the live set whenever any of them holds something, and leaves it once
    # all three are empty.
    core = massive_session._core
    start = threading.Barrier(3)
    missing: list[str] = []
    lock = threading.Lock()

    def check(what: str) -> None:
        if not _in_live_set(core):
            with lock:
                missing.append(what)

    def views() -> None:
        start.wait(TIMEOUT)
        for _ in range(ROUNDS):
            view = massive_session.streams()
            check("a held view")
            massive_session.release(view)

    def streams() -> None:
        start.wait(TIMEOUT)
        for _ in range(ROUNDS):
            result = stream_query(massive_session).blink()
            check("a held stream")
            massive_session.release(result)

    def workers() -> None:
        start.wait(TIMEOUT)
        for _ in range(ROUNDS):
            call = core.next_stream_id()
            core.workers._register(core, _read_row(call), fail=lambda _error: True, registered=[])
            check("a registered worker table")
            core.workers.release(call)  # as releasing the result, then its shutdown, do
            core.workers.shutdown_callback(call, threading.Event())()

    threads = [threading.Thread(target=run) for run in (views, streams, workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    assert missing == []
    drain()
    for shut in tuple(fake_streams.shutdown.values()):
        assert shut.wait(TIMEOUT)
    drain()
    assert not _in_live_set(core)


def _read_row(call: int) -> StreamRow:
    # A followed read's first row, as `open_followed_read` registers it.
    return StreamRow(
        stream_id=call,
        kind=StreamKind.READ,
        dataset="test.trades",
        query="m.test.trades(<table>)",
        symbol_count=0,
        symbols="",
        held_results=1,
        transport=Transport.REST,
        state=StreamState.LOADING,
        loading=0,
        created_at=1,
    )
