"""Tests for `MassiveSession` and `SessionCore` (DESIGN § 6, § 17, § 18)."""

from __future__ import annotations

import gc
import logging
import threading
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table, time_table
from deephaven.column import string_col
from deephaven.table_listener import listen

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._catalog import DatasetSpec
from deephaven_massive._domains.base import build_query
from deephaven_massive._domains.crypto import _Crypto
from deephaven_massive._domains.economy import _Economy
from deephaven_massive._domains.forex import _Forex
from deephaven_massive._domains.futures import _Futures
from deephaven_massive._domains.indicators import _Indicators
from deephaven_massive._domains.indices import _Indices
from deephaven_massive._domains.options import _Options
from deephaven_massive._domains.partners import _Benzinga, _EtfGlobal, _Fable, _Partners, _Tmx
from deephaven_massive._domains.reference import _Reference
from deephaven_massive._domains.stocks import _Stocks
from deephaven_massive._enums import AssetClass
from deephaven_massive._liveness import StreamHandle, new_scope
from deephaven_massive._query import Request, StreamQuery
from deephaven_massive._schema import TRADES

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable

    from deephaven.table import Table

    from _fakes.stream import FakeStreamTransport

TIMEOUT = 10.0

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


def label(session: dm.MassiveSession) -> str:
    return session._core.label


# --- Construction and public surface ---------------------------------------------------------


def test_default_config_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "env-key")
    with dm.MassiveSession() as m:
        assert m._core.config.api_key == "env-key"
        assert label(m).startswith("MassiveSession #")


def test_the_session_exposes_no_introspection_attributes(
    massive_session: dm.MassiveSession,
) -> None:
    public = sorted(name for name in dir(massive_session) if not name.startswith("_"))
    assert public == [
        "close",
        "connections",
        "crypto",
        "economy",
        "forex",
        "futures",
        "indicators",
        "indices",
        "options",
        "partners",
        "reference",
        "release",
        "snapshots",
        "stocks",
        "streams",
    ]


def test_repr_shows_the_label_and_state() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    number = label(m).removeprefix("MassiveSession #")
    assert repr(m) == f"<MassiveSession #{number} open>"
    m.close()
    assert repr(m) == f"<MassiveSession #{number} closed>"
    assert "k" not in repr(m).replace("closed", "")


def test_labels_count_up() -> None:
    a = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    b = dm.MassiveSession(dm.MassiveConfig(api_key="other"))
    first = int(label(a).removeprefix("MassiveSession #"))
    assert label(b) == f"MassiveSession #{first + 1}"
    a.close()
    b.close()


def test_config_must_be_a_massive_config() -> None:
    with pytest.raises(TypeError, match=r"config must be a dm\.MassiveConfig"):
        dm.MassiveSession(cast("dm.MassiveConfig", {"api_key": "k"}))


def test_construction_without_an_execution_context_fails_fast() -> None:
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            dm.MassiveSession(dm.MassiveConfig(api_key="k"))
        except dm.MassiveError as e:
            errors.append(e)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(TIMEOUT)
    assert len(errors) == 1
    assert "must be created inside a Deephaven worker" in str(errors[0])
    assert "ExecutionContext" in str(errors[0])


def test_construction_on_the_update_graph_thread_fails() -> None:
    errors: list[BaseException] = []
    seen = threading.Event()

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        try:
            dm.MassiveSession(dm.MassiveConfig(api_key="k"))
        except ValueError as e:
            errors.append(e)
        seen.set()

    scope = new_scope()
    with scope.open():
        handle = listen(time_table("PT0.01S"), on_update)
    assert seen.wait(TIMEOUT)
    handle.stop()
    scope.release()
    assert "update-graph thread" in str(errors[0])


def test_a_second_session_with_the_same_key_warns(caplog: pytest.LogCaptureFixture) -> None:
    config = dm.MassiveConfig(api_key="shared-key")
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        first = dm.MassiveSession(config)
        second = dm.MassiveSession(config)
        second.close()
        other = dm.MassiveSession(dm.MassiveConfig(api_key="other-key"))
    warnings = [r.getMessage() for r in caplog.records if "same API key" in r.getMessage()]
    assert warnings == [
        (
            f"{label(second)} uses the same API key as {label(first)}; individual plans allow "
            "one WebSocket connection per cluster per account, so the sessions will compete for "
            "connection slots"
        )
    ]
    caplog.clear()
    first.close()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        third = dm.MassiveSession(config)  # the others are closed
    assert "same API key" not in caplog.text
    third.close()
    other.close()


def test_collected_sessions_leave_the_registry() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="collected-key"))
    core = m._core
    assert core in _session._open_sessions
    m.close()
    del m, core
    gc.collect()
    assert all(c.config.api_key != "collected-key" for c in _session._open_sessions)


def test_namespaces(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    expected = {
        "stocks": _Stocks,
        "options": _Options,
        "futures": _Futures,
        "indices": _Indices,
        "forex": _Forex,
        "crypto": _Crypto,
        "reference": _Reference,
        "indicators": _Indicators,
        "economy": _Economy,
        "partners": _Partners,
    }
    for name, cls in expected.items():
        assert type(getattr(m, name)) is cls
    assert m.stocks is m.stocks
    assert type(m.partners.benzinga) is _Benzinga
    assert type(m.partners.etf_global) is _EtfGlobal
    assert type(m.partners.tmx) is _Tmx
    assert type(m.partners.fable) is _Fable


def test_unified_snapshots_shell(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"m\.stocks\.snapshots\(dm\.ALL\)"):
        massive_session.snapshots(dm.ALL)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        massive_session.snapshots(["AAPL"], order=cast("dm.Order", "asc"))
    with pytest.raises(ValueError, match="limit must be positive"):
        massive_session.snapshots(["AAPL"], limit=0)
    with pytest.raises(NotImplementedError, match="not implemented yet"):
        massive_session.snapshots(["AAPL", "O:SPY251219C00600000"], filters={"type": "CS"})


# --- Diagnostics -----------------------------------------------------------------------------


@pytest.mark.usefixtures("fake_streams")
def test_stream_rows_follow_the_holds(
    massive_session: dm.MassiveSession, stream_rows: Callable[[], list[dict[str, Any]]]
) -> None:
    q = stream_query(massive_session, ["MSFT", "AAPL"])
    a = q.append_only()
    b = q.blink()
    rows = stream_rows()
    assert len(rows) == 1
    row = rows[0]
    assert (row["StreamId"], row["Dataset"], row["HeldResults"], row["State"]) == (
        1,
        "test.trades",
        2,
        "RUNNING",
    )
    assert (row["SymbolCount"], row["Symbols"], row["Transport"]) == (2, "AAPL, MSFT", "WS")
    assert (row["Host"], row["Path"]) == ("socket.test", "/stocks")
    assert row["CreatedAt"] is not None
    massive_session.release(a)
    assert stream_rows()[0]["HeldResults"] == 1
    massive_session.release(b)
    final = stream_rows()[0]
    assert (final["HeldResults"], final["State"]) == (0, "RELEASED")
    assert massive_session.connections().size == 0


@pytest.mark.usefixtures("fake_streams")
def test_all_and_reactive_symbols_in_stream_rows(
    massive_session: dm.MassiveSession, stream_rows: Callable[[], list[dict[str, Any]]]
) -> None:
    every = stream_query(massive_session, dm.ALL).blink()
    watch = stream_query(massive_session, new_table([string_col("Symbol", ["A"])])).blink()
    rows = stream_rows()
    assert [(r["SymbolCount"], r["Symbols"]) for r in rows] == [
        (None, "ALL"),
        (None, "reactive table"),
    ]
    massive_session.release(every)
    massive_session.release(watch)


def test_a_racing_open_reuses_the_registered_stream(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    q = stream_query(massive_session)
    inner: list[Table] = []

    class Racing:
        # While the outer verb is opening, another verb on the same key opens and registers
        # its own stream first.
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None
        ) -> StreamHandle:
            handle = fake_streams.open_stream(spec, request, channel)
            if not inner:
                monkeypatch.setattr(massive_session._core, "stream_transport", lambda: fake_streams)
                inner.append(q.append_only())
            return handle

    racing = Racing()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: racing)
    outer = q.blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the losing stream was released
    assert not fake_streams.shutdown[2].is_set()
    # The losing stream never reached the diagnostics; the winner holds both results.
    assert [(r["StreamId"], r["HeldResults"]) for r in stream_rows()] == [(2, 2)]
    massive_session.release(outer)
    massive_session.release(inner[0])
    assert fake_streams.shutdown[2].wait(TIMEOUT)


# --- close() ---------------------------------------------------------------------------------


@pytest.mark.usefixtures("fake_streams")
def test_close_marks_streams_closed_and_warns_once(
    massive_session: dm.MassiveSession,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    live = stream_query(massive_session).append_only()
    released = stream_query(massive_session, ["MSFT"]).blink()
    massive_session.release(released)
    assert [r["State"] for r in stream_rows()] == ["RUNNING", "RELEASED"]
    keeper = new_scope()  # keeps the diagnostics table readable after close() releases it
    streams = massive_session.streams()
    keeper.manage(streams)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        massive_session.close()
        massive_session.close()
    assert caplog.messages == ["session closed; 1 live tables are now static"]
    # Only the stream the library still holds is closed; the released one keeps its last row.
    rows = sorted(dh_arrow.to_arrow(streams).to_pylist(), key=lambda r: r["StreamId"])
    assert [r["State"] for r in rows] == ["CLOSED", "RELEASED"]
    keeper.release()
    massive_session.release(live)  # release() keeps working after close()


def test_close_with_no_live_tables_logs_info(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="deephaven_massive._session"):
        massive_session.close()
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "session closed; 0 live tables are now static")
    ]


def test_after_close(massive_session: dm.MassiveSession) -> None:
    q = stream_query(massive_session)
    massive_session.close()
    message = f"^{label(massive_session)} is closed$"
    with pytest.raises(dm.MassiveError, match=message):
        massive_session.connections()
    with pytest.raises(dm.MassiveError, match=message):
        massive_session.streams()
    with pytest.raises(dm.MassiveError, match=message):
        q.append_only()
    # Building queries and reaching namespaces never fail.
    assert isinstance(stream_query(massive_session), StreamQuery)
    assert isinstance(massive_session.stocks, _Stocks)


def test_closers_run_and_failures_are_logged(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    ran: list[str] = []

    def failing() -> None:
        raise RuntimeError("cannot close")

    massive_session._core.on_close(failing)
    massive_session._core.on_close(lambda: ran.append("second"))
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._session"):
        massive_session.close()
    assert ran == ["second"]
    assert "a transport failed to close" in caplog.text


def test_close_on_the_update_graph_thread_fails(massive_session: dm.MassiveSession) -> None:
    errors: list[BaseException] = []
    seen = threading.Event()

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        try:
            massive_session.close()
        except ValueError as e:
            errors.append(e)
        seen.set()

    scope = new_scope()
    with scope.open():
        handle = listen(time_table("PT0.01S"), on_update)
    assert seen.wait(TIMEOUT)
    handle.stop()
    scope.release()
    assert len(errors) == 1
    assert not massive_session._core.closed


def test_context_manager_closes() -> None:
    with dm.MassiveSession(dm.MassiveConfig(api_key="k")) as m:
        assert not m._core.closed
    assert m._core.closed


def test_release_argument_errors(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match="must be a Deephaven Table"):
        massive_session.release(cast("Table", "t"))
    with pytest.raises(ValueError, match="not a live result"):
        massive_session.release(new_table([string_col("Symbol", ["A"])]))


def test_a_session_closed_while_a_stream_opens(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Closing:
        def open_stream(
            self, spec: DatasetSpec, request: Request, channel: str | None
        ) -> StreamHandle:
            handle = fake_streams.open_stream(spec, request, channel)
            massive_session.close()
            return handle

    closing = Closing()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: closing)
    with pytest.raises(dm.MassiveError, match="was closed while a stream was opening"):
        stream_query(massive_session).blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_background_threads_stop_when_the_session_is_collected() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    reaper = m._core._reaper._thread
    writer = m._core.diagnostics._thread
    del m
    gc.collect()
    reaper.join(TIMEOUT)
    writer.join(TIMEOUT)
    assert not reaper.is_alive()
    assert not writer.is_alive()
