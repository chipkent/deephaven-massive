"""Tests for `MassiveSession` and `SessionCore` (DESIGN § 6, § 17, § 18)."""

import gc
import logging
import threading
from collections.abc import Callable
from typing import Any, Protocol, cast

import pytest
from deephaven import new_table, time_table
from deephaven.column import string_col
from deephaven.table import Table
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
from deephaven_massive._liveness import new_scope
from deephaven_massive._query import Request, StreamQuery
from deephaven_massive._schema import TRADES

TIMEOUT = 10.0
Rows = Callable[[Table], list[dict[str, Any]]]

SPEC = DatasetSpec(
    name="test.trades",
    query_class=StreamQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
)


class FakeStreams(Protocol):
    # What the tests use from the `fake_streams` fixture (tests/conftest.py).
    opened: list[tuple[str, str | None]]
    shutdown: dict[int, threading.Event]

    def open_stream(self, spec: DatasetSpec, request: Request, channel: str | None) -> object: ...


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, StreamQuery)
    return q


def drain(session: dm.MassiveSession) -> None:
    done = threading.Event()
    session._core._reaper.submit(done.set)
    assert done.wait(TIMEOUT)


# --- Construction ----------------------------------------------------------------------------


def test_default_config_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "env-key")
    with dm.MassiveSession() as m:
        assert m.config.api_key == "env-key"
        assert m.name.startswith("MassiveSession#")


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
            f"{second.name} uses the same API key as {first.name}; individual plans allow one "
            "WebSocket connection per cluster per account, so the sessions will compete for "
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


def test_the_key_registry_holds_no_secret() -> None:
    with dm.MassiveSession(dm.MassiveConfig(api_key="plain-key")):
        assert "plain-key" not in _session._sessions_by_key
        assert _session._key_digest("plain-key") in _session._sessions_by_key


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


def test_stream_rows_follow_the_holds(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, dh_to_pylist: Rows
) -> None:
    q = stream_query(massive_session, ["MSFT", "AAPL"])
    a = q.append_only()
    b = q.blink()
    rows = dh_to_pylist(massive_session.streams())
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
    drain(massive_session)
    assert dh_to_pylist(massive_session.streams())[0]["HeldResults"] == 1
    massive_session.release(b)
    drain(massive_session)
    final = dh_to_pylist(massive_session.streams())[0]
    assert (final["HeldResults"], final["State"]) == (0, "RELEASED")
    assert dh_to_pylist(massive_session.connections()) == []


def test_all_and_reactive_symbols_in_stream_rows(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams, dh_to_pylist: Rows
) -> None:
    every = stream_query(massive_session, dm.ALL).blink()
    watch = stream_query(massive_session, new_table([string_col("Symbol", ["A"])])).blink()
    rows = sorted(dh_to_pylist(massive_session.streams()), key=lambda r: r["StreamId"])
    assert [(r["SymbolCount"], r["Symbols"]) for r in rows] == [
        (None, "ALL"),
        (None, "reactive table"),
    ]
    massive_session.release(every)
    massive_session.release(watch)


@pytest.mark.usefixtures("fake_streams")
def test_close_marks_streams_closed_and_warns_once(
    massive_session: dm.MassiveSession, dh_to_pylist: Rows, caplog: pytest.LogCaptureFixture
) -> None:
    live = stream_query(massive_session).append_only()
    released = stream_query(massive_session, ["MSFT"]).blink()
    massive_session.release(released)
    drain(massive_session)
    keeper = new_scope()  # keeps the diagnostics table readable after close() releases it
    keeper.manage(massive_session.streams())
    streams = massive_session.streams()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        massive_session.close()
        massive_session.close()
    assert caplog.messages == ["session closed; 1 live tables are now static"]
    assert {r["State"] for r in dh_to_pylist(streams)} == {"CLOSED"}
    with pytest.raises(dm.MassiveError, match="is closed"):
        massive_session.connections()
    massive_session.release(live)
    drain(massive_session)
    assert {r["State"] for r in dh_to_pylist(streams)} == {"CLOSED"}
    keeper.release()


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
    fake_streams: FakeStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Closing:
        def open_stream(self, spec: DatasetSpec, request: Request, channel: str | None) -> object:
            handle = fake_streams.open_stream(spec, request, channel)
            massive_session.close()
            return handle

    closing = Closing()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: closing)
    with pytest.raises(dm.MassiveError, match="was closed while a stream was opening"):
        stream_query(massive_session).blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_the_reaper_stops_when_the_session_is_collected() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    thread = m._core._reaper._thread
    m.close()
    del m
    gc.collect()
    thread.join(TIMEOUT)
    assert not thread.is_alive()
