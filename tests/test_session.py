"""Tests for `MassiveSession`: construction, the public surface, diagnostics, and `close()`."""

from __future__ import annotations

import gc
import itertools
import json
import logging
import pkgutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table, time_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.execution_context import make_user_exec_ctx
from deephaven.table_listener import listen
from deephaven.update_graph import exclusive_lock

import deephaven_massive as dm
from deephaven_massive import _debug_log, _namespaces, _session
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import AssetClass, ClassFromPrefix, NotBars
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._massive import Transport, sources
from deephaven_massive._namespaces import snapshots
from deephaven_massive._namespaces.crypto import _Crypto
from deephaven_massive._namespaces.economy import _Economy
from deephaven_massive._namespaces.forex import _Forex
from deephaven_massive._namespaces.futures import _Futures
from deephaven_massive._namespaces.indicators import _Indicators
from deephaven_massive._namespaces.indices import _Indices
from deephaven_massive._namespaces.options import _Options
from deephaven_massive._namespaces.partners import _Benzinga, _EtfGlobal, _Fable, _Partners, _Tmx
from deephaven_massive._namespaces.query_building import build_query
from deephaven_massive._namespaces.reference import _Reference
from deephaven_massive._namespaces.stocks import _Stocks
from deephaven_massive._query import Query, SymbolStreamQuery
from deephaven_massive._tables import session_core as _core
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.session_core import CLOSE_SHOWN_TIMEOUT
from tests._fakes.rest import Reply
from tests._fakes.stream import transport_of

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive._tables.diagnostics import Diagnostics
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import StreamHandle
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3
    from tests._fakes.stream import FakeStreamTransport
    from tests._fakes.ws_session import WsSession

TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(Transport.WS, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> SymbolStreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, SymbolStreamQuery)
    return q


def one_row(session: dm.MassiveSession) -> Table:
    # One row of SPEC for a fake stream's publisher: an AAPL symbol, every other column null.
    schema = session._core.catalog.dataset_schema(SPEC).arrow_schema()
    return dh_arrow.to_table(pa.Table.from_pylist([{"Symbol": "AAPL"}], schema=schema))


def wait_until(condition: Callable[[], bool]) -> None:
    # A bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.02)):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


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
        "rate_limits",
        "reference",
        "release",
        "snapshots",
        "stocks",
        "streams",
        "symbol_reads",
    ]


def test_repr_shows_the_label_and_state() -> None:
    hidden = "api-hidden-value"
    m = dm.MassiveSession(dm.MassiveConfig(api_key=hidden))
    number = label(m).removeprefix("MassiveSession #")
    assert repr(m) == f"<MassiveSession #{number} open>"
    assert hidden not in repr(m)
    m.close()
    assert repr(m) == f"<MassiveSession #{number} closed>"
    assert hidden not in repr(m)


def test_labels_count_up() -> None:
    a = dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    b = dm.MassiveSession(dm.MassiveConfig(api_key="other"))
    first = int(label(a).removeprefix("MassiveSession #"))
    assert label(b) == f"MassiveSession #{first + 1}"
    a.close()
    b.close()


def test_the_session_threads_are_named_after_the_session() -> None:
    with dm.MassiveSession(dm.MassiveConfig(api_key="k")) as m:
        number = label(m).removeprefix("MassiveSession #")
        assert m._core.thread_prefix == f"massive #{number}"
        names = {thread.name for thread in threading.enumerate()}
        assert {f"massive #{number} reaper", f"massive #{number} diagnostics"} <= names


def test_the_first_session_applies_the_logging_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_debug_log, "_switch", _debug_log._Switch())
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "verbose")
    with pytest.raises(
        ValueError, match=r"^DEEPHAVEN_MASSIVE_LOG='verbose' is not a logging level"
    ):
        dm.MassiveSession(dm.MassiveConfig(api_key="k"))


@pytest.mark.parametrize("s3", [False, True])
def test_creating_a_session_logs_its_configuration_without_secrets(
    *, s3: bool, caplog: pytest.LogCaptureFixture
) -> None:
    config = dm.MassiveConfig(
        api_key="api-hidden",
        s3_access_key_id="AKIDLOG" if s3 else None,
        s3_secret_access_key="s3-hidden" if s3 else None,
    )
    with caplog.at_level(logging.INFO, logger="deephaven_massive._session"):
        m = dm.MassiveSession(config)
    m.close()
    [line] = [r.getMessage() for r in caplog.records if "with api_key set" in r.getMessage()]
    setting = "set" if s3 else "unset"
    # Every field the repr shows, in its order, except the S3 access key id.
    shown = repr(config).replace(f"s3_access_key_id={config.s3_access_key_id!r}, ", "")
    assert line == (
        f"{label(m)}: {shown} with api_key set, s3_access_key_id {setting}, "
        f"s3_secret_access_key {setting}"
    )
    assert "s3_access_key_id=" not in line
    for secret in ("api-hidden", "AKIDLOG", "s3-hidden"):
        assert secret not in line


def test_the_logged_configuration_never_shows_a_url_s_password(
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = dm.MassiveConfig(
        api_key="k", s3_endpoint_url="https://reader:hunter2@files.massive.com"
    )
    with caplog.at_level(logging.INFO, logger="deephaven_massive._session"):
        m = dm.MassiveSession(config)
    m.close()
    [line] = [r.getMessage() for r in caplog.records if "with api_key set" in r.getMessage()]
    assert "s3_endpoint_url='https://***@files.massive.com'" in line
    assert "hunter2" not in caplog.text


def test_config_must_be_a_massive_config() -> None:
    with pytest.raises(TypeError, match=r"config must be a dm\.MassiveConfig"):
        dm.MassiveSession(cast("dm.MassiveConfig", {"api_key": "k"}))


def test_construction_under_a_frozen_query_scope_fails_fast() -> None:
    frozen = make_user_exec_ctx(freeze_vars=["dm"])
    with frozen, pytest.raises(dm.MassiveError, match="frozen copy"):
        dm.MassiveSession(dm.MassiveConfig(api_key="k"))
    with make_user_exec_ctx():  # an unfrozen user context shares the script session's scope
        dm.MassiveSession(dm.MassiveConfig(api_key="k")).close()


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
    assert "must be created inside a Deephaven server" in str(errors[0])
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


def test_a_same_key_session_warns_only_when_it_cannot_share(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Equal connection settings share the connections, so only differing settings warn.
    config = dm.MassiveConfig(api_key="shared-key")
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        first = dm.MassiveSession(config)
        sharing = dm.MassiveSession(config)
        # What a taken connection does is no connection setting: such sessions share one.
        reconnecting = dm.MassiveSession(
            dm.MassiveConfig(api_key="shared-key", connection_taken=dm.ConnectionTaken.RECONNECT)
        )
        other = dm.MassiveSession(dm.MassiveConfig(api_key="other-key"))
        assert caplog.records == []
        sharing.close()
        reconnecting.close()
        apart = dm.MassiveSession(
            dm.MassiveConfig(
                api_key="shared-key", flush_interval=0.5, subscription_confirmation_timeout=9.0
            )
        )
    warnings = [r.getMessage() for r in caplog.records if "same API key" in r.getMessage()]
    assert warnings == [
        (
            f"{label(apart)} uses the same API key as {label(first)} with different connection "
            "settings (subscription_confirmation_timeout, flush_interval), so the two cannot "
            "share WebSocket connections; Massive allows an account one connection per asset "
            "class and keeps the newer, so the live tables on the older follow their session's "
            "connection_taken"
        )
    ]
    caplog.clear()
    first.close()
    apart.close()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        third = dm.MassiveSession(dm.MassiveConfig(api_key="shared-key", flush_interval=0.5))
    assert "same API key" not in caplog.text  # the others are closed
    third.close()
    other.close()


def test_the_same_key_warning_names_the_oldest_session_that_cannot_share(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Numbers 9 and 10: text order would put #10 first, creation order puts #9 first.
    monkeypatch.setattr(_session, "_session_numbers", itertools.count(9))
    config = dm.MassiveConfig(api_key="ordered-key")
    ninth, tenth = dm.MassiveSession(config), dm.MassiveSession(config)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        apart = dm.MassiveSession(dm.MassiveConfig(api_key="ordered-key", flush_interval=0.5))
    warnings = [r.getMessage() for r in caplog.records if "same API key" in r.getMessage()]
    assert [w.split(" with ")[0] for w in warnings] == [
        "MassiveSession #11 uses the same API key as MassiveSession #9"
    ]
    for session in (ninth, tenth, apart):
        session.close()


def _open_cores() -> list[SessionCore]:
    return [core for core in (ref() for ref in _session._open_sessions) if core is not None]


def test_collected_sessions_leave_the_registry() -> None:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="collected-key"))
    core = m._core
    assert core in _open_cores()
    m.close()
    del m, core
    gc.collect()
    assert all(c.config.api_key != "collected-key" for c in _open_cores())


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


def test_unified_snapshots(massive_session: dm.MassiveSession) -> None:
    query = massive_session.snapshots(
        ["X:BTCUSD", "AAPL", "C:EURUSD", "O:SPY251219C00600000"],
        sort="ticker",
        order=dm.Order.DESC,
        limit=3,
        filters={"ticker_gte": "A"},
    )
    assert type(query) is Query
    assert query._spec is snapshots.SNAPSHOTS
    assert query._spec is _session.CATALOG.by_name["snapshots"]
    assert query._request == Request(
        symbols=("AAPL", "C:EURUSD", "O:SPY251219C00600000", "X:BTCUSD"),
        params=(("order", dm.Order.DESC), ("sort", "ticker")),
        filters=(("ticker_gte", "A"),),
        limit=3,
    )
    with pytest.raises(ValueError, match=r"m\.stocks\.snapshots\(dm\.ALL\)"):
        massive_session.snapshots(dm.ALL)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        massive_session.snapshots(["AAPL"], order=cast("dm.Order", "asc"))
    with pytest.raises(ValueError, match="limit must be positive"):
        massive_session.snapshots(["AAPL"], limit=0)
    with pytest.raises(ValueError, match=r"^snapshots takes no type filter: Massive refuses type"):
        massive_session.snapshots(["AAPL"], filters={"type": "options"})
    with pytest.raises(ValueError, match="ticker_any_of"):
        massive_session.snapshots(["AAPL"], filters={"ticker_any_of": ["MSFT"]})


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
    assert (row["SymbolCount"], row["Symbols"], row["Transport"]) == (2, "AAPL, MSFT", "WebSocket")
    assert (row["Host"], row["Path"]) == ("socket.test", "/stocks")
    assert row["Query"] == repr(q)  # the query that opened it; b reused the stream
    assert row["CreatedAt"] is not None
    massive_session.release(a)
    assert stream_rows()[0]["HeldResults"] == 1
    massive_session.release(b)
    final = stream_rows()[0]
    assert (final["HeldResults"], final["State"]) == (0, "RELEASED")
    assert massive_session.connections().size == 0


@pytest.mark.usefixtures("fake_streams")
def test_all_and_reactive_symbols_in_stream_rows(
    massive_session: dm.MassiveSession,
    stream_rows: Callable[[], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    every = stream_query(massive_session, dm.ALL).blink()
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])  # refreshing
    watch = stream_query(massive_session, watchlist).blink()
    fixed = stream_query(massive_session, new_table([string_col("Symbol", ["A"])])).blink()
    rows = stream_rows()
    assert [(r["SymbolCount"], r["Symbols"]) for r in rows] == [
        (None, "ALL"),
        (None, "reactive table"),
        (1, "A"),  # a static table is a fixed symbol list
    ]
    massive_session.release(every)
    massive_session.release(watch)
    massive_session.release(fixed)


# --- close() ---------------------------------------------------------------------------------


def test_close_marks_streams_closed_and_warns_once(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    *,
    stream_rows: Callable[[], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    live = stream_query(massive_session).append_only()
    released = stream_query(massive_session, ["MSFT"]).blink()
    massive_session.release(released)
    assert [r["State"] for r in stream_rows()] == ["RUNNING", "RELEASED"]
    assert fake_streams.shutdown[2].wait(TIMEOUT)  # the released stream left the registry
    streams = massive_session.streams()  # a view keeps the final rows after close()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.session_core"):
        massive_session.close()
        massive_session.close()
    assert caplog.messages == [
        f"{massive_session._core.label} closed; 1 live tables failed, streams 1 [1]"
    ]
    # Only the stream still in the registry is closed; the shut-down one keeps its last row.
    rows = sorted(dh_arrow.to_arrow(streams).to_pylist(), key=lambda r: r["StreamId"])
    closed = f"{label(massive_session)} was closed; this table no longer updates"
    assert [(r["State"], r["Error"]) for r in rows] == [("CLOSED", closed), ("RELEASED", None)]
    assert fake_streams.failed == {1: closed}
    massive_session.release(live)  # release() keeps working after close()


def _record_close_steps(monkeypatch: pytest.MonkeyPatch, core: SessionCore) -> list[str]:
    # Records the diagnostics release and each wait for an update cycle that close() makes.
    steps: list[str] = []
    diagnostics = type(core.diagnostics)
    release = diagnostics.release
    wait = wait_for_update_cycle

    def released(self: Diagnostics, *, wait: bool = True) -> None:
        release(self, wait=wait)
        steps.append(f"released, waiting: {wait}")

    def waited(exec_ctx: ExecutionContext, timeout: float) -> bool:
        steps.append(f"waited for a cycle, at most {timeout} s")
        return wait(exec_ctx, timeout)

    monkeypatch.setattr(diagnostics, "release", released)
    monkeypatch.setattr(_core, "wait_for_update_cycle", waited)
    return steps


def test_a_held_view_reads_the_final_rows_as_close_returns(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The writer adds the final rows, and they reach the diagnostics table, and a view of it,
    # in the next update cycle: close() waits for that cycle after the writer ended, so the
    # view reads CLOSED the moment close() returns, with no polling.
    core = massive_session._core
    result = stream_query(massive_session).append_only()
    view = massive_session.streams()
    steps = _record_close_steps(monkeypatch, core)
    massive_session.close()
    (row,) = dh_arrow.to_arrow(view).to_pylist()
    assert row["State"] == "CLOSED"
    assert steps == [
        "released, waiting: True",
        f"waited for a cycle, at most {CLOSE_SHOWN_TIMEOUT} s",
    ]
    del result, view


def test_a_table_derived_from_a_released_view_reads_the_final_rows_as_close_returns(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    drain: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The view is released before the close, as in `x = m.streams().where(...)`, whose view
    # wrapper is collected at once, but the derived table keeps the diagnostics table alive.
    # close() still waits for the cycle that takes the final rows in, so the derived table
    # reads CLOSED the moment close() returns.
    result = stream_query(massive_session).append_only()
    view = massive_session.streams()
    derived = view.head(10)  # no formula, which would hand this frame's locals to the JVM
    massive_session.release(view)
    drain()
    assert massive_session._core._views == 0
    steps = _record_close_steps(monkeypatch, massive_session._core)
    massive_session.close()
    (row,) = dh_arrow.to_arrow(derived).to_pylist()
    assert row["State"] == "CLOSED"
    assert steps == [
        "released, waiting: True",
        f"waited for a cycle, at most {CLOSE_SHOWN_TIMEOUT} s",
    ]
    del result, derived


def test_close_returns_after_a_warning_when_no_cycle_shows_the_rows_in_time(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Once the writer has released the diagnostics, a helper thread takes the update graph's
    # exclusive lock and keeps it, so no cycle can run: close() waits out its timeout and
    # returns after a WARNING that the view may not show the rows yet.
    result = stream_query(massive_session).append_only()
    view = massive_session.streams()
    update_graph = massive_session._core.exec_ctx.update_graph
    diagnostics = type(massive_session._core.diagnostics)
    release = diagnostics.release
    held, done = threading.Event(), threading.Event()

    def hold_the_lock() -> None:
        with exclusive_lock(update_graph):
            held.set()
            done.wait(TIMEOUT)

    holder = threading.Thread(target=hold_the_lock, name="lock holder", daemon=True)

    def released(self: Diagnostics, *, wait: bool = True) -> None:
        release(self, wait=wait)
        holder.start()
        assert held.wait(TIMEOUT)

    monkeypatch.setattr(diagnostics, "release", released)
    monkeypatch.setattr(_core, "CLOSE_SHOWN_TIMEOUT", 0.2)
    try:
        with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.execution_context"):
            massive_session.close()
    finally:
        done.set()
        holder.join(TIMEOUT)
    assert "no update cycle ended within 0.2 s" in caplog.text
    del result, view


def test_close_fails_derived_tables_and_marks_their_stream_closed(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    stream_rows: Callable[[], list[dict[str, Any]]],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # A derived table keeps a released stream's blink table alive, so the stream is still in
    # the registry when the session closes: its row becomes CLOSED, and the derived table
    # fails with the close, keeping its rows for a snapshot.
    result = stream_query(massive_session).append_only()
    user_scope = new_scope()
    with user_scope.open():
        derived = result.tail(5)
    fake_streams.publishers[1].add(one_row(massive_session))
    wait_until(lambda: derived.size == 1)
    massive_session.release(result)
    assert [(r["State"], r["HeldResults"]) for r in stream_rows()] == [("RELEASED", 0)]
    streams = massive_session.streams()
    massive_session.close()
    rows = dh_arrow.to_arrow(streams).to_pylist()
    closed = f"{label(massive_session)} was closed; this table no longer updates"
    assert [(r["State"], r["HeldResults"], r["Error"]) for r in rows] == [("CLOSED", 0, closed)]
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the failure shut the stream down
    wait_until(lambda: bool(derived.j_table.isFailed()))
    assert derived.snapshot().size == 1  # the failed table keeps its rows
    assert len(dh_to_pylist(derived)) == 1
    with pytest.raises(DHError, match="failed table"):
        derived.tail(1)  # not a formula: that would capture this frame's locals in the JVM
    user_scope.release()


def test_close_with_no_live_tables_logs_info(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.session_core"):
        massive_session.close()
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, f"{massive_session._core.label} closed; 0 live tables failed, streams 0 []")
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
        massive_session.rate_limits()
    with pytest.raises(dm.MassiveError, match=message):
        massive_session.symbol_reads()
    with pytest.raises(dm.MassiveError, match=message):
        q.append_only()
    # Building queries and reaching namespaces never fail.
    assert isinstance(stream_query(massive_session), SymbolStreamQuery)
    assert isinstance(massive_session.stocks, _Stocks)


def test_close_on_the_update_graph_thread_fails(massive_session: dm.MassiveSession) -> None:
    errors: list[BaseException] = []
    seen = threading.Event()
    # The JVM holds the listener, and so its closure, until it collects it: the closure reaches
    # the session through a list emptied below.
    closes = [massive_session.close]

    def on_update(_update: object, _replay: bool) -> None:
        if seen.is_set():
            return
        try:
            closes[0]()
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
    closes.clear()
    errors.clear()  # its traceback holds the session too


def test_context_manager_closes() -> None:
    with dm.MassiveSession(dm.MassiveConfig(api_key="k")) as m:
        assert not m._core.closed
    assert m._core.closed


def test_release_argument_errors(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match="must be a Deephaven Table"):
        massive_session.release(cast("Table", "t"))
    with pytest.raises(
        ValueError, match=r"not a live result of this session: release\(\) takes"
    ) as caught:
        massive_session.release(new_table([string_col("Symbol", ["A"])]))
    assert "fetch(wait=False)" in str(caught.value)
    assert "streams()" in str(caught.value)
    assert "release the live table it came from" in str(caught.value)


def test_a_session_closed_while_a_stream_opens(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def open_stream(
        spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
        massive_session.close()
        return handle

    closing = transport_of(open_stream)()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: closing)
    with pytest.raises(dm.MassiveError, match="was closed while a stream was opening"):
        stream_query(massive_session).blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


# --- In the Deephaven console --------------------------------------------------------------------
# The console runs every command holding the update graph's exclusive lock (`console_command`),
# so no update cycle runs until the command returns: each call must return without waiting for
# one, and what it writes to a live table shows once the command is over.

TRADE = {
    "ev": "T",
    "sym": "MSFT",
    "x": 4,
    "i": "1",
    "z": 3,
    "p": 114.125,
    "s": 100,
    "c": [0, 12],
    "t": 1536036818784,
    "q": 1,
}
"""One trade frame of MSFT, as Massive streams it."""


def test_stream_verbs_work_in_a_console_command(
    *,
    ws_session: WsSession,
    console_command: Callable[[Callable[[], None]], None],
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    m = ws_session.session
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    made: dict[str, Table] = {}

    def command() -> None:
        trades = m.stocks.trades("MSFT")
        made["append_only"] = trades.append_only()
        made["ring"] = trades.ring(5)
        made["last_by"] = trades.last_by()
        blink = trades.blink()
        made["watched"] = m.stocks.quotes(watchlist).append_only()
        made["streams"] = m.streams()
        made["connections"] = m.connections()
        made["columns"] = trades.columns()
        m.release(blink)

    console_command(command)
    ws_session.subscribed("T.MSFT", "Q.AAPL")  # the watchlist is followed
    ws_session.publish(TRADE)
    for shape in ("append_only", "ring", "last_by"):
        rows_when(made[shape], lambda rows: [r["Price"] for r in rows] == [114.125])
    # The reaper releases `blink` only once the command lets go of the lock: 4 held at first.
    streams = rows_when(
        made["streams"],
        lambda rows: (
            sorted((r["Dataset"], r["HeldResults"]) for r in rows)
            == [("stocks.quotes", 1), ("stocks.trades", 3)]
        ),
    )
    assert sorted((r["Dataset"], r["State"], r["HeldResults"]) for r in streams) == [
        ("stocks.quotes", "RUNNING", 1),
        ("stocks.trades", "RUNNING", 3),
    ]
    rows_when(made["connections"], lambda rows: any(r["Transport"] == "WebSocket" for r in rows))
    assert made["columns"].size > 0
    for table in made.values():
        if table.is_refreshing:
            m.release(table)


def test_rest_and_flat_file_verbs_work_in_a_console_command(
    *,
    fake_rest: FakeRestServer,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    console_command: Callable[[Callable[[], None]], None],
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_rest.reply(
        "/v2/last/trade/AAPL",
        Reply({"status": "OK", "results": {"T": "AAPL", "t": 1617901342969834000, "p": 129.84}}),
    )
    fake_rest.reply(
        "/v2/aggs/ticker/AAPL/range/1/day/1577941200000/1578113999999",
        Reply({"status": "OK", "adjusted": True, "results": [{"t": 1577941200000, "c": 75.08}]}),
    )
    ipo = {"ticker": "RAPP", "issuer_name": "Rapport Therapeutics Inc.", "ipo_status": "history"}
    fake_rest.reply("/vX/reference/ipos", Reply({"status": "OK", "results": [ipo]}))
    fake_s3.put(
        "us_stocks_sip/day_aggs_v1/2023/03/2023-03-28.csv.gz",
        make_csv_gz(
            "ticker,volume,open,close,high,low,window_start,transactions\n"
            "BCC,248274,61.68,61.99,62.565,61.41,1680033600000000000,4073\n"
        ),
    )
    # The fake S3's test credentials, passed as a mapping as the flat-file tests do.
    credentials = {"s3_access_key_id": "AKIDFLATFILES", "s3_secret_access_key": "fake-secret"}
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, **cast("dict[str, Any]", credentials)
    )
    made: dict[str, Table] = {}
    sessions: list[dm.MassiveSession] = []

    def opening() -> None:
        m = dm.MassiveSession(config)
        sessions.append(m)
        made["fetch"] = m.stocks.last_trade("AAPL").fetch()
        bars = m.stocks.bars("AAPL", dm.Timespan.DAY)
        made["rest"] = bars.history("2020-01-02", "2020-01-03", source=dm.HistorySource.REST)
        files = m.stocks.bars("BCC", dm.Timespan.DAY, adjusted=False)
        made["files"] = files.history(
            "2023-03-28", "2023-03-28", source=dm.HistorySource.FLAT_FILES
        )
        made["poll"] = m.stocks.ipos().poll(60)
        made["streams"] = m.streams()

    console_command(opening)
    assert [r["Price"] for r in dh_to_pylist(made["fetch"])] == [129.84]
    assert [r["Close"] for r in dh_to_pylist(made["rest"])] == [75.08]
    assert [r["Symbol"] for r in dh_to_pylist(made["files"])] == ["BCC"]
    rows_when(made["poll"], lambda rows: [r["Symbol"] for r in rows] == ["RAPP"])

    def closing() -> None:
        sessions[0].close()
        with dm.MassiveSession(config) as other:  # opened and closed within one command
            made["other"] = other.stocks.last_trade("AAPL").fetch()

    console_command(closing)
    assert [r["Price"] for r in dh_to_pylist(made["other"])] == [129.84]
    rows_when(made["streams"], lambda rows: [r["State"] for r in rows] == ["CLOSED"])


def test_close_in_a_console_command_returns_without_waiting_for_a_cycle(
    *,
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreamTransport,
    console_command: Callable[[Callable[[], None]], None],
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    # The diagnostics writer cannot apply the final rows while the command holds the lock, so
    # close() returns without waiting for it; the rows, and the held view, follow once the
    # command is over, and the writer then ends.
    result = stream_query(massive_session).append_only()
    view = massive_session.streams()
    rows_when(view, lambda rows: [r["State"] for r in rows] == ["RUNNING"])
    writer = massive_session._core.diagnostics.thread
    console_command(massive_session.close)
    closed = f"{label(massive_session)} was closed; this table no longer updates"
    rows_when(view, lambda rows: [(r["State"], r["Error"]) for r in rows] == [("CLOSED", closed)])
    writer.join(TIMEOUT)
    assert not writer.is_alive()
    assert fake_streams.failed == {1: closed}
    assert fake_streams.shutdown[1].wait(TIMEOUT)  # the failure shut the stream down
    del result, view


def test_repr_of_a_session_whose_construction_failed() -> None:
    partial = object.__new__(dm.MassiveSession)
    assert repr(partial) == "<MassiveSession (not started)>"


def test_verbs_work_from_a_thread_without_an_execution_context(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    # The library enters the session's context itself; the caller need not.
    q = stream_query(massive_session)
    reactive = stream_query(massive_session, new_table([string_col("Symbol", ["AAPL"])]))
    with ThreadPoolExecutor(1) as pool:  # a plain thread: no execution context of its own
        results = [
            pool.submit(q.append_only).result(TIMEOUT),
            pool.submit(q.ring, 5).result(TIMEOUT),
            pool.submit(reactive.last_by).result(TIMEOUT),
        ]
    assert all(result.is_refreshing for result in results)
    for result in results:
        massive_session.release(result)


def test_a_session_that_reads_no_flat_file_never_imports_boto3() -> None:
    # A fresh interpreter starts its own JVM (this process's cannot be shared), opens a session
    # with S3 credentials configured, reads its diagnostics, closes it, and reports which of
    # boto3 and botocore it loaded. `os._exit` skips the interpreter shutdown that can crash
    # after an embedded JVM. The script is a literal; `-P` keeps the working directory off
    # `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import json, os, sys\n"
                "from deephaven_server import Server\n"
                "Server(port=0, jvm_args=['-Xmx512m']).start()\n"
                "import deephaven_massive as dm\n"
                "config = dm.MassiveConfig(\n"
                "    api_key='k', s3_access_key_id='AKID', s3_secret_access_key='not-used'\n"
                ")\n"
                "m = dm.MassiveSession(config)\n"
                "m.streams()\n"
                "m.close()\n"
                "loaded = sorted({'boto3', 'botocore'} & set(sys.modules))\n"
                "print('LOADED ' + json.dumps(loaded), flush=True)\n"
                "os._exit(0)\n"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    )
    [line] = [line for line in result.stdout.splitlines() if line.startswith("LOADED ")]
    assert json.loads(line.removeprefix("LOADED ")) == []


# --- The composition of a session ------------------------------------------------------------


def test_every_namespace_module_feeds_the_catalog() -> None:
    package = Path(_namespaces.__file__).parent
    found = {info.name for info in pkgutil.iter_modules([str(package)])} - {"query_building"}
    named = [module.__name__ for module in _session.NAMESPACE_MODULES]
    assert len(named) == len(set(named))
    assert set(named) == {f"deephaven_massive._namespaces.{name}" for name in found}
    declared = [spec for module in _session.NAMESPACE_MODULES for spec in module.DATASETS]
    assert all(isinstance(spec, DatasetSpec) for spec in declared)
    assert list(_session.CATALOG.by_name.values()) == declared


def test_a_session_holds_the_catalog_and_its_clients(massive_session: dm.MassiveSession) -> None:
    core = massive_session._core
    assert core.catalog is _session.CATALOG
    assert isinstance(core._ports, SessionClients)


def test_every_per_class_description_is_shown_by_a_dataset_of_its_class() -> None:
    # A source corrected for one asset class only helps if a dataset of that class shows the
    # text: none may read the source, or an earlier source's text may win the column.
    shown: dict[str, set[str]] = {}
    for spec in _session.CATALOG.by_name.values():
        if spec.asset_class is not ClassFromPrefix.ANY:
            texts = shown.setdefault(spec.asset_class.value, set())
            texts.update(c.description for c in _session.CATALOG.dataset_schema(spec).columns)
    missing = [
        (transport, key, item.wire_key, asset_class)
        for transport in Transport
        for key, source in sources(transport).items()
        for item in source.fields
        for asset_class, text in item.class_descriptions
        if text not in shown.get(asset_class, set())
    ]
    assert missing == []


def test_rate_limits_show_one_ready_row_per_subscription_and_other(
    massive_session: dm.MassiveSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    assert massive_session._core.diagnostics.wait_until_shown(30)
    rows = dh_to_pylist(massive_session.rate_limits())
    assert sorted((r["Bucket"], r["State"], r["RequestsPerMinute"]) for r in rows) == [
        ("currencies", "READY", None),
        ("futures", "READY", None),
        ("indices", "READY", None),
        ("options", "READY", None),
        ("other", "READY", None),
        ("stocks", "READY", None),
    ]


def test_the_symbol_reads_view_works_in_a_console_command_and_is_released(
    *,
    massive_session: dm.MassiveSession,
    console_command: Callable[[Callable[[], None]], None],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    made: dict[str, Table] = {}

    def command() -> None:
        made["symbol_reads"] = massive_session.symbol_reads()

    console_command(command)
    view = made.pop("symbol_reads")
    assert dh_to_pylist(view) == []
    assert [column.name for column in view.columns][:2] == ["StreamId", "Symbol"]
    massive_session.release(view)
    massive_session.release(view)  # twice does nothing
