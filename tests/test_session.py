"""Tests for `MassiveSession`: construction, the public surface, diagnostics, and `close()`."""

from __future__ import annotations

import gc
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
from deephaven import new_table, time_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.execution_context import make_user_exec_ctx
from deephaven.table_listener import listen

import deephaven_massive as dm
from deephaven_massive import _namespaces, _session
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import AssetClass
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._massive import Transport, sources
from deephaven_massive._namespaces import reference
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._namespaces.crypto import _Crypto
from deephaven_massive._namespaces.economy import _Economy
from deephaven_massive._namespaces.forex import _Forex
from deephaven_massive._namespaces.futures import _Futures
from deephaven_massive._namespaces.indicators import _Indicators
from deephaven_massive._namespaces.indices import _Indices
from deephaven_massive._namespaces.options import _Options
from deephaven_massive._namespaces.partners import _Benzinga, _EtfGlobal, _Fable, _Partners, _Tmx
from deephaven_massive._namespaces.reference import _Reference
from deephaven_massive._namespaces.stocks import _Stocks
from deephaven_massive._query import Query, StreamQuery
from deephaven_massive._tables.liveness import (
    StreamHandle,
    new_scope,
)

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.
    from collections.abc import Callable

    from deephaven.table import Table

    from deephaven_massive._tables.core import SessionCore
    from tests._fakes.stream import FakeStreamTransport

TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
    keys={(Transport.WS, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)


def stream_query(session: dm.MassiveSession, symbols: object = ("AAPL",)) -> StreamQuery:
    q = build_query(session._core, SPEC, symbols=cast("list[str]", symbols))
    assert isinstance(q, StreamQuery)
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
        "reference",
        "release",
        "snapshots",
        "stocks",
        "streams",
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


def test_a_same_key_session_warns_only_when_it_cannot_share(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Equal connection settings share the connections, so only differing settings warn.
    config = dm.MassiveConfig(api_key="shared-key")
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        first = dm.MassiveSession(config)
        sharing = dm.MassiveSession(config)
        other = dm.MassiveSession(dm.MassiveConfig(api_key="other-key"))
        assert caplog.records == []
        sharing.close()
        apart = dm.MassiveSession(
            dm.MassiveConfig(api_key="shared-key", flush_interval=0.5, ack_timeout=9.0)
        )
    warnings = [r.getMessage() for r in caplog.records if "same API key" in r.getMessage()]
    assert warnings == [
        (
            f"{label(apart)} uses the same API key as {label(first)} with different connection "
            "settings (ack_timeout, flush_interval), so the two cannot share WebSocket "
            "connections; on an individual plan, which allows one connection per cluster, they "
            "compete for it"
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
        ["X:BTCUSD", "aapl", "c:eurusd", "O:SPY251219C00600000"],
        sort="ticker",
        order=dm.Order.DESC,
        limit=3,
        filters={"type": "options"},
    )
    assert type(query) is Query
    assert query._spec is reference.SNAPSHOTS
    assert query._spec is _session.CATALOG.by_name["reference.snapshots"]
    assert query._request == Request(
        symbols=("AAPL", "C:EURUSD", "O:SPY251219C00600000", "X:BTCUSD"),
        params=(("order", dm.Order.DESC), ("sort", "ticker")),
        filters=(("type", "options"),),
        limit=3,
    )
    with pytest.raises(ValueError, match=r"m\.stocks\.snapshots\(dm\.ALL\)"):
        massive_session.snapshots(dm.ALL)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        massive_session.snapshots(["AAPL"], order=cast("dm.Order", "asc"))
    with pytest.raises(ValueError, match="limit must be positive"):
        massive_session.snapshots(["AAPL"], limit=0)
    with pytest.raises(ValueError, match="filter 'type' takes"):
        massive_session.snapshots(["AAPL"], filters={"type": "CS"})
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
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._tables.core"):
        massive_session.close()
        massive_session.close()
    assert caplog.messages == ["session closed; 1 live tables failed"]
    # Only the stream still in the registry is closed; the shut-down one keeps its last row.
    rows = sorted(dh_arrow.to_arrow(streams).to_pylist(), key=lambda r: r["StreamId"])
    closed = f"{label(massive_session)} was closed; this table no longer updates"
    assert [(r["State"], r["Error"]) for r in rows] == [("CLOSED", closed), ("RELEASED", None)]
    assert fake_streams.failed == {1: closed}
    massive_session.release(live)  # release() keeps working after close()


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
        derived.where("true")
    user_scope.release()


def test_close_with_no_live_tables_logs_info(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="deephaven_massive._tables.core"):
        massive_session.close()
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "session closed; 0 live tables failed")
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
            self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
        ) -> StreamHandle:
            handle = fake_streams.open_stream(spec, request, channel, stream_id=stream_id)
            massive_session.close()
            return handle

    closing = Closing()
    monkeypatch.setattr(massive_session._core, "stream_transport", lambda: closing)
    with pytest.raises(dm.MassiveError, match="was closed while a stream was opening"):
        stream_query(massive_session).blink()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


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
    found = {info.name for info in pkgutil.iter_modules([str(package)])} - {"base"}
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
        if spec.asset_class is not None:
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
