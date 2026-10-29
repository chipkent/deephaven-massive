"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end: a real
session whose `WsConnections` reaches the fake Massive server (`tests/_fakes/ws.py`) through
real `WsClient`s, with every client callback recorded so a test waits for exactly the event it
needs. Flushes are explicit (`Harness.flush`): the flush interval is an hour unless a test needs
the flush thread itself.
"""

from __future__ import annotations

import gc
import importlib
import json
import logging
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col
from deephaven.stream.table_publisher import table_publisher

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Status, StatusKind, Transport, WsState
from deephaven_massive._namespaces.base import build_query
from deephaven_massive._query import StreamQuery
from deephaven_massive._tables.diagnostics import Diagnostics
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.stream import open_stream_tables
from deephaven_massive._tables.watchlist import SymbolWatch
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._massive import DriftReport, Reject, Source, WireError
    from tests._fakes.ws import FakeMassiveServer


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
ToPylist: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
TIMEOUT = 15.0
_JAVA_SYSTEM = cast("Any", importlib.import_module("jpy")).get_type("java.lang.System")
W = Transport.WS


# --- The live harness: a session on the fake server --------------------------------------------

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))
TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
FOREX_FMV = DatasetSpec(
    name="test.forex_fmv",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "FMV"},
    family=ChannelFamily.FMV,
    keys={(W, "forex/FMV"): SourceKeys(KeySpec("Ticker"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={None: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
OPTION_QUOTES = DatasetSpec(
    name="test.option_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={None: "Q"},
    keys={(W, "options/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

QUOTES_FUTURES = DatasetSpec(
    name="test.futures_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "Q"},
    keys={(W, "futures/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

REALTIME = HostDecision("socket.test", Recency.REALTIME, "test evidence")


class SpyClient(_massive.WsClient):
    """A real client that also records the loop-thread calls the library makes on it."""

    def __init__(
        self,
        url: str,
        journal: list[tuple[object, ...]],
        *,
        sources: Sequence[Source],
        callbacks: dict[str, Callable[..., None]],
        backoff: float,
    ) -> None:
        super().__init__(
            url,
            api_key="test-key",
            sources=sources,
            on_open=callbacks["on_open"],
            on_status=callbacks["on_status"],
            on_data=callbacks["on_data"],
            on_state=callbacks["on_state"],
            reconnect_backoff=backoff,
            max_reconnect_backoff=backoff * 2,
        )
        self._journal = journal

    @override
    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        self._journal.append(("set_required", channel, keys, symbol))
        super().set_required(channel, keys, symbol=symbol)

    @override
    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        self._journal.append(("subscribe", tuple(topics)))
        super().subscribe(generation, topics)


class Harness:
    """A session whose connections reach the fake server, with every client event recorded."""

    def __init__(
        self,
        server: FakeMassiveServer,
        *,
        decide: Callable[[AssetClass, ChannelFamily], HostDecision] | None = None,
        flush_interval: float = 3600.0,
        backoff: float = 0.05,
        **config: object,
    ) -> None:
        self.server = server
        self.backoff = backoff
        self.decisions: list[tuple[AssetClass, ChannelFamily]] = []
        self.verdicts: list[tuple[AssetClass, ChannelFamily, HostDecision]] = []
        self._decide = decide or (lambda *_: REALTIME)
        self.cond = threading.Condition()
        self.journal: list[tuple[object, ...]] = []
        self.urls: list[str] = []
        self.frames = 0
        self.statuses: list[Status] = []
        self.states: list[WsState] = []
        self.opens = 0
        self.config = dm.MassiveConfig(
            api_key="test-key", flush_interval=flush_interval, **cast("dict[str, Any]", config)
        )
        self.session = dm.MassiveSession(self.config)
        clients: Any = self.session._core._ports
        self.connections = ws.WsConnections(
            self.session._core,
            resolve_host=self.resolve,
            record_verdict=self.record_verdict,
            client_factory=self.factory,
        )
        clients._streams = self.connections
        self.session._core.on_close(self.connections.close)

    def resolve(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        # The REST transport's cache: a recorded verdict replaces the pair's decision.
        self.decisions.append((asset_class, family))
        for verdict_class, verdict_family, decision in self.verdicts:
            if (verdict_class, verdict_family) == (asset_class, family):
                return decision
        return self._decide(asset_class, family)

    def record_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        with self.cond:
            self.verdicts.append((asset_class, family, decision))
            self.cond.notify_all()

    def factory(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
    ) -> SpyClient:
        self.urls.append(url)
        path = "/" + url.split("/", 3)[3]

        callbacks = {
            "on_open": self._noting(on_open, "opens"),
            "on_status": self._noting(on_status, "statuses"),
            "on_data": self._noting(on_data, "frames"),
            "on_state": self._noting(on_state, "states"),
        }
        return SpyClient(
            self.server.url(path),
            self.journal,
            sources=sources,
            callbacks=callbacks,
            backoff=self.backoff,
        )

    def _noting(self, callback: Callable[..., None], what: str) -> Callable[..., None]:
        def call(*event: object) -> None:
            callback(*event)
            self._note(what, event)

        return call

    def _note(self, what: str, event: tuple[object, ...]) -> None:
        with self.cond:
            if what == "opens":
                self.opens += 1
            elif what == "frames":
                self.frames += 1
            elif what == "statuses":
                self.statuses.append(cast("Status", event[1]))
            else:
                self.states.append(cast("WsState", event[0]))
            self.cond.notify_all()

    def wait(self, predicate: Callable[[Harness], bool]) -> None:
        with self.cond:
            assert self.cond.wait_for(lambda: predicate(self), TIMEOUT)

    def join(self) -> dm.MassiveSession:
        # A second session with the same key and settings, sharing this harness's connections.
        session = dm.MassiveSession(self.config)
        connections = ws.WsConnections(
            session._core,
            resolve_host=self.resolve,
            record_verdict=self.record_verdict,
            client_factory=self.factory,
        )
        assert connections.group is self.connections.group
        cast("Any", session._core._ports)._streams = connections
        session._core.on_close(connections.close)
        return session

    def query(
        self, spec: DatasetSpec, symbols: object, session: dm.MassiveSession | None = None
    ) -> StreamQuery:
        core = (session or self.session)._core
        q = build_query(core, spec, symbols=cast("list[str]", symbols))
        assert isinstance(q, StreamQuery)
        return q

    def connection(self, url: str = "wss://socket.test/stocks") -> ws.Connection:
        with self.connections.group.lock:
            return self.connections.group.connections[url]

    def send(self, *items: dict[str, object]) -> None:
        before = self.frames
        self.server.send(json.dumps(list(items)))
        self.wait(lambda h: h.frames > before)

    def flush(self) -> None:
        with self.connections.group.lock:
            connections = list(self.connections.group.connections.values())
        for connection in connections:
            connection.flusher.flush()

    def on_loop(self, connection: ws.Connection, action: Callable[[], object]) -> object:
        done = threading.Event()
        result: list[object] = []

        def run() -> None:
            result.append(action())
            done.set()

        assert connection.post(run)
        assert done.wait(TIMEOUT)
        return result[0]

    def subscribed(self, *topics: str) -> None:
        # Wait until the latest connection has received a subscribe frame naming each topic.
        def named() -> bool:
            sent = {
                t
                for f in self.server.frames()
                if json.loads(f)["action"] == "subscribe"
                for t in json.loads(f)["params"].split(",")
            }
            return set(topics) <= sent

        self.server.wait_for(lambda s: bool(s.connections) and s.connections[-1].auth is not None)
        self.server.wait_for(lambda _: named())

    def unsubscribed(self, *topics: str) -> None:
        def named() -> bool:
            gone = {
                t
                for f in self.server.frames()
                if json.loads(f)["action"] == "unsubscribe"
                for t in json.loads(f)["params"].split(",")
            }
            return set(topics) <= gone

        self.server.wait_for(lambda _: named())

    def close(self) -> None:
        self.session.close()


@pytest.fixture(autouse=True)
def _no_connection_outlives_its_test() -> Iterator[None]:
    # A client left running reconnects forever, and through whatever proxy a later test sets.
    yield
    wait_for(
        lambda: (
            not [
                t
                for t in threading.enumerate()
                if t.name.startswith(("massive-ws ", "massive-flush ")) and t.is_alive()
            ]
        )
    )


@pytest.fixture
def harness(fake_ws_server: FakeMassiveServer) -> Iterator[Harness]:
    h = Harness(fake_ws_server)
    yield h
    h.close()


def trade(symbol: str, price: float = 1.0, **fields: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ev": "T",
        "sym": symbol,
        "x": 4,
        "i": "1",
        "p": price,
        "q": 1,
        "t": 1_700_000_000_000,
        "s": 100,
        "z": 3,
        "ds": "100",
    }
    row.update(fields)
    return row


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def streams_of(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert harness.session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(dh_to_pylist(harness.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert harness.session._core.diagnostics.barrier().wait(TIMEOUT)
    return [r for r in dh_to_pylist(harness.session.connections()) if r["Transport"] == "WS"]


# --- Connections and streams, end to end ------------------------------------------------------


def test_the_channel_keys_are_declared_before_its_first_subscribe(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    required = ("set_required", "T", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    assert harness.journal[:2] == [required, ("subscribe", ("T.AAPL",))]
    harness.session.release(result)


def test_a_stream_publishes_only_its_symbols(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], *, rows_when: RowsWhen
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    every = harness.query(TRADES, dm.ALL).append_only()
    harness.subscribed("T.AAPL", "T.*")
    harness.send(trade("AAPL", 1.5), trade("MSFT", 2.0))
    harness.flush()
    assert [r["Price"] for r in rows_when(aapl, lambda rows: len(rows) == 1)] == [1.5]
    every_rows = rows_when(every, lambda rows: len(rows) == 2)
    assert sorted(r["Symbol"] for r in every_rows) == ["AAPL", "MSFT"]
    rows = streams_of(harness, dh_to_pylist)
    assert [(r["Symbols"], r["RowsPublished"], r["Host"], r["Path"]) for r in rows] == [
        ("AAPL", 1, "socket.test", "/stocks"),
        ("ALL", 2, "socket.test", "/stocks"),
    ]


def test_equal_queries_share_one_stream_and_one_connection(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    second = harness.query(TRADES, ["aapl"]).last_by()
    quotes = harness.query(QUOTES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL", "Q.AAPL")
    assert len(harness.urls) == 1
    assert harness.decisions == [(AssetClass.STOCKS, ChannelFamily.TICK)] * 2
    assert harness.connection().leases == 2
    for result in (first, second, quotes):
        harness.session.release(result)


def test_wire_symbols_become_canonical(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], *, rows_when: RowsWhen
) -> None:
    result = harness.query(FOREX, ["C:EURUSD"]).append_only()
    harness.subscribed("C.EUR-USD")
    harness.send({"ev": "C", "p": "EUR/USD", "a": 1.1, "b": 1.0, "t": 1_700_000_000_000, "x": 1})
    harness.connection("wss://socket.test/forex").flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_an_fmv_frame_in_the_canonical_spelling_reaches_its_forex_stream(
    harness: Harness, *, rows_when: RowsWhen
) -> None:
    # FMV frames name the pair `C:EURUSD`, while the topic spells it `EUR-USD`.
    result = harness.query(FOREX_FMV, ["C:EURUSD"]).append_only()
    harness.subscribed("FMV.EUR-USD")
    harness.send({"ev": "FMV", "sym": "C:EURUSD", "fmv": 1.1, "t": 1_700_000_000_000_000_000})
    harness.connection(harness.urls[-1]).flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_a_refused_topic_fails_only_its_streams(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    fake_ws_server.refuse["Q.AAPL"] = "not authorized to access Q.AAPL"
    trades = harness.query(TRADES, ["AAPL"]).append_only()
    harness.subscribed("T.AAPL")
    quotes = harness.query(QUOTES, ["AAPL"]).blink()
    harness.wait(lambda h: any(s.kind is StatusKind.REFUSED for s in h.statuses))
    harness.flush()
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "socket.test/stocks refused Q.AAPL: not authorized to access Q.AAPL" in rows[1]["Error"]
    assert "MassiveConfig.ws_hosts[AssetClass.STOCKS]" in rows[1]["Error"]
    harness.send(trade("AAPL"))
    harness.flush()
    rows_when(trades, lambda rows: len(rows) == 1)  # the connection kept running
    del quotes


def test_a_rejected_row_fails_only_the_streams_of_its_symbol(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], *, rows_when: RowsWhen
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    msft = harness.query(TRADES, ["MSFT"]).blink()
    every = harness.query(TRADES, dm.ALL).blink()
    harness.subscribed("T.AAPL", "T.MSFT", "T.*")
    bad = trade("MSFT")
    del bad["t"]  # a required key
    harness.send(bad, trade("AAPL"))
    harness.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED", "FAILED"]
    assert "no SipTimestamp" in rows[1]["Error"]
    del msft, every


def test_a_row_without_a_symbol_fails_only_the_all_streams(
    harness: Harness,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).blink()
    every = harness.query(TRADES, dm.ALL).blink()
    harness.subscribed("T.AAPL", "T.*")
    nameless = trade("AAPL")
    del nameless["sym"]
    unreadable = trade("not a symbol")
    del unreadable["t"]
    with caplog.at_level(logging.ERROR, logger=ws.__name__):
        harness.send(nameless, unreadable)
    harness.flush()
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert caplog.text.count("has no valid symbol; failing the channel's ALL streams") == 2
    del aapl, every


def test_a_timestamp_out_of_bounds_fails_only_its_streams(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], *, rows_when: RowsWhen
) -> None:
    aapl = harness.query(TRADES, ["AAPL"]).append_only()
    msft = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.AAPL", "T.MSFT")
    harness.send(trade("MSFT", t=5), trade("AAPL"))
    harness.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(harness, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "timestamp unit mismatch for test.trades.Timestamp: 5 ms" in rows[1]["Error"]
    del msft


def test_drift_is_logged_once_and_shown(
    harness: Harness,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    with caplog.at_level(logging.WARNING):
        harness.send(trade("AAPL", zz=1))
        harness.send(trade("AAPL", zz=2))
        harness.flush()
    assert caplog.text.count("field 'zz' is not in the schema") == 1
    [connection] = connections_of(harness, dh_to_pylist)
    assert connection["SchemaDrift"] == "T:zz+"
    [stream] = streams_of(harness, dh_to_pylist)
    assert stream["SchemaDrift"] == "WS:zz+"
    harness.session.release(result)


def test_releasing_the_last_result_unsubscribes(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    second = harness.query(TRADES, ["AAPL"]).ring(5)
    harness.subscribed("T.AAPL")
    harness.session.release(first)
    harness.session.release(second)
    harness.unsubscribed("T.AAPL")
    assert harness.connection().leases == 0


def test_a_derived_table_keeps_the_stream_alive(
    harness: Harness, fake_ws_server: FakeMassiveServer, rows_when: RowsWhen
) -> None:
    result = harness.query(TRADES, ["AAPL"]).append_only()
    harness.subscribed("T.AAPL")
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [result.tail(100)]
    harness.session.release(result)
    del result
    gc.collect()
    harness.send(trade("AAPL"))
    harness.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    derived.clear()
    scope.release()
    harness.unsubscribed("T.AAPL")


def test_close_returns_promptly_during_a_backoff_wait(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, backoff=60.0)
    fake_ws_server.reject_next(503)
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.wait(lambda h: WsState.RECONNECTING in h.states)
    started = time.monotonic()
    harness.close()
    assert time.monotonic() - started < 5.0
    assert WsState.CLOSED in harness.states
    del result


def test_a_rejected_key_fails_every_stream(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    q = harness.query(TRADES, ["AAPL"])
    result = q.blink()
    harness.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    failed = harness.connection()
    fake_ws_server.auth_replies.append("auth_failed")  # for the reconnect
    fake_ws_server.close(1011)
    harness.wait(lambda h: WsState.FAILED in h.states)
    with harness.connections.group.lock:
        assert failed.retired
        assert harness.connections.group.connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(harness, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "rejected the API key" in row["LastError"]
    [stream] = streams_of(harness, dh_to_pylist)
    assert stream["State"] == "FAILED"
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(harness.urls) == 2
    del result, again


def test_a_fatal_handshake_names_the_status_and_host(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    fake_ws_server.reject_next(404)  # for the reconnect
    fake_ws_server.close(1011)
    harness.wait(lambda h: WsState.FAILED in h.states)
    [row] = connections_of(harness, dh_to_pylist)
    assert row["LastError"].startswith("the WebSocket connection to socket.test/stocks failed:")
    assert "404" in row["LastError"]
    del result


def test_a_reconnect_resends_the_desired_set(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    fake_ws_server.close(1011)
    harness.wait(lambda h: h.opens == 2)
    harness.subscribed("T.AAPL")  # on the new connection
    harness.wait(lambda h: sum(s.kind is StatusKind.ACK for s in h.statuses) == 2)
    [row] = connections_of(harness, dh_to_pylist)
    assert (row["State"], row["Reconnects"], row["Topics"], row["AckedTopics"]) == (
        "CONNECTED",
        1,
        1,
        1,
    )
    assert WsState.RECONNECTING in harness.states
    harness.session.release(result)


def test_an_idle_connection_closes_and_a_new_stream_opens_another(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "IDLE_GRACE", 0.05)
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    harness.session.release(result)
    harness.wait(lambda h: WsState.CLOSED in h.states)
    [row] = connections_of(harness, dh_to_pylist)
    assert row["State"] == "CLOSED"
    again = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.MSFT")
    assert len(harness.urls) == 2
    harness.session.release(again)


def test_an_opening_stream_cancels_the_idle_close(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.session.release(result)
    harness.unsubscribed("T.AAPL")
    with harness.connections.group.lock:
        connection.leases += 1  # a stream is opening
    retired = harness.on_loop(
        connection, lambda: harness.connections.group.retire(connection, idle=True)
    )
    assert retired is False
    with harness.connections.group.lock:
        connection.leases -= 1


def test_a_missing_acknowledgement_reconnects(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, ack_timeout=0.05)
    try:
        fake_ws_server.silent.add("T.AAPL")
        result = harness.query(TRADES, ["AAPL"]).blink()
        harness.wait(lambda h: h.opens >= 2)
        assert [json.loads(f)["params"] for f in fake_ws_server.frames(0)] == ["T.AAPL", "T.AAPL"]
        del result
    finally:
        harness.close()


def test_a_reactive_table_follows_its_rows(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["aapl", "not a symbol!"])]))
    result = harness.query(TRADES, watchlist).append_only()
    harness.subscribed("T.AAPL")
    harness.send(trade("AAPL"), trade("MSFT"))
    harness.flush()
    rows_when(result, lambda rows: len(rows) == 1)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    harness.subscribed("T.MSFT")
    watchlist.delete(new_table([string_col("Symbol", ["aapl"])]))
    harness.unsubscribed("T.AAPL")
    harness.send(trade("AAPL", 2.0), trade("MSFT", 3.0))
    harness.flush()
    shown = rows_when(result, lambda rows: len(rows) == 2)
    assert [r["Symbol"] for r in shown] == ["AAPL", "MSFT"]
    # The row that is no valid symbol never became a topic.
    sent = [json.loads(f)["params"] for f in fake_ws_server.frames()]
    assert sent == ["T.AAPL", "T.MSFT", "T.AAPL"]


def test_a_derived_table_keeps_following_the_watchlist(
    harness: Harness, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = harness.query(TRADES, watchlist).append_only()
    harness.subscribed("T.AAPL")
    derived = result.tail(100)
    harness.session.release(result)
    del result
    gc.collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    harness.subscribed("T.MSFT")
    harness.send(trade("MSFT"))
    harness.flush()
    rows_when(derived, lambda rows: len(rows) == 1)


def test_static_options_beyond_the_cap_raise(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000", "O:SPY251219C00620000"]
    with pytest.raises(ValueError, match="3 new option topics and 0 in use exceed the 2"):
        harness.query(OPTIONS, contracts).blink()
    first = harness.query(OPTIONS, contracts[:2]).blink()
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        harness.query(OPTIONS, contracts[2:]).blink()
    every = harness.query(OPTIONS, dm.ALL).blink()  # a wildcard counts zero
    harness.session.release(first)
    harness.session.release(every)


def test_the_cap_message_counts_topics_not_contracts(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Quotes of the contracts already traded are new topics, though no contract is new.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 3)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    trades = harness.query(OPTIONS, contracts).blink()
    with pytest.raises(
        ValueError,
        match=r"^test.option_quotes: 2 new option topics and 2 in use exceed the 3 topics one "
        r"/options connection carries; a topic is one contract on one channel",
    ):
        harness.query(OPTION_QUOTES, contracts).blink()
    harness.session.release(trades)


def test_reactive_options_beyond_the_cap_are_deferred(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00610000", "O:SPY251219C00600000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    result = harness.query(OPTIONS, watchlist).blink()
    harness.subscribed("T.O:SPY251219C00600000")
    connection = harness.connection("wss://socket.test/options")
    harness.on_loop(connection, lambda: None)
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["State"], row["Deferred"], row["Error"]) == (
        "RUNNING",
        1,
        "over capacity: 2/1; 1 deferred",
    )
    watchlist.delete(new_table([string_col("Symbol", ["O:SPY251219C00600000"])]))
    harness.subscribed("T.O:SPY251219C00610000")
    harness.on_loop(connection, lambda: None)
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["Deferred"], row["Error"]) == (0, None)
    del result


def unknown_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.UNKNOWN, "no timeframe in futures snapshot")


def test_the_real_time_verdict_moves_the_connection_once(
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        harness.connections.group.reconnect_backoff = 0.01
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda h: h.opens == 2)
        assert harness.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
        harness.subscribed("T.ESZ5")
        [row] = connections_of(harness, dh_to_pylist)
        assert (row["Host"], row["Recency"]) == ("delayed.test", "DELAYED")
        assert row["Evidence"] == "server verdict: Real-time data requires an upgrade"
        [stream] = streams_of(harness, dh_to_pylist)
        assert stream["State"] == "RUNNING"
        assert harness.verdicts == [
            (
                AssetClass.FUTURES,
                ChannelFamily.TICK,
                HostDecision(
                    "delayed.test",
                    Recency.DELAYED,
                    "server verdict: Real-time data requires an upgrade",
                ),
            )
        ]
        assert stream["Host"] == "delayed.test"  # the stream's row follows the move
        other = harness.query(FUTURES, ["NQZ5"]).blink()  # the recorded verdict decides
        assert harness.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
        harness.session.release(result)
        harness.session.release(other)
    finally:
        harness.close()


def realtime_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.REALTIME, "timeframe=REAL-TIME")


@pytest.mark.parametrize(
    ("decide", "pinned"),
    [(unknown_futures, {AssetClass.FUTURES: "socket.test"}), (realtime_futures, {})],
)
def test_a_verdict_otherwise_fails_the_step(
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    decide: Callable[[AssetClass, ChannelFamily], HostDecision],
    pinned: dict[AssetClass, str],
) -> None:
    harness = Harness(fake_ws_server, decide=decide, ws_hosts=pinned)
    try:
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda h: any(s.kind is StatusKind.REFUSED for s in h.statuses))
        harness.connection("wss://socket.test/futures").flusher.flush()
        [stream] = streams_of(harness, dh_to_pylist)
        assert stream["State"] == "FAILED"
        assert len(harness.urls) == 1
        del result
    finally:
        harness.close()


def test_a_failed_open_returns_its_lease(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no tables")

    monkeypatch.setattr(ws, "open_stream_tables", broken)
    with pytest.raises(RuntimeError, match="no tables"):
        harness.query(TRADES, ["AAPL"]).blink()
    assert harness.connection().leases == 0
    assert harness.connections.group.streams == {}


def test_a_watch_that_cannot_start_releases_the_stream(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: object) -> None:
        raise RuntimeError("no watch")

    monkeypatch.setattr(SymbolWatch, "start", broken)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        harness.query(TRADES, watchlist).blink()
    assert harness.connection().leases == 0


def test_a_lost_race_closes_the_spare_connection(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2)
    built: list[ws.Connection] = []
    real: Callable[..., ws.Connection] = ws.Connection

    def racing(*args: object, **kwargs: object) -> ws.Connection:
        connection = real(*args, **kwargs)
        built.append(connection)
        barrier.wait(TIMEOUT)
        return connection

    monkeypatch.setattr(ws, "Connection", racing)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda s: harness.query(TRADES, [s]).blink(), ["AAPL", "MSFT"]))
    assert len(built) == 2
    assert len(harness.connections.group.connections) == 1
    spare = next(c for c in built if c not in harness.connections.group.connections.values())
    assert spare.join(TIMEOUT)
    for result in results:
        harness.session.release(result)


def test_a_closed_transport_opens_nothing(harness: Harness) -> None:
    request = harness.query(TRADES, ["AAPL"])._request
    harness.connections.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        harness.connections.open_stream(
            harness.session._core, TRADES, request, channel="T", stream_id=99
        )


def test_a_shutdown_after_the_connection_ended_is_logged(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    connection.close()
    assert connection.join(TIMEOUT)
    with caplog.at_level(logging.INFO, logger=ws.__name__):
        harness.session.release(result)
        wait_for(lambda: "has closed; dropped release_stream" in caplog.text)


def test_a_failing_shutdown_is_logged(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(stream_id: int) -> None:
        raise RuntimeError(f"registry {stream_id}")

    monkeypatch.setattr(harness.session._core, "stream_shut_down", broken)
    harness.connections._shut_down(42)
    assert "the shutdown of stream 42 failed" in caplog.text


def test_the_flush_thread_publishes_by_itself(
    fake_ws_server: FakeMassiveServer, rows_when: RowsWhen
) -> None:
    harness = Harness(fake_ws_server, flush_interval=0.01)
    try:
        result = harness.query(TRADES, ["AAPL"]).append_only()
        harness.subscribed("T.AAPL")
        harness.send(trade("AAPL"))
        rows_when(result, lambda rows: len(rows) == 1)
    finally:
        harness.close()


def spare_connection(harness: Harness, host: str) -> ws.Connection:
    return ws.Connection(
        harness.connections.group,
        exec_ctx=harness.session._core.exec_ctx,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
    )


def test_a_delayed_start_waits_and_close_cancels_it(harness: Harness) -> None:
    cancelled = spare_connection(harness, "never.test")
    cancelled.start(delay=60.0)
    cancelled.start()  # a second start does nothing
    cancelled.close()
    cancelled.start()  # nor does a start after close
    assert cancelled.join(TIMEOUT)
    assert harness.opens == 0
    late = spare_connection(harness, "late.test")
    late.start(delay=0.01)
    harness.wait(lambda h: h.opens == 1)
    late.close()
    assert late.join(TIMEOUT)


def test_changes_for_a_moved_stream_follow_it(harness: Harness) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    result = harness.query(TRADES, watchlist).blink()
    old = harness.connection()
    successor = spare_connection(harness, "next.test")
    successor.start()
    with harness.connections.group.lock:
        harness.connections.group.streams[1] = successor  # moved, as the verdict moves it
        successor.take(harness.connections)
        old.give(harness.connections)
        successor.awaiting.add(1)
    harness.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    # The old loop has ended, so its methods run here, as a late callback would have there.
    old.change_symbols(1, frozenset({"AAPL"}), frozenset())
    harness.subscribed("T.AAPL")  # on the successor
    old.release_stream(1)
    harness.unsubscribed("T.AAPL")
    successor.close()
    del result


def test_a_release_that_overtakes_the_adoption_waits_for_it(harness: Harness) -> None:
    # The shutdown callback read the moved lease and posted before the old loop's hand-over.
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    old = harness.connection()
    successor = spare_connection(harness, "next.test")
    successor.start()
    with harness.connections.group.lock:
        harness.connections.group.streams[1] = successor  # moved, as the verdict moves it
        successor.take(harness.connections)
        old.give(harness.connections)
        successor.awaiting.add(1)
    successor.post(successor.release_stream, 1)
    successor.post(successor.change_symbols, 2, frozenset(), frozenset())  # not awaited: dropped
    harness.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    harness.on_loop(successor, lambda: None)  # the adoption ran
    assert harness.on_loop(successor, lambda: successor.subscriptions.wanted) is False
    assert harness.on_loop(successor, lambda: dict(successor._streams)) == {}
    successor.close()
    assert successor.join(TIMEOUT)
    del result


def test_a_hand_over_to_a_failed_successor_fails_the_streams(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    old = harness.connection()
    successor = spare_connection(harness, "next.test")
    with harness.connections.group.lock:
        successor.failure = dm.MassiveError("rejected the API key")
    harness.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)  # its last flush failed the stream
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("FAILED", "rejected the API key")
    successor.close()
    assert successor.join(TIMEOUT)
    del result


def test_stress_concurrent_verbs_and_releases(harness: Harness) -> None:
    # 16 threads open and release overlapping streams at once; every topic is released.
    symbols = [f"S{i}" for i in range(8)]

    def churn(worker: int) -> None:
        for round_ in range(10):
            symbol = symbols[(worker + round_) % len(symbols)]
            result = harness.query(TRADES, [symbol]).blink()
            harness.session.release(result)

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(churn, range(16)))
    core = harness.session._core
    wait_for(lambda: not harness.connections.group.streams)
    connection = harness.connection()
    assert harness.on_loop(connection, lambda: connection.subscriptions.wanted) is False
    assert connection.leases == 0
    wait_for(lambda: core._streams == {})


# --- Edge cases of the connection's callbacks -------------------------------------------------


def test_late_callbacks_of_an_unknown_stream_are_ignored(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.on_loop(connection, lambda: connection.release_stream(999))
    harness.on_loop(connection, lambda: connection.change_symbols(999, frozenset(), frozenset()))
    harness.connections._symbols_changed(999, frozenset({"X"}), frozenset())
    harness.on_loop(connection, connection._idle_close)  # a stream is open: it stays
    stale = Status(kind=StatusKind.REFUSED, topics=("T.AAPL",), message="not authorized")
    harness.on_loop(connection, lambda: connection._on_status(999, stale))  # an older socket's
    assert harness.connection() is connection
    harness.flush()
    [row] = streams_of(harness, dh_to_pylist)
    assert row["State"] == "RUNNING"
    harness.session.release(result)


def test_a_rejected_row_nobody_receives_fails_nothing(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    bad = trade("MSFT")
    del bad["t"]
    harness.send(bad)
    harness.flush()
    [row] = streams_of(harness, dh_to_pylist)
    assert row["State"] == "RUNNING"
    harness.session.release(result)


def test_last_message_rows_are_throttled(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    written: list[int] = []
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    harness.send(trade("AAPL"))
    harness.send(trade("AAPL"))
    assert len(written) <= 1
    harness.session.release(result)


def test_an_index_value_refusal_names_the_business_host(harness: Harness) -> None:
    connection = ws.Connection(
        harness.connections.group,
        exec_ctx=harness.session._core.exec_ctx,
        url="wss://socket.test/indices",
        asset_class=AssetClass.INDICES,
        family=ChannelFamily.TICK,
        decision=REALTIME,
    )
    connection._refused(("V.I:SPX",), "not authorized")
    assert connection._last_error is not None
    assert "index values may need business.massive.com" in connection._last_error
    assert "ws_hosts[AssetClass.INDICES]" in connection._last_error
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_connection_skips_a_session_that_is_gone(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A freed session stays one of the connection's users until its closer runs on the reaper;
    # meanwhile the connection writes it no row.
    result = harness.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    harness.subscribed("T.O:SPY251219C00600000")
    connection = harness.connection("wss://socket.test/options")
    monkeypatch.setattr(harness.connections, "_core", lambda: None)
    written: list[object] = []
    monkeypatch.setattr(Diagnostics, "write", lambda _, row: written.append(row))
    harness.on_loop(connection, connection._write_row)
    drift = _massive.DriftReport(extra=frozenset({"zz"}))
    harness.on_loop(connection, lambda: connection._record_drift("T", drift))
    assert written == []
    monkeypatch.undo()
    assert harness.on_loop(
        connection, lambda: harness.connections.group.retire(connection, idle=False)
    )
    assert harness.connections.group.reroute(connection, "real-time") is False  # retired
    connection.close()  # retired: the session's close no longer reaches it
    assert connection.join(TIMEOUT)
    del result


def test_retiring_a_replaced_connection_keeps_its_replacement(harness: Harness) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    spare = spare_connection(harness, "spare.test")
    assert harness.connections.group.retire(spare, idle=False)
    assert harness.connection() is connection
    spare.close()
    assert spare.join(TIMEOUT)
    harness.session.release(result)


def test_a_verdict_is_taken_once(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        harness.connections.group.reconnect_backoff = 0.01
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        other = harness.query(TRADES, ["AAPL"]).blink()  # a stream the move leaves alone
        connection = harness.connection("wss://socket.test/futures")
        with harness.connections.group.lock:  # a fatal failure retired it meanwhile
            del harness.connections.group.connections[connection.url]
        assert harness.on_loop(
            connection, lambda: harness.connections.group.reroute(connection, "x")
        )
        assert harness.connections.group.reroute(connection, "x") is False  # once per connection
        harness.wait(lambda h: h.urls[-1] == "wss://delayed.test/futures")
        wait_for(lambda: "wss://delayed.test/futures" in harness.connections.group.connections)
        with harness.connections.group.lock:
            owners = dict(harness.connections.group.streams)
        assert owners[1].url == "wss://delayed.test/futures"
        assert owners[2].url == "wss://socket.test/stocks"
        assert len(harness.verdicts) == 1
        harness.session.release(result)
        harness.session.release(other)
    finally:
        harness.close()


def test_a_verdict_that_cannot_be_recorded_still_moves(
    fake_ws_server: FakeMassiveServer, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)

    def broken(*_: object) -> None:
        raise RuntimeError("no REST transport")

    try:
        harness.connections.group.reconnect_backoff = 0.01
        harness.connections.record_verdict = broken
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        connection = harness.connection("wss://socket.test/futures")
        assert harness.on_loop(
            connection, lambda: harness.connections.group.reroute(connection, "x")
        )
        harness.wait(lambda h: h.urls[-1] == "wss://delayed.test/futures")
        assert "could not record the real-time verdict" in caplog.text
        harness.session.release(result)
    finally:
        harness.close()


def test_a_connection_that_does_not_end_is_reported(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    monkeypatch.setattr(ws, "JOIN_TIMEOUT", 0.01)
    monkeypatch.setattr(connection, "join", lambda _: False)
    harness.connections.close()
    assert "has not ended 0 s after close()" in caplog.text
    del result


def test_the_default_factory_builds_real_clients(harness: Harness) -> None:
    factory = ws._WsClientFactory("test-key", backoff=15.0, max_backoff=30.0)
    client = factory(
        "wss://socket.test/stocks",
        sources=[],
        on_open=lambda *_: None,
        on_status=lambda *_: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
    )
    assert isinstance(client, _massive.WsClient)
    client.close()
    client.start()
    assert client.join(TIMEOUT)
    assert "test-key" not in repr(factory)


def test_a_timer_start_after_close_does_nothing(harness: Harness) -> None:
    connection = spare_connection(harness, "never.test")
    connection.close()
    connection._start_later()  # the timer fired just as close() cancelled it
    assert connection.join(TIMEOUT)
    assert harness.opens == 0


def test_a_new_stream_cancels_a_pending_idle_close(harness: Harness) -> None:
    first = harness.query(TRADES, ["AAPL"]).blink()
    connection = harness.connection()
    harness.subscribed("T.AAPL")
    harness.session.release(first)
    harness.unsubscribed("T.AAPL")
    assert harness.on_loop(connection, lambda: connection._idle is not None)
    second = harness.query(TRADES, ["MSFT"]).blink()
    harness.subscribed("T.MSFT")
    assert harness.on_loop(connection, lambda: connection._idle is None)
    harness.session.release(second)


def test_every_message_rewrites_the_row_once_the_interval_passed(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    written: list[int] = []
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 0)
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    harness.send(trade("AAPL"))
    assert written  # the message alone rewrote the row
    harness.session.release(result)


def test_contracts_of_opening_streams_count_against_the_cap(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    first = harness.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    connection = harness.connection("wss://socket.test/options")
    with harness.connections.group.lock:
        connection.cap_pending[99] = frozenset({"T.O:SPY251219C00610000"})  # still opening
        with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
            ws.WsConnections._check_cap(OPTIONS, connection, frozenset({"T.O:SPY251219C00620000"}))
        del connection.cap_pending[99]
    harness.session.release(first)


def test_the_documented_trade_frame_is_published(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    *,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    rows_when: RowsWhen,
) -> None:
    # The frame of Massive's WebSocket quickstart: no `q`, `ds`, or `trfi`, which read as null.
    result = harness.query(TRADES, ["MSFT"]).append_only()
    harness.subscribed("T.MSFT")
    before = harness.frames
    fake_ws_server.send(load_fixture("ingest/ws", "stocks_trades.json").decode())
    harness.wait(lambda h: h.frames > before)
    harness.flush()
    rows = rows_when(result, lambda rows: len(rows) == 2)
    assert [(r["Symbol"], r["Price"], r["Size"], r["Conditions"]) for r in rows] == [
        ("MSFT", 215.9721, 100, None),
        ("MSFT", 215.989, 1, [37]),
    ]
    assert rows[0]["Timestamp"].value == 1_611_082_428_813_000_000


def test_a_verdict_while_closing_moves_nothing(
    fake_ws_server: FakeMassiveServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        connection = harness.connection("wss://socket.test/futures")
        closed = iter([False, True])  # open when the verdict arrives, closing by the move
        real = cast("Callable[[ws.Connection], bool]", vars(ws.Connection)["closing"].fget)

        def closing(self: ws.Connection) -> bool:
            return next(closed) if self is connection else real(self)

        monkeypatch.setattr(ws.Connection, "closing", property(closing))
        assert harness.on_loop(
            connection, lambda: harness.connections.group.reroute(connection, "x")
        )
        assert connection.join(TIMEOUT)  # the move closed it instead of handing it over
        assert len(harness.urls) == 2  # the successor was built, then closed unused
        del result
    finally:
        harness.close()


def test_a_verdict_joins_an_open_delayed_connection(fake_ws_server: FakeMassiveServer) -> None:
    decisions = iter(
        [
            HostDecision("socket.test", Recency.UNKNOWN, "no timeframe"),
            HostDecision("delayed.test", Recency.DELAYED, "a stream already reads it"),
        ]
    )
    harness = Harness(fake_ws_server, decide=lambda *_: next(decisions))
    try:
        harness.connections.group.reconnect_backoff = 0.01
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        delayed = harness.query(QUOTES_FUTURES, ["ESZ5"]).blink()
        moved = harness.connection("wss://socket.test/futures")
        existing = harness.connection("wss://delayed.test/futures")
        assert harness.on_loop(moved, lambda: harness.connections.group.reroute(moved, "real-time"))
        wait_for(lambda: existing.leases == 2)
        assert harness.connection("wss://delayed.test/futures") is existing
        harness.session.release(result)
        harness.session.release(delayed)
    finally:
        harness.close()


# --- Races of the connection lifecycle -------------------------------------------------------


def test_a_retired_connection_is_never_leased(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The registry is read again after the new connection is built: a retired connection is
    # gone from it, and the lease goes to the new one, never to None.
    first = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    old = harness.connection()
    harness.connections.group.retire(old, idle=False)
    real = ws.WsConnections._check_cap
    looks: list[ws.Connection | None] = []

    def watching(
        spec: DatasetSpec, connection: ws.Connection | None, topics: frozenset[str]
    ) -> None:
        looks.append(connection)
        real(spec, connection, topics)

    monkeypatch.setattr(ws.WsConnections, "_check_cap", staticmethod(watching))
    second = harness.query(TRADES, ["MSFT"]).blink()
    replacement = harness.connection()
    assert replacement is not old
    assert looks == [None, None]  # nothing the first time, then the new one is registered
    harness.subscribed("T.MSFT")
    old.close()
    assert old.join(TIMEOUT)
    harness.session.release(first)
    harness.session.release(second)


def test_a_stream_opening_as_its_connection_fails_fails_too(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    *,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The connection fails fatally after the second stream leased it, before it is attached.
    first = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    real: Callable[..., object] = open_stream_tables

    def failing_meanwhile(*args: object, **kwargs: object) -> object:
        fake_ws_server.auth_replies.append("auth_failed")
        fake_ws_server.close(1011)
        harness.wait(lambda h: WsState.FAILED in h.states)
        return real(*args, **kwargs)

    monkeypatch.setattr(ws, "open_stream_tables", failing_meanwhile)
    with pytest.raises(dm.MassiveError, match=r"stream 2 failed: .*rejected the API key"):
        harness.query(TRADES, ["MSFT"]).blink()
    wait_for(lambda: [r["State"] for r in streams_of(harness, dh_to_pylist)] == ["FAILED"] * 2)
    rows = streams_of(harness, dh_to_pylist)
    assert all("rejected the API key" in r["Error"] for r in rows)
    del first


def test_a_verb_never_sees_a_failed_table_before_its_failure(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure is recorded before the table fails, so a verb in between builds its shape.
    q = harness.query(TRADES, ["AAPL"])
    first = q.blink()
    harness.subscribed("T.AAPL")
    core = harness.session._core
    recording = threading.Event()
    proceed = threading.Event()
    real = core.fail_stream

    def gated(stream_id: int, error: str) -> None:
        recording.set()
        assert proceed.wait(TIMEOUT)
        real(stream_id, error)

    monkeypatch.setattr(core, "fail_stream", gated)
    harness.send(trade("AAPL", t=10))  # a timestamp out of bounds fails the stream
    flushing = threading.Thread(target=harness.flush)
    flushing.start()
    assert recording.wait(TIMEOUT)
    second = q.append_only()  # the table has not failed yet: no raw DHError
    proceed.set()
    flushing.join(TIMEOUT)
    harness.session.release(second)
    del first


def test_a_new_channel_on_an_open_connection_declares_its_keys_first(harness: Harness) -> None:
    trades = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    quotes = harness.query(QUOTES, ["AAPL"]).blink()
    harness.subscribed("Q.AAPL")
    required = ("set_required", "Q", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    position = harness.journal.index(required)
    assert harness.journal[position + 1] == ("subscribe", ("Q.AAPL",))
    harness.session.release(trades)
    harness.session.release(quotes)


def test_a_reactive_streams_row_follows_its_watchlist(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["MSFT", "AAPL"])]))
    result = harness.query(TRADES, watchlist).blink()
    harness.subscribed("T.AAPL", "T.MSFT")
    connection = harness.connection()
    harness.on_loop(connection, lambda: None)
    [row] = streams_of(harness, dh_to_pylist)
    assert (row["SymbolCount"], row["Symbols"]) == (2, "AAPL, MSFT")
    del result


def test_contracts_a_wildcard_covers_still_count_against_the_cap(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The wildcard may go at any time: a static contract the verb accepts is never deferred.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    every = harness.query(OPTIONS, dm.ALL).blink()
    harness.subscribed("T.*")
    harness.wait(lambda h: any(s.kind is StatusKind.ACK for s in h.statuses))
    first = harness.query(OPTIONS, ["O:SPY251219C00600000", "O:SPY251219C00610000"]).blink()
    connection = harness.connection("wss://socket.test/options")
    harness.on_loop(connection, lambda: None)
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        harness.query(OPTIONS, ["O:SPY251219C00620000"]).blink()
    assert connection.cap_pending == {}  # the opened stream counts through the admitted set
    harness.session.release(first)
    harness.session.release(every)


def test_static_contracts_take_the_room_before_a_watchlists(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    static = harness.query(OPTIONS, ["O:SPY251219C00690000"]).blink()  # sorts last
    harness.subscribed("T.O:SPY251219C00690000")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    reactive = harness.query(OPTIONS, watchlist).blink()
    harness.subscribed("T.O:SPY251219C00600000")
    connection = harness.connection("wss://socket.test/options")
    harness.on_loop(connection, lambda: None)
    rows = streams_of(harness, dh_to_pylist)
    assert [(r["StreamId"], r["Deferred"]) for r in rows] == [(1, 0), (2, 1)]
    # The static stream's contract is known as static, so it keeps its place before the cap.
    static_topics = harness.on_loop(connection, lambda: dict(connection.subscriptions._static))
    assert static_topics == {"T.O:SPY251219C00690000": 1}
    del static, reactive


def test_a_move_awaits_its_streams_until_the_hand_over(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        harness.connections.group.reconnect_backoff = 0.01
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.subscribed("T.ESZ5")
        old = harness.connection("wss://socket.test/futures")
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            held.set()
            assert release.wait(TIMEOUT)

        assert old.post(hold)  # the old loop is busy: the hand-over waits behind it
        assert held.wait(TIMEOUT)
        decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
        harness.connections.group._move(old, decision)
        successor = harness.connection("wss://delayed.test/futures")
        with harness.connections.group.lock:
            assert successor.awaiting == {1}
        release.set()
        wait_for(lambda: not successor.awaiting)  # adopted
        harness.subscribed("T.ESZ5")
        harness.session.release(result)
    finally:
        harness.close()


def test_a_failed_connection_is_not_moved(fake_ws_server: FakeMassiveServer) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        old = harness.connection("wss://socket.test/futures")
        with harness.connections.group.lock:
            old.failure = dm.MassiveError("rejected the API key")
        decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
        harness.connections.group._move(old, decision)
        assert old.join(TIMEOUT)  # closed, not handed over
        assert "wss://delayed.test/futures" not in harness.connections.group.connections
        del result
    finally:
        harness.close()


def test_a_move_whose_old_loop_ended_releases_the_successor(
    fake_ws_server: FakeMassiveServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = Harness(fake_ws_server, decide=unknown_futures)
    try:
        harness.connections.group.reconnect_backoff = 0.01
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.subscribed("T.ESZ5")
        old = harness.connection("wss://socket.test/futures")
        old.close()
        assert old.join(TIMEOUT)  # its loop ended: the hand-over cannot be posted
        # The move started before the close: it still sees the connection open.
        real = cast("Callable[[ws.Connection], bool]", vars(ws.Connection)["closing"].fget)
        monkeypatch.setattr(ws.Connection, "closing", property(lambda c: c is not old and real(c)))
        decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
        harness.connections.group._move(old, decision)
        successor = harness.connection("wss://delayed.test/futures")
        harness.on_loop(successor, lambda: None)
        with harness.connections.group.lock:
            assert successor.awaiting == set()
        harness.on_loop(successor, lambda: successor.release_stream(1))  # dropped, not kept
        assert harness.on_loop(successor, lambda: dict(successor._early)) == {}
        # A call kept for an awaited stream is forgotten once the stream is no longer awaited.
        with harness.connections.group.lock:
            successor.awaiting.add(99)
        harness.on_loop(successor, lambda: successor.release_stream(99))
        assert harness.on_loop(successor, lambda: sorted(successor._early)) == [99]
        with harness.connections.group.lock:
            successor.awaiting.discard(99)
        harness.on_loop(successor, successor.check_idle)
        assert harness.on_loop(successor, lambda: dict(successor._early)) == {}
        del result
    finally:
        harness.close()


def test_a_hand_over_applies_what_the_old_flusher_queued(
    harness: Harness, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], *, rows_when: RowsWhen
) -> None:
    refused = harness.query(TRADES, ["AAPL"]).blink()
    published = harness.query(TRADES, ["MSFT"]).append_only()
    harness.subscribed("T.AAPL", "T.MSFT")
    old = harness.connection()
    successor = spare_connection(harness, "next.test")
    successor.start()

    def queue_then_hand_over() -> None:
        old.flusher.fail([1], dm.EntitlementError("refused T.AAPL"))
        old.flusher.rows("T", [{"Ticker": "MSFT", "SipTimestamp": 1_700_000_000_000}])
        old.hand_over(successor)

    harness.on_loop(old, queue_then_hand_over)
    assert old.join(TIMEOUT)  # its last flush applied the failure and published the row
    rows_when(published, lambda rows: len(rows) == 1)
    states = [r["State"] for r in streams_of(harness, dh_to_pylist)]
    assert states == ["FAILED", "RUNNING"]
    successor.close()
    assert successor.join(TIMEOUT)
    del refused


def test_a_topic_reconnects_once_per_cluster(harness: Harness) -> None:
    assert harness.connections.group.first_no_ack("/stocks", "T.AAPL")
    assert not harness.connections.group.first_no_ack("/stocks", "T.AAPL")
    assert harness.connections.group.first_no_ack("/options", "T.AAPL")


# --- Sessions sharing connections, and how long a stream runs ----------------------------------


def jvm_collect() -> None:
    # A full collection on both sides: Python's cycles, then the JVM's heap.
    gc.collect()
    for _ in range(3):
        _JAVA_SYSTEM.gc()


def rows_of(session: dm.MassiveSession, to_pylist: ToPylist) -> list[dict[str, Any]]:
    assert session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(to_pylist(session.streams()), key=lambda r: r["StreamId"])


def test_sessions_with_one_key_share_one_connection(
    harness: Harness,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    *,
    rows_when: RowsWhen,
) -> None:
    other = harness.join()
    mine = harness.query(TRADES, ["AAPL"]).append_only()
    theirs = harness.query(TRADES, ["AAPL"], other).append_only()
    harness.subscribed("T.AAPL")
    assert harness.urls == ["wss://socket.test/stocks"]  # one connection for both sessions
    connection = harness.connection()
    with harness.connections.group.lock:
        assert connection.leases == 2
        assert set(connection.users.values()) == {1}
    harness.send(trade("AAPL"))
    harness.flush()
    rows_when(mine, lambda rows: len(rows) == 1)
    rows_when(theirs, lambda rows: len(rows) == 1)
    # Diagnostics stay per session: each shows its own stream and the shared connection.
    assert [r["StreamId"] for r in rows_of(harness.session, dh_to_pylist)] == [1]
    assert [r["StreamId"] for r in rows_of(other, dh_to_pylist)] == [2]
    for session in (harness.session, other):
        assert session._core.diagnostics.barrier().wait(TIMEOUT)
        (row,) = [r for r in dh_to_pylist(session.connections()) if r["Transport"] == "WS"]
        assert row["Host"] == "socket.test"
    # Closing one session fails its tables and leaves the connection to the other.
    other.close()
    wait_for(lambda: bool(theirs.j_table.isFailed()))
    assert not connection.closing
    with harness.connections.group.lock:
        assert connection.leases == 1
    harness.send(trade("AAPL", 2.0))
    harness.flush()
    rows_when(mine, lambda rows: len(rows) == 2)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    harness.session.release(mine)  # the last stream on the topic: now it goes
    harness.unsubscribed("T.AAPL")


def test_closing_the_last_session_on_a_connection_closes_it(harness: Harness) -> None:
    other = harness.join()
    stocks = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    forex = harness.query(FOREX, ["C:EURUSD"], other).blink()
    harness.subscribed("C.EUR-USD")
    stocks_connection = harness.connection()
    forex_connection = harness.connection("wss://socket.test/forex")
    other.close()  # it never used the stocks connection, and alone used the forex one
    assert forex_connection.join(TIMEOUT)
    assert not stocks_connection.closing
    with harness.connections.group.lock:
        assert "wss://socket.test/forex" not in harness.connections.group.connections
    harness.session.release(stocks)
    del forex


def test_only_sessions_with_equal_settings_share_connections() -> None:
    def group_of(session: dm.MassiveSession) -> ws.ConnectionGroup:
        transport = cast("ws.WsTransport", session._core.stream_transport())
        return transport._connections.group

    first = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    second = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    apart = dm.MassiveSession(dm.MassiveConfig(api_key="group-key", reconnect_backoff=45.0))
    other_key = dm.MassiveSession(dm.MassiveConfig(api_key="another-key"))
    try:
        shared = group_of(first)
        assert group_of(second) is shared
        assert group_of(apart) is not shared  # never another session's settings
        assert group_of(other_key) is not shared
        assert shared.reconnect_backoff == 30.0
        assert group_of(apart).reconnect_backoff == 45.0
        first.close()
        assert ws._groups.get(shared.key) is shared  # the group stays with its last session
    finally:
        for session in (first, second, apart, other_key):
            session.close()
    assert shared.key not in ws._groups


def test_a_stream_waiting_for_a_slot_says_so(
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    # Another connection with the key (another process, here the fake's reply) holds the slot.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    harness = Harness(fake_ws_server, backoff=1.0)
    try:
        watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
        contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
        watchlist.add(new_table([string_col("Symbol", contracts)]))
        capped = harness.query(OPTIONS, watchlist).blink()
        harness.subscribed("T.O:SPY251219C00600000")
        cap = "over capacity: 2/1; 1 deferred"
        wait_for(lambda: streams_of(harness, dh_to_pylist)[0]["Error"] == cap)
        fake_ws_server.auth_replies.append("max_connections")
        fake_ws_server.close()  # the socket drops; the reconnect finds the slot taken
        harness.wait(lambda h: WsState.WAITING_SLOT in h.states)
        stall = (
            "waiting for a WebSocket slot on socket.test/options: another connection with this "
            "API key, perhaps from another process, holds it; the plan allows one connection "
            "per cluster"
        )
        wait_for(lambda: streams_of(harness, dh_to_pylist)[0]["Error"] == stall)
        every = harness.query(OPTION_QUOTES, dm.ALL).blink()  # opens during the wait
        wait_for(lambda: [r["Error"] for r in streams_of(harness, dh_to_pylist)] == [stall] * 2)
        harness.wait(lambda h: h.opens == 2)  # the slot is free again
        wait_for(lambda: [r["Error"] for r in streams_of(harness, dh_to_pylist)] == [cap, None])
        del capped, every
    finally:
        harness.close()


def test_an_inline_sessions_derived_table_keeps_running(
    fake_ws_server: FakeMassiveServer, dh_exec_ctx: object, rows_when: RowsWhen
) -> None:
    # `dm.MassiveSession().stocks.trades("AAPL").append_only().where(...)`: the session and the
    # library's result are dropped at once, but the derived table uses the stream, so the
    # stream, its connection, and its session keep running through every collection.
    harness = Harness(fake_ws_server)
    core = weakref.ref(harness.session._core)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [harness.query(TRADES, ["AAPL"]).append_only().where("Price > 0")]
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    del harness.session, harness.connections
    jvm_collect()
    assert core() is not None
    harness.send(trade("AAPL"))
    connection.flusher.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    # Releasing the derived table stops the stream, frees the session, and closes the
    # connection that nothing else uses.
    derived.clear()
    scope.release()
    assert connection.join(TIMEOUT)
    wait_for(lambda: core() is None)
    assert WsState.CLOSED in harness.states


def test_a_watchlist_edit_after_a_jvm_collection_still_resubscribes(harness: Harness) -> None:
    # Deephaven holds a table's listeners only weakly: the stream must hold its own.
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = harness.query(TRADES, watchlist).append_only()
    harness.subscribed("T.AAPL")
    jvm_collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    harness.subscribed("T.MSFT")
    harness.session.release(result)


def test_a_failed_watchlist_fails_its_stream(harness: Harness, dh_to_pylist: ToPylist) -> None:
    scope = new_scope()
    with scope.open():
        watchlist, publisher = table_publisher("watchlist", {"Symbol": dtypes.string})
    result = harness.query(TRADES, watchlist).append_only()
    publisher.publish_failure(RuntimeError("the watchlist broke"))
    wait_for(lambda: streams_of(harness, dh_to_pylist)[0]["State"] == "FAILED")
    (row,) = streams_of(harness, dh_to_pylist)
    assert row["Error"].startswith("test.trades stream 1: its symbol table failed")
    wait_for(lambda: bool(result.j_table.isFailed()))
    scope.release()


def test_stress_two_sessions_open_and_release_while_one_closes(harness: Harness) -> None:
    other = harness.join()
    symbols = ["AAPL", "MSFT", "IBM", "GOOG"]
    started = threading.Barrier(17)
    closed_errors: list[str] = []

    def work(index: int) -> None:
        session = harness.session if index % 2 else other
        started.wait(TIMEOUT)
        for round_ in range(20):
            try:
                result = harness.query(TRADES, [symbols[(index + round_) % 4]], session).blink()
            except dm.MassiveError as error:
                assert session is other
                closed_errors.append(str(error))
                return
            session.release(result)

    with ThreadPoolExecutor(16) as pool:
        futures = [pool.submit(work, index) for index in range(16)]
        started.wait(TIMEOUT)
        other.close()
        for future in futures:
            future.result(TIMEOUT)
    # A verb that races the close finds the session closed, or its stream failed by the close.
    name = other._core.label
    assert all(f"{name} is closed" in t or f"{name} was closed" in t for t in closed_errors)
    core = harness.session._core
    drained = threading.Event()
    core.defer(drained.set)
    assert drained.wait(TIMEOUT)
    wait_for(lambda: not harness.connections.group.streams)
    with harness.connections.group.lock:
        assert all(c.leases == 0 for c in harness.connections.group.connections.values())


def test_closing_a_session_closes_a_retired_connection_it_alone_leases(harness: Harness) -> None:
    # A connection out of the registry (a fatal failure retired it) still carries the session's
    # stream until the stream shuts down; the session's close closes it too.
    result = harness.query(TRADES, ["AAPL"]).blink()
    harness.subscribed("T.AAPL")
    connection = harness.connection()
    assert harness.on_loop(
        connection, lambda: harness.connections.group.retire(connection, idle=False)
    )
    harness.close()
    assert connection.join(TIMEOUT)
    del result


def test_a_java_held_callback_of_a_freed_session_does_nothing() -> None:
    # The JVM may run a publisher's shutdown callback after the session's connections are gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    ws._weakly(cast("weakref.ref[ws.WsConnections]", gone), "_shut_down", 1)


def test_closing_a_session_drops_its_streams_from_an_ended_connection(harness: Harness) -> None:
    # Another session still leases the connection, but its loop has ended: the closing
    # session's stream is dropped directly, since no post can reach the loop any more.
    other = harness.join()
    mine = harness.query(TRADES, ["AAPL"]).blink()
    theirs = harness.query(TRADES, ["MSFT"], other).blink()
    harness.subscribed("T.AAPL", "T.MSFT")
    connection = harness.connection()
    connection.close()
    assert connection.join(TIMEOUT)
    other.close()
    assert 2 not in connection._streams
    assert 1 in connection._streams  # the other session's stream stays until it ends
    harness.session.release(mine)
    del theirs
