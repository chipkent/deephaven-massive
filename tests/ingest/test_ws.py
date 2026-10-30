"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end, on the
`ws_session` fixture (`tests/_fakes/ws_session.py`): a real session whose `WsConnections`
reaches the fake Massive server through real `WsClient`s, with every client callback recorded
so a test waits for exactly the event it needs. Flushes are explicit (`WsSession.flush`): the
flush interval is an hour unless a test needs the flush thread itself.
"""

from __future__ import annotations

import dataclasses
import gc
import importlib
import json
import logging
import re
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars, Recency
from deephaven_massive._ingest import futures_calendar, hosts, poll, ws
from deephaven_massive._massive import Cancel, Status, StatusKind, Transport, WsState
from deephaven_massive._tables.diagnostics import ConnectionState, Diagnostics, StreamUpdate
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.stream import open_stream_tables
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive._tables.watchlist import SymbolWatch

# Imported, not taken from a fixture: no shared fixture provides the manual clock that
# `poll.open_poll` needs for the thread-name test's poller, which must not cycle on its own.
from tests._fakes.clock import FakeClock

# Plain constants of the ws_session, which no fixture can hand over; sessions come from fixtures.
from tests._fakes.ws_session import REALTIME, TIMEOUT

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    import pyarrow as pa
    from deephaven.table import Table

    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
ToPylist: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
W = Transport.WS
_JAVA_SYSTEM = cast("Any", importlib.import_module("jpy")).get_type("java.lang.System")


# --- The datasets the end-to-end tests stream -------------------------------------------------

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))
TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
FOREX_FMV = DatasetSpec(
    name="test.forex_fmv",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(W, "forex/FMV"): SourceKeys(KeySpec("Ticker"))},
)
CRYPTO = DatasetSpec(
    name="test.crypto",
    summary="A test dataset.",
    asset_class=AssetClass.CRYPTO,
    channels={NotBars.ANY: "XT"},
    keys={(W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("ParticipantTimestamp"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={NotBars.ANY: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
OPTION_QUOTES = DatasetSpec(
    name="test.option_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={NotBars.ANY: "Q"},
    keys={(W, "options/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={NotBars.ANY: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

QUOTES_FUTURES = DatasetSpec(
    name="test.futures_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={NotBars.ANY: "Q"},
    keys={(W, "futures/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)


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


def settle(ws_session: WsSession) -> None:
    # Run what each live connection's loop has queued: a status's row write runs at the end of
    # its loop turn, after the status callback a test waits on, so a diagnostics read settles
    # first to see it. A connection whose loop has ended has nothing queued and is skipped.
    with ws_session.connections.group.lock:
        connections = list(ws_session.connections.group.connections.values())
    for connection in connections:
        done = threading.Event()
        if connection.post(done.set):
            assert done.wait(TIMEOUT)


def streams_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    settle(ws_session)
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(dh_to_pylist(ws_session.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    settle(ws_session)
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    rows = dh_to_pylist(ws_session.session.connections())
    return [r for r in rows if r["Transport"] == "WebSocket"]


# --- Connections and streams, end to end ------------------------------------------------------


def test_the_channel_keys_are_declared_before_its_first_subscribe(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    required = ("set_required", "T", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    assert ws_session.journal[:2] == [required, ("subscribe", ("T.AAPL",))]
    ws_session.session.release(result)


def test_a_stream_publishes_only_its_symbols(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.AAPL", "T.*")
    # A status beside the data is not a data item, so `send` does not wait for it.
    status: dict[str, object] = {"ev": "status", "status": "info", "message": "a notice"}
    ws_session.send(trade("AAPL", 1.5), status, trade("MSFT", 2.0))
    ws_session.flush()
    assert [r["Price"] for r in rows_when(aapl, lambda rows: len(rows) == 1)] == [1.5]
    every_rows = rows_when(every, lambda rows: len(rows) == 2)
    assert sorted(r["Symbol"] for r in every_rows) == ["AAPL", "MSFT"]
    rows = streams_of(ws_session, dh_to_pylist)
    assert [(r["Symbols"], r["RowsPublished"], r["Host"], r["Path"]) for r in rows] == [
        ("AAPL", 1, "socket.test", "/stocks"),
        ("ALL", 2, "socket.test", "/stocks"),
    ]


def test_equal_queries_share_one_stream_and_one_connection(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, "AAPL").last_by()
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL", "Q.AAPL")
    assert len(ws_session.urls) == 1
    assert ws_session.decisions == [(AssetClass.STOCKS, ChannelFamily.TICK)] * 2
    assert ws_session.connection().leases == 2
    for result in (first, second, quotes):
        ws_session.session.release(result)


def test_wire_symbols_become_canonical(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    result = ws_session.query(FOREX, ["C:EURUSD"]).append_only()
    ws_session.subscribed("C.EUR-USD")
    ws_session.send({"ev": "C", "p": "EUR/USD", "a": 1.1, "b": 1.0, "t": 1_700_000_000_000, "x": 1})
    ws_session.connection("wss://socket.test/forex").flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_an_fmv_frame_in_the_canonical_spelling_reaches_its_forex_stream(
    ws_session: WsSession, *, rows_when: RowsWhen
) -> None:
    # FMV frames name the pair `C:EURUSD`, while the topic spells it `EUR-USD`.
    result = ws_session.query(FOREX_FMV, ["C:EURUSD"]).append_only()
    ws_session.subscribed("FMV.EUR-USD")
    ws_session.send({"ev": "FMV", "sym": "C:EURUSD", "fmv": 1.1, "t": 1_700_000_000_000_000_000})
    ws_session.connection(ws_session.urls[-1]).flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_a_refused_topic_fails_only_its_streams(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    fake_ws_server.refuse["Q.AAPL"] = "not authorized to access Q.AAPL"
    trades = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(status.kind is StatusKind.REFUSED for status in s.statuses))
    ws_session.flush()
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "socket.test/stocks refused Q.AAPL: not authorized to access Q.AAPL" in rows[1]["Error"]
    assert "MassiveConfig.ws_hosts[AssetClass.STOCKS]" in rows[1]["Error"]
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(trades, lambda rows: len(rows) == 1)  # the connection kept running
    del quotes


def test_publish_waits_for_every_channel_of_a_frame(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, *, rows_when: RowsWhen
) -> None:
    # The client delivers a frame one channel at a time; the quote, delivered second and
    # slowly, must still reach the flusher before `publish` flushes, or with an hourly flush
    # interval its row would never be published.
    trades = ws_session.query(TRADES, ["AAPL"]).append_only()
    quotes = ws_session.query(QUOTES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL", "Q.AAPL")
    connection = ws_session.connection()
    deliver = connection._on_data
    pause = threading.Event()  # never set; only slows the second channel

    def slow_quotes(
        channel: str,
        rows: list[dict[str, object]],
        *,
        rejects: list[_massive.Reject],
        drift: _massive.DriftReport,
    ) -> None:
        if channel == "Q":
            pause.wait(0.3)
        deliver(channel, rows, rejects=rejects, drift=drift)

    monkeypatch.setattr(connection, "_on_data", slow_quotes)
    quote = {"ev": "Q", "sym": "AAPL", "bx": 4, "bp": 1.0, "bs": 100, "ax": 7, "ap": 1.1}
    ws_session.publish(trade("AAPL"), quote | {"as": 100, "t": 1_700_000_000_000, "z": 3})
    rows_when(trades, lambda rows: len(rows) == 1)
    rows_when(quotes, lambda rows: len(rows) == 1)


def test_a_rejected_row_fails_only_the_streams_of_its_symbol(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    every = ws_session.query(TRADES, dm.ALL).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT", "T.*")
    bad = trade("MSFT")
    del bad["t"]  # a required key
    ws_session.send(bad, trade("AAPL"))
    ws_session.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED", "FAILED"]
    assert "no SipTimestamp" in rows[1]["Error"]
    del msft, every


def test_a_row_without_a_symbol_fails_only_the_all_streams(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).blink()
    every = ws_session.query(TRADES, dm.ALL).blink()
    ws_session.subscribed("T.AAPL", "T.*")
    nameless = trade("AAPL")
    del nameless["sym"]
    unreadable = trade("not a symbol")
    del unreadable["t"]
    with caplog.at_level(logging.ERROR, logger=ws.__name__):
        ws_session.send(nameless, unreadable)
    ws_session.flush()
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert caplog.text.count("; failing the channel's ALL streams") == 2
    # Each line says why the row has no symbol: none at all, or the raw one that is not valid.
    assert "a T row has no valid symbol (it has none)" in caplog.text
    assert (
        "a T row has no valid symbol ('not a symbol' is not a spelling the library reads)"
        in caplog.text
    )
    del aapl, every


def test_a_timestamp_out_of_bounds_fails_only_its_streams(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    ws_session.send(trade("MSFT", t=5), trade("AAPL"))
    ws_session.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "timestamp unit mismatch for test.trades.Timestamp: 5 ms" in rows[1]["Error"]
    del msft


def test_drift_is_logged_once_and_shown(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    with caplog.at_level(logging.WARNING):
        ws_session.send(trade("AAPL", zz=1))
        ws_session.send(trade("AAPL", zz=2))
        ws_session.flush()
    assert caplog.text.count("field 'zz' is not in the schema") == 1
    [connection] = connections_of(ws_session, dh_to_pylist)
    assert connection["SchemaDrift"] == "T:zz+"
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert stream["SchemaDrift"] == "WebSocket:zz+"
    ws_session.session.release(result)


def test_releasing_the_last_result_unsubscribes(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, ["AAPL"]).ring(5)
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(first)
    ws_session.session.release(second)
    ws_session.unsubscribed("T.AAPL")
    assert ws_session.connection().leases == 0


def test_a_derived_table_keeps_the_stream_alive(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer, rows_when: RowsWhen
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [result.tail(100)]
    ws_session.session.release(result)
    del result
    gc.collect()
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    derived.clear()
    scope.release()
    ws_session.unsubscribed("T.AAPL")


def test_close_returns_promptly_during_a_backoff_wait(
    fake_ws_server: FakeMassiveServer, make_ws_session: Callable[..., WsSession]
) -> None:
    custom_session = make_ws_session(backoff=60.0)
    fake_ws_server.reject_next(503)
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: WsState.RECONNECTING in s.states)
    started = time.monotonic()
    custom_session.close()
    assert time.monotonic() - started < 5.0
    assert WsState.CLOSED in custom_session.states
    del result


def test_a_rejected_key_fails_every_stream(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    q = ws_session.query(TRADES, ["AAPL"])
    result = q.blink()
    ws_session.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    failed = ws_session.connection()
    fake_ws_server.auth_replies.append("auth_failed")  # for the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: WsState.FAILED in s.states)
    with ws_session.connections.group.lock:
        assert failed.retired
        assert ws_session.connections.group.connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "rejected the API key" in row["LastError"]
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert stream["State"] == "FAILED"
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(ws_session.urls) == 2
    del result, again


def test_a_fatal_handshake_names_the_status_and_host(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    fake_ws_server.reject_next(404)  # for the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: WsState.FAILED in s.states)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["LastError"].startswith("the WebSocket connection to socket.test/stocks failed:")
    assert "404" in row["LastError"]
    del result


def test_a_reconnect_resends_the_desired_set(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    ws_session.subscribed("T.AAPL")  # on the new connection
    ws_session.wait(lambda s: sum(status.kind is StatusKind.ACK for status in s.statuses) == 2)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Reconnects"], row["Topics"], row["AckedTopics"]) == (
        "CONNECTED",
        1,
        1,
        1,
    )
    assert WsState.RECONNECTING in ws_session.states
    ws_session.session.release(result)


def test_an_idle_connection_closes_and_a_new_stream_opens_another(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "IDLE_GRACE", 0.05)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(result)
    ws_session.wait(lambda s: WsState.CLOSED in s.states)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "CLOSED"
    again = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    assert len(ws_session.urls) == 2
    ws_session.session.release(again)


def test_an_opening_stream_cancels_the_idle_close(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(result)
    ws_session.unsubscribed("T.AAPL")
    with ws_session.connections.group.lock:
        connection.leases += 1  # a stream is opening
    retired = ws_session.on_loop(
        connection, lambda: ws_session.connections.group.retire(connection, idle=True)
    )
    assert retired is False
    with ws_session.connections.group.lock:
        connection.leases -= 1


def test_a_missing_acknowledgement_reconnects(
    fake_ws_server: FakeMassiveServer, make_ws_session: Callable[..., WsSession]
) -> None:
    custom_session = make_ws_session(ack_timeout=0.05)
    fake_ws_server.silent.add("T.AAPL")
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: s.opens >= 2)
    assert [json.loads(f)["params"] for f in fake_ws_server.frames(0)] == ["T.AAPL", "T.AAPL"]
    del result


def test_a_reactive_table_follows_its_rows(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("AAPL"), trade("MSFT"))
    ws_session.flush()
    rows_when(result, lambda rows: len(rows) == 1)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    watchlist.delete(new_table([string_col("Symbol", ["AAPL"])]))
    ws_session.unsubscribed("T.AAPL")
    ws_session.send(trade("AAPL", 2.0), trade("MSFT", 3.0))
    ws_session.flush()
    shown = rows_when(result, lambda rows: len(rows) == 2)
    assert [r["Symbol"] for r in shown] == ["AAPL", "MSFT"]
    sent = [json.loads(f)["params"] for f in fake_ws_server.frames()]
    assert sent == ["T.AAPL", "T.MSFT", "T.AAPL"]


def failed_after_flush(ws_session: WsSession, table: Table) -> bool:
    # Flush the connections, so a failure the flush thread was handed is applied.
    ws_session.flush()
    return bool(table.j_table.isFailed())


def test_a_late_watchlist_row_that_is_not_canonical_fails_the_stream(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    watchlist.add(new_table([string_col("Symbol", ["msft"])]))
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("a symbol table holds 'msft'")
    assert "dm.canonicalize_symbols(table, dm.AssetClass.STOCKS, " in row["Error"]
    # The row that is not canonical never became a topic.
    sent = {json.loads(f)["params"] for f in fake_ws_server.frames()}
    assert sent == {"T.AAPL"}


def test_a_frame_symbol_the_library_cannot_read_fails_the_all_streams(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.*")
    ws_session.send(trade("BRK/B"))
    wait_for(lambda: failed_after_flush(ws_session, every))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: Massive sent the symbol 'BRK/B', a spelling")


def test_an_unreadable_frame_symbol_fails_the_symbol_streams_too(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # The row cannot be routed, so no symbol stream may starve on it silently.
    one = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("BRK/B"))
    wait_for(lambda: failed_after_flush(ws_session, one))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: Massive sent the symbol 'BRK/B', a spelling")


def test_a_failing_upstream_watchlist_fails_the_stream(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # A canonicalize_symbols table under RAISE fails on a bad row; the stream must fail with
    # it rather than keep its old topics.
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["AAPL"])]))
    watchlist = dm.canonicalize_symbols(source, AssetClass.STOCKS)
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    source.add(new_table([string_col("Symbol", ["BRK-B"])]))
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("the symbol table failed, so the result can no longer follow")


def test_a_frame_symbol_the_library_cannot_read_is_kept_under_warn(
    make_ws_session: Callable[..., WsSession], *, rows_when: RowsWhen
) -> None:
    warned = make_ws_session(unknown_symbols=dm.UnknownSymbols.WARN)
    every = warned.query(TRADES, dm.ALL).append_only()
    warned.subscribed("T.*")
    warned.send(trade("BRK/B"), trade("AAPL"))
    warned.flush()
    shown = rows_when(every, lambda rows: len(rows) == 2)
    assert [(r["Symbol"], r["RawSymbol"]) for r in shown] == [(None, "BRK/B"), ("AAPL", None)]


def test_a_derived_table_keeps_following_the_watchlist(
    ws_session: WsSession, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    derived = result.tail(100)
    ws_session.session.release(result)
    del result
    gc.collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    ws_session.send(trade("MSFT"))
    ws_session.flush()
    rows_when(derived, lambda rows: len(rows) == 1)


def test_static_options_beyond_the_cap_raise(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000", "O:SPY251219C00620000"]
    with pytest.raises(ValueError, match="3 new option topics and 0 in use exceed the 2"):
        ws_session.query(OPTIONS, contracts).blink()
    first = ws_session.query(OPTIONS, contracts[:2]).blink()
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        ws_session.query(OPTIONS, contracts[2:]).blink()
    every = ws_session.query(OPTIONS, dm.ALL).blink()  # a wildcard counts zero
    ws_session.session.release(first)
    ws_session.session.release(every)


def test_the_cap_message_counts_topics_not_contracts(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Quotes of the contracts already traded are new topics, though no contract is new.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 3)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    trades = ws_session.query(OPTIONS, contracts).blink()
    with pytest.raises(
        ValueError,
        match=r"^test.option_quotes: 2 new option topics and 2 in use exceed the 3 topics one "
        r"/options connection carries; a topic is one contract on one channel",
    ):
        ws_session.query(OPTION_QUOTES, contracts).blink()
    ws_session.session.release(trades)


def test_reactive_options_beyond_the_cap_are_deferred(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00610000", "O:SPY251219C00600000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    result = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Deferred"], row["Error"]) == (
        "RUNNING",
        1,
        "over capacity: 2/1; 1 deferred",
    )
    watchlist.delete(new_table([string_col("Symbol", ["O:SPY251219C00600000"])]))
    ws_session.subscribed("T.O:SPY251219C00610000")
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["Deferred"], row["Error"]) == (0, None)
    del result


def unknown_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.UNKNOWN, "no timeframe in futures snapshot")


def test_the_real_time_verdict_moves_the_connection_once(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: s.opens == 2)
    assert custom_session.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
    custom_session.subscribed("T.ESZ5")
    [row] = connections_of(custom_session, dh_to_pylist)
    assert (row["Host"], row["Recency"]) == ("delayed.test", "DELAYED")
    assert row["Evidence"] == "server verdict: Real-time data requires an upgrade"
    [stream] = streams_of(custom_session, dh_to_pylist)
    assert stream["State"] == "RUNNING"
    assert custom_session.verdicts == [
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
    other = custom_session.query(FUTURES, ["NQZ5"]).blink()  # the recorded verdict decides
    assert custom_session.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
    custom_session.session.release(result)
    custom_session.session.release(other)


def realtime_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.REALTIME, "timeframe=REAL-TIME")


@pytest.mark.parametrize(
    ("decide", "pinned"),
    [(unknown_futures, {AssetClass.FUTURES: "socket.test"}), (realtime_futures, {})],
)
def test_a_verdict_otherwise_fails_the_step(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    decide: Callable[[AssetClass, ChannelFamily], HostDecision],
    pinned: dict[AssetClass, str],
) -> None:
    custom_session = make_ws_session(decide=decide, ws_hosts=pinned)
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: any(status.kind is StatusKind.REFUSED for status in s.statuses))
    custom_session.connection("wss://socket.test/futures").flusher.flush()
    [stream] = streams_of(custom_session, dh_to_pylist)
    assert stream["State"] == "FAILED"
    assert len(custom_session.urls) == 1
    del result


def test_a_failed_open_returns_its_lease(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no tables")

    monkeypatch.setattr(ws, "open_stream_tables", broken)
    with pytest.raises(RuntimeError, match="no tables"):
        ws_session.query(TRADES, ["AAPL"]).blink()
    assert ws_session.connection().leases == 0
    assert ws_session.connections.group.streams == {}


def test_a_watch_that_cannot_start_releases_the_stream(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: object) -> None:
        raise RuntimeError("no watch")

    monkeypatch.setattr(SymbolWatch, "start", broken)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        ws_session.query(TRADES, watchlist).blink()
    assert ws_session.connection().leases == 0


def test_a_lost_race_closes_the_spare_connection(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2)
    built: list[ws.Connection] = []
    built_lock = threading.Lock()
    real: Callable[..., ws.Connection] = ws.Connection

    def racing(*args: object, **kwargs: object) -> ws.Connection:
        connection = real(*args, **kwargs)
        with built_lock:
            built.append(connection)
        barrier.wait(TIMEOUT)
        return connection

    monkeypatch.setattr(ws, "Connection", racing)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda s: ws_session.query(TRADES, [s]).blink(), ["AAPL", "MSFT"]))
    assert len(built) == 2
    assert len(ws_session.connections.group.connections) == 1
    spare = next(c for c in built if c not in ws_session.connections.group.connections.values())
    assert spare.join(TIMEOUT)
    for result in results:
        ws_session.session.release(result)


def test_a_closed_transport_opens_nothing(ws_session: WsSession) -> None:
    request = ws_session.query(TRADES, ["AAPL"])._request
    ws_session.connections.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        ws_session.connections.open_stream(
            ws_session.session._core, TRADES, request, channel="T", stream_id=99
        )


def test_a_shutdown_after_the_connection_ended_is_logged(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    connection.close()
    assert connection.join(TIMEOUT)
    with caplog.at_level(logging.INFO, logger=ws.__name__):
        ws_session.session.release(result)
        wait_for(lambda: "has closed; dropped release_stream" in caplog.text)


def test_a_failing_shutdown_is_logged(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(stream_id: int) -> None:
        raise RuntimeError(f"registry {stream_id}")

    monkeypatch.setattr(ws_session.session._core, "stream_shut_down", broken)
    ws_session.connections._shut_down(42)
    assert "the shutdown of stream 42 failed" in caplog.text


def test_the_flush_thread_publishes_by_itself(
    make_ws_session: Callable[..., WsSession], rows_when: RowsWhen
) -> None:
    custom_session = make_ws_session(flush_interval=0.01)
    result = custom_session.query(TRADES, ["AAPL"]).append_only()
    custom_session.subscribed("T.AAPL")
    custom_session.send(trade("AAPL"))
    rows_when(result, lambda rows: len(rows) == 1)


def spare_connection(ws_session: WsSession, host: str) -> ws.Connection:
    return ws.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        sessions=None,
    )


def test_a_delayed_start_waits_and_close_cancels_it(ws_session: WsSession) -> None:
    cancelled = spare_connection(ws_session, "never.test")
    cancelled.start(delay=60.0)
    cancelled.start()  # a second start does nothing
    cancelled.close()
    cancelled.start()  # nor does a start after close
    assert cancelled.join(TIMEOUT)
    assert ws_session.opens == 0
    late = spare_connection(ws_session, "late.test")
    late.start(delay=0.01)
    ws_session.wait(lambda s: s.opens == 1)
    late.close()
    assert late.join(TIMEOUT)


def test_changes_for_a_moved_stream_follow_it(ws_session: WsSession) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    result = ws_session.query(TRADES, watchlist).blink()
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()
    with ws_session.connections.group.lock:
        ws_session.connections.group.streams[1] = successor  # moved, as the verdict moves it
        successor.take(ws_session.connections)
        old.give(ws_session.connections)
        successor.awaiting.add(1)
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    # The old loop has ended, so its methods run here, as a late callback would have there.
    old.change_symbols(1, frozenset({"AAPL"}), frozenset())
    ws_session.subscribed("T.AAPL")  # on the successor
    old.release_stream(1)
    ws_session.unsubscribed("T.AAPL")
    successor.close()
    del result


def test_a_release_that_overtakes_the_adoption_waits_for_it(ws_session: WsSession) -> None:
    # The shutdown callback read the moved lease and posted before the old loop's hand-over.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()
    with ws_session.connections.group.lock:
        ws_session.connections.group.streams[1] = successor  # moved, as the verdict moves it
        successor.take(ws_session.connections)
        old.give(ws_session.connections)
        successor.awaiting.add(1)
    successor.post(successor.release_stream, 1)
    successor.post(successor.change_symbols, 2, frozenset(), frozenset())  # not awaited: dropped
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    ws_session.on_loop(successor, lambda: None)  # the adoption ran
    assert ws_session.on_loop(successor, lambda: successor.subscriptions.wanted) is False
    assert ws_session.on_loop(successor, lambda: dict(successor._streams)) == {}
    successor.close()
    assert successor.join(TIMEOUT)
    del result


def test_a_hand_over_to_a_failed_successor_fails_the_streams(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    with ws_session.connections.group.lock:
        successor.failure = dm.MassiveError("rejected the API key")
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)  # its last flush failed the stream
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("FAILED", "rejected the API key")
    successor.close()
    assert successor.join(TIMEOUT)
    del result


def test_stress_concurrent_verbs_and_releases(ws_session: WsSession) -> None:
    # 16 threads open and release overlapping streams at once; every topic is released.
    symbols = [f"S{i}" for i in range(8)]

    def churn(worker: int) -> None:
        for round_ in range(10):
            symbol = symbols[(worker + round_) % len(symbols)]
            result = ws_session.query(TRADES, [symbol]).blink()
            ws_session.session.release(result)

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(churn, range(16)))
    core = ws_session.session._core
    wait_for(lambda: not ws_session.connections.group.streams)
    connection = ws_session.connection()
    assert ws_session.on_loop(connection, lambda: connection.subscriptions.wanted) is False
    assert connection.leases == 0
    wait_for(lambda: core._streams == {})


# --- Edge cases of the connection's callbacks -------------------------------------------------


def test_late_callbacks_of_an_unknown_stream_are_ignored(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.on_loop(connection, lambda: connection.release_stream(999))
    ws_session.on_loop(connection, lambda: connection.change_symbols(999, frozenset(), frozenset()))
    ws_session.connections._symbols_changed(999, frozenset({"X"}), frozenset())
    ws_session.on_loop(connection, connection._idle_close)  # a stream is open: it stays
    stale = Status(kind=StatusKind.REFUSED, topics=("T.AAPL",), message="not authorized")
    ws_session.on_loop(connection, lambda: connection._on_status(999, stale))  # an older socket's
    assert ws_session.connection() is connection
    ws_session.flush()
    [row] = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(result)


def test_a_rejected_row_nobody_receives_fails_nothing(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    bad = trade("MSFT")
    del bad["t"]
    ws_session.send(bad)
    ws_session.flush()
    [row] = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(result)


def test_last_message_rows_are_throttled(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(t.message == "subscribed to: T.AAPL" for t in s.statuses))
    connection = ws_session.connection()
    ws_session.on_loop(connection, lambda: None)  # the acknowledgement's row write runs first
    # An interval no test outlasts, and a last write one interval before the epoch: the first
    # message is due a rewrite and the second, however late it arrives, is not.
    interval = 10**18
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", interval)
    monkeypatch.setattr(ws, "_BACKLOG_ROW_DELAY", 10**9)  # the buffered rows' rewrite never fires
    written: list[int] = []

    def write() -> None:
        written.append(1)
        connection._message_row_at = time.time_ns()  # what the real writer records

    def reset() -> None:
        connection._message_row_at = -interval

    monkeypatch.setattr(connection, "_write_row", write)
    ws_session.on_loop(connection, reset)
    ws_session.send(trade("AAPL"))
    ws_session.send(trade("AAPL"))
    assert written == [1]
    ws_session.session.release(result)


def test_rows_buffered_while_row_writes_are_throttled_still_show(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The message right after a row write does not rewrite the row, so the rows it buffers
    # must still schedule the backlog rewrite: with no further message, nothing else would.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(t.message == "subscribed to: T.AAPL" for t in s.statuses))
    connection = ws_session.connection()
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 10**18)  # every message row write is throttled

    def quiet() -> bool:
        if connection._backlog_timer is not None:
            connection._backlog_timer.cancel()
            connection._backlog_timer = None
        return connection.flusher.backlog == 0

    assert ws_session.on_loop(connection, quiet)  # nothing buffered, no rewrite scheduled
    ws_session.send(trade("AAPL"))
    assert ws_session.on_loop(connection, lambda: connection._backlog_timer is not None)
    ws_session.flush()
    ws_session.session.release(result)


def test_one_acknowledgement_frame_rewrites_the_row_once(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Massive answers a subscribe frame with one status per topic, all in one frame. Each
    # status changes the counts, but the row reads every topic, so it is rewritten once for
    # the frame, not once per status: per status, a large subscription costs O(topics²) and
    # stalls the loop for seconds.
    written: list[int] = []
    write = ws.Connection._write_row

    def counting(connection: ws.Connection) -> None:
        written.append(1)
        write(connection)

    monkeypatch.setattr(ws.Connection, "_write_row", counting)
    symbols = [f"S{i:04}" for i in range(1000)]
    result = ws_session.query(TRADES, symbols).blink()
    ws_session.wait(
        lambda s: sum(t.message.startswith("subscribed to:") for t in s.statuses) == 1000
    )
    connection = ws_session.connection()
    counts = ws_session.on_loop(connection, connection.subscriptions.counts)  # after the publish
    assert counts == (1000, 1000, 0, 0)
    assert len(written) <= 4  # the connecting, connected, and subscribing rows, then the frame
    ws_session.session.release(result)


def test_an_index_value_refusal_names_the_business_host(ws_session: WsSession) -> None:
    connection = ws.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url="wss://socket.test/indices",
        asset_class=AssetClass.INDICES,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        sessions=None,
    )
    connection._refused(("V.I:SPX",), "not authorized")
    assert connection._last_error is not None
    assert "index values may need business.massive.com" in connection._last_error
    assert "ws_hosts[AssetClass.INDICES]" in connection._last_error
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_connection_skips_a_session_that_is_gone(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A freed session stays one of the connection's users until its closer runs on the reaper;
    # meanwhile the connection writes it no row.
    result = ws_session.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    monkeypatch.setattr(ws_session.connections, "_core", lambda: None)
    written: list[object] = []
    monkeypatch.setattr(Diagnostics, "write", lambda _, row: written.append(row))
    ws_session.on_loop(connection, connection._write_row)
    drift = _massive.DriftReport(extra=frozenset({"zz"}))
    ws_session.on_loop(connection, lambda: connection._record_drift("T", drift))
    assert written == []
    monkeypatch.undo()
    assert ws_session.on_loop(
        connection, lambda: ws_session.connections.group.retire(connection, idle=False)
    )
    assert ws_session.connections.group.reroute(connection, "real-time") is False  # retired
    connection.close()  # retired: the session's close no longer reaches it
    assert connection.join(TIMEOUT)
    del result


def test_retiring_a_replaced_connection_keeps_its_replacement(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    spare = spare_connection(ws_session, "spare.test")
    assert ws_session.connections.group.retire(spare, idle=False)
    assert ws_session.connection() is connection
    spare.close()
    assert spare.join(TIMEOUT)
    ws_session.session.release(result)


def test_a_verdict_settles_the_host_for_every_session_of_the_group(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # The verdict is the key's: a session that never leased the moved connection opens its
    # first stream of the class on the delayed host, without asking the real-time one again.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    other = join(custom_session)
    mine = custom_session.query(FUTURES, ["ESZ5"]).blink()
    old = custom_session.connection("wss://socket.test/futures")
    custom_session.connections.group._move(old, DELAYED)
    assert len(custom_session.verdicts) == 1
    decided = len(custom_session.decisions)
    theirs = custom_session.query(FUTURES, ["NQZ5"], other).blink()
    assert len(custom_session.decisions) == decided  # no decision asked for: the verdict's
    with custom_session.connections.group.lock:
        assert {c.url for c in custom_session.connections.group.streams.values()} == {
            "wss://delayed.test/futures"
        }
    assert len(custom_session.verdicts) == 2  # recorded with the other session, once
    again = custom_session.query(FUTURES, ["CLZ5"], other).blink()
    assert len(custom_session.verdicts) == 2
    custom_session.session.release(mine)
    other.close()
    del theirs, again


def test_a_verdict_that_arrives_while_a_host_is_decided_wins(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The verdict lands after the session decided the real-time host but before it leased a
    # connection: the lease reads the group's verdicts again and takes the delayed host. A
    # verdict whose recording fails is recorded again by the next stream.
    groups: list[ws.ConnectionGroup] = []

    def decide_as_the_verdict_lands(*_: object) -> HostDecision:
        groups[0].verdicts[AssetClass.FUTURES, ChannelFamily.TICK] = DELAYED
        return unknown_futures()

    custom_session = make_ws_session(decide=decide_as_the_verdict_lands)
    groups.append(custom_session.connections.group)
    record = custom_session.connections.record_verdict
    failures = [RuntimeError("the REST transport is closing")]

    def flaky(asset_class: AssetClass, family: ChannelFamily, decision: HostDecision) -> None:
        if failures:
            raise failures.pop()
        record(asset_class, family, decision)

    monkeypatch.setattr(custom_session.connections, "record_verdict", flaky)
    with pytest.raises(RuntimeError, match="closing"):
        custom_session.query(FUTURES, ["ESZ5"]).blink()
    assert custom_session.connections.verdicts_recorded == set()  # not recorded: retried
    with groups[0].lock:
        assert groups[0].streams == {}  # the failed open returned its lease
    first = custom_session.query(FUTURES, ["ESZ5"]).blink()
    assert custom_session.urls == ["wss://delayed.test/futures"]
    assert custom_session.verdicts == [(AssetClass.FUTURES, ChannelFamily.TICK, DELAYED)]
    monkeypatch.undo()
    custom_session.session.release(first)
    groups.clear()


def test_a_connection_built_for_the_host_a_verdict_replaced_is_dropped(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The verdict lands while the real-time connection is being built: the lease drops it
    # unused and builds the delayed host's instead.
    custom_session = make_ws_session(decide=unknown_futures)
    group = custom_session.connections.group
    build = group.factory

    def verdict_while_building(url: str, **options: object) -> object:
        if url == "wss://socket.test/futures":
            group.verdicts[AssetClass.FUTURES, ChannelFamily.TICK] = DELAYED
        return cast("Callable[..., object]", build)(url, **options)

    monkeypatch.setattr(group, "factory", verdict_while_building)
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    assert list(group.connections) == ["wss://delayed.test/futures"]
    monkeypatch.undo()
    custom_session.session.release(result)


def test_a_session_that_joins_during_a_move_records_the_verdict(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # Another session leases the connection while the verdict is being recorded: its lease
    # moves with the others, so it records the verdict too, and its next stream of the class
    # goes to the delayed host instead of being refused again.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    other = join(custom_session)
    mine = custom_session.query(FUTURES, ["ESZ5"]).blink()
    old = custom_session.connection("wss://socket.test/futures")
    record = custom_session.connections.record_verdict
    joined: list[Table] = []

    def join_while_recording(
        asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        joined.append(custom_session.query(FUTURES, ["NQZ5"], other).blink())
        record(asset_class, family, decision)

    custom_session.connections.record_verdict = join_while_recording
    custom_session.connections.group._move(old, DELAYED)
    custom_session.connections.record_verdict = record  # the closure would hold the sessions
    assert len(custom_session.verdicts) == 2  # the joining session's too
    with custom_session.connections.group.lock:
        assert {c.url for c in custom_session.connections.group.streams.values()} == {
            "wss://delayed.test/futures"
        }
    custom_session.session.release(mine)
    other.close()
    joined.clear()


def test_a_closed_sessions_view_shows_its_connection_closed(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # The session's close closes the connection only it used; the connection's last row,
    # `CLOSED`, still reaches the session's diagnostics before they are released.
    view = ws_session.session.connections()
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.close()
    (row,) = [r for r in dh_to_pylist(view) if r["Transport"] == "WebSocket"]
    assert row["State"] == "CLOSED"
    del result, view


def test_a_verdict_is_taken_once(make_ws_session: Callable[..., WsSession]) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    other = custom_session.query(TRADES, ["AAPL"]).blink()  # a stream the move leaves alone
    connection = custom_session.connection("wss://socket.test/futures")
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.group.reroute(connection, "x")
    )
    assert custom_session.connections.group.reroute(connection, "x") is False  # once per connection
    custom_session.wait(lambda s: s.urls[-1] == "wss://delayed.test/futures")
    wait_for(lambda: "wss://delayed.test/futures" in custom_session.connections.group.connections)
    with custom_session.connections.group.lock:
        owners = dict(custom_session.connections.group.streams)
    assert owners[1].url == "wss://delayed.test/futures"
    assert owners[2].url == "wss://socket.test/stocks"
    assert len(custom_session.verdicts) == 1
    custom_session.session.release(result)
    custom_session.session.release(other)


def test_a_verdict_that_cannot_be_recorded_still_moves(
    make_ws_session: Callable[..., WsSession], caplog: pytest.LogCaptureFixture
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)

    def broken(*_: object) -> None:
        raise RuntimeError("no REST transport")

    custom_session.connections.group.reconnect_backoff = 0.01
    custom_session.connections.record_verdict = broken
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    connection = custom_session.connection("wss://socket.test/futures")
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.group.reroute(connection, "x")
    )
    custom_session.wait(lambda s: s.urls[-1] == "wss://delayed.test/futures")
    assert "could not record the real-time verdict" in caplog.text
    custom_session.session.release(result)


def test_a_connection_that_does_not_end_is_reported(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    monkeypatch.setattr(ws, "JOIN_TIMEOUT", 0.01)
    monkeypatch.setattr(connection, "join", lambda _: False)
    ws_session.connections.close()
    assert "has not ended 0 s after close()" in caplog.text
    del result


def test_a_close_holding_the_update_graph_lock_joins_nothing(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    # In a console command no live table updates while close() waits, so it joins neither the
    # connections nor the futures schedule worker there; those daemons end on their own. A
    # close without the lock still joins both, each for a bounded time.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    joined: list[float] = []
    detached: list[bool] = []
    join, detach = connection.join, ws_session.connections.group.detach

    def joining(timeout: float) -> bool:
        joined.append(timeout)
        return join(timeout)

    def detaching(owner: ws.WsConnections, *, wait: bool = True) -> None:
        detached.append(wait)
        detach(owner, wait=wait)

    monkeypatch.setattr(connection, "join", joining)
    monkeypatch.setattr(ws_session.connections.group, "detach", detaching)
    console_command(ws_session.connections.close)
    assert (joined, detached) == ([], [False])
    assert join(TIMEOUT)  # the connection's threads end on their own
    ws_session.session.release(result)


@pytest.mark.parametrize("last", ["CONNECTED", "FAILED"])
def test_a_console_close_shows_its_connections_closed(
    *,
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
    rows_when: RowsWhen,
    last: str,
) -> None:
    # Joining no connection, close() under the lock cannot count on the client's own last row,
    # CLOSED, reaching the session before its diagnostics are released: it writes that row
    # itself, from the last row the client wrote (a FAILED one keeps its state), for each
    # family whose row the session shows, and the session leaves the connection's users. Here
    # the client never even closes (a server that never answers), yet a held m.connections()
    # view shows the final state once the command is over.
    m = ws_session.session
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    view = m.connections()

    def states(rows: list[dict[str, Any]]) -> list[tuple[str, str, int, int]]:
        return [
            (r["Family"], r["State"], r["Topics"], r["BufferedRows"])
            for r in rows
            if r["Transport"] == "WebSocket"
        ]

    rows_when(view, lambda rows: states(rows) == [("TICK", "CONNECTED", 1, 0)])
    with connection._lock:
        (row,) = connection._rows.values()
        connection._rows[ChannelFamily.TICK] = dataclasses.replace(
            row, state=ConnectionState(last), buffered_rows=7
        )
        # A family the session does not show a row of: show_closed writes nothing for it.
        connection._rows[ChannelFamily.FMV] = dataclasses.replace(row, family=ChannelFamily.FMV)
    client = connection._client
    stop = client.close
    monkeypatch.setattr(client, "close", lambda: None)
    try:
        console_command(m.close)
        # A FAILED row is shown as the client last wrote it; a CLOSED one holds nothing.
        final = ("TICK", "FAILED", 1, 7) if last == "FAILED" else ("TICK", "CLOSED", 0, 0)
        rows_when(view, lambda rows: states(rows) == [final])
        assert ws_session.connections not in connection.users
    finally:  # even when a check fails, the client must not outlive the test
        stop()
        assert client.join(TIMEOUT)
    del result, view


def test_a_close_without_the_lock_joins_its_connections(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    joined: list[float] = []
    join = connection.join

    def joining(timeout: float) -> bool:
        joined.append(timeout)
        return join(timeout)

    monkeypatch.setattr(connection, "join", joining)
    ws_session.connections.close()
    assert joined == [ws.JOIN_TIMEOUT]
    ws_session.session.release(result)


def test_the_default_factory_builds_real_clients(ws_session: WsSession) -> None:
    factory = ws._WsClientFactory(
        "test-key", backoff=15.0, max_backoff=30.0, thread_prefix="massive #7"
    )
    client = factory(
        "wss://socket.test/stocks",
        sources=[],
        on_open=lambda *_: None,
        on_status=lambda *_: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
    )
    assert isinstance(client, _massive.WsClient)
    assert client._thread.name == "massive #7 ws socket.test/stocks"
    client.close()
    client.start()
    assert client.join(TIMEOUT)
    assert "test-key" not in repr(factory)


def test_a_timer_start_after_close_does_nothing(ws_session: WsSession) -> None:
    connection = spare_connection(ws_session, "never.test")
    connection.close()
    connection._start_later()  # the timer fired just as close() cancelled it
    assert connection.join(TIMEOUT)
    assert ws_session.opens == 0


def test_a_new_stream_cancels_a_pending_idle_close(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(first)
    ws_session.unsubscribed("T.AAPL")
    assert ws_session.on_loop(connection, lambda: connection._idle is not None)
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    assert ws_session.on_loop(connection, lambda: connection._idle is None)
    ws_session.session.release(second)


def test_every_message_rewrites_the_row_once_the_interval_passed(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    written: list[int] = []
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 0)
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    ws_session.send(trade("AAPL"))
    assert written  # the message alone rewrote the row
    ws_session.session.release(result)


def test_contracts_of_opening_streams_count_against_the_cap(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    first = ws_session.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    connection = ws_session.connection("wss://socket.test/options")
    with ws_session.connections.group.lock:
        connection.cap_pending[99] = frozenset({"T.O:SPY251219C00610000"})  # still opening
        with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
            ws.WsConnections._check_cap(OPTIONS, connection, frozenset({"T.O:SPY251219C00620000"}))
        del connection.cap_pending[99]
    ws_session.session.release(first)


def test_the_documented_trade_frame_is_published(
    ws_session: WsSession,
    load_fixture: Callable[[str, str], bytes],
    rows_when: RowsWhen,
) -> None:
    # The frame of Massive's WebSocket quickstart: no `q`, `ds`, or `trfi`, which read as null.
    result = ws_session.query(TRADES, ["MSFT"]).append_only()
    ws_session.subscribed("T.MSFT")
    ws_session.send(*json.loads(load_fixture("ingest/ws", "stocks_trades.json")))
    ws_session.flush()
    rows = rows_when(result, lambda rows: len(rows) == 2)
    assert [(r["Symbol"], r["Price"], r["Size"], r["Conditions"]) for r in rows] == [
        ("MSFT", 215.9721, 100, None),
        ("MSFT", 215.989, 1, [37]),
    ]
    assert rows[0]["Timestamp"].value == 1_611_082_428_813_000_000


def test_a_verdict_while_closing_moves_nothing(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    connection = custom_session.connection("wss://socket.test/futures")
    closed = iter([False, True])  # open when the verdict arrives, closing by the move
    real = cast("Callable[[ws.Connection], bool]", vars(ws.Connection)["closing"].fget)

    def closing(self: ws.Connection) -> bool:
        return next(closed) if self is connection else real(self)

    monkeypatch.setattr(ws.Connection, "closing", property(closing))
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.group.reroute(connection, "x")
    )
    assert connection.join(TIMEOUT)  # the move closed it instead of handing it over
    assert len(custom_session.urls) == 2  # the successor was built, then closed unused
    del result


def test_a_verdict_joins_an_open_delayed_connection(
    make_ws_session: Callable[..., WsSession],
) -> None:
    decisions = iter(
        [
            HostDecision("socket.test", Recency.UNKNOWN, "no timeframe"),
            HostDecision("delayed.test", Recency.DELAYED, "a stream already reads it"),
        ]
    )
    custom_session = make_ws_session(decide=lambda *_: next(decisions))
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    delayed = custom_session.query(QUOTES_FUTURES, ["ESZ5"]).blink()
    moved = custom_session.connection("wss://socket.test/futures")
    existing = custom_session.connection("wss://delayed.test/futures")
    assert custom_session.on_loop(
        moved, lambda: custom_session.connections.group.reroute(moved, "real-time")
    )
    wait_for(lambda: existing.leases == 2)
    assert custom_session.connection("wss://delayed.test/futures") is existing
    custom_session.session.release(result)
    custom_session.session.release(delayed)


# --- Races of the connection lifecycle -------------------------------------------------------


def test_a_retired_connection_is_never_leased(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The registry is read again after the new connection is built: a retired connection is
    # gone from it, and the lease goes to the new one, never to None.
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    ws_session.connections.group.retire(old, idle=False)
    real = ws.WsConnections._check_cap
    looks: list[ws.Connection | None] = []

    def watching(
        spec: DatasetSpec, connection: ws.Connection | None, topics: frozenset[str]
    ) -> None:
        looks.append(connection)
        real(spec, connection, topics)

    monkeypatch.setattr(ws.WsConnections, "_check_cap", staticmethod(watching))
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    replacement = ws_session.connection()
    assert replacement is not old
    assert looks == [None, None]  # nothing the first time, then the new one is registered
    ws_session.subscribed("T.MSFT")
    old.close()
    assert old.join(TIMEOUT)
    ws_session.session.release(first)
    ws_session.session.release(second)


def test_a_stream_opening_as_its_connection_fails_fails_too(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The connection fails fatally after the second stream leased it, before it is attached.
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    real: Callable[..., object] = open_stream_tables

    def failing_meanwhile(*args: object, **kwargs: object) -> object:
        fake_ws_server.auth_replies.append("auth_failed")
        fake_ws_server.close(1011)
        ws_session.wait(lambda s: WsState.FAILED in s.states)
        return real(*args, **kwargs)

    monkeypatch.setattr(ws, "open_stream_tables", failing_meanwhile)
    with pytest.raises(dm.MassiveError, match=r"stream 2 failed: .*rejected the API key"):
        ws_session.query(TRADES, ["MSFT"]).blink()
    wait_for(lambda: [r["State"] for r in streams_of(ws_session, dh_to_pylist)] == ["FAILED"] * 2)
    rows = streams_of(ws_session, dh_to_pylist)
    assert all("rejected the API key" in r["Error"] for r in rows)
    del first


def test_a_verb_never_sees_a_failed_table_before_its_failure(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure is recorded before the table fails, so a verb in between builds its shape.
    q = ws_session.query(TRADES, ["AAPL"])
    first = q.blink()
    ws_session.subscribed("T.AAPL")
    core = ws_session.session._core
    recording = threading.Event()
    proceed = threading.Event()
    real = core.fail_stream

    def gated(stream_id: int, error: str) -> None:
        recording.set()
        assert proceed.wait(TIMEOUT)
        real(stream_id, error)

    monkeypatch.setattr(core, "fail_stream", gated)
    ws_session.send(trade("AAPL", t=10))  # a timestamp out of bounds fails the stream
    flushing = threading.Thread(target=ws_session.flush)
    flushing.start()
    assert recording.wait(TIMEOUT)
    second = q.append_only()  # the table has not failed yet: no raw DHError
    proceed.set()
    flushing.join(TIMEOUT)
    ws_session.session.release(second)
    del first


def test_a_new_channel_on_an_open_connection_declares_its_keys_first(ws_session: WsSession) -> None:
    trades = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.subscribed("Q.AAPL")
    required = ("set_required", "Q", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    position = ws_session.journal.index(required)
    assert ws_session.journal[position + 1] == ("subscribe", ("Q.AAPL",))
    ws_session.session.release(trades)
    ws_session.session.release(quotes)


def test_a_reactive_streams_row_follows_its_watchlist(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["MSFT", "AAPL"])]))
    result = ws_session.query(TRADES, watchlist).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["SymbolCount"], row["Symbols"]) == (2, "AAPL, MSFT")
    del result


def test_contracts_a_wildcard_covers_still_count_against_the_cap(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The wildcard may go at any time: a static contract the verb accepts is never deferred.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    every = ws_session.query(OPTIONS, dm.ALL).blink()
    ws_session.subscribed("T.*")
    ws_session.wait(lambda s: any(status.kind is StatusKind.ACK for status in s.statuses))
    first = ws_session.query(OPTIONS, ["O:SPY251219C00600000", "O:SPY251219C00610000"]).blink()
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        ws_session.query(OPTIONS, ["O:SPY251219C00620000"]).blink()
    assert connection.cap_pending == {}  # the opened stream counts through the admitted set
    ws_session.session.release(first)
    ws_session.session.release(every)


def test_static_contracts_take_the_room_before_a_watchlists(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    static = ws_session.query(OPTIONS, ["O:SPY251219C00690000"]).blink()  # sorts last
    ws_session.subscribed("T.O:SPY251219C00690000")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    reactive = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [(r["StreamId"], r["Deferred"]) for r in rows] == [(1, 0), (2, 1)]
    # The static stream's contract is known as static, so it keeps its place before the cap.
    static_topics = ws_session.on_loop(connection, lambda: dict(connection.subscriptions._static))
    assert static_topics == {"T.O:SPY251219C00690000": 1}
    del static, reactive


def test_a_move_awaits_its_streams_until_the_hand_over(
    make_ws_session: Callable[..., WsSession],
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        held.set()
        assert release.wait(TIMEOUT)

    assert old.post(hold)  # the old loop is busy: the hand-over waits behind it
    assert held.wait(TIMEOUT)
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections.group._move(old, decision)
    successor = custom_session.connection("wss://delayed.test/futures")
    with custom_session.connections.group.lock:
        assert successor.awaiting == {1}
    release.set()
    wait_for(lambda: not successor.awaiting)  # adopted
    custom_session.subscribed("T.ESZ5")
    custom_session.session.release(result)


def test_a_close_during_a_move_leaves_the_old_connection_to_its_hand_over(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # Between the move and the hand-over the old connection is out of the registry and holds
    # no lease, so the session's close neither closes nor joins it. The close takes the stream
    # off the successor and closes that; the hand-over then finds no stream to move and closes
    # the old connection, which ends by itself.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        held.set()
        assert release.wait(TIMEOUT)

    assert old.post(hold)  # the old loop is busy: the hand-over waits behind it
    try:
        assert held.wait(TIMEOUT)
        decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
        custom_session.connections.group._move(old, decision)
        successor = custom_session.connection("wss://delayed.test/futures")
        custom_session.close()
        assert successor.join(0)  # the close closed and joined the successor
        assert not old.closing  # and left the old connection to its hand-over
    finally:  # a failed check must not leave the old loop held
        release.set()
    assert old.join(TIMEOUT)
    with custom_session.connections.group.lock:
        assert not successor.awaiting
    del result


def test_a_failed_connection_is_not_moved(make_ws_session: Callable[..., WsSession]) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    old = custom_session.connection("wss://socket.test/futures")
    with custom_session.connections.group.lock:
        old.failure = dm.MassiveError("rejected the API key")
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections.group._move(old, decision)
    assert old.join(TIMEOUT)  # closed, not handed over
    assert "wss://delayed.test/futures" not in custom_session.connections.group.connections
    del result


def test_a_move_whose_old_loop_ended_releases_the_successor(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    old.close()
    assert old.join(TIMEOUT)  # its loop ended: the hand-over cannot be posted
    # The move started before the close: it still sees the connection open.
    real = cast("Callable[[ws.Connection], bool]", vars(ws.Connection)["closing"].fget)
    monkeypatch.setattr(ws.Connection, "closing", property(lambda c: c is not old and real(c)))
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections.group._move(old, decision)
    successor = custom_session.connection("wss://delayed.test/futures")
    custom_session.on_loop(successor, lambda: None)
    with custom_session.connections.group.lock:
        assert successor.awaiting == set()
    custom_session.on_loop(successor, lambda: successor.release_stream(1))  # dropped, not kept
    assert custom_session.on_loop(successor, lambda: dict(successor._early)) == {}
    # A call kept for an awaited stream is forgotten once the stream is no longer awaited.
    with custom_session.connections.group.lock:
        successor.awaiting.add(99)
    custom_session.on_loop(successor, lambda: successor.release_stream(99))
    assert custom_session.on_loop(successor, lambda: sorted(successor._early)) == [99]
    with custom_session.connections.group.lock:
        successor.awaiting.discard(99)
    custom_session.on_loop(successor, successor.check_idle)
    assert custom_session.on_loop(successor, lambda: dict(successor._early)) == {}
    del result


DELAYED = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")


def test_a_move_after_the_last_session_left_moves_nothing(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # The session closed between the verdict and the move: its close took the connection out
    # of the registry but had not closed it yet. The move must not open a successor that would
    # carry the closed session's stream for ever.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    group = custom_session.connections.group
    with group.lock:
        custom_session.connections._closed = True
        custom_session.connections._leave()  # what close() does first, under the lock
    group._move(old, DELAYED)
    assert "wss://delayed.test/futures" not in group.connections
    assert old.join(TIMEOUT)  # the move closed it
    group.detach(custom_session.connections)  # what close() does last
    del result


def test_a_hand_over_leaves_a_stream_whose_lease_is_gone(
    make_ws_session: Callable[..., WsSession],
) -> None:
    # A stream whose lease was returned after the move (its session closed, or it shut down)
    # stays behind: the successor never subscribes it nor waits for it.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    kept = custom_session.query(FUTURES, ["ESZ5"]).blink()
    gone = custom_session.query(FUTURES, ["NQZ5"]).blink()
    custom_session.subscribed("T.ESZ5", "T.NQZ5")
    old = custom_session.connection("wss://socket.test/futures")
    group = custom_session.connections.group
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        held.set()
        assert release.wait(TIMEOUT)

    assert old.post(hold)  # the hand-over waits behind it
    assert held.wait(TIMEOUT)
    group._move(old, DELAYED)
    successor = custom_session.connection("wss://delayed.test/futures")
    with group.lock:
        assert successor.awaiting == {1, 2}
        custom_session.connections._forget(2)  # its lease is returned; no release is posted
    release.set()
    wait_for(lambda: not successor.awaiting)
    assert custom_session.on_loop(successor, lambda: sorted(successor._streams)) == [1]
    assert [s.stream_id for s in successor.flusher.routes.sinks()] == [1]
    custom_session.session.release(kept)
    del gone


def test_an_open_that_fails_during_a_move_returns_the_moved_lease(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    kept = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    group = custom_session.connections.group

    def move_then_fail(*_: object, **__: object) -> object:
        group._move(old, DELAYED)  # the verdict moves the lease while the stream opens
        raise RuntimeError("the tables could not be built")

    monkeypatch.setattr(ws, "open_stream_tables", move_then_fail)
    with pytest.raises(RuntimeError, match="could not be built"):
        custom_session.query(FUTURES, ["NQZ5"]).blink()
    successor = custom_session.connection("wss://delayed.test/futures")
    with group.lock:
        assert set(group.streams) == {1}
        assert successor.leases == 1  # only the kept stream's
        assert successor.awaiting <= {1}
    custom_session.session.release(kept)


def test_a_moved_options_stream_counts_against_the_cap_until_adopted(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    contract = "O:SPY251219C00600000"
    result = ws_session.query(OPTIONS, [contract]).blink()
    ws_session.subscribed(f"T.{contract}")
    old = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(old, lambda: None)  # the admitted set is published
    group = ws_session.connections.group
    group.reconnect_backoff = 0.01
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        held.set()
        assert release.wait(TIMEOUT)

    assert old.post(hold)  # the successor has not adopted the stream yet
    assert held.wait(TIMEOUT)
    group._move(old, HostDecision("delayed.test", Recency.DELAYED, "server verdict: x"))
    successor = ws_session.connection("wss://delayed.test/options")
    with group.lock:
        assert successor.cap_pending == {1: frozenset({f"T.{contract}"})}
    with pytest.raises(ValueError, match="1 new option topics and 1 in use exceed"):
        ws_session.query(OPTIONS, ["O:SPY251219C00610000"]).blink()  # the cap still holds
    release.set()
    wait_for(lambda: not successor.awaiting)
    ws_session.on_loop(successor, lambda: None)
    with group.lock:
        assert successor.cap_pending == {}
        assert successor.cap_admitted == frozenset({f"T.{contract}"})
    del result


def test_a_move_whose_old_connection_fails_first_releases_the_successor(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The old connection fails after the move posted its hand-over, but before the hand-over
    # runs: the failure fails the moved stream, which shuts down and returns its lease. The
    # successor must stop awaiting the stream and stop counting its topics against the cap.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    contract = "O:SPY251219C00600000"
    result = ws_session.query(OPTIONS, [contract]).blink()
    ws_session.subscribed(f"T.{contract}")
    old = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(old, lambda: None)  # the admitted set is published
    group = ws_session.connections.group
    group.reconnect_backoff = 0.01
    held, release = threading.Event(), threading.Event()

    def fail_when_released() -> None:
        held.set()
        assert release.wait(TIMEOUT)
        error = _massive.WireError("the server went away", host="socket.test", path="/options")
        old._on_state(WsState.FAILED, error)  # what the client reports, ahead of the hand-over

    assert old.post(fail_when_released)
    assert held.wait(TIMEOUT)
    group._move(old, DELAYED)
    successor = ws_session.connection("wss://delayed.test/options")
    release.set()
    assert old.join(TIMEOUT)
    wait_for(lambda: not group.streams)  # the failed stream shut down and returned its lease
    with group.lock:
        assert successor.awaiting == set()
        assert successor.cap_pending == {}
    assert ws_session.on_loop(successor, lambda: dict(successor._early)) == {}
    assert ws_session.on_loop(successor, lambda: successor._idle is not None)  # it may go idle
    again = ws_session.query(OPTIONS, ["O:SPY251219C00610000"]).blink()  # the cap is free
    ws_session.subscribed("T.O:SPY251219C00610000")
    del result, again


def test_a_hand_over_applies_what_the_old_flusher_queued(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    refused = ws_session.query(TRADES, ["AAPL"]).blink()
    published = ws_session.query(TRADES, ["MSFT"]).append_only()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()

    def queue_then_hand_over() -> None:
        old.flusher.fail([1], dm.EntitlementError("refused T.AAPL"))
        old.flusher.rows("T", [{"Ticker": "MSFT", "SipTimestamp": 1_700_000_000_000}])
        old.hand_over(successor)

    ws_session.on_loop(old, queue_then_hand_over)
    assert old.join(TIMEOUT)  # its last flush applied the failure and published the row
    rows_when(published, lambda rows: len(rows) == 1)
    states = [r["State"] for r in streams_of(ws_session, dh_to_pylist)]
    assert states == ["FAILED", "RUNNING"]
    successor.close()
    assert successor.join(TIMEOUT)
    del refused


def test_a_topic_reconnects_once_per_cluster(ws_session: WsSession) -> None:
    assert ws_session.connections.group.first_no_ack("/stocks", "T.AAPL")
    assert not ws_session.connections.group.first_no_ack("/stocks", "T.AAPL")
    assert ws_session.connections.group.first_no_ack("/options", "T.AAPL")


# --- Sessions sharing connections, and how long a stream runs ----------------------------------


def join(ws_session: WsSession) -> dm.MassiveSession:
    # A second session with the same key and settings, sharing the wrapper's connections; the
    # wrapper's own session is closed by its fixture, this one by the test.
    session = dm.MassiveSession(ws_session.config)
    connections = ws.WsConnections(
        session._core,
        resolve_host=ws_session._resolve,
        record_verdict=ws_session._record_verdict,
        fetch_rows=ws_session._fetch_rows,
        crypto_pair=ws_session._crypto_pair,
        client_factory=ws_session.factory,
    )
    assert connections.group is ws_session.connections.group
    cast("Any", session._core._ports)._streams = connections
    session._core.on_close(connections.close)
    return session


def jvm_collect() -> None:
    # A full collection on both sides: Python's cycles, then the JVM's heap.
    gc.collect()
    for _ in range(3):
        _JAVA_SYSTEM.gc()


def rows_of(session: dm.MassiveSession, to_pylist: ToPylist) -> list[dict[str, Any]]:
    assert session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(to_pylist(session.streams()), key=lambda r: r["StreamId"])


def test_sessions_with_one_key_share_one_connection(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    *,
    rows_when: RowsWhen,
) -> None:
    other = join(ws_session)
    mine = ws_session.query(TRADES, ["AAPL"]).append_only()
    theirs = ws_session.query(TRADES, ["AAPL"], other).append_only()
    ws_session.subscribed("T.AAPL")
    assert ws_session.urls == ["wss://socket.test/stocks"]  # one connection for both sessions
    connection = ws_session.connection()
    with ws_session.connections.group.lock:
        assert connection.leases == 2
        assert set(connection.users.values()) == {1}
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(mine, lambda rows: len(rows) == 1)
    rows_when(theirs, lambda rows: len(rows) == 1)
    # Diagnostics stay per session: each shows its own stream and the shared connection.
    assert [r["StreamId"] for r in rows_of(ws_session.session, dh_to_pylist)] == [1]
    assert [r["StreamId"] for r in rows_of(other, dh_to_pylist)] == [2]
    for session in (ws_session.session, other):
        assert session._core.diagnostics.barrier().wait(TIMEOUT)
        (row,) = [r for r in dh_to_pylist(session.connections()) if r["Transport"] == "WebSocket"]
        assert row["Host"] == "socket.test"
    # Closing one session fails its tables and leaves the connection to the other.
    other.close()
    wait_for(lambda: bool(theirs.j_table.isFailed()))
    assert not connection.closing
    with ws_session.connections.group.lock:
        assert connection.leases == 1
    ws_session.send(trade("AAPL", 2.0))
    ws_session.flush()
    rows_when(mine, lambda rows: len(rows) == 2)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    ws_session.session.release(mine)  # the last stream on the topic: now it goes
    ws_session.unsubscribed("T.AAPL")


def test_closing_the_last_session_on_a_connection_closes_it(ws_session: WsSession) -> None:
    other = join(ws_session)
    stocks = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    forex = ws_session.query(FOREX, ["C:EURUSD"], other).blink()
    ws_session.subscribed("C.EUR-USD")
    stocks_connection = ws_session.connection()
    forex_connection = ws_session.connection("wss://socket.test/forex")
    other.close()  # it never used the stocks connection, and alone used the forex one
    assert forex_connection.join(TIMEOUT)
    assert not stocks_connection.closing
    with ws_session.connections.group.lock:
        assert "wss://socket.test/forex" not in ws_session.connections.group.connections
    ws_session.session.release(stocks)
    del forex


def test_closing_a_session_keeps_the_connection_it_shares(ws_session: WsSession) -> None:
    other = join(ws_session)
    stocks = ws_session.query(TRADES, ["AAPL"]).blink()
    shared = ws_session.query(TRADES, ["MSFT"], other).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    forex = ws_session.query(FOREX, ["C:EURUSD"], other).blink()
    ws_session.subscribed("C.EUR-USD")
    stocks_connection = ws_session.connection()
    forex_connection = ws_session.connection("wss://socket.test/forex")
    other.close()  # it shared the stocks connection, and alone used the forex one
    assert forex_connection.join(TIMEOUT)
    assert not stocks_connection.closing
    # Only its own stream left the shared connection.
    assert ws_session.on_loop(stocks_connection, lambda: sorted(stocks_connection._streams)) == [1]
    ws_session.session.release(stocks)
    del forex, shared


def _group_of(session: dm.MassiveSession) -> ws.ConnectionGroup:
    transport = cast("ws.WsTransport", session._core.stream_transport())
    return transport._connections.group


def test_sessions_with_equal_settings_share_one_group() -> None:
    first = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    second = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    try:
        shared = _group_of(first)
        assert _group_of(second) is shared
        assert "group-key" not in repr(shared.key)  # the key holds a digest, not the secret
        first.close()
        assert ws._groups.get(shared.key) is shared  # the group stays with its last session
    finally:
        first.close()
        second.close()
    assert shared.key not in ws._groups


# One value differing from the default for each connection setting.
OTHER_SETTINGS: dict[str, object] = {
    "api_key": "another-key",
    "ws_hosts": {AssetClass.STOCKS: "pinned.test"},
    "ack_timeout": 7.0,
    "reconnect_backoff": 45.0,
    "max_reconnect_backoff": 300.0,
    "flush_interval": 0.2,
    "rest_base_url": "https://rest.example.com",
}


def test_every_connection_setting_has_a_case() -> None:
    assert set(OTHER_SETTINGS) == set(ws.CONNECTION_SETTINGS)


@pytest.mark.parametrize("setting", sorted(OTHER_SETTINGS))
def test_a_session_with_any_other_connection_setting_gets_its_own_group(setting: str) -> None:
    # Never another session's settings: every setting that shapes a connection keeps them apart.
    base = dm.MassiveConfig(api_key="group-key")
    first = dm.MassiveSession(base)
    changes: dict[str, Any] = {setting: OTHER_SETTINGS[setting]}
    apart = dm.MassiveSession(dataclasses.replace(base, **changes))
    try:
        assert _group_of(apart) is not _group_of(first)
    finally:
        first.close()
        apart.close()


def test_a_stream_waiting_for_a_slot_says_so(
    *,
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    # Another connection with the key (another process, here the fake's reply) holds the slot.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    ws_session = make_ws_session(backoff=1.0)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    capped = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    cap = "over capacity: 2/1; 1 deferred"
    wait_for(lambda: streams_of(ws_session, dh_to_pylist)[0]["Error"] == cap)
    fake_ws_server.auth_replies.append("max_connections")
    fake_ws_server.close()  # the socket drops; the reconnect finds the slot taken
    ws_session.wait(lambda h: WsState.WAITING_SLOT in h.states)
    stall = (
        "waiting for a WebSocket slot on socket.test/options: another connection with this "
        "API key, perhaps from another process, holds it; the plan allows one connection "
        "per cluster"
    )
    wait_for(lambda: streams_of(ws_session, dh_to_pylist)[0]["Error"] == stall)
    every = ws_session.query(OPTION_QUOTES, dm.ALL).blink()  # opens during the wait
    wait_for(lambda: [r["Error"] for r in streams_of(ws_session, dh_to_pylist)] == [stall] * 2)
    ws_session.wait(lambda h: h.opens == 2)  # the slot is free again
    wait_for(lambda: [r["Error"] for r in streams_of(ws_session, dh_to_pylist)] == [cap, None])
    del capped, every


def freed(core: weakref.ref[SessionCore]) -> bool:
    # Bounded wait in Python, not in one C-level wait: a free-threaded build merges a reference
    # dropped on another thread into its creator's count only while the creator runs Python.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.02)):
        if core() is None:
            return True
        pause.wait(0.02)
    return core() is None


def test_a_session_moved_by_a_verdict_is_freed_without_the_cyclic_collector(
    fake_ws_server: FakeMassiveServer,
) -> None:
    # The old connection keeps the moved stream routed for its final flush; neither that nor
    # anything else may keep the session once nothing uses it, cycles or not.
    harness: WsSession = importlib.import_module("tests._fakes.ws_session").WsSession(
        fake_ws_server, decide=unknown_futures
    )
    gc.collect()
    gc.disable()
    try:
        harness.connections.group.reconnect_backoff = 0.01
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda s: s.opens == 2)
        harness.subscribed("T.ESZ5")
        successor = harness.connection("wss://delayed.test/futures")
        core = weakref.ref(harness.session._core)
        harness.session.release(result)
        del result, harness.session, harness.connections
        assert freed(core)
        assert successor.join(TIMEOUT)  # the session's closer closed it
    finally:
        gc.enable()


def test_a_closed_session_that_shared_a_connection_is_freed_once_its_table_is_dropped(
    ws_session: WsSession,
) -> None:
    # The closed session created the shared connection, which keeps serving the other one.
    other = join(ws_session)
    theirs = ws_session.query(TRADES, ["AAPL"], other).append_only()
    ws_session.subscribed("T.AAPL")
    mine = ws_session.query(TRADES, ["AAPL"]).append_only()
    core = weakref.ref(other._core)
    gc.collect()
    gc.disable()
    try:
        other.close()
        del other, theirs
        assert freed(core)
    finally:
        gc.enable()
    ws_session.session.release(mine)


def test_an_inline_sessions_derived_table_keeps_running(
    fake_ws_server: FakeMassiveServer, dh_exec_ctx: object, rows_when: RowsWhen
) -> None:
    # `dm.MassiveSession().stocks.trades("AAPL").append_only().where(...)`: the session and the
    # library's result are dropped at once, but the derived table uses the stream, so the
    # stream, its connection, and its session keep running through every collection.
    # Built directly, not by the fixture, which would close the session this test frees.
    harness: WsSession = importlib.import_module("tests._fakes.ws_session").WsSession(
        fake_ws_server
    )
    ws_session = harness
    del harness
    core = weakref.ref(ws_session.session._core)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [ws_session.query(TRADES, ["AAPL"]).append_only().where("Price > 0")]
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    del ws_session.session, ws_session.connections
    jvm_collect()
    assert core() is not None
    ws_session.send(trade("AAPL"))
    connection.flusher.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    # Releasing the derived table stops the stream, frees the session, and closes the
    # connection that nothing else uses.
    derived.clear()
    scope.release()
    # Wait in Python, not in join(): a free-threaded build merges a reference dropped on another
    # thread into its owner's count only while the owner runs Python code.
    wait_for(lambda: core() is None)
    assert connection.join(TIMEOUT)
    assert WsState.CLOSED in ws_session.states


def test_a_watchlist_edit_after_a_jvm_collection_still_resubscribes(ws_session: WsSession) -> None:
    # Deephaven holds a table's listeners only weakly: the stream must hold its own.
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    jvm_collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    ws_session.session.release(result)


def test_stress_two_sessions_open_and_release_while_one_closes(ws_session: WsSession) -> None:
    other = join(ws_session)
    symbols = ["AAPL", "MSFT", "IBM", "GOOG"]
    started = threading.Barrier(17)
    closed_errors: list[str] = []

    def work(index: int) -> None:
        session = ws_session.session if index % 2 else other
        started.wait(TIMEOUT)
        for round_ in range(20):
            try:
                result = ws_session.query(TRADES, [symbols[(index + round_) % 4]], session).blink()
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
    core = ws_session.session._core
    drained = threading.Event()
    core.defer(drained.set)
    assert drained.wait(TIMEOUT)
    wait_for(lambda: not ws_session.connections.group.streams)
    with ws_session.connections.group.lock:
        assert all(c.leases == 0 for c in ws_session.connections.group.connections.values())


def test_closing_a_session_closes_a_retired_connection_it_alone_leases(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A connection out of the registry (a fatal failure retired it) still carries the session's
    # stream until the stream shuts down; the session's close closes it too, reaching it through
    # the stream's lease. The close fails the stream's table first, and the next update cycle
    # shuts the blink table down, whose callback returns that lease: on a slow machine before
    # the close takes the session's leases. The callback is held here until the close is done,
    # so the close always finds the lease. (Had the callback come first, nothing would close the
    # connection but its idle close; in the library only a fatal failure retires a connection
    # that still carries a lease, and the failure ends the connection by itself.)
    shut_down = ws_session.connections._shut_down
    lock = threading.Lock()
    held: list[int] = []
    holding = True

    def hold(stream_id: int) -> None:
        with lock:
            if holding:
                held.append(stream_id)
                return
        shut_down(stream_id)

    monkeypatch.setattr(ws_session.connections, "_shut_down", hold)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    assert ws_session.on_loop(
        connection, lambda: ws_session.connections.group.retire(connection, idle=False)
    )
    ws_session.close()
    assert connection.join(0)  # the close joined it
    with lock:  # a callback held so far is passed on now, and a later one goes straight through
        holding = False
        late = list(held)
    for stream_id in late:
        shut_down(stream_id)
    del result


def test_a_java_held_callback_of_a_freed_session_does_nothing() -> None:
    # The JVM may run a publisher's shutdown callback after the session's connections are gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    ws._weakly(cast("weakref.ref[ws.WsConnections]", gone), "_shut_down", 1)


def test_an_ended_connection_holds_no_stream(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The client's terminal state lets every stream go on the loop thread, and the flush
    # thread clears its routes when it ends; a call that reaches the connection after that is
    # ignored, so no stream (nor, through it, a session) is left on a connection that is gone.
    other = join(ws_session)
    mine = ws_session.query(TRADES, ["AAPL"]).blink()
    theirs = ws_session.query(TRADES, ["MSFT"], other).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    stream = cast("ws._Stream", ws_session.on_loop(connection, lambda: connection._streams[1]))
    # A freed session shows no row: an update for its stream is dropped.
    monkeypatch.setattr(stream.sink, "_core", lambda: None)
    ws_session.on_loop(
        connection, lambda: connection._update_streams({1: StreamUpdate(deferred=1)})
    )
    monkeypatch.undo()
    connection.close()
    assert connection.join(TIMEOUT)
    assert connection._streams == {}
    assert connection.flusher.routes.sinks() == ()
    # The loop has ended: what still reaches the connection (here called directly, as no
    # post can run) leaves nothing behind.
    connection.add_stream(stream)
    connection.adopt([stream])
    connection.check_idle()
    assert connection._streams == {}
    other.close()  # its stream's release finds the loop gone
    ws_session.session.release(mine)
    del theirs


# --- The futures schedule calendar --------------------------------------------------------------


def looked_up(ws_session: WsSession, *tickers: str) -> None:
    # Wait until the calendar has asked Massive's contracts for every ticker.
    def asked() -> bool:
        sent = {
            ticker
            for path, query in ws_session.fetched
            if path == "/futures/v1/contracts"
            for ticker in cast("tuple[str, ...]", query["ticker.any_of"])
        }
        return set(tickers) <= sent

    ws_session.wait(lambda _: asked())


def test_a_futures_stream_looks_up_its_contracts_as_it_opens(ws_session: WsSession) -> None:
    result = ws_session.query(FUTURES, ["ESZ5", "GCQ6-GCJ7"]).blink()
    looked_up(ws_session, "ESZ5")
    calendar = ws_session.connections.futures_calendar()
    assert calendar is ws_session.connections.futures_calendar()
    assert calendar.wait_idle(15.0)
    # A spread is not a contract: it is never looked up.
    sent = [q["ticker.any_of"] for path, q in ws_session.fetched if path == "/futures/v1/contracts"]
    assert sent == [("ESZ5",)]
    ws_session.session.release(result)


def test_a_futures_watchlist_looks_up_the_contracts_it_adds(ws_session: WsSession) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["ESZ5"])]))
    result = ws_session.query(FUTURES, watchlist).blink()
    looked_up(ws_session, "ESZ5")
    watchlist.add(new_table([string_col("Symbol", ["NQZ5"])]))
    looked_up(ws_session, "NQZ5")
    ws_session.session.release(result)


def test_a_stock_stream_makes_no_calendar(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    assert ws_session.connections.group._calendar is None
    ws_session.session.release(result)


def test_one_calendar_wins_a_race_and_closes_with_the_connections(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[futures_calendar.FuturesCalendar] = []
    real = futures_calendar.FuturesCalendar

    def racing(
        fetch: Callable[..., list[dict[str, object]]],
        *,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch, contracts_path=contracts_path, schedules_path=schedules_path, label=label
            )

        calendar = make()
        created.append(calendar)
        if len(created) == 1:  # another caller installs its calendar meanwhile
            winner = make()
            created.append(winner)
            ws_session.connections.group._calendar = winner
        return calendar

    monkeypatch.setattr("deephaven_massive._ingest.ws.FuturesCalendar", racing)
    found = ws_session.connections.futures_calendar()
    loser, winner = created
    assert found is winner
    assert not loser._thread.is_alive()  # the loser was closed at once
    ws_session.close()
    assert not winner._thread.is_alive()


def test_no_calendar_outlives_a_closed_transport(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[futures_calendar.FuturesCalendar] = []
    real = futures_calendar.FuturesCalendar

    def closing(
        fetch: Callable[..., list[dict[str, object]]],
        *,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch, contracts_path=contracts_path, schedules_path=schedules_path, label=label
            )

        calendar = make()
        created.append(calendar)
        ws_session.connections.close()  # the session closes while the calendar starts
        return calendar

    monkeypatch.setattr("deephaven_massive._ingest.ws.FuturesCalendar", closing)
    with pytest.raises(dm.MassiveError, match="connection group has closed"):
        ws_session.connections.futures_calendar()
    assert not created[0]._thread.is_alive()  # closed at once, never installed
    # Once closed, no calendar is even created.
    with pytest.raises(dm.MassiveError, match="is closed"):
        ws_session.connections.futures_calendar()
    assert len(created) == 1


def test_the_calendar_never_reads_through_a_freed_session(ws_session: WsSession) -> None:
    # A session freed without close() stays in its group until its closer runs on the reaper;
    # its REST client may be closing, so the calendar does not read through it.
    reads: list[str] = []

    class _Freed:
        closed = False

        @staticmethod
        def core() -> None:
            return None

        @staticmethod
        def fetch_rows(path: str, _query: object, **_charging: object) -> list[dict[str, object]]:
            reads.append(path)
            return []

    group = ws_session.connections.group
    real = group.sessions
    group.sessions = cast("set[ws.WsConnections]", {_Freed()})
    try:
        with pytest.raises(dm.MassiveError, match="connection group has closed"):
            group._fetch_rows("/futures/v1/contracts", {}, bucket="futures", cancel=Cancel())
    finally:
        group.sessions = real  # before the session's close leaves the group
    assert reads == []


def test_the_calendar_outlives_the_session_that_made_it(ws_session: WsSession) -> None:
    # A shared futures connection keeps dating the other session's rows: the calendar is the
    # group's, and reads through whichever session is still open.
    other = join(ws_session)
    mine = ws_session.query(FUTURES, ["ESZ5"]).blink()
    looked_up(ws_session, "ESZ5")
    calendar = ws_session.connections.futures_calendar()
    theirs = ws_session.query(FUTURES, ["NQZ5"], other).blink()
    ws_session.close()
    del mine
    assert calendar._thread.is_alive()
    group = ws_session.connections.group
    assert group.futures_calendar(contracts_path="x", schedules_path="y") is calendar
    looked_up(ws_session, "NQZ5")  # read through the open session
    other.close()
    assert not calendar._thread.is_alive()  # the group's last session closed it
    with pytest.raises(dm.MassiveError, match="connection group has closed"):
        group._fetch_rows("/futures/v1/contracts", {}, bucket="futures", cancel=Cancel())
    del theirs


def lookups(session: WsSession) -> list[tuple[str, str]]:
    # Each crypto ticker the transport looked up, with the thread it ran on.
    return [
        cast("tuple[str, str]", call[1:]) for call in session.journal if call[0] == "crypto_pair"
    ]


# Each of these reads as two different pairs (DOT-USD or DO-TUSD); only Massive's list says which.
AMBIGUOUS = {
    "X:DOTUSD": ("DOT", "USD"),  # DOT-USD or DO-TUSD
    "X:SHIBUSD": ("SHIB", "USD"),  # SHIB-USD or SHI-BUSD
    "X:USDTUSD": ("USDT", "USD"),  # USDT-USD or USD-TUSD
    "X:BTCPYUSD": ("BTC", "PYUSD"),  # BTC-PYUSD or BTCPY-USD
}


def test_an_ambiguous_crypto_pair_subscribes_the_topic_massive_gives(ws_session: WsSession) -> None:
    # Massive's crypto ticker list splits every pair, and each ticker is looked up once.
    ws_session.pair_answers.update(AMBIGUOUS)
    ws_session.pair_answers["X:ETHWBTC"] = ("ETH", "WBTC")  # ends in BTC, quotes in WBTC
    result = ws_session.query(CRYPTO, [*AMBIGUOUS, "X:BTCUSD", "X:ETHWBTC"]).blink()
    ws_session.subscribed(
        "XT.DOT-USD", "XT.SHIB-USD", "XT.USDT-USD", "XT.BTC-PYUSD", "XT.BTC-USD", "XT.ETH-WBTC"
    )
    looked_up = sorted([*AMBIGUOUS, "X:BTCUSD", "X:ETHWBTC"])  # every pair: none is guessed
    assert sorted(symbol for symbol, _ in lookups(ws_session)) == looked_up
    again = ws_session.query(CRYPTO, ["X:DOTUSD"]).append_only()  # another stream: cached
    assert sorted(symbol for symbol, _ in lookups(ws_session)) == looked_up
    for table in (result, again):
        ws_session.session.release(table)


def test_a_failed_crypto_pair_lookup_refuses_the_stream(ws_session: WsSession) -> None:
    with pytest.raises(
        dm.MassiveError,
        match=r"^cannot stream 'X:DOTUSD': 'X:DOTUSD' cannot be split into its two currencies: "
        r"Massive's reference data does not list it$",
    ):
        ws_session.query(CRYPTO, ["X:DOTUSD"]).blink()
    assert ws_session.urls == []  # no connection opened, so no topic guessed


def test_a_watchlist_row_of_an_ambiguous_pair_is_looked_up_off_the_update_graph(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:SHIBUSD"])]))
    ws_session.pair_answers["X:SHIBUSD"] = ("SHIB", "USD")
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.SHIB-USD")  # the current row, looked up before the stream started
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    ws_session.subscribed("XT.DOT-USD")  # a later row, looked up on the lookup worker
    (_, caller), (_, worker) = lookups(ws_session)
    assert caller == threading.current_thread().name
    assert worker.startswith(f"{ws_session.session._core.thread_prefix} crypto pairs")
    watchlist.add(new_table([string_col("Symbol", ["X:ETHTUSD"])]))  # Massive does not list it
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["Error"].startswith("cannot stream 'X:ETHTUSD': 'X:ETHTUSD' cannot be split")
    assert "XT.DO-TUSD" not in {json.loads(f)["params"] for f in ws_session.server.frames()}


def test_a_lookup_that_finishes_after_its_stream_ended_changes_nothing(
    ws_session: WsSession,
) -> None:
    # A watchlist change waits on the lookup worker; if the stream has gone by the time the
    # answer arrives, the answer is kept for the session and nothing is posted or failed.
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    ws_session.connections._change_pairs(999, frozenset({"X:DOTUSD"}), frozenset())
    assert [symbol for symbol, _ in lookups(ws_session)] == ["X:DOTUSD"]
    assert ws_session.connections.pairs() == {"X:DOTUSD": ("DOT", "USD")}
    assert ws_session.urls == []  # no connection was opened for it


def test_closing_the_transport_stops_the_lookup_worker(ws_session: WsSession) -> None:
    ws_session.connections.close()
    with pytest.raises(RuntimeError, match="after shutdown"):
        ws_session.connections._lookups.submit(print)


def test_a_watchlist_whose_first_row_massive_does_not_list_is_refused(
    ws_session: WsSession,
) -> None:
    watchlist = new_table([string_col("Symbol", ["X:ETHTUSD"])])
    with pytest.raises(dm.MassiveError, match=r"^cannot stream 'X:ETHTUSD': 'X:ETHTUSD' cannot"):
        ws_session.query(CRYPTO, watchlist).append_only()
    assert ws_session.urls == []  # no connection opened


def test_an_unexpected_lookup_failure_fails_the_stream(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    ws_session.pair_answers["X:DOTUSD"] = RuntimeError("the lookup broke")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:BTCUSD"])]))
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.BTC-USD")
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["Error"] == (
        "looking up the crypto pairs ['X:DOTUSD'] failed: RuntimeError('the lookup broke')"
    )
    assert "looking up crypto pairs failed" in caplog.text


def test_a_watchlist_change_racing_close_is_dropped(ws_session: WsSession) -> None:
    # close() shuts the lookup worker down; a change that reaches it after that is dropped,
    # since the stream goes with the transport, and nothing is raised on the update graph.
    result = ws_session.query(CRYPTO, ["X:BTCUSD"]).append_only()
    ws_session.subscribed("XT.BTC-USD")
    (stream_id,) = ws_session.connections._mine
    ws_session.connections._lookups.shutdown()
    ws_session.connections._symbols_changed(stream_id, frozenset({"X:ETHUSD"}), frozenset())
    # The stream's own pair, before it started.
    assert [symbol for symbol, _ in lookups(ws_session)] == ["X:BTCUSD"]
    ws_session.session.release(result)


def test_every_crypto_pair_streams_without_a_lookup(ws_session: WsSession) -> None:
    result = ws_session.query(CRYPTO, dm.ALL).blink()
    ws_session.subscribed("XT.*")
    assert lookups(ws_session) == []
    ws_session.session.release(result)


def test_a_removal_never_overtakes_a_slow_lookup_of_the_same_pair(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The crypto pairs worker runs one change at a time, in order: a pair removed while its
    # addition still waits on the lookup is removed after it is added, never before, so the
    # stream does not end up subscribed to a pair its table no longer holds.
    gate, started = threading.Event(), threading.Event()
    lookup = ws_session.connections._crypto_pair
    change = ws_session.connections._change_pairs
    done: list[tuple[list[str], list[str]]] = []

    def slow(symbol: str) -> tuple[str, str]:
        if symbol == "X:DOTUSD":
            started.set()
            assert gate.wait(TIMEOUT)
        return lookup(symbol)

    def recorded(stream_id: int, added: frozenset[str], removed: frozenset[str]) -> None:
        change(stream_id, added, removed)
        with ws_session.cond:
            done.append((sorted(added), sorted(removed)))

    ws_session.connections._crypto_pair = slow
    # Records the order in which the worker finishes the changes.
    monkeypatch.setattr(ws_session.connections, "_change_pairs", recorded)
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:BTCUSD"])]))
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.BTC-USD")
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    assert started.wait(TIMEOUT)  # the addition waits on its lookup
    watchlist.delete(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    watchlist.add(new_table([string_col("Symbol", ["X:ETHUSD"])]))
    gate.set()

    def position(entry: Callable[[tuple[list[str], list[str]]], bool]) -> int | None:
        with ws_session.cond:
            return next((i for i, e in enumerate(done) if entry(e)), None)

    def added_dot(e: tuple[list[str], list[str]]) -> bool:
        return "X:DOTUSD" in e[0]

    def removed_dot(e: tuple[list[str], list[str]]) -> bool:
        return "X:DOTUSD" in e[1]

    wait_for(lambda: position(added_dot) is not None and position(removed_dot) is not None)
    assert cast("int", position(added_dot)) < cast("int", position(removed_dot))
    ws_session.subscribed("XT.ETH-USD")
    ws_session.unsubscribed("XT.DOT-USD")
    ws_session.session.release(result)


def test_a_verdict_on_every_frame_of_a_step_moves_the_streams_without_failing_them(
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # A step of more than 1,000 topics goes out in two subscribe frames, and the server refuses
    # each. The second verdict must find the move under way, not fail the streams being moved.
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    message = "Real-time data requires an upgrade"
    fake_ws_server.step_refusals.extend([message] * 2)
    record = custom_session.connections.record_verdict

    def record_after_both(
        asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        # The hand-over waits for the verdict to be recorded: hold it until both refusals
        # have reached the old connection, so they arrive while its streams are moving.
        custom_session.wait(lambda s: sum(t.kind is StatusKind.REFUSED for t in s.statuses) == 2)
        record(asset_class, family, decision)

    custom_session.connections.record_verdict = record_after_both
    symbols = [f"S{i:04}" for i in range(1001)]
    result = custom_session.query(FUTURES, symbols).blink()
    custom_session.wait(lambda s: s.opens == 2)
    assert custom_session.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
    custom_session.subscribed("T.S0000", "T.S1000")
    # The old connection's last flush applies whatever failure it had queued.
    wait_for(
        lambda: (
            not any(t.name == "massive-flush socket.test/futures" for t in threading.enumerate())
        )
    )
    assert len(custom_session.verdicts) == 1
    [stream] = streams_of(custom_session, dh_to_pylist)
    assert (stream["State"], stream["Error"], stream["Host"]) == (
        "RUNNING",
        None,
        "delayed.test",
    )
    custom_session.session.release(result)


def test_a_static_symbol_table_subscribes_its_symbols_once(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    fixed = new_table([string_col("Symbol", ["AAPL", "MSFT"])])
    results = [ws_session.query(TRADES, fixed).blink(), ws_session.query(TRADES, fixed).last_by()]
    ws_session.subscribed("T.AAPL", "T.MSFT")
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert (stream["SymbolCount"], stream["State"]) == (2, "RUNNING")
    for result in results:
        ws_session.session.release(result)


def test_the_next_reconnect_time_is_shown_only_while_waiting(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()

    def after(*states: tuple[WsState, float | None]) -> int | None:
        for state, detail in states:
            connection._on_state(state, detail)
        return connection._next_reconnect_at

    assert ws_session.on_loop(connection, lambda: after((WsState.RECONNECTING, 2.0))) == 2 * 10**9
    assert ws_session.on_loop(connection, lambda: after((WsState.CONNECTING, None))) is None
    ws_session.session.release(result)


def test_a_connections_drift_key_is_fixed_when_it_is_created(ws_session: WsSession) -> None:
    forex = ws_session.query(FOREX, ["C:EURUSD"]).blink()
    ws_session.subscribed("C.EUR-USD")
    connection = ws_session.connection("wss://socket.test/forex")
    key = ("WS", "forex", ChannelFamily.TICK.value)
    assert connection.key == key
    fmv = ws_session.query(FOREX_FMV, ["C:EURUSD"]).blink()  # joins the same connection
    ws_session.subscribed("FMV.EUR-USD")
    with ws_session.session._core.registry_lock:
        assert connection.families == {ChannelFamily.TICK, ChannelFamily.FMV}
    assert connection.key == key
    ws_session.session.release(forex)
    ws_session.session.release(fmv)


def test_the_backlog_rewrite_repeats_until_the_rows_are_published(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One message, then silence: the rewrite its rows scheduled must schedule the next one
    # while rows remain, so BufferedRows reaches 0 however long the flush stays behind.
    monkeypatch.setattr(ws, "_BACKLOG_ROW_DELAY", 0.01)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(t.message == "subscribed to: T.AAPL" for t in s.statuses))
    connection = ws_session.connection()
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 10**18)  # messages never rewrite the row
    fired: list[int] = []
    twice = threading.Event()
    rewrite = connection._backlog_row

    def counted() -> None:
        rewrite()
        fired.append(connection.flusher.backlog)
        if len(fired) == 2:
            twice.set()

    monkeypatch.setattr(connection, "_backlog_row", counted)
    ws_session.send(trade("AAPL"))
    assert twice.wait(TIMEOUT)  # a second rewrite, with no message since the first
    assert fired[:2] == [1, 1]
    ws_session.flush()

    def shown() -> int:
        [row] = connections_of(ws_session, dh_to_pylist)
        return cast("int", row["BufferedRows"])

    wait_for(lambda: shown() == 0)
    ws_session.session.release(result)


def test_buffered_rows_are_shown_until_they_are_published(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ws, "_BACKLOG_ROW_DELAY", 0.05)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 0)  # every message rewrites the row
    ws_session.send(trade("AAPL"), trade("AAPL"))
    assert ws_session.on_loop(connection, lambda: connection.flusher.backlog) == 2
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["BufferedRows"] == 2
    ws_session.flush()  # no message follows: the row is rewritten by itself once the rows went

    def shown() -> int:
        [row] = connections_of(ws_session, dh_to_pylist)
        return cast("int", row["BufferedRows"])

    wait_for(lambda: shown() == 0)
    ws_session.session.release(result)


# --- logging -------------------------------------------------------------------------------------

_URL = "wss://socket.test/stocks"


def test_the_connection_logs_drift_rejects_and_a_summary(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    other = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    connection = ws_session.connection()
    first = ws_session.on_loop(connection, lambda: connection._summary)
    assert first is not None  # the first generation started the summary's timer
    with caplog.at_level(logging.DEBUG):
        ws_session.send(trade("AAPL", zz=1), trade("AAPL", p="bad"))
        ws_session.flush()
        ws_session.unsubscribed("T.AAPL")  # the reject failed stream 1, which let its topic go
        ws_session.on_loop(connection, connection._summarize)
    # Each summary starts the next window's timer.
    assert ws_session.on_loop(connection, lambda: connection._summary) is not first
    assert f"(schema drift) at {_URL} channel T; symbol AAPL; first value 1" in caplog.text
    [rejected] = [m for m in caplog.messages if "a rejected row fails" in m]
    assert rejected.startswith(f"{_URL} channel T: a rejected row fails streams [1]: ")
    assert rejected.endswith('"p":"bad","q":1,"t":1700000000000,"s":100,"z":3,"ds":"100"}')
    [summary] = [m for m in caplog.messages if m.startswith(f"{_URL}: last 60 s: ")]
    assert summary.startswith(
        f"{_URL}: last 60 s: 1 data batches, rows T=1, 1 rejected; 1 of 1 topics acknowledged "
        "['T.MSFT'], 0 pending, 0 with no acknowledgement; last data T "
    )
    assert summary.endswith(" s ago; state CONNECTED, 0 reconnects")
    # The next window starts empty; with DEBUG off the summary is counted but not logged.
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        ws_session.on_loop(connection, connection._summarize)
    assert "1 data batches" not in caplog.text
    assert "0 data batches, rows none, 0 rejected" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.INFO):
        ws_session.on_loop(connection, connection._summarize)
    assert "last 60 s" not in caplog.text
    del result, other


def test_symbol_changes_and_shutdowns_are_logged(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    with caplog.at_level(logging.DEBUG, logger=ws.__name__):
        result = ws_session.query(TRADES, watchlist).append_only()
        ws_session.subscribed("T.AAPL")
        watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
        ws_session.subscribed("T.MSFT")
        ws_session.session.release(result)
        ws_session.unsubscribed("T.AAPL")  # the stream shut down and released its topics
    assert "stream 1: its symbol table added ['MSFT'] and removed []" in caplog.messages
    assert (
        f"stream 1 shut down: its route is removed and 2 topics released on {_URL}"
        in caplog.messages
    )


# --- thread names --------------------------------------------------------------------------------

_DIVIDENDS_PATH = "/stocks/v1/dividends"
_DIVIDENDS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=_DIVIDENDS_PATH,
    symbol_param="ticker.any_of",
    keys={(Transport.REST, _DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)


def test_every_library_thread_is_named_after_its_session_or_connection_group(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every thread the library starts, recorded by name. The session's own (its writer and
    # reaper, a poller and its pool) carry the session's prefix; those of the connections,
    # which serve every session of their group (a client and its flush thread, a verdict's move
    # and delayed start, the futures schedule worker), carry the group's label.
    started: list[str] = []
    start = threading.Thread.start

    def recording(thread: threading.Thread) -> None:
        started.append(thread.name)
        start(thread)

    monkeypatch.setattr(threading.Thread, "start", recording)
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: s.opens == 2)  # moved to the delayed host, after a delay
    core = custom_session.session._core
    pooled = threading.Event()

    def read(*, executor: ThreadPoolExecutor, **_: object) -> tuple[pa.Table, int]:
        executor.submit(pooled.set).result(TIMEOUT)
        return core.catalog.dataset_schema(_DIVIDENDS).arrow_schema().empty_table(), 1

    table = poll.open_poll(
        core,
        spec=_DIVIDENDS,
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=dm.PollMode.REPLACE,
        query="<Query test>",
        read=read,
        pollers=poll.Pollers(core.label),
        host="api.test",
        path=_DIVIDENDS_PATH,
        clock=FakeClock(manual=True),
    )
    assert pooled.wait(TIMEOUT)
    # The host decision's reader would name a fan-out pool after the hosts.
    readers: list[str] = []

    def decide(*_: object, fetch: Callable[..., object], **__: object) -> HostDecision:
        readers.append(cast("Any", fetch).func.__self__.threads)
        return REALTIME

    monkeypatch.setattr(hosts, "decide", decide)
    core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    prefix, group = core.thread_prefix, custom_session.connections.group.label
    assert re.fullmatch(r"massive #\d+", prefix), prefix
    assert re.fullmatch(r"massive group #\d+", group), group
    assert readers == [f"{prefix} hosts rest"]
    mine = {name.removeprefix(f"{prefix} ") for name in started if name.startswith(f"{prefix} ")}
    shared = {name.removeprefix(f"{group} ") for name in started if name.startswith(f"{group} ")}
    assert len(mine) + len(shared) == len(set(started)), started
    assert {"diagnostics", "reaper"} <= mine
    assert {
        "ws 127.0.0.1/futures",  # the client of each connection, on the fake server
        "flush socket.test/futures",
        "move socket.test/futures",
        "start delayed.test/futures",
        "flush delayed.test/futures",
        "futures schedules",
    } <= shared
    polls = sorted(role for role in mine if role.startswith("poll-"))
    assert len(polls) == 2
    assert re.fullmatch(r"poll-\d+", polls[0])
    assert polls[1] == f"{polls[0]} rest_0"
    custom_session.session.release(result)
    custom_session.session.release(table)


def test_a_reconnect_keeps_the_one_summary_timer(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    first = ws_session.on_loop(connection, lambda: connection._summary)
    assert first is not None
    fake_ws_server.close()  # the client reconnects: a second generation opens
    ws_session.wait(lambda s: s.opens == 2)
    assert ws_session.on_loop(connection, lambda: connection._summary) is first
    del result
