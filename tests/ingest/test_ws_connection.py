"""Tests for one WebSocket connection and the group that shares connections between sessions.

They run against the fake WebSocket server, through sessions, as the transport uses them.
"""

import dataclasses
import functools
import json
import logging
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, TypeAlias, cast

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._config import DELAYED_HOST, REALTIME_HOST
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars, Recency
from deephaven_massive._ingest import futures_calendar, ws, ws_connection
from deephaven_massive._ingest.host_decision import recency_of
from deephaven_massive._massive import Cancel, StatusKind, Transport, WsState
from deephaven_massive._tables.diagnostics import ConnectionState, Diagnostics
from deephaven_massive._tables.transports import HostDecision
from tests._fakes.ws import FakeMassiveServer, status_frame
from tests._fakes.ws_session import REALTIME, TIMEOUT, WsSession

RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)

ToPylist: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"

W = Transport.WS

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))

TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
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

OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={NotBars.ANY: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
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
    assert ws_session.session._core.diagnostics.wait_until_shown(TIMEOUT)
    return sorted(dh_to_pylist(ws_session.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    settle(ws_session)
    assert ws_session.session._core.diagnostics.wait_until_shown(TIMEOUT)
    rows = dh_to_pylist(ws_session.session.connections())
    return [r for r in rows if r["Transport"] == "WebSocket"]


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
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = ws_session.query(TRADES, ["AAPL"])
    result = q.blink()
    ws_session.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    other = ws_session.query(TRADES, ["MSFT"]).blink()  # a second stream the failure fails
    ws_session.subscribed("T.MSFT")
    failed = ws_session.connection()
    fake_ws_server.auth_replies.append("auth_failed")  # for the reconnect
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        fake_ws_server.close(1011)
        ws_session.wait(lambda s: WsState.FAILED in s.states)
        assert failed.flusher.join(TIMEOUT)
    # An expected failure, not a defect: one WARNING for the cause, however many streams it
    # fails, and no ERROR.
    rejected = [r for r in caplog.records if "rejected the API key" in r.getMessage()]
    assert [r.levelno for r in rejected] == [logging.WARNING]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    with ws_session.connections.group.lock:
        assert ws_session.connections.group.connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "rejected the API key" in row["LastError"]
    # Massive may answer `auth_failed` to a key whose plan has no streams of the class, too.
    assert row["LastError"].endswith(
        "; this can also mean the stocks plan includes no WebSocket streams for stocks, not "
        "only a bad key"
    )
    assert [s["State"] for s in streams_of(ws_session, dh_to_pylist)] == ["FAILED", "FAILED"]
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(ws_session.urls) == 2
    del result, other, again


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
    monkeypatch.setattr(ws_connection, "IDLE_GRACE", 0.05)
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


def test_a_missing_acknowledgement_shows_as_no_ack_and_keeps_the_connection(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    custom_session = make_ws_session(ack_timeout=0.05)
    fake_ws_server.silent.add("T.AAPL")
    quiet = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: s.opens == 1)
    wait_for(lambda: connections_of(custom_session, dh_to_pylist)[0]["NoAckTopics"] == 1)
    healthy = custom_session.query(TRADES, ["MSFT"]).blink()
    custom_session.subscribed("T.MSFT")
    # The server records the subscription before it answers: wait for its acknowledgement to
    # reach the client, or the row may not count it yet.
    custom_session.wait(lambda s: len(s.statuses) == 1)
    # The topic was sent, re-sent once, and is counted; the connection, and the other stream
    # on it, stays up.
    params = [json.loads(f)["params"] for f in fake_ws_server.frames(0)]
    assert params == ["T.AAPL", "T.AAPL", "T.MSFT"]
    [row] = connections_of(custom_session, dh_to_pylist)
    assert (row["State"], row["NoAckTopics"], row["AckedTopics"]) == ("CONNECTED", 1, 1)
    assert custom_session.opens == 1
    # The quiet stream says why its rows may be missing, without failing; the other is clean.
    errors = [(r["State"], r["Error"]) for r in streams_of(custom_session, dh_to_pylist)]
    assert errors == [
        (
            "RUNNING",
            (
                "no acknowledgement from Massive for T.AAPL; its rows may be missing; the "
                "subscription is re-sent until Massive acknowledges it"
            ),
        ),
        ("RUNNING", None),
    ]
    # A late acknowledgement clears it.
    fake_ws_server.send(status_frame("success", "subscribed to: T.AAPL"))
    custom_session.wait(lambda s: len(s.statuses) == 2)
    wait_for(lambda: streams_of(custom_session, dh_to_pylist)[0]["Error"] is None)
    [row] = connections_of(custom_session, dh_to_pylist)
    assert (row["NoAckTopics"], row["AckedTopics"]) == (0, 2)
    del quiet, healthy


def on_host(host: str, evidence: str = "test evidence") -> Callable[..., HostDecision]:
    return lambda *_: HostDecision(host, recency_of(host), evidence)


@pytest.mark.parametrize(
    ("decide", "hint"),
    [
        (
            on_host(REALTIME_HOST),
            (
                "the futures subscription's plan may not include this data in real time; if its "
                "data is delayed, use Massive's delayed futures feed: "
                "dm.MassiveConfig(delayed_subscriptions={dm.Subscription.FUTURES})"
            ),
        ),
        (
            # A snapshot without a label (a base Business key's) proves nothing about the
            # plan: its data may be served on another host, which ws_hosts pins.
            on_host(REALTIME_HOST, "no timeframe in the futures snapshot"),
            (
                "the futures snapshot carried no recency label, as a Business plan's without "
                "the trades and quotes expansion does, so the plan may serve this data on "
                "another host: pin it with MassiveConfig(ws_hosts={dm.AssetClass.FUTURES: "
                "\"<host>\"}); if the plan's data is delayed instead, use Massive's delayed "
                "futures feed: dm.MassiveConfig(delayed_subscriptions={dm.Subscription.FUTURES})"
            ),
        ),
        (
            # A refused snapshot is a Basic key's, which has no live data on either feed, so
            # the delayed feed is no advice.
            on_host(
                REALTIME_HOST,
                "the host check's snapshot (/futures/v1/snapshot) was refused: not entitled",
            ),
            (
                "Massive also refused the futures snapshot, as it does on the free Basic plan, "
                "which includes no live (WebSocket) futures data on either feed; read the data "
                "with fetch(), poll(), or history() instead"
            ),
        ),
        (
            # A key whose snapshot proved real-time data gains nothing from the delayed feed.
            on_host(REALTIME_HOST, "last_trade.timeframe=REAL-TIME"),
            (
                "the futures subscription's plan is real-time (last_trade.timeframe=REAL-TIME) "
                "but may not include this data; it may need a higher plan or an add-on"
            ),
        ),
        (
            on_host(DELAYED_HOST, "MassiveConfig.delayed_subscriptions"),
            (
                "MassiveConfig.delayed_subscriptions lists dm.Subscription.FUTURES, so its streams "
                "use Massive's delayed feed; whether Massive serves it to every plan is "
                "unverified; to use the real-time feed, remove dm.Subscription.FUTURES from "
                "delayed_subscriptions"
            ),
        ),
        (
            on_host("futures.example.com", "ws_hosts"),
            (
                "MassiveConfig.ws_hosts[AssetClass.FUTURES] pins this host; the plan may not "
                "include this data here"
            ),
        ),
    ],
)
def test_a_refused_step_fails_its_streams_and_names_the_feeds_fix(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    decide: Callable[..., HostDecision],
    hint: str,
) -> None:
    # A refusal never moves a connection: the step's streams fail, the working stream keeps
    # running, and the message says what to change for the connection's feed.
    custom_session = make_ws_session(decide=decide)
    first = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    second = custom_session.query(FUTURES, ["NQZ5"]).blink()
    custom_session.wait(lambda s: len(s.statuses) == 2)
    settle(custom_session)
    custom_session.flush()  # applies the failure
    wait_for(
        lambda: (
            [r["State"] for r in streams_of(custom_session, dh_to_pylist)] == ["RUNNING", "FAILED"]
        )
    )
    assert len(custom_session.urls) == 1
    [_, failed] = streams_of(custom_session, dh_to_pylist)
    assert "Massive said: Real-time data requires an upgrade; " + hint in failed["Error"]
    del first, second


def test_a_lost_race_closes_the_spare_connection(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2)
    built: list[ws_connection.Connection] = []
    built_lock = threading.Lock()
    real: Callable[..., ws_connection.Connection] = ws_connection.Connection

    def racing(*args: object, **kwargs: object) -> ws_connection.Connection:
        connection = real(*args, **kwargs)
        with built_lock:
            built.append(connection)
        barrier.wait(TIMEOUT)
        return connection

    monkeypatch.setattr(ws, "Connection", racing)
    monkeypatch.setattr(ws_connection, "Connection", racing)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda s: ws_session.query(TRADES, [s]).blink(), ["AAPL", "MSFT"]))
    assert len(built) == 2
    assert len(ws_session.connections.group.connections) == 1
    spare = next(c for c in built if c not in ws_session.connections.group.connections.values())
    assert spare.join(TIMEOUT)
    for result in results:
        ws_session.session.release(result)


def spare_connection(ws_session: WsSession, host: str) -> ws_connection.Connection:
    return ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        sessions=None,
    )


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
    monkeypatch.setattr(ws_connection, "_MESSAGE_ROW_NS", interval)
    monkeypatch.setattr(
        ws_connection, "_BACKLOG_ROW_DELAY", 10**9
    )  # the buffered rows' rewrite never fires
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
    monkeypatch.setattr(
        ws_connection, "_MESSAGE_ROW_NS", 10**18
    )  # every message row write is throttled

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
    write = ws_connection.Connection._write_row

    def counting(connection: ws_connection.Connection) -> None:
        written.append(1)
        write(connection)

    monkeypatch.setattr(ws_connection.Connection, "_write_row", counting)
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
    connection = ws_connection.Connection(
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
    ws_session.on_loop(connection, lambda: connection._record_drift("T", drift, dropped={}))
    assert written == []
    monkeypatch.undo()
    assert ws_session.on_loop(
        connection, lambda: ws_session.connections.group.retire(connection, idle=False)
    )
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


def test_the_default_factory_builds_real_clients(ws_session: WsSession) -> None:
    factory = ws_connection._WsClientFactory(
        "test-key", backoff=15.0, max_backoff=30.0, thread_prefix="massive #7"
    )
    client = factory(
        "wss://socket.test/stocks",
        sources=[],
        on_open=lambda *_: None,
        on_status=lambda *_: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
        reconnect_deadline=lambda: None,
    )
    assert isinstance(client, _massive.WsClient)
    assert client._thread.name == "massive #7 ws socket.test/stocks"
    client.close()
    client.start()
    assert client.join(TIMEOUT)
    assert "test-key" not in repr(factory)


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
    monkeypatch.setattr(ws_connection, "_MESSAGE_ROW_NS", 0)
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    ws_session.send(trade("AAPL"))
    assert written  # the message alone rewrote the row
    ws_session.session.release(result)


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
    looks: list[ws_connection.Connection | None] = []

    def watching(
        spec: DatasetSpec, connection: ws_connection.Connection | None, topics: frozenset[str]
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


def join(ws_session: WsSession) -> dm.MassiveSession:
    # A second session with the same key and settings, sharing the wrapper's connections; the
    # wrapper's own session is closed by its fixture, this one by the test.
    session = dm.MassiveSession(ws_session.config)
    connections = ws.WsConnections(
        session._core,
        resolve_host=ws_session._resolve,
        fetch_rows=ws_session._fetch_rows,
        crypto_pair=ws_session._crypto_pair,
        client_factory=ws_session.factory,
    )
    assert connections.group is ws_session.connections.group
    cast("Any", session._core._ports)._streams = connections
    session._core.on_close(connections.close)
    return session


def rows_of(session: dm.MassiveSession, to_pylist: ToPylist) -> list[dict[str, Any]]:
    assert session._core.diagnostics.wait_until_shown(TIMEOUT)
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
        assert session._core.diagnostics.wait_until_shown(TIMEOUT)
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


def _group_of(session: dm.MassiveSession) -> ws_connection.ConnectionGroup:
    transport = cast("ws.WsConnections", session._core.stream_transport())
    return transport.group


def test_sessions_with_equal_settings_share_one_group() -> None:
    first = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    second = dm.MassiveSession(dm.MassiveConfig(api_key="group-key"))
    try:
        shared = _group_of(first)
        assert _group_of(second) is shared
        assert "group-key" not in repr(shared.key)  # the key holds a digest, not the secret
        first.close()
        assert (
            ws_connection._groups.get(shared.key) is shared
        )  # the group stays with its last session
    finally:
        first.close()
        second.close()
    assert shared.key not in ws_connection._groups


# One value differing from the default for each connection setting.
OTHER_SETTINGS: dict[str, object] = {
    "api_key": "another-key",
    "ws_hosts": {AssetClass.STOCKS: "pinned.test"},
    "ack_timeout": 7.0,
    "reconnect_backoff": 45.0,
    "max_reconnect_backoff": 300.0,
    "max_outage": 60.0,
    "flush_interval": 0.2,
    "rest_base_url": "https://rest.example.com",
}


def test_every_connection_setting_has_a_case() -> None:
    assert set(OTHER_SETTINGS) == set(ws_connection.CONNECTION_SETTINGS)


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

    monkeypatch.setattr("deephaven_massive._ingest.ws_connection.FuturesCalendar", racing)
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

    monkeypatch.setattr("deephaven_massive._ingest.ws_connection.FuturesCalendar", closing)
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
    monkeypatch.setattr(ws_connection, "_BACKLOG_ROW_DELAY", 0.01)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(t.message == "subscribed to: T.AAPL" for t in s.statuses))
    connection = ws_session.connection()
    monkeypatch.setattr(ws_connection, "_MESSAGE_ROW_NS", 10**18)  # messages never rewrite the row
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
    monkeypatch.setattr(ws_connection, "_BACKLOG_ROW_DELAY", 0.05)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    monkeypatch.setattr(ws_connection, "_MESSAGE_ROW_NS", 0)  # every message rewrites the row
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


_URL = "wss://socket.test/stocks"


def test_dropped_rows_are_counted_by_their_canonical_symbol(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    error = _massive.WireError("dropped")
    rejects = [
        _massive.Reject(channel="T", symbol=symbol, error=error, raw="{}")
        for symbol in ("BRK.A", "BRK.A", "MSFT", None, "not a symbol")
    ]
    assert connection._dropped_symbols(rejects) == {"BRK.A": 2, "MSFT": 1}
    del result


def test_the_connection_logs_drift_dropped_rows_and_a_summary(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    other = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    connection = ws_session.connection()
    first = ws_session.on_loop(connection, lambda: connection._summary)
    assert first is not None  # the first generation started the summary's timer
    timeless = trade("AAPL")
    del timeless["t"]
    with caplog.at_level(logging.DEBUG):
        ws_session.send(trade("AAPL", zz=1), trade("AAPL", p="bad"), timeless)
        ws_session.flush()
        ws_session.on_loop(connection, connection._summarize)
    # Each summary starts the next window's timer.
    assert ws_session.on_loop(connection, lambda: connection._summary) is not first
    assert f"(schema drift) at {_URL} channel T; symbol AAPL; first value 1" in caplog.text
    [dropped] = [m for m in caplog.messages if m.startswith("dropped a T row for AAPL")]
    assert dropped.endswith('"q":1,"s":100,"z":3,"ds":"100"}')
    [summary] = [m for m in caplog.messages if m.startswith(f"{_URL}: last 60 s: ")]
    # The odd rows failed no stream: stream 1 got both rows it could read.
    assert summary.startswith(
        f"{_URL}: last 60 s: 1 data batches, rows T=2, 1 dropped; rows per stream 1 [1=2], "
        "streams with no rows 1 [2]; 2 of 2 topics acknowledged 2 [T.AAPL, T.MSFT], added 2 "
        "[T.AAPL, T.MSFT], removed 0 [], 0 pending, 0 with no acknowledgement; last data T "
    )
    assert summary.endswith(" s ago; state CONNECTED, 0 reconnects")
    # The next window starts empty and counts each stream's rows of the window only.
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        ws_session.send(trade("MSFT"))
        ws_session.flush()
        ws_session.on_loop(connection, connection._summarize)
    [summary] = [m for m in caplog.messages if m.startswith(f"{_URL}: last 60 s: ")]
    assert "1 data batches, rows T=1, 0 dropped; rows per stream 1 [2=1], " in summary
    assert "streams with no rows 1 [1]; " in summary
    assert "acknowledged 2 [T.AAPL, T.MSFT], added 0 [], removed 0 [], " in summary
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        ws_session.on_loop(connection, connection._summarize)
    assert "0 data batches, rows none, 0 dropped; rows per stream 0 [], " in caplog.text
    assert "streams with no rows 2 [1, 2]; " in caplog.text
    # A topic that left since the last summary is named.
    caplog.clear()
    ws_session.session.release(other)
    ws_session.unsubscribed("T.MSFT")
    with caplog.at_level(logging.DEBUG):
        ws_session.on_loop(connection, connection._summarize)
    assert "1 of 1 topics acknowledged 1 [T.AAPL], added 0 [], removed 1 [T.MSFT], " in caplog.text
    caplog.clear()
    with caplog.at_level(logging.INFO):
        ws_session.on_loop(connection, connection._summarize)
    assert "last 60 s" not in caplog.text  # with DEBUG off the summary is not logged
    del result


def test_a_lost_connection_warns_once_per_outage_and_its_reconnect_is_info(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    with (
        caplog.at_level(logging.INFO, logger=ws.__name__),
        caplog.at_level(logging.INFO, logger=ws_connection.__name__),
    ):
        fake_ws_server.close(1011)
        ws_session.wait(lambda s: s.opens == 2)
        ws_session.subscribed("T.AAPL")
    ours = [
        (r.levelno, r.getMessage())
        for r in caplog.records
        if r.name in {ws.__name__, ws_connection.__name__}
    ]
    down = [entry for entry in ours if "the connection is down" in entry[1]]
    assert [level for level, _ in down] == [logging.WARNING]
    assert re.fullmatch(
        rf"{re.escape(_URL)}: the connection is down \(reconnecting\); its 1 live tables get no "
        r"data until it reconnects, next attempt in \d+ s; m\.connections\(\) shows its state",
        down[0][1],
    )
    back = [entry for entry in ours if "reconnected after" in entry[1]]
    assert [level for level, _ in back] == [logging.INFO]
    # The socket is back, but no data has arrived on it yet.
    assert re.fullmatch(rf"{re.escape(_URL)}: reconnected after \d+ s; re-subscribing", back[0][1])
    ws_session.session.release(result)


def test_an_outage_names_a_full_account_and_logs_once(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    with (
        caplog.at_level(logging.INFO, logger=ws.__name__),
        caplog.at_level(logging.INFO, logger=ws_connection.__name__),
    ):
        for wait in (30.0, 60.0):  # two attempts of one outage
            ws_session.on_loop(
                connection,
                functools.partial(connection._on_state, WsState.WAITING_SLOT, time.time() + wait),
            )
    down = [r for r in caplog.records if "the connection is down" in r.getMessage()]
    assert [r.levelno for r in down] == [logging.WARNING]
    assert "is down (the account has no free connection, max_connections)" in down[0].getMessage()
    ws_session.session.release(result)


def test_an_outage_that_comes_to_wait_for_a_slot_warns_of_the_full_account_once(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    with caplog.at_level(logging.INFO, logger=ws_connection.__name__):
        # A reconnect first, then two waits for a free connection slot, all one outage.
        for state in (WsState.RECONNECTING, WsState.WAITING_SLOT, WsState.WAITING_SLOT):
            ws_session.on_loop(
                connection, functools.partial(connection._on_state, state, time.time() + 30.0)
            )
    down = [r.getMessage() for r in caplog.records if "the connection is down" in r.getMessage()]
    assert [m.split(";")[0] for m in down] == [
        f"{_URL}: the connection is down (reconnecting)",
        f"{_URL}: the connection is down (the account has no free connection, max_connections)",
    ]
    ws_session.session.release(result)


def test_the_options_cap_warns_once_per_episode_with_its_changes_at_debug(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()

    def note(old: int, new: int) -> None:
        stream = next(iter(connection._streams.values()))
        connection._log_note(stream, count=new, old_count=old, missing=frozenset())

    with caplog.at_level(logging.INFO, logger=ws_connection.__name__):
        for old, new in ((0, 2), (2, 3), (3, 0)):
            ws_session.on_loop(connection, functools.partial(note, old, new))
    levels = [r.levelno for r in caplog.records if "stream 1:" in r.getMessage()]
    # Deferred from none to some warns; a change of the count logs only at DEBUG; none
    # deferred again is the recovery, at INFO.
    assert levels == [logging.WARNING, logging.INFO]
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=ws_connection.__name__):
        ws_session.on_loop(connection, functools.partial(note, 2, 3))
    assert [
        (r.levelno, r.getMessage()) for r in caplog.records if "deferred, " in r.getMessage()
    ] == [
        (
            logging.DEBUG,
            (
                "test.trades stream 1: 3 contracts deferred, 2 before; over capacity: 1/1000; "
                "0 deferred"
            ),
        )
    ]
    ws_session.session.release(result)


_DOWN = r"connection down since (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ); no data"
"""The pattern of the `Error` of a stream whose connection is down; its group is the start."""


_NO_ACK = r"no acknowledgement from Massive for T\.\w+; its rows may be missing; .*"
"""The pattern of the `Error` part of a stream with a topic Massive never acknowledged."""


def _outage_failure(since: str, limit: str) -> str:
    # The message a stream fails with once the outage held it down for `max_outage`.
    return (
        f"the WebSocket connection to socket.test/stocks has been down since {since}, longer "
        f"than MassiveConfig.max_outage ({limit} s); the rows from {since} on are missing, so "
        "this table stopped; call the verb again for a new live table"
    )


def _ack(topic: str) -> str:
    # The acknowledgement of one topic, as Massive sends it.
    return json.dumps([{"ev": "status", "status": "success", "message": f"subscribed to: {topic}"}])


def _acks(ws_session: WsSession) -> int:
    return sum(status.kind is StatusKind.ACK for status in ws_session.statuses)


def _down(ws_session: WsSession, connection: ws_connection.Connection) -> set[int]:
    # The streams the outage holds down, as the connection's loop sees them.
    return cast("set[int]", ws_session.on_loop(connection, lambda: set(connection._episodes.down)))


def _down_since(ws_session: WsSession, connection: ws_connection.Connection) -> float | None:
    # The outage's start as the connection's loop holds it.
    return cast(
        "float | None", ws_session.on_loop(connection, lambda: connection._episodes.down_since)
    )


def _left(ws_session: WsSession, connection: ws_connection.Connection, stream_id: int) -> float:
    # Seconds until a down stream's timer fails it.
    def left() -> float:
        timer = cast("Any", connection._episodes.down[stream_id])
        return cast("float", timer.when() - connection._client.loop.time())

    return cast("float", ws_session.on_loop(connection, left))


def _expire(ws_session: WsSession, connection: ws_connection.Connection, *ids: int) -> None:
    # What the clock reaching each stream's max_outage does: its timer's callback, then a flush.
    def expire() -> None:
        for stream_id in ids or list(connection._episodes.down):
            cast("Any", connection._episodes.down[stream_id]).cancel()
            connection._stream_expired(stream_id)

    ws_session.on_loop(connection, expire)
    ws_session.flush()


def _since(error: str) -> str:
    # The outage's start named in a failure.
    return cast("re.Match[str]", re.search(r"since (\S+),", error)).group(1)


def test_an_outage_shows_until_the_reconnected_topics_are_acknowledged(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.wait(lambda s: _acks(s) == 1)
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL"}  # the reconnect's subscription stays unanswered
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    fake_ws_server.wait_for(lambda s: "T.AAPL" in s.connections[-1].topics)
    [row] = streams_of(ws_session, dh_to_pylist)
    # Reconnected, but no data flows until the topic is acknowledged: still an outage.
    down = re.fullmatch(_DOWN, row["Error"])
    assert down is not None
    assert (row["State"], row["Gaps"]) == ("RUNNING", None)
    assert 280.0 < _left(ws_session, connection, 1) <= 300.0  # the default: five minutes
    with caplog.at_level(logging.INFO, logger=ws_connection.__name__):
        fake_ws_server.send(_ack("T.AAPL"))
        wait_for(lambda: streams_of(ws_session, dh_to_pylist)[0]["Error"] is None)
        assert _down(ws_session, connection) == set()
        assert _down_since(ws_session, connection) is None  # the outage is over
    # The note is gone, but the gap it announced stays on the row: from the last frame
    # before the drop to the data's return.
    gaps = streams_of(ws_session, dh_to_pylist)[0]["Gaps"]
    assert re.fullmatch(rf"{down.group(1)} to \d{{4}}-\d\d-\d\dT\d\d:\d\d:\d\dZ", gaps)
    # Its real end, once data flows again, is logged at INFO.
    (over,) = [r for r in caplog.records if "is over" in r.getMessage()]
    assert over.levelno == logging.INFO
    assert re.fullmatch(
        rf"{re.escape(_URL)}: the outage that began at \S+Z is over after \d+ s: Massive "
        "confirmed the subscriptions of its 1 live tables, or sent them rows",
        over.getMessage(),
    )
    assert streams_of(ws_session, dh_to_pylist)[0]["State"] == "RUNNING"
    ws_session.session.release(result)


def test_an_outage_past_max_outage_fails_every_stream_and_its_derived_tables(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    other = ws_session.query(TRADES, ["MSFT"]).last_by()
    ws_session.subscribed("T.MSFT")
    derived = result.last_by("Symbol")  # a table built on the stream
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL", "T.MSFT"}  # the outage outlasts the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        _expire(ws_session, connection)
    wait_for(lambda: bool(result.j_table.isFailed() and other.j_table.isFailed()))
    wait_for(lambda: bool(derived.j_table.isFailed()))
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["FAILED", "FAILED"]
    since = _since(rows[0]["Error"])
    assert [r["Error"] for r in rows] == [_outage_failure(since, "300")] * 2
    # An expected failure: one WARNING for both streams, never an ERROR.
    failures = [r for r in caplog.records if "longer than MassiveConfig.max_outage" in r.message]
    assert [r.levelno for r in failures] == [logging.WARNING]


def test_a_zero_max_outage_fails_every_stream_at_the_first_disconnect(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    custom_session = make_ws_session(max_outage=0)
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.subscribed("T.AAPL")
    connection = custom_session.connection()
    fake_ws_server.close(1011)
    custom_session.wait(lambda s: WsState.RECONNECTING in s.states)

    def fired() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._episodes.down_since is not None and not connection._episodes.down
                ),
            )
        )

    wait_for(fired)  # the zero-second timer fires on the loop's next turn
    custom_session.flush()
    wait_for(lambda: bool(result.j_table.isFailed()))
    [row] = streams_of(custom_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("FAILED", _outage_failure(_since(row["Error"]), "0"))


def test_a_server_that_never_acknowledges_stays_down_until_each_stream_gets_data(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    custom_session = make_ws_session(ack_timeout=0.05)
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    other = custom_session.query(TRADES, ["MSFT"]).blink()
    custom_session.subscribed("T.AAPL", "T.MSFT")
    custom_session.wait(lambda s: _acks(s) == 2)
    custom_session.publish(trade("AAPL"), trade("MSFT"))  # rows that could look current
    connection = custom_session.connection()
    fake_ws_server.silent = {"T.AAPL", "T.MSFT"}
    fake_ws_server.close(1011)

    def silent_but_connected() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._state is ConnectionState.CONNECTED
                    and connection.subscriptions.counts()[3] == 2
                ),
            )
        )

    wait_for(silent_but_connected)  # NO_ACK: nothing waits for a reply, yet nothing arrived
    assert _down(custom_session, connection) == {1, 2}
    errors = [r["Error"] for r in streams_of(custom_session, dh_to_pylist)]
    # Two warnings, each its own part: the outage, and the connection's no-ack note.
    assert all(re.fullmatch(f"{_DOWN}; {_NO_ACK}", e) for e in errors)

    def data_for(symbol: str) -> None:
        # The flush that gives the stream its first rows of the new socket checks recovery
        # at once, without waiting for the next data frame or the summary.
        custom_session.publish(trade(symbol))

    data_for("AAPL")  # AAPL's data flows again; MSFT's does not
    assert _down(custom_session, connection) == {2}
    errors = [r["Error"] for r in streams_of(custom_session, dh_to_pylist)]
    # Clearing the outage's part keeps the no-ack part, which only its source clears.
    assert re.fullmatch(_NO_ACK, errors[0])
    assert re.fullmatch(f"{_DOWN}; {_NO_ACK}", errors[1])
    data_for("MSFT")
    errors = [r["Error"] for r in streams_of(custom_session, dh_to_pylist)]
    assert all(re.fullmatch(_NO_ACK, e) for e in errors)
    assert _down_since(custom_session, connection) is None
    custom_session.session.release(result)
    custom_session.session.release(other)


def test_rows_of_the_dropped_socket_flushed_late_never_recover_a_stream(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
) -> None:
    # A stalled flush publishes rows from the dropped socket after the reconnect. They prove
    # nothing about the new socket, which never acknowledges AAPL, so the stream stays down.
    custom_session = make_ws_session(ack_timeout=0.05)
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.subscribed("T.AAPL")
    custom_session.publish(trade("AAPL"))  # rows that could look current
    connection = custom_session.connection()
    custom_session.send(trade("AAPL"))  # received on the first socket, not flushed yet
    fake_ws_server.silent = {"T.AAPL"}
    fake_ws_server.close(1011)

    def no_ack_after_reconnect() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._state is ConnectionState.CONNECTED
                    and connection.subscriptions.counts()[3] == 1
                ),
            )
        )

    custom_session.wait(lambda s: s.opens == 2)
    wait_for(no_ack_after_reconnect)
    custom_session.flush()  # the late rows of the first socket
    custom_session.on_loop(connection, connection._check_recovered)
    assert _down(custom_session, connection) == {1}
    custom_session.publish(trade("AAPL"))  # a row of the new socket
    assert _down(custom_session, connection) == set()
    custom_session.session.release(result)


def test_a_topic_never_acknowledged_holds_down_only_its_stream(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    custom_session = make_ws_session(ack_timeout=0.05)
    result = custom_session.query(TRADES, ["AAPL"]).last_by()
    other = custom_session.query(TRADES, ["MSFT"]).last_by()
    custom_session.subscribed("T.AAPL", "T.MSFT")
    custom_session.wait(lambda s: _acks(s) == 2)
    custom_session.publish(trade("MSFT"))  # MSFT's last_by now holds a row
    connection = custom_session.connection()
    fake_ws_server.silent = {"T.MSFT"}
    fake_ws_server.close(1011)

    def msft_no_ack() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._state is ConnectionState.CONNECTED
                    and connection.subscriptions.counts()[3] == 1
                ),
            )
        )

    wait_for(msft_no_ack)
    # AAPL is acknowledged again: it recovers. MSFT never is: its last_by would show the rows
    # from before the drop as current, so it stays down and fails at its limit.
    assert _down(custom_session, connection) == {2}
    errors = [r["Error"] for r in streams_of(custom_session, dh_to_pylist)]
    assert errors[0] is None
    assert re.fullmatch(f"{_DOWN}; {_NO_ACK}", errors[1])
    _expire(custom_session, connection, 2)
    wait_for(lambda: bool(other.j_table.isFailed()))
    rows = streams_of(custom_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert rows[0]["Error"] is None
    assert not result.j_table.isFailed()
    custom_session.session.release(result)


def test_a_stream_that_never_received_a_row_runs_on_with_its_topic_unacknowledged(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # Before the drop and after the reconnect, Massive never confirms MSFT. Its table has no
    # row that could look current, so it keeps running with its no-ack note.
    custom_session = make_ws_session(ack_timeout=0.05)
    result = custom_session.query(TRADES, ["MSFT"]).last_by()
    custom_session.subscribed("T.MSFT")
    connection = custom_session.connection()
    fake_ws_server.silent = {"T.MSFT"}
    fake_ws_server.close(1011)

    def no_ack_after_reconnect() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._state is ConnectionState.CONNECTED
                    and connection.subscriptions.counts()[3] == 1
                ),
            )
        )

    custom_session.wait(lambda s: s.opens == 2)
    wait_for(no_ack_after_reconnect)
    custom_session.on_loop(connection, connection._check_recovered)
    assert _down(custom_session, connection) == set()
    [row] = streams_of(custom_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    assert re.fullmatch(_NO_ACK, row["Error"])
    custom_session.session.release(result)


def test_a_connection_that_never_connected_gives_its_streams_the_full_limit(
    ws_session: WsSession,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()

    def never_heard_from() -> None:
        connection._episodes.last_frame_at = 0
        connection._last_message_at = None
        connection._on_state(WsState.RECONNECTING, time.time() + 30.0)

    ws_session.on_loop(connection, never_heard_from)
    assert 290.0 < _left(ws_session, connection, 1) <= 300.0
    ws_session.session.release(result)


def test_a_stream_the_outage_failed_is_not_held_down_again(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    attempt = functools.partial(connection._on_state, WsState.RECONNECTING, time.time() + 30.0)
    ws_session.on_loop(connection, attempt)
    assert _down(ws_session, connection) == {1}

    def expire_then_retry() -> set[int]:
        # In one loop turn, before the failed table's shutdown releases the stream.
        cast("Any", connection._episodes.down[1]).cancel()
        connection._stream_expired(1)
        attempt()  # the next attempt of the same outage
        return set(connection._episodes.down)

    assert ws_session.on_loop(connection, expire_then_retry) == set()
    ws_session.flush()
    wait_for(lambda: bool(result.j_table.isFailed()))
    ws_session.session.release(result)


def test_a_stream_covered_by_its_channels_wildcard_recovers_with_it(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # Once the wildcard is acknowledged, the channel's explicit topics leave the wire: AAPL's
    # rows come through `T.*`, so its stream recovers with it, acknowledged or not.
    every = ws_session.query(TRADES, dm.ALL).blink()
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.*")
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL"}
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    wait_for(lambda: [r["Error"] for r in streams_of(ws_session, dh_to_pylist)] == [None, None])
    assert _down(ws_session, connection) == set()
    ws_session.session.release(every)
    ws_session.session.release(result)


def test_a_quiet_spell_before_a_drop_leaves_a_full_limit(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # No frame for ten minutes (a quiet night: keepalive pings are no frames), then a drop.
    # The note names when the data stopped, but the timer counts from the drop less the
    # keepalive window, so the stream survives a reconnect.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.wait(lambda s: _acks(s) == 1)
    connection = ws_session.connection()
    stopped = time.time_ns() - 600 * 10**9

    def frames_stopped_long_ago() -> None:
        connection._episodes.last_frame_at = stopped
        connection._last_message_at = None

    fake_ws_server.silent = {"T.AAPL"}  # hold the reconnect's acknowledgement for a look
    ws_session.on_loop(connection, frames_stopped_long_ago)
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    assert _down_since(ws_session, connection) == stopped / 1e9
    window = ws_connection.KEEPALIVE_WINDOW
    assert 290.0 - window < _left(ws_session, connection, 1) <= 300.0 - window
    fake_ws_server.send(_ack("T.AAPL"))  # the acknowledgement arrives
    wait_for(lambda: streams_of(ws_session, dh_to_pylist)[0]["Error"] is None)
    ws_session.flush()
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    assert not result.j_table.isFailed()
    ws_session.session.release(result)


def test_a_reconnect_that_drops_before_every_topic_is_acknowledged_keeps_the_outage(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    other = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    ws_session.wait(lambda s: _acks(s) == 2)
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.MSFT"}
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    ws_session.wait(lambda s: _acks(s) == 3)  # AAPL acknowledged again, MSFT not
    first = _down_since(ws_session, connection)
    assert first is not None
    msft = _left(ws_session, connection, 2)
    fake_ws_server.close(1011)  # drops again before MSFT's acknowledgement
    ws_session.wait(lambda s: s.opens == 3)
    assert _down_since(ws_session, connection) == first  # one outage, from the first gap
    assert _left(ws_session, connection, 2) < msft  # MSFT keeps its timer
    ws_session.session.release(result)
    ws_session.session.release(other)


def test_the_reconnect_deadline_is_the_earliest_held_down_stream_less_the_ack_timeout(
    *, ws_session: WsSession, fake_ws_server: FakeMassiveServer
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    deadline = connection._reconnect_deadline
    assert ws_session.on_loop(connection, deadline) is None  # nothing held down: no limit
    fake_ws_server.silent = {"T.AAPL"}  # the reconnect's subscription stays unanswered
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    left = _left(ws_session, connection, 1)
    seen = cast("float", ws_session.on_loop(connection, deadline))
    # The stream's time left, less the 5 s acknowledgement timeout, read a moment later.
    assert left - 5.0 - 1.0 < seen <= left - 5.0
    ws_session.session.release(result)


def test_a_stream_opened_during_an_outage_gets_its_own_limit(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL", "T.MSFT", "T.IBM"}
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    _expire(ws_session, connection)
    wait_for(lambda: bool(result.j_table.isFailed()))
    late = ws_session.query(TRADES, ["MSFT"]).blink()  # stream 2, on the still-down connection
    wait_for(lambda: 2 in _down(ws_session, connection))
    assert 290.0 < _left(ws_session, connection, 2) <= 300.0  # a full max_outage of its own
    assert re.fullmatch(_DOWN, streams_of(ws_session, dh_to_pylist)[1]["Error"])
    released = ws_session.query(TRADES, ["IBM"]).blink()  # stream 3: released before its limit
    wait_for(lambda: 3 in _down(ws_session, connection))
    ws_session.session.release(released)
    del released
    wait_for(lambda: 3 not in _down(ws_session, connection))
    _expire(ws_session, connection, 2)
    wait_for(lambda: bool(late.j_table.isFailed()))
    row = streams_of(ws_session, dh_to_pylist)[1]
    assert (row["State"], row["Error"]) == ("FAILED", _outage_failure(_since(row["Error"]), "300"))


def test_a_stream_opened_during_an_outage_recovers_with_the_feed(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL", "T.MSFT"}
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    _expire(ws_session, connection)
    wait_for(lambda: bool(result.j_table.isFailed()))
    # The failed stream's topic leaves; with no topic left, nothing proves the feed is back.
    wait_for(lambda: not ws_session.on_loop(connection, lambda: 1 in connection._streams))
    assert _down_since(ws_session, connection) is not None
    late = ws_session.query(TRADES, ["MSFT"]).blink()
    fake_ws_server.wait_for(lambda s: "T.MSFT" in s.connections[-1].topics)
    fake_ws_server.send(_ack("T.MSFT"))  # the feed is back before the late stream's limit
    wait_for(lambda: streams_of(ws_session, dh_to_pylist)[1]["Error"] is None)
    assert _down(ws_session, connection) == set()
    assert _down_since(ws_session, connection) is None
    assert streams_of(ws_session, dh_to_pylist)[1]["State"] == "RUNNING"
    ws_session.session.release(late)


def test_an_outage_without_live_tables_logs_nothing(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(result)
    ws_session.unsubscribed("T.AAPL")
    with (
        caplog.at_level(logging.INFO, logger=ws.__name__),
        caplog.at_level(logging.INFO, logger=ws_connection.__name__),
    ):
        fake_ws_server.close(1011)
        ws_session.wait(lambda s: s.opens == 2)
    messages = [
        r.getMessage() for r in caplog.records if r.name in {ws.__name__, ws_connection.__name__}
    ]
    assert not [m for m in messages if "is down" in m or "reconnected after" in m]


def _quiet_connection(
    acknowledged: frozenset[str], last: int, notes: list[str | None] | None = None
) -> ws_connection.Connection:
    # The state `Connection._check_frames` reads, without a socket: a live connection's status
    # frames would move its last-frame time while the test owns it. Each note the streams
    # would show goes to `notes`.
    state = SimpleNamespace(
        url=_URL,
        asset_class=AssetClass.STOCKS,
        _state=ConnectionState.CONNECTED,
        subscriptions=SimpleNamespace(acknowledged=acknowledged),
        _episodes=ws_connection._Episodes(last_frame_at=last),
        _last_message_at=None,
        _set_quiet_note=(notes if notes is not None else []).append,
    )
    for name in ("_check_frames", "_heard"):
        method = getattr(ws_connection.Connection, name)
        setattr(state, name, functools.partial(method, cast("ws_connection.Connection", state)))
    return cast("ws_connection.Connection", state)


def test_a_silent_connection_warns_once_while_its_market_trades(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    trading = [False]
    monkeypatch.setattr(ws_connection, "in_trading_hours", lambda _asset_class, _moment: trading[0])
    last = 1_800_000_000 * 10**9
    notes: list[str | None] = []
    connection = _quiet_connection(frozenset({"T.AAPL"}), last, notes)
    quiet = last + int(ws_connection.NO_FRAMES_WARNING * 1e9)
    with (
        caplog.at_level(logging.INFO, logger=ws.__name__),
        caplog.at_level(logging.INFO, logger=ws_connection.__name__),
    ):
        connection._check_frames(quiet)  # silent long enough, but the market is closed
        trading[0] = True
        connection._check_frames(quiet - 10**9)  # not silent long enough yet
        connection._check_frames(quiet)
        connection._check_frames(quiet + 60 * 10**9)  # the same episode: logged once
        connection._heard()  # a frame arrives: the recovery is logged at once
        connection._heard()  # and only once
    assert [
        (r.levelno, r.getMessage())
        for r in caplog.records
        if r.name in {ws.__name__, ws_connection.__name__}
    ] == [
        (
            logging.WARNING,
            (
                f"{_URL}: no frame for 300 s during stocks trading hours, though 1 topics are "
                "acknowledged, so its live tables may be stale; a market holiday or symbols "
                "that are not trading explain it, and m.connections() shows the connection's "
                "state"
            ),
        ),
        (logging.INFO, f"{_URL}: frames arrive again"),
    ]
    # The streams show the silence while it lasts, but keep running: nothing fails them.
    assert notes == [
        (
            "no data since 2027-01-15T08:00:00Z during stocks trading hours; quiet symbols or "
            "a market holiday may explain it"
        ),
        None,
    ]


def test_any_frame_clears_the_silence_note_and_the_stream_keeps_running(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    monkeypatch.setattr(ws_connection, "in_trading_hours", lambda _asset_class, _moment: True)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.wait(lambda s: any(status.kind is StatusKind.ACK for status in s.statuses))
    connection = ws_session.connection()
    later = int(2 * ws_connection.NO_FRAMES_WARNING * 1e9)

    def error() -> str | None:
        [row] = streams_of(ws_session, dh_to_pylist)
        assert row["State"] == "RUNNING"  # silence never fails a stream
        return cast("str | None", row["Error"])

    for frame in (_ack("T.AAPL"), json.dumps([trade("AAPL")])):  # a status, then data
        ws_session.on_loop(connection, lambda: connection._check_frames(time.time_ns() + later))
        assert re.fullmatch(
            r"no data since \S+Z during stocks trading hours; quiet symbols or a market holiday "
            r"may explain it",
            error() or "",
        )
        fake_ws_server.send(frame)
        wait_for(lambda: error() is None)
    ws_session.session.release(result)


def test_a_connection_with_nothing_acknowledged_is_never_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ws_connection, "in_trading_hours", lambda _asset_class, _moment: True)
    connection = _quiet_connection(frozenset(), 0)
    with (
        caplog.at_level(logging.INFO, logger=ws.__name__),
        caplog.at_level(logging.INFO, logger=ws_connection.__name__),
    ):
        connection._check_frames(int(2 * ws_connection.NO_FRAMES_WARNING * 1e9))
    assert not caplog.records


def test_the_summary_checks_for_silence(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    checked: list[int] = []
    monkeypatch.setattr(connection, "_check_frames", checked.append)
    ws_session.on_loop(connection, connection._summarize)
    assert len(checked) == 1
    ws_session.session.release(result)


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


def test_a_refused_fmv_topic_needs_a_business_plan(ws_session: WsSession) -> None:
    # FMV is never pinned, so the refusal names the plan, not `ws_hosts`.
    connection = ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url="wss://business.massive.com/forex",
        asset_class=AssetClass.FOREX,
        family=ChannelFamily.FMV,
        decision=HostDecision("business.massive.com", Recency.REALTIME, "FMV is Business-only"),
        sessions=None,
    )
    connection._refused(("FMV.C:EURUSD",), "not authorized")
    assert connection._last_error is not None
    assert connection._last_error.endswith(
        "fair-market-value (FMV) data needs a Business plan of the currencies subscription; "
        "MassiveConfig.ws_hosts never applies to it"
    )
    connection.close()
    assert connection.join(TIMEOUT)
