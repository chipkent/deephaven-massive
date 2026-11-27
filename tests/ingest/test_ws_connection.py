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
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, TypeAlias

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._config import DELAYED_HOST, REALTIME_HOST
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.feeds import channel_only_host
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars, Recency
from deephaven_massive._errors import EntitlementError, FeedRefusalError, MassiveError
from deephaven_massive._ingest import futures_calendar, ws, ws_connection, ws_silence
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._log_limits import log_failure
from deephaven_massive._massive import Cancel, StatusKind, Transport, WsState
from deephaven_massive._tables import stream
from deephaven_massive._tables.diagnostics import ConnectionState, Diagnostics, utc_text
from deephaven_massive._tables.problem_events import Problem, ProblemCause, ProblemSpans
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.stream import StreamSink
from deephaven_massive._tables.transports import HostDecision
from tests._fakes.ws import FakeMassiveServer, status_frame
from tests._fakes.ws_session import REALTIME, TIMEOUT, WsSession

RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)

ToPylist: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"

W = Transport.WS

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))

_GROUP_CLOSED = re.escape(
    "Every session that shares these WebSocket connections has closed. Open a new "
    "dm.MassiveSession() to read again."
)
"""The whole message of a futures lookup once every session sharing the connections closed."""

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
    channel_family=ChannelFamily.FMV,
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


OUTAGE = ProblemCause.OUTAGE.value
NO_ACK = ProblemCause.UNACKNOWLEDGED_TOPICS.value
QUIET = ProblemCause.QUIET_FEED.value
DRIFT = ProblemCause.SCHEMA_DRIFT.value


def problem_rows(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], cause: str
) -> list[dict[str, Any]]:
    # The session's m.problems() rows of one cause, by stream and then oldest first.
    settle(ws_session)
    assert ws_session.session._core.diagnostics.wait_until_shown(TIMEOUT)
    rows = [r for r in dh_to_pylist(ws_session.session.problems()) if r["Cause"] == cause]
    return sorted(rows, key=lambda r: (r["StreamId"], r["FirstSeen"]))


def spans_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> dict[tuple[int, str], str]:
    # The session's spans that hold now, by stream and cause, with their detail.
    settle(ws_session)
    assert ws_session.session._core.diagnostics.wait_until_shown(TIMEOUT)
    return {
        (r["StreamId"], r["Cause"]): r["Detail"]
        for r in dh_to_pylist(ws_session.session.problems())
        if ProblemCause(r["Cause"]).traits.span and r["EndedAt"] is None
    }


def ended(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]], cause: str
) -> list[tuple[int, bool]]:
    # Each span of one cause as (stream, ended), by stream and then oldest first.
    return [
        (r["StreamId"], r["EndedAt"] is not None)
        for r in problem_rows(ws_session, dh_to_pylist, cause)
    ]


def utc(stamp: datetime) -> str:
    # A timestamp of m.problems() as the library's UTC text, such as "2027-01-15T08:01:00Z".
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


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
    rejected = [r for r in caplog.records if "status auth_failed" in r.getMessage()]
    assert [r.levelno for r in rejected] == [logging.WARNING]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    with ws_session.connections.group.lock:
        assert ws_session.connections.group.connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    # Massive may answer `auth_failed` to a key whose plan has no streams of the class, too.
    assert row["LastError"] == (
        "Massive answered the authentication at 127.0.0.1/stocks with status auth_failed: "
        '"authentication failed". Massive gives this answer for a wrong key and for a key with '
        "no WebSocket streams of stocks, and its text tells which."
    )
    assert [s["State"] for s in streams_of(ws_session, dh_to_pylist)] == ["FAILED", "FAILED"]
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(ws_session.urls) == 2
    del result, other, again


def test_a_streams_copy_of_a_connection_error_names_its_dataset_and_is_a_logged_repeat(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Each dataset's streams get their own copy, prefixed, with the error's class and
    # attributes. Every copy logs at DEBUG only, whether or not the connection has logged the
    # error yet, and the connection's own log of it stays at WARNING: the alarm never depends
    # on which stream fails first, or on whether that stream's session is still open.
    error = dm.EntitlementError("Massive answered no.", status=403, request_id="r1")
    log = logging.getLogger("test.stream_copy")
    with caplog.at_level(logging.DEBUG, logger=log.name):
        first = ws_connection.stream_copy(error, "test.trades")
        second = ws_connection.stream_copy(error, "test.quotes")
        assert str(first) == "test.trades: Massive answered no."
        assert str(second) == "test.quotes: Massive answered no."
        assert type(first) is dm.EntitlementError
        assert (first.status, first.request_id, first.source) == (403, "r1", error.source)
        assert not log_failure(log, first, "%s")  # a repeat, though nobody logged the error
        assert not log_failure(log, second, "%s")
        assert log_failure(log, error, "%s")  # the connection's own log is the WARNING
        assert not log_failure(log, ws_connection.stream_copy(error, "test.trades"), "%s")
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.DEBUG, "test.trades: Massive answered no."),
        (logging.DEBUG, "test.quotes: Massive answered no."),
        (logging.WARNING, "Massive answered no."),
        (logging.DEBUG, "test.trades: Massive answered no."),
    ]


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
    assert row["LastError"] == (
        "Massive answered the WebSocket connection to 127.0.0.1/stocks with HTTP 404."
    )
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
    assert (row["State"], row["Reconnects"], row["Topics"], row["ConfirmedTopics"]) == (
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
    custom_session = make_ws_session(subscription_confirmation_timeout=0.05)
    fake_ws_server.silent.add("T.AAPL")
    quiet = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: s.opens == 1)
    wait_for(lambda: connections_of(custom_session, dh_to_pylist)[0]["UnconfirmedTopics"] == 1)
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
    assert (row["State"], row["UnconfirmedTopics"], row["ConfirmedTopics"]) == ("CONNECTED", 1, 1)
    assert custom_session.opens == 1
    # The quiet stream has a span that says why its rows may be missing, without failing it;
    # the other is clean.
    errors = [(r["State"], r["Error"]) for r in streams_of(custom_session, dh_to_pylist)]
    assert errors == [("RUNNING", None), ("RUNNING", None)]
    assert spans_of(custom_session, dh_to_pylist) == {
        (1, NO_ACK): (
            "Massive has not confirmed the subscription T.AAPL, so its rows may be missing. The "
            "library re-sends it until Massive confirms."
        )
    }
    # A late acknowledgement ends it.
    fake_ws_server.send(status_frame("success", "subscribed to: T.AAPL"))
    custom_session.wait(lambda s: len(s.statuses) == 2)
    wait_for(lambda: ended(custom_session, dh_to_pylist, NO_ACK) == [(1, True)])
    (span,) = problem_rows(custom_session, dh_to_pylist, NO_ACK)
    assert (span["Action"], span["Rows"]) == ("DELAYED", 1)
    [row] = connections_of(custom_session, dh_to_pylist)
    assert (row["UnconfirmedTopics"], row["ConfirmedTopics"]) == (0, 2)
    del quiet, healthy


def on_host(host: str, evidence: str = "test evidence") -> Callable[..., HostDecision]:
    # The hint reads the host and the evidence; the recency plays no part in it.
    return lambda *_: HostDecision(host, Recency.UNKNOWN, evidence)


@pytest.mark.parametrize(
    ("decide", "hint"),
    [
        (
            on_host(REALTIME_HOST),
            (
                "The usual cause is a key without real-time futures data. To use Massive's "
                "delayed futures feed, add dm.Subscription.FUTURES to delayed_subscriptions in "
                "dm.MassiveConfig."
            ),
        ),
        (
            # A snapshot without a label says nothing about the host: Massive may serve the
            # data on another host, which websocket_hosts pins.
            on_host(REALTIME_HOST, "no timeframe in the futures snapshot"),
            (
                "Massive's futures snapshot did not say whether this key's data is real-time or "
                "delayed, so the library chose the real-time host. To stream from another host, "
                "pass "
                'websocket_hosts={dm.AssetClass.FUTURES: "<host>"} to dm.MassiveConfig. To use '
                "Massive's delayed futures feed, add dm.Subscription.FUTURES to "
                "delayed_subscriptions in dm.MassiveConfig."
            ),
        ),
        (
            # A refused snapshot is quoted: the key may have no live data on either feed, so
            # the delayed feed is no advice.
            on_host(
                REALTIME_HOST,
                "the host check's snapshot (/futures/v1/snapshot) was refused: not entitled",
            ),
            (
                "Massive also refused the futures snapshot: the host check's snapshot "
                "(/futures/v1/snapshot) was refused: not entitled. The usual cause is a key "
                "without live futures data. Read it with fetch(), poll(), or history() instead."
            ),
        ),
        (
            # A key whose snapshot proved real-time data gains nothing from the delayed feed.
            on_host(REALTIME_HOST, "last_trade.timeframe=REAL-TIME"),
            (
                "The futures snapshot answered real-time data, last_trade.timeframe=REAL-TIME, "
                "so Massive's delayed feed is no alternative."
            ),
        ),
        (
            on_host(DELAYED_HOST, "MassiveConfig.delayed_subscriptions"),
            (
                "delayed_subscriptions lists dm.Subscription.FUTURES, so this table streams from "
                "Massive's delayed feed. To stream from the real-time feed, remove "
                "dm.Subscription.FUTURES from delayed_subscriptions."
            ),
        ),
        (
            on_host("futures.example.com", "websocket_hosts"),
            (
                "This host comes from websocket_hosts in dm.MassiveConfig. Remove "
                "dm.AssetClass.FUTURES from websocket_hosts, or set it to a host Massive gives you."
            ),
        ),
        pytest.param(
            # A pinned real-time host keeps the pin's hint, whatever its snapshot said.
            on_host(REALTIME_HOST, "websocket_hosts, last_trade.timeframe=REAL-TIME"),
            (
                "This host comes from websocket_hosts in dm.MassiveConfig. Remove "
                "dm.AssetClass.FUTURES from websocket_hosts, or set it to a host Massive gives you."
            ),
            id="pinned real-time host",
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
    assert failed["Error"] == (
        f"test.futures: Massive answered the subscription to T.NQZ5 on {custom_session.urls[0]}: "
        f'"Real-time data requires an upgrade". {hint}'
    )
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
    # The waiting, connecting, connected, and subscribing rows, then the frame.
    assert len(written) <= 5
    ws_session.session.release(result)


@pytest.mark.parametrize(
    ("asset_class", "topics", "channel", "listed"),
    [
        (AssetClass.INDICES, ("V.I:SPX", "A.I:SPX"), "V", "A.I:SPX and V.I:SPX"),
        (AssetClass.OPTIONS, ("AD.*",), "AD", "AD.*"),
    ],
    ids=["index values", "admin messages"],
)
def test_a_refusal_names_the_one_host_a_channel_s_page_documents(
    ws_session: WsSession,
    *,
    asset_class: AssetClass,
    topics: tuple[str, ...],
    channel: str,
    listed: str,
) -> None:
    # Index values are documented only on the Business feed and the admin messages only on the
    # real-time feed: a connection on another host names that host for the channel alone.
    connection = ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url=f"wss://delayed.test/{asset_class.value}",
        asset_class=asset_class,
        family=ChannelFamily.TICK,
        decision=HostDecision("delayed.test", Recency.DELAYED, "delayed_subscriptions"),
        sessions=None,
    )
    connection._refused(topics, "not authorized")
    assert connection._last_error is not None
    # The generated host, which test_generate ties to the channel's vendored page.
    only = channel_only_host(asset_class, channel)
    subscription = asset_class.name
    assert connection._last_error == (
        f"Massive answered the subscription to {listed} on wss://delayed.test/"
        f'{asset_class.value}: "not authorized". Massive documents the {asset_class.value} '
        f"{channel} channel only on {only}. delayed_subscriptions lists "
        f"dm.Subscription.{subscription}, so this table streams from Massive's delayed feed. To "
        f"stream from the real-time feed, remove dm.Subscription.{subscription} from "
        "delayed_subscriptions."
    )
    assert connection._last_error.count("Massive documents") == 1  # not for the A channel
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_refusal_on_the_one_documented_host_adds_no_host(ws_session: WsSession) -> None:
    connection = ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url="wss://socket.test/options",
        asset_class=AssetClass.OPTIONS,
        family=ChannelFamily.TICK,
        decision=HostDecision(REALTIME_HOST, Recency.REALTIME, "real-time"),
        sessions=None,
    )
    connection._refused(("AD.*",), "not authorized")
    assert connection._last_error is not None
    assert "Massive documents" not in connection._last_error
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
    monkeypatch.setattr(connection, "join", lambda _: False)  # as if it waited JOIN_TIMEOUT
    ws_session.connections.close()
    assert (
        f"{_URL}: the connection had not closed 5 s after close(), so it closes in the "
        "background. Nothing to do."
    ) in caplog.messages
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


def join(ws_session: WsSession, config: dm.MassiveConfig | None = None) -> dm.MassiveSession:
    # A second session with the same key and connection settings, sharing the wrapper's
    # connections; `config`, when given, is the wrapper's with other settings it differs in.
    # The wrapper's own session is closed by its fixture, this one by the test.
    session = dm.MassiveSession(ws_session.config if config is None else config)
    connections = ws.WsConnections(
        session._core,
        resolve_host=ws_session._resolve,
        fetch_rows=ws_session._fetch_rows,
        crypto_pair=ws_session._crypto_pair,
        client_factory=ws_session.factory,
    )
    assert connections.group is ws_session.connections.group
    ports = session._core._ports
    assert isinstance(ports, SessionClients)
    ports._streams = connections
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
    transport = session._core.stream_transport()
    assert isinstance(transport, ws.WsConnections)
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
    "websocket_hosts": {AssetClass.STOCKS: "pinned.test"},
    "subscription_confirmation_timeout": 7.0,
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
    # Wait until the calendar has asked Massive's snapshot for every ticker.
    def asked() -> bool:
        sent: set[str] = set()
        for path, query in ws_session.fetched:
            if path == "/futures/v1/snapshot":
                asked_for = query["ticker.any_of"]
                assert isinstance(asked_for, tuple)
                sent.update(asked_for)
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
        snapshot_path: str,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch,
                snapshot_path=snapshot_path,
                contracts_path=contracts_path,
                schedules_path=schedules_path,
                label=label,
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
        snapshot_path: str,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch,
                snapshot_path=snapshot_path,
                contracts_path=contracts_path,
                schedules_path=schedules_path,
                label=label,
            )

        calendar = make()
        created.append(calendar)
        ws_session.connections.close()  # the session closes while the calendar starts
        return calendar

    monkeypatch.setattr("deephaven_massive._ingest.ws_connection.FuturesCalendar", closing)
    with pytest.raises(
        dm.MassiveError,
        match=rf"^{_GROUP_CLOSED}$",
    ):
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
        def fetch_rows(
            template: str, query: Mapping[str, object], *, bucket: str, cancel: Cancel
        ) -> list[dict[str, object]]:
            del query, bucket, cancel
            reads.append(template)
            return []

    group = ws_session.connections.group
    real = group.sessions
    group.sessions = {_Freed()}
    try:
        with pytest.raises(
            dm.MassiveError,
            match=rf"^{_GROUP_CLOSED}$",
        ):
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
    found = group.futures_calendar(snapshot_path="w", contracts_path="x", schedules_path="y")
    assert found is calendar
    looked_up(ws_session, "NQZ5")  # read through the open session
    other.close()
    assert not calendar._thread.is_alive()  # the group's last session closed it
    with pytest.raises(
        dm.MassiveError,
        match=rf"^{_GROUP_CLOSED}$",
    ):
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
        buffered = row["BufferedRows"]
        assert isinstance(buffered, int)
        return buffered

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
        buffered = row["BufferedRows"]
        assert isinstance(buffered, int)
        return buffered

    wait_for(lambda: shown() == 0)
    ws_session.session.release(result)


_URL = "wss://socket.test/stocks"


def test_the_connection_reports_drift_and_logs_dropped_rows_and_a_summary(
    ws_session: WsSession,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
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
    drifts = problem_rows(ws_session, dh_to_pylist, DRIFT)  # a problem of each stream
    assert [(r["StreamId"], r["Field"]) for r in drifts] == [(1, "zz"), (2, "zz")]
    assert all(
        r["Detail"]
        == (
            "Massive sent the field 'zz', which its documentation lacks, so the rows are "
            "published without it. It was first sent for AAPL. Its first value: 1."
        )
        for r in drifts
    )
    [dropped] = [m for m in caplog.messages if m.startswith("cannot place a T row for AAPL")]
    assert dropped.endswith('"q":1,"s":100,"z":3,"ds":"100"}')
    [summary] = [m for m in caplog.messages if m.startswith(f"{_URL}: last 60 s: ")]
    # The odd rows failed no stream: stream 1 got the one row read whole; the row with a bad
    # price was withheld, and the timeless one counts as dropped traffic.
    assert summary.startswith(
        f"{_URL}: last 60 s: 1 data batches, rows T=2, 1 dropped; rows per stream 1 [1=1], "
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
        rf"{re.escape(_URL)}: the connection is down, so its 1 live table gets no data until it "
        r"reconnects\. The library tries again in \d+ s\. m\.connections\(\) shows its state\.",
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
    assert down[0].getMessage() == (
        f"{_URL}: the connection is down, so its 1 live table gets no data until it reconnects. "
        'Massive answered the login with "max_connections", so another connection with this '
        "API key holds its place. The library tries again in 30 s. Stop the other "
        "program or session that uses the key."
    )
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
    assert [m.split(". ")[1] for m in down] == [
        "The library tries again in 30 s",
        (
            'Massive answered the login with "max_connections", so another connection with this '
            "API key holds its place"
        ),
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
                "test.trades stream 1: 3 contracts deferred, 2 before; The connection wants 1 "
                "options subscription, over Massive's cap of 1,000, so 0 wait."
            ),
        )
    ]
    ws_session.session.release(result)


_DOWN = (
    r"The connection has been down since (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ), so no data arrives\."
)
"""The pattern of the `outage` span's detail of a stream whose connection is down; its group
is the start."""


_NO_ACK = (
    r"Massive has not confirmed the subscription T\.\w+, so its rows may be missing\. The "
    r"library re-sends it until Massive confirms\."
)
"""The pattern of the `unacknowledged_topics` span's detail of a stream with a topic Massive
never acknowledged."""


def _outage_failure(since: str, limit: str) -> str:
    # The message a stream fails with once the outage held it down for `max_outage`, prefixed
    # with its dataset, as every stream's failure is.
    return (
        f"test.trades: The WebSocket connection to socket.test/stocks has been down since "
        f"{since}, longer than max_outage={limit} s, so this table stopped and the rows from "
        "then on are missing. Call m.test.trades again for a new live table."
    )


def _ack(topic: str) -> str:
    # The acknowledgement of one topic, as Massive sends it.
    return json.dumps([{"ev": "status", "status": "success", "message": f"subscribed to: {topic}"}])


def _acks(ws_session: WsSession) -> int:
    return sum(status.kind is StatusKind.ACK for status in ws_session.statuses)


def _down(ws_session: WsSession, connection: ws_connection.Connection) -> set[int]:
    # The streams the outage holds down, as the connection's loop sees them.
    down = ws_session.on_loop(connection, lambda: set(connection._episodes.down))
    assert isinstance(down, set)
    return down


def _down_since(ws_session: WsSession, connection: ws_connection.Connection) -> float | None:
    # The outage's start as the connection's loop holds it.
    since = ws_session.on_loop(connection, lambda: connection._episodes.down_since)
    assert since is None or isinstance(since, float)
    return since


def _left(ws_session: WsSession, connection: ws_connection.Connection, stream_id: int) -> float:
    # Seconds until a down stream's timer fails it.
    def left() -> float:
        timer = connection._episodes.down[stream_id]
        return timer.when() - connection._client.loop.time()

    seconds = ws_session.on_loop(connection, left)
    assert isinstance(seconds, float)
    return seconds


def _expire(ws_session: WsSession, connection: ws_connection.Connection, *ids: int) -> None:
    # What the clock reaching each stream's max_outage does: its timer's callback, then a flush.
    def expire() -> None:
        for stream_id in ids or list(connection._episodes.down):
            connection._episodes.down[stream_id].cancel()
            connection._stream_expired(stream_id)

    ws_session.on_loop(connection, expire)
    ws_session.flush()


def _since(error: str) -> str:
    # The outage's start named in a failure.
    found = re.search(r"since (\S+),", error)
    assert found is not None
    return found.group(1)


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
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    # Reconnected, but no data flows until the topic is acknowledged: still an outage.
    down = re.fullmatch(_DOWN, spans_of(ws_session, dh_to_pylist)[(1, OUTAGE)])
    assert down is not None
    assert 280.0 < _left(ws_session, connection, 1) <= 300.0  # the default: five minutes
    with caplog.at_level(logging.INFO, logger=ws_connection.__name__):
        fake_ws_server.send(_ack("T.AAPL"))
        wait_for(lambda: ended(ws_session, dh_to_pylist, OUTAGE) == [(1, True)])
        assert _down(ws_session, connection) == set()
        assert _down_since(ws_session, connection) is None  # the outage is over
    # The span ends, and its row stays: from the last frame before the drop to the data's
    # return.
    (gap,) = problem_rows(ws_session, dh_to_pylist, OUTAGE)
    assert (gap["Action"], utc(gap["SpanStart"])) == ("GAP", down.group(1))
    assert gap["EndedAt"] >= gap["SpanStart"]
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
    failure = connection._episodes.failures["test.trades"]
    assert (failure.source, failure.is_retryable) == (dm.ErrorSource.NETWORK, True)
    # An expected failure: one WARNING for both streams, never an ERROR.
    failures = [r for r in caplog.records if "longer than max_outage=" in r.message]
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
        # The stream stays expired until its table fails, which needs the flush below; the
        # outage itself can end first, once the reconnect's subscription is acknowledged. The
        # set is read on the loop: read here, it could show the stream before the same loop
        # callback queued the stream's failure for the flush.
        return (
            custom_session.on_loop(connection, lambda: bool(connection._episodes.expired)) is True
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
    custom_session = make_ws_session(subscription_confirmation_timeout=0.05)
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
    spans = spans_of(custom_session, dh_to_pylist)
    # Two spans of each stream, each its own: the outage, and the connection's no-ack span.
    assert spans.keys() == {(1, OUTAGE), (1, NO_ACK), (2, OUTAGE), (2, NO_ACK)}
    for stream_id in (1, 2):
        assert re.fullmatch(_DOWN, spans[(stream_id, OUTAGE)])
        assert re.fullmatch(_NO_ACK, spans[(stream_id, NO_ACK)])

    def data_for(symbol: str) -> None:
        # The flush that gives the stream its first rows of the new socket checks recovery
        # at once, without waiting for the next data frame or the summary.
        custom_session.publish(trade(symbol))

    data_for("AAPL")  # AAPL's data flows again; MSFT's does not
    assert _down(custom_session, connection) == {2}
    # Ending the outage's span keeps the no-ack span, which only its source ends.
    assert spans_of(custom_session, dh_to_pylist).keys() == {(1, NO_ACK), (2, OUTAGE), (2, NO_ACK)}
    data_for("MSFT")
    assert spans_of(custom_session, dh_to_pylist).keys() == {(1, NO_ACK), (2, NO_ACK)}
    assert [r["Error"] for r in streams_of(custom_session, dh_to_pylist)] == [None, None]
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
    custom_session = make_ws_session(subscription_confirmation_timeout=0.05)
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
    custom_session = make_ws_session(subscription_confirmation_timeout=0.05)
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
    spans = spans_of(custom_session, dh_to_pylist)
    assert spans.keys() == {(2, OUTAGE), (2, NO_ACK)}
    assert re.fullmatch(_DOWN, spans[(2, OUTAGE)])
    assert re.fullmatch(_NO_ACK, spans[(2, NO_ACK)])
    _expire(custom_session, connection, 2)
    wait_for(lambda: bool(other.j_table.isFailed()))
    rows = streams_of(custom_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert rows[0]["Error"] is None
    assert not result.j_table.isFailed()
    custom_session.session.release(result)


def test_a_topic_massive_refused_never_holds_a_dropped_stream_down(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
) -> None:
    # XYZ is refused, so it is never acknowledged on any socket. After a reconnect the quiet
    # stream recovers once AAPL is acknowledged, without waiting for a row to arrive.
    fake_ws_server.refuse["T.XYZ"] = "not authorized"
    custom_session = make_ws_session()
    result = custom_session.query(TRADES, ["AAPL", "XYZ"]).last_by()
    custom_session.subscribed("T.AAPL", "T.XYZ")
    custom_session.publish(trade("AAPL"))  # rows that could look current
    connection = custom_session.connection()
    # Massive's error names no topic, so the library finds XYZ by subscribing the topics apart.
    wait_for(
        lambda: bool(
            custom_session.on_loop(
                connection, lambda: all(s.refused for s in connection._streams.values())
            )
        )
    )
    fake_ws_server.close(1011)
    custom_session.wait(lambda s: s.opens == 2)

    def acknowledged_again() -> bool:
        return bool(
            custom_session.on_loop(
                connection,
                lambda: (
                    connection._state is ConnectionState.CONNECTED
                    and connection.subscriptions.counts()[1] == 1
                ),
            )
        )

    wait_for(acknowledged_again)
    custom_session.on_loop(connection, connection._check_recovered)
    assert _down(custom_session, connection) == set()
    custom_session.session.release(result)


def test_a_stream_that_never_received_a_row_runs_on_with_its_topic_unacknowledged(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # Before the drop and after the reconnect, Massive never confirms MSFT. Its table has no
    # row that could look current, so it keeps running with its no-ack note.
    custom_session = make_ws_session(subscription_confirmation_timeout=0.05)
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
    assert (row["State"], row["Error"]) == ("RUNNING", None)
    spans = spans_of(custom_session, dh_to_pylist)
    assert spans.keys() == {(1, NO_ACK)}
    assert re.fullmatch(_NO_ACK, spans[(1, NO_ACK)])
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
        connection._episodes.down[1].cancel()
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
    wait_for(lambda: ended(ws_session, dh_to_pylist, OUTAGE) == [(1, True), (2, True)])
    assert [r["Error"] for r in streams_of(ws_session, dh_to_pylist)] == [None, None]
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
    wait_for(lambda: ended(ws_session, dh_to_pylist, OUTAGE) == [(1, True)])
    (gap,) = problem_rows(ws_session, dh_to_pylist, OUTAGE)
    assert utc(gap["SpanStart"]) == utc_text(stopped / 1e9)  # when data stopped
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


def test_the_reconnect_deadline_is_the_earliest_held_down_stream_less_the_confirmation_wait(
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
    seen = ws_session.on_loop(connection, deadline)
    assert isinstance(seen, float)
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
    assert re.fullmatch(_DOWN, spans_of(ws_session, dh_to_pylist)[(2, OUTAGE)])
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
    wait_for(lambda: (2, True) in ended(ws_session, dh_to_pylist, OUTAGE))
    assert (2, OUTAGE) not in spans_of(ws_session, dh_to_pylist)
    assert _down(ws_session, connection) == set()
    assert _down_since(ws_session, connection) is None
    assert streams_of(ws_session, dh_to_pylist)[1]["State"] == "RUNNING"
    ws_session.session.release(late)


def test_a_stream_released_during_an_outage_ends_only_its_own_outage_span(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    fake_ws_server.silent = {"T.AAPL", "T.MSFT"}  # the outage outlasts the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    wait_for(lambda: _down(ws_session, connection) == {1, 2})
    wait_for(lambda: ended(ws_session, dh_to_pylist, OUTAGE) == [(1, False), (2, False)])
    core = ws_session.session._core
    reported: list[Problem] = []
    report = core.problems

    def recording(problems: Sequence[Problem]) -> None:
        reported.extend(problems)
        report(problems)

    monkeypatch.setattr(core, "problems", recording)
    ws_session.session.release(first)
    del first
    wait_for(lambda: not ws_session.on_loop(connection, lambda: 1 in connection._streams))
    # The connection ends the released stream's span itself, as it leaves.
    assert [
        (p.cause, p.spans) for p in reported if isinstance(p, ProblemSpans) and p.stream_id == 1
    ] == [(ProblemCause.OUTAGE, {})]
    # The other stream is still down, its span open.
    assert ended(ws_session, dh_to_pylist, OUTAGE) == [(1, True), (2, False)]
    assert re.fullmatch(_DOWN, spans_of(ws_session, dh_to_pylist)[(2, OUTAGE)])
    assert _down(ws_session, connection) == {2}
    ws_session.session.release(second)


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


def test_any_frame_ends_the_quiet_feed_span_and_the_stream_keeps_running(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    monkeypatch.setattr(ws_silence, "in_trading_hours", lambda _asset_class, _moment: True)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.wait(lambda s: any(status.kind is StatusKind.ACK for status in s.statuses))
    connection = ws_session.connection()
    later = int(2 * ws_silence.NO_FRAMES_WARNING * 1e9)

    def quiet() -> str | None:
        [row] = streams_of(ws_session, dh_to_pylist)
        assert (row["State"], row["Error"]) == ("RUNNING", None)  # silence never fails a stream
        return spans_of(ws_session, dh_to_pylist).get((1, QUIET))

    for frame in (_ack("T.AAPL"), json.dumps([trade("AAPL")])):  # a status, then data
        ws_session.on_loop(connection, lambda: connection._check_frames(time.time_ns() + later))
        assert re.fullmatch(
            r"Massive has sent nothing since \S+Z during stocks trading hours\.",
            quiet() or "",
        )
        fake_ws_server.send(frame)
        wait_for(lambda: quiet() is None)
    assert ended(ws_session, dh_to_pylist, QUIET) == [(1, True), (1, True)]  # one per episode
    ws_session.session.release(result)


def _go_quiet(connection: ws_connection.Connection, note: str) -> None:
    # Each stream's quiet_feed span as the silence check would show it. Straight to the spans:
    # a status frame the fake server sends would end a real episode meanwhile.
    connection._report_spans(
        {
            stream_id: {ProblemCause.QUIET_FEED: ({"quiet": (None, None, note, 0)}, None)}
            for stream_id in connection._streams
        }
    )


def test_a_span_reaches_only_the_streams_whose_session_lives(
    *, ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, dh_to_pylist: ToPylist
) -> None:
    # A session freed without close() keeps its streams on the connection until its closer
    # runs on the reaper; a span shown meanwhile reaches only the streams of live sessions.
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    core_of = StreamSink.core

    def freed(sink: StreamSink) -> SessionCore | None:
        return None if sink.stream_id == 2 else core_of(sink)

    monkeypatch.setattr(StreamSink, "core", freed)
    ws_session.on_loop(connection, lambda: _go_quiet(connection, "quiet"))
    assert spans_of(ws_session, dh_to_pylist) == {(1, QUIET): "quiet"}
    assert ws_session.on_loop(connection, lambda: sorted(connection._spans)) == [1]
    monkeypatch.undo()
    ws_session.session.release(first)
    ws_session.session.release(second)


def test_a_span_that_did_not_change_is_not_reported_again(
    *, ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, dh_to_pylist: ToPylist
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    core = ws_session.session._core
    reported: list[Problem] = []
    report = core.problems

    def recording(problems: Sequence[Problem]) -> None:
        reported.extend(problems)
        report(problems)

    monkeypatch.setattr(core, "problems", recording)
    for _ in range(2):  # the same note twice: its span holds on, unchanged
        ws_session.on_loop(connection, lambda: _go_quiet(connection, "quiet"))
    assert [(p.cause, p.stream_id) for p in reported if p.cause is ProblemCause.QUIET_FEED] == [
        (ProblemCause.QUIET_FEED, 1)
    ]
    assert spans_of(ws_session, dh_to_pylist) == {(1, QUIET): "quiet"}
    ws_session.session.release(result)


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


def test_a_refused_fmv_topic_names_its_one_host(ws_session: WsSession) -> None:
    # FMV is never pinned, so the refusal names Massive's answer, not `websocket_hosts`.
    connection = ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url="wss://business.massive.com/forex",
        asset_class=AssetClass.FOREX,
        family=ChannelFamily.FMV,
        decision=HostDecision(
            "business.massive.com", Recency.REALTIME, "FMV channel's documented host"
        ),
        sessions=None,
    )
    connection._refused(("FMV.C:EURUSD",), "not authorized")
    assert connection._last_error is not None
    assert connection._last_error == (
        "Massive answered the subscription to FMV.C:EURUSD on wss://business.massive.com/forex: "
        '"not authorized". Massive documents one host for FMV data, business.massive.com, and '
        "websocket_hosts does not apply to it."
    )
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_refusal_adds_a_real_time_note_only_to_the_streams_that_carry_one(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # One status may refuse several topics; only the admin messages' table names its plans.
    m = ws_session.session
    notices = m.options.admin_messages().blink()
    trades = m.options.trades(dm.ALL).blink()
    ws_session.subscribed("AD.*", "T.*")
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: connection._refused(("AD.*", "T.*"), "no"))
    ws_session.flush()
    errors = {r["Dataset"]: r["Error"] for r in streams_of(ws_session, dh_to_pylist)}
    note = "options.admin_messages streams only in real time. Its plans are listed at "
    assert note in errors["options.admin_messages"]
    assert errors["options.trades"] == (
        "options.trades: Massive answered the subscription to AD.* and T.* on "
        'wss://socket.test/options: "no". '
        "Massive documents the options AD channel only on socket.massive.com. To stream from "
        'another host, pass websocket_hosts={dm.AssetClass.OPTIONS: "<host>"} to '
        "dm.MassiveConfig."
    )
    assert "streams only in real time" not in errors["options.trades"]
    m.release(notices)
    m.release(trades)


def test_a_refused_wildcard_is_the_feed_s_refusal_and_a_refused_symbol_its_own(
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # One status refuses a channel's wildcard and another channel's one symbol, before Massive
    # answered either: the stream that asked for every symbol gets the feed's refusal, and the
    # other, refused every symbol it was opened with, its symbol's. The refusal is one error of
    # the connection, so it is alarmed once, whatever the kinds. One subscription is sent at a
    # time, so Q.AAPL goes once T.* is left unconfirmed: a short wait keeps the test quick.
    ws_session = make_ws_session(subscription_confirmation_timeout=0.05)
    fake_ws_server.auto_ack = False
    m = ws_session.session
    every = m.stocks.trades(dm.ALL).blink()
    one = m.stocks.quotes(["AAPL"]).blink()
    ws_session.subscribed("T.*", "Q.AAPL")
    connection = ws_session.connection()
    failed: dict[int, MassiveError] = {}
    fail = stream.Flusher.fail

    def recording(self: stream.Flusher, ids: Iterable[int], error: MassiveError) -> None:
        for stream_id in ids:
            failed[stream_id] = error
        fail(self, ids, error)

    monkeypatch.setattr(stream.Flusher, "fail", recording)
    with caplog.at_level(logging.WARNING, logger=ws_connection.__name__):
        ws_session.on_loop(connection, lambda: connection._refused(("T.*", "Q.AAPL"), "no"))
        ws_session.flush()
    ids = {r["Dataset"]: r["StreamId"] for r in streams_of(ws_session, dh_to_pylist)}
    assert type(failed[ids["stocks.trades"]]) is FeedRefusalError
    assert type(failed[ids["stocks.quotes"]]) is EntitlementError
    assert str(failed[ids["stocks.trades"]]).removeprefix("stocks.trades: ") == str(
        failed[ids["stocks.quotes"]]
    ).removeprefix("stocks.quotes: ")
    warned = [
        r for r in caplog.records if r.levelno == logging.WARNING and '"no"' in r.getMessage()
    ]
    assert len(warned) == 1
    m.release(every)
    m.release(one)


def test_a_refusal_of_two_datasets_gives_each_its_own_copy_and_warns_once(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # One status refuses the topics of two datasets that share one message: each table's
    # Error starts with its own dataset, and the log carries the refusal once, at WARNING from
    # the connection, before either stream fails: each stream's copy is a DEBUG repeat.
    m = ws_session.session
    trades = m.options.trades(dm.ALL).blink()
    quotes = m.options.quotes(dm.ALL).blink()
    ws_session.subscribed("T.*", "Q.*")
    connection = ws_session.connection("wss://socket.test/options")
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        ws_session.on_loop(connection, lambda: connection._refused(("Q.*", "T.*"), "no"))
        ws_session.flush()
        rows = streams_of(ws_session, dh_to_pylist)
    errors = {r["Dataset"]: r["Error"] for r in rows}
    ids = {r["Dataset"]: r["StreamId"] for r in rows}
    answered = (
        'Massive answered the subscription to Q.* and T.* on wss://socket.test/options: "no". '
        'To stream from another host, pass websocket_hosts={dm.AssetClass.OPTIONS: "<host>"} '
        "to dm.MassiveConfig."
    )
    assert errors == {
        "options.trades": f"options.trades: {answered}",
        "options.quotes": f"options.quotes: {answered}",
    }
    loggers = (ws_connection.__name__, "deephaven_massive._tables.stream")
    logged = [
        (r.name, r.levelno, r.getMessage())
        for r in caplog.records
        if r.name in loggers and answered in r.getMessage()
    ]
    assert logged[0] == (ws_connection.__name__, logging.WARNING, answered)
    assert sorted(logged[1:]) == [
        (
            "deephaven_massive._tables.stream",
            logging.DEBUG,
            f"options.quotes stream {ids['options.quotes']} failed: {answered}",
        ),
        (
            "deephaven_massive._tables.stream",
            logging.DEBUG,
            f"options.trades stream {ids['options.trades']} failed: {answered}",
        ),
    ]
    m.release(trades)
    m.release(quotes)


_TIME = r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ"

_TAKEN_ERROR = (
    r"Another connection with this API key took the WebSocket connection to socket\.test/stocks "
    rf"at ({_TIME}), so this table stopped\. Stop the other program or session that uses the key "
    r"and call the method again, or pass connection_taken=dm\.ConnectionTaken\.RECONNECT to "
    r"dm\.MassiveConfig to take the connection back\."
)
_STREAM_TAKEN_ERROR = r"test\.trades: " + _TAKEN_ERROR
"""The same failure as a stream's `Error`: the stream's own copy, prefixed with its dataset."""


def test_a_taken_connection_fails_its_tables_and_closes_by_default(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    caplog: pytest.LogCaptureFixture,
) -> None:
    q = ws_session.query(TRADES, ["AAPL"])
    result = q.blink()
    ws_session.subscribed("T.AAPL")
    taken = ws_session.connection()
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        fake_ws_server.evict()
        ws_session.wait(lambda s: WsState.CLOSED in s.states)
        assert taken.flusher.join(TIMEOUT)  # its last flush applied the failure
    # It never reconnects, which would take the connection back from the other program.
    assert ws_session.states == [WsState.CONNECTING, WsState.TAKEN, WsState.CLOSED]
    assert len(fake_ws_server.connections) == 1
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "CLOSED"
    assert re.fullmatch(_TAKEN_ERROR, row["LastError"])
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert stream["State"] == "FAILED"
    # The stream's copy names its dataset; the connection's `LastError` above does not.
    match = re.fullmatch(_STREAM_TAKEN_ERROR, stream["Error"])
    assert match is not None
    assert result.j_table.isFailed()
    # The connection's own WARNING is the one alarm; the stream's failure is a DEBUG repeat.
    assert [(r.levelno, r.getMessage()) for r in caplog.records if "took th" in r.getMessage()] == [
        (
            logging.WARNING,
            (
                f"{_URL}: another connection with this API key took this one at "
                f"{match.group(1)}. Massive closed it with the reason max_connections. No live "
                "table's session set connection_taken=RECONNECT, so its 1 live table failed and "
                "the connection closed."
            ),
        )
    ]
    with ws_session.connections.group.lock:
        assert ws_session.connections.group.connections == {}  # the next stream opens anew
    again = q.blink()
    ws_session.subscribed("T.AAPL")
    assert len(ws_session.urls) == 2
    ws_session.session.release(again)


def test_a_reconnect_session_says_when_it_takes_the_connection_back(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # A long backoff holds the connection in its wait, so the row and the note can be read.
    session = make_ws_session(backoff=60.0, connection_taken=dm.ConnectionTaken.RECONNECT)
    result = session.query(TRADES, ["AAPL"]).blink()
    session.subscribed("T.AAPL")
    fake_ws_server.evict()
    session.wait(lambda s: WsState.TAKEN in s.states)
    [row] = connections_of(session, dh_to_pylist)
    assert row["State"] == "RECONNECTING"
    assert row["NextReconnectAt"] is not None
    assert re.fullmatch(
        rf"Another connection with this API key took this connection at {_TIME}\. The library "
        rf"takes it back at {_TIME}\.",
        row["LastError"],
    )
    [stream] = streams_of(session, dh_to_pylist)
    assert (stream["State"], stream["Error"]) == ("RUNNING", None)
    assert re.fullmatch(
        rf"The connection has been down since {_TIME}, so no data arrives\. Another connection "
        rf"with this API key took it at {_TIME}, and the library takes it back at {_TIME}\.",
        spans_of(session, dh_to_pylist)[(1, OUTAGE)],
    )
    assert not result.j_table.isFailed()
    session.close()


def test_a_reconnect_session_takes_the_connection_back_and_streams_on(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    rows_when: RowsWhen,
) -> None:
    session = make_ws_session(connection_taken=dm.ConnectionTaken.RECONNECT)
    result = session.query(TRADES, ["AAPL"]).append_only()
    session.subscribed("T.AAPL")
    fake_ws_server.evict()
    session.wait(lambda s: s.opens == 2)
    session.subscribed("T.AAPL")  # on the new socket
    session.send(trade("AAPL"))
    session.flush()
    rows_when(result, lambda rows: len(rows) == 1)
    assert WsState.TAKEN in session.states
    session.session.release(result)


def test_a_shared_connection_taken_fails_only_the_tables_of_fail_sessions(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    rows_when: RowsWhen,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Differing only in connection_taken, the sessions still share one connection (`join`
    # checks it), so they never take it from each other.
    other = join(
        ws_session,
        dataclasses.replace(ws_session.config, connection_taken=dm.ConnectionTaken.RECONNECT),
    )
    mine = ws_session.query(TRADES, ["AAPL"]).append_only()
    theirs = ws_session.query(TRADES, ["AAPL"], other).append_only()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    with caplog.at_level(logging.WARNING, logger=ws_connection.__name__):
        fake_ws_server.evict()
        ws_session.wait(lambda s: s.opens == 2)  # a RECONNECT table remains: it reconnects
    # The failing table is never held down by the outage the other one rides.
    [failing] = [r["StreamId"] for r in rows_of(ws_session.session, dh_to_pylist)]
    assert ws_session.on_loop(connection, lambda: sorted(connection._episodes.expired)) == [failing]
    ws_session.flush()  # applies the failure
    wait_for(lambda: bool(mine.j_table.isFailed()))
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(theirs, lambda rows: len(rows) == 1)
    assert not theirs.j_table.isFailed()
    [warning] = [r.getMessage() for r in caplog.records if "took this one" in r.getMessage()]
    assert re.fullmatch(
        rf"{re.escape(_URL)}: another connection with this API key took this one at {_TIME}\. "
        r"Massive closed it with the reason max_connections\. The 1 live table whose session set "
        rf"connection_taken=RECONNECT reconnects at {_TIME} and takes it back\. The 1 live table "
        r"whose session set connection_taken=FAIL has failed\.",
        warning,
    )
    other.release(theirs)
    other.close()


def test_a_reconnect_session_that_lets_go_during_the_wait_closes_the_connection(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
) -> None:
    # With no table left to take the connection back, a reconnect would only evict the other
    # program, so the connection closes instead.
    session = make_ws_session(backoff=60.0, connection_taken=dm.ConnectionTaken.RECONNECT)
    result = session.query(TRADES, ["AAPL"]).blink()
    session.subscribed("T.AAPL")
    fake_ws_server.evict()
    session.wait(lambda s: WsState.TAKEN in s.states)
    session.session.release(result)
    session.wait(lambda s: WsState.CLOSED in s.states)
    assert len(fake_ws_server.connections) == 1
    [row] = connections_of(session, dh_to_pylist)
    assert row["State"] == "CLOSED"


def test_a_fail_sessions_stream_opening_when_the_connection_is_taken_fails_as_it_attaches(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A long backoff keeps the connection in its reconnect wait while the stream attaches.
    ws_session = make_ws_session(backoff=60.0)
    other = join(
        ws_session,
        dataclasses.replace(ws_session.config, connection_taken=dm.ConnectionTaken.RECONNECT),
    )
    theirs = ws_session.query(TRADES, ["AAPL"], other).blink()  # it keeps the connection
    ws_session.subscribed("T.AAPL")
    entered, go = threading.Event(), threading.Event()
    attach = ws.WsConnections._attach

    def held_attach(self: ws.WsConnections, stream: ws_connection.ConnectionStream) -> None:
        if self is ws_session.connections:  # leased, not yet routed, until the test says go
            entered.set()
            assert go.wait(TIMEOUT)
        attach(self, stream)

    monkeypatch.setattr(ws.WsConnections, "_attach", held_attach)
    with ThreadPoolExecutor(1) as pool:
        opening = pool.submit(lambda: ws_session.query(TRADES, ["MSFT"]).blink())
        assert entered.wait(TIMEOUT)
        fake_ws_server.evict()
        ws_session.wait(lambda s: WsState.TAKEN in s.states)
        go.set()
        try:  # the verb names the failure its stream met while it opened
            with pytest.raises(dm.MassiveError, match=r"stream \d+ failed: Another connection"):
                opening.result(TIMEOUT)
            [stream] = streams_of(ws_session, dh_to_pylist)
            assert re.fullmatch(_STREAM_TAKEN_ERROR, stream["Error"])
            assert not theirs.j_table.isFailed()
            other.release(theirs)
        finally:
            other.close()


def test_a_stream_that_opens_during_the_reconnect_wait_rides_the_reconnect(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # It was never taken, so a FAIL session's stream that opens during the wait runs on, even
    # when another stream's release re-applies the setting while it opens.
    session = make_ws_session(backoff=60.0)
    other = join(
        session, dataclasses.replace(session.config, connection_taken=dm.ConnectionTaken.RECONNECT)
    )
    kept = session.query(TRADES, ["AAPL"], other).blink()
    leaving = session.query(TRADES, ["IBM"], other).blink()
    session.subscribed("T.AAPL", "T.IBM")
    fake_ws_server.evict()
    session.wait(lambda s: WsState.TAKEN in s.states)
    entered, go = threading.Event(), threading.Event()
    attach = ws.WsConnections._attach

    def held_attach(self: ws.WsConnections, stream: ws_connection.ConnectionStream) -> None:
        if self is session.connections:
            entered.set()
            assert go.wait(TIMEOUT)
        attach(self, stream)

    monkeypatch.setattr(ws.WsConnections, "_attach", held_attach)
    connection = session.connection()
    try:
        with ThreadPoolExecutor(1) as pool:
            opening = pool.submit(lambda: session.query(TRADES, ["MSFT"]).blink())
            assert entered.wait(TIMEOUT)
            other.release(leaving)  # the connection re-applies the setting while it opens
            wait_for(lambda: session.on_loop(connection, lambda: len(connection._streams)) == 1)
            go.set()
            mine = opening.result(TIMEOUT)
        session.flush()
        assert not mine.j_table.isFailed()
        other.release(kept)
    finally:
        other.close()


def test_a_reconnected_connection_forgets_it_was_taken(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Once it has taken the connection back, its last table leaving is an ordinary idle close.
    monkeypatch.setattr(ws_connection, "IDLE_GRACE", 0.05)
    session = make_ws_session(connection_taken=dm.ConnectionTaken.RECONNECT)
    result = session.query(TRADES, ["AAPL"]).blink()
    session.subscribed("T.AAPL")
    with caplog.at_level(logging.INFO, logger=ws_connection.__name__):
        fake_ws_server.evict()
        session.wait(lambda s: s.opens == 2)
        session.subscribed("T.AAPL")  # on the new socket
        session.session.release(result)
        session.wait(lambda s: WsState.CLOSED in s.states)
    messages = [r.getMessage() for r in caplog.records]
    assert len([m for m in messages if "took this one at" in m]) == 1  # the eviction's
    assert f"closing {_URL}: no stream for 0 s" in messages
    assert not result.j_table.isFailed()


def test_a_taken_connection_decides_to_close_once(
    *,
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two tables leaving during the wait: the first change closes, and a later one finds the
    # close decided, so the WARNING and the failures come once.
    session = make_ws_session(backoff=60.0, connection_taken=dm.ConnectionTaken.RECONNECT)
    first = session.query(TRADES, ["AAPL"]).blink()
    second = session.query(TRADES, ["MSFT"]).blink()
    session.subscribed("T.AAPL", "T.MSFT")
    connection = session.connection()
    fake_ws_server.evict()
    session.wait(lambda s: WsState.TAKEN in s.states)
    with caplog.at_level(logging.WARNING, logger=ws_connection.__name__):
        # Both leases go before the loop hears of either, as when a session releases its tables.
        with session.connections.group.lock:
            leases = dict(session.connections.group.streams)
        for stream_id in leases:
            with session.connections.group.lock:
                session.connections._forget(stream_id)

        def twice() -> None:
            connection._after_change()
            connection._after_change()

        session.on_loop(connection, twice)
        session.wait(lambda s: WsState.CLOSED in s.states)
    closes = [r for r in caplog.records if "the connection closed." in r.getMessage()]
    assert len(closes) == 1
    del first, second


def test_a_client_state_with_no_rule_is_a_defect(ws_session: WsSession) -> None:
    # Every state has its rule; a new one fails until it has, never only shows in the row.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    with pytest.raises(AssertionError, match="BOGUS"):
        ws_session.on_loop(connection, lambda: connection._on_state("BOGUS", None))  # type: ignore[arg-type]  # the point: refused at run time
    ws_session.session.release(result)


# --- a connection with no topic: WAITING, no socket ------------------------------------------
#
# What a connection does on each event, by state; each cell has a test below, except where
# noted. A stream's open that fails leaves a waiting connection with no socket, which the next
# stream reuses (`test_a_failed_open_returns_its_lease` in test_ws.py), and `connect()` after
# the client ended does nothing (test_client.py).
#
# | state     | first topic       | last topic goes | last stream released | session close |
# |-----------|-------------------|-----------------|----------------------|---------------|
# | WAITING   | connects          | (none to go)    | closes, no socket    | no socket     |
# | CONNECTED | (already open)    | stays connected | idle close (above)   | (above)       |
#
# A socket dropped after the first topic reconnects, and an outage is dated from the first
# failed attempt, never from the wait before it.


def empty_watchlist(dh_input_table: Callable[..., InputTable]) -> InputTable:
    # A watchlist with no row: its stream wants no topic.
    return dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])


def add_row(watchlist: InputTable, symbol: str) -> None:
    watchlist.add(new_table([string_col("Symbol", [symbol])]))


def state_of(ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]) -> str:
    [row] = connections_of(ws_session, dh_to_pylist)
    return str(row["State"])


def test_a_stream_with_no_topic_waits_without_a_socket(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    result = ws_session.query(TRADES, empty_watchlist(dh_input_table)).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    connection = ws_session.connection()
    # A change with nothing to subscribe leaves it waiting.
    ws_session.on_loop(connection, connection.check_idle)
    assert state_of(ws_session, dh_to_pylist) == "WAITING"
    assert (len(ws_session.urls), ws_session.opens, ws_session.states) == (1, 0, [])
    assert fake_ws_server.connections == []
    ws_session.session.release(result)


def test_the_first_topic_opens_the_socket(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = empty_watchlist(dh_input_table)
    result = ws_session.query(TRADES, watchlist).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    add_row(watchlist, "AAPL")
    ws_session.subscribed("T.AAPL")
    assert state_of(ws_session, dh_to_pylist) == "CONNECTED"
    assert (ws_session.opens, ws_session.states) == (1, [WsState.CONNECTING])
    ws_session.session.release(result)


def test_a_connection_stays_open_when_its_last_topic_goes(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = empty_watchlist(dh_input_table)
    add_row(watchlist, "AAPL")
    result = ws_session.query(TRADES, watchlist).blink()
    ws_session.subscribed("T.AAPL")
    watchlist.delete(new_table([string_col("Symbol", ["AAPL"])]))
    ws_session.unsubscribed("T.AAPL")
    # Its stream remains: the socket stays open, and no second socket opens.
    assert state_of(ws_session, dh_to_pylist) == "CONNECTED"
    assert ws_session.opens == 1
    assert WsState.CLOSED not in ws_session.states
    ws_session.session.release(result)


def test_a_waiting_connection_released_closes_and_leaves_the_registry(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    monkeypatch.setattr(ws_connection, "IDLE_GRACE", 0.05)
    result = ws_session.query(TRADES, empty_watchlist(dh_input_table)).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    connection = ws_session.connection()
    ws_session.session.release(result)
    ws_session.wait(lambda s: WsState.CLOSED in s.states)
    assert connection.join(TIMEOUT)
    assert ws_session.connections.group.connections == {}
    assert state_of(ws_session, dh_to_pylist) == "CLOSED"
    assert (ws_session.opens, ws_session.states) == (0, [WsState.CLOSED])
    assert fake_ws_server.connections == []


def test_a_session_closed_while_waiting_opens_no_socket(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    result = ws_session.query(TRADES, empty_watchlist(dh_input_table)).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    connection = ws_session.connection()
    ws_session.session.close()
    assert connection.join(TIMEOUT)
    assert result.j_table.isFailed()
    assert (ws_session.opens, ws_session.states) == (0, [WsState.CLOSED])
    assert fake_ws_server.connections == []


def test_a_socket_dropped_after_the_first_topic_reconnects(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = empty_watchlist(dh_input_table)
    result = ws_session.query(TRADES, watchlist).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    add_row(watchlist, "AAPL")
    ws_session.subscribed("T.AAPL")
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    ws_session.subscribed("T.AAPL")  # on the new socket
    assert WsState.RECONNECTING in ws_session.states
    ws_session.session.release(result)


def test_an_outage_starts_at_the_first_failed_attempt_not_while_waiting(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    # The time spent waiting for a topic is no outage: the outage, which max_outage limits,
    # is dated from the first attempt that failed, after the first topic arrived.
    watchlist = empty_watchlist(dh_input_table)
    result = ws_session.query(TRADES, watchlist).blink()
    wait_for(lambda: state_of(ws_session, dh_to_pylist) == "WAITING")
    connection = ws_session.connection()
    assert ws_session.on_loop(connection, lambda: connection._episodes.down_since) is None
    fake_ws_server.reject_next(503)
    added_at = time.time()
    add_row(watchlist, "AAPL")
    ws_session.wait(lambda s: WsState.RECONNECTING in s.states)
    down_since = ws_session.on_loop(connection, lambda: connection._episodes.down_since)
    assert isinstance(down_since, float)
    assert down_since >= added_at
    ws_session.subscribed("T.AAPL")  # the next attempt connects
    ws_session.session.release(result)


def test_the_no_acknowledgement_detail_names_two_subscriptions_or_counts_many() -> None:
    text = ws_connection.Connection._no_ack_text
    assert text(frozenset({"T.AAPL", "Q.AAPL"})) == (
        "Massive has not confirmed the subscriptions Q.AAPL and T.AAPL, so their rows may be "
        "missing. The library re-sends them until Massive confirms."
    )
    many = frozenset(f"T.S{n}" for n in range(7))
    assert text(many) == (
        "Massive has not confirmed 7 subscriptions, the first 5: T.S0, T.S1, T.S2, T.S3, and "
        "T.S4, so their rows may be missing. The library re-sends them until Massive confirms."
    )
