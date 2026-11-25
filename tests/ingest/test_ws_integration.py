"""Live checks of the session's WebSocket handling: feeds, a dropped socket, a taken connection.

Each test records the session's diagnostics and Massive's messages (`evidence`). Streams
subscribe to one symbol and are released at once. The reconnect check closes the session's
socket from under it, through the wire client's private `_run_generation`, which receives each
authenticated socket: the library has no public way to drop a connection, and needs none. The
taken-connection test opens a second connection to a cluster on purpose: it is marked
`integration_limits`, runs only with `-m integration_limits`, and should run alone and last,
with no other client of the key. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven import update_graph as dh_update_graph
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._massive import WsClient, set_reply_capture
from deephaven_massive.queries import StreamHistoryQuery
from tests._live import refusals, streams, summary
from tests._live.feeds import with_delayed

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._massive import CapturedReply
    from tests._live.recency import RecencyAnswer

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
RecencyLabel: TypeAlias = "Callable[[dm.Subscription], RecencyAnswer]"
MarketStatus: TypeAlias = "Callable[[str], str]"

_WAIT = 45.0
"""Seconds a test waits for a frame or a state change."""

_FIRST_WAIT = 20.0
"""Seconds the taken-connection test waits for a session's trades; crypto trades all the time."""

_RECOVERY_WAIT = 120.0
"""Seconds the reconnect check waits for the stream to deliver rows again after the drop."""

_FOLLOW_WAIT = 60.0
"""Seconds the watchlist check waits for each step: a pair's first trade, or a row's leaving.
Bitcoin and Ether trade every few seconds at any hour."""


_SocketT = TypeVar("_SocketT", bound="_Socket")
_EndT = TypeVar("_EndT")


class _Socket(Protocol):
    """The part of an authenticated socket the reconnect check uses: its transport."""

    @property
    def transport(self) -> asyncio.Transport:
        """The socket's asyncio transport, whose `abort()` drops the connection at once."""
        ...


def _wait_until(
    read: Callable[[], list[dict[str, Any]]],
    done: Callable[[list[dict[str, Any]]], bool],
    *,
    wait: float = _FOLLOW_WAIT,
) -> list[dict[str, Any]]:
    """Read rows until `done` accepts them, or until `wait` seconds have passed.

    Returns:
        The last rows read, which the caller checks again, so a timeout fails with them.
    """
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + wait
    while True:
        rows = read()
        if done(rows) or time.monotonic() >= deadline:
            return rows
        pause.wait(0.25)


def _subscribe_minute_bars(
    config: dm.MassiveConfig,
    *,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> tuple[int, list[str]]:
    """Stream `AM.AAPL` in a session of its own, record what happened, and return the outcome.

    Returns:
        How many rows arrived, and the stream's `Error` and the connection's `LastError`, where
        set.
    """
    session = dm.MassiveSession(config)
    try:
        bars = session.stocks.bars("AAPL", dm.Timespan.MINUTE)
        assert isinstance(bars, StreamHistoryQuery)  # 1-minute bars stream
        table = bars.append_only()
        rows = wait_rows(session, table, time.monotonic() + _WAIT)
        for line in stream_report(session):
            evidence(line)
        errors = [str(r["Error"]) for r in table_rows(session.streams()) if r["Error"]]
        errors += [str(r["LastError"]) for r in table_rows(session.connections()) if r["LastError"]]
        session.release(table)
        evidence(f"AM.AAPL rows received: {len(rows)}; errors: {errors or 'none'}")
    finally:
        session.close()
    return len(rows), errors


@pytest.mark.settles("8")
@pytest.mark.timeout(110)
def test_what_massive_says_to_a_delayed_key_on_the_real_time_feed(
    live_config: dm.MassiveConfig,
    *,
    recency_label: RecencyLabel,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """What Massive answers when a key whose stocks data it labels delayed subscribes in real time.

    A subscription not listed in `MassiveConfig.delayed_subscriptions` always streams from
    `socket.massive.com`, the real-time feed. First asks Massive how it labels the key's stocks
    snapshot (`recency_label`), and skips, quoting the answer, when Massive refused the snapshot
    or does not label it delayed: the question is about a key whose data it does. Then leaves
    stocks out of `delayed_subscriptions` and pins them to that host, so the library's own check
    of the snapshot does not refuse the stream first, subscribes `AM.AAPL`, minute bars, and
    records the stream's error and the connection's last error. The refusal does not depend on
    the market being open. Passes when every error is a refusal (`refusals.is_entitlement_text`,
    such as the library's `Massive answered the subscription to`): the refusal is the
    evidence the question asks for. Fails on any other error, such as an outage or a taken
    connection, which says nothing of what Massive allows. Skips, quoting the rows, when Massive
    served the stream with no error.
    """
    answer = recency_label(dm.Subscription.STOCKS)
    if answer.refusal is not None:
        pytest.skip(f"{summary.REFUSED}{answer.refusal}")
    if not answer.delayed:
        pytest.skip(
            f"Massive does not label this key's stocks data delayed ({answer.text()}); the "
            f"question is about a key whose stocks data it labels delayed"
        )
    config = with_delayed(live_config, live_config.delayed_subscriptions - {dm.Subscription.STOCKS})
    hosts = {**config.websocket_hosts, dm.AssetClass.STOCKS: "socket.massive.com"}
    rows, errors = _subscribe_minute_bars(
        dataclasses.replace(config, websocket_hosts=hosts),
        wait_rows=wait_rows,
        table_rows=table_rows,
        stream_report=stream_report,
        evidence=evidence,
    )
    others = [error for error in errors if not refusals.is_entitlement_text(error)]
    assert not others, f"the stream failed for a reason other than a refusal: {others}"
    if not errors:
        pytest.skip(
            f"Massive served this key real-time rows on socket.massive.com: {rows} AM.AAPL "
            f"row(s) in {_WAIT:.0f} s, no error"
        )


@pytest.mark.verifies("70")
@pytest.mark.timeout(200)
def test_whether_massive_serves_a_real_time_key_on_the_delayed_feed(
    live_config: dm.MassiveConfig,
    *,
    recency_label: RecencyLabel,
    wait_acked: WaitAcked,
    wait_rows: WaitRows,
    market_status: MarketStatus,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether Massive serves a key it labels real-time on its delayed feed, `delayed.massive.com`.

    A subscription listed in `MassiveConfig.delayed_subscriptions` always streams from the
    delayed feed, even for a key Massive serves real-time. First asks Massive how it labels
    the key's stocks snapshot (`recency_label`), and skips, quoting the answer, when it labels
    it delayed: the question is about a key whose data is not, or when Massive refused the
    snapshot. Then lists stocks as delayed (dropping any host pin of stocks to another feed),
    subscribes `T.AAPL` (trades, which flow in the extended hours too), and records the
    stream's host, acknowledgement, rows, and any error. While the stock market is closed the
    test skips after the acknowledgement. While it is open, it waits up to 45 s for a trade;
    the delayed feed lags the market by an amount the tests do not know, so no trade skips,
    naming how long the session has been open. Passes when Massive acknowledged the
    subscription without refusing it and, when a trade came, sent it: a key Massive serves
    real-time may choose delayed data. A refusal would mean the setting needs a key whose data
    Massive labels delayed, and the library's message would have to say so.
    """
    answer = recency_label(dm.Subscription.STOCKS)
    if answer.refusal is not None:
        pytest.skip(f"{summary.REFUSED}{answer.refusal}")
    if answer.delayed:
        pytest.skip(f"Massive labels the key's stocks data delayed ({answer.text()})")
    session = dm.MassiveSession(with_delayed(live_config, {dm.Subscription.STOCKS}))
    try:
        table = session.stocks.trades("AAPL").append_only()
        acked = wait_acked(session, 1, time.monotonic() + _WAIT)
        status = market_status("stocks")
        now = datetime.now(UTC)
        opened = streams.opened_at("stocks", status, family="trades", now=now)
        rows: list[dict[str, Any]] = []
        if opened is not None:
            rows = wait_rows(session, table, time.monotonic() + _WAIT)
        for line in stream_report(session):
            evidence(line)
        errors = [str(r["Error"]) for r in table_rows(session.streams()) if r["Error"]]
        errors += [str(r["LastError"]) for r in table_rows(session.connections()) if r["LastError"]]
        session.release(table)
    finally:
        session.close()
    evidence(f"T.AAPL acknowledged {acked}; rows {len(rows)}; errors {errors or 'none'}")
    evidence(f"refused on the delayed feed: {bool(errors)}")
    assert not errors, f"Massive refused a real-time key on the delayed feed: {errors}"
    assert acked, "Massive did not acknowledge T.AAPL on the delayed feed"
    if opened is None:
        pytest.skip(f"not exercised: the stock market is {status} (acknowledged)")
    if not rows:
        minutes = (datetime.now(UTC) - opened).total_seconds() / 60
        pytest.skip(
            f"no row on the delayed feed in {_WAIT:.0f} s; the market has been open "
            f"{minutes:.0f} min (acknowledged)"
        )


@pytest.mark.timeout(200)
def test_a_stream_comes_back_with_rows_after_its_socket_drops(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    monkeypatch: pytest.MonkeyPatch,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    evidence: Evidence,
) -> None:
    """What the session does when its streaming socket drops: reconnect and deliver rows again.

    Streams `X:BTCUSD` trades (crypto trades all the time) until the first rows arrive, then
    aborts the socket's transport on the wire client's loop thread, as a network failure
    would, and samples the crypto WebSocket row of `m.connections()` (the class also has a
    `Transport=REST` row) every quarter second. Passes when the connection counts
    a reconnect, is `CONNECTED` again, its stream is `RUNNING`, and a trade the library
    received after the drop reached the table, within 120 s. Every connection state seen is
    recorded in order, `WAITING_SLOT` included: a reconnect that finds the old connection's
    slot still held waits for it. A stream Massive refuses skips, quoting Massive.
    """
    sockets: list[tuple[WsClient, _Socket]] = []

    def recorded(
        run: Callable[[WsClient, _SocketT], Awaitable[_EndT]],
    ) -> Callable[[WsClient, _SocketT], Awaitable[_EndT]]:
        async def recording(self: WsClient, websocket: _SocketT) -> _EndT:
            sockets.append((self, websocket))
            return await run(self, websocket)

        return recording

    monkeypatch.setattr(WsClient, "_run_generation", recorded(WsClient._run_generation))
    table = live_session.crypto.trades("X:BTCUSD").append_only()
    first = wait_rows(live_session, table, time.monotonic() + _FIRST_WAIT)
    if not first:
        skip_if_not_entitled(live_session)
    assert first, "no X:BTCUSD trade arrived before the drop"
    client, websocket = sockets[-1]
    dropped = time.time_ns()
    client.loop.call_soon_threadsafe(websocket.transport.abort)
    states: list[str] = []
    after: list[dict[str, Any]] = []
    connection: dict[str, Any] = {}
    stream: dict[str, Any] = {}
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + _RECOVERY_WAIT
    while time.monotonic() < deadline:
        (connection,) = [  # the REST row of the same class is not the socket that dropped
            r
            for r in table_rows(live_session.connections())
            if r["Transport"] == "WebSocket" and r["AssetClass"] == "crypto"
        ]
        stream = table_rows(live_session.streams())[0]
        if not states or states[-1] != connection["State"]:
            states.append(str(connection["State"]))
        after = [r for r in table_rows(table) if int(r["TimestampInsert"]) > dropped]
        recovered = connection["State"] == "CONNECTED" and stream["State"] == "RUNNING"
        if connection["Reconnects"] and recovered and after:
            break
        pause.wait(0.25)
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"connection states after the drop: {' -> '.join(states)}")
    evidence(f"waited for a slot: {'WAITING_SLOT' in states}")
    evidence(f"reconnects {connection.get('Reconnects')}; trades after the drop {len(after)}")
    assert connection.get("Reconnects"), f"the connection never reconnected: {states}"
    assert stream.get("State") == "RUNNING", f"the stream is {stream.get('State')}"
    assert after, f"no trade reached the table in {_RECOVERY_WAIT:.0f} s after the drop"


@pytest.mark.verifies("12")
@pytest.mark.integration_limits
@pytest.mark.timeout(200)
def test_a_newer_connection_with_one_key_takes_the_older_ones_place(
    live_session: dm.MassiveSession,
    *,
    live_config: dm.MassiveConfig,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    caplog: pytest.LogCaptureFixture,
    evidence: Evidence,
) -> None:
    """Whether Massive keeps the newer of two connections with one key, and the older table fails.

    Deliberately takes a second connection slot (marked `integration_limits`; run it alone and
    last, with no other client of the key). Streams `X:BTCUSD` trades in one session, then opens
    a second session on the same cluster that streams them too. Sessions of one process whose
    key and connection settings are all equal share their WebSocket connections, so the second
    session's `max_outage` is one second longer than the first's: that alone gives it a socket
    of its own, and the library's WARNING that the two cannot share connections must appear.

    Passes when, under the default `connection_taken` (`FAIL`), Massive closes the first
    session's connection within 45 s of the second's: the first table fails with the error that
    names the taken connection, its crypto connection shows `CLOSED`, and the second session's
    table keeps receiving trades afterward. A failure here means Massive kept the older
    connection, or refused the newer one at its key (`WAITING_SLOT`), and the handling of a
    taken connection needs revisiting; the evidence holds both sessions' rows.
    """
    first = live_session.crypto.trades("X:BTCUSD").append_only()
    first_rows = wait_rows(live_session, first, time.monotonic() + _FIRST_WAIT)
    evidence(f"first session: {len(first_rows)} trades before the second connects")
    if not first_rows:
        for line in stream_report(live_session):
            evidence(line)
        pytest.skip("the first session received no trade, so it holds no slot to compete for")
    # The live session's earlier tests may have left failed streams of their own.
    failed_before = {
        r["StreamId"] for r in table_rows(live_session.streams()) if r["State"] == "FAILED"
    }

    def newly_failed(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [r for r in rows if r["State"] == "FAILED" and r["StreamId"] not in failed_before]

    # A different connection setting keeps the second session out of the first's connections.
    config = dataclasses.replace(live_config, max_outage=live_config.max_outage + 1)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._session"):
        second_session = dm.MassiveSession(config)
    apart = any("cannot share WebSocket connections" in r.getMessage() for r in caplog.records)
    evidence(f"second session opens its own connections: {apart}")
    try:
        assert apart, "the second session shares the first's connections, so none competes"
        second = second_session.crypto.trades("X:BTCUSD").append_only()
        started = time.monotonic()
        streams = _wait_until(
            lambda: table_rows(live_session.streams()),
            lambda rows: bool(newly_failed(rows)),
            wait=_WAIT,
        )
        evidence(f"first session after {time.monotonic() - started:.1f} s:")
        for line in stream_report(live_session):
            evidence(f"    {line}")
        evidence("second session meanwhile:")
        for line in stream_report(second_session):
            evidence(f"    {line}")
        errors = [str(r["Error"]) for r in newly_failed(streams)]
        assert errors, "Massive did not close the older connection"
        assert errors[0].startswith(
            "crypto.trades: Another connection with this API key took the WebSocket "
            "connection to socket.massive.com/crypto at "
        ), errors[0]
        # The table's failure and the connection's CLOSED row land on other threads, so each
        # may show a cycle after the stream's row.
        failed = _wait_until(
            lambda: [{"Failed": bool(first.j_table.isFailed())}],
            lambda rows: rows[0]["Failed"],
            wait=_WAIT,
        )
        assert failed[0]["Failed"], "the first table never failed"
        crypto = _wait_until(
            lambda: [
                r
                for r in table_rows(live_session.connections())
                if r["Transport"] == "WebSocket" and r["AssetClass"] == "crypto"
            ],
            lambda rows: [r["State"] for r in rows] == ["CLOSED"],
            wait=_WAIT,
        )
        assert [r["State"] for r in crypto] == ["CLOSED"]
        seen = len(table_rows(second))
        later = _wait_until(
            lambda: table_rows(second), lambda rows: len(rows) > seen, wait=_FIRST_WAIT
        )
        evidence(f"second session: {len(later) - seen} trades after the first was taken")
        assert len(later) > seen, "the newer connection got no trade after taking the older's"
        assert not second.j_table.isFailed()
        second_session.release(second)
    finally:
        second_session.close()
    live_session.release(first)


@pytest.mark.timeout(330)
def test_a_crypto_stream_follows_its_watchlist(
    live_session: dm.MassiveSession,
    *,
    dh_exec_ctx: ExecutionContext,
    table_rows: TableRows,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    evidence: Evidence,
) -> None:
    """Whether a stream subscribes and unsubscribes as rows of its watchlist come and go.

    Streams `m.crypto.trades(watchlist).last_by()` from a keyed input table holding `X:BTCUSD`,
    then adds `X:ETHUSD`, deletes it, and adds it again, as a user typing in the web UI would.
    Crypto trades all the time, so each step waits at most 60 s. Passes when `X:ETHUSD` gets a
    row after it is added and joins the stream's `Symbols` in `m.streams()`; its row leaves the
    table and `Symbols` drops it once it is deleted, with `X:BTCUSD` still streaming; and after
    it is added again, the table shows no row of it from before (each `X:ETHUSD` row was
    received after the re-add) until a new trade arrives. A failure here means a watchlist edit
    did not reach Massive, or a deleted pair's row outlived its subscription. A stream Massive
    refuses skips, quoting Massive.
    """
    btc, eth = "X:BTCUSD", "X:ETHUSD"
    with dh_update_graph.shared_lock(dh_exec_ctx.update_graph):
        watchlist = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    watchlist.add(new_table([string_col("Symbol", [btc])]))
    board = live_session.crypto.trades(watchlist).last_by()

    def board_rows() -> list[dict[str, Any]]:
        return table_rows(board)

    def symbols() -> str:
        (stream,) = [
            r for r in table_rows(live_session.streams()) if r["Dataset"] == "crypto.trades"
        ]
        return str(stream["Symbols"])

    def pairs(rows: list[dict[str, Any]]) -> set[str]:
        return {str(r["Symbol"]) for r in rows}

    first = _wait_until(board_rows, lambda rows: btc in pairs(rows))
    evidence(f"{btc} first row: {btc in pairs(first)}; Symbols {symbols()!r}")
    if btc not in pairs(first):
        skip_if_not_entitled(live_session)
    assert btc in pairs(first), f"no {btc} trade arrived in {_FOLLOW_WAIT:.0f} s"

    watchlist.add(new_table([string_col("Symbol", [eth])]))
    added = _wait_until(board_rows, lambda rows: eth in pairs(rows) and eth in symbols())
    shown = symbols()
    evidence(f"after adding {eth}: pairs {sorted(pairs(added))}; Symbols {shown!r}")
    assert eth in pairs(added), f"no {eth} row arrived in {_FOLLOW_WAIT:.0f} s after adding it"
    assert eth in shown, f"m.streams() Symbols does not list {eth}: {shown!r}"

    watchlist.delete(new_table([string_col("Symbol", [eth])]))
    deleted = _wait_until(board_rows, lambda rows: eth not in pairs(rows) and symbols() == btc)
    shown = symbols()
    evidence(f"after deleting {eth}: pairs {sorted(pairs(deleted))}; Symbols {shown!r}")
    assert eth not in pairs(deleted), f"the {eth} row stayed after its watchlist row was deleted"
    assert shown == btc, f"m.streams() Symbols still reads {shown!r}"
    assert btc in pairs(deleted), f"{btc} lost its row when {eth} was deleted"

    readded_at = time.time_ns()
    watchlist.add(new_table([string_col("Symbol", [eth])]))
    at_once = [r for r in board_rows() if r["Symbol"] == eth]
    again = _wait_until(board_rows, lambda rows: eth in pairs(rows))
    rows = [r for r in again if r["Symbol"] == eth]
    stale = [r for r in at_once + rows if int(r["TimestampInsert"]) <= readded_at]
    for line in stream_report(live_session):
        evidence(line)
    evidence(f"after re-adding {eth}: rows at once {len(at_once)}; later {len(rows)}")
    live_session.release(board)
    assert not stale, f"the table showed an {eth} row from before the re-add: {stale}"
    assert rows, f"no new {eth} trade arrived in {_FOLLOW_WAIT:.0f} s after re-adding it"


@pytest.mark.timeout(180)
def test_an_empty_watchlist_waits_and_its_first_pair_connects(
    live_session: dm.MassiveSession,
    *,
    dh_exec_ctx: ExecutionContext,
    table_rows: TableRows,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    evidence: Evidence,
) -> None:
    """Whether a stream with nothing to subscribe opens no socket until its first pair arrives.

    Streams `m.crypto.trades(watchlist).last_by()` from an empty keyed input table, in a fresh
    session, so no other stream holds a crypto connection. Passes when the session's crypto
    WebSocket row in `m.connections()` reads `WAITING` with no message received and no
    reconnect, and, once `X:BTCUSD` is added, reads `CONNECTED` and a `X:BTCUSD` trade arrives;
    Bitcoin trades every few seconds at any hour. A failure means a connection opened a socket
    with nothing to stream, or a watchlist's first row did not connect it. A stream Massive
    refuses once connected skips, quoting Massive.
    """
    btc = "X:BTCUSD"
    with dh_update_graph.shared_lock(dh_exec_ctx.update_graph):
        watchlist = input_table({"Symbol": dtypes.string}, key_cols="Symbol")
    board = live_session.crypto.trades(watchlist).last_by()

    def crypto_rows() -> list[dict[str, Any]]:
        return [
            r
            for r in table_rows(live_session.connections())
            if r["Transport"] == "WebSocket" and r["AssetClass"] == "crypto"
        ]

    waiting = _wait_until(crypto_rows, lambda rows: [r["State"] for r in rows] == ["WAITING"])
    evidence(f"before the first pair: {[(r['State'], r['LastMessageAt']) for r in waiting]}")
    assert [r["State"] for r in waiting] == ["WAITING"], waiting
    (row,) = waiting
    assert row["LastMessageAt"] is None, "a waiting connection received a message"
    assert row["Reconnects"] == 0

    def board_rows() -> list[dict[str, Any]]:
        return table_rows(board)

    watchlist.add(new_table([string_col("Symbol", [btc])]))
    traded = _wait_until(board_rows, bool)
    for line in stream_report(live_session):
        evidence(line)
    states = [r["State"] for r in crypto_rows()]
    evidence(f"after adding {btc}: connection {states}; {len(traded)} rows")
    if not traded:
        skip_if_not_entitled(live_session)
    live_session.release(board)
    assert states == ["CONNECTED"], states
    assert traded, f"no {btc} trade arrived in {_FOLLOW_WAIT:.0f} s after adding it"


_HOLD = 90.0
"""Seconds the real-time-only checks keep their streams open: long enough for Massive to answer
every subscription, and to take an older connection of the key if a newer one counts against it."""

_TAKEN = "another connection with this API key took"
"""The start of the library's words, in a stream's error or a connection's last error, for a
connection Massive closed because a newer one on the key took its place."""


def _states(session: dm.MassiveSession, table_rows: TableRows) -> dict[str, tuple[str, str | None]]:
    """Return each stream's state and error in `m.streams()`, by its dataset."""
    return {
        str(r["Dataset"]): (str(r["State"]), r["Error"] and str(r["Error"]))
        for r in table_rows(session.streams())
    }


def _hold(
    session: dm.MassiveSession, table_rows: TableRows, datasets: int
) -> dict[str, tuple[str, str | None]]:
    """Wait `_HOLD` seconds, or until every one of `datasets` streams has failed.

    Returns:
        The streams' states and errors at the end (`_states`).
    """
    deadline = time.monotonic() + _HOLD
    pause = threading.Event()  # never set; only paces the wait
    states = _states(session, table_rows)
    while time.monotonic() < deadline:
        failed = [state for state, _ in states.values() if state == "FAILED"]
        if len(failed) == datasets:
            break
        pause.wait(1.0)
        states = _states(session, table_rows)
    return states


@pytest.mark.timeout(180)
def test_fmv_streams_from_its_host_beside_a_delayed_stocks_stream(
    live_config: dm.MassiveConfig,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether stocks FMV streams beside delayed stock trades, and whether either takes the other.

    Lists stocks in `MassiveConfig.delayed_subscriptions`, which moves `T.SPY` to
    `delayed.massive.com` and never applies to fair market value, then streams
    `m.stocks.fmv("SPY")`, which uses `business.massive.com`, the one host Massive documents for
    it. Waits for Massive to acknowledge both topics, holds both streams for `_HOLD` seconds,
    and records the two connections and the streams. Passes when no stream or connection
    reports that the other connection took its place (a second connection on one key that
    Massive counts as the same cluster), Massive acknowledged both topics, and both streams
    still run. Skips, quoting Massive, when Massive refused either subscription and nothing was
    taken: the library sent the request and Massive answered.
    """
    config = dataclasses.replace(
        live_config,
        delayed_subscriptions=live_config.delayed_subscriptions | {dm.Subscription.STOCKS},
    )
    session = dm.MassiveSession(config)
    try:
        trades = session.stocks.trades("SPY").blink()
        fmv = session.stocks.fmv("SPY").blink()
        acked = wait_acked(session, 2, time.monotonic() + _WAIT)
        states = _hold(session, table_rows, 2)
        last_errors = [
            str(r["LastError"]) for r in table_rows(session.connections()) if r["LastError"]
        ]
        for line in stream_report(session):
            evidence(line)
        session.release(trades)
        session.release(fmv)
    finally:
        session.close()
    evidence(f"acknowledged topics {acked}; streams after {_HOLD:.0f} s: {states}")
    taken = [error for _, error in states.values() if error and _TAKEN in error]
    taken += [error for error in last_errors if _TAKEN in error]
    assert not taken, f"one connection took the other's place: {taken}"
    refused = {
        name: error
        for name, (state, error) in states.items()
        if state == "FAILED" and error and "Massive answered the subscription to" in error
    }
    if refused:
        pytest.skip(f"Massive refused: {refused}")
    assert acked == 2, f"Massive acknowledged {acked} of the 2 topics"
    assert {name: state for name, (state, _) in states.items()} == {
        "stocks.trades": "RUNNING",
        "stocks.fmv": "RUNNING",
    }


@pytest.mark.timeout(180)
def test_admin_messages_on_the_delayed_options_feed(
    live_config: dm.MassiveConfig,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """What Massive answers to the options admin messages on its delayed feed.

    Massive documents `AD.*` only on the real-time feed, and the library sends it wherever the
    session's options streams go, so Massive's answer decides. Lists options in
    `MassiveConfig.delayed_subscriptions`, streams `m.options.admin_messages()` on
    `delayed.massive.com`, and records Massive's acknowledgement or refusal. Passes when Massive
    acknowledged `AD.*` there and the stream runs; messages come only with FLEX activity or an
    exchange's notice, so no row is required. Skips, quoting Massive, when Massive refused it:
    the refusal is then the table's error, which the evidence records.
    """
    config = dataclasses.replace(
        live_config,
        delayed_subscriptions=live_config.delayed_subscriptions | {dm.Subscription.OPTIONS},
    )
    session = dm.MassiveSession(config)
    try:
        table = session.options.admin_messages().append_only()
        acked = wait_acked(session, 1, time.monotonic() + _WAIT)
        states = _states(session, table_rows)
        for line in stream_report(session):
            evidence(line)
        session.release(table)
    finally:
        session.close()
    evidence(f"AD.* acknowledged topics {acked}; streams {states}")
    state, error = states["options.admin_messages"]
    if state == "FAILED" and error and "Massive answered the subscription to" in error:
        pytest.skip(f"Massive refused: {error}")
    assert acked, "Massive neither acknowledged nor refused AD.* on the delayed options feed"
    assert state == "RUNNING", f"the admin messages stream is {state}: {error}"


_REFUSAL_LINGER = 10.0
"""Seconds the two-topic check keeps listening after the stream fails or the wait ends."""

_FMV_SYMBOLS = ("AAPL", "MSFT")
"""Two stocks whose fair market value topics one subscribe frame asks for together."""


def _status_messages(frames: list[CapturedReply], hosts: set[str]) -> list[tuple[str, str]]:
    """Return the `(status, message)` of each status object in the frames of the given hosts.

    Returns:
        Each status object Massive sent on one of the hosts, in order; a frame that is not a
        JSON list of objects holds none.
    """
    found: list[tuple[str, str]] = []
    for reply in frames:
        if reply.host not in hosts:
            continue
        try:
            objects = json.loads(reply.text)
        except ValueError:
            continue
        if not isinstance(objects, list):
            continue
        found.extend(
            (str(o.get("status")), str(o.get("message")))
            for o in objects
            if isinstance(o, dict) and o.get("ev") == "status"
        )
    return found


@pytest.mark.settles("148")
@pytest.mark.timeout(110)
def test_a_refusal_of_two_topics_in_one_step_names_them(
    live_config: dm.MassiveConfig,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    evidence: Evidence,
) -> None:
    """Whether Massive names each topic in its refusal of a subscribe frame of two topics.

    Streams `m.stocks.fmv` of two symbols in one session, which subscribes both fair market
    value topics in one frame to `business.massive.com`, and records every status object Massive
    sends on that connection. Waits up to 45 s for both topics to be acknowledged or the stream
    to fail, then another 10 s so that a second refusal, if Massive sends one per topic, is
    captured too. Records, for each status, which of the two symbols its message names. Skips,
    quoting the statuses, when Massive acknowledged both topics and refused none: the key is
    entitled and there is no refusal to read. Passes when Massive refused with at least one
    status, since the refusal is the evidence, which reads three ways. A refusal naming both
    symbols, or two refusals each naming one, mean Massive names the topics it refuses. A
    refusal naming neither symbol means a step of several topics is refused as a whole, and the
    library cannot tell which topic Massive refused. Skips, quoting Massive, when a stream
    failed with a refusal and no status was captured. Fails when Massive neither acknowledged
    both topics nor refused any within the wait, since there is then nothing to record.
    """
    frames: list[CapturedReply] = []
    lock = threading.Lock()

    def keep(reply: CapturedReply) -> None:
        with lock:
            frames.append(reply)

    set_reply_capture(keep)
    try:
        session = dm.MassiveSession(live_config)
        try:
            table = session.stocks.fmv(list(_FMV_SYMBOLS)).blink()
            acked = wait_acked(session, len(_FMV_SYMBOLS), time.monotonic() + _WAIT)
            if acked < len(_FMV_SYMBOLS):
                threading.Event().wait(_REFUSAL_LINGER)  # never set; only waits for later frames
            for line in stream_report(session):
                evidence(line)
            hosts = {
                str(r["Host"])
                for r in table_rows(session.connections())
                if r["Transport"] == "WebSocket"
            }
            with lock:
                statuses = _status_messages(list(frames), hosts)
            refused = [message for status, message in statuses if status == "error"]
            if not refused:
                skip_if_not_entitled(session)
            session.release(table)
        finally:
            session.close()
    finally:
        set_reply_capture(None)
    evidence(f"acknowledged topics {acked} of {len(_FMV_SYMBOLS)}; hosts {sorted(hosts)}")
    for status, message in statuses:
        named = [symbol for symbol in _FMV_SYMBOLS if symbol in message]
        evidence(f"status {status!r}: {message!r}; names {named or 'neither symbol'}")
    if acked == len(_FMV_SYMBOLS) and not refused:
        pytest.skip(
            f"Massive acknowledged both fair market value topics, so nothing was refused: "
            f"{[message for _, message in statuses]}"
        )
    assert refused, (
        f"Massive neither acknowledged both topics nor refused any in {_WAIT:.0f} s: {statuses}"
    )
