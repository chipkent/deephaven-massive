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
import logging
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven import update_graph as dh_update_graph
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._massive import WsClient
from tests._live import streams

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
MarketStatus: TypeAlias = "Callable[[str], str]"

_WAIT = 45.0
"""Seconds a test waits for a frame or a state change."""

_FIRST_WAIT = 20.0
"""Seconds the taken-connection test waits for a session's trades; crypto trades all the time."""

_DELAYED_FEED_LAG = timedelta(minutes=15)
"""How far Massive's delayed stocks feed lags the market: its first trade of a session comes
this long after the session opens."""

_RECOVERY_WAIT = 120.0
"""Seconds the reconnect check waits for the stream to deliver rows again after the drop."""

_FOLLOW_WAIT = 60.0
"""Seconds the watchlist check waits for each step: a pair's first trade, or a row's leaving.
Bitcoin and Ether trade every few seconds at any hour."""


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
) -> list[str]:
    """Stream `AM.AAPL` in a session of its own, record what happened, and return the errors.

    Returns:
        The stream's `Error` and the connection's `LastError`, where set.
    """
    session = dm.MassiveSession(config)
    try:
        table = session.stocks.bars("AAPL", dm.Timespan.MINUTE).append_only()
        rows = wait_rows(session, table, time.monotonic() + _WAIT)
        for line in stream_report(session):
            evidence(line)
        errors = [str(r["Error"]) for r in table_rows(session.streams()) if r["Error"]]
        errors += [str(r["LastError"]) for r in table_rows(session.connections()) if r["LastError"]]
        session.release(table)
        evidence(f"AM.AAPL rows received: {len(rows)}; errors: {errors or 'none'}")
    finally:
        session.close()
    return errors


@pytest.mark.needs("stocks:starter")
@pytest.mark.settles("8")
@pytest.mark.timeout(110)
def test_what_massive_says_to_a_delayed_key_on_the_real_time_feed(
    delayed_key: str,
    *,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """What Massive answers when a delayed plan subscribes on the real-time feed.

    A subscription not listed in `MassiveConfig.delayed_subscriptions` always streams from
    `socket.massive.com`, and a delayed plan is refused there. Pins stocks to that host with a
    delayed key (`MASSIVE_DELAYED_API_KEY`, or the main key when Stocks is on Starter or
    Developer), so the snapshot check does not refuse the verb first, subscribes `AM.AAPL`,
    minute bars, which every plan from Starter includes, and records the stream's error and the
    connection's last error. The refusal does not depend on the market being open. Passes when
    Massive refused the stream; the evidence quotes its text, which the error message shows.
    """
    config = dm.MassiveConfig(
        api_key=delayed_key, websocket_hosts={dm.AssetClass.STOCKS: "socket.massive.com"}
    )
    errors = _subscribe_minute_bars(
        config,
        wait_rows=wait_rows,
        table_rows=table_rows,
        stream_report=stream_report,
        evidence=evidence,
    )
    assert errors, "Massive served a delayed key on the real-time feed"


@pytest.mark.needs("stocks:advanced")
@pytest.mark.verifies("70")
@pytest.mark.timeout(1200)
def test_whether_massive_serves_a_real_time_key_on_the_delayed_feed(
    live_config: dm.MassiveConfig,
    *,
    wait_acked: WaitAcked,
    wait_rows: WaitRows,
    market_status: MarketStatus,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether Massive serves a real-time key on its delayed feed, `delayed.massive.com`.

    A subscription listed in `MassiveConfig.delayed_subscriptions` always streams from the
    delayed feed, even on a real-time plan. With a real-time Stocks plan, lists stocks as
    delayed, subscribes `T.AAPL` (trades, which flow in the extended hours too), and records
    the stream's host, acknowledgement, rows, and any error. While the stock market is closed
    the test skips after the acknowledgement. While it is open, it waits until the session
    has been open longer than the delayed feed's 15 minutes, then up to 45 s for a trade, and
    requires one, so the test runs up to about 17 minutes in the first quarter hour of a
    session. Passes when Massive acknowledged the subscription without refusing it and, in an
    open market, sent a trade: a real-time key may choose delayed data. A refusal would mean
    the setting needs a delayed plan, and the library's message would have to say so.
    """
    config = dataclasses.replace(live_config, delayed_subscriptions={dm.Subscription.STOCKS})
    session = dm.MassiveSession(config)
    try:
        table = session.stocks.trades("AAPL").append_only()
        acked = wait_acked(session, 1, time.monotonic() + _WAIT)
        status = market_status("stocks")
        now = datetime.now(UTC)
        opened = streams.opened_at("stocks", status, family="trades", now=now)
        rows: list[dict[str, Any]] = []
        if opened is not None:
            lag = max((opened + _DELAYED_FEED_LAG - now).total_seconds(), 0.0)
            evidence(f"the session opened {opened}; waiting {lag:.0f} s for the feed's lag")
            rows = wait_rows(session, table, time.monotonic() + lag + _WAIT)
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
    assert rows, f"no AAPL trade arrived on the delayed feed of an open market ({status})"


@pytest.mark.needs("currencies:starter")
@pytest.mark.timeout(200)
def test_a_stream_comes_back_with_rows_after_its_socket_drops(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    monkeypatch: pytest.MonkeyPatch,
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
    slot still held waits for it.
    """
    sockets: list[tuple[WsClient, _Socket]] = []
    run_generation = cast(
        "Callable[[WsClient, _Socket], Awaitable[object]]", WsClient._run_generation
    )

    async def recording(self: WsClient, websocket: _Socket) -> object:
        sockets.append((self, websocket))
        return await run_generation(self, websocket)

    monkeypatch.setattr(WsClient, "_run_generation", recording)
    table = live_session.crypto.trades("X:BTCUSD").append_only()
    first = wait_rows(live_session, table, time.monotonic() + _FIRST_WAIT)
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


@pytest.mark.needs("currencies:starter")
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
            "another connection with this API key took the WebSocket connection to "
            "socket.massive.com/crypto at "
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


@pytest.mark.needs("currencies:starter")
@pytest.mark.timeout(330)
def test_a_crypto_stream_follows_its_watchlist(
    live_session: dm.MassiveSession,
    *,
    dh_exec_ctx: ExecutionContext,
    table_rows: TableRows,
    stream_report: Report,
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
    did not reach Massive, or a deleted pair's row outlived its subscription.
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
