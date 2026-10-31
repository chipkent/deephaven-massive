"""Live checks of the session's WebSocket handling: the feeds a plan may use, and the slot limit.

Each test records the session's diagnostics and Massive's messages (`evidence`). Streams
subscribe to one symbol and are released at once. The slot test opens a second connection to a
cluster on purpose: it is marked `integration_limits`, runs only with `-m integration_limits`,
and should run alone and last, with no other client of the key. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"

_WAIT = 45.0
"""Seconds a test waits for a frame or a state change."""

_FIRST_WAIT = 20.0
"""Seconds the slot test waits for the first session's trades; crypto trades all the time."""


def _wait_for_state(
    session: dm.MassiveSession, table_rows: TableRows, *, states: set[str]
) -> list[dict[str, Any]]:
    """Wait until one of a session's WebSocket connections is in one of `states`, or time out.

    Returns:
        The session's connection rows when the wait ended.
    """
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + _WAIT
    while True:
        rows = table_rows(session.connections())
        if any(r["State"] in states for r in rows) or time.monotonic() >= deadline:
            return rows
        pause.wait(0.5)


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
        api_key=delayed_key, ws_hosts={dm.AssetClass.STOCKS: "socket.massive.com"}
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
@pytest.mark.settles("70")
@pytest.mark.timeout(110)
def test_whether_massive_serves_a_real_time_key_on_the_delayed_feed(
    live_config: dm.MassiveConfig,
    *,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether Massive serves a real-time key on its delayed feed, `delayed.massive.com`.

    A subscription listed in `MassiveConfig.delayed_subscriptions` always streams from the
    delayed feed, even on a real-time plan. With a real-time Stocks plan, lists stocks as
    delayed, subscribes `AM.AAPL`, and records the stream's host, acknowledgement, rows, and any
    error. Reading it: an acknowledgement means a real-time key may choose delayed data; a
    refusal means the setting needs a delayed plan, and its message must say so.
    """
    config = dataclasses.replace(live_config, delayed_subscriptions={dm.Subscription.STOCKS})
    errors = _subscribe_minute_bars(
        config,
        wait_rows=wait_rows,
        table_rows=table_rows,
        stream_report=stream_report,
        evidence=evidence,
    )
    evidence(f"refused on the delayed feed: {bool(errors)}")


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("12")
@pytest.mark.integration_limits
@pytest.mark.timeout(100)
def test_a_second_connection_with_one_key_waits_for_a_slot(
    live_session: dm.MassiveSession,
    *,
    live_config: dm.MassiveConfig,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """What happens when a second session with the same key connects to a busy cluster.

    Deliberately takes a second connection slot (marked `integration_limits`; run it alone and
    last, with no other client of the key). Streams `X:BTCUSD` trades in one session, opens a
    second session on the same cluster, and records both sessions' connection states and
    errors while both are open and after the second closes. Reading it: the second waiting
    (`WAITING_SLOT`, a `max_connections` message) while the first keeps running confirms the
    library's slot handling; the first being dropped instead means Massive keeps the newer
    connection, and the slot rule needs revisiting.
    """
    first = live_session.crypto.trades("X:BTCUSD").append_only()
    first_rows = wait_rows(live_session, first, time.monotonic() + _FIRST_WAIT)
    evidence(f"first session: {len(first_rows)} trades before the second connects")
    if not first_rows:
        for line in stream_report(live_session):
            evidence(line)
        pytest.skip("the first session received no trade, so it may hold no slot to compete for")
    second_session = dm.MassiveSession(live_config)
    try:
        second = second_session.crypto.trades("X:BTCUSD").append_only()
        started = time.monotonic()
        _wait_for_state(second_session, table_rows, states={"WAITING_SLOT", "FAILED"})
        evidence(f"second session after {time.monotonic() - started:.1f} s:")
        for line in stream_report(second_session):
            evidence(f"    {line}")
        evidence("first session while the second is open:")
        for line in stream_report(live_session):
            evidence(f"    {line}")
        second_session.release(second)
    finally:
        second_session.close()
    evidence("first session after the second closed:")
    for line in stream_report(live_session):
        evidence(f"    {line}")
    live_session.release(first)
