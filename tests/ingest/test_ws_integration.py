"""Live checks of the session's WebSocket handling: the delayed-key verdict and the slot limit.

Each test records the session's diagnostics and Massive's messages (`evidence`). Streams
subscribe to one symbol and are released at once. The slot test is the one test that opens a
second connection to a cluster, on purpose, and runs only with `MASSIVE_LIVE_SECOND_CONNECTION`
set.
"""

from __future__ import annotations

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

TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"

_WAIT = 45.0
"""Seconds a test waits for a frame or a state change."""


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


@pytest.mark.timeout(110)
def test_text_of_the_delayed_key_refusal_on_the_real_time_host(
    live_env: Callable[[str], str],
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    key = live_env("MASSIVE_DELAYED_API_KEY")
    config = dm.MassiveConfig(api_key=key, ws_hosts={dm.AssetClass.STOCKS: "socket.massive.com"})
    session = dm.MassiveSession(config)
    try:
        table = session.stocks.trades("AAPL").append_only()
        rows = wait_rows(session, table, _WAIT)
        session.release(table)
        for line in stream_report(session):
            evidence(line)
        evidence(f"T.AAPL rows received on socket.massive.com with a delayed key: {len(rows)}")
    finally:
        session.close()


@pytest.mark.timeout(110)
def test_a_second_connection_with_one_key_waits_for_a_slot(
    live_session: dm.MassiveSession,
    *,
    live_opt_in: Callable[[str], None],
    live_config: dm.MassiveConfig,
    wait_rows: WaitRows,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    live_opt_in("MASSIVE_LIVE_SECOND_CONNECTION")
    first = live_session.crypto.trades("X:BTCUSD").append_only()
    first_rows = wait_rows(live_session, first, _WAIT)
    evidence(f"first session: {len(first_rows)} trades before the second connects")
    second_session = dm.MassiveSession(live_config)
    try:
        second = second_session.crypto.trades("X:BTCUSD").append_only()
        started = time.monotonic()
        _wait_for_state(second_session, table_rows, states={"WAITING_SLOT", "FAILED"})
        evidence(f"second session after {time.monotonic() - started:.1f} s:")
        for line in stream_report(second_session):
            evidence(f"    {line}")
        second_session.release(second)
    finally:
        second_session.close()
    evidence("first session after the second closed:")
    for line in stream_report(live_session):
        evidence(f"    {line}")
    live_session.release(first)
