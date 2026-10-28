"""Live checks of the WebSocket host decision: the index value host, Business keys, and evidence.

Each test records what the session decided and what Massive answered (`evidence`). A stream
subscribes to one symbol, waits for its first rows, and is released at once; each session
holds one connection per cluster and is closed at teardown.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"

_STREAM_WAIT = 100.0
"""Seconds a stream test waits for its first frame; these streams tick in market hours."""


def _timeframe_fields(value: object, prefix: str = "") -> list[str]:
    """List the dotted paths of every key named like `timeframe` in a decoded JSON value.

    Returns:
        The paths, such as `["last_trade.timeframe"]`.
    """
    if not isinstance(value, dict):
        return []
    found = []
    for key, inner in value.items():
        path = f"{prefix}{key}"
        if "timeframe" in key:
            found.append(f"{path}={inner!r}")
        found.extend(_timeframe_fields(inner, f"{path}."))
    return found


@pytest.mark.timeout(110)
def test_index_values_stream_on_the_resolved_host(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    table = live_session.indices.values("I:SPX").append_only()
    rows = wait_rows(live_session, table, _STREAM_WAIT)
    live_session.release(table)
    for line in stream_report(live_session):
        evidence(line)
    evidence(f"V.I:SPX rows received: {len(rows)}; first: {rows[:1]}")
    if not rows:
        pytest.skip("no index value arrived (see the connection and stream above)")


@pytest.mark.timeout(110)
def test_minute_bars_on_the_host_resolved_for_a_business_key(
    live_env: Callable[[str], str],
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    key = live_env("MASSIVE_BUSINESS_API_KEY")
    session = dm.MassiveSession(dm.MassiveConfig(api_key=key))
    try:
        table = session.stocks.bars("AAPL").append_only()
        rows = wait_rows(session, table, _STREAM_WAIT)
        session.release(table)
        for line in stream_report(session):
            evidence(line)
        evidence(f"AM.AAPL rows received: {len(rows)}; first: {rows[:1]}")
    finally:
        session.close()


def test_which_timeframe_fields_the_futures_snapshot_carries(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    rows = live_rest.get(
        source(Transport.REST, "/futures/v1/snapshot"), {}, {"product_code": "ES", "limit": 3}
    ).rows
    for row in rows:
        evidence(f"futures snapshot {row.get('ticker')}: {_timeframe_fields(row) or 'none'}")
    assert rows
