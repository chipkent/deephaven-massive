"""Live checks of the WebSocket host decision: the index value host, Business keys, and evidence.

Each test records what the session decided and what Massive answered (`evidence`). A stream
subscribes to one symbol, waits for its first rows, and is released at once; each session
holds one connection per cluster and is closed at teardown. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import RestClient, Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

    import deephaven_massive as dm

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Evidence: TypeAlias = "Callable[[str], None]"

_STREAM_WAIT = 60.0
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


@pytest.mark.needs("indices:starter")
@pytest.mark.settles("5")
@pytest.mark.timeout(100)
def test_index_values_stream_on_the_resolved_host(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether index values (`V.I:SPX`) stream on the host the session resolves for indices.

    The documentation names `business.` for `V` while the plan table names `socket.` or
    `delayed.`; the library uses the indices class's own host. Records the session's
    connection and stream (host, state, acknowledgement, error) and the first rows. Reading it:
    rows on the resolved host confirm it; a refusal names the host to use instead. Runs in
    market hours only.
    """
    table = live_session.indices.values("I:SPX").append_only()
    rows = wait_rows(live_session, table, time.monotonic() + _STREAM_WAIT)
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"V.I:SPX rows received: {len(rows)}; first: {rows[:1]}")
    if not rows:
        pytest.skip("no index value arrived (see the connection and stream above)")


@pytest.mark.needs("stocks:business")
@pytest.mark.settles("6")
@pytest.mark.timeout(100)
def test_minute_bars_on_the_host_resolved_for_a_business_key(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Which host a base Business key's minute bars (`AM.AAPL`) stream from.

    The library resolves `business.massive.com` for a Business key's snapshot shape and warns.
    With Stocks Business declared, records the session's host, state, acknowledgement, and
    first rows. Reading it: `AM.AAPL` acknowledged on `business.` confirms the rule; a refusal
    there means the base Business tick host is another one.
    """
    table = live_session.stocks.bars("AAPL").append_only()
    rows = wait_rows(live_session, table, time.monotonic() + _STREAM_WAIT)
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"AM.AAPL rows received: {len(rows)}; first: {rows[:1]}")


@pytest.mark.needs("futures:starter")
@pytest.mark.settles("7")
def test_which_timeframe_fields_the_futures_snapshot_carries(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which `timeframe` fields the futures snapshot carries, which the host decision reads.

    They are in the OpenAPI specification and absent from the documentation page. Records every
    `timeframe` field of three ES rows. Reading it: present fields can drive the futures host
    decision; absent ones confirm the library's fallback (`socket.`, recency unknown).
    """
    rows = live_rest.get(
        source(Transport.REST, "/futures/v1/snapshot"), {}, {"product_code": "ES", "limit": 3}
    ).rows
    for row in rows:
        evidence(f"futures snapshot {row.get('ticker')}: {_timeframe_fields(row) or 'none'}")
    assert rows
