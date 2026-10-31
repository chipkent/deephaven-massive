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

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
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
@pytest.mark.timeout(160)
def test_index_values_stream_on_the_resolved_host(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    rows_due: Callable[[str], tuple[str, bool]],
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether index values (`V.I:SPX`) stream on the host the session resolves for indices.

    The documentation names `business.` for `V` while the plan table names `socket.` or `delayed.`;
    the library uses the indices class's own host. Records the session's connection and stream
    (host, state, acknowledgement, error) and asserts that Massive acknowledged the topic, whatever
    the hour. Values are required only once the S&P indices have been open longer than the plan's
    feed delay, counted from 09:30 New York time (15 minutes on Indices Starter, none on real-time
    plans); before that, and while they are closed, the test skips after the acknowledgement.
    Reading it: the host the evidence names, with rows, is the host that serves index values; a
    refusal names the host to use instead.
    """
    table = live_session.indices.values("I:SPX").append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    status, due = rows_due("indices")
    deadline = time.monotonic() + _STREAM_WAIT
    rows = wait_rows(live_session, table, deadline) if due else []
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"V.I:SPX rows received: {len(rows)}; first: {rows[:1]}")
    assert acked, "Massive acknowledged no index value topic (see the connection above)"
    if not due:
        pytest.skip(
            f"no index value is due: the S&P indices are {status}, or within the feed delay"
        )
    assert rows, f"no index value arrived in {_STREAM_WAIT:.0f} s, though one was due"


@pytest.mark.needs("stocks:business")
@pytest.mark.verifies("6")
@pytest.mark.timeout(100)
def test_minute_bars_on_the_host_resolved_for_a_business_key(
    live_session: dm.MassiveSession,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether a Business key's minute bars (`AM.AAPL`) stream from `business.massive.com`.

    The library resolves `business.massive.com` for a Business key's snapshot shape and warns.
    With Stocks Business held, streams `AM.AAPL` and records the session's host, state, and
    acknowledgement. Passes when the stocks connection is on `business.massive.com` and Massive
    acknowledged the topic there, which it does whether or not the market is open; a refusal
    there means the base Business tick host is another one.
    """
    table = live_session.stocks.bars("AAPL", dm.Timespan.MINUTE).append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    hosts = {str(r["Host"]) for r in table_rows(live_session.connections())}
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"AM.AAPL hosts {sorted(hosts)}; acknowledged topics {acked}")
    assert hosts, "the session opened no connection"
    assert all("business.massive.com" in host for host in hosts), f"connected to {sorted(hosts)}"
    assert acked, "Massive acknowledged no AM.AAPL topic on business.massive.com"


@pytest.mark.needs("futures:starter")
@pytest.mark.settles("7", "69")
def test_which_recency_labels_the_futures_snapshot_carries(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which recency labels the futures snapshot carries: its envelope status and timeframes.

    The `timeframe` fields are in the OpenAPI specification and absent from the documentation
    page, so a delayed futures plan is told by the envelope status alone (`DELAYED`). Records
    the envelope status and every `timeframe` field of three ES rows. Reading it: on a delayed
    plan (Futures Starter or Developer), a `DELAYED` status confirms that the stream verb and
    `fetch()` refuse delayed futures data unless futures are listed in
    `MassiveConfig.delayed_subscriptions`; `OK` means only Massive's refusal on the real-time
    feed catches a delayed plan.
    """
    page = live_rest.get(
        source(Transport.REST, "/futures/v1/snapshot"),
        {},
        {"product_code": "ES", "limit": 3},
        bucket="futures",
    )
    evidence(f"futures snapshot envelope status: {page.status!r}")
    for row in page.rows:
        evidence(f"futures snapshot {row.get('ticker')}: {_timeframe_fields(row) or 'none'}")
    assert page.rows
