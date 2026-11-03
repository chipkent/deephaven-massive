"""Live checks of the WebSocket host decision: the index value host, Business keys, and evidence.

Each test records what the session decided and what Massive answered (`evidence`). A stream
subscribes to one symbol, waits for its first rows, and is released at once; each session
holds one connection per cluster and is closed at teardown. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Page, RestClient, Transport, source
from tests._live.live_run.stream_units import DELAYED, RECENCY

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from tests._live.plans import LivePlans

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
@pytest.mark.verifies("5")
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
    Passes when the class's own host acknowledges the topic and, once values are due, sends
    them; a refusal names the host to use instead.
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
def test_minute_bars_of_a_business_key_on_the_real_time_host(
    live_session: dm.MassiveSession,
    *,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether a Business key's minute bars (`AM.AAPL`) stream from `socket.massive.com`.

    A base Business key's snapshot carries no `timeframe`, so the library streams its TICK
    channels from the real-time host, guessing no other. With Stocks Business held, streams
    `AM.AAPL` and records the session's host, state, and acknowledgement. Passes when the
    stocks connection is on `socket.massive.com` and Massive acknowledged the topic there,
    which it does whether or not the market is open; a refusal there means the base Business
    TICK host is another one, which the user pins with `MassiveConfig.websocket_hosts`.
    """
    table = live_session.stocks.bars("AAPL", dm.Timespan.MINUTE).append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    hosts = {str(r["Host"]) for r in table_rows(live_session.connections())}
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    evidence(f"AM.AAPL hosts {sorted(hosts)}; acknowledged topics {acked}")
    assert hosts, "the session opened no connection"
    assert all("socket.massive.com" in host for host in hosts), f"connected to {sorted(hosts)}"
    assert acked, "Massive acknowledged no AM.AAPL topic on socket.massive.com"


_FUTURES_SNAPSHOT_ROWS = 100
"""ES contracts the futures snapshot checks read in one request: sorted by ticker, the first
rows are far-dated contracts that never trade and carry no recency label, so a few rows can
miss every traded one."""


def _futures_snapshot(live_rest: RestClient) -> Page:
    """Read up to `_FUTURES_SNAPSHOT_ROWS` ES rows of the futures snapshot raw, in one request.

    Returns:
        The page: its envelope `status` and its `rows`.
    """
    return live_rest.get(
        source(Transport.REST, "/futures/v1/snapshot"),
        {},
        {"product_code": "ES", "limit": _FUTURES_SNAPSHOT_ROWS},
        bucket="futures",
    )


def _futures_labels(page: Page, evidence: Evidence) -> int:
    """Record the futures snapshot's envelope status and each labelled row's `timeframe` fields.

    A row's ticker sits under `details`. Rows without a label are counted, not listed.

    Returns:
        How many rows carry at least one `timeframe` field.
    """
    evidence(f"futures snapshot envelope status: {page.status!r}")
    labelled = 0
    for row in page.rows:
        fields = _timeframe_fields(row)
        if fields:
            labelled += 1
            details = row.get("details")
            ticker = details.get("ticker") if isinstance(details, dict) else None
            evidence(f"futures snapshot {ticker}: {fields}")
    evidence(f"futures snapshot rows: {len(page.rows)}; with a timeframe: {labelled}")
    return labelled


@pytest.mark.needs("futures:starter")
@pytest.mark.verifies("7")
def test_the_futures_snapshot_carries_recency_labels(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether the futures snapshot carries `timeframe` recency labels on its traded contracts.

    The `timeframe` fields are in the OpenAPI specification and absent from the documentation
    page; the stream host decision reads them. Reads up to 100 ES contracts in one request, as
    the host probe does, and records the envelope status and the labels of every row that
    carries one, by ticker. Passes when at least one row carries a `timeframe` field: only
    traded contracts do, and the front ES contracts trade in every session.
    """
    page = _futures_snapshot(live_rest)
    labelled = _futures_labels(page, evidence)
    assert page.rows
    assert labelled, "no futures snapshot row carries a timeframe field"


@pytest.mark.needs("futures:delayed")
@pytest.mark.settles("69")
def test_which_envelope_status_a_delayed_futures_plan_gets(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which envelope status a delayed futures plan's snapshot carries: `DELAYED` or `OK`.

    Only a delayed plan (Futures Starter or Developer) can answer this, so a real-time plan
    skips it. Records the envelope status and the labels of every labelled ES row. Reading it:
    a `DELAYED` status confirms that the stream verb and `fetch()` refuse delayed futures data
    unless futures are listed in `MassiveConfig.delayed_subscriptions`; `OK` means only
    Massive's refusal on the real-time feed catches a delayed plan.
    """
    page = _futures_snapshot(live_rest)
    _futures_labels(page, evidence)
    assert page.rows


@pytest.mark.needs("options:delayed")
@pytest.mark.settles("71")
def test_which_recency_labels_a_delayed_option_chain_carries(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which recency labels a delayed options plan's SPY chain carries: its status and timeframes.

    The chain is the options host check's snapshot, and `fetch()` and `poll()` of options
    snapshots refuse a `DELAYED` label unless options are listed in
    `MassiveConfig.delayed_subscriptions`. Only a delayed plan (Options Starter or Developer)
    can answer this, so a real-time plan skips it. Records the envelope status and every
    `timeframe` field of three contracts. Reading it: a `DELAYED` status or
    `last_trade.timeframe` confirms the refusal; neither means only Massive's refusal on the
    real-time feed catches a delayed options plan, and the chain's tables show `Recency`
    `UNKNOWN`.
    """
    page = live_rest.get(
        source(Transport.REST, "/v3/snapshot/options/{underlyingAsset}"),
        {"underlyingAsset": "SPY"},
        {"limit": 3},
        bucket="options",
    )
    evidence(f"option chain envelope status: {page.status!r}")
    for row in page.rows:
        evidence(f"option chain row: {_timeframe_fields(row) or 'none'}")
    assert page.rows


_DELAYED_ORDER = ("stocks", "futures", "indices", "options")
"""The classes the default-configuration check tries, first held on a delayed plan first."""


@pytest.mark.timeout(100)
def test_a_delayed_plan_is_refused_on_the_default_configuration(
    live_plans: LivePlans,
    *,
    front_contract: Callable[[str, date], str],
    index_symbol: str,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether a session on the default configuration shows a delayed plan's refusal.

    The live sessions list every class on a delayed plan in
    `MassiveConfig.delayed_subscriptions`, which hides what a user who lists nothing meets.
    This check takes the first class the key holds on a delayed plan (Stocks, Futures,
    Indices, then Options) and, in a session with the key alone and nothing listed, streams
    one symbol's minute bars (index values for indices) or, for options, fetches the SPY chain.
    Passes when the library refused it with an `EntitlementError`, or the stream failed with a
    refusal, and the error says what to do; the evidence quotes it. Skips without a delayed
    plan.
    """
    held = [
        name for name in _DELAYED_ORDER if RECENCY.get((name, live_plans.tiers[name])) == DELAYED
    ]
    if not held:
        pytest.skip(
            "needs a delayed plan: Stocks, Options, or Futures Starter or Developer, or "
            "Indices Starter"
        )
    name = held[0]
    config = dm.MassiveConfig(api_key=os.environ["MASSIVE_API_KEY"].strip())
    session = dm.MassiveSession(config)
    errors: list[str] = []
    try:
        try:
            if name == "options":
                session.options.chain("SPY").fetch()
            else:
                today = datetime.now(UTC).date()
                queries = {
                    "stocks": lambda: session.stocks.bars("AAPL", dm.Timespan.MINUTE),
                    "futures": lambda: session.futures.bars(
                        front_contract("ES", today), dm.Timespan.MINUTE
                    ),
                    "indices": lambda: session.indices.values(index_symbol),
                }
                table = queries[name]().append_only()
                pause = threading.Event()  # never set; only paces the polling
                deadline = time.monotonic() + _STREAM_WAIT
                while time.monotonic() < deadline and not errors:
                    errors = [
                        str(r["Error"])
                        for r in table_rows(session.streams())
                        if r["State"] == "FAILED"
                    ]
                    pause.wait(0.25)
                for line in stream_report(session):
                    evidence(line)
                session.release(table)
        except dm.EntitlementError as error:
            errors = [str(error)]
    finally:
        session.close()
    evidence(f"{name} on the default configuration: {errors or 'served'}")
    assert errors, f"a delayed {name} plan was served on the default configuration"
    assert "delayed" in errors[0].lower(), f"the refusal names no remedy: {errors[0]}"
