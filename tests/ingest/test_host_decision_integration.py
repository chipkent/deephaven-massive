"""Live checks of the WebSocket host decision: the index value host, unlabelled keys, evidence.

Each test records what the session decided and what Massive answered (`evidence`). A stream
subscribes to one symbol, waits for its first rows, and is released at once; each session
holds one connection per cluster and is closed at teardown. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Page, RestClient, Transport, source
from tests._live import recency

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from tests._live.recency import RecencyAnswer

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession], None]"
RecencyLabel: TypeAlias = "Callable[[dm.Subscription], RecencyAnswer]"

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


@pytest.mark.verifies("5")
@pytest.mark.timeout(160)
def test_index_values_stream_on_the_resolved_host(
    live_session: dm.MassiveSession,
    live_config: dm.MassiveConfig,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    rows_due: Callable[[str], tuple[str, bool]],
    skip_if_not_entitled: SkipIfNotEntitled,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether index values (`V.I:NDX`) stream on the host the session resolves for indices.

    The documentation names `business.massive.com` for `V` while Massive's table of feeds names
    `socket.` or `delayed.`; the library uses the indices class's own host. Any index serves the
    question, so the test reads the Nasdaq-100. Records the session's connection and stream
    (host, state, acknowledgement, error) and asserts that Massive acknowledged the topic,
    whatever the hour; a refused stream skips, quoting Massive. Values are due from 09:30 New
    York time until the 16:00 close (13:00 on a half trading day), by the market's clock alone,
    whatever Massive's status says after it; at any other time the test skips after the
    acknowledgement. The delayed feed lags by an amount the tests do not know, so when the
    owner's settings list indices as delayed, no value skips instead of failing. Passes when
    the class's own host acknowledges the topic and, once values are due, sends them; a
    refusal names the host to use instead.
    """
    table = live_session.indices.values("I:NDX").append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    status, due = rows_due("indices")
    deadline = time.monotonic() + _STREAM_WAIT
    rows = wait_rows(live_session, table, deadline) if due else []
    for line in stream_report(live_session):
        evidence(line)
    skip_if_not_entitled(live_session)
    live_session.release(table)
    evidence(f"V.I:NDX rows received: {len(rows)}; first: {rows[:1]}")
    assert acked, "Massive acknowledged no index value topic (see the connection above)"
    if not due:
        pytest.skip(f"no index value is due: the indices are {status}, or past the close")
    if not rows and dm.Subscription.INDICES in live_config.delayed_subscriptions:
        pytest.skip(f"no row on the delayed feed in {_STREAM_WAIT:.0f} s, though one was due")
    assert rows, f"no index value arrived in {_STREAM_WAIT:.0f} s, though one was due"


@pytest.mark.verifies("6")
@pytest.mark.timeout(100)
def test_minute_bars_of_an_unlabelled_key_stream_from_the_real_time_host(
    live_session: dm.MassiveSession,
    *,
    recency_label: RecencyLabel,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether minute bars (`AM.AAPL`) of a key whose snapshot has no label stream from `socket.`.

    When a key's stocks snapshot carries no `timeframe`, the library streams its TICK channels
    from the real-time host, `socket.massive.com`, guessing no other. First asks Massive how
    it labels the key's stocks snapshot (`recency_label`), and skips, quoting the answer, when
    the snapshot carries a label or Massive refuses it: the question is about a key whose
    snapshot carries none. Then streams `AM.AAPL` and records the session's host, state, and
    acknowledgement. Passes when the stocks connection is on `socket.massive.com` and Massive
    acknowledged the topic there, which it does whether or not the market is open; a refusal
    there means such a key's TICK host is another one, which the user pins with
    `MassiveConfig.websocket_hosts`.
    """
    answer = recency_label(dm.Subscription.STOCKS)
    if not answer.unlabelled:
        pytest.skip(
            f"{answer.text()}; the question is about a key whose stocks snapshot carries no "
            "recency label"
        )
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


@pytest.mark.settles("69")
def test_which_envelope_status_delayed_futures_data_gets(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which envelope status a futures snapshot of delayed data carries: `DELAYED` or `OK`.

    Only a key whose futures data Massive serves delayed can answer this. Records the envelope
    status and the labels of every labelled ES row on every key, then skips when no label
    says `DELAYED`, since the key's data is not delayed. Reading it: a `DELAYED` status
    confirms that the stream verb and `fetch()` refuse delayed futures data unless futures
    are listed in `MassiveConfig.delayed_subscriptions`; `OK` with a `DELAYED` timeframe means
    only Massive's refusal on the real-time feed catches such a key.
    """
    page = _futures_snapshot(live_rest)
    _futures_labels(page, evidence)
    assert page.rows
    answer = recency.answered("futures", page.status, page.rows)
    if not answer.delayed:
        pytest.skip(f"no label says DELAYED ({answer.text()}); the question needs delayed data")


@pytest.mark.settles("71")
def test_which_recency_labels_a_delayed_option_chain_carries(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which recency labels a SPY chain of delayed options data carries: its status and timeframes.

    The chain is the options host check's snapshot, and `fetch()` and `poll()` of options
    snapshots refuse a `DELAYED` label unless options are listed in
    `MassiveConfig.delayed_subscriptions`. Only a key whose options data Massive serves delayed
    can answer this. Records the envelope status and every `timeframe` field of three
    contracts on every key, then skips when no label says `DELAYED`. Reading it: a `DELAYED`
    status or `last_trade.timeframe` confirms the refusal; neither, on a key Massive serves
    delayed, means only Massive's refusal on the real-time feed catches it, and the chain's
    tables show `Recency` `UNKNOWN`.
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
    answer = recency.answered("options", page.status, page.rows)
    if not answer.delayed:
        pytest.skip(f"no label says DELAYED ({answer.text()}); the question needs delayed data")


_DELAYED_ORDER = ("stocks", "futures", "indices", "options")
"""The subscriptions the default-configuration check tries, in order, among those the owner's
settings list in `delayed_subscriptions`."""


@pytest.mark.timeout(100)
def test_a_delayed_subscription_is_refused_on_the_default_configuration(
    live_config: dm.MassiveConfig,
    *,
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Whether a session on the default configuration shows the refusal of delayed data.

    The live sessions list the owner's delayed subscriptions in
    `MassiveConfig.delayed_subscriptions`, which hides what a user who lists nothing meets.
    This check takes the first subscription the owner's settings list there (stocks,
    futures, indices, then options) and, in a session with the key alone and nothing listed,
    streams one symbol's minute bars (the Nasdaq-100's values for indices) or, for options,
    fetches the SPY chain. Passes when the library refused it with an `EntitlementError`, or
    the stream failed with a refusal, and the error says what to do; the evidence quotes it.
    Skips when the settings list none of them.
    """
    held = [
        name
        for name in _DELAYED_ORDER
        if dm.Subscription(name) in live_config.delayed_subscriptions
    ]
    if not held:
        pytest.skip("no subscription is listed in delayed_subscriptions")
    name = held[0]
    config = dm.MassiveConfig(api_key=live_config.api_key)
    session = dm.MassiveSession(config)
    errors: list[str] = []
    try:
        try:
            if name == "options":
                session.options.chain("SPY").fetch(wait=True)
            else:
                today = datetime.now(UTC).date()
                queries = {
                    "stocks": lambda: session.stocks.bars("AAPL", dm.Timespan.MINUTE),
                    "futures": lambda: session.futures.bars(
                        front_contract("ES", today), dm.Timespan.MINUTE
                    ),
                    "indices": lambda: session.indices.values("I:NDX"),
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
    assert errors, f"delayed {name} data was served on the default configuration"
    assert "delayed" in errors[0].lower(), f"the refusal names no remedy: {errors[0]}"
