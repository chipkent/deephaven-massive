"""Live checks of the library's REST reads against Massive: which bucket each request is charged to.

Massive counts REST requests per subscription, and counts reference and cross-asset requests
on their own. The library charges each request to a bucket of its rate governor, which
`m.rate_limits()` shows one row each: a subscription (`stocks`, `options`, `indices`,
`currencies`, `futures`), or `other` for the data Massive assigns to none. The test here makes
real reads and records how the rows' counts move. It asserts the library's own charging, which
no answer of Massive changes; what Massive itself counts is recorded by the rate-limit tests of
`tests/massive/rest/test_client_integration.py`. A second test sends each list endpoint a
request that names a ticker the key may not be entitled to, through the wire client, and
records how Massive answers. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, assert_never

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Transport, WireEntitlementError, source
from tests._live import option_requests, recorded_requests

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._massive import RestClient


pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)

_OTHER = "other"
"""The bucket of the data Massive assigns to no subscription."""

_READS = 2
"""The requests the test's two reads make: one page each."""


def _attempts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Return each bucket's count of attempts from the rows of `m.rate_limits()`.

    Returns:
        `Attempts` by `Bucket`.
    """
    return {str(row["Bucket"]): int(row["Attempts"]) for row in rows}


@pytest.mark.verifies("66")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_a_reference_lookup_and_the_market_status_count_in_the_other_row(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    rows_when: RowsWhen,
    evidence: Evidence,
) -> None:
    """Whether a ticker lookup filtered by market and the market status count in `other`.

    The library charges every reference read, one filtered by a class's market included, and
    the market status, which belongs to no subscription, to the `other` bucket. Reads one
    stock ticker (`reference.tickers(market=STOCKS, limit=1)`, one request) and the market
    status (one request), and records the `Attempts` of every row of `m.rate_limits()` before
    and after. The session shares the run's rate governor, so the counts include earlier
    tests' requests; only their changes count.

    Passes when the `other` row gained at least the two requests (a retry would add more) and
    no subscription's row changed; a failure names the rows that moved, a bug in the library's
    charging. Reading it beside the rate-limit tests: if Massive charges a market-filtered
    lookup to its class, this charging is what must change.
    """
    view = live_session.rate_limits()
    try:
        before = _attempts(rows_when(view, lambda rows: _OTHER in _attempts(rows)))
        tickers = table_rows(
            live_session.reference.tickers(market=dm.Market.STOCKS, limit=1).fetch(wait=True)
        )
        status = table_rows(live_session.reference.market_status().fetch(wait=True))
        evidence(f"reference.tickers(market=stocks, limit=1): {[r['Symbol'] for r in tickers]}")
        evidence(f"reference.market_status(): {len(status)} row")
        after = _attempts(
            rows_when(view, lambda rows: _attempts(rows)[_OTHER] >= before[_OTHER] + _READS)
        )
    finally:
        live_session.release(view)
    moved = {bucket: after[bucket] - before[bucket] for bucket in after}
    evidence(f"rate_limits() Attempts before {before}; after {after}; changes {moved}")
    assert tickers, "the stock ticker lookup returned no row"
    assert moved[_OTHER] >= _READS
    assert {bucket: change for bucket, change in moved.items() if bucket != _OTHER and change} == {}


class _Names(Enum):
    """What the names of a list endpoint's test case stand for."""

    TICKERS = "tickers"
    """The names are the tickers sent."""
    OPTION_CONTRACTS = "option contracts"
    """The names are underlyings; one listed contract of each is sent."""
    FRONT_FUTURES = "front futures"
    """The names are futures products; each one's front contract is sent."""


@dataclasses.dataclass(frozen=True)
class _ListEndpoint:
    """One list endpoint of the library, and the tickers its test sends.

    Attributes:
        path: The endpoint's REST path.
        parameter: The query parameter that takes the tickers.
        bucket: The rate-limit bucket of the endpoint's data.
        names: What the request names (see `kind`): at least two, of classes or sources the
            key may not all be entitled to.
        kind: What `names` stand for.
    """

    path: str
    parameter: str
    bucket: str
    names: tuple[str, ...]
    kind: _Names = _Names.TICKERS


_LIST_ENDPOINTS = {
    "snapshots": _ListEndpoint(
        "/v3/snapshot", "ticker.any_of", "stocks", ("AAPL", "I:SPX", "C:EURUSD", "X:BTCUSD")
    ),
    "indices.snapshots": _ListEndpoint(
        "/v3/snapshot/indices", "ticker.any_of", "indices", ("I:SPX", "I:NDX", "I:VIX")
    ),
    "options.snapshots": _ListEndpoint(
        "/v3/snapshot", "ticker.any_of", "options", ("AAPL", "SPX"), _Names.OPTION_CONTRACTS
    ),
    "forex.snapshots": _ListEndpoint(
        "/v2/snapshot/locale/global/markets/forex/tickers",
        "tickers",
        "currencies",
        ("C:EURUSD", "C:USDJPY"),
    ),
    "crypto.snapshots": _ListEndpoint(
        "/v2/snapshot/locale/global/markets/crypto/tickers",
        "tickers",
        "currencies",
        ("X:BTCUSD", "X:ETHUSD"),
    ),
    "options.greeks": _ListEndpoint(
        option_requests.GREEKS, "ticker.any_of", "options", ("AAPL", "SPX"), _Names.OPTION_CONTRACTS
    ),
    "options.open_interest": _ListEndpoint(
        option_requests.OPEN_INTEREST,
        "ticker.any_of",
        "options",
        ("AAPL", "SPX"),
        _Names.OPTION_CONTRACTS,
    ),
    "options.open_interest_by_underlying": _ListEndpoint(
        option_requests.OPEN_INTEREST_BY_UNDERLYING,
        "underlying_ticker.any_of",
        "options",
        ("AAPL", "SPX"),
    ),
    "options.unusual_trades": _ListEndpoint(
        option_requests.UNUSUAL_TRADES,
        "ticker.any_of",
        "options",
        ("AAPL", "SPX"),
        _Names.OPTION_CONTRACTS,
    ),
    "futures.contracts": _ListEndpoint(
        "/futures/v1/contracts", "ticker.any_of", "futures", ("ES", "ZC"), _Names.FRONT_FUTURES
    ),
}
"""The ten list endpoints, by the dataset they serve."""

_LISTED_CONTRACTS: dict[str, str] = {}
"""One listed option contract of each underlying asked about, kept for the run's later cases."""

_ROW_EXCERPT = 200
"""The most characters of a row the evidence records."""

_ROWS_RECORDED = 5
"""The most rows of an answer the evidence records."""


@dataclasses.dataclass(frozen=True)
class _Answer:
    """Massive's answer to one request: its rows, or its refusal.

    Attributes:
        rows: The rows of a 200 answer; empty for a refusal.
        refusal: Massive's refusal as the wire client words it, with its status; None for a 200.
    """

    rows: list[dict[str, Any]]
    refusal: str | None


def _listed_contract(client: RestClient, underlying: str, *, evidence: Evidence) -> str:
    """Return one option contract Massive lists for an underlying today.

    Returns:
        The contract's ticker; the test skips, quoting the answer, when Massive lists none.
    """
    if underlying not in _LISTED_CONTRACTS:
        path, query = option_requests.first_contract(underlying=underlying)
        page = recorded_requests.get(
            client,
            source(Transport.REST, path),
            path_params={},
            query=query,
            bucket="options",
            evidence=evidence,
        )
        rows = page.rows
        if not rows:
            pytest.skip(f"Massive lists no option contract of {underlying}")
        _LISTED_CONTRACTS[underlying] = str(rows[0]["ticker"])
    return _LISTED_CONTRACTS[underlying]


def _tickers(
    endpoint: _ListEndpoint,
    client: RestClient,
    *,
    front_contract: Callable[[str, date], str],
    evidence: Evidence,
) -> list[str]:
    """Return the tickers one endpoint's request names.

    Returns:
        The endpoint's names, or the contracts they stand for.
    """
    kind = endpoint.kind
    if kind is _Names.TICKERS:
        return list(endpoint.names)
    if kind is _Names.OPTION_CONTRACTS:
        return [_listed_contract(client, name, evidence=evidence) for name in endpoint.names]
    if kind is _Names.FRONT_FUTURES:
        today = datetime.now(UTC).date()
        return [front_contract(product, today) for product in endpoint.names]
    assert_never(kind)


def _ask(
    client: RestClient, endpoint: _ListEndpoint, tickers: list[str], *, evidence: Evidence
) -> _Answer:
    """Send one request naming tickers to a list endpoint and record Massive's answer.

    Returns:
        The rows of a 200 answer, or Massive's refusal; any other failure propagates, since it
        says nothing of what the key is entitled to.
    """
    try:
        page = recorded_requests.get(
            client,
            source(Transport.REST, endpoint.path),
            path_params={},
            query={endpoint.parameter: tickers},
            bucket=endpoint.bucket,
            evidence=evidence,
        )
    except WireEntitlementError as error:
        return _Answer([], f"HTTP {error.status}: {error.message}")
    return _Answer(page.rows, None)


def _record_rows(label: str, rows: list[dict[str, Any]], evidence: Evidence) -> None:
    """Record how many rows an answer has, every row's `error`, and the first few rows."""
    evidence(f"{label}: {len(rows)} rows")
    for row in rows:
        if row.get("error"):
            evidence(
                f"    error row: ticker {row.get('ticker')!r}, error {row.get('error')!r}, "
                f"message {row.get('message')!r}"
            )
    for row in rows[:_ROWS_RECORDED]:
        evidence(f"    {str(row)[:_ROW_EXCERPT]}")


@pytest.mark.settles("150")
@pytest.mark.requests(5)
@pytest.mark.timeout(120)
@pytest.mark.parametrize("dataset", list(_LIST_ENDPOINTS))
def test_a_list_request_with_an_unentitled_ticker(
    live_rest: RestClient,
    front_contract: Callable[[str, date], str],
    *,
    dataset: str,
    evidence: Evidence,
) -> None:
    """How one list endpoint answers a request that names a ticker the key may lack.

    The ten list endpoints are the ones `m.snapshots`, `m.indices.snapshots`,
    `m.options.snapshots`, `m.forex.snapshots`, `m.crypto.snapshots`, `m.options.greeks`,
    `m.options.open_interest`, `m.options.open_interest_by_underlying`,
    `m.options.unusual_trades`, and `m.futures.contracts` send. Each case sends its endpoint one
    request through the wire client that names tickers of several classes or sources (an
    underlying's option contract is one listed contract, a futures product's is its front
    contract), and records Massive's answer, with its `request_id`. When Massive refuses the
    request, sends each ticker alone to see which it refused. Skips, quoting the rows, when
    Massive answers 200 with no row's `error` set: the key is entitled to all of it, or the
    endpoint answers an unentitled ticker with nothing. Passes when Massive answers the
    request with rows of which some carry an `error` (a per-row answer: the other tickers are
    served) or refuses it with an HTTP 403 (every ticker of the request is refused together);
    the evidence shows which, and for a refusal which tickers were served alone. Fails on any
    other failure, which says nothing of the key's entitlements.
    """
    endpoint = _LIST_ENDPOINTS[dataset]
    tickers = _tickers(endpoint, live_rest, front_contract=front_contract, evidence=evidence)
    evidence(f"{dataset}: {endpoint.path} {endpoint.parameter}={tickers}")
    answer = _ask(live_rest, endpoint, tickers, evidence=evidence)
    if answer.refusal is None:
        _record_rows("answer to the whole request", answer.rows, evidence)
        errors = [row for row in answer.rows if row.get("error")]
        evidence(f"rows with an error: {len(errors)} of {len(answer.rows)}")
        if not errors:
            pytest.skip(f"Massive answered HTTP 200 with {len(answer.rows)} rows and no row error")
        return
    evidence(f"the whole request was refused: {answer.refusal}")
    for ticker in tickers:
        alone = _ask(live_rest, endpoint, [ticker], evidence=evidence)
        if alone.refusal is None:
            _record_rows(f"{ticker} alone: HTTP 200", alone.rows, evidence)
        else:
            evidence(f"{ticker} alone: {alone.refusal}")
