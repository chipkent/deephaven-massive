"""Live checks of the library's REST reads against Massive: which bucket each request is charged to.

Massive counts REST requests per subscription, and counts reference and cross-asset requests
on their own. The library charges each request to a bucket of its rate governor, which
`m.rate_limits()` shows one row each: a subscription (`stocks`, `options`, `indices`,
`currencies`, `futures`), or `other` for the data Massive assigns to none. The test here makes
real reads and records how the rows' counts move. It asserts the library's own charging, which
no answer of Massive changes; what Massive itself counts is recorded by the rate-limit tests of
`tests/massive/rest/test_client_integration.py`. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

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


@pytest.mark.needs("stocks:basic")
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
    tests' requests; only their changes count. It needs Stocks on Basic only in that it may
    read a stock lookup there: the two requests stay within a minute's 5.

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
