"""Live checks of the partner datasets against Massive: the Benzinga consensus ticker.

Each test records what Massive sent (`evidence`). Raw rows from the wire REST client serve
where the question is whether a field is on the wire at all, which the library's table would
hide behind its own error. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pytest

from deephaven_massive._massive import Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive._massive import RestClient

pytestmark = pytest.mark.integration

# Fixture type, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"

_CONSENSUS: Final = "/benzinga/v1/consensus-ratings/{ticker}"
_CONSENSUS_TICKERS: Final = ("AAPL", "MSFT", "NVDA")
"""Three widely covered stocks, each with a consensus from many analysts."""


@pytest.mark.verifies("68")
@pytest.mark.requests(3)
def test_every_consensus_row_names_its_ticker(*, live_rest: RestClient, evidence: Evidence) -> None:
    """Whether every Benzinga consensus-ratings row carries `ticker`, as the library requires.

    The vendor's schema marks `ticker` optional, but the library makes each consensus row's
    `ticker` its `Symbol` and its `poll()` key, so a row without one fails the call. Reads the
    consensus of three stocks raw, one request each, charged to the Stocks bucket as the
    library charges them, and records each row's `ticker`. Reading it: a pass confirms the
    library; a row without `ticker`, or with another stock's, fails the test, and then the
    dataset needs another key. When Massive refuses a request, the test skips, quoting it.
    """
    rest_path = source(Transport.REST, _CONSENSUS)
    for ticker in _CONSENSUS_TICKERS:
        rows = live_rest.get(rest_path, {"ticker": ticker}, {}, bucket="stocks").rows
        sent = [row.get("ticker") for row in rows]
        evidence(f"{ticker}: {len(rows)} rows, tickers {sent}")
        assert rows, f"{ticker}: no consensus row"
        assert all(value == ticker for value in sent), f"{ticker}: rows name {sent}"
