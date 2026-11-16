"""Live checks of the financial statements' argument values against Massive.

Each test sends a value the library could refuse before any request straight to Massive's
REST endpoint through the wire client, and records Massive's status, message, and row count
(`evidence`), ending with a verdict line: ERROR, EMPTY, or DATA (`tests/_live/bad_values.py`).
The tests are marked `settles`: they assert only that Massive answered, and the owner reads
the verdict. ERROR means Massive judges the value itself; EMPTY, or DATA that does not answer
the request, means it would return wrong or empty data with no error. How to run these tests
is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive import WireEntitlementError
from tests._live import bad_values, refusals

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive._massive import RestClient

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"


@pytest.mark.settles("99")
@pytest.mark.requests(1)
def test_massive_answers_trailing_twelve_month_balance_sheets(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the balance sheets answer to `timeframe=trailing_twelve_months` for AAPL.

    `m.stocks.financials.balance_sheets` refuses that timeframe, which the other statements
    take. DATA counts the rows of that timeframe. Massive's refusal of the request answers the
    question too: the test records it as the verdict and passes, so the item is read with the
    evidence instead of skipped.
    """
    try:
        assert bad_values.ask_all(live_rest, bad_values.TRAILING_BALANCE_SHEET, evidence)
    except WireEntitlementError as error:
        evidence(f"verdict: Massive refused the request: {refusals.refusal_line(error)}")
