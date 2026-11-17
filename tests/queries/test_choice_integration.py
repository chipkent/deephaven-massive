"""Live checks of the bars' timespan and multiplier values against Massive.

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

from tests._live import bad_values

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive._massive import RestClient

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"


@pytest.mark.settles("100", "107")
@pytest.mark.requests(2)
def test_massive_answers_futures_bars_of_a_day(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the futures aggregates answer to `resolution=1day` for the front E-mini contract.

    The library sends `Timespan.DAY` for futures bars as `1day`, and dates and keeps them as
    one-session bars. DATA cannot be compared with the request: read whether the bars span a
    day or a session. It also sends `0session`, a multiplier below 1, which the library sends
    for Massive to judge: ERROR means Massive refuses it; EMPTY or DATA, that the library
    should.
    """
    assert bad_values.ask_all(live_rest, bad_values.FUTURES_DAY, evidence)
    assert bad_values.ask_all(live_rest, bad_values.FUTURES_MULTIPLIER, evidence)


@pytest.mark.settles("101")
@pytest.mark.requests(1)
def test_massive_answers_stock_bars_of_a_session(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the aggregates range answers to AAPL's bars of `timespan=session`.

    The library sends `Timespan.SESSION` outside futures for Massive to judge. DATA cannot be
    compared with the request: read the bars' spacing.
    """
    assert bad_values.ask_all(live_rest, bad_values.SESSION_OUTSIDE_FUTURES, evidence)


@pytest.mark.settles("102")
@pytest.mark.requests(1)
def test_massive_answers_a_bar_multiplier_of_0(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the aggregates range answers to AAPL's bars with multiplier 0.

    The library sends a multiplier below 1 for Massive to judge. DATA cannot be compared with
    the request: read the bars' spacing.
    """
    assert bad_values.ask_all(live_rest, bad_values.MULTIPLIER, evidence)
