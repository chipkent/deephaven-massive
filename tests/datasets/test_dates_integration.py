"""Live checks of date ranges against Massive.

Each test sends a value the library refuses before any request straight to Massive's REST
endpoint through the wire client, and records Massive's status, message, and row count
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


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("105")
@pytest.mark.requests(2)
def test_massive_answers_a_start_after_the_end(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive answers to a range whose start is after its end.

    The library refuses one (`check_order`). Two forms: AAPL's day bars from a week ago to two
    weeks ago, and AAPL's dividends with `ex_dividend_date.gte` a month ago and `.lte` a year
    ago. No row can answer either: DATA means Massive reads the range some other way.
    """
    assert bad_values.ask_all(live_rest, bad_values.START_AFTER_END, evidence)


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("106")
@pytest.mark.requests(1)
def test_massive_answers_a_start_after_now(live_rest: RestClient, *, evidence: Evidence) -> None:
    """What Massive answers to AAPL's day bars from 30 days ahead to today.

    The library refuses a start after now when `end` is left out, which reads through now.
    No row can answer it: DATA means Massive reads the range some other way.
    """
    assert bad_values.ask_all(live_rest, bad_values.START_AFTER_NOW, evidence)
