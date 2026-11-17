"""Live checks of the indicators' argument values against Massive.

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


@pytest.mark.settles("90")
@pytest.mark.requests(1)
def test_massive_answers_an_indicator_timespan_of_second(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive answers to AAPL's simple moving average with `timespan=second`.

    The library refuses every timespan the indicator paths do not list (they list `minute`
    to `year`). DATA cannot be compared with the request: read the rows' timestamps.
    """
    assert bad_values.ask_all(live_rest, bad_values.INDICATOR_TIMESPAN, evidence)


@pytest.mark.settles("91")
@pytest.mark.requests(1)
def test_massive_answers_an_indicator_window_of_0(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What Massive answers to AAPL's simple moving average with `window=0`.

    The library refuses a window below 1. Massive answered it with HTTP 200 and an empty
    body, which `ask` records as EMPTY. DATA cannot be compared with the request: read the
    values.
    """
    assert bad_values.ask_all(live_rest, bad_values.WINDOW, evidence)
