"""Live checks of filter values against Massive.

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
@pytest.mark.settles("97")
@pytest.mark.requests(1)
def test_massive_answers_an_enum_filter_value_it_does_not_list(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the tickers list answers to `market=equities`, a value its specification does not list.

    The library refuses a filter value outside an enum parameter's values. DATA counts the
    rows of market `equities`.
    """
    assert bad_values.ask_all(live_rest, bad_values.ENUM_FILTER, evidence)


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("98")
@pytest.mark.requests(1)
def test_massive_answers_a_filter_list_item_with_a_space(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the dividends answer to `ticker.any_of=AAPL, MSFT`, whose second item has a space.

    The library refuses an item with surrounding whitespace. DATA counts the MSFT rows: none
    means Massive matched ` MSFT` as written, and so dropped that ticker with no error.
    """
    assert bad_values.ask_all(live_rest, bad_values.FILTER_WHITESPACE, evidence)
