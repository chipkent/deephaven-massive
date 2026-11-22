"""Live checks of the list endpoints' `sort` and `limit` values against Massive.

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


@pytest.mark.settles("93")
@pytest.mark.requests(1)
def test_massive_answers_a_sort_value_its_path_does_not_list(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the conditions answer to `sort=exchange`, a field their `sort` does not list.

    The library sends a value outside a listed `sort` for Massive to judge. DATA cannot be
    compared with the request: read the rows' order.
    """
    assert bad_values.ask_all(live_rest, bad_values.ENUM_SORT, evidence)


@pytest.mark.settles("94")
@pytest.mark.requests(1)
def test_massive_answers_a_sort_field_its_rows_do_not_have(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the dividends answer to `sort=no_such_field.asc`.

    On a path whose `sort` takes comma-separated fields, the library sends a part that names
    no field of the rows for Massive to judge. DATA cannot be compared with the request.
    """
    assert bad_values.ask_all(live_rest, bad_values.SORT_FIELD, evidence)


@pytest.mark.settles("95")
@pytest.mark.requests(1)
def test_massive_answers_a_sort_part_with_a_space(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the dividends answer to `sort=ticker, ex_dividend_date.desc`, a part with a space.

    The library sends a part with surrounding whitespace as written. DATA cannot be compared
    with the request: read whether the rows come newest first.
    """
    assert bad_values.ask_all(live_rest, bad_values.SORT_WHITESPACE, evidence)


@pytest.mark.settles("96")
@pytest.mark.requests(1)
def test_massive_answers_a_limit_of_0(live_rest: RestClient, *, evidence: Evidence) -> None:
    """What the dividends answer to `limit=0`, which the library refuses as below 1.

    DATA means Massive reads 0 as some other limit: the row count says which.
    """
    assert bad_values.ask_all(live_rest, bad_values.LIMIT, evidence)
