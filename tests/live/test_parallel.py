"""Tests of the refusal to run live tests on parallel workers (`tests/_live/parallel.py`).

The refusal reads the tests a run selected, so each case lists the ids a selection leaves.
"""

from __future__ import annotations

from tests._live import parallel

_UNIT = "tests/namespaces/test_stocks.py::test_trades_columns"
_LIVE = "tests/namespaces/test_stocks_integration.py::test_trades_arrive"
_LIVE_NEEDS = "tests/massive/test_ws_integration.py::test_delayed_feed[stocks]"


def test_a_test_is_live_when_its_file_is_a_file_of_live_tests() -> None:
    assert parallel.is_live(_LIVE)
    assert parallel.is_live("tests/massive/test_ws_integration.py")
    assert not parallel.is_live(_UNIT)


def test_a_live_suffix_in_the_test_name_does_not_make_a_test_live() -> None:
    assert not parallel.is_live("tests/test_layout.py::test_names[x_integration.py]")


def test_a_selection_of_unit_tests_only_may_run_in_parallel() -> None:
    assert parallel.refusal([_UNIT, "tests/test_layout.py::test_mirrors"]) is None


def test_an_empty_selection_may_run_in_parallel() -> None:
    assert parallel.refusal([]) is None


def test_the_live_tests_an_and_expression_selects_are_refused() -> None:
    # `-m "integration and needs"` selects only live tests that carry a `needs` marker.
    message = parallel.refusal([_LIVE_NEEDS])
    assert message is not None
    assert "selects 1 of them" in message
    assert _LIVE_NEEDS in message
    assert message.endswith("drop -n, or pass -n 0")


def test_the_live_tests_an_empty_expression_selects_are_refused() -> None:
    # `-m ""` selects every test, the live ones among the unit ones.
    message = parallel.refusal([_UNIT, _LIVE, _LIVE_NEEDS])
    assert message is not None
    assert "selects 2 of them (the first is " + _LIVE + ")" in message
