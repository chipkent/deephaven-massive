"""Tests of the refusal to run live tests on parallel workers (`tests/_live/parallel.py`).

The refusal reads the tests a run selected, so each case lists the ids a selection leaves.
"""

from __future__ import annotations

import pytest

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
    assert not parallel.is_live("tests/test_layout.py::test_names[a::x_integration.py]")


def test_a_separator_in_a_live_test_parameter_keeps_the_test_live() -> None:
    assert parallel.is_live("tests/massive/test_ws_integration.py::test_topic[a::b]")


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


def test_the_hook_ends_a_run_that_selects_a_live_test() -> None:
    with pytest.raises(pytest.exit.Exception) as ended:
        parallel.pytest_xdist_node_collection_finished([_UNIT, _LIVE])
    assert ended.value.returncode == pytest.ExitCode.USAGE_ERROR
    assert ended.value.msg == parallel.refusal([_LIVE])


def test_the_hook_lets_a_run_of_unit_tests_go_on() -> None:
    parallel.pytest_xdist_node_collection_finished([_UNIT])
