"""Tests of the live run's state and its due rules (`tests/_live/live_run/state.py`).

The state round-trips through `state.json`; the entitlement ids, the rise of a tier, and the
rules that decide whether a check runs again are checked case by case.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._live.live_run import state as run_state
from tests._live.live_run.state import Attempt, RunState, check_of, is_due, record
from tests._live.plans import TIERS

if TYPE_CHECKING:
    from pathlib import Path

DAY = "2026-11-03"


def _tiers(**tiers: str) -> dict[str, str]:
    return {name: tiers.get(name, "basic") for name in TIERS}


def _state() -> RunState:
    state = RunState(commit="abc")
    state.current = run_state.entitlement_id(state, _tiers(), [])
    return state


def test_the_state_round_trips_and_a_missing_one_is_none(tmp_path: Path) -> None:
    assert run_state.load(tmp_path) is None
    state = _state()
    state.attempts.append(Attempt("evidence", "evidence/2026-11-03-1", DAY, "t0", pid=7))
    check_of(state, "evidence", "a.py::t").items = ["21"]
    run_state.save(tmp_path, state)
    run_state.save(tmp_path, state)
    assert run_state.load(tmp_path) == state
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_a_state_of_another_version_is_refused(tmp_path: Path) -> None:
    (tmp_path / "state.json").write_text('{"version": 99}')
    with pytest.raises(ValueError, match="layout version 99"):
        run_state.load(tmp_path)


def test_entitlement_ids_name_each_combination_once() -> None:
    state = _state()
    assert run_state.entitlement_id(state, _tiers(), []) == "e1"
    assert run_state.entitlement_id(state, _tiers(stocks="starter"), ["b", "a"]) == "e2"
    assert run_state.entitlement_id(state, _tiers(stocks="starter"), ["a", "b"]) == "e2"
    assert run_state.tiers_text(state, "e2").endswith("futures=basic; flat files: a, b")
    assert run_state.tiers_text(state, "e1").endswith("flat files: none")
    assert run_state.tiers_text(state, "") == ""


def test_a_tier_rises_only_upward_between_known_entitlements() -> None:
    state = _state()
    high = run_state.entitlement_id(state, _tiers(options="starter"), [])
    assert run_state.tier_rose(state, "e1", high)
    assert not run_state.tier_rose(state, high, "e1")
    assert not run_state.tier_rose(state, "", high)
    assert not run_state.tier_rose(state, high, high)


def test_a_new_check_is_due_and_a_pass_is_not_until_a_tier_rises() -> None:
    state = _state()
    check = check_of(state, "evidence", "t")
    assert is_due(state, check)
    record(state, check, status="passed", reason="", day=DAY)
    assert not is_due(state, check)
    state.current = run_state.entitlement_id(state, _tiers(stocks="starter"), [])
    assert is_due(state, check)
    record(state, check, status="skipped", reason="no frame arrived", day=DAY)
    assert (check.status, check.passed_under) == ("passed", state.current)
    assert not is_due(state, check)


def test_two_failures_are_final_and_an_interruption_counts_nothing() -> None:
    state = _state()
    check = check_of(state, "smoke", "t")
    record(state, check, status="failed", reason="call: KeyError", day=DAY)
    record(state, check, status="interrupted", reason="", day="later")
    assert (is_due(state, check), check.last_run) == (True, DAY)
    record(state, check, status="error", reason="the child ended first", day=DAY)
    assert check.failures == 2
    assert not is_due(state, check)
    check_of(state, "other", "u")
    run_state.redo(state, "smoke")
    assert (check.status, check.failures, is_due(state, check)) == ("not run", 0, True)


@pytest.mark.parametrize("status", ["refused", "skipped"])
def test_a_need_or_refusal_waits_for_another_entitlement(status: str) -> None:
    state = _state()
    check = check_of(state, "evidence", "t")
    record(state, check, status=status, reason="needs Stocks Starter ($29/month)", day=DAY)
    assert not is_due(state, check)
    state.current = run_state.entitlement_id(state, _tiers(indices="starter"), [])
    assert is_due(state, check)


def test_a_condition_skip_is_retried_three_times() -> None:
    state = _state()
    check = check_of(state, "luld", "t")
    for _ in range(3):
        assert is_due(state, check)
        record(state, check, status="skipped", reason="no LULD frame arrived", day=DAY)
    assert (check.condition_skips, is_due(state, check)) == (3, False)


def test_a_stream_check_skipped_while_the_market_was_closed_is_due_on_a_trading_day() -> None:
    """A stream that ran while its market was closed proved no row, so it must run again.

    A stream check skips, rather than passes, when its market is closed, and the skip counts
    as a condition of the moment: the check stays due, so the next attempt, on a trading day,
    runs it while rows can arrive.
    """
    state = _state()
    check = check_of(state, "smoke", "t.py::test_every[stocks.trades-stream]")
    saturday, monday = "2026-10-31", "2026-11-02"
    record(
        state,
        check,
        status="skipped",
        reason="not exercised: market closed (acknowledged)",
        day=saturday,
    )
    assert (check.status, check.passed_under, check.condition_skips) == ("skipped", "", 1)
    assert is_due(state, check)
    record(state, check, status="passed", reason="", day=monday)
    assert not is_due(state, check)


def test_need_reasons_are_told_from_conditions() -> None:
    assert run_state.is_need("not entitled: needs a Stocks plan")
    assert run_state.is_need("outside the plan's history: ...")
    assert not run_state.is_need("is the market closed?")
    assert run_state.key("g", "n") == "g n"
