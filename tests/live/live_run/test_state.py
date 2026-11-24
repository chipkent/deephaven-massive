"""Tests of the live run's state and its due rules (`tests/_live/live_run/state.py`).

The state round-trips through `state.json`; the subscription a refusal names, and the rules
that decide whether a check runs again, are checked case by case.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._live.live_run import state as run_state
from tests._live.live_run.state import Attempt, RunState, check_of, is_due, record

if TYPE_CHECKING:
    from pathlib import Path

DAY = "2026-11-03"
NEXT_DAY = "2026-11-04"

REFUSAL = (
    "refused: HTTP 403 from GET /v3/quotes/AAPL on api.massive.com (request_id 1): "
    "You are not entitled to this data"
)


def _state() -> RunState:
    return RunState(commit="abc")


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
    (tmp_path / "state.json").write_text('{"version": 1}')
    with pytest.raises(ValueError, match="layout version 1"):
        run_state.load(tmp_path)


def test_a_new_check_is_due_and_a_pass_is_final() -> None:
    state = _state()
    check = check_of(state, "evidence", "t")
    assert is_due(check, DAY)
    record(state, check, status="passed", reason="", day=DAY)
    assert not is_due(check, NEXT_DAY)
    record(state, check, status="skipped", reason="no frame arrived", day=NEXT_DAY)
    assert (check.status, check.last_run) == ("passed", NEXT_DAY)
    assert not is_due(check, NEXT_DAY)


@pytest.mark.parametrize("status", ["problem present", "problem fixed"])
def test_a_massive_problem_check_that_ran_counts_no_failure(status: str) -> None:
    state = _state()
    check = check_of(state, "evidence", "t")
    record(state, check, status=status, reason="volume 12 against 13", day=DAY)
    assert (check.status, check.reason, check.failures) == (status, "volume 12 against 13", 0)
    assert not is_due(check, DAY)


def test_a_problem_still_present_is_checked_again_the_next_day_and_a_fix_is_final() -> None:
    state = _state()
    present, fixed = check_of(state, "evidence", "a"), check_of(state, "evidence", "b")
    record(state, present, status="problem present", reason="volume 12", day=DAY)
    record(state, fixed, status="problem fixed", reason="close it", day=DAY)
    assert is_due(present, NEXT_DAY)
    assert not is_due(fixed, NEXT_DAY)


def test_two_failures_are_final_and_an_interruption_counts_nothing() -> None:
    state = _state()
    check = check_of(state, "smoke", "t")
    record(state, check, status="failed", reason="call: KeyError", day=DAY)
    record(state, check, status="interrupted", reason="", day="later")
    assert (is_due(check, DAY), check.last_run) == (True, DAY)
    record(state, check, status="error", reason="the child ended first", day=DAY)
    assert check.failures == 2
    assert not is_due(check, NEXT_DAY)
    check_of(state, "other", "u")
    run_state.redo(state, "smoke")
    assert (check.status, check.failures, is_due(check, DAY)) == ("not run", 0, True)


@pytest.mark.parametrize(
    ("status", "reason"),
    [("refused", REFUSAL), ("skipped", "not listed as delayed: list stocks in ...")],
)
def test_a_refusal_or_an_unlisted_delay_runs_again_at_the_first_attempt_of_each_day(
    status: str, reason: str
) -> None:
    """Massive's refusal, or the owner's settings, change only with the key or the file.

    Such a check runs once a day, at its group's first attempt, and never counts as a
    condition skip, so it never runs out of attempts.
    """
    state = _state()
    check = check_of(state, "evidence", "t")
    record(state, check, status=status, reason=reason, day=DAY)
    assert run_state.is_daily(check)
    assert not is_due(check, DAY)
    assert is_due(check, NEXT_DAY)
    assert check.condition_skips == 0


def test_a_cleared_refusal_reruns_the_other_refused_checks_of_its_subscription_at_once() -> None:
    """When one of a subscription's checks stops being refused, the key gained something.

    Every other check of that subscription that Massive refused is due again the same day,
    and its group may run again (`reruns`); a refusal of another subscription waits for the
    next day.
    """
    state = _state()
    first = check_of(state, "evidence", "a.py::t")
    second = check_of(state, "smoke", "b.py::u")
    other = check_of(state, "smoke", "c.py::v")
    record(state, first, status="refused", reason=REFUSAL, day=DAY)
    record(state, second, status="refused", reason=REFUSAL, day=DAY)
    record(
        state,
        other,
        status="refused",
        reason="refused: HTTP 403 from GET /v3/snapshot/indices on h: no",
        day=DAY,
    )
    assert (first.subscription, other.subscription) == ("stocks", "indices")
    assert not run_state.reruns(state, "smoke")
    record(state, first, status="passed", reason="", day=DAY)
    assert (second.rerun, other.rerun, first.rerun) == (True, False, False)
    assert is_due(second, DAY)
    assert not is_due(other, DAY)
    assert run_state.reruns(state, "smoke")
    record(state, second, status="refused", reason=REFUSAL, day=DAY)
    assert not run_state.reruns(state, "smoke")
    assert not is_due(second, DAY)


@pytest.mark.parametrize(
    ("status", "reason", "reruns"),
    [
        ("passed", "", True),
        ("failed", "call: KeyError", True),
        ("skipped", "not listed as delayed: list stocks in ...", True),
        ("skipped", "the market was closed", False),
        ("skipped", "no frame arrived", False),
        ("error", "the child ended first", False),
    ],
)
def test_a_refusal_reruns_its_subscription_only_once_massive_answers_otherwise(
    status: str, reason: str, reruns: bool
) -> None:
    """Only an answer from Massive says the key changed; a skip for the market says nothing."""
    state = _state()
    first = check_of(state, "evidence", "a.py::t")
    second = check_of(state, "smoke", "b.py::u")
    record(state, first, status="refused", reason=REFUSAL, day=DAY)
    record(state, second, status="refused", reason=REFUSAL, day=DAY)
    record(state, first, status=status, reason=reason, day=DAY)
    assert second.rerun is reruns


UNLISTED = "not listed as delayed: AM.*: list stocks in delayed_subscriptions"


def test_a_check_of_a_listed_subscription_reruns_its_unlisted_and_refused_checks() -> None:
    """When the settings start listing a subscription, its other daily checks run at once.

    A check skipped because the settings do not list its subscription as delayed stores that
    subscription; once another check of it passes, every refused or unlisted check of it is
    due again the same day, and its group may run again (`reruns`).
    """
    state = _state()
    unit = check_of(state, "stream-day", "stocks")
    refused = check_of(state, "smoke", "b.py::u")
    listed = check_of(state, "evidence", "a.py::test_stocks_snapshot")
    record(state, unit, status="skipped", reason=UNLISTED, day=DAY)
    record(state, refused, status="refused", reason=REFUSAL, day=DAY)
    record(state, listed, status="skipped", reason=UNLISTED, day=DAY)
    assert (unit.subscription, listed.subscription) == ("stocks", "stocks")
    assert not run_state.reruns(state, "stream-day")
    record(state, listed, status="passed", reason="", day=DAY)
    assert (unit.rerun, refused.rerun, listed.rerun) == (True, True, False)
    assert is_due(unit, DAY)
    assert run_state.reruns(state, "stream-day")


@pytest.mark.parametrize(
    ("status", "reason", "reruns"),
    [
        ("passed", "", True),
        ("failed", "call: KeyError", True),
        ("skipped", UNLISTED, False),
        ("refused", REFUSAL, False),
        ("skipped", "the market was closed", False),
    ],
)
def test_an_unlisted_check_reruns_its_subscription_only_once_the_settings_list_it(
    status: str, reason: str, reruns: bool
) -> None:
    """Another skip for the listing, or Massive's refusal, says the settings gained nothing."""
    state = _state()
    first = check_of(state, "evidence", "a.py::test_stocks_t")
    second = check_of(state, "stream-day", "stocks")
    record(state, first, status="skipped", reason=UNLISTED, day=DAY)
    record(state, second, status="skipped", reason=UNLISTED, day=DAY)
    record(state, first, status=status, reason=reason, day=DAY)
    assert second.rerun is reruns


@pytest.mark.parametrize(
    ("name", "reason", "subscription"),
    [
        ("t", REFUSAL, "stocks"),
        ("t", "refused: HTTP 403 from GET /v2/aggs/ticker/O%3ASPY/range on h: no", "options"),
        ("crypto", "refused: HTTP 403: not entitled", "currencies"),
        ("futures", "refused: NOT_AUTHORIZED", "futures"),
        ("a.py::test_forex_quotes", "refused: HTTP 403: not entitled", "currencies"),
        ("a.py::test_x", "refused: the indices stream: not entitled", "indices"),
        ("a.py::test_x", "refused: HTTP 403 from GET /v1/marketstatus/now on h: no", ""),
    ],
)
def test_the_subscription_a_refusal_names(name: str, reason: str, subscription: str) -> None:
    assert run_state.refused_subscription(name, reason) == subscription


def test_a_condition_skip_is_retried_three_times() -> None:
    state = _state()
    check = check_of(state, "luld", "t")
    for _ in range(3):
        assert is_due(check, DAY)
        record(state, check, status="skipped", reason="no LULD frame arrived", day=DAY)
    assert (check.condition_skips, is_due(check, DAY)) == (3, False)
    assert not run_state.is_daily(check)


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
    assert (check.status, check.condition_skips) == ("skipped", 1)
    assert is_due(check, monday)
    record(state, check, status="passed", reason="", day=monday)
    assert not is_due(check, monday)


@pytest.mark.parametrize(
    ("group", "name"),
    [
        ("smoke", "t.py::test_every[stocks.trades-stream]"),
        ("evidence", "t.py::test_whether_massive_serves_a_real_time_key_on_the_delayed_feed"),
        ("stream-day", "stocks"),
    ],
)
def test_a_stream_pass_from_before_rows_were_required_runs_again(group: str, name: str) -> None:
    """A stream that passed on its acknowledgement alone, under the older rule, proved nothing.

    Its pass, stored without a rule version, leaves it due until it passes under the rule that
    requires rows; a skip on the way does not retire it.
    """
    state = _state()
    check = check_of(state, group, name)
    check.status = "passed"  # as an older state stored it
    assert is_due(check, DAY)
    record(state, check, status="skipped", reason="not exercised: market closed", day=DAY)
    assert is_due(check, DAY)
    record(state, check, status="passed", reason="", day=DAY)
    assert (check.passed_rules, is_due(check, DAY)) == (run_state.ROW_RULES, False)
    other = check_of(state, "smoke", "t.py::test_every[stocks.trades-fetch]")
    other.status = "passed"
    assert not is_due(other, DAY)
    assert run_state.key("g", "n") == "g n"


def test_an_unknown_status_is_refused_and_leaves_the_check_as_it_was() -> None:
    state = _state()
    check = check_of(state, "evidence", "t")
    before = repr(check)
    with pytest.raises(ValueError, match="unknown status 'skiped'"):
        record(state, check, status="skiped", reason="closed market", day=DAY)
    assert repr(check) == before
    assert check.condition_skips == 0


@pytest.mark.parametrize(
    ("status", "counts"),
    [
        ("passed", (0, 0, run_state.ROW_RULES)),
        ("failed", (1, 0, 0)),
        ("error", (1, 0, 0)),
        ("refused", (0, 0, 0)),
        ("skipped", (0, 1, 0)),
    ],
)
def test_each_status_moves_its_own_count(status: str, counts: tuple[int, int, int]) -> None:
    """Each stored status is handled by name: only a plain skip counts as a condition skip."""
    state = _state()
    check = check_of(state, "evidence", "t")
    reason = REFUSAL if status == "refused" else "market closed"
    record(state, check, status=status, reason=reason, day=DAY)
    assert (check.status, check.last_run) == (status, DAY)
    assert (check.failures, check.condition_skips, check.passed_rules) == counts
