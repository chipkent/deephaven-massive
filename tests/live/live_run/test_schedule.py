"""Tests of the live run's next step (`tests/_live/schedule.py`).

Each test builds a state and the run's memory by hand and asks what the run does at a New York
time: probe, run a group, or sleep, by the fixed timetable.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from datetime import time as timetable_time

import pytest

from tests._live.calendar import NEW_YORK
from tests._live.live_run import schedule, timetable
from tests._live.live_run.context import RunMemory
from tests._live.live_run.schedule import Step, next_step
from tests._live.live_run.state import Attempt, RunState, check_of, entitlement_id, record

BASIC = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "basic")

WEDNESDAY = date(2026, 11, 4)


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=NEW_YORK)


def _state(**tiers: str) -> RunState:
    state = RunState(commit="c")
    state.current = entitlement_id(state, BASIC | tiers, [])
    return state


def _collected(state: RunState) -> None:
    for group in schedule.PYTEST_GROUPS:
        state.known[group] = [f"tests/x.py::test_{group}"]


def _probed(now: datetime) -> RunMemory:
    return RunMemory(probed_at=now - timedelta(minutes=1))


def _finish(state: RunState, group: str, day: date) -> None:
    state.attempts.append(Attempt(group, f"{group}/x", day.isoformat(), "", status="finished"))


def test_the_first_step_of_a_run_is_a_probe() -> None:
    assert next_step(_state(), RunMemory(), _at(WEDNESDAY, 12)) == Step("probe")


def test_the_days_probe_comes_once_its_time_passes() -> None:
    memory = RunMemory(probed_at=_at(WEDNESDAY, 7))
    assert next_step(_state(), memory, _at(WEDNESDAY, 8, 1)) == Step("probe")


def test_a_failed_probe_is_tried_again_five_minutes_later_up_to_three_times() -> None:
    now = _at(WEDNESDAY, 12)
    memory = RunMemory(probed_at=now - timedelta(minutes=5), retries=1)
    assert next_step(_state(), memory, now) == Step("probe")
    memory.retries = schedule.PROBE_RETRIES
    assert next_step(_state(), memory, now).action != "probe"
    memory.retries, memory.probed_at = 1, now - timedelta(minutes=4)
    assert next_step(_state(), memory, now).action != "probe"


def test_the_pause_after_a_probe_is_slept_through() -> None:
    now = _at(WEDNESDAY, 12)
    memory = _probed(now)
    memory.not_before = now + timedelta(seconds=65)
    assert next_step(_state(), memory, now) == Step(
        "sleep", "the pause after the probe", memory.not_before
    )


def test_any_time_groups_run_at_once_and_leave_when_done() -> None:
    state = _state()
    _collected(state)
    now = _at(date(2026, 11, 7), 6)  # a Saturday, before any slot
    memory = _probed(now)
    state.any_time = ["stream-day-reconcile", "evidence"]
    step = next_step(state, memory, now)
    assert step == Step("run", "evidence", _at(date(2026, 11, 8), 3))
    assert state.any_time == ["evidence"]  # the reconciliation had nothing due


def test_any_time_groups_wait_through_a_stream_day() -> None:
    state = _state(stocks="starter")
    _collected(state)
    state.stream_day = WEDNESDAY.isoformat()
    state.any_time = ["evidence"]
    now = _at(WEDNESDAY, 4)
    assert next_step(state, _probed(now), now) == Step("run", "stream-day", _at(WEDNESDAY, 20, 30))
    assert state.any_time == ["evidence"]


def test_an_any_time_group_stops_before_a_planned_stream_days_probe() -> None:
    state = _state()
    assert schedule.any_time_stop(state, WEDNESDAY) == _at(date(2026, 11, 5), 3)
    state.stream_day = "2026-11-05"
    assert schedule.any_time_stop(state, WEDNESDAY) == _at(date(2026, 11, 5), 3)
    state.stream_day = WEDNESDAY.isoformat()  # today's is no later stop
    assert schedule.any_time_stop(state, WEDNESDAY) == _at(date(2026, 11, 5), 3)


def test_an_any_time_stop_never_passes_a_stream_probe_before_three(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state()
    state.stream_day = "2026-11-05"
    monkeypatch.setattr(timetable, "HARD_STOP", timetable_time(4, 0))
    assert schedule.any_time_stop(state, WEDNESDAY) == _at(date(2026, 11, 5), 3, 15)


def test_an_ordinary_day_waits_for_the_luld_check_then_runs_the_sequence() -> None:
    state = _state()
    _collected(state)
    early = _at(WEDNESDAY, 8, 30)
    assert next_step(state, _probed(early), early) == Step("sleep", "luld", _at(WEDNESDAY, 9, 28))
    late = _at(WEDNESDAY, 11, 31)
    assert next_step(state, _probed(late), late) == Step(
        "run", "rate-limits", _at(WEDNESDAY + timedelta(days=1), 3)
    )


def test_a_group_that_ran_today_or_has_nothing_due_is_passed_over() -> None:
    state = _state()
    _collected(state)
    now = _at(WEDNESDAY, 12)
    _finish(state, "rate-limits", WEDNESDAY)
    _finish(state, "evidence", WEDNESDAY)
    record(
        state,
        check_of(state, "smoke", "tests/x.py::test_smoke"),
        status="passed",
        reason="",
        day="2026-11-04",
    )
    assert next_step(state, _probed(now), now) == Step(
        "run", "responses", _at(WEDNESDAY + timedelta(days=1), 3)
    )


def test_limits_wait_until_after_the_close() -> None:
    state = _state()
    state.known["limits-ws"] = ["tests/x.py::test_wide"]
    state.known["limits-second-connection"] = ["tests/x.py::test_second"]
    state.known["evidence"] = state.known["smoke"] = state.known["luld"] = []
    state.known["rate-limits"] = []
    for example in schedule.known_names(state, "responses", WEDNESDAY) or []:
        record(state, check_of(state, "responses", example), status="passed", reason="", day="")
    now = _at(WEDNESDAY, 13)
    assert next_step(state, _probed(now), now) == Step("sleep", "limits-ws", _at(WEDNESDAY, 16, 15))


def test_a_slot_past_its_latest_start_waits_for_the_next_day() -> None:
    state = _state()
    _collected(state)
    now = _at(WEDNESDAY, 23)
    assert next_step(state, _probed(now), now) == Step(
        "sleep", "the next probe", _at(WEDNESDAY + timedelta(days=1), 8)
    )


def test_a_weekend_sleeps_until_mondays_probe() -> None:
    now = _at(date(2026, 11, 7), 12)
    assert next_step(_state(), _probed(now), now) == Step(
        "sleep", "the next probe", _at(date(2026, 11, 9), 8)
    )


def test_a_stream_day_probes_early_and_streams() -> None:
    state = _state(stocks="starter")
    state.stream_day = WEDNESDAY.isoformat()
    memory = RunMemory(probed_at=_at(WEDNESDAY, 3))
    assert next_step(state, memory, _at(WEDNESDAY, 3, 20)) == Step("probe")
    memory.probed_at = _at(WEDNESDAY, 3, 20)
    assert next_step(state, memory, _at(WEDNESDAY, 3, 21)) == Step(
        "sleep", "stream-day", _at(WEDNESDAY, 3, 45)
    )
    assert next_step(state, memory, _at(WEDNESDAY, 4)) == Step(
        "run", "stream-day", _at(WEDNESDAY, 20, 30)
    )


def test_the_next_probe_after_a_planned_stream_day_is_its_early_one() -> None:
    state = _state(stocks="starter")
    state.stream_day = "2026-11-05"
    assert schedule.next_probe(state, WEDNESDAY) == _at(date(2026, 11, 5), 3, 15)


def test_a_stream_day_restarts_a_dead_child_a_few_times_only() -> None:
    state = _state(stocks="starter")
    for _ in range(schedule.MAX_STREAM_STARTS):
        assert not schedule.ran_today(state, "stream-day", WEDNESDAY)
        state.attempts.append(Attempt("stream-day", "s", WEDNESDAY.isoformat(), ""))
    assert schedule.ran_today(state, "stream-day", WEDNESDAY)


def test_an_interrupted_attempt_does_not_count_as_run() -> None:
    state = _state()
    state.attempts.append(Attempt("evidence", "e", WEDNESDAY.isoformat(), "", "interrupted"))
    assert not schedule.ran_today(state, "evidence", WEDNESDAY)


@pytest.mark.parametrize(
    ("group", "tiers", "names"),
    [
        ("evidence", {}, None),
        (
            "stream-day",
            {"stocks": "starter", "currencies": "starter"},
            ["stocks delayed", "currencies real-time"],
        ),
        ("throughput", {"stocks": "advanced", "indices": "starter"}, ["stocks real-time"]),
        ("upgrade-check", {}, []),
    ],
)
def test_the_known_checks_of_each_group(
    group: str, tiers: dict[str, str], names: list[str] | None
) -> None:
    assert schedule.known_names(_state(**tiers), group, WEDNESDAY) == names


def test_without_any_probe_no_unit_is_known() -> None:
    assert schedule.known_names(RunState(commit="c"), "stream-day", WEDNESDAY) == []


def test_half_day_checks_are_named_after_their_half_day() -> None:
    state = _state()
    state.known["half-day"] = ["t.py::test_stamps"]
    monday = date(2026, 11, 30)
    assert schedule.known_names(state, "half-day", monday) == ["t.py::test_stamps after 2026-11-27"]
    assert schedule.half_day_suffix(WEDNESDAY) == ""


def test_a_reconciliation_is_due_for_each_unit_that_streamed_a_whole_day() -> None:
    state = _state(stocks="starter")
    check = check_of(state, "stream-day", "stocks delayed")
    record(state, check, status="passed", reason="", day="2026-11-03")
    check_of(state, "stream-day", "options delayed")
    assert schedule.due_names(state, "stream-day-reconcile", WEDNESDAY) == []  # no day yet
    state.stream_days = ["2026-11-04"]
    assert schedule.due_names(state, "stream-day-reconcile", WEDNESDAY) == []  # not over
    state.stream_days = ["2026-11-03"]
    assert schedule.due_names(state, "stream-day-reconcile", WEDNESDAY) == ["stocks delayed"]


def test_any_time_leaves_out_the_stream_examples() -> None:
    state = _state()
    every = schedule.due_names(state, "responses", WEDNESDAY) or []
    rest_only = schedule.due_names(state, "responses", WEDNESDAY, any_time=True) or []
    assert "stream-forex-quotes" in every
    assert "stream-forex-quotes" not in rest_only
    assert set(rest_only) < set(every)


def test_has_due_counts_a_group_never_collected() -> None:
    state = _state()
    assert schedule.has_due(state, "evidence", WEDNESDAY)
    state.known["evidence"] = []
    assert not schedule.has_due(state, "evidence", WEDNESDAY)


def test_the_next_start_of_a_group_is_shown_in_new_york_time() -> None:
    state = _state()
    morning = _at(WEDNESDAY, 9)
    assert schedule.next_start_text(state, "evidence", morning) == "Wed 2026-11-04 11:30 EST"
    noon = _at(WEDNESDAY, 12)
    assert schedule.next_start_text(state, "evidence", noon) == "Wed 2026-11-04 12:00 EST"
    evening = _at(WEDNESDAY, 19)
    assert schedule.next_start_text(state, "evidence", evening) == "Thu 2026-11-05 11:30 EST"
    assert schedule.next_start_text(state, "upgrade-check", noon) == "not scheduled"


# --- the stream night ---------------------------------------------------------------------------

THURSDAY = date(2026, 11, 5)


def _night_state(evening: date = WEDNESDAY) -> RunState:
    state = _state(currencies="starter", futures="starter")
    _collected(state)
    state.stream_night = evening.isoformat()
    return state


def test_a_planned_night_runs_from_its_evening_to_the_next_morning() -> None:
    state = _night_state()
    now = _at(WEDNESDAY, 16, 30)
    assert next_step(state, _probed(now), now) == Step("run", "stream-night", _at(THURSDAY, 9, 30))


def test_a_night_restarts_after_midnight_until_its_last_start() -> None:
    state = _night_state()
    state.attempts.append(Attempt("stream-night", "n", WEDNESDAY.isoformat(), ""))
    assert schedule.night_starts(state) == 1
    now = _at(THURSDAY, 2)
    assert next_step(state, _probed(now), now) == Step("run", "stream-night", _at(THURSDAY, 9, 30))
    late = _at(THURSDAY, 9, 1)  # past 09:00: the morning's own groups come next
    assert next_step(state, _probed(late), late) == Step("sleep", "luld", _at(THURSDAY, 9, 28))


def test_a_night_whose_child_started_too_often_runs_no_more() -> None:
    state = _night_state()
    for _ in range(schedule.MAX_STREAM_STARTS):
        state.attempts.append(Attempt("stream-night", "n", WEDNESDAY.isoformat(), ""))
    now = _at(WEDNESDAY, 23)
    assert next_step(state, _probed(now), now) == Step("sleep", "the next probe", _at(THURSDAY, 8))


def test_a_night_whose_units_passed_runs_no_more() -> None:
    state = _night_state()
    for unit in ("crypto real-time", "futures delayed"):
        record(state, check_of(state, "stream-night", unit), status="passed", reason="", day="")
    now = _at(WEDNESDAY, 17)
    assert next_step(state, _probed(now), now).group != "stream-night"


def test_any_time_groups_wait_through_the_nights_hours() -> None:
    state = _night_state()
    for _ in range(schedule.MAX_STREAM_STARTS):
        state.attempts.append(Attempt("stream-night", "n", WEDNESDAY.isoformat(), ""))
    state.any_time = ["evidence"]
    now = _at(THURSDAY, 1)
    assert next_step(state, _probed(now), now) == Step(
        "sleep", "the stream night's latest restart", _at(THURSDAY, 9)
    )
    assert state.any_time == ["evidence"]
    late = _at(THURSDAY, 9)  # the night's child may start no more: the morning goes on
    assert next_step(state, _probed(late), late) == Step(
        "run", "evidence", _at(date(2026, 11, 6), 3)
    )


def test_no_slot_starts_during_a_night_whose_child_gave_up() -> None:
    state = _night_state()
    state.known = {group: [] for group in state.known}
    state.known["limits-ws"] = ["tests/x.py::test_wide"]
    for example in schedule.known_names(state, "responses", WEDNESDAY) or []:
        record(state, check_of(state, "responses", example), status="passed", reason="", day="")
    for _ in range(schedule.MAX_STREAM_STARTS):
        state.attempts.append(Attempt("stream-night", "n", WEDNESDAY.isoformat(), ""))
    evening = _at(WEDNESDAY, 18)  # limits-ws is open until 21:00, but the night holds the key
    assert next_step(state, _probed(evening), evening) == Step(
        "sleep", "the next probe", _at(THURSDAY, 8)
    )
    assert not schedule.night_may_start(state, evening)


def test_the_evenings_groups_stop_before_the_night_and_none_starts_late() -> None:
    state = _night_state()
    noon = _at(WEDNESDAY, 12)
    assert schedule.night_clearance(state, noon) == _at(WEDNESDAY, 16, 15)
    assert next_step(state, _probed(noon), noon) == Step(
        "run", "rate-limits", _at(WEDNESDAY, 16, 15)
    )
    early = _at(WEDNESDAY, 8, 30)  # a wait that ends before the clearance stays
    assert next_step(state, _probed(early), early) == Step("sleep", "luld", _at(WEDNESDAY, 9, 28))
    late = _at(WEDNESDAY, 16, 20)
    assert next_step(state, _probed(late), late) == Step(
        "sleep", "stream-night", _at(WEDNESDAY, 16, 30)
    )
    assert schedule.night_clearance(state, _at(WEDNESDAY, 16, 30)) is None


def test_a_group_that_would_open_after_the_clearance_waits_for_another_day() -> None:
    state = _night_state()
    state.known = {group: [] for group in state.known}
    state.known["limits-ws"] = ["tests/x.py::test_wide"]
    for example in schedule.known_names(state, "responses", WEDNESDAY) or []:
        record(state, check_of(state, "responses", example), status="passed", reason="", day="")
    now = _at(WEDNESDAY, 13)
    assert next_step(state, _probed(now), now) == Step(
        "sleep", "stream-night", _at(WEDNESDAY, 16, 30)
    )


def test_a_sunday_sleeps_until_its_night_rather_than_mondays_probe() -> None:
    sunday = date(2026, 11, 8)
    state = _night_state(sunday)
    now = _at(sunday, 9)
    assert next_step(state, _probed(now), now) == Step("sleep", "stream-night", _at(sunday, 16, 30))
    start = _at(sunday, 16, 30)
    assert next_step(state, _probed(start), start) == Step(
        "run", "stream-night", _at(date(2026, 11, 9), 9, 30)
    )


def test_the_night_units_and_their_reconciliation() -> None:
    state = _night_state()
    assert schedule.known_names(state, "stream-night", WEDNESDAY) == [
        "crypto real-time",
        "futures delayed",
    ]
    record(
        state,
        check_of(state, "stream-night", "crypto real-time"),
        status="passed",
        reason="",
        day="",
    )
    state.stream_nights = ["2026-11-04"]
    assert schedule.due_names(state, "stream-night-reconcile", WEDNESDAY) == []  # not over
    assert schedule.due_names(state, "stream-night-reconcile", THURSDAY) == ["crypto real-time"]


def test_the_next_start_of_the_night_is_shown_in_new_york_time() -> None:
    state = _night_state()
    noon = _at(WEDNESDAY, 12)
    assert schedule.next_start_text(state, "stream-night", noon) == "Wed 2026-11-04 16:30 EST"
    during = _at(THURSDAY, 1)
    assert schedule.next_start_text(state, "stream-night", during) == "Thu 2026-11-05 01:00 EST"
    after = _at(THURSDAY, 9, 30)
    assert schedule.next_start_text(state, "stream-night", after) == "not scheduled"
    assert schedule.next_start_text(_state(), "stream-night", noon) == "not scheduled"
