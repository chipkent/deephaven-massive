"""Tests of the live run's daily timetable (`tests/_live/live_run/timetable.py`).

Days of late 2026 serve as fixed examples: Thanksgiving is Thursday 26 November, its Friday a
half day, so Monday 30 November follows a half day.
"""

from __future__ import annotations

from datetime import date, time

from tests._live.live_run import timetable
from tests._live.live_run.timetable import DayKind

MONDAY = date(2026, 11, 2)
TUESDAY = date(2026, 11, 3)
WEDNESDAY = date(2026, 11, 4)
AFTER_HALF_DAY = date(2026, 11, 30)


def test_each_kind_of_day() -> None:
    def kind(day: date, stream_day: str = "", stream_days: tuple[str, ...] = ()) -> DayKind:
        return timetable.day_kind(day, stream_day=stream_day, stream_days=list(stream_days))

    assert kind(date(2026, 11, 1)) is DayKind.CLOSED  # a Sunday
    assert kind(date(2026, 11, 26)) is DayKind.CLOSED  # Thanksgiving
    assert kind(MONDAY, stream_day="2026-11-02") is DayKind.STREAM
    assert kind(TUESDAY, stream_days=("2026-11-02",)) is DayKind.AFTER_STREAM
    assert kind(TUESDAY) is DayKind.ORDINARY


def test_the_half_day_a_day_follows() -> None:
    assert timetable.after_half_day(AFTER_HALF_DAY) == date(2026, 11, 27)
    assert timetable.after_half_day(TUESDAY) is None


def test_probe_times() -> None:
    assert timetable.probe_time(DayKind.STREAM) == time(3, 15)
    assert timetable.probe_time(DayKind.ORDINARY) == time(8, 0)
    assert timetable.probe_time(DayKind.AFTER_STREAM) == time(8, 0)
    assert timetable.probe_time(DayKind.CLOSED) is None


def test_slots_of_each_kind_of_day() -> None:
    def groups(day: date, kind: DayKind) -> list[str]:
        return [slot.group for slot in timetable.slots(day, kind)]

    assert groups(MONDAY, DayKind.CLOSED) == []
    assert groups(MONDAY, DayKind.STREAM) == ["stream-day"]
    ordinary = groups(TUESDAY, DayKind.ORDINARY)
    assert ordinary[0] == "luld"
    assert "half-day" not in ordinary
    assert "stream-night-reconcile" in ordinary
    assert groups(TUESDAY, DayKind.AFTER_STREAM)[0] == "throughput"
    assert "half-day" in groups(AFTER_HALF_DAY, DayKind.ORDINARY)


def test_stops() -> None:
    assert timetable.stop_of(MONDAY, timetable.STREAM_DAY) == timetable.at(MONDAY, time(20, 30))
    assert timetable.stop_of(MONDAY, timetable.LULD) == timetable.at(TUESDAY, time(3, 0))
    assert timetable.at(MONDAY, time(9, 30)).utcoffset() is not None


def test_a_stream_day_avoids_days_after_a_stream_day_or_a_half_day() -> None:
    assert timetable.next_stream_day(MONDAY, [], night="") == TUESDAY
    assert timetable.next_stream_day(MONDAY, ["2026-11-02"], night="") == date(2026, 11, 4)
    # From the half day, Monday 30 November follows it; Tuesday 1 December is the next.
    assert timetable.next_stream_day(date(2026, 11, 25), [], night="") == date(2026, 11, 27)
    assert timetable.next_stream_day(date(2026, 11, 27), [], night="") == date(2026, 12, 1)


def test_a_stream_day_avoids_the_evening_and_the_morning_of_the_planned_night() -> None:
    # Monday evening's night holds Monday's evening and Tuesday's morning.
    assert timetable.next_stream_day(date(2026, 11, 1), [], night="2026-11-02") == WEDNESDAY
    assert timetable.next_stream_day(MONDAY, [], night="2026-11-02") == WEDNESDAY


def test_a_night_runs_from_the_evening_to_the_next_morning() -> None:
    assert timetable.night_start(MONDAY) == timetable.at(MONDAY, time(16, 30))
    assert timetable.night_end(MONDAY) == timetable.at(TUESDAY, time(9, 30))


def test_which_evenings_can_be_a_night() -> None:
    def can(evening: date, *, stream_day: str = "", closures: dict[str, str] | None = None) -> bool:
        return timetable.can_be_night(evening, stream_day=stream_day, closures=closures or {})

    assert can(date(2026, 11, 1))  # a Sunday: futures open for the week that evening
    assert can(MONDAY)
    assert can(date(2026, 11, 5))  # a Thursday, before a full Friday
    assert not can(date(2026, 11, 6))  # a Friday: no futures session runs into Saturday
    assert not can(date(2026, 11, 7))  # a Saturday
    assert not can(date(2026, 11, 25))  # its morning is Thanksgiving
    assert not can(date(2026, 11, 26))  # Thanksgiving, before a half day
    assert can(date(2026, 11, 29))  # the Sunday after
    # Massive's own closures: a closed morning, or an early close on the evening, rule it out.
    assert not can(MONDAY, closures={"2026-11-03": "closed"})
    assert not can(TUESDAY, closures={"2026-11-03": "early-close"})
    assert can(date(2026, 11, 1), closures={"2026-11-01": "closed"})  # a Sunday's own status
    # The planned stream day needs its morning and its evening.
    assert not can(MONDAY, stream_day="2026-11-03")
    assert not can(TUESDAY, stream_day="2026-11-03")
    assert can(WEDNESDAY, stream_day="2026-11-03")


def test_a_night_is_planned_for_the_first_evening_after_today_that_can_be_one() -> None:
    def plan(today: date, stream_day: str = "") -> date:
        return timetable.next_stream_night(today, stream_day=stream_day, closures={})

    assert plan(MONDAY) == TUESDAY
    assert plan(date(2026, 11, 5)) == date(2026, 11, 8)  # Friday and Saturday cannot be
    assert plan(MONDAY, stream_day="2026-11-04") == date(2026, 11, 5)
