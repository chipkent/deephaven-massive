"""Tests of the live run's daily timetable (`tests/_live/timetable.py`).

Days of late 2026 serve as fixed examples: Thanksgiving is Thursday 26 November, its Friday a
half day, so Monday 30 November follows a half day.
"""

from __future__ import annotations

from datetime import date, time

from tests._live import timetable
from tests._live.timetable import DayKind

MONDAY = date(2026, 11, 2)
TUESDAY = date(2026, 11, 3)
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
    assert groups(TUESDAY, DayKind.AFTER_STREAM)[0] == "throughput"
    assert "half-day" in groups(AFTER_HALF_DAY, DayKind.ORDINARY)


def test_stops() -> None:
    assert timetable.stop_of(MONDAY, timetable.STREAM_DAY) == timetable.at(MONDAY, time(20, 30))
    assert timetable.stop_of(MONDAY, timetable.LULD) == timetable.at(TUESDAY, time(3, 0))
    assert timetable.at(MONDAY, time(9, 30)).utcoffset() is not None


def test_a_stream_day_avoids_days_after_a_stream_day_or_a_half_day() -> None:
    assert timetable.next_stream_day(MONDAY, []) == TUESDAY
    assert timetable.next_stream_day(MONDAY, ["2026-11-02"]) == date(2026, 11, 4)
    # From the half day, Monday 30 November follows it; Tuesday 1 December is the next.
    assert timetable.next_stream_day(date(2026, 11, 25), []) == date(2026, 11, 27)
    assert timetable.next_stream_day(date(2026, 11, 27), []) == date(2026, 12, 1)
