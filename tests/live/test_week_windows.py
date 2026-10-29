"""Tests of the week-window rule (`tests/_live/week_windows.py`)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from tests._live import week_windows
from tests._live.calendar import NEW_YORK

_HOUR = 3_600_000
_MONDAY = int(datetime(2026, 9, 21, tzinfo=UTC).timestamp() * 1000)
_HOURS: list[dict[str, Any]] = [
    {"t": _MONDAY - 3 * 24 * _HOUR + n * _HOUR, "v": float(n + 1)} for n in range(24 * 14)
]
"""Hour bars of rising volume, so every candidate window has a volume of its own."""


def _week(start_ms: int) -> float:
    return float(
        sum(h["v"] for h in _HOURS if start_ms <= h["t"] < start_ms + week_windows.WEEK_MS)
    )


def test_the_candidates_mark_monday_midnight_in_the_class_zone() -> None:
    found = week_windows.candidates(_MONDAY, UTC)
    assert len(found) == 21
    library = [label for label, _, is_library in found if is_library]
    assert library == ["Mon 09-21 00:00 UTC"]
    new_york = [
        label for label, _, is_library in week_windows.candidates(_MONDAY, NEW_YORK) if is_library
    ]
    assert new_york == ["Mon 09-21 00:00 New York"]


def test_the_library_week_is_closest_when_it_is_the_bucket() -> None:
    scored = week_windows.score(_MONDAY, _week(_MONDAY), _HOURS, zone=UTC)
    assert scored[0] == (0.0, "Mon 09-21 00:00 UTC", True)
    assert week_windows.library_is_closest(scored)


def test_a_sunday_evening_bucket_is_not_the_library_week() -> None:
    sunday_evening = _MONDAY - 7 * _HOUR  # 17:00 New York, daylight time
    scored = week_windows.score(_MONDAY, _week(sunday_evening), _HOURS, zone=UTC)
    assert scored[0][1] == "Sun 09-20 17:00 New York"
    assert not week_windows.library_is_closest(scored)


def test_a_tie_counts_for_the_library_week() -> None:
    # A stock-like fortnight: trading on weekday afternoons only, so the windows that start
    # at Monday 00:00 UTC and at Monday 00:00 New York hold the same hours.
    day = 24 * _HOUR
    hours = [
        {"t": _MONDAY + d * day + h * _HOUR, "v": float(d * 10 + h)}
        for d in range(14)
        if d % 7 < 5
        for h in range(14, 21)
    ]
    volume = float(sum(h["v"] for h in hours if h["t"] < _MONDAY + week_windows.WEEK_MS))
    scored = week_windows.score(_MONDAY, volume, hours, zone=NEW_YORK)
    tied = [label for error, label, _ in scored if error == 0.0]
    assert "Mon 09-21 00:00 UTC" in tied
    assert "Mon 09-21 00:00 New York" in tied
    assert week_windows.library_is_closest(scored)


@pytest.mark.parametrize(
    ("stamp", "checked"),
    [
        (datetime(2026, 9, 26, tzinfo=UTC), True),  # the Saturday before the first Monday
        (datetime(2026, 9, 25, tzinfo=UTC), False),
        (datetime(2026, 10, 19, tzinfo=UTC), True),  # the last week's Monday
        (datetime(2026, 10, 20, tzinfo=UTC), False),
    ],
)
def test_the_checked_weeks(stamp: datetime, *, checked: bool) -> None:
    weeks = (date(2026, 9, 28), date(2026, 10, 25))
    assert week_windows.is_checked(int(stamp.timestamp() * 1000), weeks) is checked
