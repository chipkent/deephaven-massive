"""Tests of the week-window rule (`tests/_live/week_windows.py`)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from tests._live import week_windows
from tests._live.calendar import NEW_YORK

_HOUR = 3_600_000
_DAY = 24 * _HOUR
_SUNDAY = int(datetime(2026, 9, 20, tzinfo=UTC).timestamp() * 1000)
_HOURS: list[dict[str, Any]] = [
    {"t": _SUNDAY - 3 * _DAY + n * _HOUR, "v": float(n + 1)} for n in range(24 * 14)
]
"""Hour bars of rising volume, so every candidate window has a volume of its own."""


def _week(start_ms: int) -> float:
    return float(
        sum(h["v"] for h in _HOURS if start_ms <= h["t"] < start_ms + week_windows.WEEK_MS)
    )


def test_the_candidates_mark_sunday_midnight_in_the_class_zone() -> None:
    found = week_windows.candidates(_SUNDAY, UTC)
    assert len(found) == 21
    library = [label for label, _, is_library in found if is_library]
    assert library == ["Sun 09-20 00:00 UTC"]
    new_york = [
        label for label, _, is_library in week_windows.candidates(_SUNDAY, NEW_YORK) if is_library
    ]
    assert new_york == ["Sun 09-20 00:00 New York"]


def test_the_library_week_is_closest_when_it_is_the_bucket() -> None:
    scored = week_windows.score(_SUNDAY, _week(_SUNDAY), _HOURS, zone=UTC)
    assert scored[0] == (0.0, "Sun 09-20 00:00 UTC", True)
    assert week_windows.library_is_closest(scored)


def test_a_monday_bucket_is_not_the_library_week() -> None:
    monday = _SUNDAY + _DAY
    scored = week_windows.score(_SUNDAY, _week(monday), _HOURS, zone=UTC)
    assert scored[0][1] == "Mon 09-21 00:00 UTC"
    assert not week_windows.library_is_closest(scored)


def test_a_tie_counts_for_the_library_week() -> None:
    # A stock-like fortnight: trading on weekday afternoons only, so the windows that start
    # at Sunday 00:00 New York and at Monday 00:00 New York hold the same hours.
    monday = _SUNDAY + _DAY
    hours = [
        {"t": monday + d * _DAY + h * _HOUR, "v": float(d * 10 + h)}
        for d in range(14)
        if d % 7 < 5
        for h in range(14, 21)
    ]
    volume = float(sum(h["v"] for h in hours if h["t"] < monday + week_windows.WEEK_MS))
    scored = week_windows.score(_SUNDAY, volume, hours, zone=NEW_YORK)
    tied = [label for error, label, _ in scored if error == 0.0]
    assert "Sun 09-20 00:00 New York" in tied
    assert "Mon 09-21 00:00 New York" in tied
    assert week_windows.library_is_closest(scored)


def test_the_stock_week_of_the_live_check_is_its_sunday_week_of_day_bars() -> None:
    # AAPL in the live check of 2026-10-30: the week bar stamped Sunday 2026-10-18 04:00 UTC
    # (00:00 New York time) holds exactly the volume of the day bars of Monday to Friday.
    stamp = int(datetime(2026, 10, 18, 4, tzinfo=UTC).timestamp() * 1000)
    volumes = {
        12: 25900599.107368,
        13: 38983839.306983,
        14: 50061053.04604,
        15: 39767256.151435,
        16: 59946368.513045,
        19: 34573181.500186,
        20: 35851382.759126,
        21: 35614126.795002,
        22: 46898875.748938,
        23: 39539690.012018,
    }
    days = [
        {"t": int(datetime(2026, 10, day, 4, tzinfo=UTC).timestamp() * 1000), "v": volume}
        for day, volume in volumes.items()
    ]
    scored = week_windows.score(stamp, 192477256.81527, days, zone=NEW_YORK)
    assert scored[0][0] <= week_windows.MATCH
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
