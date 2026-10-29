"""Tests of the live tests' exchange calendar and recent days (`tests/_live/calendar.py`)."""

from __future__ import annotations

from datetime import date

import pytest

from tests._live import calendar

# Weekday closures of the New York Stock Exchange, 2024 to 2026, without the unscheduled
# national day of mourning of 2025-01-09, which no fixed rule predicts.
_NYSE_CLOSED = frozenset(
    {
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 12, 25),
        date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 12, 25),
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    }
)  # fmt: skip


def test_the_holiday_rules_give_the_nyse_closures() -> None:
    found = calendar.market_holidays(2024) | calendar.market_holidays(2025)
    assert found | calendar.market_holidays(2026) == _NYSE_CLOSED


def test_a_saturday_new_year_closes_no_friday_before() -> None:
    assert date(2021, 12, 31) not in calendar.market_holidays(2021)
    assert date(2022, 1, 1) in calendar.market_holidays(2022)


def test_a_sunday_new_year_closes_the_monday_after() -> None:
    assert date(2023, 1, 2) in calendar.market_holidays(2023)


def test_juneteenth_is_a_holiday_from_2022() -> None:
    assert date(2021, 6, 18) not in calendar.market_holidays(2021)
    assert date(2022, 6, 20) in calendar.market_holidays(2022)


def test_a_weekend_holiday_moves_to_the_nearest_weekday() -> None:
    assert calendar.observed(date(2027, 12, 25)) == date(2027, 12, 24)
    assert calendar.observed(date(2022, 12, 25)) == date(2022, 12, 26)
    assert calendar.observed(date(2026, 12, 25)) == date(2026, 12, 25)


@pytest.mark.parametrize(
    "easter",
    [
        date(2000, 4, 23), date(2008, 3, 23), date(2011, 4, 24), date(2019, 4, 21),
        date(2024, 3, 31), date(2025, 4, 20), date(2026, 4, 5), date(2027, 3, 28),
        date(2038, 4, 25),
    ],
)  # fmt: skip
def test_easter(easter: date) -> None:
    assert calendar.easter(easter.year) == easter


def test_nth_and_last_weekday() -> None:
    assert calendar.nth_weekday(2026, 5, 0, nth=-1) == date(2026, 5, 25)
    assert calendar.nth_weekday(2026, 12, 0, nth=-1) == date(2026, 12, 28)
    assert calendar.nth_weekday(2026, 12, 3, nth=2) == date(2026, 12, 10)
    assert calendar.thanksgiving(2026) == date(2026, 11, 26)


def test_months_before() -> None:
    assert calendar.months_before(date(2026, 1, 20), 2) == date(2025, 11, 1)
    assert calendar.months_before(date(2026, 12, 5), -1) == date(2027, 1, 1)


def test_trading_days_skip_weekends_and_holidays() -> None:
    assert calendar.is_trading_day(date(2026, 10, 29))
    assert not calendar.is_trading_day(date(2026, 10, 31))
    assert not calendar.is_trading_day(date(2026, 11, 26))
    assert calendar.trading_day_before(date(2026, 1, 20)) == date(2026, 1, 16)
    assert calendar.trading_day_before(date(2026, 11, 27)) == date(2026, 11, 25)


def test_the_recent_monday_is_published_and_traded() -> None:
    assert calendar.recent_monday(date(2026, 10, 29)) == date(2026, 10, 26)
    assert calendar.recent_monday(date(2026, 10, 27)) == date(2026, 10, 19)
    assert calendar.recent_monday(date(2026, 9, 10)) == date(2026, 8, 31)


def test_the_recent_week_has_ended_and_has_no_holiday() -> None:
    assert calendar.recent_week(date(2026, 10, 29)) == (date(2026, 10, 19), date(2026, 10, 25))
    assert calendar.recent_week(date(2026, 10, 25)) == (date(2026, 10, 12), date(2026, 10, 18))
    assert calendar.recent_week(date(2026, 9, 14)) == (date(2026, 8, 31), date(2026, 9, 6))


def test_the_recent_month_has_ended() -> None:
    assert calendar.recent_month(date(2026, 1, 5)) == (date(2025, 12, 1), date(2025, 12, 31))


def test_half_days() -> None:
    assert calendar.half_days(2025) == [date(2025, 7, 3), date(2025, 11, 28), date(2025, 12, 24)]
    # July 3, 2026, a Friday, is the observed Independence Day, not a half day.
    assert calendar.half_days(2026) == [date(2026, 11, 27), date(2026, 12, 24)]
    assert calendar.is_half_day(date(2025, 11, 28))
    assert not calendar.is_half_day(date(2025, 11, 26))


@pytest.mark.parametrize(
    ("today", "full", "half"),
    [
        (date(2026, 10, 29), date(2025, 12, 23), date(2025, 12, 24)),
        (date(2026, 11, 29), date(2025, 12, 23), date(2025, 12, 24)),
        (date(2026, 11, 30), date(2026, 11, 25), date(2026, 11, 27)),
        (date(2025, 7, 7), date(2025, 7, 2), date(2025, 7, 3)),
    ],
)
def test_the_stamp_days_are_the_latest_half_day_of_an_ended_week(
    today: date, full: date, half: date
) -> None:
    assert calendar.stamp_days(today)["day"] == (full, half)


def test_the_stamp_days_of_every_unit() -> None:
    assert calendar.stamp_days(date(2026, 10, 29)) == {
        "day": (date(2025, 12, 23), date(2025, 12, 24)),
        "week": (date(2025, 12, 15), date(2025, 12, 28)),
        "month": (date(2026, 8, 1), date(2026, 9, 30)),
        "quarter": (date(2026, 4, 1), date(2026, 9, 30)),
        "year": (date(2026, 1, 1), date(2026, 10, 29)),
    }


def test_the_recent_holiday_is_a_week_old_or_older() -> None:
    assert calendar.recent_holiday("good friday", date(2026, 10, 29)) == date(2026, 4, 3)
    assert calendar.recent_holiday("good friday", date(2026, 4, 9)) == date(2025, 4, 18)
    assert calendar.recent_holiday("martin luther king day", date(2026, 1, 26)) == date(2026, 1, 19)


def test_today_is_a_new_york_day() -> None:
    assert isinstance(calendar.today(), date)


def test_memorial_day_is_the_last_monday_of_may() -> None:
    assert date(2027, 5, 31) in calendar.market_holidays(2027)
    assert date(2027, 5, 24) not in calendar.market_holidays(2027)
    assert date(2021, 5, 31) in calendar.market_holidays(2021)


def test_a_week_whose_monday_is_a_holiday_is_skipped() -> None:
    # Martin Luther King Jr. Day 2026 is Monday, January 19.
    assert calendar.recent_week(date(2026, 1, 26)) == (date(2026, 1, 12), date(2026, 1, 18))


def test_a_week_whose_friday_is_a_holiday_is_skipped() -> None:
    # Good Friday 2026 is April 3.
    assert calendar.recent_week(date(2026, 4, 6)) == (date(2026, 3, 23), date(2026, 3, 29))
