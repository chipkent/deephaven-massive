"""Tests of the front futures contract by roll rule (`tests/_live/contracts.py`)."""

from __future__ import annotations

from datetime import date

import pytest

from tests._live import contracts


@pytest.mark.parametrize(
    ("day", "front"),
    [
        (date(2026, 10, 29), "ESZ6"),
        (date(2026, 12, 9), "ESZ6"),
        (date(2026, 12, 10), "ESH7"),  # Friday December 18 less eight days
        (date(2026, 3, 2), "ESH6"),
        (date(2026, 4, 3), "ESM6"),
        (date(2026, 5, 7), "ESM6"),
        (date(2027, 9, 8), "ESU7"),
        (date(2027, 9, 9), "ESZ7"),
        # December 2028 starts on a Friday: the roll, the 7th, is the first Thursday.
        (date(2028, 12, 6), "ESZ8"),
        (date(2028, 12, 7), "ESH9"),
    ],
)
def test_the_e_mini_rolls_eight_days_before_its_third_friday(day: date, front: str) -> None:
    assert contracts.front_contract("ES", day) == front


@pytest.mark.parametrize(
    ("day", "front"),
    [
        (date(2026, 2, 26), "ZCH6"),
        (date(2026, 2, 27), "ZCK6"),  # first notice day of March, the month's last weekday
        (date(2026, 3, 2), "ZCK6"),
        (date(2026, 12, 2), "ZCH7"),
    ],
)
def test_corn_rolls_on_its_first_notice_day(day: date, front: str) -> None:
    assert contracts.front_contract("ZC", day) == front


@pytest.mark.parametrize(
    ("day", "front"),
    [
        (date(2026, 9, 29), "LEV6"),
        (date(2026, 9, 30), "LEZ6"),  # the last weekday before October, the contract's month
        (date(2026, 10, 30), "LEZ6"),
        (date(2026, 11, 30), "LEG7"),
    ],
)
def test_live_cattle_rolls_before_its_delivery_month(day: date, front: str) -> None:
    assert contracts.front_contract("LE", day) == front


def test_roll_days() -> None:
    assert contracts.roll_day("ES", 2026, 12) == date(2026, 12, 10)
    assert contracts.roll_day("ZC", 2026, 5) == date(2026, 4, 30)
