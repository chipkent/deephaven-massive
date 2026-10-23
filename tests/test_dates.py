"""Tests for date parsing and the day rules (DESIGN § 12, § 16)."""

from datetime import UTC, date, datetime, time, timedelta
from typing import cast
from zoneinfo import ZoneInfo

import pytest

from deephaven_massive._dates import (
    CHICAGO,
    DAY_ZONES,
    NEW_YORK,
    TimeRange,
    flat_file_published,
    futures_sessions,
    history_range,
    parse_date_like,
)
from deephaven_massive._enums import AssetClass

UTC_ZONE = ZoneInfo("UTC")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2025-03-03", date(2025, 3, 3)),
        (" 2025-03-03 ", date(2025, 3, 3)),
        (date(2025, 3, 3), date(2025, 3, 3)),
        ("2025-03-03T09:30:00-05:00", datetime(2025, 3, 3, 14, 30, tzinfo=UTC)),
        ("2025-03-03T14:30:00Z", datetime(2025, 3, 3, 14, 30, tzinfo=UTC)),
        (datetime(2025, 3, 3, 14, 30, tzinfo=UTC), datetime(2025, 3, 3, 14, 30, tzinfo=UTC)),
    ],
)
def test_parse_date_like(value: str | date, expected: date) -> None:
    parsed = parse_date_like(value, "start")
    assert parsed == expected
    assert type(parsed) is type(expected)


@pytest.mark.parametrize(
    "value", ["2025-03-03T09:30:00", datetime.fromisoformat("2025-03-03T09:30")]
)
def test_naive_times_are_rejected(value: str | datetime) -> None:
    with pytest.raises(ValueError, match=r"^timezone required$"):
        parse_date_like(value, "start")


def test_unparseable_and_wrong_types() -> None:
    with pytest.raises(ValueError, match="start='tomorrow' is not an ISO-8601"):
        parse_date_like("tomorrow", "start")
    with pytest.raises(ValueError, match="ISO-8601"):
        parse_date_like("2025-13-45", "end")
    with pytest.raises(TypeError, match="end must be a str, date, or datetime"):
        parse_date_like(cast("str", 20250303), "end")


def test_day_zones() -> None:
    assert DAY_ZONES[AssetClass.STOCKS] is NEW_YORK
    assert DAY_ZONES[AssetClass.OPTIONS] is NEW_YORK
    assert DAY_ZONES[AssetClass.INDICES] is NEW_YORK
    assert DAY_ZONES[AssetClass.FOREX] == UTC_ZONE
    assert DAY_ZONES[AssetClass.CRYPTO] == UTC_ZONE
    assert DAY_ZONES[AssetClass.FUTURES] is CHICAGO
    assert set(DAY_ZONES) == set(AssetClass)


def test_whole_days_are_inclusive_in_new_york() -> None:
    window = history_range("2025-03-03", "2025-03-03", NEW_YORK)
    assert window.start == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert window.end == datetime(2025, 3, 4, 5, tzinfo=UTC)
    assert window.end_ns - window.start_ns == 86_400 * 10**9


def test_daylight_saving_days() -> None:
    # 2025-03-09 is 23 hours long in New York.
    window = history_range(date(2025, 3, 9), date(2025, 3, 9), NEW_YORK)
    assert window.end - window.start == timedelta(hours=23)


def test_utc_days_and_datetimes() -> None:
    window = history_range("2025-03-03", datetime(2025, 3, 3, 12, tzinfo=UTC), UTC_ZONE)
    assert window == TimeRange(
        datetime(2025, 3, 3, tzinfo=UTC), datetime(2025, 3, 3, 12, tzinfo=UTC)
    )
    assert window.start_ns == 1_740_960_000 * 10**9


def test_empty_ranges_are_rejected() -> None:
    with pytest.raises(ValueError, match="must be before end"):
        history_range("2025-03-04", "2025-03-03", NEW_YORK)
    moment = datetime(2025, 3, 3, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="must be before end"):
        history_range(moment, moment, NEW_YORK)


def test_partition_dates() -> None:
    window = history_range("2025-03-03", "2025-03-05", NEW_YORK)
    assert window.partition_dates(NEW_YORK) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )
    # An end at exactly midnight excludes that day.
    exact = TimeRange(datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 4, 5, tzinfo=UTC))
    assert exact.partition_dates(NEW_YORK) == (date(2025, 3, 3),)


def test_epoch_ns_is_exact() -> None:
    window = TimeRange(
        datetime(2025, 3, 3, 0, 0, 0, 1, tzinfo=UTC), datetime(2025, 3, 3, 0, 0, 1, tzinfo=UTC)
    )
    assert window.start_ns % 10**9 == 1_000


def test_futures_session_widening() -> None:
    sessions = futures_sessions(date(2025, 3, 3), date(2025, 3, 5))
    assert (sessions.first, sessions.last) == (date(2025, 3, 3), date(2025, 3, 5))
    # One day either side, at midnight in Chicago (UTC-6 before March 9).
    assert sessions.window.start == datetime(2025, 3, 2, 6, tzinfo=UTC)
    assert sessions.window.end == datetime(2025, 3, 7, 6, tzinfo=UTC)
    single = futures_sessions(date(2025, 3, 3), date(2025, 3, 3))
    assert single.window.end - single.window.start == timedelta(days=3)
    with pytest.raises(ValueError, match="must not be after end"):
        futures_sessions(date(2025, 3, 5), date(2025, 3, 3))


CUTOFF = time(11, 30)


def _published(partition: date, now: datetime) -> bool:
    return flat_file_published(partition, now=now, zone=NEW_YORK, cutoff=CUTOFF)


def test_flat_file_publish_cutoff() -> None:
    today = date(2025, 3, 4)
    yesterday = date(2025, 3, 3)
    before = datetime(2025, 3, 4, 11, 29, tzinfo=NEW_YORK)
    after = datetime(2025, 3, 4, 11, 31, tzinfo=NEW_YORK)
    assert not _published(yesterday, before)
    assert _published(yesterday, after)
    assert _published(date(2025, 3, 2), before)
    assert not _published(today, after)
    assert not _published(date(2025, 3, 5), after)


def test_flat_file_day_zone_is_the_class_zone() -> None:
    # 01:00 UTC on March 4 is still March 3 in New York but March 4 in UTC, so March 3 is
    # "today" for a New York class and "yesterday" for a UTC class. The cutoff compares the
    # New York time of day (20:00 on March 3), exactly as DESIGN § 16 states the predicate.
    now = datetime(2025, 3, 4, 1, tzinfo=UTC)
    march_3 = date(2025, 3, 3)
    assert not flat_file_published(march_3, now=now, zone=NEW_YORK, cutoff=CUTOFF)
    assert flat_file_published(march_3, now=now, zone=UTC_ZONE, cutoff=CUTOFF)
    assert not flat_file_published(date(2025, 3, 4), now=now, zone=UTC_ZONE, cutoff=CUTOFF)
