"""Tests for the conversions between aware datetimes and epoch nanoseconds."""

from datetime import UTC, datetime, timedelta, timezone

from deephaven_massive._datasets.epoch import DAY_NS, EPOCH, epoch_instant, epoch_ns


def test_the_epoch_is_1970_in_utc() -> None:
    assert datetime(1970, 1, 1, tzinfo=UTC) == EPOCH
    assert epoch_ns(EPOCH) == 0


def test_a_datetime_converts_exactly_whatever_its_zone() -> None:
    moment = datetime(2025, 3, 3, 9, 30, 0, 123_456, tzinfo=timezone(timedelta(hours=-5)))
    ns = epoch_ns(moment)
    assert ns == 1_741_012_200_123_456_000
    assert epoch_instant(ns) == moment
    assert epoch_ns(datetime(1970, 1, 2, tzinfo=UTC)) == DAY_NS


def test_nanoseconds_floor_to_the_microsecond_on_both_sides_of_the_epoch() -> None:
    assert epoch_instant(1_999) == EPOCH + timedelta(microseconds=1)
    assert epoch_instant(-1) == EPOCH - timedelta(microseconds=1)
    assert epoch_ns(EPOCH - timedelta(microseconds=1)) == -1_000
