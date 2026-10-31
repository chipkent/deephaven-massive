"""Tests for date parsing, the kinds of bounds, and the day rules."""

import importlib
import logging
import re
from datetime import UTC, date, datetime, timedelta
from typing import TypeVar, cast

import numpy as np
import pytest
from deephaven.time import to_j_instant, to_j_local_date, to_j_local_time, to_j_zdt

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import (
    DAY_ZONES,
    MIXED_BOUNDS,
    NEW_YORK,
    DaySpan,
    TimeRange,
    bounds,
    calendar_day,
    check_order,
    day_class,
    day_end,
    day_start,
    day_zone,
    instant_bound,
    is_trading_day,
    midnight,
    parse_date_like,
)
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    futures_session_open,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix
from deephaven_massive._massive import WireUnit

T = TypeVar("T")

_DATES_LOG = "deephaven_massive._datasets.dates"

NOW = datetime(2025, 6, 18, 15, tzinfo=UTC)
"""A Wednesday afternoon, after every range of these tests unless a test says otherwise."""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2025-03-03", date(2025, 3, 3)),
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
    # The message names the parameter, the value as passed, and the fix.
    expected = f"start={value!r} needs a timezone: pass an aware datetime or a date"
    with pytest.raises(ValueError, match=f"^{re.escape(expected)}$"):
        parse_date_like(value, "start")


def test_the_naive_time_message_names_the_parameter_and_the_fix() -> None:
    with pytest.raises(ValueError, match="needs a timezone") as caught:
        parse_date_like(datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None), "start")
    assert str(caught.value) == (
        "start=datetime.datetime(2025, 1, 2, 9, 30) needs a timezone: "
        "pass an aware datetime or a date"
    )


def test_unparseable_and_wrong_types() -> None:
    with pytest.raises(ValueError, match="start='tomorrow' is not an ISO-8601"):
        parse_date_like("tomorrow", "start")
    with pytest.raises(ValueError, match="ISO-8601"):
        parse_date_like("2025-13-45", "end")
    with pytest.raises(TypeError, match=r"^end must be a str, date, datetime, Java Instant, "):
        parse_date_like(cast("str", 20250303), "end")


def test_day_zones() -> None:
    assert DAY_ZONES[AssetClass.STOCKS] is NEW_YORK
    assert DAY_ZONES[AssetClass.OPTIONS] is NEW_YORK
    assert DAY_ZONES[AssetClass.INDICES] is NEW_YORK
    assert DAY_ZONES[AssetClass.FOREX] == UTC
    assert DAY_ZONES[AssetClass.CRYPTO] == UTC
    # A futures date names a session, not a calendar day.
    assert set(DAY_ZONES) == set(AssetClass) - {AssetClass.FUTURES}


def test_whole_days_are_inclusive_in_new_york() -> None:
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 3))
    window = span.instants(NEW_YORK)
    assert window.start == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert window.end == datetime(2025, 3, 4, 5, tzinfo=UTC)
    assert window.end_ns - window.start_ns == 86_400 * 10**9
    assert span.days() == (date(2025, 3, 3),)


@pytest.mark.parametrize(
    ("day", "hours"),
    [(date(2025, 3, 9), 23), (date(2025, 11, 2), 25), (date(2025, 3, 10), 24)],
)
def test_daylight_saving_days(day: date, hours: int) -> None:
    window = DaySpan(day, day).instants(NEW_YORK)
    assert window.end - window.start == timedelta(hours=hours)


def test_utc_days() -> None:
    window = DaySpan(date(2025, 3, 3), date(2025, 3, 3)).instants(UTC)
    assert window == TimeRange(datetime(2025, 3, 3, tzinfo=UTC), datetime(2025, 3, 4, tzinfo=UTC))
    assert window.start_ns == 1_740_960_000 * 10**9


def test_partition_dates() -> None:
    window = DaySpan(date(2025, 3, 3), date(2025, 3, 5)).instants(NEW_YORK)
    assert window.partition_dates(NEW_YORK) == (
        date(2025, 3, 3),
        date(2025, 3, 4),
        date(2025, 3, 5),
    )
    # An end at exactly midnight excludes that day; an empty range has no dates.
    exact = TimeRange(datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 4, 5, tzinfo=UTC))
    assert exact.partition_dates(NEW_YORK) == (date(2025, 3, 3),)
    moment = datetime(2025, 3, 3, 12, tzinfo=UTC)
    assert TimeRange(moment, moment).empty
    assert TimeRange(moment, moment).partition_dates(NEW_YORK) == ()
    assert not exact.empty
    assert DaySpan(date(2025, 3, 4), date(2025, 3, 3)).empty
    assert DaySpan(date(2025, 3, 4), date(2025, 3, 3)).days() == ()


def test_partition_dates_across_the_spring_dst_change() -> None:
    window = DaySpan(date(2025, 3, 8), date(2025, 3, 10)).instants(NEW_YORK)
    assert window.partition_dates(NEW_YORK) == (
        date(2025, 3, 8),
        date(2025, 3, 9),
        date(2025, 3, 10),
    )


def test_epoch_ns_is_exact() -> None:
    window = TimeRange(
        datetime(2025, 3, 3, 0, 0, 0, 1, tzinfo=UTC), datetime(2025, 3, 3, 0, 0, 1, tzinfo=UTC)
    )
    assert window.start_ns % 10**9 == 1_000


def _spec(asset_class: AssetClass | ClassFromPrefix) -> DatasetSpec:
    return DatasetSpec(
        name="x.history",
        summary="A dataset for the tests.",
        asset_class=asset_class,
        history_path="/v3/trades/{stockTicker}",
    )


def test_dates_give_whole_days_and_datetimes_exact_instants() -> None:
    stocks = bounds(_spec(AssetClass.STOCKS), "2025-03-03", date(2025, 3, 3), now=NOW)
    assert stocks == DaySpan(date(2025, 3, 3), date(2025, 3, 3))
    start = datetime(2025, 3, 3, 9, 30, tzinfo=NEW_YORK)
    end = datetime(2025, 3, 3, 16, tzinfo=NEW_YORK)
    exact = bounds(_spec(AssetClass.STOCKS), start, end.isoformat(), now=NOW)
    assert exact == TimeRange(start.astimezone(UTC), end.astimezone(UTC))
    assert exact.start.tzinfo is UTC
    futures = bounds(_spec(AssetClass.FUTURES), "2025-03-03", "2025-03-05", now=NOW)
    assert futures == DaySpan(date(2025, 3, 3), date(2025, 3, 5))


def test_equal_dates_are_one_day_and_equal_datetimes_an_empty_range() -> None:
    one = bounds(_spec(AssetClass.STOCKS), "2025-03-03", "2025-03-03", now=NOW)
    assert isinstance(one, DaySpan)
    assert one.days() == (date(2025, 3, 3),)
    moment = datetime(2025, 3, 3, 12, tzinfo=UTC)
    empty = bounds(_spec(AssetClass.STOCKS), moment, moment, now=NOW)
    assert isinstance(empty, TimeRange)
    assert empty.empty


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2025-03-03", datetime(2025, 3, 3, 16, tzinfo=NEW_YORK)),
        (datetime(2025, 3, 3, 9, tzinfo=NEW_YORK), date(2025, 3, 4)),
        ("2025-03-03", "2025-03-04T00:00:00Z"),
    ],
)
@pytest.mark.parametrize("asset_class", [AssetClass.STOCKS, AssetClass.FUTURES])
def test_mixed_bounds_are_refused(start: object, end: object, asset_class: AssetClass) -> None:
    with pytest.raises(ValueError, match=f"^{re.escape(MIXED_BOUNDS)}; got start="):
        bounds(_spec(asset_class), cast("str", start), cast("str", end), now=NOW)


def test_reversed_ranges_are_refused() -> None:
    with pytest.raises(ValueError, match=r"^start \(.*\) must not be after end \(.*\)$"):
        bounds(_spec(AssetClass.FUTURES), "2025-03-05", "2025-03-03", now=NOW)
    later = datetime(2025, 3, 3, 12, 0, 0, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="must not be after end"):
        bounds(_spec(AssetClass.STOCKS), later, datetime(2025, 3, 3, 12, tzinfo=UTC), now=NOW)
    with pytest.raises(ValueError, match=r"start='2025-03-03T09:30' needs a timezone"):
        bounds(_spec(AssetClass.STOCKS), "2025-03-03T09:30", "2025-03-04", now=NOW)


def test_an_open_end_reads_through_now() -> None:
    # 2025-06-18 15:00 UTC: 11:00 in New York, 10:00 in Chicago (the session of 06-18).
    stocks = bounds(_spec(AssetClass.STOCKS), "2025-06-16", None, now=NOW)
    assert stocks == DaySpan(date(2025, 6, 16), date(2025, 6, 18))
    late = datetime(2025, 6, 19, 1, tzinfo=UTC)  # 21:00 on 06-18 in New York, 06-19 in UTC
    assert bounds(_spec(AssetClass.STOCKS), "2025-06-16", None, now=late) == DaySpan(
        date(2025, 6, 16), date(2025, 6, 18)
    )
    assert bounds(_spec(AssetClass.CRYPTO), "2025-06-16", None, now=late) == DaySpan(
        date(2025, 6, 16), date(2025, 6, 19)
    )
    # A Chicago evening belongs to the next session.
    evening = datetime(2025, 6, 18, 23, tzinfo=UTC)  # 18:00 Chicago
    futures = bounds(_spec(AssetClass.FUTURES), "2025-06-16", None, now=evening)
    assert futures == DaySpan(date(2025, 6, 16), date(2025, 6, 19))
    start = datetime(2025, 6, 18, 13, tzinfo=UTC)
    assert bounds(_spec(AssetClass.STOCKS), start, None, now=NOW) == TimeRange(start, NOW)
    assert bounds(_spec(AssetClass.STOCKS), NOW, None, now=NOW).empty


def test_a_start_after_now_with_an_open_end_is_refused() -> None:
    with pytest.raises(ValueError, match=r"must not be after now .*; end=None reads through now$"):
        bounds(_spec(AssetClass.STOCKS), "2025-06-19", None, now=NOW)
    with pytest.raises(ValueError, match="end=None reads through now"):
        bounds(_spec(AssetClass.STOCKS), NOW + timedelta(microseconds=1), None, now=NOW)


def test_epoch_ns_of_a_mid_day_instant() -> None:
    window = TimeRange(
        datetime(2025, 3, 3, 14, 30, 15, 250_000, tzinfo=UTC), datetime(2025, 3, 3, 15, tzinfo=UTC)
    )
    # 2025-03-03T00:00Z is 1_740_960_000 s; 14:30:15.25 is 52_215.25 s later, 15:00 is 54_000.
    assert window.start_ns == 1_741_012_215_250_000_000
    assert window.end_ns == 1_741_014_000_000_000_000


def test_time_axis_order_compares_instants_across_the_fall_back_hour() -> None:
    # 01:30 EDT (fold=0) comes 45 minutes before 01:15 EST (fold=1) on 2025-11-02; compared by
    # wall clock (same tzinfo) the pair would look reversed.
    first = datetime(2025, 11, 2, 1, 30, tzinfo=NEW_YORK)
    later = datetime(2025, 11, 2, 1, 15, fold=1, tzinfo=NEW_YORK)
    check_order(first, later)
    check_order(first, first)  # equal instants are an empty range, not an error
    check_order(date(2025, 3, 3), date(2025, 3, 3))
    with pytest.raises(ValueError, match="must not be after end"):
        check_order(later, first)
    with pytest.raises(ValueError, match=re.escape(MIXED_BOUNDS)):
        check_order(date(2025, 3, 3), later)


def test_calendar_day_refuses_a_datetime() -> None:
    # A calendar date is a whole day: which day an instant names would depend on a zone.
    late = datetime(2025, 3, 4, 2, 0, tzinfo=UTC)
    for asset_class in (
        AssetClass.STOCKS,
        AssetClass.CRYPTO,
        AssetClass.FUTURES,
        ClassFromPrefix.ANY,
    ):
        with pytest.raises(
            ValueError, match=r"^x\.history: date=datetime\.datetime\(.* is a calendar date; pass"
        ):
            calendar_day(_spec(asset_class), late, "date")
    with pytest.raises(ValueError, match="is a calendar date"):
        calendar_day(_spec(AssetClass.STOCKS), "2025-03-03T09:30:00Z", "date")
    # A Java instant names no calendar day either.
    for java in (
        _present(to_j_instant("2025-03-03T14:30:00Z")),
        _present(to_j_zdt("2025-03-03T09:30:00 America/New_York")),
    ):
        with pytest.raises(ValueError, match=r"date=.* is a calendar date; pass a date$"):
            calendar_day(_spec(AssetClass.STOCKS), java, "date")
    # The message shows what the caller passed when the value is its conversion.
    with pytest.raises(ValueError, match=r"^x\.history: date='as passed' is a calendar date"):
        calendar_day(_spec(AssetClass.STOCKS), late, "date", given="as passed")


def test_calendar_day_keeps_a_date_as_given() -> None:
    assert calendar_day(_spec(AssetClass.CRYPTO), "2025-03-03", "date") == date(2025, 3, 3)
    assert calendar_day(_spec(AssetClass.STOCKS), date(2025, 3, 3), "date") == date(2025, 3, 3)
    with pytest.raises(ValueError, match=r"date='2025-03-03T09:30' needs a timezone"):
        calendar_day(_spec(AssetClass.STOCKS), "2025-03-03T09:30", "date")


_INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
    fetch_paths_by_class={AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}"},
)


def test_only_a_dataset_with_a_path_per_class_follows_its_symbols_class() -> None:
    crypto, stocks = AssetClass.CRYPTO, AssetClass.STOCKS
    # A dataset's own class wins.
    assert day_class(_spec(stocks), [crypto]) is stocks
    # A path per symbol class: the symbols' shared class, else New York days.
    assert day_class(_INDICATOR, [crypto, crypto]) is crypto
    assert day_class(_INDICATOR, [stocks]) is stocks
    assert day_class(_INDICATOR, [stocks, crypto]) is ClassFromPrefix.ANY
    assert day_class(_INDICATOR) is ClassFromPrefix.ANY
    # Any other dataset without a class keeps New York days, whatever its symbols.
    assert day_class(_spec(ClassFromPrefix.ANY), [crypto]) is ClassFromPrefix.ANY


def test_history_bounds_follow_the_symbols_class_only_with_a_path_per_class() -> None:
    crypto = bounds(_INDICATOR, "2025-03-03", None, now=NOW, classes=[AssetClass.CRYPTO])
    unclassed = bounds(
        _spec(ClassFromPrefix.ANY), "2025-03-03", None, now=NOW, classes=[AssetClass.CRYPTO]
    )
    late = datetime(2025, 6, 19, 1, tzinfo=UTC)  # 06-19 in UTC, still 06-18 in New York
    assert bounds(_INDICATOR, "2025-03-03", None, now=late, classes=[AssetClass.CRYPTO]) == (
        DaySpan(date(2025, 3, 3), date(2025, 6, 19))
    )
    assert bounds(
        _spec(ClassFromPrefix.ANY), "2025-03-03", None, now=late, classes=[AssetClass.CRYPTO]
    ) == (DaySpan(date(2025, 3, 3), date(2025, 6, 18)))
    assert crypto == unclassed  # at NOW both zones are on 06-18


@pytest.mark.parametrize(
    "text",
    [
        "2025-01-02T09:30:00.123456789+00:00",
        "2025-01-02T09:30:00,1234567Z",  # a comma, and only seven digits
        "2025-01-02T093000.123456789Z",  # the basic form
    ],
)
def test_iso_text_finer_than_a_microsecond_rounds_outward(
    text: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Like Deephaven's own instants: a start rounds down, an end up, so no row is lost.
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    moment = datetime(2025, 1, 2, 9, 30, 0, 123456, tzinfo=UTC)
    assert parse_date_like(text, "start") == moment
    assert parse_date_like(text, "end", upper=True) == moment + timedelta(microseconds=1)
    assert [line.split(" to ")[0] for line in caplog.messages] == [
        f"start={text} rounded down",
        f"end={text} rounded up",
    ]


def test_iso_text_with_only_zeros_below_the_microsecond_is_exact(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    text = "2025-01-02T09:30:00.123456000Z"
    exact = datetime(2025, 1, 2, 9, 30, 0, 123456, tzinfo=UTC)
    assert parse_date_like(text, "end", upper=True) == exact
    assert parse_date_like("2025-01-02T09:30:00.123456Z", "start") == exact
    assert caplog.messages == []


@pytest.mark.parametrize("text", ["20250303", "2025-W10-1"])
def test_every_iso_date_form_is_a_date(text: str) -> None:
    assert parse_date_like(text, "start") == date(2025, 3, 3)


@pytest.mark.parametrize("text", [" 2025-03-03", "2025-03-03 ", "\t2025-03-03T09:30:00Z"])
def test_date_text_with_surrounding_whitespace_is_refused(text: str) -> None:
    # Text is taken exactly as written; the message names the trimmed text to pass instead.
    expected = f"start={text!r} has surrounding whitespace; write {text.strip()!r}"
    with pytest.raises(ValueError, match=f"^{re.escape(expected)}$"):
        parse_date_like(text, "start")


def test_each_class_has_its_day_zone_and_day_start() -> None:
    assert day_zone(ClassFromPrefix.ANY) is NEW_YORK
    assert day_zone(AssetClass.CRYPTO) is UTC
    assert day_zone(AssetClass.FUTURES) is CHICAGO
    assert day_start(AssetClass.FOREX, date(2025, 3, 3)) == datetime(2025, 3, 3, tzinfo=UTC)
    assert day_start(ClassFromPrefix.ANY, date(2025, 3, 3)) == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert day_start(AssetClass.FUTURES, date(2025, 3, 4)) == futures_session_open(date(2025, 3, 4))


def test_day_starts_and_ends_come_in_utc_whatever_the_day_zone() -> None:
    # One zone for every instant these return, so no caller mixes a New York midnight with a
    # UTC one when it compares, prints, or offsets them.
    day = date(2025, 3, 3)
    classes = (AssetClass.STOCKS, AssetClass.INDICES, AssetClass.FUTURES, AssetClass.CRYPTO)
    for asset_class in classes:
        assert day_start(asset_class, day).tzinfo is UTC
        assert day_end(asset_class, day).tzinfo is UTC
    assert midnight(day, NEW_YORK).tzinfo is UTC


def test_an_instant_bound_keeps_a_whole_day_and_steps_back_one_unit() -> None:
    day = date(2025, 3, 3)
    assert instant_bound(AssetClass.CRYPTO, "t.gte", day, unit=WireUnit.MS, upper=False) == (
        datetime(2025, 3, 3, tzinfo=UTC)
    )
    assert instant_bound(AssetClass.CRYPTO, "t.lte", day, unit=WireUnit.MS, upper=True) == (
        datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC)
    )
    instant = datetime(2025, 3, 3, 12, tzinfo=UTC)
    assert instant_bound(ClassFromPrefix.ANY, "t.lt", instant, unit=None, upper=True) == instant
    with pytest.raises(ValueError, match=r"^t\.gt takes nanoseconds, so it cannot bound"):
        instant_bound(ClassFromPrefix.ANY, "t.gt", day, unit=WireUnit.NS, upper=False)
    with pytest.raises(ValueError, match=r"^t\.lte takes ISO-8601 instants of no documented"):
        instant_bound(ClassFromPrefix.ANY, "t.lte", day, unit=None, upper=True)


def test_a_trading_day_is_a_weekday_a_forex_sunday_or_any_crypto_day() -> None:
    saturday, sunday, monday = date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)
    for asset_class in (AssetClass.STOCKS, AssetClass.FUTURES, ClassFromPrefix.ANY):
        assert is_trading_day(asset_class, monday)
        assert not is_trading_day(asset_class, saturday)
        assert not is_trading_day(asset_class, sunday)
    # Forex trades from Sunday evening to Friday evening UTC.
    assert is_trading_day(AssetClass.FOREX, sunday)
    assert not is_trading_day(AssetClass.FOREX, saturday)
    assert is_trading_day(AssetClass.FOREX, monday)
    assert all(is_trading_day(AssetClass.CRYPTO, day) for day in (saturday, sunday, monday))


def test_a_day_ends_by_its_class() -> None:
    sunday = date(2025, 3, 9)  # New York clocks move forward overnight into Monday
    saturday = date(2025, 3, 8)
    assert day_end(AssetClass.STOCKS, saturday) == datetime(2025, 3, 9, 5, tzinfo=UTC)
    assert day_end(AssetClass.STOCKS, sunday) == datetime(2025, 3, 10, 4, tzinfo=UTC)
    assert day_end(ClassFromPrefix.ANY, sunday) == datetime(2025, 3, 10, 4, tzinfo=UTC)
    assert day_end(AssetClass.INDICES, sunday) == datetime(2025, 3, 10, 5, tzinfo=UTC)
    assert day_end(AssetClass.CRYPTO, sunday) == datetime(2025, 3, 10, tzinfo=UTC)
    assert day_end(AssetClass.FUTURES, sunday) == datetime(2025, 3, 9, 22, tzinfo=UTC)


def _present(value: T | None) -> T:
    # Deephaven's converters return None for None; these tests never pass one.
    assert value is not None
    return value


def test_deephaven_java_dates_and_instants_are_bounds() -> None:
    # The values Deephaven's own time functions return, as a Deephaven console holds them.
    day = _present(to_j_local_date("2025-03-03"))
    instant = _present(to_j_instant("2025-03-03T14:30:00.123456Z"))
    zoned = _present(to_j_zdt("2025-03-03T09:30:00.5 America/New_York"))
    assert parse_date_like(day, "start") == date(2025, 3, 3)
    assert parse_date_like(instant, "start") == datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    moment = parse_date_like(zoned, "end")
    assert moment == datetime(2025, 3, 3, 14, 30, 0, 500000, tzinfo=UTC)
    assert cast("datetime", moment).utcoffset() == timedelta(hours=-5)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("2025-03-03T14:30:00.123456789Z", id="instant"),
        pytest.param("2025-03-03T09:30:00.123456789 America/New_York", id="zoned"),
    ],
)
def test_java_instants_round_outward_to_the_microsecond(
    value: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Deephaven's instants carry nanoseconds the user never chose. A start rounds down, so it
    # still includes the row it came from; an end rounds up, so the range loses no row.
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    java = _present(to_j_zdt(value) if " " in value else to_j_instant(value))
    start = cast("datetime", parse_date_like(java, "start"))
    end = cast("datetime", parse_date_like(java, "end", upper=True))
    assert start.astimezone(UTC) == datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    assert end.astimezone(UTC) == datetime(2025, 3, 3, 14, 30, 0, 123457, tzinfo=UTC)
    assert caplog.messages == [
        f"start={java} rounded down to 2025-03-03T14:30:00.123456+00:00, by 789.0 ns",
        f"end={java} rounded up to 2025-03-03T14:30:00.123457+00:00, by 211.0 ns",
    ]


def test_an_instant_on_a_microsecond_is_not_moved_or_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    pd = importlib.import_module("pandas")  # untyped: pandas ships no stubs
    exact = datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    for value in (
        _present(to_j_instant("2025-03-03T14:30:00.123456Z")),
        pd.Timestamp("2025-03-03T14:30:00.123456", tz="UTC"),
    ):
        assert parse_date_like(value, "end", upper=True) == exact
    assert caplog.messages == []


def test_an_instant_before_1970_rounds_outward_too() -> None:
    # Before the epoch the count is negative; down is still earlier and up still later.
    java = _present(to_j_instant("1969-12-31T23:59:59.999999500Z"))
    last = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    assert parse_date_like(java, "start") == last
    assert parse_date_like(java, "end", upper=True) == datetime(1970, 1, 1, tzinfo=UTC)
    numpy_time = np.datetime64("1969-12-31T23:59:59.999999500", "ns")
    assert parse_date_like(numpy_time, "start") == last
    assert parse_date_like(numpy_time, "end", upper=True) == datetime(1970, 1, 1, tzinfo=UTC)


def test_an_end_past_the_last_microsecond_of_9999_is_the_largest_datetime(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Rounding up cannot pass the largest datetime, which no row reaches; a start still rounds
    # down to it.
    jpy = importlib.import_module("jpy")  # untyped: a Java instant past numpy's range
    java = jpy.get_type("java.time.Instant").parse("9999-12-31T23:59:59.999999500Z")
    largest = datetime.max.replace(tzinfo=UTC)
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    assert parse_date_like(java, "end", upper=True) == largest
    assert parse_date_like(java, "start") == largest
    # The log says what really happened: the end moved down, to the largest datetime.
    assert caplog.messages == [
        f"end={java} rounded down (clamped) to {largest.isoformat()}, by 500.0 ns",
        f"start={java} rounded down to {largest.isoformat()}, by 500.0 ns",
    ]


def test_java_times_a_datetime_cannot_hold_are_refused() -> None:
    jpy = importlib.import_module("jpy")  # untyped: Java's own extreme values
    for java in (jpy.get_type("java.time.Instant").MAX, jpy.get_type("java.time.LocalDate").MAX):
        with pytest.raises(ValueError, match=r"^start=.* is not a time from year 1 to 9999"):
            parse_date_like(java, "start")


def test_a_numpy_day_is_a_date() -> None:
    assert parse_date_like(np.datetime64("2025-03-03"), "start") == date(2025, 3, 3)


@pytest.mark.parametrize("value", [np.datetime64("2025-03-06", "W"), np.datetime64("2025-03")])
def test_a_numpy_week_month_or_year_is_refused(value: np.datetime64) -> None:
    # As an end it would shrink the range to the period's first day, and a numpy week starts
    # on a Thursday.
    with pytest.raises(ValueError, match=r"a numpy.datetime64 of a week, month, or year; pass a"):
        parse_date_like(value, "end", upper=True)


_MORNING = datetime(2025, 3, 3, 9, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "moment", "start", "end"),
    [
        pytest.param(np.datetime64("2025-03-03T09:30"), _MORNING, 0, 0, id="minutes"),
        pytest.param(
            np.datetime64("2025-03-03T09:30:00.123456", "us"), _MORNING, 123456, 123456, id="us"
        ),
        pytest.param(
            np.datetime64("2025-03-03T09:30:00.123456789", "ns"),
            _MORNING,
            123456,
            123457,
            id="ns",
        ),
        # A picosecond time spans only some 106 days either side of 1970.
        pytest.param(
            np.datetime64("1970-01-02T00:00:00.000000000001", "ps"),
            datetime(1970, 1, 2, tzinfo=UTC),
            0,
            1,
            id="ps",
        ),
    ],
)
def test_a_numpy_time_is_an_instant_in_utc_rounded_outward(
    *, value: np.datetime64, moment: datetime, start: int, end: int
) -> None:
    # A numpy time has no zone; Deephaven reads it as UTC, and so does the library.
    assert parse_date_like(value, "start") == moment + timedelta(microseconds=start)
    assert parse_date_like(value, "end", upper=True) == moment + timedelta(microseconds=end)


def test_a_numpy_time_logs_that_it_is_read_as_utc_and_how_it_is_rounded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    value = np.datetime64("2025-03-03T09:30:00.123456789", "ns")
    parse_date_like(value, "end", upper=True)
    assert caplog.messages == [
        f"end={value!r} has no timezone; read as UTC",
        f"end={value} rounded up to 2025-03-03T09:30:00.123457+00:00, by 211.0 ns",
    ]


def test_an_aware_pandas_timestamp_keeps_its_nanoseconds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A pandas.Timestamp holds nanoseconds a datetime would drop; they round outward.
    caplog.set_level(logging.DEBUG, logger=_DATES_LOG)
    pd = importlib.import_module("pandas")  # untyped: pandas ships no stubs
    stamp = pd.Timestamp("2025-03-03T04:30:00.123456789", tz="America/New_York")
    moment = datetime(2025, 3, 3, 9, 30, 0, 123456, tzinfo=UTC)
    assert parse_date_like(stamp, "start") == moment
    assert parse_date_like(stamp, "end", upper=True) == moment + timedelta(microseconds=1)
    assert caplog.messages == [
        f"start={stamp} rounded down to 2025-03-03T09:30:00.123456+00:00, by 789.0 ns",
        f"end={stamp} rounded up to 2025-03-03T09:30:00.123457+00:00, by 211.0 ns",
    ]


def test_a_pandas_timestamp_without_a_zone_or_value_is_refused() -> None:
    # Like a naive datetime, a naive pandas.Timestamp cannot be placed: read as UTC, a New York
    # user's 09:30 would become 04:30.
    pd = importlib.import_module("pandas")  # untyped: pandas ships no stubs
    with pytest.raises(ValueError, match=r"needs a timezone: pass an aware datetime or a date"):
        parse_date_like(pd.Timestamp("2025-03-03T09:30"), "start")
    with pytest.raises(ValueError, match=r"^start=NaT is NaT, not a time$"):
        parse_date_like(pd.NaT, "start")
    # The message shows the value as passed, not its numpy form.
    late = pd.Timestamp(np.datetime64("10000-01-01T00:00", "s")).tz_localize("UTC")
    with pytest.raises(ValueError, match=r"^start=10000-01-01 00:00:00\+00:00 is not a time"):
        parse_date_like(late, "start")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (np.datetime64("NaT", "D"), "is NaT, not a time"),
        (np.datetime64("NaT", "ns"), "is NaT, not a time"),
        (np.datetime64("10000-01-01"), "is not a time from year 1 to 9999"),
        (np.datetime64("10000-01-01T00:00"), "is not a time from year 1 to 9999"),
    ],
)
def test_a_numpy_value_that_is_no_time_a_datetime_holds_is_refused(
    value: np.datetime64, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        parse_date_like(value, "start")


def test_another_java_value_is_the_wrong_type() -> None:
    with pytest.raises(TypeError, match=r"^start must be a str, date, datetime, Java Instant"):
        parse_date_like(_present(to_j_local_time("09:30")), "start")
