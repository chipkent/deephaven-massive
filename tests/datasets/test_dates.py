"""Tests for date parsing, the kinds of bounds, the bar windows, and the day rules."""

import re
from datetime import UTC, date, datetime, time, timedelta
from typing import cast

import pytest

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import (
    CHICAGO,
    DAY_ZONES,
    FUTURES_HOLIDAY_MARGIN,
    MIXED_BOUNDS,
    NEW_YORK,
    DaySpan,
    TimeRange,
    add_weekdays,
    bar_days,
    bar_sessions,
    bar_starts,
    bounds,
    calendar_day,
    check_order,
    day_class,
    day_start,
    day_zone,
    flat_file_published,
    futures_bar_request,
    futures_period_last,
    futures_session,
    futures_session_before,
    futures_session_end,
    futures_session_open,
    instant_bound,
    intraday,
    parse_date_like,
    publish_time,
    unit_ns,
)
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._massive import WireUnit

NOW = datetime(2025, 6, 18, 15, tzinfo=UTC)
"""A Wednesday afternoon, after every range of these tests unless a test says otherwise."""


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
    with pytest.raises(TypeError, match="end must be a str, date, or datetime"):
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


def test_futures_bar_request_reaches_a_session_a_holiday_opened_early() -> None:
    # From the first session's 17:00 Chicago open (UTC-6 before March 9) less the four-day
    # holiday margin, to 18:00 Chicago on the last session date.
    assert timedelta(days=4) == FUTURES_HOLIDAY_MARGIN
    requested = futures_bar_request(DaySpan(date(2025, 3, 3), date(2025, 3, 5)))
    assert requested.start == datetime(2025, 2, 26, 23, tzinfo=UTC)
    assert requested.end == datetime(2025, 3, 5, 18, tzinfo=CHICAGO)
    # Good Friday 2024: the session of Monday 2024-04-01 opened on Thursday at 17:00 Chicago.
    good_friday = futures_bar_request(DaySpan(date(2024, 4, 1), date(2024, 4, 1)))
    assert good_friday.start < datetime(2024, 3, 28, 17, tzinfo=CHICAGO)
    # Martin Luther King Day 2025: the session of 2025-01-21 opened on Sunday 17:00.
    mlk = futures_bar_request(DaySpan(date(2025, 1, 21), date(2025, 1, 21)))
    assert mlk.start <= datetime(2025, 1, 19, 17, tzinfo=CHICAGO) < mlk.end


def test_futures_bar_request_across_the_spring_dst_change() -> None:
    requested = futures_bar_request(DaySpan(date(2025, 3, 8), date(2025, 3, 10)))
    assert requested.start == datetime(2025, 3, 3, 23, tzinfo=UTC)  # CST, UTC-6
    assert requested.end == datetime(2025, 3, 10, 23, tzinfo=UTC)  # 18:00 CDT, UTC-5


CUTOFF = time(11, 30)


def test_publish_time_in_new_york() -> None:
    assert publish_time(date(2025, 3, 3), zone=NEW_YORK, cutoff=CUTOFF) == datetime(
        2025, 3, 4, 11, 30, tzinfo=NEW_YORK
    )


def test_publish_time_in_utc() -> None:
    # March 4 00:00 UTC is March 3 19:00 in New York; the first 11:30 after it is March 4.
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=CUTOFF) == datetime(
        2025, 3, 4, 11, 30, tzinfo=NEW_YORK
    )
    # A cutoff later than 19:00 New York time falls on the same New York evening.
    late = time(20, 0)
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=late) == datetime(
        2025, 3, 3, 20, 0, tzinfo=NEW_YORK
    )
    early = time(18, 0)
    assert publish_time(date(2025, 3, 3), zone=UTC, cutoff=early) == datetime(
        2025, 3, 4, 18, 0, tzinfo=NEW_YORK
    )


def test_publish_time_is_strictly_after_the_day_starts() -> None:
    # A cutoff at exactly the New York midnight that starts the next day is not "after" it.
    assert publish_time(date(2025, 3, 3), zone=NEW_YORK, cutoff=time(0, 0)) == datetime(
        2025, 3, 5, 0, 0, tzinfo=NEW_YORK
    )


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
    # 01:00 UTC on March 4 is 20:00 New York time on March 3. For a UTC class, March 3's file
    # lands at 11:30 New York time on March 4, so it is not published yet.
    now = datetime(2025, 3, 4, 1, tzinfo=UTC)
    march_3 = date(2025, 3, 3)
    assert not flat_file_published(march_3, now=now, zone=NEW_YORK, cutoff=CUTOFF)
    assert not flat_file_published(march_3, now=now, zone=UTC, cutoff=CUTOFF)
    assert flat_file_published(date(2025, 3, 2), now=now, zone=UTC, cutoff=CUTOFF)
    later = datetime(2025, 3, 4, 11, 30, tzinfo=NEW_YORK)
    assert flat_file_published(march_3, now=later, zone=UTC, cutoff=CUTOFF)


def _spec(asset_class: AssetClass | None) -> DatasetSpec:
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


def test_futures_session_before_excludes_the_end() -> None:
    opens = datetime(2025, 1, 21, 17, tzinfo=CHICAGO)
    assert futures_session_before(opens) == date(2025, 1, 21)
    assert futures_session_before(opens + timedelta(microseconds=1)) == date(2025, 1, 22)


def test_publish_time_across_the_spring_dst_change() -> None:
    published = publish_time(date(2025, 3, 8), zone=UTC, cutoff=time(11, 30))
    assert published == datetime(2025, 3, 9, 11, 30, tzinfo=NEW_YORK)
    assert published.astimezone(UTC) == datetime(2025, 3, 9, 15, 30, tzinfo=UTC)


def test_publication_compares_instants_across_the_fall_back_hour() -> None:
    cutoff = time(1, 30)
    published = publish_time(date(2025, 11, 1), zone=NEW_YORK, cutoff=cutoff)
    assert published.astimezone(UTC) == datetime(2025, 11, 2, 5, 30, tzinfo=UTC)  # 01:30 EDT
    # 01:15 EST (fold=1) is 06:15Z, 45 minutes after publication, though its wall clock reads
    # earlier than 01:30.
    later = datetime(2025, 11, 2, 1, 15, tzinfo=NEW_YORK, fold=1)
    assert flat_file_published(date(2025, 11, 1), now=later, zone=NEW_YORK, cutoff=cutoff)
    earlier = datetime(2025, 11, 2, 1, 15, tzinfo=NEW_YORK, fold=0)
    assert not flat_file_published(date(2025, 11, 1), now=earlier, zone=NEW_YORK, cutoff=cutoff)


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


@pytest.mark.parametrize(
    ("moment", "session"),
    [
        (datetime(2025, 3, 3, 10, tzinfo=CHICAGO), date(2025, 3, 3)),
        (datetime(2025, 3, 3, 16, 59, 59, 999999, tzinfo=CHICAGO), date(2025, 3, 3)),
        (datetime(2025, 3, 3, 17, tzinfo=CHICAGO), date(2025, 3, 4)),
        (datetime(2025, 3, 3, 23, 30, tzinfo=CHICAGO), date(2025, 3, 4)),
        (datetime(2025, 3, 2, 17, tzinfo=CHICAGO), date(2025, 3, 3)),  # the Sunday open
        (datetime(2025, 3, 9, 17, tzinfo=CHICAGO), date(2025, 3, 10)),  # after spring forward
        (datetime(2025, 3, 3, 23, tzinfo=UTC), date(2025, 3, 4)),  # 17:00 CST
        (datetime(2025, 7, 1, 22, tzinfo=UTC), date(2025, 7, 2)),  # 17:00 CDT
        (datetime(2025, 7, 1, 21, 59, tzinfo=UTC), date(2025, 7, 1)),  # 16:59 CDT
    ],
)
def test_futures_session_rolls_at_17_chicago(moment: datetime, session: date) -> None:
    assert futures_session(moment) == session


def test_calendar_day_refuses_a_datetime() -> None:
    # A calendar date is a whole day: which day an instant names would depend on a zone.
    late = datetime(2025, 3, 4, 2, 0, tzinfo=UTC)
    for asset_class in (AssetClass.STOCKS, AssetClass.CRYPTO, AssetClass.FUTURES, None):
        with pytest.raises(ValueError, match=r"^x\.history: date is a calendar date; pass a date$"):
            calendar_day(_spec(asset_class), late, "date")
    with pytest.raises(ValueError, match="is a calendar date"):
        calendar_day(_spec(AssetClass.STOCKS), "2025-03-03T09:30:00Z", "date")


def test_calendar_day_keeps_a_date_as_given() -> None:
    assert calendar_day(_spec(AssetClass.CRYPTO), "2025-03-03", "date") == date(2025, 3, 3)
    assert calendar_day(_spec(AssetClass.STOCKS), date(2025, 3, 3), "date") == date(2025, 3, 3)
    with pytest.raises(ValueError, match=r"date='2025-03-03T09:30' needs a timezone"):
        calendar_day(_spec(AssetClass.STOCKS), "2025-03-03T09:30", "date")


@pytest.mark.parametrize("session", [date(2025, 3, 4), date(2025, 3, 10), date(2025, 11, 3)])
def test_a_session_opens_at_17_chicago_on_the_day_before(session: date) -> None:
    opens = futures_session_open(session)
    eve = session - timedelta(days=1)
    assert opens == datetime(eve.year, eve.month, eve.day, 17, tzinfo=CHICAGO)
    # The roll agrees: the opening instant belongs to the session, the one before does not.
    assert futures_session(opens) == session
    assert futures_session(opens - timedelta(microseconds=1)) == eve


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
    assert day_class(_INDICATOR, [stocks, crypto]) is None
    assert day_class(_INDICATOR) is None
    # Any other dataset without a class keeps New York days, whatever its symbols.
    assert day_class(_spec(None), [crypto]) is None


def test_history_bounds_follow_the_symbols_class_only_with_a_path_per_class() -> None:
    crypto = bounds(_INDICATOR, "2025-03-03", None, now=NOW, classes=[AssetClass.CRYPTO])
    unclassed = bounds(_spec(None), "2025-03-03", None, now=NOW, classes=[AssetClass.CRYPTO])
    late = datetime(2025, 6, 19, 1, tzinfo=UTC)  # 06-19 in UTC, still 06-18 in New York
    assert bounds(_INDICATOR, "2025-03-03", None, now=late, classes=[AssetClass.CRYPTO]) == (
        DaySpan(date(2025, 3, 3), date(2025, 6, 19))
    )
    assert bounds(_spec(None), "2025-03-03", None, now=late, classes=[AssetClass.CRYPTO]) == (
        DaySpan(date(2025, 3, 3), date(2025, 6, 18))
    )
    assert crypto == unclassed  # at NOW both zones are on 06-18


def test_text_finer_than_a_microsecond_is_refused_not_cut() -> None:
    with pytest.raises(ValueError, match=r"more than six fractional digits of a second"):
        parse_date_like("2025-01-02T09:30:00.123456789+00:00", "timestamp_gte")
    with pytest.raises(ValueError, match=r"more than six fractional digits of a second"):
        parse_date_like("2025-01-02T09:30:00,1234567Z", "start")
    with pytest.raises(ValueError, match=r"more than six fractional digits of a second"):
        parse_date_like("2025-06-02T120000.123456789Z", "timestamp_gte")  # the basic form
    assert parse_date_like("2025-01-02T09:30:00.123456Z", "start") == datetime(
        2025, 1, 2, 9, 30, 0, 123456, tzinfo=UTC
    )


def test_each_class_has_its_day_zone_and_day_start() -> None:
    assert day_zone(None) is NEW_YORK
    assert day_zone(AssetClass.CRYPTO) is UTC
    assert day_zone(AssetClass.FUTURES) is CHICAGO
    assert day_start(AssetClass.FOREX, date(2025, 3, 3)) == datetime(2025, 3, 3, tzinfo=UTC)
    assert day_start(None, date(2025, 3, 3)) == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert day_start(AssetClass.FUTURES, date(2025, 3, 4)) == futures_session_open(date(2025, 3, 4))


def test_an_instant_bound_keeps_a_whole_day_and_steps_back_one_unit() -> None:
    day = date(2025, 3, 3)
    assert instant_bound(AssetClass.CRYPTO, "t.gte", day, unit=WireUnit.MS, upper=False) == (
        datetime(2025, 3, 3, tzinfo=UTC)
    )
    assert instant_bound(AssetClass.CRYPTO, "t.lte", day, unit=WireUnit.MS, upper=True) == (
        datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC)
    )
    instant = datetime(2025, 3, 3, 12, tzinfo=UTC)
    assert instant_bound(None, "t.lt", instant, unit=None, upper=True) == instant
    with pytest.raises(ValueError, match=r"^t\.gt takes nanoseconds, so it cannot bound"):
        instant_bound(None, "t.gt", day, unit=WireUnit.NS, upper=False)
    with pytest.raises(ValueError, match=r"^t\.lte takes ISO-8601 instants of no documented"):
        instant_bound(None, "t.lte", day, unit=None, upper=True)


# --- Whole bar windows ---------------------------------------------------------------------------


def ny(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=NEW_YORK).astimezone(UTC)


def utc(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def stock_bars(first: date, last: date, timespan: Timespan, *, multiplier: int = 1) -> TimeRange:
    span = DaySpan(first, last).instants(NEW_YORK)
    return bar_starts(AssetClass.STOCKS, span, timespan=timespan, multiplier=multiplier)


def kept(starts: TimeRange, stamp: datetime) -> bool:
    return starts.start <= stamp.astimezone(UTC) < starts.end


def test_intraday_bars_must_end_inside_the_range() -> None:
    assert intraday(Timespan.MINUTE)
    assert not intraday(Timespan.DAY)
    start = datetime(2025, 3, 3, 14, 30, 20, tzinfo=UTC)
    end = datetime(2025, 3, 3, 14, 45, 30, tzinfo=UTC)
    starts = bar_starts(
        AssetClass.STOCKS, TimeRange(start, end), timespan=Timespan.MINUTE, multiplier=1
    )
    # The 14:30 bar starts before the range and the 14:45 bar ends after it.
    assert starts == TimeRange(
        datetime(2025, 3, 3, 14, 31, tzinfo=UTC), datetime(2025, 3, 3, 14, 45, tzinfo=UTC)
    )
    assert starts.start == datetime(2025, 3, 3, 14, 31, tzinfo=UTC)
    # Five-minute bars: the last one to fit starts at 14:40.
    five = bar_starts(
        AssetClass.STOCKS, TimeRange(start, end), timespan=Timespan.MINUTE, multiplier=5
    )
    assert five.end == datetime(2025, 3, 3, 14, 41, tzinfo=UTC)
    # A range shorter than one bar keeps none.
    short = TimeRange(start, start + timedelta(seconds=30))
    none = bar_starts(AssetClass.STOCKS, short, timespan=Timespan.MINUTE, multiplier=1)
    assert none.empty
    seconds = bar_starts(AssetClass.STOCKS, short, timespan=Timespan.SECOND, multiplier=1)
    assert seconds == short


def test_hour_bars_across_the_spring_dst_change() -> None:
    # 2025-03-09 is 23 hours long in New York: its bars of one hour are 23.
    starts = stock_bars(date(2025, 3, 9), date(2025, 3, 9), Timespan.HOUR)
    assert starts.end - starts.start == timedelta(hours=23)


@pytest.mark.parametrize(
    ("day", "hours"), [(date(2025, 3, 9), 23), (date(2025, 11, 2), 25), (date(2025, 3, 3), 24)]
)
def test_day_bars_are_new_york_days_on_dst_days(day: date, hours: int) -> None:
    starts = stock_bars(day, day, Timespan.DAY)
    assert starts == TimeRange(ny(day), ny(day + timedelta(days=1)))
    assert starts.end - starts.start == timedelta(hours=hours)
    assert starts.start == ny(day)


def test_day_bars_of_a_range_of_instants_must_be_whole_days() -> None:
    # 09:30 Monday to 09:30 Wednesday holds only Tuesday whole.
    span = TimeRange(
        datetime(2025, 3, 3, 9, 30, tzinfo=NEW_YORK), datetime(2025, 3, 5, 9, 30, tzinfo=NEW_YORK)
    )
    starts = bar_starts(AssetClass.STOCKS, span, timespan=Timespan.DAY, multiplier=1)
    assert starts == TimeRange(ny(date(2025, 3, 4)), ny(date(2025, 3, 5)))
    assert starts.start == ny(date(2025, 3, 4))
    # Crypto days are UTC days.
    crypto = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.DAY, multiplier=1)
    assert crypto == TimeRange(utc(date(2025, 3, 4)), utc(date(2025, 3, 5)))


def test_week_bars_run_monday_to_friday_and_skip_weekends() -> None:
    # The week of 2025-03-03 (Monday) to 03-07 (Friday).
    week = stock_bars(date(2025, 3, 3), date(2025, 3, 7), Timespan.WEEK)
    assert kept(week, ny(date(2025, 3, 3)))
    # A bar Massive stamps on the Sunday before its week names that week.
    assert kept(week, ny(date(2025, 3, 2)))
    assert not kept(week, ny(date(2025, 3, 10)))
    # A range from a Saturday to a Sunday holds the whole week between; the weekend days
    # around it hold no bar of their own.
    weekend = stock_bars(date(2025, 3, 1), date(2025, 3, 9), Timespan.WEEK)
    assert weekend == week
    # A range from Tuesday holds no whole week of the 3rd, nor a week ending on Thursday.
    assert stock_bars(date(2025, 3, 4), date(2025, 3, 7), Timespan.WEEK).empty
    assert stock_bars(date(2025, 3, 3), date(2025, 3, 6), Timespan.WEEK).empty
    # The kept starts, and the request, begin on the Saturday before the first whole week.
    assert stock_bars(date(2025, 3, 1), date(2025, 3, 21), Timespan.WEEK).start == ny(
        date(2025, 3, 1)
    )
    # Two-week bars: the last one to fit starts a week before the range's last week.
    two = stock_bars(date(2025, 3, 3), date(2025, 3, 21), Timespan.WEEK, multiplier=2)
    assert kept(two, ny(date(2025, 3, 10)))
    assert not kept(two, ny(date(2025, 3, 17)))


def test_mlk_week_is_still_monday_to_friday() -> None:
    # Martin Luther King Day (Monday 2025-01-20) closed the market; with no holiday calendar
    # the week's bar still covers Monday to Friday, so a range from Tuesday excludes it.
    assert kept(
        stock_bars(date(2025, 1, 20), date(2025, 1, 24), Timespan.WEEK), ny(date(2025, 1, 21))
    )
    assert stock_bars(date(2025, 1, 21), date(2025, 1, 24), Timespan.WEEK).empty


def test_good_friday_2024_months_and_weeks() -> None:
    # Good Friday (2024-03-29) closed the stock market; March still ends on the 31st.
    march = date(2024, 3, 1)
    assert stock_bars(march, date(2024, 3, 28), Timespan.MONTH).empty
    assert kept(stock_bars(march, date(2024, 3, 29), Timespan.MONTH), ny(march))
    assert kept(stock_bars(march, date(2024, 3, 31), Timespan.MONTH), ny(march))
    # Its week is still Monday 03-25 to Friday 03-29.
    week = stock_bars(date(2024, 3, 25), date(2024, 3, 29), Timespan.WEEK)
    assert kept(week, ny(date(2024, 3, 25)))
    assert stock_bars(date(2024, 3, 25), date(2024, 3, 28), Timespan.WEEK).empty


def test_crypto_weeks_run_monday_to_sunday() -> None:
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 8)).instants(UTC)  # Monday to Saturday
    short = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.WEEK, multiplier=1)
    assert short.empty
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 9)).instants(UTC)
    week = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.WEEK, multiplier=1)
    assert week == TimeRange(utc(date(2025, 3, 3)), utc(date(2025, 3, 10)))
    assert week.start == utc(date(2025, 3, 3))


@pytest.mark.parametrize(
    ("timespan", "first", "last", "stamps", "outside"),
    [
        (
            Timespan.MONTH,
            date(2025, 1, 15),
            date(2025, 4, 30),
            [date(2025, 2, 1), date(2025, 4, 1)],
            [date(2025, 1, 1), date(2025, 5, 1)],
        ),
        (
            Timespan.QUARTER,
            date(2024, 12, 31),
            date(2025, 9, 30),
            [date(2025, 1, 1), date(2025, 7, 1)],
            [date(2024, 10, 1), date(2025, 10, 1)],
        ),
        (
            Timespan.YEAR,
            date(2023, 1, 1),
            date(2024, 12, 30),
            [date(2023, 1, 1)],
            [date(2024, 1, 1), date(2022, 1, 1)],
        ),
    ],
)
def test_calendar_periods_must_lie_wholly_inside(
    *, timespan: Timespan, first: date, last: date, stamps: list[date], outside: list[date]
) -> None:
    starts = stock_bars(first, last, timespan)
    assert all(kept(starts, ny(day)) for day in stamps)
    assert not any(kept(starts, ny(day)) for day in outside)


def test_a_month_bar_needs_the_month_whole_from_its_first_day() -> None:
    starts = stock_bars(date(2025, 3, 5), date(2025, 3, 20), Timespan.MONTH)
    assert starts.empty
    two = stock_bars(date(2025, 1, 1), date(2025, 4, 30), Timespan.MONTH, multiplier=2)
    assert kept(two, ny(date(2025, 3, 1)))
    assert not kept(two, ny(date(2025, 4, 1)))
    assert stock_bars(date(2025, 1, 2), date(2025, 6, 30), Timespan.MONTH).start == ny(
        date(2025, 2, 1)
    )
    assert starts.start == ny(date(2025, 4, 1))


def test_an_unfinished_period_ends_after_now() -> None:
    # A range that ends now leaves out the current month, week, and day.
    now = datetime(2025, 6, 18, 15, tzinfo=UTC)
    span = TimeRange(ny(date(2025, 5, 1)), now)
    month = bar_starts(AssetClass.STOCKS, span, timespan=Timespan.MONTH, multiplier=1)
    assert kept(month, ny(date(2025, 5, 1)))
    assert not kept(month, ny(date(2025, 6, 1)))
    day = bar_starts(AssetClass.STOCKS, span, timespan=Timespan.DAY, multiplier=1)
    assert kept(day, ny(date(2025, 6, 17)))
    assert not kept(day, ny(date(2025, 6, 18)))


def test_futures_bars_by_instants_use_sessions() -> None:
    # The session of 2025-03-04 runs from 17:00 Chicago on 03-03 to 17:00 on 03-04.
    span = TimeRange(
        datetime(2025, 3, 3, 17, tzinfo=CHICAGO).astimezone(UTC),
        datetime(2025, 3, 4, 17, tzinfo=CHICAGO).astimezone(UTC),
    )
    starts = bar_starts(AssetClass.FUTURES, span, timespan=Timespan.SESSION, multiplier=1)
    assert starts == span
    assert starts.start == span.start
    late = TimeRange(span.start + timedelta(minutes=1), span.end)
    assert bar_starts(AssetClass.FUTURES, late, timespan=Timespan.SESSION, multiplier=1).empty
    # A week of sessions opens on Sunday evening and ends at Friday's 17:00.
    week = TimeRange(
        datetime(2025, 3, 2, 17, tzinfo=CHICAGO).astimezone(UTC),
        datetime(2025, 3, 7, 17, tzinfo=CHICAGO).astimezone(UTC),
    )
    weeks = bar_starts(AssetClass.FUTURES, week, timespan=Timespan.WEEK, multiplier=1)
    assert kept(weeks, datetime(2025, 3, 2, 17, tzinfo=CHICAGO))
    shorter = TimeRange(week.start, week.end - timedelta(hours=1))
    assert bar_starts(AssetClass.FUTURES, shorter, timespan=Timespan.WEEK, multiplier=1).empty


def test_futures_bar_sessions_keep_whole_periods_of_session_dates() -> None:
    week = DaySpan(date(2025, 3, 3), date(2025, 3, 7))
    assert bar_sessions(week, timespan=Timespan.SESSION, multiplier=1) == week
    # A week bar is labelled with a session date of its week; Saturday and Sunday belong to
    # the next week.
    assert bar_sessions(week, timespan=Timespan.WEEK, multiplier=1) == DaySpan(
        date(2025, 3, 1), date(2025, 3, 7)
    )
    assert bar_sessions(
        DaySpan(date(2025, 3, 4), date(2025, 3, 7)), timespan=Timespan.WEEK, multiplier=1
    ).empty
    # Good Friday 2024 had no session; the week of 03-25 is still Monday to Friday.
    good_friday = DaySpan(date(2024, 3, 25), date(2024, 3, 28))
    assert bar_sessions(good_friday, timespan=Timespan.WEEK, multiplier=1).empty
    # A two-month bar labelled with a February session covers January and February if the
    # label is its last session, February and March if it is its first: both fit only for
    # February labels.
    months = bar_sessions(
        DaySpan(date(2025, 1, 1), date(2025, 3, 31)), timespan=Timespan.MONTH, multiplier=2
    )
    assert months == DaySpan(date(2025, 2, 1), date(2025, 2, 28))


def test_futures_bars_of_several_sessions_fit_under_both_readings_of_their_label() -> None:
    week = DaySpan(date(2025, 3, 3), date(2025, 3, 7))
    # Two sessions: a Monday label could cover the Friday before; a Friday label the Monday
    # after. Sessions count as weekdays, so Tuesday to Thursday fit either way.
    assert bar_sessions(week, timespan=Timespan.SESSION, multiplier=2) == DaySpan(
        date(2025, 3, 4), date(2025, 3, 6)
    )
    # Over a weekend: Friday and the Monday after are consecutive sessions.
    across = DaySpan(date(2025, 3, 6), date(2025, 3, 10))
    assert bar_sessions(across, timespan=Timespan.SESSION, multiplier=2) == DaySpan(
        date(2025, 3, 7), date(2025, 3, 7)
    )
    # A range that starts and ends on a weekend holds the weekdays between.
    weekend = DaySpan(date(2025, 3, 1), date(2025, 3, 9))
    assert bar_sessions(weekend, timespan=Timespan.SESSION, multiplier=1) == week
    assert bar_sessions(week, timespan=Timespan.SESSION, multiplier=5) == DaySpan(
        date(2025, 3, 7), date(2025, 3, 3)
    )
    assert bar_sessions(week, timespan=Timespan.SESSION, multiplier=5).empty
    two_weeks = DaySpan(date(2025, 3, 3), date(2025, 3, 21))
    assert bar_sessions(two_weeks, timespan=Timespan.WEEK, multiplier=2) == DaySpan(
        date(2025, 3, 8), date(2025, 3, 14)
    )


def test_futures_session_bar_starts_count_weekdays() -> None:
    # A three-session bar that ends on Monday's session starts in Thursday's.
    span = TimeRange(
        futures_session_open(date(2025, 3, 3)).astimezone(UTC),
        datetime(2025, 3, 10, 17, tzinfo=CHICAGO).astimezone(UTC),
    )
    starts = bar_starts(AssetClass.FUTURES, span, timespan=Timespan.SESSION, multiplier=3)
    assert starts.start == futures_session_open(date(2025, 3, 3)).astimezone(UTC)
    assert starts.end == futures_session_open(date(2025, 3, 7)).astimezone(UTC)
    # Ending on a Saturday keeps the bars that end with Friday's session.
    saturday = TimeRange(span.start, datetime(2025, 3, 8, 12, tzinfo=CHICAGO).astimezone(UTC))
    starts = bar_starts(AssetClass.FUTURES, saturday, timespan=Timespan.SESSION, multiplier=2)
    assert starts.end == futures_session_open(date(2025, 3, 7)).astimezone(UTC)


def test_add_weekdays_skips_weekends_both_ways() -> None:
    friday, saturday = date(2025, 3, 7), date(2025, 3, 8)
    assert add_weekdays(friday, 0) == friday
    assert add_weekdays(friday, 1) == date(2025, 3, 10)
    assert add_weekdays(date(2025, 3, 10), -1) == friday
    assert add_weekdays(saturday, 1) == date(2025, 3, 10)
    assert add_weekdays(saturday, -1) == friday
    assert add_weekdays(date(2024, 3, 28), 3) == date(2024, 4, 2)  # no holiday calendar


def test_bar_days_cover_each_period_s_window() -> None:
    sunday, monday = date(2025, 3, 2), date(2025, 3, 3)
    stock_week = (monday, date(2025, 3, 8))
    assert bar_days(AssetClass.STOCKS, sunday, timespan=Timespan.WEEK, multiplier=1) == stock_week
    assert bar_days(AssetClass.STOCKS, monday, timespan=Timespan.WEEK, multiplier=1) == stock_week
    assert bar_days(AssetClass.CRYPTO, monday, timespan=Timespan.WEEK, multiplier=2) == (
        monday,
        date(2025, 3, 17),
    )
    assert bar_days(AssetClass.STOCKS, monday, timespan=Timespan.DAY, multiplier=3) == (
        monday,
        date(2025, 3, 6),
    )
    # June 2024 starts on a Saturday: its stock window starts Monday the 3rd and ends Friday
    # the 28th; forex keeps the whole month.
    june = date(2024, 6, 1)
    assert bar_days(AssetClass.STOCKS, june, timespan=Timespan.MONTH, multiplier=1) == (
        date(2024, 6, 3),
        date(2024, 6, 29),
    )
    assert bar_days(AssetClass.FOREX, june, timespan=Timespan.MONTH, multiplier=1) == (
        june,
        date(2024, 7, 1),
    )
    assert bar_days(AssetClass.STOCKS, date(2025, 1, 1), timespan=Timespan.YEAR, multiplier=1) == (
        date(2025, 1, 1),
        date(2026, 1, 1),
    )


def test_futures_period_last_takes_the_later_reading() -> None:
    label = date(2024, 3, 25)
    assert futures_period_last(label, timespan=Timespan.WEEK, multiplier=1) == date(2024, 3, 29)
    assert futures_period_last(label, timespan=Timespan.WEEK, multiplier=2) == date(2024, 4, 5)
    assert futures_period_last(label, timespan=Timespan.MONTH, multiplier=1) == date(2024, 3, 29)
    assert futures_period_last(label, timespan=Timespan.QUARTER, multiplier=1) == date(2024, 3, 29)


def test_a_futures_session_is_over_at_17_chicago() -> None:
    assert futures_session_end(date(2025, 3, 3)) == datetime(2025, 3, 3, 17, tzinfo=CHICAGO)
    assert futures_session(futures_session_end(date(2025, 3, 3))) == date(2025, 3, 4)


def test_unit_ns_is_one_intraday_unit() -> None:
    assert unit_ns(Timespan.SECOND) == 10**9
    assert unit_ns(Timespan.MINUTE) == 60 * 10**9
    assert unit_ns(Timespan.HOUR) == 3_600 * 10**9


def test_months_leave_out_their_leading_and_trailing_weekend_days() -> None:
    # June 2025 starts on a Sunday: a range from Monday the 2nd holds it whole, and its bar
    # is still requested from the 1st, where Massive stamps it.
    starts = stock_bars(date(2025, 6, 2), date(2025, 6, 30), Timespan.MONTH)
    assert kept(starts, ny(date(2025, 6, 1)))
    assert starts.start == ny(date(2025, 6, 1))
    # Crypto trades every day, so its June starts on the 1st.
    span = DaySpan(date(2025, 6, 2), date(2025, 6, 30)).instants(UTC)
    crypto = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.MONTH, multiplier=1)
    assert crypto.empty
    # Futures months are months of session dates, which are weekdays.
    march = bar_sessions(
        DaySpan(date(2024, 3, 1), date(2024, 3, 29)), timespan=Timespan.MONTH, multiplier=1
    )
    assert march == DaySpan(date(2024, 3, 1), date(2024, 3, 31))


def forex_bars(first: date, last: date, timespan: Timespan) -> TimeRange:
    span = DaySpan(first, last).instants(UTC)
    return bar_starts(AssetClass.FOREX, span, timespan=timespan, multiplier=1)


def test_forex_periods_keep_their_weekends() -> None:
    # Forex trades from Sunday evening UTC: March 2024 ends on Sunday the 31st, so a range
    # ending Friday the 29th leaves March out, and June 2025, which starts on Sunday the 1st,
    # needs a range from the 1st.
    assert forex_bars(date(2024, 3, 1), date(2024, 3, 29), Timespan.MONTH).empty
    assert kept(
        forex_bars(date(2024, 3, 1), date(2024, 3, 31), Timespan.MONTH), utc(date(2024, 3, 1))
    )
    assert forex_bars(date(2025, 6, 2), date(2025, 6, 30), Timespan.MONTH).empty
    assert kept(
        forex_bars(date(2025, 6, 1), date(2025, 6, 30), Timespan.MONTH), utc(date(2025, 6, 1))
    )
    # Weeks are calendar weeks, Monday to Sunday.
    assert forex_bars(date(2025, 3, 3), date(2025, 3, 7), Timespan.WEEK).empty
    week = forex_bars(date(2025, 3, 3), date(2025, 3, 9), Timespan.WEEK)
    assert week == TimeRange(utc(date(2025, 3, 3)), utc(date(2025, 3, 10)))
    quarters = forex_bars(date(2024, 1, 1), date(2024, 3, 29), Timespan.QUARTER)
    assert quarters.empty


def test_an_open_forex_month_is_unfinished_until_sunday_ends() -> None:
    # history("2024-03-01") on Sunday 2024-03-31 at 12:00 UTC: Sunday evening's trading has
    # not happened yet, so March is still unfinished.
    now = datetime(2024, 3, 31, 12, tzinfo=UTC)
    span = TimeRange(utc(date(2024, 3, 1)), now)
    assert bar_starts(AssetClass.FOREX, span, timespan=Timespan.MONTH, multiplier=1).empty
    # Stocks do not trade on Sunday: their March ended with Friday the 29th.
    stocks = TimeRange(ny(date(2024, 3, 1)), now)
    month = bar_starts(AssetClass.STOCKS, stocks, timespan=Timespan.MONTH, multiplier=1)
    assert kept(month, ny(date(2024, 3, 1)))
