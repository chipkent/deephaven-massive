"""Tests for date parsing and the day rules."""

import re
from datetime import UTC, date, datetime, time, timedelta
from typing import cast

import pytest

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.dates import (
    CHICAGO,
    DAY_ZONES,
    FUTURES_HOLIDAY_MARGIN,
    NEW_YORK,
    FuturesSessions,
    TimeRange,
    bounds,
    calendar_day,
    check_order,
    day_class,
    day_start,
    day_zone,
    flat_file_published,
    futures_session,
    futures_session_before,
    futures_session_open,
    futures_sessions,
    history_range,
    instant_bound,
    parse_date_like,
    publish_time,
)
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import WireUnit


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
    window = history_range("2025-03-03", "2025-03-03", NEW_YORK)
    assert window.start == datetime(2025, 3, 3, 5, tzinfo=UTC)
    assert window.end == datetime(2025, 3, 4, 5, tzinfo=UTC)
    assert window.end_ns - window.start_ns == 86_400 * 10**9


def test_daylight_saving_days() -> None:
    # 2025-03-09 is 23 hours long in New York.
    window = history_range(date(2025, 3, 9), date(2025, 3, 9), NEW_YORK)
    assert window.end - window.start == timedelta(hours=23)


def test_utc_days_and_datetimes() -> None:
    window = history_range("2025-03-03", datetime(2025, 3, 3, 12, tzinfo=UTC), UTC)
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


def test_futures_session_window() -> None:
    sessions = futures_sessions(date(2025, 3, 3), date(2025, 3, 5))
    assert (sessions.first, sessions.last) == (date(2025, 3, 3), date(2025, 3, 5))
    # From the first session's 17:00 Chicago open (UTC-6 before March 9) less the holiday
    # margin, to the regular open of the session after the last, which is exact.
    assert timedelta(days=4) == FUTURES_HOLIDAY_MARGIN
    assert sessions.window.start == datetime(2025, 2, 26, 23, tzinfo=UTC)
    assert sessions.window.end == datetime(2025, 3, 5, 23, tzinfo=UTC)
    single = futures_sessions(date(2025, 3, 3), date(2025, 3, 3))
    assert single.window.end - single.window.start == timedelta(days=5)


def test_the_margin_covers_a_friday_holiday_folded_into_monday() -> None:
    # Good Friday 2024: the session of Monday 2024-04-01 opened on Thursday at 17:00 Chicago.
    sessions = futures_sessions(date(2024, 4, 1), date(2024, 4, 1))
    assert sessions.window.start < datetime(2024, 3, 28, 17, tzinfo=CHICAGO)


def test_a_session_after_a_holiday_is_inside_its_window() -> None:
    # CME traded Martin Luther King Day (Monday 2025-01-20) from Sunday 17:00 Chicago time
    # under the trade date 2025-01-21, a day before that session's regular open.
    sessions = futures_sessions(date(2025, 1, 21), date(2025, 1, 21))
    sunday_open = datetime(2025, 1, 19, 17, tzinfo=CHICAGO)
    assert sessions.window.start <= sunday_open < sessions.window.end
    assert sessions.window.end == datetime(2025, 1, 21, 17, tzinfo=CHICAGO)
    with pytest.raises(ValueError, match="must not be after end"):
        futures_sessions(date(2025, 3, 5), date(2025, 3, 3))


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


def test_bounds_follow_the_class_day_zone() -> None:
    stocks = bounds(_spec(AssetClass.STOCKS), "2025-03-03", "2025-03-03")
    assert stocks == history_range("2025-03-03", "2025-03-03", NEW_YORK)
    crypto = bounds(_spec(AssetClass.CRYPTO), "2025-03-03", "2025-03-03")
    assert isinstance(crypto, TimeRange)
    assert crypto.start == datetime(2025, 3, 3, tzinfo=UTC)
    unclassed = bounds(_spec(None), "2025-03-03", "2025-03-03")
    assert unclassed == stocks


def test_bounds_of_futures_dates_are_sessions() -> None:
    sessions = bounds(_spec(AssetClass.FUTURES), "2025-03-03", date(2025, 3, 5))
    assert isinstance(sessions, FuturesSessions)
    assert (sessions.first, sessions.last) == (date(2025, 3, 3), date(2025, 3, 5))
    assert sessions == futures_sessions(date(2025, 3, 3), date(2025, 3, 5))


def test_bounds_of_futures_datetimes_filter_timestamps() -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    later = datetime(2025, 3, 4, 15, tzinfo=UTC)
    assert bounds(_spec(AssetClass.FUTURES), moment, later) == TimeRange(moment, later)
    assert CHICAGO.key == "America/Chicago"


def test_mixed_futures_bounds_keep_the_session_rule_on_the_date_side() -> None:
    # A date start names its whole session, the evening before its date included.
    end = datetime(2024, 12, 17, 16, tzinfo=CHICAGO)
    window = bounds(_spec(AssetClass.FUTURES), "2024-12-17", end)
    assert window == FuturesSessions(
        date(2024, 12, 17),
        None,
        TimeRange(
            (datetime(2024, 12, 16, 17, tzinfo=CHICAGO) - FUTURES_HOLIDAY_MARGIN).astimezone(UTC),
            end.astimezone(UTC),
        ),
    )
    # A date end names its session up to the next session's regular open.
    start = datetime(2024, 12, 16, 18, tzinfo=CHICAGO)
    window = bounds(_spec(AssetClass.FUTURES), start, "2024-12-17")
    assert window == FuturesSessions(
        None,
        date(2024, 12, 17),
        TimeRange(start.astimezone(UTC), datetime(2024, 12, 17, 23, tzinfo=UTC)),
    )
    with pytest.raises(ValueError, match="must be before end"):
        bounds(_spec(AssetClass.FUTURES), datetime(2024, 12, 17, 18, tzinfo=CHICAGO), "2024-12-17")


def test_a_date_start_may_end_before_its_regular_open_only_after_a_holiday() -> None:
    spec = _spec(AssetClass.FUTURES)
    monday = datetime(2025, 1, 20, 10, tzinfo=CHICAGO)
    # Martin Luther King Day folded into the 2025-01-21 session, which opened on Sunday.
    folded = bounds(spec, "2025-01-21", monday)
    assert isinstance(folded, FuturesSessions)
    assert folded.first == date(2025, 1, 21)
    # Two weekday sessions (01-20 and 01-21) cannot both fold into 01-22.
    with pytest.raises(ValueError, match="the session of 2025-01-22 cannot open that early"):
        bounds(spec, "2025-01-22", monday)
    # Good Friday folded into Monday's session, which opened on Thursday 17:00.
    friday = bounds(spec, "2024-04-01", datetime(2024, 3, 29, 9, tzinfo=CHICAGO))
    assert isinstance(friday, FuturesSessions)
    assert friday.first == date(2024, 4, 1)
    with pytest.raises(ValueError, match="cannot open that early"):
        bounds(spec, "2024-04-01", datetime(2024, 3, 28, 10, tzinfo=CHICAGO))


def test_an_instant_start_at_the_next_sessions_open_is_empty() -> None:
    # The session of 2025-01-21 ends where the next one opens: 17:00 Chicago on 01-21.
    opens = datetime(2025, 1, 21, 17, tzinfo=CHICAGO)
    with pytest.raises(ValueError, match="must be before end"):
        futures_sessions(opens, date(2025, 1, 21))
    assert futures_sessions(opens - timedelta(microseconds=1), date(2025, 1, 21)).last


def test_futures_session_before_excludes_the_end() -> None:
    opens = datetime(2025, 1, 21, 17, tzinfo=CHICAGO)
    assert futures_session_before(opens) == date(2025, 1, 21)
    assert futures_session_before(opens + timedelta(microseconds=1)) == date(2025, 1, 22)


def test_bounds_reject_bad_ranges() -> None:
    with pytest.raises(ValueError, match="must not be after end"):
        bounds(_spec(AssetClass.FUTURES), "2025-03-05", "2025-03-03")
    with pytest.raises(ValueError, match=r"start='2025-03-03T09:30' needs a timezone"):
        bounds(_spec(AssetClass.STOCKS), "2025-03-03T09:30", "2025-03-04")


def test_futures_window_across_the_spring_dst_change() -> None:
    sessions = futures_sessions(date(2025, 3, 8), date(2025, 3, 10))
    assert sessions.window.start == datetime(2025, 3, 3, 23, tzinfo=UTC)  # CST, UTC-6
    assert sessions.window.end == datetime(2025, 3, 10, 22, tzinfo=UTC)  # CDT, UTC-5


def test_publish_time_across_the_spring_dst_change() -> None:
    published = publish_time(date(2025, 3, 8), zone=UTC, cutoff=time(11, 30))
    assert published == datetime(2025, 3, 9, 11, 30, tzinfo=NEW_YORK)
    assert published.astimezone(UTC) == datetime(2025, 3, 9, 15, 30, tzinfo=UTC)


def test_the_fall_dst_day_is_25_hours() -> None:
    window = history_range("2025-11-02", "2025-11-02", NEW_YORK)
    assert window.end - window.start == timedelta(hours=25)


def test_partition_dates_across_the_spring_dst_change() -> None:
    window = history_range("2025-03-08", "2025-03-10", NEW_YORK)
    assert window.partition_dates(NEW_YORK) == (
        date(2025, 3, 8),
        date(2025, 3, 9),
        date(2025, 3, 10),
    )


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
    with pytest.raises(ValueError, match="must be before end"):
        check_order(later, first)


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


def test_calendar_day_is_the_day_in_the_class_zone() -> None:
    late = datetime(2025, 3, 4, 2, 0, tzinfo=UTC)  # 21:00 on 3 March in New York and Chicago
    assert calendar_day(_spec(AssetClass.STOCKS), late, "date") == date(2025, 3, 3)
    assert calendar_day(_spec(None), late, "date") == date(2025, 3, 3)
    assert calendar_day(_spec(AssetClass.CRYPTO), late, "date") == date(2025, 3, 4)
    assert calendar_day(_spec(AssetClass.FUTURES), late, "date") == date(2025, 3, 3)
    early = datetime(2025, 3, 4, 5, 30, tzinfo=UTC)  # 23:30 in Chicago, 00:30 in New York
    assert calendar_day(_spec(AssetClass.FUTURES), early, "date") == date(2025, 3, 3)
    assert calendar_day(_spec(AssetClass.STOCKS), early, "date") == date(2025, 3, 4)


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
    crypto = bounds(_INDICATOR, "2025-03-03", "2025-03-03", classes=[AssetClass.CRYPTO])
    assert crypto == TimeRange(datetime(2025, 3, 3, tzinfo=UTC), datetime(2025, 3, 4, tzinfo=UTC))
    unclassed = bounds(_spec(None), "2025-03-03", "2025-03-03", classes=[AssetClass.CRYPTO])
    assert unclassed == TimeRange(
        datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 4, 5, tzinfo=UTC)
    )


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
