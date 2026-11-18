"""Tests for the bar periods: which bars lie wholly inside a range, and the days a bar covers."""

from datetime import UTC, date, datetime, timedelta

import pytest

from deephaven_massive._datasets.bar_periods import (
    bar_days,
    bar_starts,
    futures_bar_ended,
    futures_bar_request,
    futures_bucket_end,
    futures_first_bucket_day,
    intraday,
    unit_ns,
)
from deephaven_massive._datasets.dates import (
    NEW_YORK,
    DaySpan,
    TimeRange,
    bar_zone,
)
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    FUTURES_HOLIDAY_MARGIN,
)
from deephaven_massive._enums import AssetClass, Timespan

S, W, M, Q, Y = Timespan.SESSION, Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR


def session_request(first: date, last: date) -> TimeRange:
    return futures_bar_request(DaySpan(first, last), timespan=S, multiplier=1)


def test_futures_bar_request_reaches_a_session_a_holiday_opened_early() -> None:
    # From the first session's 17:00 Chicago open (UTC-6 before March 9) less the four-day
    # holiday margin, to 18:00 Chicago on the last session date.
    assert timedelta(days=4) == FUTURES_HOLIDAY_MARGIN
    requested = session_request(date(2025, 3, 3), date(2025, 3, 5))
    assert requested.start == datetime(2025, 2, 26, 23, tzinfo=UTC)
    assert requested.end == datetime(2025, 3, 5, 18, tzinfo=CHICAGO)
    # Good Friday 2024: the session of Monday 2024-04-01 opened on Thursday at 17:00 Chicago.
    good_friday = session_request(date(2024, 4, 1), date(2024, 4, 1))
    assert good_friday.start < datetime(2024, 3, 28, 17, tzinfo=CHICAGO)
    # Martin Luther King Day 2025: the session of 2025-01-21 opened on Sunday 17:00.
    mlk = session_request(date(2025, 1, 21), date(2025, 1, 21))
    assert mlk.start <= datetime(2025, 1, 19, 17, tzinfo=CHICAGO) < mlk.end
    # Intraday bars take the same margin.
    minutes = futures_bar_request(
        DaySpan(date(2025, 3, 3), date(2025, 3, 5)), timespan=Timespan.MINUTE, multiplier=5
    )
    assert minutes == requested


def test_futures_bar_request_across_the_spring_dst_change() -> None:
    requested = session_request(date(2025, 3, 8), date(2025, 3, 10))
    assert requested.start == datetime(2025, 3, 3, 23, tzinfo=UTC)  # CST, UTC-6
    assert requested.end == datetime(2025, 3, 10, 23, tzinfo=UTC)  # 18:00 CDT, UTC-5


def test_longer_futures_bars_are_requested_from_the_first_bucket_that_can_start_the_range() -> None:
    def start(first: date, timespan: Timespan, multiplier: int = 1) -> datetime:
        span = DaySpan(first, date(2026, 10, 30))
        return futures_bar_request(span, timespan=timespan, multiplier=multiplier).start

    # Bars of several sessions: 00:00 UTC on the first date, or on the Saturday before a
    # Monday, since Monday's session opens on Sunday evening.
    assert start(date(2026, 9, 1), S, 2) == datetime(2026, 9, 1, tzinfo=UTC)
    assert start(date(2026, 8, 3), S, 2) == datetime(2026, 8, 1, tzinfo=UTC)
    # Weeks and longer: 00:00 UTC on the first day of the period that holds the first date.
    assert start(date(2026, 9, 1), M) == datetime(2026, 9, 1, tzinfo=UTC)
    assert start(date(2026, 9, 15), M, 2) == datetime(2026, 9, 1, tzinfo=UTC)
    assert start(date(2026, 8, 5), W) == datetime(2026, 8, 2, tzinfo=UTC)  # its Sunday
    assert start(date(2026, 8, 15), Q) == datetime(2026, 7, 1, tzinfo=UTC)
    assert start(date(2026, 8, 15), Y) == datetime(2026, 1, 1, tzinfo=UTC)
    # Every request ends at 18:00 Chicago on the last session date.
    span = DaySpan(date(2026, 9, 1), date(2026, 10, 1))
    assert futures_bar_request(span, timespan=M, multiplier=1).end == datetime(
        2026, 10, 1, 18, tzinfo=CHICAGO
    )


def test_the_first_bucket_day_holds_no_session_before_the_first_date() -> None:
    # A Tuesday to Friday is its own first day; a Saturday, Sunday, or Monday reaches back to
    # the Saturday, whose bucket's first session is Monday's.
    assert futures_first_bucket_day(date(2026, 9, 1)) == date(2026, 9, 1)  # Tuesday
    assert futures_first_bucket_day(date(2026, 8, 7)) == date(2026, 8, 7)  # Friday
    assert futures_first_bucket_day(date(2026, 8, 3)) == date(2026, 8, 1)  # Monday
    assert futures_first_bucket_day(date(2026, 8, 2)) == date(2026, 8, 1)  # Sunday
    assert futures_first_bucket_day(date(2026, 8, 1)) == date(2026, 8, 1)  # Saturday


def test_futures_buckets_follow_massive_s_grid() -> None:
    # The live ESZ6 bars of 2026: the next bucket starts where each one ends.
    assert futures_bucket_end(date(2026, 8, 1), timespan=S, multiplier=2) == date(2026, 8, 3)
    assert futures_bucket_end(date(2026, 8, 13), timespan=S, multiplier=2) == date(2026, 8, 15)
    assert futures_bucket_end(date(2026, 8, 2), timespan=S, multiplier=1) == date(2026, 8, 3)
    assert futures_bucket_end(date(2026, 8, 2), timespan=W, multiplier=2) == date(2026, 8, 16)
    assert futures_bucket_end(date(2026, 9, 1), timespan=M, multiplier=1) == date(2026, 10, 1)
    assert futures_bucket_end(date(2026, 7, 1), timespan=M, multiplier=2) == date(2026, 9, 1)
    assert futures_bucket_end(date(2026, 10, 1), timespan=Q, multiplier=1) == date(2027, 1, 1)
    assert futures_bucket_end(date(2025, 1, 1), timespan=Y, multiplier=1) == date(2026, 1, 1)


def close(day: date) -> datetime:
    # 17:00 Chicago on a session date: the session's close and the next one's open.
    return datetime(day.year, day.month, day.day, 17, tzinfo=CHICAGO)


@pytest.mark.parametrize(
    ("first_day", "label", "timespan", "multiplier", "ended"),
    [
        # The live ESZ6 bars whose label is their bucket's last possible session end at it.
        pytest.param(date(2026, 8, 2), date(2026, 8, 3), S, 1, date(2026, 8, 3), id="1-session"),
        pytest.param(date(2026, 8, 1), date(2026, 8, 3), S, 2, date(2026, 8, 3), id="sat-sun"),
        pytest.param(date(2026, 8, 3), date(2026, 8, 5), S, 2, date(2026, 8, 5), id="mon-tue"),
        # Thursday and Friday hold only Friday's session, which opens on Thursday.
        pytest.param(date(2026, 8, 13), date(2026, 8, 14), S, 2, date(2026, 8, 14), id="thu-fri"),
        # Labor Day 2026: the session of 09-08 opened on Sunday 09-06.
        pytest.param(date(2026, 9, 6), date(2026, 9, 8), S, 2, date(2026, 9, 8), id="labor-day"),
        pytest.param(date(2026, 9, 1), date(2026, 10, 1), M, 1, date(2026, 10, 1), id="sept"),
        pytest.param(date(2026, 10, 1), date(2026, 10, 30), M, 1, date(2026, 10, 30), id="oct"),
        pytest.param(date(2026, 7, 1), date(2026, 9, 1), M, 2, date(2026, 9, 1), id="2-month"),
        # A livestock session trades by day and ends on the Friday it opens.
        pytest.param(date(2026, 8, 7), date(2026, 8, 7), S, 2, date(2026, 8, 7), id="fri-sat"),
        # A label before the bucket's latest possible session may not be final: the bar ends
        # a weekday after that session. The 2-month bar of 2026-11-02 runs to 2027-01-01, a
        # Friday, so to 17:00 Chicago on Monday 2027-01-04.
        pytest.param(date(2026, 11, 1), date(2026, 11, 2), M, 2, date(2027, 1, 4), id="open"),
        pytest.param(date(2026, 8, 3), date(2026, 8, 4), S, 2, date(2026, 8, 6), id="one-seen"),
        # A year whose last session, 2025-12-31, opens on its last day: the session of
        # 2026-01-01 could still open on 12-31, so the bar ends after 2026-01-02's session.
        pytest.param(date(2025, 1, 1), date(2025, 12, 31), Y, 1, date(2026, 1, 2), id="year"),
    ],
)
def test_a_futures_bar_has_certainly_ended_once_no_session_can_join_it(
    *, first_day: date, label: date, timespan: Timespan, multiplier: int, ended: date
) -> None:
    assert futures_bar_ended(first_day, label, timespan=timespan, multiplier=multiplier) == close(
        ended
    )


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
    # The week of 2025-03-03 (Monday) to 03-07 (Friday), which Massive starts on Sunday 03-02.
    week = stock_bars(date(2025, 3, 3), date(2025, 3, 7), Timespan.WEEK)
    assert week == TimeRange(ny(date(2025, 3, 2)), ny(date(2025, 3, 9)))
    assert kept(week, ny(date(2025, 3, 2)))
    assert not kept(week, ny(date(2025, 3, 1)))
    assert not kept(week, ny(date(2025, 3, 9)))
    # A range from a Saturday to a Sunday holds the whole week between; the weekend days
    # around it hold no bar of their own.
    weekend = stock_bars(date(2025, 3, 1), date(2025, 3, 9), Timespan.WEEK)
    assert weekend == week
    # A range from Tuesday holds no whole week of the 3rd, nor a week ending on Thursday.
    assert stock_bars(date(2025, 3, 4), date(2025, 3, 7), Timespan.WEEK).empty
    assert stock_bars(date(2025, 3, 3), date(2025, 3, 6), Timespan.WEEK).empty
    # The kept starts, and the request, begin on the Sunday of the first whole week.
    assert stock_bars(date(2025, 3, 1), date(2025, 3, 21), Timespan.WEEK).start == ny(
        date(2025, 3, 2)
    )
    # Two-week bars: the last one to fit starts a week before the range's last week.
    two = stock_bars(date(2025, 3, 3), date(2025, 3, 21), Timespan.WEEK, multiplier=2)
    assert kept(two, ny(date(2025, 3, 9)))
    assert not kept(two, ny(date(2025, 3, 16)))


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


def test_crypto_weeks_run_sunday_to_saturday() -> None:
    # Massive starts a crypto week at 00:00 UTC on its Sunday (2026-10-11 in the live check).
    span = DaySpan(date(2025, 3, 3), date(2025, 3, 8)).instants(UTC)  # Monday to Saturday
    short = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.WEEK, multiplier=1)
    assert short.empty
    span = DaySpan(date(2025, 3, 2), date(2025, 3, 8)).instants(UTC)  # Sunday to Saturday
    week = bar_starts(AssetClass.CRYPTO, span, timespan=Timespan.WEEK, multiplier=1)
    assert week == TimeRange(utc(date(2025, 3, 2)), utc(date(2025, 3, 9)))


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


def test_bar_days_cover_each_period_s_window() -> None:
    sunday, monday = date(2025, 3, 2), date(2025, 3, 3)
    # A stock week stamped on its Sunday covers its Monday to Friday; a crypto or index week
    # its whole Sunday-to-Saturday calendar week.
    stock_week = (monday, date(2025, 3, 8))
    assert bar_days(AssetClass.STOCKS, sunday, timespan=Timespan.WEEK, multiplier=1) == stock_week
    assert bar_days(AssetClass.CRYPTO, sunday, timespan=Timespan.WEEK, multiplier=2) == (
        sunday,
        date(2025, 3, 16),
    )
    assert bar_days(AssetClass.INDICES, sunday, timespan=Timespan.WEEK, multiplier=1) == (
        sunday,
        date(2025, 3, 9),
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


def test_an_index_month_keeps_its_weekend_days() -> None:
    # An index can update at weekends: June 2024, which starts on a Saturday, is whole.
    june = date(2024, 6, 1)
    assert bar_days(AssetClass.INDICES, june, timespan=Timespan.MONTH, multiplier=1) == (
        june,
        date(2024, 7, 1),
    )


def test_an_index_week_bar_needs_its_whole_calendar_week() -> None:
    # Monday to Friday holds a stock week but not an index week, which runs Sunday to Saturday
    # in Chicago, where Massive starts an index bar of a day or longer.
    weekdays = TimeRange(
        datetime(2025, 3, 3, tzinfo=NEW_YORK), datetime(2025, 3, 8, tzinfo=NEW_YORK)
    )
    whole = TimeRange(datetime(2025, 3, 2, tzinfo=CHICAGO), datetime(2025, 3, 9, tzinfo=CHICAGO))
    week = Timespan.WEEK
    assert not bar_starts(AssetClass.STOCKS, weekdays, timespan=week, multiplier=1).empty
    assert bar_starts(AssetClass.INDICES, weekdays, timespan=week, multiplier=1).empty
    assert not bar_starts(AssetClass.INDICES, whole, timespan=week, multiplier=1).empty


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime(2025, 6, 2, 5, tzinfo=UTC), datetime(2025, 6, 3, 5, tzinfo=UTC)),
        (datetime(2025, 1, 6, 6, tzinfo=UTC), datetime(2025, 1, 7, 6, tzinfo=UTC)),
    ],
    ids=["daylight", "standard"],
)
def test_an_index_day_bar_fits_the_chicago_day_it_covers(start: datetime, end: datetime) -> None:
    # Massive stamps an index day bar at midnight in Chicago (01:00 New York), so a range of
    # exactly that Chicago day holds the bar; a stock's day is New York's, so the same range,
    # which starts an hour into the New York day, holds no stock day bar.
    day = TimeRange(start, end)
    index = bar_starts(AssetClass.INDICES, day, timespan=Timespan.DAY, multiplier=1)
    assert index == TimeRange(start, end)
    assert bar_starts(AssetClass.STOCKS, day, timespan=Timespan.DAY, multiplier=1).empty
    new_york = TimeRange(start - timedelta(hours=1), end - timedelta(hours=1))
    stock = bar_starts(AssetClass.STOCKS, new_york, timespan=Timespan.DAY, multiplier=1)
    assert stock == new_york


@pytest.mark.parametrize("asset_class", [AssetClass.STOCKS, AssetClass.OPTIONS, AssetClass.INDICES])
def test_bars_of_several_days_span_calendar_days_weekends_included(asset_class: AssetClass) -> None:
    # Massive stamps 2-day stock bars on Friday 2026-10-16, Sunday the 18th, and Tuesday the
    # 20th, each holding its two calendar days: Friday's bar holds Friday's trading alone.
    stamps = [date(2026, 10, 16), date(2026, 10, 18), date(2026, 10, 20)]
    for day in stamps:
        covered = bar_days(asset_class, day, timespan=Timespan.DAY, multiplier=2)
        assert covered == (day, day + timedelta(days=2))
    # Friday to Wednesday holds all three whole; Friday to Tuesday only the first two. The
    # days are those of the class's bar zone: Chicago for an index.
    zone = bar_zone(asset_class)
    midnights = [datetime(day.year, day.month, day.day, tzinfo=zone) for day in stamps]
    span = DaySpan(date(2026, 10, 16), date(2026, 10, 21)).instants(zone)
    starts = bar_starts(asset_class, span, timespan=Timespan.DAY, multiplier=2)
    assert [kept(starts, stamp) for stamp in midnights] == [True, True, True]
    short = DaySpan(date(2026, 10, 16), date(2026, 10, 20)).instants(zone)
    starts = bar_starts(asset_class, short, timespan=Timespan.DAY, multiplier=2)
    assert [kept(starts, stamp) for stamp in midnights] == [True, True, False]


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
    # Weeks are calendar weeks, Sunday to Saturday, as Massive stamps them.
    assert forex_bars(date(2025, 3, 3), date(2025, 3, 9), Timespan.WEEK).empty
    week = forex_bars(date(2025, 3, 2), date(2025, 3, 8), Timespan.WEEK)
    assert week == TimeRange(utc(date(2025, 3, 2)), utc(date(2025, 3, 9)))

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
