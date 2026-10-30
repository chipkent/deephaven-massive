"""Tests for the bar periods: which bars lie wholly inside a range, and the days a bar covers."""

from datetime import UTC, date, datetime, timedelta

import pytest

from deephaven_massive._datasets.bar_periods import (
    bar_days,
    bar_sessions,
    bar_starts,
    futures_bar_request,
    futures_period_last,
    intraday,
    last_trading_day,
    unit_ns,
)
from deephaven_massive._datasets.dates import (
    NEW_YORK,
    DaySpan,
    TimeRange,
)
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    FUTURES_HOLIDAY_MARGIN,
    futures_session_open,
)
from deephaven_massive._enums import AssetClass, Timespan


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


# Weekday closures of the New York Stock Exchange, 2024 to 2026, including the national day of
# mourning on 2025-01-09.
NYSE_CLOSED = frozenset(
    {
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 12, 25),
        date(2025, 1, 1), date(2025, 1, 9), date(2025, 1, 20), date(2025, 2, 17),
        date(2025, 4, 18), date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4),
        date(2025, 9, 1), date(2025, 11, 27), date(2025, 12, 25),
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    }
)  # fmt: skip

# Weekdays on which the CME equity and interest-rate futures hold no session of their own: the
# trading folds into the next session's date.
CME_FOLDED = NYSE_CLOSED - {date(2025, 1, 9)}


@pytest.mark.parametrize("closed", [NYSE_CLOSED, CME_FOLDED], ids=["nyse", "cme"])
def test_the_last_trading_day_bounds_every_window_of_the_calendar(closed: frozenset[date]) -> None:
    # Every start date from 2024 to 2026 and every count up to a quarter of sessions: the day
    # the n-th session falls on is never after the bound.
    day, trading = date(2024, 1, 1), []
    while day.year < 2027:
        if day.weekday() < 5 and day not in closed:
            trading.append(day)
        day += timedelta(days=1)
    for first in range(len(trading)):
        for n in range(1, 64):
            if first + n > len(trading):
                break
            assert trading[first + n - 1] <= last_trading_day(trading[first], n), (
                trading[first],
                n,
            )


def test_the_last_trading_day_allows_a_weekday_more_and_one_per_five_sessions() -> None:
    thursday = date(2025, 3, 6)
    assert last_trading_day(thursday, 1) == date(2025, 3, 7)
    assert last_trading_day(thursday, 2) == date(2025, 3, 10)
    assert last_trading_day(thursday, 6) == date(2025, 3, 17)
    # From 2024-12-24 the New York exchange is shut on 12-25, 01-01, and 01-09: the sixth and
    # eleventh sessions fall on the bound itself.
    assert last_trading_day(date(2024, 12, 24), 6) == date(2025, 1, 2)
    assert last_trading_day(date(2024, 12, 24), 11) == date(2025, 1, 10)


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
        date(2025, 3, 7),
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
