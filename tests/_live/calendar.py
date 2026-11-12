"""The US exchanges' calendar, and the recent days the live tests read.

No live test reads a fixed date: a fixed date ages out of a Basic plan's two years of history
and can land on a holiday. Each computes its days from the run's New York day with the
exchanges' fixed holiday rules, here. The library keeps no holiday calendar on purpose, so this
one lives with the tests, with the exchanges' weekend rule: they trade Monday to Friday. It
imports neither the library nor Deephaven, so the live run's parent, which starts no JVM,
can use it. A closure no rule predicts (a national day of mourning) is not known: a test that
reads one finds no rows there and says so.

Every function is pure but `today`, which reads the clock.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

NEW_YORK: Final = ZoneInfo("America/New_York")
"""The zone the US exchanges keep their calendar in."""

SATURDAY: Final = 5
"""`date.weekday()` of a Saturday: the exchanges trade on the five days before it."""

StampDays = dict[str, tuple[date, date]]
"""The day-stamp checks' dates, by unit: `"day"` the full and the half trading day; `"week"`,
`"month"`, `"quarter"`, and `"year"` the first and last day to read of that unit's bars."""


def today() -> date:
    """Return the run's day in New York, where the US exchanges keep their calendar.

    Returns:
        The day.
    """
    return datetime.now(NEW_YORK).date()


def easter(year: int) -> date:
    """Return Easter Sunday of a year, by the anonymous Gregorian algorithm.

    Args:
        year: The year.

    Returns:
        The date; Good Friday, an exchange holiday, is two days before.
    """
    a, b, c = year % 19, year // 100, year % 100
    d, e = divmod(b, 4)
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 19 * weekday) // 433
    month = (h + weekday - 7 * m + 90) // 25
    return date(year, month, (h + weekday - 7 * m + 33 * month + 19) % 32)


def months_before(day: date, months: int) -> date:
    """Return the first day of the month `months` before the month of `day`.

    Args:
        day: Any day of the month counted from.
        months: How many months back, 0 for `day`'s own month, negative for later months.

    Returns:
        The first of that month.
    """
    index = day.year * 12 + day.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def nth_weekday(year: int, month: int, weekday: int, *, nth: int) -> date:
    """Return the nth given weekday of a month, or the last one for `nth=-1`.

    Args:
        year: The year.
        month: The month, 1 to 12.
        weekday: Monday is 0.
        nth: 1 for the first, 2 for the second, and so on; -1 for the last.

    Returns:
        The date.
    """
    if nth > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (nth - 1))
    last = months_before(date(year, month, 1), -1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def thanksgiving(year: int) -> date:
    """Return the US Thanksgiving Day of a year: the fourth Thursday of November.

    Args:
        year: The year.

    Returns:
        The date. US exchanges close that day and close early, at 13:00 New York time, on the
        Friday after.
    """
    return nth_weekday(year, 11, 3, nth=4)


def observed(day: date) -> date:
    """Move a fixed-date holiday that falls on a weekend to the weekday the exchanges close.

    Args:
        day: The holiday's date.

    Returns:
        The Friday before a Saturday, the Monday after a Sunday, or the day itself.
    """
    return day + timedelta(days={5: -1, 6: 1}.get(day.weekday(), 0))


def market_holidays(year: int) -> frozenset[date]:
    """Return the days the US stock exchanges close in a year, by their fixed rules.

    New Year's Day (not moved back into December when it falls on a Saturday), Martin Luther
    King Jr. Day, Washington's Birthday, Good Friday, Memorial Day, Juneteenth (from 2022),
    Independence Day, Labor Day, Thanksgiving, and Christmas, each fixed-date one observed on
    the nearest weekday.

    Args:
        year: The year.

    Returns:
        The closed days.
    """
    new_year = date(year, 1, 1)
    days = {
        new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year,
        nth_weekday(year, 1, 0, nth=3),
        nth_weekday(year, 2, 0, nth=3),
        easter(year) - timedelta(days=2),
        nth_weekday(year, 5, 0, nth=-1),
        observed(date(year, 7, 4)),
        nth_weekday(year, 9, 0, nth=1),
        thanksgiving(year),
        observed(date(year, 12, 25)),
    }
    if year >= 2022:
        days.add(observed(date(year, 6, 19)))
    return frozenset(days)


def is_trading_day(day: date) -> bool:
    """Return whether the US stock exchanges open on a day.

    The exchanges trade Monday to Friday, except on the holidays of `market_holidays`. So it
    describes the stock, option, and index calendar. Crypto trades every day and forex on US
    holidays: the days they share with stocks (the recent Monday) are at most a week older for
    it. A futures contract is chosen by its own roll rules (`contracts`).

    Args:
        day: The day.

    Returns:
        True for a stock trading day.
    """
    return day.weekday() < SATURDAY and day not in market_holidays(day.year)


def trading_day_before(day: date) -> date:
    """Return the last trading day before a day.

    Args:
        day: The day.

    Returns:
        The trading day.
    """
    found = day - timedelta(days=1)
    while not is_trading_day(found):
        found -= timedelta(days=1)
    return found


def trading_day_after(day: date) -> date:
    """Return the first trading day after a day.

    Args:
        day: The day.

    Returns:
        The trading day.
    """
    found = day + timedelta(days=1)
    while not is_trading_day(found):
        found += timedelta(days=1)
    return found


def index_check_days(day: date) -> tuple[date, ...]:
    """Return the trading days the many-index check reads: recent ones, a Monday, a holiday's next.

    They are the three trading days before `day` (the latest is the day `/prev` serves, whose
    flat files exist only from 11:30 New York time the next day), the most recent Monday whose
    files are published (`recent_monday`), and the first trading day after the most recent
    weekday holiday, at least two days before `day` so its files are published. A day that is
    two of these at once is read once.

    Args:
        day: The run's New York day.

    Returns:
        Four or five distinct trading days, oldest first.
    """
    latest = trading_day_before(day)
    recent = [latest, trading_day_before(latest)]
    recent.append(trading_day_before(recent[-1]))
    published = day - timedelta(days=2)
    holidays = sorted(
        (h for year in (day.year - 1, day.year) for h in market_holidays(year)), reverse=True
    )
    after = next(
        after
        for holiday in holidays
        if holiday.weekday() < 5 and (after := trading_day_after(holiday)) <= published
    )
    return tuple(sorted({*recent, recent_monday(day), after}))


def recent_monday(day: date) -> date:
    """Return the most recent Monday that was a trading day and whose flat files are published.

    It is at least two days before `day`, so its files, published by 11:30 New York time the
    next day, exist whatever the hour.

    Args:
        day: The run's New York day.

    Returns:
        The Monday.
    """
    monday = day - timedelta(days=2)
    monday -= timedelta(days=monday.weekday())
    while not is_trading_day(monday):
        monday -= timedelta(days=7)
    return monday


def recent_week(day: date) -> tuple[date, date]:
    """Return the most recent ended calendar week whose five weekdays were all trading days.

    Args:
        day: The run's New York day.

    Returns:
        Its Monday and its Sunday, which is before `day`.
    """
    sunday = day - timedelta(days=(day.weekday() + 1) % 7 or 7)
    while not all(is_trading_day(sunday - timedelta(days=n)) for n in range(2, 7)):
        sunday -= timedelta(days=7)
    return sunday - timedelta(days=6), sunday


def recent_month(day: date) -> tuple[date, date]:
    """Return the most recent ended calendar month.

    Args:
        day: The run's New York day.

    Returns:
        Its first and last day.
    """
    last = day.replace(day=1) - timedelta(days=1)
    return last.replace(day=1), last


def half_days(year: int) -> list[date]:
    """Return the days of a year US exchanges close early, at 13:00 New York time.

    By the exchanges' rules: the Friday after Thanksgiving, and July 3 and December 24 when
    they fall Monday to Thursday (on a Friday the next day's holiday moves onto them).

    Args:
        year: The year.

    Returns:
        The half trading days, in date order.
    """
    found = [date(year, 7, 3), thanksgiving(year) + timedelta(days=1), date(year, 12, 24)]
    return [day for day in found if day.weekday() < 4 or day.month == 11]


def is_half_day(day: date) -> bool:
    """Return whether US exchanges close early, at 13:00 New York time, on a day.

    Args:
        day: The day.

    Returns:
        True for a half trading day (see `half_days`).
    """
    return day in half_days(day.year)


def stamp_days(day: date) -> StampDays:
    """Return the day-stamp checks' dates: a recent half day's week and recent longer periods.

    The half day is the most recent one (the Friday after Thanksgiving, July 3, or December
    24) whose calendar week ended before `day`, so every bar that week holds, an index week
    updated around the clock included, is complete and its flat files are published. A
    December 24 half day stays the latest until the next Thanksgiving week ends, so it can be
    about eleven months old, still inside every plan's two years of history. The longer periods
    are anchored to `day` instead, so they are as recent as whole periods can be.

    Args:
        day: The run's day, in New York time.

    Returns:
        By unit: `"day"` the trading day before the half day (a full trading day) and the half
        day; `"week"` the Monday of the week before the half day's and the Sunday ending the
        half day's week; `"month"` the two whole months before the current month; `"quarter"`
        the two whole quarters before the current quarter; and `"year"` January 1 of this year
        to `day`, which holds this year's unfinished bar (none yet before the year's first
        trading day).
    """
    half = max(
        candidate
        for year in (day.year - 1, day.year)
        for candidate in half_days(year)
        if candidate + timedelta(days=6 - candidate.weekday()) < day
    )
    monday = half - timedelta(days=half.weekday())
    this_month = day.replace(day=1)
    this_quarter = date(day.year, (day.month - 1) // 3 * 3 + 1, 1)
    return {
        "day": (trading_day_before(half), half),
        "week": (monday - timedelta(days=7), monday + timedelta(days=6)),
        "month": (months_before(day, 2), this_month - timedelta(days=1)),
        "quarter": (months_before(this_quarter, 6), this_quarter - timedelta(days=1)),
        "year": (date(day.year, 1, 1), day),
    }


HOLIDAY_RULES: Final[Mapping[str, Callable[[int], date]]] = {
    "martin luther king day": lambda year: nth_weekday(year, 1, 0, nth=3),
    "good friday": lambda year: easter(year) - timedelta(days=2),
}
"""The exchange holidays a live test reads around, by name, as a function of the year."""


def recent_holiday(name: str, day: date) -> date:
    """Return the most recent occurrence of an exchange holiday, a week or more before a day.

    The week lets the sessions around the holiday end and their data be published.

    Args:
        name: `"martin luther king day"` or `"good friday"`.
        day: The run's New York day.

    Returns:
        The date.
    """
    rule = HOLIDAY_RULES[name]
    this_year = rule(day.year)
    return this_year if this_year <= day - timedelta(days=7) else rule(day.year - 1)
