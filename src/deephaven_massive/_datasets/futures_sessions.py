"""The futures session rule: which trading session an instant belongs to, scalar and Arrow.

A futures trading day is a *session*, named by its `session_end_date`. The futures trading day
rolls at 17:00 Chicago time, so an instant belongs to the session of its Chicago date until
17:00 and to the next day's session from then on (`futures_session`). No session ends on a
Saturday or Sunday: what trades from Friday 17:00 belongs to Monday's session
(`weekday_on_or_after`), so without a holiday calendar, counting sessions is counting weekdays
(`add_weekdays`). An exchange holiday folds into the next session, which then opens earlier:
after Good Friday 2024 the session of Monday 2024-04-01 opened on Thursday 2024-03-28 at 17:00
(`FUTURES_HOLIDAY_MARGIN`).

One constant, `FUTURES_SESSION_ROLL`, is the whole rule. `session_days` and `weekday_sessions`
apply it to an Arrow column, for the rows `row_times` dates, and a test checks that the two
forms agree. The day rules of `dates` (a futures date names a session) and the bar periods of
`bar_periods` build on this module, which imports nothing else from the library.

Every function is pure and thread-safe.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, time, timedelta
from typing import Final
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

__all__ = [
    "CHICAGO",
    "FUTURES_HOLIDAY_MARGIN",
    "FUTURES_SESSION_END",
    "FUTURES_SESSION_ROLL",
    "add_weekdays",
    "futures_session",
    "futures_session_before",
    "futures_session_end",
    "futures_session_open",
    "session_days",
    "weekday_on_or_after",
    "weekday_on_or_before",
    "weekday_sessions",
]


CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone in which futures sessions open and roll (17:00 America/Chicago)."""

FUTURES_HOLIDAY_MARGIN: Final = timedelta(days=4)
"""How far before its regular open a futures session may start, so a request reaches it.

A holiday's trading folds into the next session, which then opens on an earlier evening. A
Monday holiday moves Tuesday's open one day earlier (the session of 2025-01-21 opened on
Sunday 2025-01-19 at 17:00 Chicago time). The longest known fold is a Friday holiday whose
trading goes to Monday's session (Good Friday 2024: the session of 2024-04-01 opened on
Thursday 2024-03-28 at 17:00), exactly three days before the regular Sunday open; the fourth
day is slack for rows Massive stamps just before an open. The rows a widened request returns
from earlier sessions are dropped by their session, `Date` (Massive's `session_end_date`)."""

FUTURES_SESSION_ROLL: Final = timedelta(hours=7)
"""Adding this to a Chicago time moves 17:00, when the next futures session opens, to midnight:
the one constant of the 17:00 Chicago session rule, which `futures_session` applies to an
instant and `session_days` to a column of them."""

FUTURES_SESSION_END: Final = time(17)
"""The Chicago wall-clock time a futures session is over by: the next session opens then, and the
session's own trading closes by 16:00 (the hour between is the daily maintenance break)."""

_ONE_DAY: Final = timedelta(days=1)

_ONE_MICROSECOND: Final = timedelta(microseconds=1)

_SATURDAY: Final = 5

_DAY_NS: Final = 86_400 * 10**9

_ROLL_NS: Final = FUTURES_SESSION_ROLL // timedelta(microseconds=1) * 1000
"""`FUTURES_SESSION_ROLL` in nanoseconds, for the column form of the session rule."""

_EPOCH_WEEKDAY: Final = calendar.THURSDAY
"""The weekday (Monday is 0) of 1970-01-01, day 0 of the epoch."""


def futures_session_end(session: date) -> datetime:
    """Return the instant by which a futures session is over: 17:00 Chicago on its date.

    The session's trading closes by 16:00 Chicago time and the next session opens at 17:00, so
    17:00 is the first instant no part of the session can still be open, whatever its products'
    own closes. It is a futures session bar's `TimestampEnd`.

    Args:
        session: The session's `session_end_date`.

    Returns:
        The aware instant, in Chicago time.
    """
    return datetime.combine(session, FUTURES_SESSION_END, tzinfo=CHICAGO)


def add_weekdays(day: date, count: int) -> date:
    """Return the day `count` weekdays after another, or before it when `count` is negative.

    Futures sessions end on weekdays only, so counting sessions without a holiday calendar is
    counting weekdays: one weekday after a Friday is the Monday. A Saturday or Sunday counts as
    the gap it sits in: one weekday after it is the Monday, one before it the Friday.

    Args:
        day: The day to count from.
        count: The number of weekdays to move; 0 returns `day` unchanged.

    Returns:
        The day.
    """
    step = _ONE_DAY if count > 0 else -_ONE_DAY
    moved = day
    for _ in range(abs(count)):
        moved += step
        while moved.weekday() >= _SATURDAY:
            moved += step
    return moved


def weekday_on_or_after(day: date) -> date:
    """Return the day itself on a weekday, else the Monday after it.

    No futures session ends on a Saturday or Sunday: what trades from Friday 17:00 Chicago time,
    and from the Sunday open, belongs to Monday's session, so this rolls `futures_session` onto
    the session a row is dated with. `weekday_on_or_before` is its mirror.

    Args:
        day: Any day.

    Returns:
        A weekday.
    """
    while day.weekday() >= _SATURDAY:
        day += _ONE_DAY
    return day


def weekday_on_or_before(day: date) -> date:
    """Return the day itself on a weekday, else the Friday before it.

    The last session a range of session dates can hold ends on this day: no session ends on a
    Saturday or Sunday.

    Args:
        day: Any day.

    Returns:
        A weekday.
    """
    while day.weekday() >= _SATURDAY:
        day -= _ONE_DAY
    return day


def futures_session_open(session: date) -> datetime:
    """Return the instant a futures trading session opens: 17:00 Chicago on the day before.

    The inverse of `futures_session` at the session's first instant, from the same roll; a
    session a holiday folded into opens earlier (`FUTURES_HOLIDAY_MARGIN`).

    Args:
        session: The session's `session_end_date`.

    Returns:
        The aware instant, in Chicago time.
    """
    midnight = datetime(session.year, session.month, session.day, tzinfo=CHICAGO)
    return midnight - FUTURES_SESSION_ROLL


def futures_session(moment: datetime) -> date:
    """Return the futures trading session an instant belongs to, as its `session_end_date`.

    The futures trading day rolls at 17:00 America/Chicago (the weekly open is Sunday 17:00
    CT, and 16:00 to 17:00 CT is the daily maintenance break), so an instant before 17:00
    Chicago time belongs to the session ending on its Chicago date, and one at or after 17:00 to
    the session ending the next day. Daylight-saving changes are followed through the zone.

    Args:
        moment: An aware `datetime`.

    Returns:
        The session's end date.
    """
    return (moment.astimezone(CHICAGO) + FUTURES_SESSION_ROLL).date()


def futures_session_before(end: datetime) -> date:
    """Return the futures session of the last instant before an exclusive end.

    A range's `end` instant is excluded, so the range's last session is the one the instant
    just before it belongs to: an end at a session's 17:00 Chicago open closes the previous
    session, not the one that opens then.

    Args:
        end: An aware `datetime`, the first instant excluded.

    Returns:
        The session's end date (`futures_session` of `end` less one microsecond, the finest
        step a `datetime` takes).
    """
    return futures_session(end - _ONE_MICROSECOND)


def session_days(chicago: pa.Array) -> pa.Array:
    """Return the futures session of each instant by the 17:00 Chicago rule, as epoch days.

    The column form of `futures_session`, from the same constant: the session ends on the
    Chicago date of the instant plus `FUTURES_SESSION_ROLL`, so an instant from 17:00 Chicago
    time on belongs to the next day's session.

    Args:
        chicago: The instants as Chicago wall-clock times, still counted in nanoseconds as if
            they were UTC instants, `int64` (`row_times` converts a column of instants with
            `zoneinfo`).

    Returns:
        The session dates, days since the epoch, `int64`.
    """
    return pc.divide(pc.add(chicago, _ROLL_NS), _DAY_NS)


def weekday_sessions(days: pa.Array) -> pa.Array:
    """Move session dates that fall on a weekend to the Monday after.

    The column form of `weekday_on_or_after`: no futures session ends on a Saturday or
    Sunday.

    Args:
        days: Session dates, days since the epoch, `int64`.

    Returns:
        The dates, every one a weekday.
    """
    shifted = pc.add(days, _EPOCH_WEEKDAY)
    weekday = pc.subtract(shifted, pc.multiply(pc.divide(shifted, 7), 7))
    saturday, sunday = pc.equal(weekday, calendar.SATURDAY), pc.equal(weekday, calendar.SUNDAY)
    move = pc.if_else(saturday, 2, pc.if_else(sunday, 1, 0))
    return pc.add(days, move)
