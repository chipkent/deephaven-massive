"""Tests for the futures session rule: the session of an instant, scalar and Arrow."""

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest

from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    add_weekdays,
    futures_session,
    futures_session_before,
    futures_session_end,
    futures_session_open,
    session_days,
    weekday_on_or_after,
    weekday_sessions,
)


def test_futures_session_before_excludes_the_end() -> None:
    opens = datetime(2025, 1, 21, 17, tzinfo=CHICAGO)
    assert futures_session_before(opens) == date(2025, 1, 21)
    assert futures_session_before(opens + timedelta(microseconds=1)) == date(2025, 1, 22)


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


@pytest.mark.parametrize("session", [date(2025, 3, 4), date(2025, 3, 10), date(2025, 11, 3)])
def test_a_session_opens_at_17_chicago_on_the_day_before(session: date) -> None:
    opens = futures_session_open(session)
    eve = session - timedelta(days=1)
    assert opens == datetime(eve.year, eve.month, eve.day, 17, tzinfo=CHICAGO)
    # The roll agrees: the opening instant belongs to the session, the one before does not.
    assert futures_session(opens) == session
    assert futures_session(opens - timedelta(microseconds=1)) == eve


def test_add_weekdays_skips_weekends_both_ways() -> None:
    friday, saturday = date(2025, 3, 7), date(2025, 3, 8)
    assert add_weekdays(friday, 0) == friday
    assert add_weekdays(friday, 1) == date(2025, 3, 10)
    assert add_weekdays(date(2025, 3, 10), -1) == friday
    assert add_weekdays(saturday, 1) == date(2025, 3, 10)
    assert add_weekdays(saturday, -1) == friday
    assert add_weekdays(date(2024, 3, 28), 3) == date(2024, 4, 2)  # no holiday calendar


def test_a_futures_session_is_over_at_17_chicago() -> None:
    assert futures_session_end(date(2025, 3, 3)) == datetime(2025, 3, 3, 17, tzinfo=CHICAGO)
    assert futures_session(futures_session_end(date(2025, 3, 3))) == date(2025, 3, 4)


def test_the_column_session_rule_agrees_with_the_scalar_one() -> None:
    # Every quarter hour of 2024 to 2026, both daylight-saving changes of each year included,
    # and the instants either side of each 17:00 Chicago roll.
    start = datetime(2024, 1, 1, tzinfo=UTC)
    moments = [start + timedelta(minutes=15 * k) for k in range(3 * 366 * 96)]
    moments += [m - timedelta(microseconds=1) for m in moments]
    # Each instant's Chicago wall clock, counted as if it were a UTC instant.
    wall = [m.astimezone(CHICAGO).replace(tzinfo=UTC) for m in moments]
    column = pa.array([int(w.timestamp() * 10**6) * 1000 for w in wall], pa.int64())
    days = weekday_sessions(session_days(column)).to_pylist()
    epoch = date(1970, 1, 1)
    expected = [(weekday_on_or_after(futures_session(m)) - epoch).days for m in moments]
    assert days == expected
