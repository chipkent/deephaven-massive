"""Tests of when a live stream must deliver rows (`tests/_live/streams.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests._live import streams
from tests._live.calendar import NEW_YORK

_OPEN = datetime(2026, 10, 29, 9, 30, tzinfo=NEW_YORK)


def test_the_index_session_opens_at_half_past_nine_in_new_york() -> None:
    now = datetime(2026, 10, 29, 15, 0, tzinfo=UTC)
    assert streams.index_session_open(now) == _OPEN


_DAY = datetime(2026, 10, 29, 12, 0, tzinfo=NEW_YORK)
_EARLY = datetime(2026, 10, 29, 4, 0, tzinfo=NEW_YORK)


@pytest.mark.parametrize(
    ("market", "status", "family", "opened"),
    [
        ("stocks", "extended-hours", "trades", _EARLY),
        ("stocks", "open", "quotes", _EARLY),
        ("stocks", "closed", "trades", None),
        ("stocks", "extended-hours", "bars", None),
        ("stocks", "open", "bars", _OPEN),
        ("options", "extended-hours", "trades", None),
        ("options", "open", "trades", _OPEN),
        ("indices", "open", "values", _OPEN),
        ("fx", "open", "quotes", _DAY),
        ("crypto", "closed", "trades", None),
    ],
)
def test_a_stream_waits_for_the_session_its_rows_come_from(
    market: str, status: str, *, family: str, opened: datetime | None
) -> None:
    assert streams.opened_at(market, status, family=family, now=_DAY) == opened


_AFTER_CLOSE = datetime(2026, 11, 9, 17, 53, tzinfo=NEW_YORK)
_MORNING = datetime(2026, 11, 9, 10, 0, tzinfo=NEW_YORK)


@pytest.mark.parametrize("market", ["stocks", "options", "indices"])
def test_no_regular_session_rows_are_due_after_the_close_whatever_the_status(market: str) -> None:
    assert streams.opened_at(market, "open", family="values", now=_AFTER_CLOSE) is None


def test_regular_session_rows_are_due_before_the_close() -> None:
    opened = streams.opened_at("indices", "open", family="values", now=_MORNING)
    assert opened == datetime(2026, 11, 9, 9, 30, tzinfo=NEW_YORK)


def test_stock_trades_stay_due_in_the_evening_extended_hours() -> None:
    opened = streams.opened_at("stocks", "extended-hours", family="trades", now=_AFTER_CLOSE)
    assert opened == datetime(2026, 11, 9, 4, 0, tzinfo=NEW_YORK)


@pytest.mark.parametrize(
    ("now", "close"),
    [
        (_MORNING, datetime(2026, 11, 9, 16, 0, tzinfo=NEW_YORK)),
        (
            datetime(2026, 11, 27, 15, 0, tzinfo=UTC),
            datetime(2026, 11, 27, 13, 0, tzinfo=NEW_YORK),
        ),
    ],
)
def test_the_regular_session_closes_at_four_or_at_one_on_a_half_day(
    now: datetime, close: datetime
) -> None:
    assert streams.regular_close(now) == close
    assert streams.opened_at("indices", "open", family="values", now=close) is None
    assert streams.opened_at(
        "indices", "open", family="values", now=close - timedelta(seconds=1)
    ) == streams.index_session_open(now)


def test_a_futures_session_opens_when_its_status_says() -> None:
    since = datetime(2026, 10, 28, 18, 0, tzinfo=NEW_YORK)
    assert streams.opened_at("futures", "open", family="bars", now=_DAY, since=since) == since
    assert streams.opened_at("futures", "pause", family="bars", now=_DAY, since=since) is None


@pytest.mark.parametrize(
    ("now", "deadline"),
    [
        (
            datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC),
            datetime(2026, 10, 29, 12, 1, 15, tzinfo=UTC),
        ),
        (
            datetime(2026, 10, 29, 12, 0, 59, 900, tzinfo=UTC),
            datetime(2026, 10, 29, 12, 1, 15, tzinfo=UTC),
        ),
    ],
)
def test_a_minute_bar_is_due_just_past_the_next_minute(now: datetime, deadline: datetime) -> None:
    assert streams.minute_bar_deadline(now) == deadline


def _new_york(day: int, hour: int, minute: int) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=NEW_YORK)


@pytest.mark.parametrize(
    ("now", "session"),
    [
        (_new_york(10, 3, 59), streams.OVERNIGHT),
        (_new_york(10, 4, 0), streams.PRE_MARKET),
        (_new_york(10, 9, 29), streams.PRE_MARKET),
        (_new_york(10, 9, 30), streams.REGULAR),
        (_new_york(10, 15, 59), streams.REGULAR),
        (_new_york(10, 16, 0), streams.AFTER_HOURS),
        (_new_york(10, 19, 59), streams.AFTER_HOURS),
        (_new_york(10, 20, 0), streams.OVERNIGHT),
        (_new_york(27, 12, 59), streams.REGULAR),  # the day after Thanksgiving closes at 13:00
        (_new_york(27, 13, 0), streams.AFTER_HOURS),
        (_new_york(27, 16, 59), streams.AFTER_HOURS),
        (_new_york(27, 17, 0), streams.OVERNIGHT),
        (_new_york(26, 10, 0), streams.OVERNIGHT),  # Thanksgiving
        (_new_york(14, 10, 0), streams.OVERNIGHT),  # a Saturday
    ],
)
def test_the_stock_session_follows_the_new_york_clock(now: datetime, session: str) -> None:
    assert streams.stock_session(now.astimezone(UTC)) == session


_EVENING = datetime(2026, 11, 10, 18, 40, tzinfo=NEW_YORK)


@pytest.mark.parametrize(
    ("market", "family", "now", "quiet"),
    [
        ("stocks", "quotes", _EVENING, True),
        ("stocks", "trades", datetime(2026, 11, 10, 8, 0, tzinfo=NEW_YORK), True),
        ("stocks", "quotes", _MORNING, False),
        ("stocks", "trades", datetime(2026, 11, 10, 20, 0, tzinfo=NEW_YORK), False),
        ("stocks", "trades", datetime(2026, 11, 10, 15, 59, tzinfo=NEW_YORK), False),
        ("stocks", "trades", datetime(2026, 11, 10, 16, 0, tzinfo=NEW_YORK), True),
        ("stocks", "bars", _EVENING, False),
        ("options", "trades", _EVENING, False),
    ],
)
def test_one_stocks_ticks_may_go_quiet_only_in_the_extended_hours(
    market: str, family: str, *, now: datetime, quiet: bool
) -> None:
    assert streams.may_go_quiet(market, family, now=now) is quiet
