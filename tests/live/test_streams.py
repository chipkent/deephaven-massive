"""Tests of when a delayed stream must deliver rows (`tests/_live/streams.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests._live import plans, streams
from tests._live.calendar import NEW_YORK

_OPEN = datetime(2026, 10, 29, 9, 30, tzinfo=NEW_YORK)


def test_the_index_session_opens_at_half_past_nine_in_new_york() -> None:
    now = datetime(2026, 10, 29, 15, 0, tzinfo=UTC)
    assert streams.index_session_open(now) == _OPEN


@pytest.mark.parametrize(
    ("declared", "name", "after", "due"),
    [
        ("indices=starter", "indices", timedelta(minutes=14, seconds=59), False),
        ("indices=starter", "indices", timedelta(minutes=15), True),
        ("indices=advanced", "indices", timedelta(0), True),
        ("futures=starter", "futures", timedelta(minutes=9), False),
        ("futures=developer", "futures", timedelta(minutes=9, seconds=59), False),
        ("futures=developer", "futures", timedelta(minutes=10), True),
        ("futures=advanced", "futures", timedelta(seconds=1), True),
    ],
)
def test_rows_are_due_once_the_feed_delay_has_passed(
    declared: str, name: str, *, after: timedelta, due: bool
) -> None:
    held = plans.declared_plans(declared)
    assert streams.rows_due(held, name, opened=_OPEN, now=_OPEN + after) is due


def test_no_rows_are_due_while_the_market_is_closed() -> None:
    held = plans.declared_plans("indices=advanced")
    assert not streams.rows_due(held, "indices", opened=None, now=_OPEN)


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


def test_a_futures_session_opens_when_its_status_says() -> None:
    since = datetime(2026, 10, 28, 18, 0, tzinfo=NEW_YORK)
    assert streams.opened_at("futures", "open", family="bars", now=_DAY, since=since) == since
    assert streams.opened_at("futures", "pause", family="bars", now=_DAY, since=since) is None


def test_a_delayed_stock_plan_waits_out_its_delay() -> None:
    held = plans.declared_plans("stocks=starter")
    assert not streams.rows_due(held, "stocks", opened=_EARLY, now=_EARLY + timedelta(minutes=14))
    assert streams.rows_due(held, "stocks", opened=_EARLY, now=_EARLY + timedelta(minutes=15))


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
