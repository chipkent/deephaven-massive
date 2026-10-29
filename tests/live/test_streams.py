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
