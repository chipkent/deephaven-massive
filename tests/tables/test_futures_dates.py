"""Tests for what a futures flush asks its session calendar (`_tables/futures_dates.py`)."""

from __future__ import annotations

from deephaven_massive._tables.futures_dates import (
    UNLISTED_AFTER,
    UNLISTED_AFTER_TEXT,
    LookupState,
    Readiness,
)


def test_a_state_says_nothing_beyond_its_readiness_by_default() -> None:
    state = LookupState(Readiness.WAITING)
    assert (state.reason, state.failing_since, state.error, state.loaded_at) == (
        "",
        None,
        None,
        None,
    )


def test_the_unlisted_wait_is_worded_as_it_lasts() -> None:
    assert UNLISTED_AFTER.total_seconds() == 30 * 60
    assert UNLISTED_AFTER_TEXT == "30 minutes"
