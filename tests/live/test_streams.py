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
        (_new_york(10, 3, 59), streams.StockSession.OVERNIGHT),  # Monday night, before Tuesday
        (_new_york(10, 4, 0), streams.StockSession.PRE_MARKET),
        (_new_york(10, 9, 29), streams.StockSession.PRE_MARKET),
        (_new_york(10, 9, 30), streams.StockSession.REGULAR),
        (_new_york(10, 15, 59), streams.StockSession.REGULAR),
        (_new_york(10, 16, 0), streams.StockSession.AFTER_HOURS),
        (_new_york(10, 19, 59), streams.StockSession.AFTER_HOURS),
        (_new_york(10, 20, 0), streams.StockSession.CLOSED),  # the nightly pause
        (_new_york(10, 20, 59), streams.StockSession.CLOSED),
        (_new_york(10, 21, 0), streams.StockSession.OVERNIGHT),
        (_new_york(13, 21, 0), streams.StockSession.CLOSED),  # Friday night
        (_new_york(14, 3, 0), streams.StockSession.CLOSED),  # Saturday, early
        (_new_york(14, 10, 0), streams.StockSession.CLOSED),  # a Saturday
        (_new_york(15, 20, 59), streams.StockSession.CLOSED),  # Sunday evening
        (_new_york(15, 21, 0), streams.StockSession.OVERNIGHT),  # Sunday night, before Monday
        (_new_york(16, 3, 59), streams.StockSession.OVERNIGHT),
        (_new_york(1, 21, 0), streams.StockSession.OVERNIGHT),  # the day the clocks go back
        (_new_york(25, 21, 0), streams.StockSession.CLOSED),  # the night before Thanksgiving
        (_new_york(26, 3, 0), streams.StockSession.CLOSED),
        (_new_york(26, 10, 0), streams.StockSession.CLOSED),  # Thanksgiving
        (_new_york(26, 21, 0), streams.StockSession.OVERNIGHT),  # before the half day
        (_new_york(27, 3, 0), streams.StockSession.OVERNIGHT),
        (_new_york(27, 12, 59), streams.StockSession.REGULAR),  # the half day closes at 13:00
        (_new_york(27, 13, 0), streams.StockSession.AFTER_HOURS),
        (_new_york(27, 16, 59), streams.StockSession.AFTER_HOURS),
        (_new_york(27, 17, 0), streams.StockSession.CLOSED),
        (_new_york(27, 21, 0), streams.StockSession.CLOSED),  # the half day is a Friday
    ],
)
def test_the_stock_session_follows_the_new_york_clock(
    now: datetime, session: streams.StockSession
) -> None:
    assert streams.stock_session(now.astimezone(UTC)) is session


@pytest.mark.parametrize(
    ("now", "opened"),
    [
        (_new_york(10, 8, 0), _new_york(10, 4, 0)),  # pre-market
        (_new_york(10, 12, 0), _new_york(10, 4, 0)),  # regular
        (_new_york(10, 18, 0), _new_york(10, 4, 0)),  # after hours
        (_new_york(10, 20, 30), _new_york(10, 20, 30)),  # the pause: Massive's word stands
        (_new_york(10, 22, 0), _new_york(10, 21, 0)),  # overnight, before midnight
        (_new_york(11, 2, 0), _new_york(10, 21, 0)),  # overnight, after midnight
        (_new_york(16, 1, 0), _new_york(15, 21, 0)),  # the Sunday night session
        (_new_york(14, 12, 0), _new_york(14, 12, 0)),  # a Saturday, likewise
    ],
)
@pytest.mark.parametrize("status", ["open", "extended-hours"])
def test_stock_ticks_are_due_from_the_open_of_the_session_massive_says_trades(
    now: datetime, opened: datetime | None, status: str
) -> None:
    assert streams.opened_at("stocks", status, family="trades", now=now) == opened
    assert streams.opened_at("stocks", status, family="quotes", now=now) == opened


@pytest.mark.parametrize(
    "now",
    [
        _new_york(10, 8, 0),  # pre-market
        _new_york(10, 12, 0),  # regular
        _new_york(10, 18, 0),  # after hours
        _new_york(10, 20, 30),  # the nightly pause
        _new_york(10, 22, 0),  # overnight
        _new_york(11, 2, 0),  # overnight, after midnight
        _new_york(14, 12, 0),  # a Saturday
    ],
)
def test_no_stock_tick_is_due_while_massive_says_the_market_is_closed(now: datetime) -> None:
    assert streams.opened_at("stocks", "closed", family="trades", now=now) is None


@pytest.mark.parametrize("market", ["stocks", "options", "indices", "fx", "crypto"])
@pytest.mark.parametrize("family", ["trades", "bars", "values"])
def test_a_status_the_tests_do_not_know_fails_by_name(market: str, family: str) -> None:
    with pytest.raises(ValueError, match="'overnight_trading' is none the tests know"):
        streams.opened_at(market, "overnight_trading", family=family, now=_DAY)


@pytest.mark.parametrize("state", list(streams.MarketState))
def test_every_known_status_reads_by_its_name(state: streams.MarketState) -> None:
    assert streams.market_state(state.value) is state


@pytest.mark.parametrize(("market", "family"), [("stocks", "bars"), ("options", "trades")])
def test_a_regular_session_stream_is_not_due_overnight(market: str, family: str) -> None:
    now = _new_york(10, 22, 0)
    assert streams.opened_at(market, "extended-hours", family=family, now=now) is None
    assert streams.opened_at(market, "open", family=family, now=now) is None


_EVENING = datetime(2026, 11, 10, 18, 40, tzinfo=NEW_YORK)


@pytest.mark.parametrize(
    ("market", "family", "now", "quiet"),
    [
        ("stocks", "quotes", _EVENING, True),
        ("stocks", "trades", datetime(2026, 11, 10, 8, 0, tzinfo=NEW_YORK), True),
        ("stocks", "quotes", _MORNING, False),
        ("stocks", "trades", datetime(2026, 11, 10, 20, 0, tzinfo=NEW_YORK), False),
        ("stocks", "trades", datetime(2026, 11, 10, 22, 0, tzinfo=NEW_YORK), True),
        ("stocks", "quotes", datetime(2026, 11, 11, 3, 0, tzinfo=NEW_YORK), True),
        ("stocks", "trades", datetime(2026, 11, 14, 12, 0, tzinfo=NEW_YORK), False),
        ("stocks", "trades", datetime(2026, 11, 10, 15, 59, tzinfo=NEW_YORK), False),
        ("stocks", "trades", datetime(2026, 11, 10, 16, 0, tzinfo=NEW_YORK), True),
        ("stocks", "bars", _EVENING, False),
        ("options", "trades", _EVENING, False),
    ],
)
def test_one_stocks_ticks_may_go_quiet_only_outside_the_regular_session(
    market: str, family: str, *, now: datetime, quiet: bool
) -> None:
    assert streams.may_go_quiet(market, family, now=now) is quiet


def test_a_session_no_case_handles_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(streams, "stock_session", lambda _at: "lunch")
    with pytest.raises(AssertionError):
        streams.session_open(_DAY)
    with pytest.raises(AssertionError):
        streams.may_go_quiet("stocks", "trades", now=_DAY)


@pytest.mark.parametrize(("market", "family"), [("stocks", "trades"), ("indices", "values")])
def test_a_market_state_no_case_handles_is_refused(
    monkeypatch: pytest.MonkeyPatch, market: str, family: str
) -> None:
    monkeypatch.setattr(streams, "market_state", lambda _status: "halted")
    with pytest.raises(AssertionError):
        streams.opened_at(market, "open", family=family, now=_DAY)
