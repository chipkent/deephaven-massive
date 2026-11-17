"""Tests of the choice of recent splits and dividends (`tests/_live/events.py`)."""

from __future__ import annotations

from datetime import date
from typing import Any

from tests._live import events

_WINDOW = (date(2025, 10, 29), date(2026, 10, 26))


def _split(
    symbol: str, executed: str, ratio: tuple[float, float], *, kind: str = "forward_split"
) -> dict[str, Any]:
    return {
        "Symbol": symbol,
        "ExecutionDate": executed,
        "SplitFrom": ratio[0],
        "SplitTo": ratio[1],
        "AdjustmentType": kind,
    }


_SPLITS = [
    _split("LATE", "2026-11-20", (1.0, 2.0), kind="forward_split"),  # announced, after the window
    _split("FIRST", "2026-09-14", (1.0, 4.0), kind="forward_split"),
    _split("SMALL", "2026-09-08", (2.0, 3.0), kind="forward_split"),  # a ratio under 2
    _split("BRK.B", "2026-08-03", (1.0, 2.0), kind="forward_split"),  # not a plain ticker
    _split("AGAIN", "2026-07-06", (1.0, 2.0), kind="forward_split"),
    _split("AGAIN", "2026-10-05", (10.0, 1.0), kind="reverse_split"),  # a later split hides it
    _split("SAT", "2026-06-06", (1.0, 3.0), kind="forward_split"),  # not a trading day
    _split("DIV", "2026-05-04", (1.0, 1.05), kind="stock_dividend"),
    _split("OLD", "2025-06-02", (1.0, 2.0), kind="forward_split"),  # before the window
    _split("LAST", "2026-03-02", (1.0, 2.0), kind="forward_split"),
]


def test_the_event_window_is_a_year_ending_three_days_back() -> None:
    assert events.event_window(date(2026, 10, 29)) == _WINDOW


def test_forward_splits_keep_plain_recent_and_last_splits() -> None:
    found = events.forward_splits(_SPLITS, _WINDOW)
    assert found == [
        events.Split("FIRST", date(2026, 9, 11), date(2026, 9, 14), 4.0),
        events.Split("LAST", date(2026, 2, 27), date(2026, 3, 2), 2.0),
    ]


def test_a_later_split_of_any_kind_counts() -> None:
    assert events.split_after(_SPLITS, "AGAIN", date(2026, 7, 6))
    assert not events.split_after(_SPLITS, "AGAIN", date(2026, 10, 5))
    assert not events.split_after(_SPLITS, "NONE", date(2020, 1, 1))


def test_special_dividends_skip_a_split_from_a_week_before() -> None:
    dividends: list[dict[str, Any]] = [
        {"Symbol": "CASH", "ExDividendDate": "2026-09-15", "CashAmount": 5.0},
        {"Symbol": "FIRST", "ExDividendDate": "2026-09-16", "CashAmount": 2.0},  # split on the 14th
        {"Symbol": "WKND", "ExDividendDate": "2026-09-19", "CashAmount": 1.0},  # a Saturday
        {"Symbol": "BRK.A", "ExDividendDate": "2026-09-15", "CashAmount": 1.0},
        {"Symbol": "NULL", "ExDividendDate": "2026-08-17", "CashAmount": None},
    ]
    assert events.special_dividends(dividends, _SPLITS) == [
        events.Dividend("CASH", date(2026, 9, 15), 5.0),
        events.Dividend("NULL", date(2026, 8, 17), 0.0),
    ]


def test_first_common_keeps_common_stocks_and_stops_at_five() -> None:
    asked: list[str] = []

    def is_common(symbol: str) -> bool:
        asked.append(symbol)
        return not symbol.startswith("ETF")

    symbols = ["ETF1", "A", "B", "ETF2", "C", "D", "E", "F", "G"]
    kept = events.first_common(symbols, lambda s: s, is_common)
    assert kept == ["A", "B", "C", "D", "E"]
    assert asked == ["ETF1", "A", "B", "ETF2", "C", "D", "E"]


def test_first_common_keeps_fewer_when_the_events_run_out() -> None:
    assert events.first_common(["A", "ETF", "B"], lambda s: s, lambda s: s != "ETF") == ["A", "B"]
