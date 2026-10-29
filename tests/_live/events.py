"""The recent splits and special dividends the corporate-action checks read.

A fixed split or dividend ages out of a Basic plan's two years of history, so the checks find
recent ones through the library's own `stocks.splits` and `stocks.dividends`, once per run.
This module chooses among the rows those return; the `recent_splits` and `special_dividends`
fixtures in `tests/conftest.py` fetch them and check each candidate's ticker type.

An event is kept only when no split of any kind follows it: adjusted bars restate the prices
before every later split too, which would hide or fake what the check reads.
"""

from __future__ import annotations

import dataclasses
import re
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Final, TypeVar

from tests._live.calendar import is_trading_day, trading_day_before

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

EVENT_DAYS: Final = 365
"""How many days back the search for a recent split or special dividend reaches."""

EVENT_SETTLED: Final = 3
"""Days an event must lie behind the run's day, so the bars after it exist."""

EVENT_CANDIDATES: Final = 5
"""How many recent events are kept, newest first; a test takes the first one that has bars."""

PLAIN_TICKER: Final = re.compile(r"[A-Z]{1,5}")
"""A plain listed ticker: no class suffix, warrant, or unit, which may lack bars. ETFs,
leveraged ones included, have plain tickers too, so a candidate is also checked to be a common
stock (`first_common`)."""

_DIVIDEND_SPAN: Final = timedelta(days=8)
"""How far before an ex-dividend date the dividend check reads, so a split after it counts."""

Row = dict[str, Any]
"""A row of the library's `stocks.splits` or `stocks.dividends`, as `table_rows` reads it."""

E = TypeVar("E")


@dataclasses.dataclass(frozen=True)
class Split:
    """A recent forward split of a US-listed stock.

    Attributes:
        symbol: The stock.
        before: The last trading day before the split, the last one on the old basis.
        executed: The execution date, the first trading day on the new basis.
        ratio: `SplitTo / SplitFrom`, such as 10.0 for a 10-for-1 split.
    """

    symbol: str
    before: date
    executed: date
    ratio: float


@dataclasses.dataclass(frozen=True)
class Dividend:
    """A recent special cash dividend of a US-listed stock.

    Attributes:
        symbol: The stock.
        ex_date: The ex-dividend date.
        amount: The cash amount per share, as declared.
    """

    symbol: str
    ex_date: date
    amount: float


def event_window(day: date) -> tuple[date, date]:
    """Return the first and last day an event search reads.

    Args:
        day: The run's New York day.

    Returns:
        From a year before `day` to three days before it.
    """
    return day - timedelta(days=EVENT_DAYS), day - timedelta(days=EVENT_SETTLED)


def split_after(splits: Iterable[Row], symbol: str, day: date) -> bool:
    """Tell whether a stock has a split of any kind executed after a day.

    Args:
        splits: Every split from the window's start on, announced future ones included.
        symbol: The stock.
        day: The day.

    Returns:
        True when one of the stock's splits executes after `day`.
    """
    return any(
        row["Symbol"] == symbol and date.fromisoformat(str(row["ExecutionDate"])) > day
        for row in splits
    )


def forward_splits(splits: list[Row], window: tuple[date, date]) -> list[Split]:
    """Choose the forward splits a split check may read, newest first.

    A candidate is a forward split with a ratio of at least 2, of a plain ticker, executed on
    a trading day inside the window, with no later split of the same stock.

    Args:
        splits: Every split from the window's start on, newest first.
        window: The first and last execution date (`event_window`).

    Returns:
        The candidates, in the order of `splits`.
    """
    found = []
    for row in splits:
        executed = date.fromisoformat(str(row["ExecutionDate"]))
        ratio = float(row["SplitTo"]) / float(row["SplitFrom"])
        symbol = str(row["Symbol"])
        forward = row["AdjustmentType"] == "forward_split" and ratio >= 2
        inside = window[0] <= executed <= window[1] and is_trading_day(executed)
        if forward and inside and _plain_and_last(splits, symbol, executed):
            found.append(Split(symbol, trading_day_before(executed), executed, ratio))
    return found


def _plain_and_last(splits: Iterable[Row], symbol: str, day: date) -> bool:
    """Tell whether a ticker is plain and has no split of any kind after a day.

    Args:
        splits: Every split from the window's start on.
        symbol: The ticker.
        day: The day.

    Returns:
        True for a plain ticker (`PLAIN_TICKER`) without a later split.
    """
    return PLAIN_TICKER.fullmatch(symbol) is not None and not split_after(splits, symbol, day)


def special_dividends(dividends: Iterable[Row], splits: list[Row]) -> list[Dividend]:
    """Choose the special cash dividends a dividend check may read, in the given order.

    A candidate is of a plain ticker, goes ex-dividend on a trading day, and has no split of
    any kind from a week before its ex-dividend date on, the span the check reads.

    Args:
        dividends: The special dividends of the window, newest first.
        splits: Every split from the window's start on.

    Returns:
        The candidates.
    """
    found = []
    for row in dividends:
        ex_date = date.fromisoformat(str(row["ExDividendDate"]))
        symbol = str(row["Symbol"])
        if is_trading_day(ex_date) and _plain_and_last(splits, symbol, ex_date - _DIVIDEND_SPAN):
            found.append(Dividend(symbol, ex_date, float(row["CashAmount"] or 0.0)))
    return found


def first_common(
    events: Iterable[E], symbol: Callable[[E], str], is_common: Callable[[str], bool]
) -> list[E]:
    """Keep the events of common stocks, in order, up to `EVENT_CANDIDATES`.

    Each candidate's ticker is checked only until enough are kept, since each check is a
    request.

    Args:
        events: The candidate events.
        symbol: Returns an event's ticker.
        is_common: Tells whether a ticker is a common stock (an ETF, leveraged or not, a fund,
            or a warrant is not).

    Returns:
        The first events whose ticker is a common stock.
    """
    kept: list[E] = []
    for event in events:
        if len(kept) == EVENT_CANDIDATES:
            break
        if is_common(symbol(event)):
            kept.append(event)
    return kept
