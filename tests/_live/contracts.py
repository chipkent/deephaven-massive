"""The front futures contract of a day, by each product's roll rule.

A futures test reads the contract most of a product's trading is in on its day: a fixed
contract expires (`ESH6` did in March 2026) and then streams nothing. Trading leaves a contract
on its roll day for the next listed one; the products the live tests read are the E-mini
S&P 500 (CME), corn (CBOT), live cattle (CME), and Micro Bitcoin (CME).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Final

from tests._live.calendar import nth_weekday, trading_day_before

MONTH_CODES: Final = "FGHJKMNQUVXZ"
"""The futures month codes, January to December."""

CONTRACT_MONTHS: Final = {"ES": "HMUZ", "ZC": "HKNUZ", "LE": "GJMQVZ", "MBT": MONTH_CODES}
"""The contract months of each product, by month code."""


def roll_day(product: str, year: int, month: int) -> date:
    """Return the day trading leaves a contract for the next one.

    An E-mini S&P 500 contract expires on its month's third Friday, and the market rolls to
    the next contract eight days before, on the Thursday of the week before. A corn contract's
    holders must take delivery from its first notice day, the last business day of the month
    before its month, so trading leaves it by then. A live cattle contract is delivered during
    its own month, so trading leaves it by the last business day before that month too. A
    Micro Bitcoin contract is listed for every month and settles on its month's last Friday,
    where trading moves to the next month's contract.

    Args:
        product: `"ES"`, `"ZC"`, `"LE"`, or `"MBT"`.
        year: The contract's year.
        month: The contract's month.

    Returns:
        The roll day; on and after it the next contract is the front one.
    """
    if product == "ES":
        return nth_weekday(year, month, 4, nth=3) - timedelta(days=8)
    if product == "MBT":
        return nth_weekday(year, month, 4, nth=-1)
    return trading_day_before(date(year, month, 1))


def front_contract(product: str, day: date) -> str:
    """Return a product's front contract on a day: the first listed one not yet rolled.

    Args:
        product: `"ES"`, `"ZC"`, `"LE"`, or `"MBT"`.
        day: The day.

    Returns:
        Such as `"ESZ6"` on 2026-10-29, `"ESH7"` on 2026-12-10, `"ZCK6"` on 2026-03-02,
        `"LEZ6"` on 2026-10-30, or `"MBTX6"` on 2026-10-30.
    """
    months = [MONTH_CODES.index(code) + 1 for code in CONTRACT_MONTHS[product]]
    listed = [(year, month) for year in (day.year, day.year + 1) for month in months]
    year, month = next((y, m) for y, m in listed if roll_day(product, y, m) > day)
    return f"{product}{MONTH_CODES[month - 1]}{year % 10}"
