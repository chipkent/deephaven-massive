"""The raw requests of the options live tests: open interest, unusual trades, tickers, contracts.

Some options live tests read Massive's raw rows through the wire REST client, which encodes each
query value by the kind the spec gives its parameter and refuses a value of another type before
sending: a date filter takes a `date`, a flag a `bool`, a limit an `int`. Each function here
builds one such request, as its path and query, from arguments typed for those kinds, so mypy
refuses a date passed as ISO text and `tests/live/test_option_requests.py` encodes every
request offline. A mistake then fails in CI, not in the owner's live run. A `None` argument is
sent as no filter, since the client leaves a `None` query value out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from datetime import date

__all__ = [
    "CONTRACTS",
    "GREEKS",
    "OPEN_INTEREST",
    "TICKERS",
    "UNUSUAL_TRADES",
    "Request",
    "contracts",
    "greeks",
    "index_list",
    "open_interest",
    "stock_list",
    "unusual_trades",
]

OPEN_INTEREST = "/options/v1/open-interest"
GREEKS = "/options/v1/analytics"
UNUSUAL_TRADES = "/options/v1/unusual-trades"
TICKERS = "/v3/reference/tickers"
CONTRACTS = "/v3/reference/options/contracts"

Request: TypeAlias = "tuple[str, dict[str, object]]"
"""A REST path that takes no path parameters, and its query values by wire name."""


def open_interest(*, limit: int, root: str | None = None, through: date | None = None) -> Request:
    """Return a request of one page of open interest.

    Args:
        limit: The most rows on the page.
        root: The OCC root whose contracts to read; None for every root.
        through: The last day to read (`date.lte`); None for the latest.

    Returns:
        The path and query.
    """
    return OPEN_INTEREST, {"root": root, "date.lte": through, "limit": limit}


def greeks(*, limit: int, underlying: str | None = None) -> Request:
    """Return a request of one page of the latest greeks.

    Args:
        limit: The most rows on the page.
        underlying: The underlying as sent (`underlying_ticker`), such as `"SPX"` or `"I:SPX"`,
            to see which spelling Massive answers; None for every underlying.

    Returns:
        The path and query.
    """
    return GREEKS, {"underlying_ticker": underlying, "limit": limit}


def unusual_trades(*, limit: int) -> Request:
    """Return a request of one page of the latest unusual trades of any root.

    Args:
        limit: The most rows on the page.

    Returns:
        The path and query.
    """
    return UNUSUAL_TRADES, {"limit": limit}


def index_list(*, active: bool) -> Request:
    """Return a request of Massive's index list, active or retired.

    Args:
        active: True for the indices Massive lists as active, False for the retired ones.

    Returns:
        The path and query, 1,000 indices a page.
    """
    return TICKERS, {"market": "indices", "active": active, "limit": 1000}


def stock_list() -> Request:
    """Return a request of Massive's active stock list, which holds funds too.

    Returns:
        The path and query, 1,000 tickers a page.
    """
    return TICKERS, {"market": "stocks", "active": True, "limit": 1000}


def contracts(*, underlying: str, limit: int) -> Request:
    """Return a request of one page of today's contracts of one underlying.

    Args:
        underlying: The underlying as the contracts list takes it, bare for an index (`SPX`).
        limit: The most rows on the page.

    Returns:
        The path and query.
    """
    return CONTRACTS, {"underlying_ticker": underlying, "limit": limit}
