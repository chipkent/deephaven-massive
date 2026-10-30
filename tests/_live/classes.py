"""Which asset class a raw REST request reads, and so which rate-limit bucket it is paced in.

Massive sells one subscription per asset class and rate-limits each one on its Basic plan, so
a raw request a live test sends takes its permit from its class's bucket of the run's rate
governor. The same reading of a path names the class of a refused request, whose plan decides
whether a history refusal is expected (`plans.history_verdict`). `market_lookup` chooses the
class whose ticker lookups (`/v3/reference/tickers?market=...`) a rate-limit check spends a
minute of, to see whether Massive charges a lookup filtered by a class's market to that class.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

TICKER_CLASSES: Final = {"X:": "currencies", "C:": "currencies", "I:": "indices", "O:": "options"}
"""The class of a ticker by its prefix; a ticker without one is a stock."""

_PATH_CLASSES: Final = (
    ("/options", "options"),
    ("/indices", "indices"),
    ("/crypto", "currencies"),
    ("/forex", "currencies"),
    ("/fx/", "currencies"),
    ("currencies", "currencies"),
    ("/conversion/", "currencies"),
    ("/stocks", "stocks"),
    ("/v2/last/nbbo", "stocks"),
)
"""The class of a path without a ticker, by a word in it."""

_STOCK_PATHS: Final = ("/v2/aggs/ticker/", "/v3/trades/", "/v3/quotes/")
"""Paths that serve stocks when they name an unprefixed ticker."""


def bucket_of(template: str, path_params: Mapping[str, object]) -> str:
    """Return the rate governor's bucket of a raw live request: the subscription of its data.

    A path that names a ticker is charged to the ticker's class, by its prefix (`X:` and `C:`
    currencies, `I:` indices, `O:` options, any other stocks); the futures paths to futures; a
    path without a ticker by a word of its template. Reference data, economy data, market
    status, partner data, and the unified snapshot belong to no subscription (`other`).

    Args:
        template: The generated path template, such as `"/v2/aggs/ticker/{stocksTicker}/prev"`.
        path_params: The request's placeholder values.

    Returns:
        `"stocks"`, `"options"`, `"indices"`, `"currencies"`, `"futures"`, or `"other"`.
    """
    if template.startswith("/futures/"):
        return "futures"
    for name, value in path_params.items():
        if "icker" in name:
            return TICKER_CLASSES.get(str(value)[:2], "stocks")
    return next((name for word, name in _PATH_CLASSES if word in template), "other")


def class_of_path(path: str) -> str | None:
    """Return the class of the data a request path reads, its placeholders filled in.

    Args:
        path: The decoded path of a sent request, such as `"/v2/aggs/ticker/O:SPY.../range/..."`.

    Returns:
        The class: futures for a `/futures/` path; else the class of the first segment with a
        ticker prefix; else the class a word of the path names; else stocks for an aggregates,
        trades, or quotes path of an unprefixed ticker; None for data of no class.
    """
    if path.startswith("/futures/"):
        return "futures"
    prefixed = [TICKER_CLASSES[part[:2]] for part in path.split("/") if part[:2] in TICKER_CLASSES]
    if prefixed:
        return prefixed[0]
    named = next((name for word, name in _PATH_CLASSES if word in path), None)
    if named is not None:
        return named
    return "stocks" if path.startswith(_STOCK_PATHS) else None


TICKER_MARKETS: Final = {
    "indices": ("indices",),
    "currencies": ("fx", "crypto"),
    "stocks": ("stocks",),
}
"""The markets of `/v3/reference/tickers` that list each class's tickers, by class, in the order
a market lookup check prefers the class: the one a run needs least first. Options and futures
tickers are listed by endpoints of their own."""


@dataclass(frozen=True)
class MarketLookup:
    """The class a market lookup check spends, and the markets it looks tickers up in.

    Attributes:
        name: The class on Basic whose minute the check spends, such as `"indices"`.
        markets: The class's own markets (`TICKER_MARKETS`), such as `("fx", "crypto")`.
        other_market: A market of another class, the check's control: its lookup is never
            charged to the spent class.
    """

    name: str
    markets: tuple[str, ...]
    other_market: str


def market_lookup(tiers: Mapping[str, str]) -> MarketLookup | None:
    """Choose the class whose market-filtered ticker lookups a rate-limit check tests.

    Args:
        tiers: The plan of each class, such as `LivePlans.tiers`.

    Returns:
        The first class of `TICKER_MARKETS` the key holds on Basic, with its markets and the
        first market of the next class in that order (wrapping around); None when none of
        them is on Basic.
    """
    names = list(TICKER_MARKETS)
    for index, name in enumerate(names):
        if tiers.get(name) == "basic":
            other = names[(index + 1) % len(names)]
            return MarketLookup(name, TICKER_MARKETS[name], TICKER_MARKETS[other][0])
    return None
