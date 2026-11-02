"""Which live streams a key's subscriptions allow: the units of the live run's streaming groups.

A *streaming unit* is one asset class at one recency, such as `"stocks delayed"` or
`"currencies real-time"`. The live-run program (`live_run`) streams each unit for one full
trading day (`stream-day`), compares that day with history the next day (`stream-day-reconcile`),
and measures the throughput of the busiest ones (`throughput`). A unit is new, and so due, when
a class starts to stream or moves from delayed to real-time data.

The overnight group (`stream-night`) has units of its own, named by what they stream: `crypto`
for the Currencies subscription, whose crypto pairs trade around the clock, and `futures`, such
as `"crypto real-time"` or `"futures delayed"`.

The recency of each tier, and the documented delay of delayed data, come from Massive's plan
tables; Basic streams nothing. Every function is pure.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

DELAYED: Final = "delayed"
REAL_TIME: Final = "real-time"

RECENCY: Final[Mapping[tuple[str, str], str]] = {
    ("stocks", "starter"): DELAYED,
    ("stocks", "developer"): DELAYED,
    ("stocks", "advanced"): REAL_TIME,
    ("stocks", "business"): REAL_TIME,
    ("stocks", "business_expansion"): REAL_TIME,
    ("options", "starter"): DELAYED,
    ("options", "developer"): DELAYED,
    ("options", "advanced"): REAL_TIME,
    ("options", "business"): REAL_TIME,
    ("options", "business_expansion"): REAL_TIME,
    ("indices", "starter"): DELAYED,
    ("indices", "advanced"): REAL_TIME,
    ("indices", "business"): REAL_TIME,
    ("currencies", "starter"): REAL_TIME,
    ("currencies", "business"): REAL_TIME,
    ("futures", "starter"): DELAYED,
    ("futures", "developer"): DELAYED,
    ("futures", "advanced"): REAL_TIME,
    ("futures", "business"): REAL_TIME,
}
"""The recency of each streaming tier (`vendor/massive/websocket/`'s plan tables); a tier not
listed, Basic, streams nothing."""

DELAYS: Final[Mapping[str, timedelta]] = {
    "stocks": timedelta(minutes=15),
    "options": timedelta(minutes=15),
    "indices": timedelta(minutes=15),
    "futures": timedelta(minutes=10),
}
"""How far each class's delayed data lags the market; currencies are never delayed."""

NIGHT_CLASSES: Final[Mapping[str, str]] = {"currencies": "crypto", "futures": "futures"}
"""The subscriptions a stream night streams, each with the name its night unit starts with."""

THROUGHPUT_CLASSES: Final = ("stocks", "currencies")
"""The classes whose throughput is measured: the two with a busy stream of every symbol."""


def unit(name: str, recency: str) -> str:
    """Name a streaming unit.

    Args:
        name: The class.
        recency: `DELAYED` or `REAL_TIME`.

    Returns:
        Such as `"stocks delayed"`.
    """
    return f"{name} {recency}"


def stream_units(tiers: Mapping[str, str]) -> list[str]:
    """Return the units a key's tiers can stream.

    Args:
        tiers: Each class's tier.

    Returns:
        One unit per class that streams, in the classes' order.
    """
    return [
        unit(name, RECENCY[name, tier]) for name, tier in tiers.items() if (name, tier) in RECENCY
    ]


def night_units(tiers: Mapping[str, str]) -> list[str]:
    """Return the units a key's tiers can stream through a night.

    Args:
        tiers: Each class's tier.

    Returns:
        A `crypto` unit when Currencies streams and a `futures` unit when Futures does, such as
        `["crypto real-time", "futures delayed"]`.
    """
    return [
        unit(NIGHT_CLASSES[name], RECENCY[name, tier])
        for name, tier in tiers.items()
        if name in NIGHT_CLASSES and (name, tier) in RECENCY
    ]


def throughput_units(tiers: Mapping[str, str]) -> list[str]:
    """Return the units whose throughput is measured.

    Args:
        tiers: Each class's tier.

    Returns:
        The stream units of `THROUGHPUT_CLASSES`.
    """
    return [u for u in stream_units(tiers) if u.split()[0] in THROUGHPUT_CLASSES]


def delay_of(unit_name: str) -> timedelta:
    """Return how far a unit's data lags the market.

    Args:
        unit_name: The unit.

    Returns:
        The class's documented delay for a delayed unit, else zero.
    """
    name, recency = unit_name.split()
    return DELAYS.get(name, timedelta()) if recency == DELAYED else timedelta()
