"""The units of the live run's streaming groups: one per subscription, and their night units.

A *streaming unit* is one subscription's streams, named by the subscription, such as `"stocks"`
or `"currencies"`. The live-run program (`live_run`) streams each unit for one full trading day
(`stream-day`), compares that day with history the next day (`stream-day-reconcile`), and
measures the throughput of the busiest ones (`throughput`). Every subscription has a unit,
whatever the key holds: Massive's refusal of a unit's streams is its answer, recorded as
`refused`.

The overnight group (`stream-night`) has units of its own, named by what they stream: `crypto`
for the Currencies subscription, whose crypto pairs trade around the clock, and `futures`.

Which feed a unit streams from is the owner's settings' choice (`delayed_subscriptions`), made
in the child that opens the session; a unit's name does not change with it. Every function is
pure.
"""

from __future__ import annotations

from typing import Final

SUBSCRIPTIONS: Final = ("stocks", "options", "indices", "currencies", "futures")
"""Massive's subscriptions, one per asset class, in the order their units run."""

NIGHT_UNITS: Final = {"crypto": "currencies", "futures": "futures"}
"""The units a stream night streams, each with the subscription it belongs to."""

THROUGHPUT_UNITS: Final = ("stocks", "currencies")
"""The units whose throughput is measured: the two with a busy stream of every symbol."""


def stream_units() -> list[str]:
    """Return the units of a stream day.

    Returns:
        One unit per subscription, in `SUBSCRIPTIONS` order.
    """
    return list(SUBSCRIPTIONS)


def night_units() -> list[str]:
    """Return the units of a stream night.

    Returns:
        `["crypto", "futures"]`.
    """
    return list(NIGHT_UNITS)


def throughput_units() -> list[str]:
    """Return the units whose throughput is measured.

    Returns:
        `THROUGHPUT_UNITS`, as a list.
    """
    return list(THROUGHPUT_UNITS)


def subscription_of(unit: str) -> str:
    """Return the subscription a streaming unit belongs to.

    Args:
        unit: A stream-day or stream-night unit.

    Returns:
        The subscription: a night's `crypto` unit is the Currencies subscription's.
    """
    return NIGHT_UNITS.get(unit, unit)
