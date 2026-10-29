"""Which seven days a week bar covers: the candidate windows and the library's week.

Massive stamps a week bar on a day it does not document, so the week checks rebuild each bar's
volume from hour bars over candidate seven-day windows and see which window is the bar's
bucket. The library starts every week at Monday 00:00 in the class's day zone (UTC for crypto
and forex, New York for stocks); a check passes when that window is the closest candidate,
ties allowed. Deciding by comparison, not by an absolute tolerance, keeps a noisy volume from
failing a right answer. For stocks, which do not trade at weekends, every start from Friday
evening to Monday morning ties, so the check can only catch a week that starts mid-week.

Every function is pure; `tests/conftest.py`'s `week_bar_report` reads the bars and calls them.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import TYPE_CHECKING, Any, Final

from tests._live.calendar import NEW_YORK

if TYPE_CHECKING:
    from collections.abc import Sequence

MATCH: Final = 1e-9
"""The relative volume error within which a candidate window is the bar's bucket."""

WEEK_MS: Final = 7 * 86_400_000
"""Milliseconds per week."""

Candidate = tuple[float, str, bool]
"""A scored candidate window: its relative volume error, its label, and whether it is the
library's week."""


def candidates(stamp_ms: int, zone: tzinfo) -> list[tuple[str, int, bool]]:
    """Return the candidate starts of the seven days a week bar may cover.

    Args:
        stamp_ms: The bar's stamp, in epoch milliseconds.
        zone: The class's day zone, whose Monday 00:00 the library starts a week at.

    Returns:
        `(label, start_ms, library)` triples: 00:00 UTC, 00:00 New York time, and 17:00 New
        York time (the forex trading day's start) of each day from three days before the
        stamp's UTC day to three days after; `library` marks Monday 00:00 in `zone`.
    """
    day = datetime.fromtimestamp(stamp_ms / 1000, UTC).date()
    found = []
    for offset in range(-3, 4):
        start = day + timedelta(days=offset)
        for hour, where, name in (
            (0, UTC, "UTC"),
            (0, NEW_YORK, "New York"),
            (17, NEW_YORK, "New York"),
        ):
            moment = datetime.combine(start, time(hour), where)
            library = start.weekday() == 0 and hour == 0 and where == zone
            label = f"{start:%a %m-%d} {hour:02d}:00 {name}"
            found.append((label, int(moment.timestamp()) * 1000, library))
    return found


def score(
    stamp_ms: int, volume: float, hours: Sequence[dict[str, Any]], *, zone: tzinfo
) -> list[Candidate]:
    """Score every candidate window of a week bar by how far its hour-bar volume is from the bar's.

    Args:
        stamp_ms: The bar's stamp, in epoch milliseconds.
        volume: The bar's volume.
        hours: The hour bars around it, as Massive sent them (`t` in epoch ms, `v`).
        zone: The class's day zone.

    Returns:
        The candidates, closest first.
    """

    def total(start_ms: int) -> float:
        return float(sum(h["v"] for h in hours if start_ms <= h["t"] < start_ms + WEEK_MS))

    return sorted(
        (abs(total(start) - volume) / max(abs(volume), 1e-12), label, library)
        for label, start, library in candidates(stamp_ms, zone)
    )


def library_is_closest(scored: Sequence[Candidate]) -> bool:
    """Tell whether the library's week is the closest candidate window, ties allowed.

    Args:
        scored: The candidates, closest first (`score`).

    Returns:
        True when the library's window is within `MATCH` of the closest error.
    """
    closest = scored[0][0]
    return any(library and error <= closest + MATCH for error, _, library in scored)


def is_checked(stamp_ms: int, weeks: tuple[date, date]) -> bool:
    """Tell whether a week bar belongs to the weeks a check reads.

    A week bar may be stamped on its Monday or on the weekend before (as late as Saturday in
    New York, 00:00 UTC Sunday), so one is checked when its UTC stamp day falls from the
    Saturday before the first Monday to the last week's Monday.

    Args:
        stamp_ms: The bar's stamp, in epoch milliseconds.
        weeks: The first week's Monday and the last week's Sunday.

    Returns:
        True for a bar of those weeks.
    """
    day = datetime.fromtimestamp(stamp_ms / 1000, UTC).date()
    return weeks[0] - timedelta(days=2) <= day <= weeks[1] - timedelta(days=6)
