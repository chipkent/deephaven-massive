"""When a live stream must deliver rows: an open market whose delayed feed has caught up.

A stream test asserts that Massive acknowledged its topics, whatever the hour, and requires
rows only when they must arrive. That needs the market open, and, on a delayed plan, open for
longer than the plan's delay: a 15-minute delayed index feed sends the first values of a
session fifteen minutes after its open. The delays are those of the vendored WebSocket "Plan
Recency" tables (`FEED_DELAYS`); the index session opens at 09:30 New York time, and the
futures session at the time of Massive's futures market status. Before the rows are due, the
test skips after the acknowledgement.

Every function is pure; `tests/conftest.py`'s `rows_due` fixture reads the market status and
calls them.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Final

from tests._live.calendar import NEW_YORK

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tests._live.plans import LivePlans

FEED_DELAYS: Final[Mapping[tuple[str, str], timedelta]] = {
    ("indices", "starter"): timedelta(minutes=15),
    ("futures", "starter"): timedelta(minutes=10),
    ("futures", "developer"): timedelta(minutes=10),
}
"""How far each plan's stream lags the market; a plan not listed is real-time. A key the probe
reads as Futures Developer may hold Advanced, which is real-time: the longer wait is the safe
reading."""

INDEX_OPEN: Final = time(9, 30)
"""When the S&P indices start computing each trading day, in New York time."""


def index_session_open(now: datetime) -> datetime:
    """Return 09:30 New York time of the day of an instant, when the index session opens.

    Args:
        now: An aware instant.

    Returns:
        The open, in New York time.
    """
    return datetime.combine(now.astimezone(NEW_YORK).date(), INDEX_OPEN, NEW_YORK)


def rows_due(plans: LivePlans, name: str, *, opened: datetime | None, now: datetime) -> bool:
    """Tell whether a class's stream must have delivered rows by now.

    Args:
        plans: The run's plans, whose class plan sets the feed's delay.
        name: The class, `"indices"` or `"futures"`.
        opened: When the market opened, as an aware instant; None when it is not open.
        now: The present, as an aware instant.

    Returns:
        True when the market is open and has been open at least the plan's delay.
    """
    if opened is None:
        return False
    delay = FEED_DELAYS.get((name, plans.tiers[name]), timedelta(0))
    return now >= opened + delay
