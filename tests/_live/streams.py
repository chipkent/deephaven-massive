"""When a live stream must deliver rows: an open market whose delayed feed has caught up.

A stream test asserts that Massive acknowledged its topics, whatever the hour, and requires
rows only when they must arrive. That needs the market open for the stream's kind of rows, and,
on a delayed plan, open for longer than the plan's delay: a 15-minute delayed index feed sends
the first values of a session fifteen minutes after its open. Stock trades and quotes flow in
the extended hours too, from 04:00 New York time; every other stock stream, and every option and
index stream, needs the regular session, from 09:30; the futures session opens at the time of
Massive's futures market status, and forex and crypto at no time this module knows, so their
rows are due as soon as their market is open. The delays are those of the vendored WebSocket
"Plan Recency" tables (`FEED_DELAYS`). Before the rows are due, the test skips after the
acknowledgement, and a pass always means that rows reached the table.

A minute bar arrives only after its minute has ended, so a test that streams minute bars waits
until a little past the next minute boundary (`minute_bar_deadline`).

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
    ("stocks", "starter"): timedelta(minutes=15),
    ("stocks", "developer"): timedelta(minutes=15),
    ("options", "starter"): timedelta(minutes=15),
    ("options", "developer"): timedelta(minutes=15),
    ("indices", "starter"): timedelta(minutes=15),
    ("futures", "starter"): timedelta(minutes=10),
    ("futures", "developer"): timedelta(minutes=10),
}
"""How far each plan's stream lags the market; a plan not listed is real-time."""

INDEX_OPEN: Final = time(9, 30)
"""When the regular session opens each trading day, in New York time: the S&P indices start
computing, and stocks and options open."""

EXTENDED_OPEN: Final = time(4, 0)
"""When the stock market's extended hours open each trading day, in New York time."""

EXTENDED_HOURS_FAMILIES: Final = frozenset({"trades", "quotes"})
"""The stream families whose stock rows flow in the extended hours (Massive's status
`extended-hours`): trades and quotes. A stream's family is the last part of its dataset's
name, such as `"trades"` for `stocks.trades`."""

MINUTE_BAR_LAG: Final = timedelta(seconds=15)
"""How long after its minute ends a test waits for a minute bar: Massive sends the bar once the
minute is over, within a few seconds."""


def index_session_open(now: datetime) -> datetime:
    """Return 09:30 New York time of the day of an instant, when the index session opens.

    Args:
        now: An aware instant.

    Returns:
        The open, in New York time.
    """
    return datetime.combine(now.astimezone(NEW_YORK).date(), INDEX_OPEN, NEW_YORK)


def opened_at(
    market: str, status: str, *, family: str, now: datetime, since: datetime | None = None
) -> datetime | None:
    """Return when the session a stream's rows come from opened, or None when it is not open.

    Args:
        market: The market, as the `market_status` fixture names it: `"stocks"`, `"options"`,
            `"indices"`, `"fx"`, `"crypto"`, or `"futures"`.
        status: Massive's status of that market: `"open"`, `"extended-hours"`, `"closed"`,
            and so on, or the futures `market_event`.
        family: The stream's family, the last part of its dataset's name, such as `"trades"`.
        now: The present, as an aware instant.
        since: When the market opened, where Massive's status says (the futures status's
            `timestamp`); None when it does not.

    Returns:
        For stock trades and quotes while the market is `open` or `extended-hours`, 04:00 New
        York time of `now`'s day; for every other stock, option, or index stream while it is
        `open`, 09:30; for another market that is `open`, `since`, or `now` when it is None;
        None in every other case.
    """
    if market == "stocks" and family in EXTENDED_HOURS_FAMILIES:
        if status in {"open", "extended-hours"}:
            return datetime.combine(now.astimezone(NEW_YORK).date(), EXTENDED_OPEN, NEW_YORK)
        return None
    if status != "open":
        return None
    if market in {"stocks", "options", "indices"}:
        return index_session_open(now)
    return now if since is None else since


def minute_bar_deadline(now: datetime) -> datetime:
    """Return when a minute bar stream must have sent a bar: just past the next minute's end.

    Args:
        now: An aware instant, when the stream's subscription was acknowledged.

    Returns:
        The next whole minute after `now` plus `MINUTE_BAR_LAG`: at most 75 seconds away.
    """
    minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return minute + MINUTE_BAR_LAG


def rows_due(plans: LivePlans, name: str, *, opened: datetime | None, now: datetime) -> bool:
    """Tell whether a class's stream must have delivered rows by now.

    Args:
        plans: The run's plans, whose class plan sets the feed's delay.
        name: The class, such as `"stocks"` or `"futures"`.
        opened: When the market opened, as an aware instant; None when it is not open.
        now: The present, as an aware instant.

    Returns:
        True when the market is open and has been open at least the plan's delay.
    """
    if opened is None:
        return False
    delay = FEED_DELAYS.get((name, plans.tiers[name]), timedelta(0))
    return now >= opened + delay
