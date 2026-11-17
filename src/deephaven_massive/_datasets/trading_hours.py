"""Trading hours: when each asset class's live feed is expected to send data.

A WebSocket connection that receives no frame while it holds acknowledged subscriptions is
worth a WARNING only when its market is trading; at night or at a weekend the silence is the
market's. `in_trading_hours` gives that gate, per asset class, from each market's regular hours:

- **Stocks, options, and indices**: weekdays, 09:30 to 16:00 New York time, the regular
  session. The pre-market, the after hours, and the overnight stock session (21:00 to 04:00)
  are left out, since many symbols trade nothing then.
- **Futures**: Sunday 17:00 to Friday 16:00 Chicago time, less the daily pause from 16:00 to
  17:00 (`futures_sessions`).
- **Forex**: Sunday 17:00 to Friday 17:00 New York time, the global interbank week.
- **Crypto**: always.

There is no holiday calendar: on a market holiday a class counts as trading, so the no-frames
WARNING says that a holiday explains it. The hours are conservative in the other direction:
any instant they include is one in which an active symbol normally trades.

Every function is pure and thread-safe.
"""

from __future__ import annotations

import calendar
from datetime import datetime, time
from typing import Final

from deephaven_massive._datasets.dates import NEW_YORK
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._enums import AssetClass

__all__ = ["in_trading_hours"]

_OPEN: Final = time(9, 30)
"""The New York time the regular session of stocks, options, and indices opens."""

_CLOSE: Final = time(16)
"""The New York time the regular session of stocks, options, and indices closes."""

_FUTURES_PAUSE: Final = (time(16), time(17))
"""The Chicago times between which futures pause each weekday: [16:00, 17:00)."""

_WEEK_EDGE: Final = time(17)
"""The local time a futures (Chicago) or forex (New York) week opens on Sunday and, for forex,
closes on Friday."""


def in_trading_hours(asset_class: AssetClass, moment: datetime) -> bool:
    """Tell whether an asset class's market is in its regular trading hours at an instant.

    Args:
        asset_class: The class of the feed.
        moment: The instant, a timezone-aware `datetime`.

    Returns:
        True when the class's market trades at `moment` by its regular hours, holidays
        ignored (see the module docstring for each class's hours).
    """
    if asset_class is AssetClass.CRYPTO:
        return True
    if asset_class is AssetClass.FUTURES:
        return _futures_trading(moment.astimezone(CHICAGO))
    local = moment.astimezone(NEW_YORK)
    if asset_class is AssetClass.FOREX:
        return _forex_trading(local)
    return local.weekday() < calendar.SATURDAY and _OPEN <= local.time() < _CLOSE


def _futures_trading(local: datetime) -> bool:
    """Tell whether futures trade at a Chicago time: Sunday 17:00 to Friday 16:00, less 16-17.

    Args:
        local: The instant in Chicago time.

    Returns:
        True inside the futures week and outside the daily pause.
    """
    day, clock = local.weekday(), local.time()
    opening = day == calendar.SUNDAY and clock < _WEEK_EDGE
    closed = (
        day == calendar.SATURDAY
        or opening
        or (day == calendar.FRIDAY and clock >= _FUTURES_PAUSE[0])
    )
    return not closed and not _FUTURES_PAUSE[0] <= clock < _FUTURES_PAUSE[1]


def _forex_trading(local: datetime) -> bool:
    """Tell whether forex trades at a New York time: Sunday 17:00 to Friday 17:00.

    Args:
        local: The instant in New York time.

    Returns:
        True inside the forex week.
    """
    day, clock = local.weekday(), local.time()
    if day == calendar.SUNDAY:
        return clock >= _WEEK_EDGE
    return day < calendar.FRIDAY or (day == calendar.FRIDAY and clock < _WEEK_EDGE)
