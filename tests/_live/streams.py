"""When a live stream must deliver rows: an open session whose delayed feed has caught up.

A stream test asserts that Massive acknowledged its topics, whatever the hour, and requires
rows only when they must arrive. That needs the market open for the stream's kind of rows, and,
on a delayed plan, open for longer than the plan's delay: a 15-minute delayed index feed sends
the first values of a session fifteen minutes after its open. Stock trades and quotes are due in
the extended hours too, from 04:00 New York time, though one stock can go a minute without
either then, so a test of one stock requires them only in the regular session
(`may_go_quiet`). Every other stock stream, and every option
and index stream, is due only in the regular session, from
09:30 to its close at 16:00 (13:00 on a half trading day), whatever Massive's status says after
the close. The futures session opens at the time of Massive's futures market status, and forex
and crypto at no time this module knows, so their rows are due as soon as their market is open.
The delays are those of the vendored WebSocket "Plan Recency" tables (`FEED_DELAYS`). Before
the rows are due, the test skips after the acknowledgement, and a pass always means that rows
reached the table.

A minute bar arrives only after its minute has ended, so a test that streams minute bars waits
until a little past the next minute boundary (`minute_bar_deadline`).

Stock bars come from trades, so they flow in every stock session, as the README says:
pre-market from 04:00, the regular session from 09:30, and after hours from its close to 20:00
(17:00 on a half trading day). Massive sends a bar only for a minute with trades, though, and
one stock can go a minute without a trade outside the regular session, so a test that streams
one stock's bars requires them only in the regular session. `stock_session` names the session
a plan's feed is in, so a test of several stocks, such as the README's streaming examples, can
check what each session sends, an empty table overnight included.

Every function is pure; `tests/conftest.py`'s `rows_due` fixture reads the market status and
calls them.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Final

from tests._live.calendar import NEW_YORK, is_half_day, is_trading_day

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

REGULAR_CLOSE: Final = time(16, 0)
"""When the regular session closes on a full trading day, in New York time, for stocks,
options, and indices alike.

A stream that needs the regular session has no rows due from then on, even while Massive's
market status still says `open`: on 2026-11-09 `indicesGroups.s_and_p` read `open` at 17:53,
though I:SPX's last value came at 16:05. The S&P indices send their closing values for a few
minutes past 16:00, so a stream that starts before the close still gets values during its wait;
one that starts later is skipped, even on a delayed plan whose feed still lags behind the
close, because no value after the close is certain to come."""

HALF_DAY_CLOSE: Final = time(13, 0)
"""When the regular session closes on a half trading day, in New York time."""

EXTENDED_OPEN: Final = time(4, 0)
"""When the stock market's extended hours open each trading day, in New York time."""

EXTENDED_HOURS_FAMILIES: Final = frozenset({"trades", "quotes"})
"""The stream families whose stock rows are due in the extended hours (Massive's status
`extended-hours`): trades and quotes. Bars flow then too, but only for a minute with trades,
which one stock may lack outside the regular session. A stream's family is the last part of
its dataset's name, such as `"trades"` for `stocks.trades`."""

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


def regular_close(now: datetime) -> datetime:
    """Return when the regular session of the day of an instant closes, in New York time.

    Args:
        now: An aware instant.

    Returns:
        13:00 New York time of that day on a half trading day (`calendar.is_half_day`), 16:00
        on any other.
    """
    day = now.astimezone(NEW_YORK).date()
    return datetime.combine(day, HALF_DAY_CLOSE if is_half_day(day) else REGULAR_CLOSE, NEW_YORK)


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
        `open` and before the regular session's close (`regular_close`), 09:30; for another
        market that is `open`, `since`, or `now` when it is None; None in every other case.
    """
    if market == "stocks" and family in EXTENDED_HOURS_FAMILIES:
        if status in {"open", "extended-hours"}:
            return datetime.combine(now.astimezone(NEW_YORK).date(), EXTENDED_OPEN, NEW_YORK)
        return None
    if status != "open":
        return None
    if market in {"stocks", "options", "indices"}:
        return index_session_open(now) if now < regular_close(now) else None
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
    return now >= opened + feed_delay(plans, name)


def feed_delay(plans: LivePlans, name: str) -> timedelta:
    """Return how far a class's stream lags the market on the run's plan.

    Args:
        plans: The run's plans.
        name: The class, such as `"stocks"` or `"futures"`.

    Returns:
        The class plan's delay in `FEED_DELAYS`, zero on a real-time plan.
    """
    return FEED_DELAYS.get((name, plans.tiers[name]), timedelta(0))


EXTENDED_CLOSE: Final = time(20, 0)
"""When after-hours stock trading ends on a full trading day, in New York time."""

HALF_DAY_EXTENDED_CLOSE: Final = time(17, 0)
"""When after-hours stock trading ends on a half trading day, in New York time."""

PRE_MARKET: Final = "pre-market"
REGULAR: Final = "regular"
AFTER_HOURS: Final = "after-hours"
OVERNIGHT: Final = "overnight"
"""The stock sessions `stock_session` names; `OVERNIGHT` is any time with no session, a day the
market is closed included."""


def stock_session(now: datetime, delay: timedelta) -> str:
    """Name the stock session a feed delayed by `delay` is sending at `now`.

    Args:
        now: An aware instant.
        delay: The plan's feed delay (`FEED_DELAYS`), zero on a real-time plan.

    Returns:
        `PRE_MARKET` from 04:00 to 09:30 New York time, `REGULAR` until the close
        (`regular_close`), `AFTER_HOURS` until 20:00 (17:00 on a half trading day), and
        `OVERNIGHT` at any other time and on a day the market is closed, each of the feed's
        own time, `now` less the delay.
    """
    feed = (now - delay).astimezone(NEW_YORK)
    day = feed.date()
    if not is_trading_day(day):
        return OVERNIGHT
    clock = feed.time()
    late = HALF_DAY_EXTENDED_CLOSE if is_half_day(day) else EXTENDED_CLOSE
    if clock < EXTENDED_OPEN or clock >= late:
        return OVERNIGHT
    if clock < INDEX_OPEN:
        return PRE_MARKET
    return REGULAR if feed < regular_close(feed) else AFTER_HOURS


def may_go_quiet(market: str, family: str, *, now: datetime, delay: timedelta) -> bool:
    """Tell whether one symbol's stream may send no row in a test's wait, though rows are due.

    Stock trades and quotes are due in the extended hours (`EXTENDED_HOURS_FAMILIES`), but one
    stock trades and quotes far less often then than in the regular session, and can go a
    minute without either: on 2026-11-10 at 18:40 New York time, in the after-hours session,
    Massive acknowledged AAPL's quote stream and sent it no quote in 60 s, while AAPL's trade
    stream got one trade in the same minute. A test that streams one stock then skips when no
    row comes, and fails only in the regular session.

    Args:
        market: The market, as the `market_status` fixture names it, such as `"stocks"`.
        family: The stream's family, the last part of its dataset's name, such as `"quotes"`.
        now: The present, as an aware instant.
        delay: How far the stream's feed lags the market (`feed_delay`), zero when real-time.

    Returns:
        True for stock trades and quotes while the feed is in the pre-market or after hours
        (`stock_session`); False for every other stream, and in every other session.
    """
    if market != "stocks" or family not in EXTENDED_HOURS_FAMILIES:
        return False
    return stock_session(now, delay) in {PRE_MARKET, AFTER_HOURS}
