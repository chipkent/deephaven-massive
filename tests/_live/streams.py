"""When a live stream must deliver rows: an open session of the stream's market.

A stream test asserts that Massive acknowledged its topics, whatever the hour, and requires rows
only when they must arrive, which needs the market open for the stream's kind of rows. Stock
trades and quotes are due in every session Massive says the market trades in: the extended hours
from 04:00 New York time, and the overnight session from 21:00 once Massive's status says the
market trades then (`opened_at`). One stock can go a minute without either outside the regular
session, so a test of one stock requires them only in the regular session (`may_go_quiet`).
Every other stock stream, and every option and index stream, is due only in the regular session,
from 09:30 to its close at 16:00 (13:00 on a half trading day), whatever Massive's status says
after the close. The futures session opens at the time of Massive's futures market status, and
forex and crypto at no time this module knows, so their rows are due as soon as their market is
open. Before the rows are due, the test skips after the acknowledgement, and a pass always means
that rows reached the table.

Massive's delayed feed lags the market by an amount this module does not know: Massive decides
it, and a stream shows it in its rows' age. So the rows are due by the market's clock alone,
and a test of a subscription the owner lists in `delayed_subscriptions` skips, rather than
fails, when no row comes while rows are due.

A minute bar arrives only after its minute has ended, so a test that streams minute bars waits
until a little past the next minute boundary (`minute_bar_deadline`).

Stock bars come from trades, so they flow in every stock session, as the README says:
pre-market from 04:00, the regular session from 09:30, and after hours from its close to 20:00
(17:00 on a half trading day). Massive sends a bar only for a minute with trades, though, and
one stock can go a minute without a trade outside the regular session, so a test that streams
one stock's bars requires them only in the regular session. `stock_session` names the session
of a row's own time, so a test of several stocks, such as the README's streaming examples, can
check what each session sends.

The overnight session runs from 21:00 to 04:00 New York time on each night before a trading day,
once the consolidated tape trades 23 hours a day; the hour from 20:00 to 21:00 is its nightly
pause. `stock_session` names that window by the clock alone, since the clock cannot tell
whether the session has begun: whether the market trades in it is Massive's status to say
(`market_state`), so no launch date is written here.

Massive's statuses of `/v1/marketstatus/now` are a closed set to these tests (`MarketState`):
a value they do not know fails the test that read it (`market_state`), so a new overnight
status is added by name rather than taken for `closed`.

Every function is pure; `tests/conftest.py`'s `rows_due` fixture reads the market status and
calls them.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from enum import Enum
from typing import Final, assert_never

from tests._live.calendar import NEW_YORK, is_half_day, is_trading_day

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
one that starts later is skipped, even on a delayed feed that still lags behind the close,
because no value after the close is certain to come."""

HALF_DAY_CLOSE: Final = time(13, 0)
"""When the regular session closes on a half trading day, in New York time."""

EXTENDED_OPEN: Final = time(4, 0)
"""When the stock market's extended hours open each trading day, in New York time, and when
the overnight session before it closes."""

OVERNIGHT_OPEN: Final = time(21, 0)
"""When the overnight stock session opens, in New York time, on the evening before a trading
day: Massive's blog post of 2026-08-26 on the consolidated tape's 23-hour day gives 21:00 to
04:00, after a pause from 20:00 to 21:00."""


class MarketState(Enum):
    """A status `/v1/marketstatus/now` gives a market, as the tests know it by name.

    Massive's page for the endpoint lists no values, so this is what the tests have seen:
    `open` in the regular session, `extended-hours` before and after it, and `closed`.
    """

    OPEN = "open"
    EXTENDED_HOURS = "extended-hours"
    CLOSED = "closed"


def market_state(status: str) -> MarketState:
    """Read Massive's status of a market by name.

    Args:
        status: The value `/v1/marketstatus/now` gives the market, such as `market` for stocks.

    Returns:
        Its `MarketState`.

    Raises:
        ValueError: If the value is none the tests know, such as a new status Massive sends in
            the overnight stock session; the message names it, so it can be added here by name.
    """
    try:
        return MarketState(status)
    except ValueError:
        known = ", ".join(state.value for state in MarketState)
        raise ValueError(
            f"Massive's market status {status!r} is none the tests know ({known}): one entry "
            "for it in tests/_live/streams.py MarketState fixes every test that read it"
        ) from None


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
        status: Massive's status of that market, read by name (`market_state`), or for futures
            its `market_event`, which is due only when it reads `"open"`.
        family: The stream's family, the last part of its dataset's name, such as `"trades"`.
        now: The present, as an aware instant.
        since: When the market opened, where Massive's status says (the futures status's
            `timestamp`); None when it does not.

    Returns:
        When the stream's rows became due:

        - stock trades and quotes while the status is `open` or `extended-hours`: the open of
          the session `now` falls in (`session_open`: 04:00, not the overnight session's 21:00,
          even after an overnight session), or `now` when no session runs, since Massive's
          status says the market trades;
        - another stock, option, or index stream while `open` and before the regular
          session's close (`regular_close`): 09:30;
        - another market that is `open`: `since`, or `now` when it is None;
        - otherwise None.

    Raises:
        ValueError: If `status` is a status of `/v1/marketstatus/now` the tests do not know.
    """
    if market == "futures":  # Massive documents no values of `market_event`; only `open` is due
        return (now if since is None else since) if status == "open" else None
    state = market_state(status)
    if market == "stocks" and family in EXTENDED_HOURS_FAMILIES:
        match state:
            case MarketState.OPEN | MarketState.EXTENDED_HOURS:
                opened = session_open(now)
                # Massive says the market trades at a time no session runs (its status may lag
                # the after-hours close): its word stands, so the rows are due from now.
                return now if opened is None else opened
            case MarketState.CLOSED:
                return None
            case _ as unreachable:
                assert_never(unreachable)
    match state:
        case MarketState.OPEN:
            if market in {"stocks", "options", "indices"}:
                return index_session_open(now) if now < regular_close(now) else None
            return now if since is None else since
        case MarketState.EXTENDED_HOURS | MarketState.CLOSED:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def minute_bar_deadline(now: datetime) -> datetime:
    """Return when a minute bar stream must have sent a bar: just past the next minute's end.

    Args:
        now: An aware instant, when the stream's subscription was acknowledged.

    Returns:
        The next whole minute after `now` plus `MINUTE_BAR_LAG`: at most 75 seconds away.
    """
    minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return minute + MINUTE_BAR_LAG


EXTENDED_CLOSE: Final = time(20, 0)
"""When after-hours stock trading ends on a full trading day, in New York time, and the
overnight session's nightly pause begins."""

HALF_DAY_EXTENDED_CLOSE: Final = time(17, 0)
"""When after-hours stock trading ends on a half trading day, in New York time."""


class StockSession(Enum):
    """The stock session an instant falls in, by the New York clock and the exchanges' calendar.

    `OVERNIGHT` names the 21:00-to-04:00 window by the clock alone. Whether anything trades in
    it is Massive's status to say (`market_state`).
    """

    PRE_MARKET = "pre-market"
    REGULAR = "regular"
    AFTER_HOURS = "after-hours"
    OVERNIGHT = "overnight"
    """21:00 on the evening before a trading day to 04:00 of that day."""
    CLOSED = "closed"
    """Any time no session runs: a day the market is closed, the hours after the after-hours
    session before 21:00, and a night not followed by a trading day."""


def stock_session(at: datetime) -> StockSession:
    """Name the stock session an instant of the feed falls in.

    Args:
        at: An aware instant of the feed's own time: a row's timestamp, so a delayed feed is
            named by the time its rows carry, not the wall clock.

    Returns:
        `PRE_MARKET` from 04:00 to 09:30 New York time on a trading day, `REGULAR` until the
        close (`regular_close`), `AFTER_HOURS` until 20:00 (17:00 on a half trading day),
        `OVERNIGHT` from 21:00 to 04:00 on a night before a trading day, and `CLOSED` at any
        other time.
    """
    feed = at.astimezone(NEW_YORK)
    day, clock = feed.date(), feed.time()
    if clock < EXTENDED_OPEN:
        return StockSession.OVERNIGHT if is_trading_day(day) else StockSession.CLOSED
    if clock >= OVERNIGHT_OPEN:
        tomorrow = day + timedelta(days=1)
        return StockSession.OVERNIGHT if is_trading_day(tomorrow) else StockSession.CLOSED
    late = HALF_DAY_EXTENDED_CLOSE if is_half_day(day) else EXTENDED_CLOSE
    if not is_trading_day(day) or clock >= late:
        return StockSession.CLOSED
    if clock < INDEX_OPEN:
        return StockSession.PRE_MARKET
    return StockSession.REGULAR if feed < regular_close(feed) else StockSession.AFTER_HOURS


def session_open(now: datetime) -> datetime | None:
    """Return when the stock session of an instant opened, for trades and quotes.

    Args:
        now: An aware instant.

    Returns:
        04:00 New York time of `now`'s day in the pre-market, regular, and after-hours sessions,
        which trades and quotes run through without a break; in the overnight session, 21:00
        of the evening it began; None at a time no session runs.
    """
    wall = now.astimezone(NEW_YORK)
    match stock_session(now):
        case StockSession.PRE_MARKET | StockSession.REGULAR | StockSession.AFTER_HOURS:
            return datetime.combine(wall.date(), EXTENDED_OPEN, NEW_YORK)
        case StockSession.OVERNIGHT:
            evening = (
                wall.date() - timedelta(days=1) if wall.time() < EXTENDED_OPEN else wall.date()
            )
            return datetime.combine(evening, OVERNIGHT_OPEN, NEW_YORK)
        case StockSession.CLOSED:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def may_go_quiet(market: str, family: str, *, now: datetime) -> bool:
    """Tell whether one symbol's stream may send no row in a test's wait, though rows are due.

    Stock trades and quotes are due outside the regular session too (`EXTENDED_HOURS_FAMILIES`),
    but one stock trades and quotes far less often then, and can go a minute without either:
    on 2026-11-10 at 18:40 New York time, in the after-hours session, Massive acknowledged
    AAPL's quote stream and sent it no quote in 60 s, while AAPL's trade stream got one trade in
    the same minute. A test that streams one stock then
    skips when no row comes, and fails only in the regular session.

    Args:
        market: The market, as the `market_status` fixture names it, such as `"stocks"`.
        family: The stream's family, the last part of its dataset's name, such as `"quotes"`.
        now: The present, as an aware instant.

    Returns:
        True for stock trades and quotes while `now` is in the pre-market, the after hours, or
        the overnight session (`stock_session`); False for every other stream, and in the
        regular session or at a time no session runs.
    """
    if market != "stocks" or family not in EXTENDED_HOURS_FAMILIES:
        return False
    match stock_session(now):
        case StockSession.PRE_MARKET | StockSession.AFTER_HOURS | StockSession.OVERNIGHT:
            return True
        case StockSession.REGULAR | StockSession.CLOSED:
            return False
        case _ as unreachable:
            assert_never(unreachable)
