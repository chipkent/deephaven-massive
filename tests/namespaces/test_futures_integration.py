"""Live checks of futures data against Massive: sessions, bar labels, snapshots, and tickers.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, answers the question. Raw rows come from the wire REST client where
the library filters or relabels the very rows in question; the library's own answer is
recorded beside them. Every request names one contract or a few products. How to run these
tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import logging
import re
import time
from collections import Counter
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"

_CONTRACT = "ESH6"
"""The front E-mini S&P 500 contract in the first quarter of 2026."""

_HOLIDAY_SESSION = date(2026, 1, 20)
"""The session after Martin Luther King Day 2026."""

_PRODUCTS = ("ES", "NQ", "CL", "NG", "GC", "SI", "ZN", "ZC", "6E")
"""Products of all four venues whose schedules the weekday check reads."""

_SCHEDULE_LIMIT = 20_000
"""The most schedule events read; a result of exactly this many is cut short."""

_AGGS = "/futures/v1/aggs/{ticker}"
_SNAPSHOT = "/futures/v1/snapshot"
_CONTRACTS = "/futures/v1/contracts"
_SCHEDULES = "/futures/v1/schedules"

_CONTRACT_SHAPE = re.compile(r"[A-Z0-9]+?[FGHJKMNQUVXZ]\d{1,2}")
"""A single contract: a product code, a month letter, and a one- or two-digit year."""


def _page(client: RestClient, path: str, *, query: Mapping[str, object]) -> list[dict[str, Any]]:
    """Read the raw rows of the first page of a futures request.

    The path values are the query's `ticker` for a path with a `{ticker}` placeholder.

    Returns:
        The rows as Massive sent them. A refusal raises the wire error.
    """
    path_params = {"ticker": query["ticker"]} if "{ticker}" in path else {}
    rest = {key: value for key, value in query.items() if not path_params or key != "ticker"}
    return client.get(source(Transport.REST, path), path_params, rest).rows


def _aggregates(
    client: RestClient,
    resolution: str,
    *,
    start: datetime,
    end: datetime,
    evidence: Evidence,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Read the raw bars of the test contract at one resolution; record a refusal.

    Returns:
        The bars, oldest first; empty when Massive rejects the request (an unknown
        resolution), which is recorded as evidence.

    Raises:
        WireEntitlementError: When the request is refused for want of a plan.
    """
    query = {
        "ticker": _CONTRACT,
        "resolution": resolution,
        "window_start.gte": start,
        "window_start.lt": end,
        "sort": "window_start.asc",
        "limit": limit,
    }
    try:
        return _page(client, _AGGS, query=query)
    except WireEntitlementError:
        raise
    except WireError as error:
        evidence(f"{resolution} from {start:%Y-%m-%d}: {type(error).__name__}: {error}")
        return []


@pytest.mark.needs("futures:starter")
@pytest.mark.settles("23")
def test_where_the_futures_snapshot_carries_the_ticker(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Where the futures snapshot carries a contract's ticker.

    The schema nests the ticker and product code as `details.ticker` and
    `details.product_code`, while both documented samples send them at the top level, which
    the library reads (`Symbol` and `ProductCode`). Records the top-level `ticker` and
    `product_code` and the nested pair of three ES rows, and the library's `Symbol`. Reading
    it: top-level keys only, with `Symbol` filled, confirm the samples; nested keys alone mean
    the library must read those instead.
    """
    rows = _page(live_rest, _SNAPSHOT, query={"product_code": "ES", "limit": 3})
    for row in rows:
        details = row.get("details")
        evidence(
            f"snapshot row: top-level ticker={row.get('ticker')!r} "
            f"product_code={row.get('product_code')!r} "
            f"details.ticker={details.get('ticker') if isinstance(details, dict) else None!r} "
            "details.product_code="
            f"{details.get('product_code') if isinstance(details, dict) else None!r} "
            f"keys={sorted(row)}"
        )
    assert rows
    first = rows[0]
    nested = first.get("details")
    ticker = first.get("ticker") or (nested.get("ticker") if isinstance(nested, dict) else None)
    assert ticker, "no ticker anywhere in the first snapshot row"
    library = table_rows(live_session.futures.snapshots(str(ticker)).fetch())
    evidence(f"library snapshots({ticker}): Symbol {[r['Symbol'] for r in library]}")


@pytest.mark.needs("futures:basic")
@pytest.mark.settles("29")
@pytest.mark.requests(8)
@pytest.mark.timeout(100)
def test_the_session_after_a_holiday_and_year_candles(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """When the session after a holiday opens, and how year candles and their filters behave.

    Records the ES schedule around Martin Luther King Day 2026, the last minute bar before the
    2026-01-20 session and its first minute bars (read raw, from 2026-01-16), the library's
    minute bars of that session, and the 1-year candles of `ESH6` from 2025-01-01 and from
    mid-year 2025-06-15. Reading it: a session opening at or before the previous regular open,
    with its bars kept by the library, confirms the 4-day margin covers it; a 2025 candle
    returned for the mid-year filter means `window_start` filters match the candle's start
    (snap), its absence that they do not.
    """
    schedule = _page(
        live_rest,
        _SCHEDULES,
        query={
            "product_code": "ES",
            "session_end_date.gte": date(2026, 1, 15),
            "session_end_date.lte": date(2026, 1, 21),
            "limit": 100,
        },
    )
    for event in schedule:
        evidence(f"ES schedule: {event}")
    raw = _aggregates(
        live_rest,
        "1min",
        start=datetime(2026, 1, 16, tzinfo=UTC),
        end=datetime(2026, 1, 20, 12, tzinfo=UTC),
        evidence=evidence,
        limit=50_000,
    )
    session = [bar for bar in raw if bar.get("session_end_date") == _HOLIDAY_SESSION.isoformat()]
    before = [bar for bar in raw if bar.get("session_end_date") != _HOLIDAY_SESSION.isoformat()]
    if before:
        last = before[-1]
        evidence(
            f"last minute bar before the holiday session: "
            f"{instant_text(int(last['window_start']))} (session {last.get('session_end_date')})"
        )
    for bar in session[:3]:
        evidence(
            f"first minute bars of {_HOLIDAY_SESSION}: {instant_text(int(bar['window_start']))}"
        )
    minutes = table_rows(
        live_session.futures.bars(_CONTRACT).history(_HOLIDAY_SESSION, _HOLIDAY_SESSION)
    )
    evidence(f"library minute bars of {_HOLIDAY_SESSION}: {len(minutes)} rows")
    if minutes:
        evidence(f"    first {instant_text(minutes[0]['Timestamp'])} {minutes[0]}")
        evidence(f"    last {instant_text(minutes[-1]['Timestamp'])}")
    for start in (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 6, 15, tzinfo=UTC)):
        candles = _aggregates(
            live_rest, "1year", start=start, end=datetime(2027, 1, 1, tzinfo=UTC), evidence=evidence
        )
        shown = [
            (instant_text(int(bar["window_start"])), bar.get("session_end_date")) for bar in candles
        ]
        evidence(f"1year candles from {start:%Y-%m-%d}: {shown}")
    assert schedule
    assert session, f"no minute bar of the {_HOLIDAY_SESSION} session among {len(raw)} read"


@pytest.mark.needs("futures:basic")
@pytest.mark.settles("36")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_futures_sessions_end_only_on_weekdays(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether a futures session can end on a Saturday or Sunday.

    Reads every schedule event of March 2026 for nine products across the four venues and
    records the weekday count of `SessionEndDate` (ISO text) and every weekend session. Reading
    it: none confirms that requesting weekday sessions only is complete; any weekend session
    means the library's session iteration must include those days.
    """
    rows = table_rows(
        live_session.futures.schedules(
            product_code=list(_PRODUCTS),
            start=date(2026, 3, 1),
            end=date(2026, 3, 31),
            limit=_SCHEDULE_LIMIT,
        ).fetch()
    )
    ended = [
        (row["ProductCode"], date.fromisoformat(row["SessionEndDate"]))
        for row in rows
        if row["SessionEndDate"]
    ]
    days = Counter(day.strftime("%a") for _, day in ended)
    weekend = sorted({(product, day.isoformat()) for product, day in ended if day.weekday() >= 5})
    cut = " (cut short at the limit)" if len(rows) == _SCHEDULE_LIMIT else ""
    evidence(f"schedules 2026-03 of {_PRODUCTS}: {len(rows)} events{cut}")
    evidence(f"session end weekdays {dict(days)}")
    evidence(f"sessions ending on a Saturday or Sunday: {weekend[:50]}")
    assert rows


@pytest.mark.needs("futures:basic")
@pytest.mark.settles("40")
@pytest.mark.requests(10)
@pytest.mark.timeout(100)
def test_labels_and_alignment_of_futures_bars_of_several_sessions(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which session labels a bar of several sessions, and what the bars align to.

    Records, for `ESH6` in the first quarter of 2026, every 1-session bar's `window_start` and
    `session_end_date`, then 2-session bars requested from 2026-01-01 and from 2026-01-03 (one
    session later), and 2-month bars from 2026-01-01 and from 2026-02-01, side by side. Reading
    it: compare each 2-session bar's `session_end_date` with the 1-session list: its first or
    its last session is the label. Pairs that stay the same for both starts align to a calendar;
    pairs that shift with the start align to `{from}`, and dates cannot select them. A refused
    resolution is recorded and the rest still run.
    """
    end = datetime(2026, 3, 21, tzinfo=UTC)
    requests = (
        ("1session", datetime(2026, 1, 1, tzinfo=UTC)),
        ("2session", datetime(2026, 1, 1, tzinfo=UTC)),
        ("2session", datetime(2026, 1, 3, tzinfo=UTC)),
        ("1month", datetime(2026, 1, 1, tzinfo=UTC)),
        ("2month", datetime(2026, 1, 1, tzinfo=UTC)),
        ("2month", datetime(2026, 2, 1, tzinfo=UTC)),
    )
    shown = 0
    for resolution, start in requests:
        bars = _aggregates(live_rest, resolution, start=start, end=end, evidence=evidence)
        shown += len(bars)
        labels = [
            (instant_text(int(bar["window_start"])), bar.get("session_end_date")) for bar in bars
        ]
        evidence(
            f"{resolution} from {start:%Y-%m-%d}: {len(bars)} bars; "
            f"(window_start, session_end_date) {labels}"
        )
    library = table_rows(
        live_session.futures.bars(_CONTRACT, dm.Timespan.SESSION, multiplier=2).history(
            datetime(2026, 1, 1, tzinfo=UTC), end
        )
    )
    windows = [
        (instant_text(r["TimestampStart"]), instant_text(r["TimestampEnd"]), r["Date"])
        for r in library
    ]
    evidence(f"library 2-session bars kept by their own window (start, end, Date): {windows}")
    assert shown


@pytest.mark.needs("futures:basic")
@pytest.mark.settles("futures ticker shapes")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_shapes_of_futures_contract_tickers(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which shapes futures contract tickers take: one- or two-digit years, and spreads.

    Records one page of NG contracts with the tickers outside a product code, a month letter,
    and a one- or two-digit year, and one page of combo (spread) tickers. Reading it: every
    single contract inside that shape confirms the ticker grammar; anything else extends it,
    or is left to the handling of unknown symbols.
    """
    singles = _page(live_rest, _CONTRACTS, query={"product_code": "NG", "limit": 1000})
    combos = _page(live_rest, _CONTRACTS, query={"type": "combo", "limit": 50})
    tickers = sorted({str(row.get("ticker")) for row in singles})
    odd = [ticker for ticker in tickers if not _CONTRACT_SHAPE.fullmatch(ticker)]
    evidence(f"NG contracts: {len(tickers)} tickers, e.g. {tickers[:10]} ... {tickers[-10:]}")
    evidence(f"NG tickers outside letters, a month code, and a 1-2 digit year: {odd}")
    evidence(f"combo (spread) tickers: {sorted({str(row.get('ticker')) for row in combos})}")
    library = table_rows(live_session.futures.contracts(product_code="NG", limit=20).fetch())
    evidence(f"library contracts(NG): Ticker {[r['Ticker'] for r in library]}")
    assert tickers


_CHICAGO = ZoneInfo("America/Chicago")
_STREAM_WAIT = 60.0
"""Seconds a stream test waits for its first rows; futures trade nearly around the clock."""


def _chicago(nanos: int) -> str:
    """Write epoch nanoseconds as Chicago wall time with its weekday.

    Returns:
        Such as `"Mon 2026-03-02 16:45"`.
    """
    return f"{datetime.fromtimestamp(nanos / 1e9, UTC).astimezone(_CHICAGO):%a %Y-%m-%d %H:%M}"


@pytest.mark.needs("futures:basic")
@pytest.mark.settles("50")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_session_labels_around_the_roll_and_a_holiday(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Which session Massive labels rows with around 17:00 Chicago time and after a holiday.

    Records, per minute, the `Date` (Massive's `session_end_date`) of `ESH6` minute bars from
    15:30 to 17:30 Chicago time on Monday 2026-03-02, of `ZCK6` (CBOT, whose pre-open starts at
    16:45) over the same hour, and of `ESM6` around Good Friday 2026 (Thursday 2026-04-02 from
    16:00 Chicago time to Monday 2026-04-06 09:00), beside the session the 17:00 Chicago rule
    gives. Reading it: labels that change at 17:00 confirm the WebSocket rule; a label that
    changes at 16:45 for ZC, or Thursday evening rows labelled Monday, are what the schedule
    correction must catch.
    """
    windows = (
        (
            "ESH6",
            datetime(2026, 3, 2, 21, 30, tzinfo=UTC),
            datetime(2026, 3, 2, 23, 30, tzinfo=UTC),
        ),
        (
            "ZCK6",
            datetime(2026, 3, 2, 21, 30, tzinfo=UTC),
            datetime(2026, 3, 2, 23, 30, tzinfo=UTC),
        ),
        ("ESM6", datetime(2026, 4, 2, 21, 0, tzinfo=UTC), datetime(2026, 4, 6, 14, 0, tzinfo=UTC)),
    )
    found = 0
    for contract, start, end in windows:
        rows = table_rows(
            live_session.futures.bars(contract, dm.Timespan.MINUTE).history(start, end)
        )
        found += len(rows)
        changes: list[str] = []
        last = None
        for row in rows:
            label = row["Date"]
            if label != last:
                changes.append(f"{_chicago(row['TimestampStart'])} -> {label}")
                last = label
        evidence(f"{contract} {start:%Y-%m-%d}: {len(rows)} bars; label changes {changes}")
    assert found


@pytest.mark.needs("futures:starter")
@pytest.mark.settles("50", "51")
@pytest.mark.timeout(100)
def test_streamed_futures_dates_and_one_second_ends(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which session the library gives streamed futures rows, and how long a one-second bar is.

    Streams `ESH6` trades and one-second bars until their first rows arrive, and records each
    row's `Date`, the `m.streams()` errors (a schedule miss shows there), and the one-second
    bars' `TimestampStart` and `TimestampEnd`, with the WARNING the library logs when Massive's
    `e` is not the start plus one second. Reading it: no miss and one-second windows confirm the
    calendar and the documented length; the WARNING means Massive's bars span more, as its
    documented sample suggests, and the library keeps the later end.
    """
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    trades = live_session.futures.trades(_CONTRACT).append_only()
    seconds = live_session.futures.bars(_CONTRACT, dm.Timespan.SECOND).append_only()
    deadline = time.monotonic() + _STREAM_WAIT
    rows = wait_rows(live_session, trades, deadline)
    bars = wait_rows(live_session, seconds, deadline)
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(trades)
    live_session.release(seconds)
    evidence(f"trade Dates: {sorted({str(r['Date']) for r in rows})}")
    spans = [(r["TimestampStart"], (r["TimestampEnd"] - r["TimestampStart"]) / 1e9) for r in bars]
    evidence(f"one-second bars (start, end - start in s): {spans[:5]}")
    evidence(
        f"end warnings: {[r.getMessage() for r in caplog.records if 'one-second' in r.message]}"
    )
    assert rows or bars, "no futures row arrived in 60 s"
