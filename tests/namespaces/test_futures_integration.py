"""Live checks of futures data against Massive: sessions, bar labels, snapshots, and tickers.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw rows come from the wire REST client
where the library filters or relabels the very rows in question; the library's own answer is
recorded beside them. Every request names one contract or one product.
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_CONTRACT = "ESH5"
"""The front E-mini S&P 500 contract in the first quarter of 2025."""

_HOLIDAY_SESSION = date(2025, 1, 21)
"""The session after Martin Luther King Day 2025."""

_AGGS = "/futures/v1/aggs/{ticker}"
_TRADES = "/futures/v1/trades/{ticker}"
_SNAPSHOT = "/futures/v1/snapshot"
_CONTRACTS = "/futures/v1/contracts"
_SCHEDULES = "/futures/v1/schedules"

_CONTRACT_SHAPE = re.compile(r"[A-Z0-9]+?[FGHJKMNQUVXZ]\d{1,2}")
"""A single contract: a product code, a month letter, and a one- or two-digit year."""


def _page(client: RestClient, path: str, *, query: Mapping[str, object]) -> list[dict[str, Any]]:
    """Read the raw rows of the first page of a futures request.

    The path values are the query's `ticker` for a path with a `{ticker}` placeholder.

    Returns:
        The rows as Massive sent them.
    """
    path_params = {"ticker": query["ticker"]} if "{ticker}" in path else {}
    rest = {key: value for key, value in query.items() if not path_params or key != "ticker"}
    return client.get(source(Transport.REST, path), path_params, rest).rows


def test_where_the_futures_snapshot_carries_the_ticker(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    rows = _page(live_rest, _SNAPSHOT, query={"product_code": "ES", "limit": 3})
    for row in rows:
        details = row.get("details")
        evidence(
            f"snapshot row: top-level ticker={row.get('ticker')!r} "
            f"details.ticker={details.get('ticker') if isinstance(details, dict) else None!r} "
            f"keys={sorted(row)}"
        )
    assert rows
    first = rows[0]
    nested = first.get("details")
    ticker = first.get("ticker") or (nested.get("ticker") if isinstance(nested, dict) else None)
    assert ticker, "no ticker anywhere in the first snapshot row"
    library = table_rows(live_session.futures.snapshots(str(ticker)).fetch())
    evidence(f"library snapshots({ticker}): Symbol {[r['Symbol'] for r in library]}")


@pytest.mark.timeout(110)
def test_the_session_after_a_holiday_and_year_candles(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    schedule = _page(
        live_rest,
        _SCHEDULES,
        query={
            "product_code": "ES",
            "session_end_date.gte": date(2025, 1, 16),
            "session_end_date.lte": date(2025, 1, 22),
            "limit": 100,
        },
    )
    for event in schedule:
        evidence(f"ES schedule: {event}")
    trades = _page(
        live_rest,
        _TRADES,
        query={
            "ticker": _CONTRACT,
            "session_end_date": _HOLIDAY_SESSION,
            "sort": "timestamp.asc",
            "limit": 5,
        },
    )
    for trade in trades:
        evidence(f"first trades of {_HOLIDAY_SESSION}: {instant_text(int(trade['timestamp']))}")
    minutes = table_rows(
        live_session.futures.bars(_CONTRACT).history(_HOLIDAY_SESSION, _HOLIDAY_SESSION)
    )
    evidence(f"library minute bars of {_HOLIDAY_SESSION}: {len(minutes)} rows")
    if minutes:
        evidence(f"    first {instant_text(minutes[0]['Timestamp'])} {minutes[0]}")
        evidence(f"    last {instant_text(minutes[-1]['Timestamp'])}")
    years = _page(
        live_rest,
        _AGGS,
        query={
            "ticker": _CONTRACT,
            "resolution": "1year",
            "window_start.gte": datetime(2023, 1, 1, tzinfo=UTC),
            "limit": 10,
        },
    )
    for bar in years:
        stamp = instant_text(int(bar["window_start"]))
        evidence(f"1year candle: window_start={stamp} session={bar.get('session_end_date')}")
    assert schedule
    assert trades


@pytest.mark.timeout(110)
def test_futures_sessions_end_only_on_weekdays(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    rows = table_rows(
        live_session.futures.schedules(
            start=date(2025, 3, 1), end=date(2025, 3, 31), limit=50_000
        ).fetch()
    )
    days = Counter(row["SessionEndDate"].strftime("%a") for row in rows if row["SessionEndDate"])
    weekend = sorted(
        {
            (row["ProductCode"], row["SessionEndDate"].isoformat())
            for row in rows
            if row["SessionEndDate"] and row["SessionEndDate"].weekday() >= 5
        }
    )
    evidence(f"schedules 2025-03: {len(rows)} events; session end weekdays {dict(days)}")
    evidence(f"sessions ending on a Saturday or Sunday: {weekend[:50]}")
    assert rows


@pytest.mark.timeout(110)
def test_labels_and_alignment_of_futures_bars_of_several_sessions(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    start, end = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 3, 22, tzinfo=UTC)
    shown = 0
    for resolution in ("1session", "2session", "1month", "2month"):
        bars = _page(
            live_rest,
            _AGGS,
            query={
                "ticker": _CONTRACT,
                "resolution": resolution,
                "window_start.gte": start,
                "window_start.lt": end,
                "sort": "window_start.asc",
                "limit": 200,
            },
        )
        shown += len(bars)
        labels = [
            (instant_text(int(bar["window_start"])), bar.get("session_end_date"))
            for bar in bars[:30]
        ]
        evidence(f"{resolution}: {len(bars)} bars; (window_start, session_end_date) {labels}")
    library = table_rows(
        live_session.futures.bars(_CONTRACT, dm.Timespan.SESSION, multiplier=2).history(start, end)
    )
    evidence(
        "library 2-session bars kept by their own window: "
        f"{[(instant_text(r['Timestamp']), r['SessionEndDate']) for r in library[:30]]}"
    )
    assert shown


@pytest.mark.timeout(110)
def test_shapes_of_futures_contract_tickers(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
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
