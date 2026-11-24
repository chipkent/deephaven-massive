"""Live checks of futures data against Massive: sessions, bar labels, snapshots, and tickers.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST client where the library filters or relabels the very rows in question; the
library's own answer is recorded beside them. Every request names one contract or a few products.
How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.epoch import epoch_ns
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)
from deephaven_massive.queries import StreamHistoryQuery
from tests._live import futures_bar_buckets
from tests._live.calendar import months_before
from tests._live.ticker_lists import accepts

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import TypeAlias

    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from tests._live.futures_bar_buckets import Bar
    from tests._live.ticker_lists import SpreadRead

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
SpreadReads: TypeAlias = "Callable[[Sequence[Mapping[str, Any]]], list[SpreadRead]]"
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
SpreadReport: TypeAlias = (
    "Callable[[Sequence[Mapping[str, Any]], Mapping[str, Sequence[Mapping[str, Any]]]], list[str]]"
)

_PRODUCTS = ("ES", "NQ", "CL", "NG", "GC", "SI", "ZN", "ZC", "6E")
"""Products of all four venues whose schedules the weekday check reads."""

_SCHEDULE_LIMIT = 20_000
"""The most schedule events read; a result of exactly this many is cut short."""

_AGGS = "/futures/v1/aggs/{ticker}"
_SNAPSHOT = "/futures/v1/snapshot"
_CONTRACTS = "/futures/v1/contracts"
_PRODUCTS_PATH = "/futures/v1/products"
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
    return client.get(source(Transport.REST, path), path_params, rest, bucket="futures").rows


def _aggregates(
    client: RestClient,
    contract: str,
    resolution: str,
    *,
    start: datetime,
    end: datetime,
    evidence: Evidence,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Read the raw bars of one contract at one resolution; record a rejection.

    Returns:
        The bars, oldest first; empty when Massive rejects the request (an unknown
        resolution), which is recorded as evidence.

    Raises:
        WireEntitlementError: When Massive refuses the key the request.
    """
    query = {
        "ticker": contract,
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


@pytest.mark.verifies("23")
def test_where_the_futures_snapshot_carries_the_ticker(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Where the futures snapshot carries a contract's ticker.

    The schema nests the ticker and product code as `details.ticker` and
    `details.product_code`, while both documented samples send them at the top level. The
    live run of 2026-11-02 found them nested, which the library now reads (`Symbol` and
    `ProductCode`). Records the top-level `ticker` and `product_code` and the nested
    pair of three ES rows, and the library's `Symbol`. Passes when every row nests its ticker
    and product code under `details` and the library fills `Symbol`; top-level keys alone mean
    the library must read those again.
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
    library = table_rows(live_session.futures.snapshots(str(ticker)).fetch(wait=True))
    evidence(f"library snapshots({ticker}): Symbol {[r['Symbol'] for r in library]}")
    flat = [row for row in rows if not isinstance(row.get("details"), dict)]
    unnested = [
        row
        for row in rows
        if not {"ticker", "product_code"} <= set(row.get("details") or {})
        or "ticker" in row
        or "product_code" in row
    ]
    assert not flat, f"rows without a details object: {flat}"
    assert not unnested, f"rows without the nested ticker and product code: {unnested}"
    assert library, f"the library returned no snapshot of {ticker}"
    assert all(r["Symbol"] == ticker for r in library), f"library rows {library}"


@pytest.mark.verifies("29")
@pytest.mark.requests(6)
@pytest.mark.timeout(100)
def test_the_session_after_a_holiday_and_year_candles(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    recent_holiday: Callable[[str], date],
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """When the session after a holiday opens, and how year candles and their filters behave.

    Takes the most recent Martin Luther King Day and the ES contract that was front then.
    Records the ES schedule around the holiday, the last minute bar before the session after
    it and that session's first minute bars (read raw, from the Friday before), the library's
    minute bars of that session, and the contract's 1-year candles from January 1 and from
    mid-June of the year before: 4 raw requests, and the library's read of about 2. Passes
    when the session after the holiday has minute bars and the library keeps them: the 4-day
    margin covers it. A candle of the year before returned for the mid-year filter means
    `window_start` filters match the candle's start (snap), its absence that they do not.
    """
    holiday = recent_holiday("martin luther king day")
    session_day = holiday + timedelta(days=1)
    contract = front_contract("ES", holiday)
    schedule = _page(
        live_rest,
        _SCHEDULES,
        query={
            "product_code": "ES",
            "session_end_date.gte": holiday - timedelta(days=5),
            "session_end_date.lte": session_day + timedelta(days=1),
            "limit": 100,
        },
    )
    for event in schedule:
        evidence(f"ES schedule: {event}")
    raw = _aggregates(
        live_rest,
        contract,
        "1min",
        start=_midnight(holiday - timedelta(days=3)),
        end=_midnight(session_day) + timedelta(hours=12),
        evidence=evidence,
        limit=50_000,
    )
    session = [bar for bar in raw if bar.get("session_end_date") == session_day.isoformat()]
    before = [bar for bar in raw if bar.get("session_end_date") != session_day.isoformat()]
    if before:
        last = before[-1]
        evidence(
            f"{contract} last minute bar before the holiday session: "
            f"{instant_text(int(last['window_start']))} (session {last.get('session_end_date')})"
        )
    for bar in session[:3]:
        evidence(f"first minute bars of {session_day}: {instant_text(int(bar['window_start']))}")
    minutes = table_rows(
        live_session.futures.bars(contract, dm.Timespan.MINUTE).history(
            session_day, session_day, wait=True
        )
    )
    evidence(f"library minute bars of {session_day}: {len(minutes)} rows")
    if minutes:
        evidence(f"    first {instant_text(minutes[0]['TimestampStart'])} {minutes[0]}")
        evidence(f"    last {instant_text(minutes[-1]['TimestampStart'])}")
    assert minutes, f"the library kept no minute bar of the {session_day} session"
    year = holiday.year - 1
    for start in (date(year, 1, 1), date(year, 6, 15)):
        candles = _aggregates(
            live_rest,
            contract,
            "1year",
            start=_midnight(start),
            end=_midnight(date(year + 2, 1, 1)),
            evidence=evidence,
        )
        shown = [
            (instant_text(int(bar["window_start"])), bar.get("session_end_date")) for bar in candles
        ]
        evidence(f"1year candles from {start}: {shown}")
    assert schedule
    assert session, f"no minute bar of the {session_day} session among {len(raw)} read"


@pytest.mark.verifies("36")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_futures_sessions_end_only_on_weekdays(
    live_session: dm.MassiveSession,
    *,
    recent_month: tuple[date, date],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether a futures session can end on a Saturday or Sunday.

    Reads every schedule event of the most recent whole month for nine products across the
    four venues and records the weekday count of `SessionEndDate` (ISO text) and every weekend
    session. Passes when no session ends on a weekend, so requesting weekday sessions only is
    complete; a weekend session means the library's session iteration must include those days.
    """
    start, end = recent_month
    rows = table_rows(
        live_session.futures.schedules(
            product_code=list(_PRODUCTS), start=start, end=end, limit=_SCHEDULE_LIMIT
        ).fetch(wait=True)
    )
    ended = [
        (row["ProductCode"], date.fromisoformat(row["SessionEndDate"]))
        for row in rows
        if row["SessionEndDate"]
    ]
    days = Counter(day.strftime("%a") for _, day in ended)
    weekend = sorted({(product, day.isoformat()) for product, day in ended if day.weekday() >= 5})
    cut = " (cut short at the limit)" if len(rows) == _SCHEDULE_LIMIT else ""
    evidence(f"schedules {start}..{end} of {_PRODUCTS}: {len(rows)} events{cut}")
    evidence(f"session end weekdays {dict(days)}")
    evidence(f"sessions ending on a Saturday or Sunday: {weekend[:50]}")
    assert rows
    assert not weekend, f"sessions ending on a Saturday or Sunday: {weekend[:50]}"


@pytest.mark.settles("40")
@pytest.mark.requests(8)
@pytest.mark.timeout(100)
def test_labels_and_alignment_of_futures_bars_of_several_sessions(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    recent_month: tuple[date, date],
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which session labels a bar of several sessions, and what the bars align to.

    Reads the three whole months ending with the most recent one, for the ES contract that
    was front at their end (it trades through all three). Records every 1-session bar's
    `window_start` and `session_end_date`, then 2-session bars requested from the first day
    and from one day later (a shift of two days lands on the same 2-day grid whichever way the
    bars align, so it cannot tell the two apart), and 2-month bars from the first day and from
    a month later, side by side. Reading it: compare each 2-session bar's `session_end_date`
    with the 1-session list: its first or its last session is the label. Pairs that stay the
    same for both starts align to a calendar; pairs that shift with the start align to
    `{from}`, and dates cannot select them. A refused resolution is recorded and the rest still
    run. Then records the library's 2-session bars of the same months: 6 raw requests, and
    the library's read of about 2.
    """
    contract = front_contract("ES", recent_month[1])
    first = recent_month[0].replace(day=1)
    for _ in range(2):
        first = (first - timedelta(days=1)).replace(day=1)
    start = _midnight(first)
    end = _midnight(recent_month[1] + timedelta(days=1))
    later_month = _midnight((first + timedelta(days=32)).replace(day=1))
    requests = (
        ("1session", start),
        ("2session", start),
        ("2session", start + timedelta(days=1)),
        ("1month", start),
        ("2month", start),
        ("2month", later_month),
    )
    shown = 0
    for resolution, since in requests:
        bars = _aggregates(live_rest, contract, resolution, start=since, end=end, evidence=evidence)
        shown += len(bars)
        labels = [
            (instant_text(int(bar["window_start"])), bar.get("session_end_date")) for bar in bars
        ]
        evidence(
            f"{contract} {resolution} from {since:%Y-%m-%d}: {len(bars)} bars; "
            f"(window_start, session_end_date) {labels}"
        )
    library = table_rows(
        live_session.futures.bars(contract, dm.Timespan.SESSION, multiplier=2).history(
            start, end, wait=True
        )
    )
    windows = [
        (instant_text(r["TimestampStart"]), instant_text(r["TimestampEnd"]), r["Date"])
        for r in library
    ]
    evidence(f"library 2-session bars kept by their own window (start, end, Date): {windows}")
    assert shown


@pytest.mark.settles("107")
@pytest.mark.requests(5)
@pytest.mark.timeout(100)
def test_futures_day_bars_against_session_bars(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    recent_month: tuple[date, date],
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether Massive's `day` bars of futures are its `session` bars, at 1 and 5 units.

    Reads the most recent whole month of the ES contract that was front at its end, as
    `1day`, `1session`, `5day`, and `5session` bars, and records for each pair whether the
    bars are equal field by field, and the first pair that differs. Then reads the month's
    `dm.Timespan.DAY` bars through the library, which dates and keeps them as 1-session bars,
    and records their windows and `Date` beside the raw `1session` labels. Reading it: equal
    pairs at both multipliers, and library windows that end on each bar's session, settle that
    `DAY` is `SESSION` for futures; any difference means `DAY` needs its own rules.
    """
    contract = front_contract("ES", recent_month[1])
    start = _midnight(recent_month[0])
    end = _midnight(recent_month[1] + timedelta(days=1))
    read = {
        resolution: _aggregates(
            live_rest, contract, resolution, start=start, end=end, evidence=evidence
        )
        for resolution in ("1day", "1session", "5day", "5session")
    }
    for days, sessions in (("1day", "1session"), ("5day", "5session")):
        pairs = list(zip(read[days], read[sessions], strict=False))
        differ = next(((a, b) for a, b in pairs if a != b), None)
        evidence(
            f"{contract} {days} {len(read[days])} bars, {sessions} {len(read[sessions])} bars; "
            f"equal: {len(read[days]) == len(read[sessions]) and differ is None}; first "
            f"difference: {differ}"
        )
    labels = [
        (instant_text(int(bar["window_start"])), bar.get("session_end_date"))
        for bar in read["1session"]
    ]
    evidence(f"{contract} 1session (window_start, session_end_date): {labels}")
    library = table_rows(
        live_session.futures.bars(contract, dm.Timespan.DAY).history(
            recent_month[0], recent_month[1], wait=True
        )
    )
    windows = [
        (instant_text(r["TimestampStart"]), instant_text(r["TimestampEnd"]), r["Date"])
        for r in library
    ]
    evidence(f"library DAY bars (start, end, Date): {windows}")
    assert read["1day"]
    assert library


@pytest.mark.verifies("47")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_shapes_of_futures_contract_tickers(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which shapes single futures contract tickers take: one- or two-digit years.

    Records one page of NG single contracts (`type=single`: without it, the page held only
    spreads) with the tickers outside a product code, a month letter, and a one- or two-digit
    year, and the library's `contracts` of NG single contracts with each row's ticker and date.
    Passes when every single contract fits that shape, the library's ticker grammar (a year
    is never rewritten, so `ESZ25` is not `ESZ5`); anything else extends it, or is left to the
    handling of unknown symbols. Spread tickers are the next test's.

    Without a `date`, Massive's page held few distinct tickers among many rows, every row a
    distinct `(ticker, date)` pair: each is the contract on another day (its point-in-time
    history), not a repeat. The library reads 20 rows in one page, so it repeats no page.
    """
    singles = _page(
        live_rest, _CONTRACTS, query={"product_code": "NG", "type": "single", "limit": 1000}
    )
    tickers = sorted({str(row.get("ticker")) for row in singles})
    days = {(str(row.get("ticker")), str(row.get("date"))) for row in singles}
    odd = [ticker for ticker in tickers if not _CONTRACT_SHAPE.fullmatch(ticker)]
    evidence(f"NG contracts: {len(tickers)} tickers, e.g. {tickers[:10]} ... {tickers[-10:]}")
    evidence(f"NG page: {len(singles)} rows, {len(days)} distinct (ticker, date) pairs")
    evidence(f"NG tickers outside letters, a month code, and a 1-2 digit year: {odd}")
    library = table_rows(
        live_session.futures.contracts(
            product_code="NG", limit=20, filters={"type": "single"}
        ).fetch(wait=True)
    )
    evidence(f"library contracts(NG): (Symbol, Date) {[(r['Symbol'], r['Date']) for r in library]}")
    assert tickers
    assert not odd, f"NG single contracts outside the ticker grammar: {odd}"


_ES_SINGLE = re.compile(r"ES(?P<month>[FGHJKMNQUVXZ])(?P<year>\d{1,2})")
"""An ES single contract: the product code, a month letter, and a one- or two-digit year."""


@pytest.mark.verifies("72")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_how_massive_writes_a_contracts_year(
    live_rest: RestClient, *, recent_monday: date, evidence: Evidence
) -> None:
    """Whether Massive writes a futures contract's year with one digit or two, and answers both.

    Reads the ES single contracts listed on the most recent published Monday (`date`, a
    weekday that has point-in-time rows whenever the run is; without it, the page of
    1,000 rows holds the daily history of only a few contracts) and records each distinct
    ticker with its last trade date, grouped by the number of digits of its year. Then asks for
    the first one-digit contract under its two-digit spelling (`ESZ6` as `ESZ26`), and the
    first two-digit one under its one-digit spelling, and records how many rows each answer
    holds. Passes when Massive answers neither other spelling: the library never rewrites a
    year, so a caller who writes `ESZ25` for Massive's `ESZ5` gets no rows, and a spelling
    Massive answered would mean the library should rewrite it.
    """
    listed_on = recent_monday
    rows = _page(
        live_rest,
        _CONTRACTS,
        query={"product_code": "ES", "type": "single", "date": listed_on, "limit": 1000},
    )
    expiries = sorted({(str(r.get("ticker")), str(r.get("last_trade_date"))) for r in rows})
    by_digits: dict[int, list[tuple[str, str]]] = {}
    for ticker, expiry in expiries:
        shape = _ES_SINGLE.fullmatch(ticker)
        by_digits.setdefault(0 if shape is None else len(shape["year"]), []).append(
            (ticker, expiry)
        )
    for digits, contracts in sorted(by_digits.items()):
        evidence(f"ES contracts with a {digits}-digit year (0: another shape): {contracts}")
    answered: dict[str, int] = {}
    for digits, contracts in sorted(by_digits.items()):
        if digits == 0:
            continue
        ticker, expiry = contracts[0]
        shape = _ES_SINGLE.fullmatch(ticker)
        assert shape is not None
        year = expiry[2:4] if digits == 1 else expiry[3]
        other = f"ES{shape['month']}{year}"
        query = {"ticker": other, "date": listed_on, "limit": 10}
        answer = _page(live_rest, _CONTRACTS, query=query)
        evidence(f"{ticker} (last trade {expiry}) asked for as {other}: {len(answer)} rows")
        answered[other] = len(answer)
    assert rows, "Massive listed no ES contract"
    assert not any(answered.values()), f"Massive answered another spelling: {answered}"


@pytest.mark.verifies("48")
@pytest.mark.requests(12)
@pytest.mark.timeout(100)
def test_forms_of_futures_spread_tickers_on_every_venue(
    live_rest: RestClient,
    *,
    futures_spread_reads: SpreadReads,
    futures_spread_report: SpreadReport,
    evidence: Evidence,
) -> None:
    """Which forms futures spread (combo) tickers take on each venue, and how they name their legs.

    Massive documents no grammar for spreads; the library accepts any combo shape (runs of
    upper-case letters and digits joined by single `-`, `:`, or spaces, such as `GCQ6-GCZ6`,
    `BOB 02-01 U6`, `1Y:SA 03M F6`, `10YF5-10YG5XXX`) and
    builds no spread from its legs. Reads every combo product (`/futures/v1/products?type=combo`,
    one page of up to 50,000), then, for each of the four exchanges (CME, CBOT, NYMEX,
    COMEX), one page of up to 1,000 combo contracts (`/futures/v1/contracts?type=combo`) of
    two of its single products (whose calendar spreads may carry their legs' product code)
    and up to 20 of its combo products, taken in turn from each strategy type, and the same
    of any other venue the products name: one request for the products and one per venue.

    Records each venue's combo products by strategy type, and, per venue read, every ticker
    form (each run of letters written `A`, each run of digits `9`) with its count, how many
    of its tickers `canonical_symbol_future_spread` accepts, and examples with their contract
    name, product code, and the legs written out whole in the ticker; then every ticker the
    library refuses. Passes when the library accepts every combo ticker read: a refused form
    must join the grammar (or stay a ticker kept as written). The names and legs say whether
    a builder from legs could produce each form.
    """
    products = _page(live_rest, _PRODUCTS_PATH, query={"type": "combo", "limit": 50_000})
    reads = futures_spread_reads(products)
    contracts = {
        read.venue: _page(
            live_rest,
            _CONTRACTS,
            query={"type": "combo", "product_code.any_of": list(read.product_codes), "limit": 1000},
        )
        for read in reads
    }
    for read in reads:
        evidence(f"{read.venue} combo contracts read for products {list(read.product_codes)}")
    for line in futures_spread_report(products, contracts):
        evidence(line)
    assert products, "Massive listed no combo product"
    refused = sorted(
        {
            str(row["ticker"])
            for rows in contracts.values()
            for row in rows
            if row.get("ticker") and not accepts(dm.canonical_symbol_future_spread, row["ticker"])
        }
    )
    assert not refused, f"combo tickers the library refuses: {refused[:20]}"


_CHICAGO = ZoneInfo("America/Chicago")
_STREAM_WAIT = 60.0
"""Seconds a stream test waits for its first rows; futures trade nearly around the clock."""


def _chicago(nanos: int) -> str:
    """Write epoch nanoseconds as Chicago wall time with its weekday.

    Returns:
        Such as `"Mon 2026-03-02 16:45"`.
    """
    return f"{datetime.fromtimestamp(nanos / 1e9, UTC).astimezone(_CHICAGO):%a %Y-%m-%d %H:%M}"


def _label_changes(rows: list[dict[str, Any]], time_column: str) -> list[str]:
    """List where the session label (`Date`) of time-ordered rows changes.

    Args:
        rows: The rows, oldest first.
        time_column: The column of each row's time, such as `TimestampStart` for a bar.

    Returns:
        One entry per change, the first row's included, such as
        `"Mon 2026-10-26 17:00 -> 2026-10-27"`.
    """
    changes: list[str] = []
    last = None
    for row in rows:
        label = row["Date"]
        if label != last:
            changes.append(f"{_chicago(row[time_column])} -> {label}")
            last = label
    return changes


@pytest.mark.settles("58")
@pytest.mark.requests(5)
@pytest.mark.timeout(100)
def test_session_labels_around_the_roll_and_a_holiday(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    recent_holiday: Callable[[str], date],
    front_contract: Callable[[str, date], str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which session Massive labels rows with around 17:00 Chicago time and after a holiday.

    Records, per minute, the `Date` (Massive's `session_end_date`) of the front ES contract's
    minute bars from 15:30 to 17:30 Chicago time on the most recent published Monday, of the
    front corn contract's quotes and trades (CBOT, whose pre-open starts at 16:45 and whose
    session opens at 19:00, so only quotes fall in the pre-open) from 16:00 to 19:30, and of the
    ES contract front around the most recent Good Friday (from 16:00 Chicago time on the
    Thursday before to 09:00 on the Monday after), beside the session the 17:00 Chicago rule
    gives. Reading it: labels that change at 17:00 confirm the WebSocket rule; a label that
    changes at 16:45 for ZC, or Thursday evening rows labelled Monday, are what the schedule
    correction must catch.
    """
    monday = datetime(recent_monday.year, recent_monday.month, recent_monday.day, tzinfo=_CHICAGO)
    good_friday = recent_holiday("good friday")
    thursday = good_friday - timedelta(days=1)
    holiday_start = datetime(thursday.year, thursday.month, thursday.day, 16, tzinfo=_CHICAGO)
    holiday_end = holiday_start + timedelta(days=4, hours=-7)
    es_start = monday + timedelta(hours=15, minutes=30)
    found = 0
    for contract, start, end in (
        (front_contract("ES", recent_monday), es_start, es_start + timedelta(hours=2)),
        (front_contract("ES", good_friday), holiday_start, holiday_end),
    ):
        bars = live_session.futures.bars(contract, dm.Timespan.MINUTE).history(
            start, end, wait=True
        )
        rows = table_rows(bars)
        found += len(rows)
        changes = _label_changes(rows, "TimestampStart")
        evidence(f"{contract} {start:%Y-%m-%d}: {len(rows)} bars; label changes {changes}")
    _record_corn_labels(
        live_session,
        front_contract("ZC", recent_monday),
        monday,
        table_rows=table_rows,
        evidence=evidence,
    )
    assert found


def _record_corn_labels(
    live_session: dm.MassiveSession,
    corn: str,
    monday: datetime,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Record the session labels of a corn contract's quotes and trades, 16:00 to 19:30 Chicago.

    CBOT grains trade from 19:00, so only quotes fall in the 16:45 pre-open; their labels from
    16:45 to 19:00 are recorded on a line of their own, and the trades are the control from the
    open. Each read Massive refuses is recorded with Massive's refusal, not raised.

    Args:
        live_session: The session that reads.
        corn: The front corn contract, such as `"ZCZ6"`.
        monday: Midnight of the Monday read, in Chicago time.
        table_rows: Reads a table's rows.
        evidence: Records a line.
    """
    start = monday + timedelta(hours=16)
    end = start + timedelta(hours=3, minutes=30)
    pre_open = (monday + timedelta(hours=16, minutes=45), monday + timedelta(hours=19))
    reads = (("quotes", live_session.futures.quotes), ("trades", live_session.futures.trades))
    for kind, read in reads:
        try:
            rows = table_rows(read(corn).history(start, end, wait=True))
        except dm.EntitlementError as error:
            evidence(f"{corn} {kind} {start:%Y-%m-%d}: refused, {error}")
            continue
        changes = _label_changes(rows, "Timestamp")
        evidence(f"{corn} {start:%Y-%m-%d}: {len(rows)} {kind}; label changes {changes}")
        if kind == "quotes":
            first, last = (int(t.timestamp() * 1e9) for t in pre_open)
            labels = sorted({str(r["Date"]) for r in rows if first <= r["Timestamp"] < last})
            evidence(f"{corn} quotes 16:45 to 19:00 Chicago: labels {labels}")


@pytest.mark.settles("58")
@pytest.mark.verifies("59")
@pytest.mark.timeout(160)
def test_streamed_futures_dates_and_one_second_ends(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    live_config: dm.MassiveConfig,
    front_contract: Callable[[str, date], str],
    wait_rows: WaitRows,
    wait_acked: Callable[[dm.MassiveSession, int, float], int],
    rows_due: Callable[[str], tuple[str, bool]],
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    stream_report: Report,
    evidence: Evidence,
) -> None:
    """Which session the library gives streamed futures bars, and how long a one-second bar is.

    Streams the front ES contract's minute and one-second bars, and asserts that Massive
    acknowledged both topics, whatever the hour. Rows are required only while ES is `open`, by
    `/futures/v1/market-status`; otherwise the test skips after the acknowledgement. The delayed
    feed lags by an amount the tests do not know, so when the owner's settings list futures as
    delayed, no bar skips instead of failing. Records each row's `Date`, the `m.streams()` errors (a
    schedule miss shows there), and the one-second bars' `TimestampStart` and `TimestampEnd`, with
    the WARNING the library logs when Massive's `e` is not the start plus one second. Passes when
    every one-second bar spans one second: `TimestampEnd` is Massive's `e` as sent, so another span
    means Massive's bars span that, as its documented sample (60 seconds) suggests. Reading the
    rest: no miss confirms the calendar. A refused stream skips, quoting Massive.
    """
    caplog.set_level(logging.WARNING, logger="deephaven_massive")
    contract = front_contract("ES", datetime.now(_CHICAGO).date())
    minute_bars = live_session.futures.bars(contract, dm.Timespan.MINUTE)
    second_bars = live_session.futures.bars(contract, dm.Timespan.SECOND)
    assert isinstance(minute_bars, StreamHistoryQuery)  # 1-minute bars stream
    assert isinstance(second_bars, StreamHistoryQuery)  # 1-second bars stream
    minutes = minute_bars.append_only()
    seconds = second_bars.append_only()
    acked = wait_acked(live_session, 2, time.monotonic() + _STREAM_WAIT)
    status, due = rows_due("futures")
    deadline = time.monotonic() + _STREAM_WAIT
    rows = wait_rows(live_session, minutes, deadline) if due else []
    bars = wait_rows(live_session, seconds, deadline) if due else []
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(minutes)
    live_session.release(seconds)
    evidence(f"{contract} minute-bar Dates: {sorted({str(r['Date']) for r in rows})}")
    spans = [(r["TimestampStart"], (r["TimestampEnd"] - r["TimestampStart"]) / 1e9) for r in bars]
    evidence(f"one-second bars (start, end - start in s): {spans[:5]}")
    warned = [r.getMessage() for r in caplog.records if "start plus its length" in r.message]
    evidence(f"end warnings: {warned}")
    skip_if_not_entitled(live_session)
    assert acked >= 2, f"Massive acknowledged {acked} of the 2 topics"
    if not due:
        pytest.skip(f"no ES bar is due: ES is {status}")
    if not (rows or bars) and dm.Subscription.FUTURES in live_config.delayed_subscriptions:
        pytest.skip(f"no bar on the delayed feed in {_STREAM_WAIT:.0f} s, though ES is {status}")
    assert rows or bars, f"no futures bar arrived in {_STREAM_WAIT:.0f} s, though one was due"
    assert bars, f"no one-second bar arrived in {_STREAM_WAIT:.0f} s, though one was due"
    longer = [span for span in spans if span[1] != 1.0]
    assert not longer, f"one-second bars spanning other than a second: {longer[:5]}"


def _midnight(day: date) -> datetime:
    """Return 00:00 UTC of a day.

    Returns:
        The instant.
    """
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


_BUCKET_PRODUCTS = ("ES", "ZC", "LE", "MBT")
"""Products whose sessions open at different hours: ES at 17:00 Chicago time, corn at 19:00
(after midnight UTC), and live cattle at 08:30, a day session; and Micro Bitcoin, whose market
trades a cryptocurrency that never closes, to show whether any session opens on a Friday
evening or a Saturday."""

_ALL_ROWS = 50_000
"""The largest page Massive serves; `rest_rows` follows the pages beyond it."""


def _bars(
    rest_rows: RestRows, contract: str, resolution: str, *, since: datetime, end: datetime
) -> list[Bar]:
    """Read every bar of one contract and resolution, from `since` (the `{from}`) to `end`.

    Returns:
        The bars, oldest first.
    """
    query = {
        "resolution": resolution,
        "window_start.gte": since,
        "window_start.lt": end,
        "sort": "window_start.asc",
        "limit": _ALL_ROWS,
    }
    rows = rest_rows(_AGGS, {"ticker": contract}, query)
    return [futures_bar_buckets.bar_of(row) for row in rows]


def _judge(
    contract: str,
    resolution: str,
    bars: Sequence[Bar],
    *,
    inputs: futures_bar_buckets.BucketInputs,
    since: datetime,
    evidence: Evidence,
) -> str:
    """Check every bar of one read against its bucket's contents; record each and the verdict.

    Returns:
        The verdict line (`bucket_verdict`), labelled with the read's `{from}`.
    """
    read_from = futures_bar_buckets.utc_midnight_ns(since.date())
    verdicts = []
    for bar in bars:
        line, found = futures_bar_buckets.bucket_check(
            resolution, bar, inputs, read_from_ns=read_from
        )
        evidence(f"{contract} from {since:%Y-%m-%d} {line}")
        verdicts.append(found)
    verdict = futures_bar_buckets.bucket_verdict(resolution, verdicts)
    return f"{verdict} (read from {since:%Y-%m-%d})"


def _shorter_bars(
    rest_rows: RestRows, contract: str, first: date, *, end: datetime, evidence: Evidence
) -> futures_bar_buckets.BucketInputs:
    """Read the 1-session and minute bars a contract's longer bars are checked against.

    The 1-session bars run from two months before the month that starts on `first`, so a
    2-month bucket holding it is covered; the minute bars from a week before it. Both end at
    `end`.

    Returns:
        The bars, and the spans they cover.
    """
    sessions_from = _midnight(months_before(first, 2))
    minutes_from = _midnight(first - timedelta(days=7))
    sessions = _bars(rest_rows, contract, "1session", since=sessions_from, end=end)
    minutes = _bars(rest_rows, contract, "1min", since=minutes_from, end=end)
    evidence(f"{contract}: {len(sessions)} 1-session bars, {len(minutes)} minute bars")
    return futures_bar_buckets.BucketInputs(
        sessions=sessions,
        sessions_from_ns=futures_bar_buckets.utc_midnight_ns(sessions_from.date()),
        minutes=minutes,
        minutes_from_ns=futures_bar_buckets.utc_midnight_ns(minutes_from.date()),
        until_ns=min(futures_bar_buckets.utc_midnight_ns(end.date()), time.time_ns()),
    )


def _grid(
    rest_rows: RestRows,
    contract: str,
    inputs: futures_bar_buckets.BucketInputs,
    *,
    first: date,
    end: datetime,
    evidence: Evidence,
) -> list[str]:
    """Read 2-session bars from an even and an odd epoch day; check each and compare the reads.

    Returns:
        The verdict lines: each read's bucket checks, and the grid verdict.
    """
    even, odd = futures_bar_buckets.even_and_odd_days(first)
    reads, verdicts = {}, []
    for day in (even, odd):
        since = _midnight(day)
        reads[day] = _bars(rest_rows, contract, "2session", since=since, end=end)
        verdicts.append(
            _judge(contract, "2session", reads[day], inputs=inputs, since=since, evidence=evidence)
        )
    after = futures_bar_buckets.utc_midnight_ns(max(even, odd) + timedelta(days=2))
    grid = futures_bar_buckets.grid_verdict(reads[even], reads[odd], after_ns=after)
    verdicts.append(f"2session from the even day {even} and the odd day {odd}: {grid}")
    return verdicts


def _bucket_verdicts(
    rest_rows: RestRows, contract: str, month: tuple[date, date], *, evidence: Evidence
) -> tuple[list[str], bool]:
    """Read one contract's bars around a month and record every bucket check.

    Returns:
        The contract's verdict lines, and whether it had both 1-session and minute bars.
    """
    first, end = month[0], _midnight(month[1] + timedelta(days=8))
    inputs = _shorter_bars(rest_rows, contract, first, end=end, evidence=evidence)
    placed = [s for s in inputs.sessions if s.start_ns >= inputs.minutes_from_ns]
    lines, placement = futures_bar_buckets.placements(placed, inputs.minutes)
    for line in lines:
        evidence(f"{contract} {line}")
    verdicts = [f"1session placement: {placement}"]
    # The library judges a bucket finished on the assumption that no session opens on a Friday
    # evening or a Saturday: such a session would start on a Friday or Saturday UTC day (ES
    # hours) or carry a Saturday or Sunday label.
    opens = Counter(f"{futures_bar_buckets.utc_day(bar.start_ns):%a}" for bar in inputs.sessions)
    labels = Counter(
        f"{date.fromisoformat(bar.label):%a}" for bar in inputs.sessions if bar.label is not None
    )
    verdicts.append(f"1session bars start on (UTC day) {dict(opens)}, labelled {dict(labels)}")
    verdicts.extend(_grid(rest_rows, contract, inputs, first=first, end=end, evidence=evidence))
    for resolution in ("1week", "1month"):
        bars = _bars(rest_rows, contract, resolution, since=_midnight(first), end=end)
        if resolution == "1week":
            starts = Counter(f"{futures_bar_buckets.utc_day(bar.start_ns):%a}" for bar in bars)
            verdicts.append(f"1week bars start on (UTC day) {dict(starts)}")
        verdicts.append(
            _judge(
                contract, resolution, bars, inputs=inputs, since=_midnight(first), evidence=evidence
            )
        )
    since = _midnight(months_before(first, 2))
    verdicts.extend(_clipping(rest_rows, contract, inputs, since=since, end=end, evidence=evidence))
    return verdicts, bool(inputs.sessions and inputs.minutes)


def _clipping(
    rest_rows: RestRows,
    contract: str,
    inputs: futures_bar_buckets.BucketInputs,
    *,
    since: datetime,
    end: datetime,
    evidence: Evidence,
) -> list[str]:
    """Read one 2-month bucket from before its start and from a month into it; compare the two.

    The first read, from `since`, may also return the bucket before, which `{from}` snaps
    into; the bucket compared is the first that starts on or after `since`, so its sessions
    were all read. The second read starts a month into that bucket.

    Returns:
        The verdict lines: each read's bucket checks, and the clipping verdict.
    """
    read = _bars(rest_rows, contract, "2month", since=since, end=end)
    after = futures_bar_buckets.utc_midnight_ns(since.date())
    whole = [bar for bar in read if bar.start_ns >= after][:1]
    evidence(f"{contract} 2month from {since:%Y-%m-%d}: starts {[_ns_text(b) for b in read]}")
    if not whole:
        return [f"2month: no bar starts on or after {since:%Y-%m-%d}"]
    inside = _midnight(months_before(futures_bar_buckets.utc_day(whole[0].start_ns), -1))
    part = _bars(rest_rows, contract, "2month", since=inside, end=end)[:1]
    return [
        _judge(contract, "2month", whole, inputs=inputs, since=since, evidence=evidence),
        _judge(contract, "2month", part, inputs=inputs, since=inside, evidence=evidence),
        f"2month from {since:%Y-%m-%d} and from {inside:%Y-%m-%d}: "
        + futures_bar_buckets.clipping_verdict(whole, part),
    ]


def _ns_text(bar: Bar) -> str:
    """Write a bar's start for the evidence.

    Returns:
        Its `window_start` in UTC and Chicago time.
    """
    return futures_bar_buckets.stamp_text(bar.start_ns)


@pytest.mark.settles("40", "81")
@pytest.mark.requests(40)
@pytest.mark.timeout(900)
def test_futures_bar_buckets_against_the_bars_inside_them(
    *,
    recent_month: tuple[date, date],
    front_contract: Callable[[str, date], str],
    rest_rows: RestRows,
    evidence: Evidence,
) -> None:
    """Which bucket each futures bar of a session or longer covers, checked against its contents.

    A simpler rule for the library's futures bar windows needs Massive to put each whole session in
    the bucket of the UTC day of its first minute, with buckets of whole UTC days on a fixed grid
    from `window_start` (N days for N sessions, 7N for weeks, calendar months), each labelled with
    its last session. For the contract of ES, corn (ZC), live cattle (LE), and Micro Bitcoin (MBT)
    that was front at the end of the most recent whole month, it reads that month's minute bars
    (from a week before to a week after) and 1-session bars (from two months before), and records:
    each 1-session bar's `window_start` beside its session's first minute bar; 2-session bars read
    from an even and from an odd epoch day, and whether the two reads share their windows (a fixed
    grid) or each starts at its `{from}`; the UTC weekday 1-week bars start on; and each 2-session,
    week, month, and 2-month bar against its bucket's candidate contents (the whole sessions
    starting in its UTC days, the next N sessions, the minute bars of its UTC days, and the
    sessions from `{from}` when `{from}` falls inside it), with each one's relative error and
    whether its last session is the bar's label; the UTC days 1-session bars start on and the
    weekdays of their labels, which show whether any session opens on a Friday evening or a
    Saturday; and one 2-month bucket read from before it and from a month into it, the same bar or
    clipped at `{from}`. It ends with a verdict line per question and contract. Reading it: whole
    sessions in its UTC days matching every bar, a fixed grid, and the same 2-month bar from both
    reads confirm the model; anything else says where it breaks.

    It asserts only that each contract has 1-session and minute bars. Cost: about 10 REST
    requests per contract (the minute bars take two or three pages), some 40 in all.
    """
    found = {}
    for product in _BUCKET_PRODUCTS:
        contract = front_contract(product, recent_month[1])
        found[contract] = _bucket_verdicts(rest_rows, contract, recent_month, evidence=evidence)
    for contract, (lines, _) in found.items():
        for line in lines:
            evidence(f"VERDICT {contract} {line}")
    empty = [contract for contract, (_, complete) in found.items() if not complete]
    assert not empty, f"no 1-session or minute bars for {empty}"


_FOLLOW_WAIT = 90.0
"""Seconds a followed read's step may take: a request of a subscription Massive limits may
wait for a later minute."""


def _codes_until(
    table_rows: TableRows, result: Table, done: Callable[[set[str]], bool]
) -> set[str]:
    """Read a result's product codes until `done` holds, failing after `_FOLLOW_WAIT` seconds.

    Returns:
        The distinct `ProductCode` values that satisfied it.
    """
    pause = threading.Event()  # never set; only paces the loop
    deadline = time.monotonic() + _FOLLOW_WAIT
    while True:
        codes = {str(row["ProductCode"]) for row in table_rows(result)}
        if done(codes):
            return codes
        assert time.monotonic() < deadline, f"the followed result never got there: {codes}"
        pause.wait(0.25)


@pytest.mark.requests(2)
@pytest.mark.timeout(300)
def test_contracts_follow_a_live_table_of_product_codes(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    table_rows: TableRows,
    evidence: Evidence,
    dh_input_table: Callable[..., InputTable],
) -> None:
    """Whether `fetch()` of futures contracts follows a live table of product codes.

    Follows an input table holding `ES` with `m.futures.contracts(product_code=codes,
    as_of=<the most recent published Monday>, limit=10).fetch()`, adds `NQ` (one request), and
    deletes it. Passes when the result holds only ES rows at the call, gains NQ rows once NQ is
    read, and loses them once NQ's row is deleted, while the ES rows stay. Each step waits at
    most 90 s.
    """
    m = live_session
    codes = dh_input_table({"ProductCode": dtypes.string}, key_cols="ProductCode")
    codes.add(new_table([string_col("ProductCode", ["ES"])]))
    result = m.futures.contracts(product_code=codes, as_of=recent_monday, limit=10).fetch(wait=True)
    try:
        first = _codes_until(table_rows, result, lambda seen: seen == {"ES"})
        evidence(f"at the call: {sorted(first)}, {result.size} rows")
        codes.add(new_table([string_col("ProductCode", ["NQ"])]))
        added = _codes_until(table_rows, result, lambda seen: seen == {"ES", "NQ"})
        evidence(f"after adding NQ: {sorted(added)}, {result.size} rows")
        codes.delete(new_table([string_col("ProductCode", ["NQ"])]))
        left = _codes_until(table_rows, result, lambda seen: seen == {"ES"})
        evidence(f"after deleting NQ: {sorted(left)}, {result.size} rows")
    finally:
        m.release(result)


_TAS_SAMPLE = {"CLT": "CL", "SBT": "ZS", "HG0": "HG", "HGT": "HG", "GCT": "GC", "ZWT": "ZW"}
"""Trade at Settlement products with no schedule of their own, each with the product it settles
to: crude oil, soybeans (`SBT` settles to `ZS`, not `SB`), copper's spot-month and active-month
TAS (`HG0` and `HGT`, both on `HG`), gold, and wheat."""

_TAS_TRADES = 50
"""How many of each TAS contract's latest trades to read."""

_TRADES = "/futures/v1/trades/{ticker}"

_SCHEDULE_PAGE = 1000
"""The schedule events read for one product: one page."""

_EXAMPLES = 5
"""How many trades outside their session, or with no session, to record whole."""


def _front_contracts(client: RestClient, *, evidence: Evidence) -> dict[str, str]:
    """Return the soonest-expiring active single contract of each sampled TAS product.

    Asks the contracts list for the products' active single contracts on the latest of the last
    four days it holds rows for, and records the day and the contracts chosen. A contract whose
    last trade date has passed is skipped.
    """
    today = datetime.now(_CHICAGO).date()
    day, rows = today, []
    for back in range(4):
        day = today - timedelta(days=back)
        query = {
            "product_code.any_of": list(_TAS_SAMPLE),
            "date": day,
            "type": "single",
            "active": True,
        }
        rows = _page(client, _CONTRACTS, query={**query, "limit": 1000})
        if rows:
            break
    fronts: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row["last_trade_date"]) < today.isoformat():
            continue
        code = str(row["product_code"])
        if code not in fronts or str(row["last_trade_date"]) < str(fronts[code]["last_trade_date"]):
            fronts[code] = row
    evidence(
        f"TAS contracts on {day}: {len(rows)} rows; "
        f"front contracts {[row['ticker'] for row in fronts.values()]}"
    )
    return {code: str(row["ticker"]) for code, row in fronts.items()}


def _session_windows(
    client: RestClient, product: str, days: Sequence[str]
) -> dict[str, tuple[int, int]] | None:
    """Return each session's first and last schedule event, in epoch nanoseconds, by its date.

    Reads one page of the product's schedule events for the sessions whose `session_end_date`
    lies between the earliest and the latest of `days`.

    Returns:
        The windows, or None when the page is full, so later sessions may be missing.
    """
    query = {
        "product_code": product,
        "session_end_date.gte": date.fromisoformat(min(days)),
        "session_end_date.lte": date.fromisoformat(max(days)),
        "limit": _SCHEDULE_PAGE,
    }
    rows = _page(client, _SCHEDULES, query=query)
    if len(rows) == _SCHEDULE_PAGE:
        return None
    events: dict[str, list[int]] = {}
    for row in rows:
        stamp = datetime.fromisoformat(str(row["timestamp"]))
        events.setdefault(str(row["session_end_date"]), []).append(epoch_ns(stamp))
    return {day: (min(stamps), max(stamps)) for day, stamps in events.items()}


def _compare_with_sessions(
    client: RestClient,
    trades: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
    underlying: str,
    evidence: Evidence,
) -> None:
    """Record whether each dated trade falls inside the underlying's session of its date.

    Counts the trades inside their session, outside it, and dated a day the underlying's
    schedule has no session for. Records up to five trades of each of the last two kinds whole.
    """
    days = {str(t.get("session_end_date")) for t in trades}
    dated = sorted(day for day in days if day != "None")
    if not dated:
        return
    windows = _session_windows(client, underlying, dated)
    if windows is None:
        evidence(f"{ticker}: {underlying}'s schedule filled a page; trades not compared")
        return
    verdicts: dict[str, list[Mapping[str, Any]]] = {}
    for trade in trades:
        if trade.get("session_end_date") is None:
            continue
        window = windows.get(str(trade["session_end_date"]))
        if window is None:
            verdict = "no session"
        elif window[0] <= int(trade["timestamp"]) <= window[1]:
            verdict = "inside"
        else:
            verdict = "outside"
        verdicts.setdefault(verdict, []).append(trade)
    spans = {day: (_chicago(first), _chicago(last)) for day, (first, last) in windows.items()}
    counts = {verdict: len(found) for verdict, found in verdicts.items()}
    evidence(f"{ticker} against {underlying}'s sessions {spans}: {counts}")
    for verdict in ("outside", "no session"):
        for trade in verdicts.get(verdict, [])[:_EXAMPLES]:
            evidence(f"{ticker} {verdict}: {_chicago(int(trade['timestamp']))} {trade}")


@pytest.mark.settles("137")
@pytest.mark.requests(23)
@pytest.mark.timeout(200)
def test_trade_at_settlement_trades_and_their_sessions(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which trading day Massive gives Trade at Settlement (TAS) trades: the underlying's or not.

    Massive's schedules hold no session for a TAS product, so the library withholds its streamed
    rows. Its REST trades document `session_end_date`, the trading day. For six TAS products,
    finds each one's front contract on the contracts list (up to 4 requests), reads its latest
    50 trades and the first 50 of the latest trade's session, the trades nearest the evening
    open (12 requests), and records how many carry `session_end_date` and its values. Then
    reads the settling product's schedule for those days (up to 6 requests) and records, for
    each trade, whether its time falls inside the underlying's session of the same date: from the
    session's first schedule event to its last. Last, records the snapshot rows of the six
    contracts whole (1 request), to show any field that names a session or the underlying.

    Passes when every request is answered; the dates and the comparison are evidence.
    """
    fronts = _front_contracts(live_rest, evidence=evidence)
    for code, underlying in _TAS_SAMPLE.items():
        ticker = fronts.get(code)
        if ticker is None:
            evidence(f"{code}: no single contract on the contracts list")
            continue
        query = {"ticker": ticker, "sort": "timestamp.desc", "limit": _TAS_TRADES}
        latest = _page(live_rest, _TRADES, query=query)
        first: list[dict[str, Any]] = []
        if latest and latest[0].get("session_end_date") is not None:
            day = date.fromisoformat(str(latest[0]["session_end_date"]))
            query = {**query, "sort": "timestamp.asc", "session_end_date": day}
            first = _page(live_rest, _TRADES, query=query)
        trades = latest + first
        days = Counter(str(t.get("session_end_date")) for t in trades)
        evidence(
            f"{ticker}: {len(latest)} latest and {len(first)} first trades of the latest "
            f"session; session_end_date {dict(days)}"
        )
        for label, trade in (("latest", latest[:1]), ("first", first[:1])):
            for row in trade:
                evidence(f"{ticker} {label} trade, {_chicago(int(row['timestamp']))}: {row}")
        _compare_with_sessions(
            live_rest, trades, ticker=ticker, underlying=underlying, evidence=evidence
        )
    if not fronts:
        evidence("no TAS contract found, so no snapshot read")
        return
    query = {"ticker.any_of": list(fronts.values()), "limit": 1000}
    for row in _page(live_rest, _SNAPSHOT, query=query):
        evidence(f"snapshot row: {row}")
