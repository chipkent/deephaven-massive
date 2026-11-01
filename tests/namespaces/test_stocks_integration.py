"""Live checks of stock data against Massive: stream time units, bar alignment and stamps, filings.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked; a test marked `verifies` also asserts the answer the library assumes, and fails
when Massive no longer gives it. Streams assert that Massive acknowledged their topics, wait for
rows only while their market is open, and are released at once; raw bars come from the wire REST
client, because the library filters the very bars in question. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._query import HistoryQuery

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
MarketStatus: TypeAlias = "Callable[[str], str]"
MinuteSpanReport: TypeAlias = "Callable[[str, date], int]"
WeekBarReport: TypeAlias = "Callable[[str, str, str], tuple[list[str], int, list[str]]]"
DaySpan: TypeAlias = "tuple[date, date]"
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
StampedBar: TypeAlias = "tuple[int, tuple[float | None, ...]]"
StampDays: TypeAlias = "dict[str, tuple[date, date]]"
RangeBars: TypeAlias = "Callable[[str, str, tuple[date, date]], list[dict[str, Any]]]"
ToStampedBar: TypeAlias = "Callable[[Mapping[str, Any]], StampedBar]"
DayBarReport: TypeAlias = (
    "Callable[[str, tuple[date, ...], Mapping[str, Mapping[date, list[StampedBar]]]], list[date]]"
)
PrevBarReport: TypeAlias = "Callable[[str], int]"
PeriodBarReport: TypeAlias = (
    "Callable[[str, Mapping[str, tuple[date, date]], Mapping[str, tuple[int, int]]], bool]"
)
LibraryStamps: TypeAlias = "Callable[[Callable[[], Table]], str]"
FindSplits: TypeAlias = "Callable[[dm.MassiveSession], list[Any]]"
FindDividends: TypeAlias = "Callable[[dm.MassiveSession], list[Any]]"

_SYMBOL = "AAPL"
_RANGE = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_MS = 1_000_000
_NEW_YORK = ZoneInfo("America/New_York")
_SPLIT_TOLERANCE = 0.01
"""The relative error within which an as-traded price is the adjusted one times the ratio;
Massive rounds adjusted prices."""
_SAME_PRICE = 1e-9
"""The relative error within which two closes of one day are the same price."""
_SAME_VOLUME = 1e-9
"""The relative error within which a bar's volume is the sum of its days' volumes."""
_CANDIDATES = 3
"""How many recent events a corporate-action test tries before it gives up on bars."""
_GROUPED = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
_DAY_FILE = "us_stocks_sip/day_aggs_v1"
_MINUTE_FILE = "us_stocks_sip/minute_aggs_v1"
_DAY_NS = 86_400 * 1_000_000_000
_STREAM_WAIT = 60.0
"""Seconds a stream test waits for its first frame; stocks trade on weekdays only."""


def _bars(
    rest_rows: RestRows, span: str, *, multiplier: int, bounds: tuple[object, object]
) -> list[dict[str, Any]]:
    """Read raw aggregate bars of the test symbol from the range endpoint, unfiltered.

    Returns:
        The bars as Massive sent them, oldest first.
    """
    params = {"stocksTicker": _SYMBOL, "multiplier": multiplier, "timespan": span}
    params |= {"from": bounds[0], "to": bounds[1]}
    return rest_rows(_RANGE, params, {"sort": "asc", "limit": 50_000})


def _stream_evidence(
    session: dm.MassiveSession,
    table: Table,
    *,
    rows: list[dict[str, Any]],
    report: Report,
    evidence: Evidence,
    table_rows: TableRows,
) -> tuple[list[dict[str, Any]], str | None]:
    """Record a stream's diagnostics, then release the stream.

    The diagnostics are read before the release, which the session carries out on another
    thread, so they show the stream's acknowledgement and error as they were.

    Returns:
        The rows the caller received, and the error of a stream that failed (None when none
        did).
    """
    for line in report(session):
        evidence(line)
    failed = [r["Error"] for r in table_rows(session.streams()) if r["State"] == "FAILED"]
    session.release(table)
    return rows, (str(failed[0]) if failed else None)


@pytest.mark.needs("stocks:advanced")
@pytest.mark.verifies("21")
@pytest.mark.timeout(100)
def test_luld_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    market_status: MarketStatus,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession, str], None],
    instant_text: Text,
    evidence: Evidence,
    table_rows: TableRows,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Whether a WebSocket `LULD` frame's `t` converts to the current instant.

    `LULD` `t` is documented in milliseconds while the sample has 19 digits, so the library
    reads each value by its magnitude and logs once which unit the column arrived in; the
    recorded log line settles the unit. Streams the bands of every ticker (`LULD.*`), so some
    band changes within seconds while the regular session is open, and asserts that Massive
    acknowledged the topic. Bands exist only in the regular session, so when
    `/v1/marketstatus/now` does not say `open` the test skips after the acknowledgement.
    Passes when the first row's instant is within a day of now; a failed stream fails the
    test, quoting the error; a refused stream (a Business plan without the trades and quotes
    expansion has no LULD) skips, naming the plan.
    """
    caplog.set_level(logging.INFO, logger="deephaven_massive")
    table = live_session.stocks.luld(dm.ALL).append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    status = market_status("stocks")
    deadline = time.monotonic() + _STREAM_WAIT
    received = wait_rows(live_session, table, deadline) if status == "open" else []
    rows, failure = _stream_evidence(
        live_session,
        table,
        rows=received,
        report=stream_report,
        evidence=evidence,
        table_rows=table_rows,
    )
    skip_if_not_entitled(live_session, "Stocks Advanced, or Business with the expansion")
    if failure is not None:
        pytest.fail(f"the LULD stream failed (a unit mismatch fails its bound check): {failure}")
    assert acked, "Massive acknowledged no LULD topic"
    if status != "open":
        pytest.skip(f"the stock market is {status}: LULD bands change only in the regular session")
    detected = [r.getMessage() for r in caplog.records if "read by magnitude" in r.message]
    evidence(f"magnitude rule log: {detected}")
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"LULD row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    assert rows, f"no LULD frame arrived in {_STREAM_WAIT:.0f} s of an open session"
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("41")
@pytest.mark.requests(2)
@pytest.mark.timeout(60)
def test_an_ipo_keeps_its_issuer_name(
    live_session: dm.MassiveSession,
    *,
    recent_month: DaySpan,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether an IPO's issuer name changes while the offering moves through its statuses.

    Reads every IPO listed in the twelve months ending with the most recent whole month and
    records each ticker whose rows carry more than one `IssuerName`, with their statuses.
    Reading it: none means the `poll()` key `Symbol` and `IssuerName` keeps one row per
    offering; a ticker whose names differ only in spelling (a suffix, punctuation) means a
    renamed offering would leave a stale row, while wholly different issuers are a recycled
    ticker, which the key keeps apart on purpose.
    """
    start, end = _year_before(recent_month)
    rows = table_rows(live_session.stocks.ipos(start=start, end=end).fetch())
    names: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        names.setdefault(str(row["Symbol"]), set()).add(
            (str(row["IssuerName"]), str(row["IpoStatus"]))
        )
    several = {symbol: seen for symbol, seen in names.items() if len({n for n, _ in seen}) > 1}
    evidence(
        f"IPOs {start}..{end}: {len(rows)} rows, {len(names)} tickers, {len(several)} with >1 name"
    )
    for symbol, seen in sorted(several.items())[:10]:
        evidence(f"    {symbol}: {sorted(seen)}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("33")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_stock_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    recent_week: DaySpan,
    week_bar_report: WeekBarReport,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a stock week bar covers the week from its Sunday 00:00 New York time.

    For each raw week bar of `AAPL` over the four weeks ending with the most recent week without a
    holiday, records its stamp and the candidate windows whose day-bar volume is closest to its
    own (hour bars do not add up to a stock week bar's volume, day bars do); `MATCH` marks the
    bucket. Passes when, for every bar of those weeks, the week from Sunday 00:00 New York time,
    where Massive stamps it and the library's week starts, is the closest window, ties allowed.
    Stocks do not trade at weekends, so every start from Friday evening to Monday morning ties: the
    check can only catch a week that starts in the middle of the week. A failure names the bars
    whose closest window starts elsewhere. The last line shows which bars the library's `history()`
    keeps.
    """
    lines, checked, misses = week_bar_report("stocksTicker", _SYMBOL, "New York")
    for line in lines:
        evidence(line)
    first, last = recent_week[0] - timedelta(days=7), recent_week[1]
    kept = live_session.stocks.bars(_SYMBOL, dm.Timespan.WEEK).history(
        first, last, source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history {first}..{last} keeps the bars stamped {stamps}")
    assert checked >= 3, f"only {checked} week bars of the four weeks"
    assert not misses, f"week bars closer to another window than the library's: {misses}"


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("34")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_month_bars_from_mid_month_against_the_whole_month(
    *, recent_month: DaySpan, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether the range returns a partial month bar for a `{from}` inside a month.

    Records `AAPL` month bars over the four months ending with the most recent whole month,
    from the first month's first day and from its 15th. Reading it: a first bar of the next
    month, or a first-month bar with the whole month's volume, means no partial bar; a
    first-month bar with less volume is a partial bar, which the library already avoids by
    starting at period boundaries.
    """
    index = recent_month[0].year * 12 + recent_month[0].month - 1 - 3
    start = date(index // 12, index % 12 + 1, 1)
    middle = start.replace(day=15)
    whole = _bars(rest_rows, "month", multiplier=1, bounds=(start, recent_month[1]))
    partial = _bars(rest_rows, "month", multiplier=1, bounds=(middle, recent_month[1]))
    for name, bars in ((f"from {start}", whole), (f"from {middle}", partial)):
        for bar in bars:
            evidence(
                f"{name}: t={instant_text(bar['t'] * _MS)} o={bar['o']} c={bar['c']} v={bar['v']}"
            )
    assert whole
    assert partial


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("35")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_bars_of_several_units_align_to_boundaries_or_to_from(
    *, recent_week: DaySpan, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether bars of several units align to clock and calendar boundaries or to `{from}`.

    Records `AAPL` 5-minute bars from 14:32 UTC on the Monday of the most recent week without
    a holiday, and 2-week bars from a Wednesday eight weeks before it. Reading it: stamps on
    :30 and :35 and on Mondays mean boundaries, as the library assumes; stamps at 14:32 and on
    Wednesdays mean `{from}`, and the library's bar windows must follow.
    """
    monday = recent_week[0]
    at = datetime(monday.year, monday.month, monday.day, 14, 32, tzinfo=UTC)
    five = _bars(rest_rows, "minute", multiplier=5, bounds=(at, at + timedelta(hours=1)))
    wednesday = monday - timedelta(days=54)
    two_weeks = _bars(rest_rows, "week", multiplier=2, bounds=(wednesday, recent_week[1]))
    evidence(
        f"5-minute bars from {at:%Y-%m-%d} 14:32 UTC: {[instant_text(b['t'] * _MS) for b in five]}"
    )
    evidence(
        f"2-week bars from Wednesday {wednesday}: "
        f"{[(instant_text(b['t'] * _MS), b['v']) for b in two_weeks]}"
    )
    assert five
    assert two_weeks


def _symbol_rows(query: HistoryQuery, day: date) -> Table:
    """Return the test symbol's rows of one day of a `dm.ALL` history over REST.

    Returns:
        The rows, filtered in Deephaven (`_only_the_symbol`).
    """
    return _only_the_symbol(query.history(day, day, source=dm.HistorySource.REST))


def _only_the_symbol(table: Table) -> Table:
    """Keep the test symbol's rows of a table.

    The filter runs in a frame that holds only the table. A formula hands the locals of the
    frame that calls it to the JVM, which keeps them until its own garbage collection: a
    frame holding the query would keep the query's session, and its reaper thread, alive
    after the test closed it.

    Returns:
        The rows, filtered in Deephaven.
    """
    return table.where(f"Symbol == `{_SYMBOL}`")


def _record_library_stock_stamps(
    session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """Record the library's windows of the bars the REST stamp check reads raw, call by call.

    One evidence line per library call, each recorded as it returns: `history()` of the two days,
    `previous_day_bar().fetch()`, the grouped-daily `dm.ALL` history of each day (AAPL's row), and
    the week and month history.
    """
    days = stamp_days["day"]
    rest = dm.HistorySource.REST
    day_bars = session.stocks.bars(_SYMBOL, dm.Timespan.DAY)
    every = session.stocks.bars(dm.ALL, dm.Timespan.DAY)
    evidence(
        "library history(REST): " + library_stamps(partial(day_bars.history, *days, source=rest))
    )
    previous = session.stocks.previous_day_bar(_SYMBOL)
    evidence(f"library previous_day_bar fetch() (/prev): {library_stamps(previous.fetch)}")
    for day in days:
        outcome = library_stamps(partial(_symbol_rows, every, day))
        evidence(f"library dm.ALL history(REST) {day} (grouped daily): {outcome}")
    for span in (dm.Timespan.WEEK, dm.Timespan.MONTH):
        query = session.stocks.bars(_SYMBOL, span)
        outcome = library_stamps(partial(query.history, *stamp_days[span.value], source=rest))
        evidence(f"library {span.value} history: {outcome}")


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("50")
@pytest.mark.requests(5)
@pytest.mark.timeout(100)
def test_stock_day_and_longer_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    *,
    classes_on_basic: frozenset[str],
    stamp_days: StampDays,
    rest_rows: RestRows,
    stamped_bar: ToStampedBar,
    day_bar_report: DayBarReport,
    prev_bar_report: PrevBarReport,
    period_bar_report: PeriodBarReport,
    minute_span_report: MinuteSpanReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How every REST source stamps AAPL's 1-day, week, and month bars, and whether they agree.

    The days are the most recent half day whose week has ended (the Friday after Thanksgiving,
    July 3, or December 24, closing at 13:00 New York time) and the full trading day before
    it. Records, each stamp in UTC and New York time: the range endpoint's 1-day bars around
    both days, each day's first and last minute bar, and each day's grouped-daily row, with
    which day's range bar it matches and the largest relative error; `/prev`, which serves
    only the most recent completed trading day (a short-day stamp shows only on a run the
    trading day after a half day), beside the range bars of that day; the week bars of the
    half day's week and the week before, and the month bars of the two whole months before
    the current one; and the library's `TimestampStart`, `TimestampEnd`, and `Date` for the
    same requests. With Stocks on Basic, which allows 5 requests a minute, it reads the range
    bars of both days, the grouped-daily row and minute bars of the half day only, and the week
    and month bars, and leaves out `/prev` and the library's lines. Reading it: 1-day stamps
    at 00:00, 13:00, or 16:00 New York time, longer bars at 00:00 New York time on their
    period's first day (or the weekend before a week's Monday), and matching values confirm
    the library's day label; minute bars ending at 12:59 on the half day confirm a whole day
    cut short; a raw stamp of any other form is served by the library, labelled by the New
    York day it falls on, with a WARNING in the log.
    """
    days = stamp_days["day"]
    basic = "stocks" in classes_on_basic
    read = days[-1:] if basic else days  # the half day is the last
    grouped = {
        day: [
            stamped_bar(r) for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _SYMBOL
        ]
        for day in read
    }
    missing = day_bar_report(_SYMBOL, days, {"grouped daily": grouped})
    minutes = [minute_span_report(_SYMBOL, day) for day in read]
    spans = {span: stamp_days[span] for span in ("week", "month")}
    periods = period_bar_report(_SYMBOL, spans, {"week": (2, 2), "month": (2, 2)})
    if basic:
        evidence("Stocks on Basic: /prev and the library's lines are not read")
    else:
        assert prev_bar_report(_SYMBOL), "/prev returned no bar"
        _record_library_stock_stamps(
            live_session, stamp_days=stamp_days, library_stamps=library_stamps, evidence=evidence
        )
    assert not missing, f"no range bar on {missing}"
    assert all(minutes), f"a stamp day has no minute bars: {dict(zip(read, minutes, strict=True))}"
    assert all(grouped.values()), f"a stamp day has no grouped-daily row: {grouped}"
    assert periods, "the range did not return one bar for each of two weeks and two months"


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.settles("50")
@pytest.mark.timeout(200)
def test_stock_day_bar_stamps_of_the_day_file(
    live_session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    day_bar_report: DayBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How the stock flat files stamp AAPL's bars of a full and a half day, and whether REST agrees.

    Records the `us_stocks_sip/day_aggs_v1` AAPL row of the most recent half day whose week
    has ended and of the full trading day before it, its `window_start` in UTC and New York
    time, beside the range endpoint's bars around those days and which day's bar it matches
    with the largest relative error; the first and last AAPL rows of `minute_aggs_v1` on both
    days; and the library's `history(source=FLAT_FILES)` windows. Reading it: a stamp the
    library accepts (00:00 or 16:00 New York time, or 13:00 on the half day), a match on the
    same day, and minute rows ending where the REST minute bars end confirm one day cut for
    files and REST.
    """
    days = stamp_days["day"]
    files: dict[date, list[StampedBar]] = {}
    for day in days:
        result = flat_file_tickers(_DAY_FILE, day, [_SYMBOL])
        files[day] = [] if result is None else [stamped_bar(r) for r in result[0]]
        minutes = flat_file_tickers(_MINUTE_FILE, day, [_SYMBOL])
        rows = [] if minutes is None else [r for r in minutes[0] if r["window_start"]]
        ends = [_instant_text_ns(int(str(r["window_start"]))) for r in rows[:1] + rows[-1:]]
        evidence(f"{_MINUTE_FILE} {day}: {len(rows)} rows; first and last window_start {ends}")
    missing = day_bar_report(_SYMBOL, days, {_DAY_FILE: files})
    as_traded = live_session.stocks.bars(_SYMBOL, dm.Timespan.DAY, adjusted=False)
    outcome = library_stamps(partial(as_traded.history, *days, source=dm.HistorySource.FLAT_FILES))
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert not missing, f"no range bar on {missing}"
    assert all(files.values()), f"the day file lacks a stamp day: {files}"


def _days(
    session: dm.MassiveSession,
    symbol: str,
    *,
    span: tuple[date, date],
    adjusted: bool,
    table_rows: TableRows,
) -> dict[str, dict[str, Any]]:
    """Read a symbol's REST day bars on one split basis, by `Date`.

    Returns:
        Each day's bar.
    """
    table = session.stocks.bars(symbol, dm.Timespan.DAY, adjusted=adjusted).history(
        span[0], span[1], source=dm.HistorySource.REST
    )
    return {str(row["Date"]): row for row in table_rows(table)}


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("54")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_every_page_echoes_its_adjustment(
    live_rest: RestClient, *, recent_week: DaySpan, evidence: Evidence
) -> None:
    """Whether every aggregates page echoes `adjusted`, `next_url` pages and grouped daily too.

    Pages through the minute bars of AAPL over the most recent week without a holiday and the
    week before, with a small `limit`, so the range answers in several pages, and reads the
    grouped daily of that week's Monday and Tuesday, recording each page's `adjusted` echo.
    Reading it: `True` on every page confirms the library's page-by-page check; a missing echo
    on a later page means the check can only warn there.
    """
    monday = recent_week[0]
    range_path = source(Transport.REST, _RANGE)
    params = {
        "stocksTicker": _SYMBOL,
        "multiplier": 1,
        "timespan": "minute",
        "from": monday - timedelta(days=7),
        "to": monday + timedelta(days=4),
    }
    echoes = [
        page.adjusted
        for page, _ in zip(
            live_rest.pages(range_path, params, {"adjusted": True, "limit": 5000}, bucket="stocks"),
            range(3),
            strict=False,
        )
    ]
    evidence(f"range pages' adjusted echoes: {echoes}")
    grouped = source(Transport.REST, _GROUPED)
    for day in (monday, monday + timedelta(days=1)):
        page = live_rest.get(grouped, {"date": day}, {"adjusted": False}, bucket="stocks")
        evidence(f"grouped daily {day}: adjusted echo {page.adjusted}")
    assert echoes


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("55")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_cash_dividends_are_never_adjusted(
    live_session: dm.MassiveSession,
    *,
    special_dividends: FindDividends,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive's adjustment leaves prices alone around a special cash dividend.

    Finds the year's most recent special cash dividend of a common stock with no later split
    (`m.stocks.dividends`) and records the stock's day bars from a week before its ex-dividend
    date to two days after, with `adjusted=true` and `adjusted=false`. Passes when the ratio
    of the adjusted to the as-traded close takes no step across the ex-dividend date: the same
    ratio on every day, so `adjusted` concerns splits only, as the library documents, and an
    adjustment for the dividend would show as a step on the ex-date. With Stocks on Basic
    only the newest such dividend is tried (`special_dividends` keeps one), so the test sends
    at most 4 stock requests. Skips when the year had no such dividend with bars.
    """
    dividends = special_dividends(live_session)
    spans = {
        d.symbol: (d.ex_date - timedelta(days=7), d.ex_date + timedelta(days=2)) for d in dividends
    }
    found = None
    for dividend in dividends[:_CANDIDATES]:
        span = spans[dividend.symbol]
        adjusted = _days(
            live_session, dividend.symbol, span=span, adjusted=True, table_rows=table_rows
        )
        traded = _days(
            live_session, dividend.symbol, span=span, adjusted=False, table_rows=table_rows
        )
        if adjusted and adjusted.keys() == traded.keys():
            found = dividend, adjusted, traded
            break
        evidence(f"{dividend.symbol} {span[0]}..{span[1]}: bars missing on a basis; next")
    if found is None:
        pytest.skip(f"no recent special cash dividend with bars: {dividends}")
    dividend, adjusted, traded = found
    ratios = {
        day: float(adjusted[day]["Close"]) / float(traded[day]["Close"]) for day in sorted(adjusted)
    }
    evidence(
        f"{dividend.symbol} special dividend of {dividend.amount} ex {dividend.ex_date}: "
        f"adjusted / as-traded close by day {ratios}"
    )
    before = [r for day, r in ratios.items() if day < dividend.ex_date.isoformat()]
    after = [r for day, r in ratios.items() if day >= dividend.ex_date.isoformat()]
    assert before, f"no bar before the ex-date {dividend.ex_date}"
    assert after, f"no bar on or after the ex-date {dividend.ex_date}"
    steps = [r / before[-1] - 1 for r in (*before, *after)]
    assert all(abs(step) <= _SAME_PRICE for step in steps), f"the ratio steps: {ratios}"


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("56")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_stock_dividend_events_and_the_adjustment(
    live_session: dm.MassiveSession,
    *,
    recent_month: DaySpan,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether `adjusted=true` applies a `stock_dividend` event of the splits dataset.

    Finds the most recent `stock_dividend` event of the year ending with the most recent whole
    month and records its ticker's day bars on both bases across its execution date. Reading
    it: prices before the date that differ by the event's factor mean such events are adjusted
    like splits; equal prices mean they are not, and the README's recipe should skip them.
    """
    start, end = _year_before(recent_month)
    events = table_rows(
        live_session.stocks.splits(
            start=start,
            end=end,
            sort="execution_date",
            order=dm.Order.DESC,
            filters={"adjustment_type": "stock_dividend"},
            limit=1,
        ).fetch()
    )
    evidence(f"a stock_dividend event: {events[:1]}")
    if not events:
        pytest.skip(f"no stock_dividend event from {start} to {end}")
    event = events[0]
    day = date.fromisoformat(str(event["ExecutionDate"]))
    first, last = day - timedelta(days=7), day + timedelta(days=3)
    symbol = str(event["Symbol"])
    adjusted = _days(live_session, symbol, span=(first, last), adjusted=True, table_rows=table_rows)
    traded = _days(live_session, symbol, span=(first, last), adjusted=False, table_rows=table_rows)
    for row_day in sorted(adjusted):
        evidence(
            f"{symbol} {row_day}: adjusted close={adjusted[row_day]['Close']} "
            f"traded close={traded.get(row_day, {}).get('Close')}"
        )
    assert adjusted


@pytest.mark.needs("stocks:basic", "currencies:basic")
@pytest.mark.settles("61")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_daily_indicator_stamps(
    live_session: dm.MassiveSession,
    *,
    recent_week: DaySpan,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """At which time of day Massive stamps daily indicator values, for a stock and for crypto.

    Records the daily SMA values of AAPL and `X:BTCUSD` over the most recent week without a
    holiday as Massive sent them, then the library's answer to the same request: each value's
    `TimestampStart` and `Date`, or the `MassiveError` the read raised. The library expects a
    daily value where its class's range bars are stamped, 00:00 New York time for a stock and
    00:00 UTC for crypto. Reading it: raw stamps there mean the labels are right; stamps
    elsewhere are still served, labelled by the day they fall on, with a WARNING in the log,
    and say which day each value really belongs to.
    """
    # The wire client encodes a date parameter from a `date`, never from text.
    start, end = recent_week[0], recent_week[1] + timedelta(days=1)
    found = 0
    paths = {"AAPL": ("stockTicker", "AAPL"), "X:BTCUSD": ("cryptoTicker", "X:BTCUSD")}
    for symbol, (name, ticker) in paths.items():
        query = {"timespan": "day", "window": 5, "limit": 50}
        query |= {"timestamp.gte": start, "timestamp.lt": end}
        raw = rest_rows(f"/v1/indicators/sma/{{{name}}}", {name: ticker}, query)
        found += len(raw)
        sent = [instant_text(r["timestamp"] * _MS) for r in raw]
        evidence(f"{symbol} daily SMA stamps as sent: {sent}")
        q = live_session.indicators.sma(symbol, 5, start=start, end=end, timespan=dm.Timespan.DAY)
        try:
            rows = table_rows(q.fetch())
        except dm.MassiveError as error:
            evidence(f"{symbol} daily SMA failed in the library: {error}")
            continue
        stamps = [(instant_text(r["TimestampStart"]), r["Date"]) for r in rows]
        evidence(f"{symbol} daily SMA (TimestampStart, Date): {stamps}")
    assert found


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("62")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_unknown_ticker_statuses(
    live_rest: RestClient, *, recent_week: DaySpan, evidence: Evidence
) -> None:
    """What each endpoint answers for a ticker that does not exist.

    Requests `ZZZZZQ` from `/prev`, the range (over the most recent week without a holiday),
    `/v3/trades`, and the ticker details, and records each answer's status and row count.
    Reading it: an empty 200 or a 404 both return no rows in the library; a 404 is the only way
    a symbol is dropped with a WARNING.
    """
    ticker = "ZZZZZQ"
    requests = (
        ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": ticker}),
        (
            _RANGE,
            {"stocksTicker": ticker, "multiplier": 1, "timespan": "day"}
            | {"from": recent_week[0], "to": recent_week[0] + timedelta(days=4)},
        ),
        ("/v3/trades/{stockTicker}", {"stockTicker": ticker}),
        ("/v3/reference/tickers/{ticker}", {"ticker": ticker}),
    )
    answered = 0
    for template, params in requests:
        # Ticker details are reference data of no subscription; the rest are stocks data.
        bucket = "other" if template.startswith("/v3/reference/") else "stocks"
        try:
            page = live_rest.get(source(Transport.REST, template), params, {}, bucket=bucket)
        except WireError as error:
            evidence(f"{template}: HTTP {error.status} {error.message[:120]}")
        else:
            evidence(f"{template}: HTTP 200, status {page.status}, {len(page.rows)} rows")
        answered += 1
    assert answered == len(requests)


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("63")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_bars_of_several_days_count_calendar_days(
    *, recent_week: DaySpan, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether a stock bar of several days counts calendar days, as the library assumes.

    Records AAPL's 2-day and 1-day bars from the Monday of the week before the most recent
    week without a holiday to that week's Friday. Passes when the volume of every 2-day bar
    inside that span equals the 1-day volumes of the two calendar days from its New York
    stamp date: a bar stamped on a Friday holds Friday's volume alone, and the next one is
    stamped on the Sunday and holds Monday's. The library's `TimestampEnd` counts the same
    calendar days; a bar that counted trading days would hold Friday's and Monday's, and a
    failure names it.
    """
    first, last = recent_week[0] - timedelta(days=7), recent_week[0] + timedelta(days=4)
    two = _bars(rest_rows, "day", multiplier=2, bounds=(first, last))
    one = _bars(rest_rows, "day", multiplier=1, bounds=(first, last))
    evidence(f"2-day bars (t, v): {[(instant_text(b['t'] * _MS), b['v']) for b in two]}")
    evidence(f"1-day bars (t, v): {[(instant_text(b['t'] * _MS), b['v']) for b in one]}")
    daily = {_new_york_date(b["t"]): b["v"] for b in one}
    checked, misses = 0, []
    for bar in two:
        start = _new_york_date(bar["t"])
        if start + timedelta(days=1) > last:
            continue
        expected = daily.get(start, 0.0) + daily.get(start + timedelta(days=1), 0.0)
        checked += 1
        if abs(bar["v"] - expected) > _SAME_VOLUME * max(expected, 1.0):
            misses.append((start.isoformat(), bar["v"], expected))
    evidence(f"2-day bars checked: {checked}; not their two calendar days: {misses}")
    assert checked >= 4, f"only {checked} 2-day bars inside {first}..{last}"
    assert not misses, f"2-day bars whose volume is not their two calendar days': {misses}"


def _new_york_date(millis: int) -> date:
    """Return the New York date of a bar stamp.

    Args:
        millis: Massive's `t`, in epoch milliseconds.

    Returns:
        The date the stamp falls on in New York time.
    """
    return datetime.fromtimestamp(millis / 1000, UTC).astimezone(_NEW_YORK).date()


def _year_before(month: DaySpan) -> DaySpan:
    """Return the twelve whole months ending with a month.

    Returns:
        The first day of the eleventh month before it and its last day.
    """
    first, last = month
    index = first.year * 12 + first.month - 1 - 11
    return date(index // 12, index % 12 + 1, 1), last


def _instant_text_ns(nanos: int) -> str:
    """Write epoch nanoseconds as UTC and New York wall time.

    Returns:
        Such as `"2025-11-28T18:00:00Z = 13:00 New York"`.
    """
    utc = datetime.fromtimestamp(nanos / 1e9, UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%SZ} = {utc.astimezone(_NEW_YORK):%H:%M} New York"


def _with_bars(
    session: dm.MassiveSession,
    candidates: list[tuple[str, tuple[date, date]]],
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> tuple[str, dict[str, dict[str, Any]], dict[str, dict[str, Any]]] | None:
    """Read the REST day bars of each candidate on both bases until one has every day.

    Args:
        session: The live session.
        candidates: `(symbol, (first, last))` pairs, in order; at most three are tried.
        table_rows: Snapshots a table into rows.
        evidence: Records a candidate that lacks bars.

    Returns:
        The first candidate's symbol and its adjusted and as-traded bars by `Date`, or None
        when none of them has a bar on both bases on the span's first and last day.
    """
    for symbol, span in candidates[:_CANDIDATES]:
        adjusted = _days(session, symbol, span=span, adjusted=True, table_rows=table_rows)
        traded = _days(session, symbol, span=span, adjusted=False, table_rows=table_rows)
        ends = {span[0].isoformat(), span[1].isoformat()}
        if ends <= adjusted.keys() and ends <= traded.keys():
            return symbol, adjusted, traded
        evidence(f"{symbol} {span[0]}..{span[1]}: no bar on both bases of both ends; next")
    return None


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("53")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_split_basis_before_and_on_the_split(
    live_session: dm.MassiveSession,
    *,
    recent_splits: FindSplits,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How a recent forward split shows in adjusted and as-traded REST bars.

    Finds the year's most recent forward split of a common stock with a ratio of at least 2 and no
    later split (`m.stocks.splits`) and records its day bars, with `adjusted=true` and
    `adjusted=false`, on the last trading day before the split and on the execution date. Passes
    when, before the split, the as-traded close is the adjusted close times the ratio and the
    adjusted volume the as-traded volume times the ratio (within 1%), and both bases agree on the
    execution date: `adjusted` restates prices before a split by its ratio, as the library
    documents. With Stocks on Basic only the newest such split is tried (`recent_splits` keeps
    one), so the test sends at most 3 stock requests. Skips when the year had no such split
    with bars.
    """
    splits = recent_splits(live_session)
    found = _with_bars(
        live_session,
        [(s.symbol, (s.before, s.executed)) for s in splits],
        table_rows=table_rows,
        evidence=evidence,
    )
    if found is None:
        pytest.skip(f"no recent forward split with bars on both bases: {splits}")
    symbol, adjusted, traded = found
    split = next(s for s in splits if s.symbol == symbol)
    for day in (split.before.isoformat(), split.executed.isoformat()):
        a, t = adjusted[day], traded[day]
        evidence(
            f"{symbol} {day} (ratio {split.ratio}): adjusted close={a['Close']} "
            f"volume={a['Volume']}; as traded close={t['Close']} volume={t['Volume']}"
        )
    before, executed = split.before.isoformat(), split.executed.isoformat()
    price = float(traded[before]["Close"]) / float(adjusted[before]["Close"])
    volume = float(adjusted[before]["Volume"]) / float(traded[before]["Volume"])
    assert abs(price / split.ratio - 1) < _SPLIT_TOLERANCE, f"price ratio {price}"
    assert abs(volume / split.ratio - 1) < _SPLIT_TOLERANCE, f"volume ratio {volume}"
    same = float(adjusted[executed]["Close"]) / float(traded[executed]["Close"])
    assert abs(same - 1) < _SAME_PRICE, f"the bases differ on the execution date: {same}"


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.verifies("53")
@pytest.mark.requests(12)
@pytest.mark.timeout(100)
def test_day_file_before_a_split_is_as_traded(
    live_session: dm.MassiveSession,
    *,
    recent_splits: FindSplits,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether the stock day file holds a split stock's last day before the split as traded.

    Finds the year's most recent forward split (as the REST split test does) and records the
    stock's `us_stocks_sip/day_aggs_v1` row of the last trading day before it, beside the REST
    as-traded close of that day. Passes when the file's close is the as-traded close, so the
    library rightly serves only `adjusted=False` from the files. Skips when the year had no
    such split with bars.
    """
    splits = recent_splits(live_session)
    found = _with_bars(
        live_session,
        [(s.symbol, (s.before, s.executed)) for s in splits],
        table_rows=table_rows,
        evidence=evidence,
    )
    if found is None:
        pytest.skip(f"no recent forward split with bars on both bases: {splits}")
    symbol, _adjusted, traded = found
    before = next(s for s in splits if s.symbol == symbol).before
    result = flat_file_tickers(_DAY_FILE, before, [symbol])
    rows = [] if result is None else result[0]
    rest_close = float(traded[before.isoformat()]["Close"])
    evidence(f"{symbol} {_DAY_FILE} {before}: {rows}; REST as-traded close {rest_close}")
    assert rows, f"no {symbol} row in the {before} day file"
    assert abs(float(str(rows[0]["close"])) / rest_close - 1) < _SPLIT_TOLERANCE / 10


def _previous_bar_status(client: RestClient) -> str:
    """Return the envelope status of AAPL's previous day bar, as Massive labels it for a key.

    Returns:
        The status, such as `"OK"` or `"DELAYED"`.
    """
    return client.get(
        source(Transport.REST, "/v2/aggs/ticker/{stocksTicker}/prev"),
        {"stocksTicker": _SYMBOL},
        {},
        bucket="stocks",
    ).status


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("73")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_past_stock_data_is_served_with_the_default_config(
    live_config: dm.MassiveConfig,
    *,
    live_rest: RestClient,
    rest_for_key: Callable[[str], RestClient],
    previous_trading_day: date,
    evidence: Evidence,
) -> None:
    """The previous day's bar and a past day's open and close fetch on the default config.

    Massive labels past data `DELAYED` on Basic and Starter keys, though its rows are final, so
    only datasets of the market's current state are checked against
    `MassiveConfig.delayed_subscriptions`. The question needs a key whose past data Massive
    labels `DELAYED`: the test reads AAPL's previous day bar raw and records the envelope
    status, and uses the main key when it says `DELAYED`, else `MASSIVE_DELAYED_API_KEY` when
    that key's answer says so; without such a key it skips, "needs a Basic or Starter stocks
    key". A session on that key with no subscription listed then reads AAPL's
    `previous_day_bar` and its `daily_open_close` of the last trading day before the run.
    Fails if either raises `EntitlementError` or returns no row.
    """
    status = _previous_bar_status(live_rest)
    evidence(f"the main key's previous day bar envelope status: {status!r}")
    key = live_config.api_key
    if status != "DELAYED":
        key = os.environ.get("MASSIVE_DELAYED_API_KEY", "").strip()
        if not key:
            pytest.skip(
                f"needs a Basic or Starter stocks key: Massive labels the main key's past data "
                f"{status!r}, not DELAYED, and MASSIVE_DELAYED_API_KEY is not set"
            )
        status = _previous_bar_status(rest_for_key(key))
        evidence(f"MASSIVE_DELAYED_API_KEY's previous day bar envelope status: {status!r}")
        if status != "DELAYED":
            pytest.skip(
                f"needs a Basic or Starter stocks key: Massive labels the past data of both "
                f"keys {status!r}, not DELAYED"
            )
    config = dataclasses.replace(live_config, api_key=key, delayed_subscriptions=frozenset())
    session = dm.MassiveSession(config)
    try:
        previous = session.stocks.previous_day_bar(_SYMBOL).fetch()
        evidence(f"previous_day_bar fetch(): {previous.size} row(s)")
        day = session.stocks.daily_open_close(_SYMBOL, previous_trading_day).fetch()
        evidence(f"daily_open_close {previous_trading_day} fetch(): {day.size} row(s)")
    finally:
        session.close()
    assert previous.size == 1, "previous_day_bar returned no row"
    assert day.size == 1, f"daily_open_close returned no row for {previous_trading_day}"
