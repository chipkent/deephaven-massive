"""Live checks of stock data against Massive: stream time units, bar alignment and stamps, filings.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked; a test marked `verifies` also asserts the answer the library assumes, and fails
when Massive no longer gives it. Streams assert that Massive acknowledged their topics, wait for
rows only while their market is open, and are released at once; raw bars come from the wire REST
client, because the library filters the very bars in question. How to run these tests is in
`docs/live-tests.md`.

A test marked `massive_issue` checks an open Massive problem: it asserts what Massive's fix
brings, so it fails as expected while the problem lasts, and a pass means Massive fixed it
(`tests/_live/massive_problems.py`).
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from functools import partial
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, WireError, source
from tests._live import recency, recorded_requests, streams
from tests._live.calendar import is_trading_day, trading_day_after, trading_day_before
from tests._live.feeds import with_delayed
from tests._live.massive_problems import expect_fixed

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping, Sequence
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive.queries import HistoryQuery

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
DayBars: TypeAlias = "dict[date, list[StampedBar]]"
DayBarReport: TypeAlias = (
    "Callable[[str, tuple[date, ...], Mapping[str, Mapping[date, list[StampedBar]]]], DayBars]"
)
PrevBarReport: TypeAlias = "Callable[[str], list[StampedBar]]"
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
_DAY_STAMPS = frozenset({(0, 0, 0), (13, 0, 0), (16, 0, 0)})
"""The New York times of day (hour, minute, second) at which grouped daily and the day files are
expected to stamp a stock 1-day bar: midnight, a half day's 13:00 close, and the 16:00 close.
The library also accepts the 04:00 pre-market open, where `/prev` stamps it."""
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


@pytest.mark.verifies("21")
@pytest.mark.timeout(100)
def test_luld_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    market_status: MarketStatus,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
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
    test, quoting the error; a stream Massive refuses skips, quoting Massive.
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
    skip_if_not_entitled(live_session)
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
    rows = table_rows(live_session.stocks.ipos(start=start, end=end).fetch(wait=True))
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
        first, last, source=dm.HistorySource.REST, wait=True
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history {first}..{last} keeps the bars stamped {stamps}")
    assert checked >= 3, f"only {checked} week bars of the four weeks"
    assert not misses, f"week bars closer to another window than the library's: {misses}"


@pytest.mark.verifies("34")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_month_bars_from_mid_month_against_the_whole_month(
    *, recent_month: DaySpan, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether the range returns a partial month bar for a `{from}` inside a month.

    Records `AAPL` month bars over the four months ending with the most recent whole month,
    from the first month's first day and from its 15th. Passes when every bar read from the
    15th is one read from the 1st, with the same stamp and volume: Massive sends no partial bar
    for the month `{from}` falls inside, whether it drops that month or sends it whole. A
    first-month bar with less volume is a partial bar, which the library avoids anyway by
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
    sent = {(bar["t"], bar["v"]) for bar in whole}
    extra = [(bar["t"], bar["v"]) for bar in partial if (bar["t"], bar["v"]) not in sent]
    assert not extra, f"bars from mid-month that are not whole months: {extra}"


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
    return _only_the_symbol(query.history(day, day, source=dm.HistorySource.REST, wait=True))


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
        "library history(REST): "
        + library_stamps(partial(day_bars.history, *days, source=rest, wait=True))
    )
    previous = session.stocks.previous_day_bar(_SYMBOL)
    prev_stamps = library_stamps(partial(previous.fetch, wait=True))
    evidence(f"library previous_day_bar fetch() (/prev): {prev_stamps}")
    for day in days:
        outcome = library_stamps(partial(_symbol_rows, every, day))
        evidence(f"library dm.ALL history(REST) {day} (grouped daily): {outcome}")
    for span in (dm.Timespan.WEEK, dm.Timespan.MONTH):
        query = session.stocks.bars(_SYMBOL, span)
        outcome = library_stamps(
            partial(query.history, *stamp_days[span.value], source=rest, wait=True)
        )
        evidence(f"library {span.value} history: {outcome}")


@pytest.mark.verifies("50")
@pytest.mark.requests(14)
@pytest.mark.timeout(100)
def test_stock_day_and_longer_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    *,
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
    same requests: about 14 requests in all. Passes when every source
    answered and each grouped-daily row is stamped where grouped daily is expected to stamp a
    1-day bar: at 00:00, 13:00, or 16:00 New York time of its own day. Reading the rest: range
    stamps there, a `/prev` stamp there or at 04:00, longer bars at 00:00 New York time on
    their period's first day (or the weekend before a week's Monday), and matching values
    confirm the library's day label; minute bars ending at 12:59 on the half day confirm a
    whole day cut short; a raw stamp of any other form is withheld by the library, with a
    WARNING in the log.
    Grouped-daily volume runs a little below the range bar's, so only the stamps are asserted.
    """
    days = stamp_days["day"]
    grouped = {
        day: [
            stamped_bar(r) for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _SYMBOL
        ]
        for day in days
    }
    ranged = day_bar_report(_SYMBOL, days, {"grouped daily": grouped})
    minutes = [minute_span_report(_SYMBOL, day) for day in days]
    spans = {span: stamp_days[span] for span in ("week", "month")}
    periods = period_bar_report(_SYMBOL, spans, {"week": (2, 2), "month": (2, 2)})
    assert prev_bar_report(_SYMBOL), "/prev returned no bar"
    _record_library_stock_stamps(
        live_session, stamp_days=stamp_days, library_stamps=library_stamps, evidence=evidence
    )
    assert all(ranged.values()), f"a stamp day has no range bar: {ranged}"
    assert all(minutes), f"a stamp day has no minute bars: {dict(zip(days, minutes, strict=True))}"
    assert all(grouped.values()), f"a stamp day has no grouped-daily row: {grouped}"
    assert periods, "the range did not return one bar for each of two weeks and two months"
    odd = {day: [b[0] for b in bars if not _day_stamp(b[0], day)] for day, bars in grouped.items()}
    assert not any(odd.values()), f"grouped-daily stamps the library does not expect: {odd}"


@pytest.mark.verifies("50")
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
    days; and the library's `history(source=FLAT_FILES)` windows. Passes when the file holds
    each day and stamps it where the library expects a 1-day bar: at 00:00, 13:00, or 16:00 New
    York time of its own day. Reading the rest: a match with the range bar of the same day, and
    minute rows ending where the REST minute bars end, confirm one day cut for files and REST.
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
    ranged = day_bar_report(_SYMBOL, days, {_DAY_FILE: files})
    as_traded = live_session.stocks.bars(_SYMBOL, dm.Timespan.DAY, adjusted=False)
    outcome = library_stamps(
        partial(as_traded.history, *days, source=dm.HistorySource.FLAT_FILES, wait=True)
    )
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert all(ranged.values()), f"a stamp day has no range bar: {ranged}"
    assert all(files.values()), f"the day file lacks a stamp day: {files}"
    odd = {day: [b[0] for b in bars if not _day_stamp(b[0], day)] for day, bars in files.items()}
    assert not any(odd.values()), f"day-file stamps the library does not expect: {odd}"


_PREV = "/v2/aggs/ticker/{stocksTicker}/prev"
_VOLUME_DAYS: Final = (date(2025, 12, 23), date(2025, 12, 24))
"""The days on which grouped daily was first seen to report less AAPL volume than the range day
bar and the day file: 29,593,863 against 29,641,999, and 17,892,492 against 17,910,574."""


def _new_york_text(millis: int) -> str:
    """Write a bar stamp as New York time, the zone the stock day-bar stamps are read in.

    Args:
        millis: Massive's `t`, in epoch milliseconds.

    Returns:
        Text such as `"2026-11-18 04:00 New York"`.
    """
    local = datetime.fromtimestamp(millis / 1000, UTC).astimezone(_NEW_YORK)
    return f"{local:%Y-%m-%d %H:%M} New York"


def _same_prices(left: StampedBar, right: StampedBar) -> bool:
    """Tell whether two stamped bars have the same open, high, low, and close.

    Returns:
        True when each price is present on both and they agree within `_SAME_PRICE`.
    """
    pairs = list(zip(left[1][:4], right[1][:4], strict=True))
    return all(
        a is not None and b is not None and abs(a - b) <= _SAME_PRICE * max(abs(a), abs(b))
        for a, b in pairs
    )


def _same_volume(left: StampedBar, right: StampedBar) -> bool:
    """Tell whether two stamped bars have the same volume, within `_SAME_VOLUME`.

    Returns:
        True when both have a volume and they agree.
    """
    a, b = left[1][4], right[1][4]
    return a is not None and b is not None and abs(a - b) <= _SAME_VOLUME * max(abs(a), abs(b))


def _only_bar(bars: Sequence[StampedBar], name: str, day: date) -> StampedBar:
    """Return the one bar a source sent for a day, failing plainly when it sent none or several.

    Returns:
        The bar.
    """
    assert len(bars) == 1, f"{name} sent {len(bars)} {_SYMBOL} bars for {day}: {bars}"
    return bars[0]


@pytest.mark.massive_issue(792)
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_a_stock_day_bar_has_one_stamp_on_every_rest_source(
    *, rest_rows: RestRows, stamped_bar: ToStampedBar, evidence: Evidence
) -> None:
    """Whether `/prev`, the range endpoint, and grouped daily stamp AAPL's day bar alike.

    Reads AAPL's `/prev` bar, the range endpoint's 1-day bar of the same New York day, and that
    day's grouped-daily row: 3 requests. Today Massive stamps one bar three ways: 00:00 New York
    time on the range, 04:00 on `/prev`, and 16:00 on grouped daily (and `/prev` moved from 16:00
    to 04:00 without notice). The evidence first asserts that each source sent one bar of the
    day with the same prices, so the three stamps belong to one bar. A pass means Massive
    stamps the bar alike on all three: close the issue, then update the library's per-source
    stamp handling and the dataset docs that list each source's stamp.
    """
    prevs = rest_rows(_PREV, {"stocksTicker": _SYMBOL}, {})
    assert len(prevs) == 1, f"/prev sent {len(prevs)} {_SYMBOL} bars: {prevs}"
    prev = stamped_bar(prevs[0])
    day = _new_york_date(prev[0])
    params = {"stocksTicker": _SYMBOL, "multiplier": 1, "timespan": "day", "from": day, "to": day}
    ranged = [stamped_bar(r) for r in rest_rows(_RANGE, params, {})]
    grouped = [
        stamped_bar(r) for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _SYMBOL
    ]
    bars = {
        f"GET {_PREV}": prev,
        f"GET {_RANGE} {day}..{day}": _only_bar(
            [b for b in ranged if _new_york_date(b[0]) == day], "the range", day
        ),
        f"GET {_GROUPED} {day}": _only_bar(grouped, "grouped daily", day),
    }
    stamps = {request: f"t={bar[0]} ({_new_york_text(bar[0])})" for request, bar in bars.items()}
    evidence(f"{_SYMBOL} day bar of {day}: {stamps}")
    assert all(_same_prices(bar, prev) for bar in bars.values()), f"not one bar: {bars}"
    expect_fixed(
        evidence=evidence,
        fixed=len({bar[0] for bar in bars.values()}) == 1,
        shown=f"{_SYMBOL} day bar of {day}, its stamp by source: {stamps}",
    )


def _expect_equal_day_volumes(
    days: Sequence[date],
    *,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    evidence: Evidence,
) -> None:
    """Expect grouped daily's AAPL volume of each day to equal the range day bar's and the file's.

    Reads the as-traded range day bars over the days (1 request), each day's as-traded
    grouped-daily row (1 request a day), and each day's `us_stocks_sip/day_aggs_v1` row, which is
    as traded. Asserts first that every source sent one bar of each day, with the same prices,
    so the volumes compared belong to one bar; then expects the three volumes of every day
    equal, so a fix of only some days still shows the problem.
    """
    params = {"stocksTicker": _SYMBOL, "multiplier": 1, "timespan": "day"}
    params |= {"from": days[0], "to": days[-1]}
    ranged = [stamped_bar(r) for r in rest_rows(_RANGE, params, {"adjusted": False})]
    differ: list[str] = []
    for day in days:
        grouped_rows = rest_rows(_GROUPED, {"date": day}, {"adjusted": False})
        read = flat_file_tickers(_DAY_FILE, day, [_SYMBOL])
        assert read is not None, f"{_DAY_FILE} has no file for {day}"
        bars = {
            "range": _only_bar(
                [b for b in ranged if _new_york_date(b[0]) == day], "the range", day
            ),
            "grouped daily": _only_bar(
                [stamped_bar(r) for r in grouped_rows if r.get("T") == _SYMBOL],
                "grouped daily",
                day,
            ),
            _DAY_FILE: _only_bar([stamped_bar(r) for r in read[0]], _DAY_FILE, day),
        }
        volumes = {name: bar[1][4] for name, bar in bars.items()}
        evidence(f"{_SYMBOL} {day} volumes: {volumes}")
        assert all(_same_prices(bar, bars["range"]) for bar in bars.values()), f"{day}: {bars}"
        # A missing volume is no measurement, so it never reads as the volumes disagreeing.
        assert all(v is not None for v in volumes.values()), f"{day}: no volume in {volumes}"
        grouped = bars["grouped daily"]
        if not (_same_volume(grouped, bars["range"]) and _same_volume(grouped, bars[_DAY_FILE])):
            differ.append(
                f"{day}: GET {_GROUPED} adjusted=false v={volumes['grouped daily']}, "
                f"GET {_RANGE} adjusted=false v={volumes['range']}, "
                f"{_DAY_FILE} volume={volumes[_DAY_FILE]}"
            )
    expect_fixed(
        evidence=evidence,
        fixed=not differ,
        shown=f"{_SYMBOL} volumes that differ, {len(differ)} of {len(days)} days: {differ}",
    )


@pytest.mark.massive_issue(799)
@pytest.mark.requests(3)
@pytest.mark.timeout(200)
def test_grouped_daily_stock_volume_equals_the_range_bar_on_the_reported_days(
    *,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    evidence: Evidence,
) -> None:
    """Whether grouped daily's AAPL volume equals the range bar's and the day file's, 2025-12-23/24.

    These are the days the problem was reported on, so a pass here means Massive corrected the
    data it already served. Today grouped daily sends less volume than the range day bar and the
    day file, which agree: 29,593,863 against 29,641,999 on 2025-12-23 (0.16% lower), and
    17,892,492 against 17,910,574 on 2025-12-24, with the same prices on all three. Reads the
    range once, grouped daily for each day, and each day's day file: 3 requests and two files.
    A pass means Massive fixed these days: close the issue if the recent-days check passes too,
    and remove the stamp test's note that grouped-daily volume runs lower.
    """
    _expect_equal_day_volumes(
        _VOLUME_DAYS,
        rest_rows=rest_rows,
        flat_file_tickers=flat_file_tickers,
        stamped_bar=stamped_bar,
        evidence=evidence,
    )


@pytest.mark.massive_issue(799)
@pytest.mark.requests(6)
@pytest.mark.timeout(300)
def test_grouped_daily_stock_volume_equals_the_range_bar_on_recent_days(
    *,
    recent_week: DaySpan,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    evidence: Evidence,
) -> None:
    """Whether grouped daily's AAPL volume equals the range bar's and the day file's this week.

    Compares the five trading days of the most recent ended week without a holiday, so a fix
    of only the data Massive builds from now on shows apart from a correction of the old days
    (the reported-days check), and one lucky day does not read as a fix. Grouped daily has sent
    less volume than the range day bar and the day file, which agree, with the same prices on
    all three. Reads the range once, grouped daily for each day, and each day's day file: 6
    requests and five files. A pass means Massive's recent data is fixed: close the issue if the
    reported-days check passes too, and remove the stamp test's note that grouped-daily volume
    runs lower.
    """
    monday = recent_week[0]
    _expect_equal_day_volumes(
        [monday + timedelta(days=n) for n in range(5)],
        rest_rows=rest_rows,
        flat_file_tickers=flat_file_tickers,
        stamped_bar=stamped_bar,
        evidence=evidence,
    )


_DIVIDEND_TRIES: Final = 10
"""How many `stock_dividend` events the dividend check tries for one whose ticker has day bars:
most such tickers are thin over-the-counter issues without a bar in a given month, so a handful
is not enough."""

_DIVIDEND_WINDOW: Final = timedelta(days=30)
"""How far before a `stock_dividend` event's execution date the check looks for a day bar: a
thin ticker may go weeks without a trade."""


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
        span[0], span[1], source=dm.HistorySource.REST, wait=True
    )
    return {str(row["Date"]): row for row in table_rows(table)}


@pytest.mark.verifies("54")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_every_page_echoes_its_adjustment(
    live_rest: RestClient, *, recent_week: DaySpan, evidence: Evidence
) -> None:
    """Whether every aggregates page echoes `adjusted`, `next_url` pages and grouped daily too.

    Pages through the minute bars of AAPL over the most recent week without a holiday and the
    week before, with a small `limit`, so the range answers in several pages, and reads the
    grouped daily of that week's Monday with `adjusted=true` and its Tuesday with
    `adjusted=false`, so an echo that follows the request can be told from a constant, recording
    each page's `adjusted` echo. Passes when every page echoes the value it was sent, which the
    library's page-by-page check relies on; a missing echo on a later page means the check can
    only warn there.
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
    grouped_echoes = []
    for day, adjusted in ((monday, True), (monday + timedelta(days=1), False)):
        page = live_rest.get(grouped, {"date": day}, {"adjusted": adjusted}, bucket="stocks")
        evidence(f"grouped daily {day} adjusted={adjusted}: adjusted echo {page.adjusted}")
        grouped_echoes.append((adjusted, page.adjusted))
    assert len(echoes) > 1, f"the range answered in {len(echoes)} page"
    assert all(echo is True for echo in echoes), f"range pages' echoes: {echoes}"
    assert all(sent is echo for sent, echo in grouped_echoes), f"grouped: {grouped_echoes}"


@pytest.mark.verifies("55")
@pytest.mark.requests(13)
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
    date to two days after, with `adjusted=true` and `adjusted=false`, trying the next
    dividend while a stock's bars differ by basis or miss a side of the ex-date (a thinly
    traded stock may not trade for days). Passes when the ratio of the adjusted to the
    as-traded close takes no step across the ex-dividend date: the same ratio on every day, so
    `adjusted` concerns splits only, as the library documents, and an adjustment for the
    dividend would show as a step on the ex-date. It tries up to three such dividends, two
    requests each, after the run's search for them (`special_dividends`, up to seven requests,
    sent once a run): at most 13 stock requests. Skips when no dividend tried has bars on both
    sides.
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
        ex_date = dividend.ex_date.isoformat()
        both_sides = bool(adjusted) and min(adjusted) < ex_date <= max(adjusted)
        if both_sides and adjusted.keys() == traded.keys():
            found = dividend, adjusted, traded
            break
        evidence(
            f"{dividend.symbol} {span[0]}..{span[1]}: days {sorted(adjusted)} adjusted, "
            f"{sorted(traded)} as traded: no bar on a basis or on a side of the ex-date; next"
        )
    if found is None:
        pytest.skip(f"no recent special cash dividend with bars around it: {dividends}")
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
    steps = [r / before[-1] - 1 for r in (*before, *after)]
    assert all(abs(step) <= _SAME_PRICE for step in steps), f"the ratio steps: {ratios}"


@pytest.mark.settles("56")
@pytest.mark.requests(12)
@pytest.mark.timeout(100)
def test_stock_dividend_events_and_the_adjustment(
    live_session: dm.MassiveSession,
    *,
    recent_month: DaySpan,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether `adjusted=true` applies a `stock_dividend` event of the splits dataset.

    Walks the most recent `stock_dividend` events of the year ending with the most recent whole
    month, newest first, until one's ticker has a day bar in the 30 days before its execution
    date (many such tickers are thinly traded or delisted), at most 10 of them, and records each
    event's adjustment factor and that ticker's day bars on both bases across the
    date. Skips as not exercised when none has such a bar; fails when the as-traded bars lack
    the earlier day the adjusted ones hold.
    Reading it: prices before the date that differ by the event's factor mean such events are
    adjusted like splits; equal prices mean they are not, and the README's recipe should skip
    them.
    """
    start, end = _year_before(recent_month)
    events = table_rows(
        live_session.stocks.splits(
            start=start,
            end=end,
            sort="execution_date",
            order=dm.Order.DESC,
            filters={"adjustment_type": "stock_dividend"},
            limit=_DIVIDEND_TRIES,
        ).fetch(wait=True)
    )
    listed = [(e["Symbol"], e["ExecutionDate"], e["HistoricalAdjustmentFactor"]) for e in events]
    evidence(f"stock_dividend events (symbol, execution date, adjustment factor): {listed}")
    for event in events:
        day = date.fromisoformat(str(event["ExecutionDate"]))
        span = (day - _DIVIDEND_WINDOW, day + timedelta(days=3))
        symbol = str(event["Symbol"])
        adjusted = _days(live_session, symbol, span=span, adjusted=True, table_rows=table_rows)
        earlier = sorted(d for d in adjusted if d < day.isoformat())
        if earlier:
            break
        evidence(f"{symbol} has no day bar from {span[0]} to before {day}")
    else:
        pytest.skip(
            f"not exercised: none of {len(events)} stock_dividend events from {start} to {end} "
            "has a day bar before its date"
        )
    traded = _days(live_session, symbol, span=span, adjusted=False, table_rows=table_rows)
    for row_day in sorted(adjusted):
        evidence(
            f"{symbol} {row_day}: adjusted close={adjusted[row_day]['Close']} "
            f"traded close={traded.get(row_day, {}).get('Close')}"
        )
    assert earlier[-1] in traded, f"{symbol} has no as-traded bar of {earlier[-1]}"


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
    elsewhere are still served at those stamps, with no `Date` and a WARNING in the log, and
    say which day each value really belongs to.
    """
    # The wire client encodes a date parameter from a `date`, never from text. The raw request
    # takes an exclusive bound, the day after the week; the library's `end` is inclusive.
    start, end = recent_week[0], recent_week[1]
    found = 0
    paths = {"AAPL": ("stockTicker", "AAPL"), "X:BTCUSD": ("cryptoTicker", "X:BTCUSD")}
    for symbol, (name, ticker) in paths.items():
        query = {"timespan": "day", "window": 5, "limit": 50}
        query |= {"timestamp.gte": start, "timestamp.lt": end + timedelta(days=1)}
        raw = rest_rows(f"/v1/indicators/sma/{{{name}}}", {name: ticker}, query)
        found += len(raw)
        sent = [instant_text(r["timestamp"] * _MS) for r in raw]
        evidence(f"{symbol} daily SMA stamps as sent: {sent}")
        q = live_session.indicators.sma(symbol, 5, start=start, end=end, timespan=dm.Timespan.DAY)
        try:
            rows = table_rows(q.fetch(wait=True))
        except dm.MassiveError as error:
            evidence(f"{symbol} daily SMA failed in the library: {error}")
            continue
        stamps = [(instant_text(r["TimestampStart"]), r["Date"]) for r in rows]
        evidence(f"{symbol} daily SMA (TimestampStart, Date): {stamps}")
    assert found


@pytest.mark.verifies("62")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_unknown_ticker_statuses(
    live_rest: RestClient, *, recent_week: DaySpan, evidence: Evidence
) -> None:
    """What each endpoint answers for a ticker that does not exist.

    Requests `ZZZZZQ` from `/prev`, the range (over the most recent week without a holiday),
    `/v3/trades`, and the ticker details, and records each answer's status and row count.
    Passes when each answer is an empty 200 or a 404, both of which the library reads as no
    rows; a 404 is the only way a symbol is dropped with a WARNING. Another status, or rows,
    means the library would report an error, or data, for a ticker that does not exist.
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
    unexpected = []
    for template, params in requests:
        # Ticker details are reference data of no subscription; the rest are stocks data.
        bucket = "other" if template.startswith("/v3/reference/") else "stocks"
        try:
            page = live_rest.get(source(Transport.REST, template), params, {}, bucket=bucket)
        except WireError as error:
            evidence(f"{template}: HTTP {error.status} {error.message[:120]}")
            if error.status != 404:
                unexpected.append(f"{template}: HTTP {error.status}")
        else:
            evidence(f"{template}: HTTP 200, status {page.status}, {len(page.rows)} rows")
            if page.rows:
                unexpected.append(f"{template}: {len(page.rows)} rows")
    assert not unexpected, f"answers other than an empty 200 or a 404: {unexpected}"


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


def _day_stamp(millis: int, day: date) -> bool:
    """Tell whether a 1-day bar's stamp is one the library expects for its day.

    Args:
        millis: The stamp, in epoch milliseconds.
        day: The bar's trading day.

    Returns:
        True for 00:00, 13:00 (a half day's close), or 16:00 (the regular close) New York time
        of `day`.
    """
    local = datetime.fromtimestamp(millis / 1000, UTC).astimezone(_NEW_YORK)
    return local.date() == day and (local.hour, local.minute, local.second) in _DAY_STAMPS


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


@pytest.mark.verifies("53")
@pytest.mark.requests(12)
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
    documents. It tries up to three such splits, two requests each, after the run's search for
    them (`recent_splits`, up to six requests, sent once a run): at most 12 stock requests. Skips
    when the year had no such split with bars.
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


@pytest.mark.verifies("73")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_past_stock_data_is_served_with_the_default_config(
    live_config: dm.MassiveConfig,
    *,
    live_rest: RestClient,
    previous_trading_day: date,
    evidence: Evidence,
) -> None:
    """The previous day's bar and a past day's open and close fetch on the default config.

    Massive labels past data `DELAYED` on some keys, though its rows are final, so only
    datasets of the market's current state are checked against
    `MassiveConfig.delayed_subscriptions`. The question needs a key whose past data Massive
    labels `DELAYED`: the test reads AAPL's previous day bar raw, records the envelope status,
    and skips, quoting it, when the status is not `DELAYED`. A session on the key with no
    subscription listed then reads AAPL's `previous_day_bar` and its `daily_open_close` of the
    last trading day before the run. Fails if the library refuses either read as delayed data
    the settings do not list (an `EntitlementError` whose source is `ErrorSource.USER`, which
    the test catches so the live-test hook does not skip on it), or if either returns no row.

    Raises:
        EntitlementError: When Massive refuses a read, which skips the test, quoting Massive.
    """
    status = _previous_bar_status(live_rest)
    evidence(f"the key's previous day bar envelope status: {status!r}")
    if status != "DELAYED":
        pytest.skip(
            f"Massive labels this key's past stock data {status!r}; the question is about a key "
            f"whose past data it labels DELAYED"
        )
    session = dm.MassiveSession(with_delayed(live_config, frozenset()))
    try:
        previous = session.stocks.previous_day_bar(_SYMBOL).fetch(wait=True)
        evidence(f"previous_day_bar fetch(): {previous.size} row(s)")
        day = session.stocks.daily_open_close(_SYMBOL, previous_trading_day).fetch(wait=True)
        evidence(f"daily_open_close {previous_trading_day} fetch(): {day.size} row(s)")
    except dm.EntitlementError as error:
        if error.source is not dm.ErrorSource.USER:
            raise  # Massive's refusal: the live-test hook skips, quoting it
        # The settings' refusal of delayed data is the failure the question asks about; the
        # hook would report it as a skip.
        refusal = " ".join(str(error).split())
        evidence(f"the library refused past data on the default config: {refusal}")
        pytest.fail(f"the library refused past data on the default config: {refusal}")
    finally:
        session.close()
    assert previous.size == 1, "previous_day_bar returned no row"
    assert day.size == 1, f"daily_open_close returned no row for {previous_trading_day}"


# --- The overnight stock session ----------------------------------------------------------------

_OVERNIGHT_SYMBOLS: Final = ("SPY", "QQQ", "AAPL")
"""Stocks whose trades the overnight stream test waits for: among the most traded in the regular
session."""


@pytest.mark.requests(2)
@pytest.mark.timeout(60)
def test_what_massive_calls_the_overnight_stock_session(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What `/v1/marketstatus/now` and the unified snapshot say in the overnight stock session.

    The live tests read `/v1/marketstatus/now` to decide whether stock rows are due, and know
    its statuses by name (`streams.MarketState`); no Massive page says what it reports in the
    overnight session. Runs between 21:00 and 04:00 New York time on a night before a trading
    day (`streams.stock_session`), and skips at any other hour. Reads `/v1/marketstatus/now`,
    which belongs to no subscription, records its `market`, `exchanges`, `earlyHours`, and
    `afterHours`, and fails when `market` is a status the tests do not know
    (`streams.market_state`), naming it, so the tests learn it by name. Then reads SPY's
    unified snapshot and records its `market_status`, which Massive documents as
    `overnight_trading` in this session from 2026-12-07, and its `session`; a refused snapshot
    skips, quoting Massive. A `market` of `closed` beside a snapshot of `overnight_trading`
    means the tests need another sign of the session.
    """
    now = datetime.now(UTC)
    session = streams.stock_session(now)
    evidence(f"New York clock {now.astimezone(_NEW_YORK)}: {session.value}")
    if session is not streams.StockSession.OVERNIGHT:
        pytest.skip(f"not the overnight window: the clock is in {session.value}")
    body = live_rest.get(source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other")
    status: Any = body.body
    fields = ("market", "exchanges", "earlyHours", "afterHours", "serverTime")
    evidence(f"/v1/marketstatus/now: { ({f: status.get(f) for f in fields}) }")
    state = streams.market_state(str(status.get("market")))
    evidence(f"market status read as {state.name}")
    snapshot = live_rest.get(
        source(Transport.REST, "/v3/snapshot"), {}, {"ticker.any_of": "SPY"}, bucket="stocks"
    ).rows
    evidence(
        "SPY unified snapshot: "
        f"{[(row.get('market_status'), row.get('session')) for row in snapshot]}"
    )


def _ended_trading_day(wall: datetime) -> date:
    """Return the latest trading day whose after-hours session has ended by a New York time.

    Returns:
        The day of `wall` from 20:00 on a trading day, else the trading day before it.
    """
    ended = wall >= datetime.combine(wall.date(), streams.EXTENDED_CLOSE, _NEW_YORK)
    return wall.date() if ended and is_trading_day(wall.date()) else trading_day_before(wall.date())


def _last_trade(live_rest: RestClient, day: date) -> list[datetime]:
    """Return when the last SPY trade Massive files under a date happened, in New York time.

    One request: `/v3/trades/SPY` with `timestamp` the date, newest first, one trade.

    Returns:
        The trade's SIP time, or nothing when Massive has no trade under the date.
    """
    trades = live_rest.get(
        source(Transport.REST, "/v3/trades/{stockTicker}"),
        {"stockTicker": "SPY"},
        {"timestamp": day, "order": "desc", "sort": "timestamp", "limit": 1},
        bucket="stocks",
    ).rows
    return [datetime.fromtimestamp(int(str(t["sip_timestamp"])) / 1e9, _NEW_YORK) for t in trades]


@pytest.mark.settles("144")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_which_day_massive_counts_overnight_stock_trades_in(
    rest_rows: RestRows, live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Which trading day Massive counts SPY's trades from 21:00 to midnight in.

    Takes the latest trading day D whose after-hours session has ended, and reads SPY's
    unadjusted minute bars from 21:00 on the evening before D to 00:00 after D. Skips,
    recording the count, when no minute starts before 04:00 on D: Massive had no overnight
    session before D, as before 2026-12-07. Otherwise records three answers, each read by a
    person:

    - D's day bar's open beside the opens of the first minute from 21:00 the evening before,
      from midnight, and from 04:00: the one it equals is where Massive's day of D begins;
    - the time of the last trade Massive files under the date before D (`/v3/trades` with
      `timestamp` that date, newest first), as supporting evidence only: near 20:00 would mean
      the evening's overnight trades count in D, but Massive does not document which day or
      zone a date `timestamp` selects;
    - D's day-bar volume beside the minutes' volume over the trade-date span (21:00 before D
      to 20:00 on D) and over D's calendar day, as supporting evidence, since late prints and
      corrections can keep either sum from matching.

    Passes when the requests worked.
    """
    day = _ended_trading_day(datetime.now(_NEW_YORK))
    start = datetime.combine(day - timedelta(days=1), streams.OVERNIGHT_OPEN, _NEW_YORK)
    midnight = datetime.combine(day, clock_time(0), _NEW_YORK)
    opening = datetime.combine(day, streams.EXTENDED_OPEN, _NEW_YORK)
    close = datetime.combine(day, streams.EXTENDED_CLOSE, _NEW_YORK)
    end = midnight + timedelta(days=1)
    params = {"stocksTicker": "SPY", "multiplier": 1, "timespan": "minute"}
    params |= {"from": start, "to": end - timedelta(milliseconds=1)}
    minutes = rest_rows(_RANGE, params, {"sort": "asc", "limit": 50_000, "adjusted": False})
    bars = [(int(bar["t"]), float(bar["o"]), float(bar["v"])) for bar in minutes]
    overnight = [t for t, _, _ in bars if t < opening.timestamp() * 1000]
    evidence(f"SPY minutes from {start} to {end}: {len(bars)}; before {opening}: {len(overnight)}")
    if not overnight:
        pytest.skip(f"Massive has no SPY minute between {start} and {opening}")
    day_params = {"stocksTicker": "SPY", "multiplier": 1, "timespan": "day"}
    day_params |= {"from": day, "to": day}
    days = rest_rows(_RANGE, day_params, {"adjusted": False})
    evidence(f"SPY day bar of {day}: {[(b.get('t'), b.get('o'), b.get('v')) for b in days]}")
    for since in (start, midnight, opening):
        first = next((bar for bar in bars if bar[0] >= since.timestamp() * 1000), None)
        evidence(f"open of the first minute from {since}: {first and first[1]}")
    before = day - timedelta(days=1)
    evidence(f"last SPY trade Massive files under {before}: {_last_trade(live_rest, before)}")

    def volume(low: datetime, high: datetime) -> float:
        lo, hi = low.timestamp() * 1000, high.timestamp() * 1000
        return sum(v for t, _, v in bars if lo <= t < hi)

    evidence(f"minute volume, trade-date span {start} to {close}: {volume(start, close)}")
    evidence(f"minute volume, calendar day {midnight} to {end}: {volume(midnight, end)}")


@pytest.mark.settles("144")
@pytest.mark.timeout(200)
def test_overnight_stock_trades_stream(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    market_status: MarketStatus,
    stream_report: Report,
    skip_if_not_entitled: Callable[[dm.MassiveSession], None],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """What the trade stream sends in the overnight stock session, and the `Date` it gets.

    Streams the trades of SPY, QQQ, and AAPL and asserts that Massive acknowledged the three
    topics, whatever the hour. Waits for trades only in the overnight window
    (`streams.stock_session`) while Massive's stock status says the market trades
    (`streams.market_state`); at any other time it skips after the acknowledgement. Records
    how many trades came, their conditions and exchanges, and for the first five trades the
    library's `Date`, the New York calendar date, beside the date the trade counts in under
    the trade-date convention Massive's blog describes (the next trading day for one at 21:00
    or later). No trade in the wait skips: one minute of the overnight session may have
    none. A refused stream skips, quoting Massive; a failed one fails the test.
    """
    table = live_session.stocks.trades(list(_OVERNIGHT_SYMBOLS)).append_only()
    acked = wait_acked(live_session, len(_OVERNIGHT_SYMBOLS), time.monotonic() + _STREAM_WAIT)
    now = datetime.now(UTC)
    session = streams.stock_session(now)
    status = market_status("stocks")
    trading = streams.market_state(status) is not streams.MarketState.CLOSED
    overnight = session is streams.StockSession.OVERNIGHT and trading
    deadline = time.monotonic() + _STREAM_WAIT
    received = wait_rows(live_session, table, deadline) if overnight else []
    rows, failure = _stream_evidence(
        live_session,
        table,
        rows=received,
        report=stream_report,
        evidence=evidence,
        table_rows=table_rows,
    )
    skip_if_not_entitled(live_session)
    assert failure is None, f"the trade stream failed: {failure}"
    assert acked >= len(_OVERNIGHT_SYMBOLS), "Massive did not acknowledge every trade topic"
    if not overnight:
        pytest.skip(f"not the overnight session: {session.value}, Massive says {status}")
    evidence(f"{len(rows)} trades; conditions {sorted({str(r['Conditions']) for r in rows})}")
    evidence(f"exchanges {sorted({r['Exchange'] for r in rows})}")
    for row in rows[:5]:
        stamp = datetime.fromtimestamp(int(row["Timestamp"]) / 1e9, _NEW_YORK)
        late = stamp.time() >= streams.OVERNIGHT_OPEN
        counted = trading_day_after(stamp.date()) if late else stamp.date()
        evidence(f"{row['Symbol']} at {stamp}: Date {row['Date']}, trade date {counted}")
    if not rows:
        pytest.skip(f"no trade in {_STREAM_WAIT:.0f} s of the overnight session")


_MIXED_CLASSES = ("AAPL", "I:SPX")
"""A stock and an index, which the unified snapshot answers in one request."""


@pytest.mark.settles("149")
@pytest.mark.requests(1)
@pytest.mark.timeout(60)
def test_a_mixed_class_snapshot_labels_rows_not_the_envelope(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether the unified snapshot of a stock and an index labels the envelope `DELAYED`.

    Requests `/v3/snapshot` with `AAPL` and `I:SPX` in one `ticker.any_of`, one request, and
    records the envelope's `status`, each row's ticker, `type`, `market_status`, `error`, and
    recency labels (`timeframe` and its dotted forms), and the labels read as the library reads
    them (`read_labels`) for the whole answer and for each row alone. A request Massive refuses
    skips, quoting Massive, and its text is the evidence of what a mixed request does when the
    key lacks one class. Skips, quoting the labels, when no label of the answer says `DELAYED`:
    every label is real-time, so the question about a delayed label has no subject. Passes when
    some label says `DELAYED` and the answer has a row. The evidence says whether the envelope
    carries the label (`status=DELAYED`), or only some rows do; an envelope
    that is `OK` beside a delayed row means a mixed request is labelled by row only.
    """
    page = recorded_requests.get(
        live_rest,
        source(Transport.REST, "/v3/snapshot"),
        path_params={},
        query={"ticker.any_of": list(_MIXED_CLASSES)},
        bucket="stocks",
        evidence=evidence,
    )
    evidence(f"envelope status {page.status!r}; {len(page.rows)} rows")
    for row in page.rows:
        fields = {
            key: row.get(key) for key in ("ticker", "type", "market_status", "error", "message")
        }
        label = recency.answered("row", "", [row]).text()
        evidence(f"row {fields}; {label}")
    whole = recency.answered("answer", page.status, page.rows)
    evidence(whole.text())
    if not whole.delayed:
        pytest.skip(f"no label says DELAYED, so nothing to place: {whole.text()}")
    assert page.rows, "Massive labelled the answer delayed and sent no row"
    evidence(f"the envelope says DELAYED: {page.status == 'DELAYED'}")
