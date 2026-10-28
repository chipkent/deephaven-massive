"""Live checks of stock data against Massive: stream time units, bar alignment, and filings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked,
except where the check is a plain yes or no. Streams subscribe to one symbol, wait for their
first rows, and are released at once; raw bars come from the wire REST client, because the
library filters the very bars in question. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
HalfDayReport: TypeAlias = "Callable[[str, str], list[str]]"
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_SYMBOL = "AAPL"
_RANGE = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_MS = 1_000_000
_NEW_YORK = ZoneInfo("America/New_York")
_NVDA = (date(2024, 6, 7), date(2024, 6, 10))
"""NVDA's last day before its 10-for-1 split, and the split's execution date."""
_COST = (date(2023, 12, 22), date(2023, 12, 29))
"""The days around COST's special dividend, ex-dividend on 2023-12-27."""
_HALF_DAYS = (date(2025, 11, 28), date(2025, 12, 24))
"""US market half days, closing at 13:00 New York time."""
_GROUPED = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
_DAY_NS = 86_400 * 1_000_000_000
_STREAM_WAIT = 60.0
"""Seconds a stream test waits for its first frame; stocks trade on weekdays only."""

_FILINGS_LIMIT = 5_000
"""The most rows read of each filings dataset; a result of exactly this many is cut short."""


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
    wait_rows: WaitRows,
    report: Report,
    evidence: Evidence,
    table_rows: TableRows,
) -> tuple[list[dict[str, Any]], str | None]:
    """Wait for a stream's first rows, record the diagnostics, then release the stream.

    The diagnostics are read before the release, which the session carries out on another
    thread, so they show the stream's acknowledgement and error as they were.

    Returns:
        The rows received before the wait ended, and the error of a stream that failed (None
        when none did).
    """
    rows = wait_rows(session, table, time.monotonic() + _STREAM_WAIT)
    for line in report(session):
        evidence(line)
    failed = [r["Error"] for r in table_rows(session.streams()) if r["State"] == "FAILED"]
    session.release(table)
    return rows, (str(failed[0]) if failed else None)


@pytest.mark.needs("stocks:advanced")
@pytest.mark.settles("21")
@pytest.mark.timeout(100)
def test_luld_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
    table_rows: TableRows,
) -> None:
    """Whether a WebSocket `LULD` frame's `t` converts to the current instant.

    `LULD` `t` is documented in milliseconds while the sample has 19 digits; the library reads
    nanoseconds and fails loudly (a failed stream) on a mismatch. Run this test in the first
    minutes after the 09:30 New York open, when bands are published for every symbol; later,
    one symbol's bands change only on a large move. Reading it: an instant within a day of now
    confirms nanoseconds; a failed stream naming the bound means milliseconds (the test then
    fails, quoting the error).
    """
    table = live_session.stocks.luld(_SYMBOL).append_only()
    rows, failure = _stream_evidence(
        live_session,
        table,
        wait_rows=wait_rows,
        report=stream_report,
        evidence=evidence,
        table_rows=table_rows,
    )
    if failure is not None:
        pytest.fail(f"the LULD stream failed (a unit mismatch fails its bound check): {failure}")
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"LULD row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    if not rows:
        pytest.skip(f"no LULD frame for {_SYMBOL} arrived (run at the 09:30 New York open)")
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("25")
@pytest.mark.requests(8)
@pytest.mark.timeout(100)
def test_filings_rows_without_a_ticker(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How many rows of the four filings datasets lack `ticker`.

    Reads March 2026 of each (up to 5,000 rows, flagged when cut short) and records how many
    rows have no `Symbol`. Reading it: any count is the answer; the library keeps such rows
    with a null `Symbol` either way.
    """
    filings = live_session.stocks.filings
    start, end = date(2026, 3, 1), date(2026, 3, 31)
    queries = {
        "filings.index": filings.index(start=start, end=end, limit=_FILINGS_LIMIT),
        "filings.sections_10k": filings.sections_10k(start=start, end=end, limit=_FILINGS_LIMIT),
        "filings.text_8k": filings.text_8k(start=start, end=end, limit=_FILINGS_LIMIT),
        "filings.risk_factors": filings.risk_factors(start=start, end=end, limit=_FILINGS_LIMIT),
    }
    counts = {}
    for name, query in queries.items():
        rows = table_rows(query.fetch())
        lacking = [row for row in rows if row["Symbol"] is None]
        counts[name] = len(rows)
        cut = " (cut short at the limit)" if len(rows) == _FILINGS_LIMIT else ""
        evidence(f"{name} {start}..{end}: {len(rows)} rows{cut}, {len(lacking)} without a ticker")
        for row in lacking[:3]:
            evidence(f"    e.g. {row}")
    assert any(counts.values())


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("41")
@pytest.mark.requests(2)
@pytest.mark.timeout(60)
def test_an_ipo_keeps_its_issuer_name(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether an IPO's issuer name changes while the offering moves through its statuses.

    Reads every IPO listed in 2025 and records each ticker whose rows carry more than one
    `IssuerName`, with their statuses. Reading it: none means the `poll()` key `Symbol` and
    `IssuerName` keeps one row per offering; a ticker whose names differ only in spelling
    (a suffix, punctuation) means a renamed offering would leave a stale row, while wholly
    different issuers are a recycled ticker, which the key keeps apart on purpose.
    """
    rows = table_rows(
        live_session.stocks.ipos(start=date(2025, 1, 1), end=date(2025, 12, 31)).fetch()
    )
    names: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        names.setdefault(str(row["Symbol"]), set()).add(
            (str(row["IssuerName"]), str(row["IpoStatus"]))
        )
    several = {symbol: seen for symbol, seen in names.items() if len({n for n, _ in seen}) > 1}
    evidence(f"2025 IPOs: {len(rows)} rows, {len(names)} tickers, {len(several)} with >1 name")
    for symbol, seen in sorted(several.items())[:10]:
        evidence(f"    {symbol}: {sorted(seen)}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("33")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_stock_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    week_bar_report: Callable[[str, str], list[str]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which days a stock week bar covers, and where it is stamped.

    For each raw week bar of `AAPL` around 2026-02-23 to 2026-03-22, records its stamp and the
    candidate windows whose hour-bar volume is closest to its own; `MATCH` marks the bucket.
    Reading it: a Monday 00:00 UTC match confirms Monday-to-Friday weeks, whether the bar is
    stamped on the Monday or on the Sunday before (the library keeps both for their week); a
    match elsewhere means the library's stock week windows need that start. The last line
    shows which bars the library's `history()` keeps.
    """
    lines = week_bar_report("stocksTicker", _SYMBOL)
    for line in lines:
        evidence(line)
    kept = live_session.stocks.bars(_SYMBOL, dm.Timespan.WEEK).history(
        date(2026, 3, 2), date(2026, 3, 15), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history 2026-03-02..2026-03-15 keeps the bars stamped {stamps}")
    assert len(lines) > 1


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("34")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_month_bars_from_mid_month_against_the_whole_month(
    *, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether the range returns a partial month bar for a `{from}` inside a month.

    Records `AAPL` month bars from 2026-03-01 and from 2026-03-15. Reading it: a first bar of
    April, or a March bar with the whole month's volume, means no partial bar; a March bar with
    less volume is a partial bar, which the library already avoids by starting at period
    boundaries.
    """
    whole = _bars(rest_rows, "month", multiplier=1, bounds=(date(2026, 3, 1), date(2026, 6, 30)))
    partial = _bars(rest_rows, "month", multiplier=1, bounds=(date(2026, 3, 15), date(2026, 6, 30)))
    for name, bars in (("from 2026-03-01", whole), ("from 2026-03-15", partial)):
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
    *, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether bars of several units align to clock and calendar boundaries or to `{from}`.

    Records `AAPL` 5-minute bars from 14:32 UTC and 2-week bars from Wednesday 2026-03-04.
    Reading it: stamps on :30 and :35 and on Mondays mean boundaries, as the library assumes;
    stamps at 14:32 and on Wednesdays mean `{from}`, and the library's bar windows must follow.
    """
    five = _bars(
        rest_rows,
        "minute",
        multiplier=5,
        bounds=(datetime(2026, 3, 3, 14, 32, tzinfo=UTC), datetime(2026, 3, 3, 15, 32, tzinfo=UTC)),
    )
    two_weeks = _bars(rest_rows, "week", multiplier=2, bounds=(date(2026, 3, 4), date(2026, 4, 30)))
    evidence(f"5-minute bars from 14:32 UTC: {[instant_text(b['t'] * _MS) for b in five]}")
    evidence(
        "2-week bars from Wednesday 2026-03-04: "
        f"{[(instant_text(b['t'] * _MS), b['v']) for b in two_weeks]}"
    )
    assert five
    assert two_weeks


def _new_york(nanos: int) -> str:
    """Write epoch nanoseconds as UTC and New York wall time.

    Returns:
        Such as `"2025-11-28T18:00:00Z = 13:00 New York"`.
    """
    utc = datetime.fromtimestamp(nanos / 1e9, UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%SZ} = {utc.astimezone(_NEW_YORK):%H:%M} New York"


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("half-day stamps")
@pytest.mark.requests(6)
@pytest.mark.timeout(100)
def test_half_day_stock_bars_and_their_stamps(
    rest_rows: RestRows, *, half_day_report: HalfDayReport, evidence: Evidence
) -> None:
    """How REST stamps the bars of a half trading day, which closes at 13:00 New York time.

    For 2025-11-28 and 2025-12-24, records `AAPL`'s range day bar, its first and last minute
    bars, and its grouped-daily row, each stamp in UTC and New York time. Reading it: day
    bars at midnight with minute bars ending at 12:59 confirm the library keeps a half day as
    a whole day; a day bar stamped at 13:00 or 16:00 shows where Massive cuts it, and the
    library's day labelling must follow.
    """
    lines = half_day_report("stocksTicker", _SYMBOL)
    for line in lines:
        evidence(line)
    for day in _HALF_DAYS:
        rows = [r for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _SYMBOL]
        for row in rows:
            evidence(f"{_SYMBOL} {day} grouped daily: t={_new_york(int(row['t']) * _MS)}")
    assert len(lines) > 1


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.settles("half-day stamps")
@pytest.mark.timeout(100)
def test_half_day_stock_flat_files_and_their_stamps(
    flat_file_tickers: FlatFileTickers, *, evidence: Evidence
) -> None:
    """How the stock flat files stamp the bars of a half trading day.

    For 2025-11-28 and 2025-12-24, records `AAPL`'s `day_aggs_v1` row and the first and last
    rows of `minute_aggs_v1`, each `window_start` in UTC and New York time. Reading it beside
    the REST test: the same stamps mean one day cut for both sources.
    """
    read = 0
    for day in _HALF_DAYS:
        for prefix in ("us_stocks_sip/day_aggs_v1", "us_stocks_sip/minute_aggs_v1"):
            result = flat_file_tickers(prefix, day, [_SYMBOL])
            if result is None:
                evidence(f"{prefix} {day}: no file")
                continue
            rows = [r for r in result[0] if r["window_start"] is not None]
            read += bool(rows)
            stamps = [_new_york(int(str(r["window_start"]))) for r in (rows[:1] + rows[-1:])]
            evidence(f"{prefix} {day}: {len(rows)} rows; first and last window_start {stamps}")
    assert read


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
@pytest.mark.settles("45")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_nvda_split_basis_before_and_on_the_split(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """How NVDA's 10-for-1 split of 2024-06-10 shows in adjusted and as-traded REST bars.

    Records NVDA's 2024-06-07 and 2024-06-10 day bars with `adjusted=true` and
    `adjusted=false`. Reading it: an as-traded 2024-06-07 close near 1208 and an adjusted one
    near 120.9, with the adjusted volume about ten times the other, confirm that `adjusted`
    restates prices before a split; 2024-06-10, the execution date, is the control on which
    both agree.
    """
    adjusted = _days(live_session, "NVDA", span=_NVDA, adjusted=True, table_rows=table_rows)
    traded = _days(live_session, "NVDA", span=_NVDA, adjusted=False, table_rows=table_rows)
    for day in ("2024-06-07", "2024-06-10"):
        a, t = adjusted.get(day, {}), traded.get(day, {})
        evidence(
            f"NVDA {day}: adjusted close={a.get('Close')} volume={a.get('Volume')}; "
            f"as traded close={t.get('Close')} volume={t.get('Volume')}"
        )
    assert adjusted
    assert traded


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.settles("45")
@pytest.mark.timeout(100)
def test_nvda_day_file_before_the_split(
    flat_file_tickers: FlatFileTickers, *, evidence: Evidence
) -> None:
    """Whether the stock day file holds NVDA's 2024-06-07 bar as traded.

    Records NVDA's `us_stocks_sip/day_aggs_v1` row of 2024-06-07. Reading it: a close near
    1208 confirms that the flat files are as traded, so the library rightly serves only
    `adjusted=False` from them.
    """
    result = flat_file_tickers("us_stocks_sip/day_aggs_v1", date(2024, 6, 7), ["NVDA"])
    rows = [] if result is None else result[0]
    evidence(f"NVDA day_aggs_v1 2024-06-07: {rows}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("46")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_every_page_echoes_its_adjustment(live_rest: RestClient, *, evidence: Evidence) -> None:
    """Whether every aggregates page echoes `adjusted`, `next_url` pages and grouped daily too.

    Pages through two weeks of AAPL minute bars with a small `limit`, so the range answers in
    several pages, and reads two grouped-daily days, recording each page's `adjusted` echo.
    Reading it: `True` on every page confirms the library's page-by-page check; a missing echo
    on a later page means the check can only warn there.
    """
    range_path = source(Transport.REST, _RANGE)
    params = {
        "stocksTicker": _SYMBOL,
        "multiplier": 1,
        "timespan": "minute",
        "from": date(2026, 3, 2),
        "to": date(2026, 3, 13),
    }
    echoes = [
        page.adjusted
        for page, _ in zip(
            live_rest.pages(range_path, params, {"adjusted": True, "limit": 5000}),
            range(3),
            strict=False,
        )
    ]
    evidence(f"range pages' adjusted echoes: {echoes}")
    grouped = source(Transport.REST, _GROUPED)
    for day in (date(2026, 3, 2), date(2026, 3, 3)):
        page = live_rest.get(grouped, {"date": day}, {"adjusted": False})
        evidence(f"grouped daily {day}: adjusted echo {page.adjusted}")
    assert echoes


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("47")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_cash_dividends_are_never_adjusted(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether Massive's adjustment changes prices around COST's special dividend.

    Records COST's day bars around its ex-dividend date 2023-12-27 (a $15 special dividend) with
    `adjusted=true` and `adjusted=false`. Reading it: equal prices confirm that `adjusted`
    concerns splits only, as the library documents; different prices before the ex-date mean
    Massive adjusts for dividends too.
    """
    adjusted = _days(live_session, "COST", span=_COST, adjusted=True, table_rows=table_rows)
    traded = _days(live_session, "COST", span=_COST, adjusted=False, table_rows=table_rows)
    differ = sorted(
        day for day in adjusted if day in traded and adjusted[day]["Close"] != traded[day]["Close"]
    )
    evidence(f"COST days whose adjusted close differs from the traded one: {differ}")
    assert adjusted


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("48")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_stock_dividend_events_and_the_adjustment(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether `adjusted=true` applies a `stock_dividend` event of the splits dataset.

    Finds one recent `stock_dividend` event and records its ticker's day bars on both bases
    across its execution date. Reading it: prices before the date that differ by the event's
    factor mean such events are adjusted like splits; equal prices mean they are not, and the
    README's recipe should skip them.
    """
    events = table_rows(
        live_session.stocks.splits(
            start="2025-01-01", filters={"adjustment_type": "stock_dividend"}, limit=1
        ).fetch()
    )
    evidence(f"a stock_dividend event: {events[:1]}")
    if not events:
        return
    event = events[0]
    day = date.fromisoformat(str(event["ExecutionDate"]))
    first, last = date.fromordinal(day.toordinal() - 7), date.fromordinal(day.toordinal() + 3)
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
@pytest.mark.settles("53")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_daily_indicator_stamps(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """At which time of day Massive stamps daily indicator values, for a stock and for crypto.

    Records a week of daily SMA values of AAPL and `X:BTCUSD`: each value's `TimestampStart`
    and `Date`, which the library takes from the stamp's day. Reading it: stamps at 00:00 New
    York time (AAPL) and 00:00 UTC (crypto) mean the day labels are right; stamps elsewhere in
    a day say which day each value belongs to, and the labels must follow.
    """
    found = 0
    for symbol in ("AAPL", "X:BTCUSD"):
        rows = table_rows(
            live_session.indicators.sma(
                symbol, 5, start="2026-03-02", end="2026-03-09", timespan=dm.Timespan.DAY
            ).fetch()
        )
        found += len(rows)
        stamps = [(instant_text(r["TimestampStart"]), r["Date"]) for r in rows]
        evidence(f"{symbol} daily SMA (TimestampStart, Date): {stamps}")
    assert found


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("54")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_unknown_ticker_statuses(live_rest: RestClient, *, evidence: Evidence) -> None:
    """What each endpoint answers for a ticker that does not exist.

    Requests `ZZZZZQ` from `/prev`, the range, `/v3/trades`, and the ticker details, and
    records each answer's status and row count. Reading it: an empty 200 or a 404 both return
    no rows in the library; a 404 is the only way a symbol is dropped with a WARNING.
    """
    ticker = "ZZZZZQ"
    requests = (
        ("/v2/aggs/ticker/{stocksTicker}/prev", {"stocksTicker": ticker}),
        (
            _RANGE,
            {"stocksTicker": ticker, "multiplier": 1, "timespan": "day"}
            | {"from": date(2026, 3, 2), "to": date(2026, 3, 6)},
        ),
        ("/v3/trades/{stockTicker}", {"stockTicker": ticker}),
        ("/v3/reference/tickers/{ticker}", {"ticker": ticker}),
    )
    answered = 0
    for template, params in requests:
        try:
            page = live_rest.get(source(Transport.REST, template), params, {})
        except WireError as error:
            evidence(f"{template}: HTTP {error.status} {error.message[:120]}")
        else:
            evidence(f"{template}: HTTP 200, status {page.status}, {len(page.rows)} rows")
        answered += 1
    assert answered == len(requests)
