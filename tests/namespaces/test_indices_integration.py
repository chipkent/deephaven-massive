"""Live checks of index data against Massive: `I:` encoding, ticker spellings, and bar stamps.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw responses come from the wire REST
client, because the library rewrites the very spellings in question; the library's own answer
is recorded beside them. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import partial
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
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
HalfDayReport: TypeAlias = "Callable[[str, str], list[str]]"
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
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
LargestError: TypeAlias = (
    "Callable[[tuple[float | None, ...], tuple[float | None, ...]], float | None]"
)
LibraryStamps: TypeAlias = "Callable[[Callable[[], Table]], str]"

_DAY = date(2026, 6, 1)
"""A past Monday, inside a Basic plan's two years of history, for the endpoints that take a date."""

_SNAPSHOTS_PLAN = "Indices Starter ($49/month)"
"""The plan of the indices snapshot and the unified snapshot of an index."""


def _raw_requests(index: str) -> tuple[tuple[str, dict[str, object], dict[str, object]], ...]:
    """Return one request of each REST endpoint that serves an index.

    Returns:
        Each request's path template, path values, and query.
    """
    return (
        ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": index}, {}),
        (
            "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}",
            {"indicesTicker": index, "multiplier": 1, "timespan": "day", "from": _DAY, "to": _DAY},
            {},
        ),
        ("/v1/open-close/{indicesTicker}/{date}", {"indicesTicker": index, "date": _DAY}, {}),
        ("/v3/snapshot/indices", {}, {"ticker.any_of": [index]}),
        ("/v3/snapshot", {}, {"ticker.any_of": [index]}),
        (
            "/v1/indicators/sma/{stockTicker}",
            {"stockTicker": index},
            {"timespan": "day", "window": 10, "limit": 2},
        ),
    )


_TICKER_KEYS = ("T", "ticker", "symbol", "underlying")


def _spellings(value: object) -> dict[str, object]:
    """Collect the values of the keys that may carry a ticker in one decoded JSON object.

    Returns:
        A mapping from each such key found to its value.
    """
    if not isinstance(value, dict):
        return {}
    return {key: value[key] for key in _TICKER_KEYS if key in value}


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("26")
@pytest.mark.requests(2)
def test_a_percent_encoded_index_ticker_in_a_path_reads_as_the_index(
    live_session: dm.MassiveSession,
    *,
    index_symbol: str,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive reads a percent-encoded colon in a path (`I%3ASPX`) as `I:SPX`.

    The library percent-encodes every path value whole, so `/prev` of an index is requested
    as `/v2/aggs/ticker/I%3A<index>/prev`. Uses `I:SPX`, or `I:NDX` when Indices is on Basic,
    which excludes SPX. Passes when the library's `fetch()` returns one row for the index. A
    failure (no row, or a 404) means a colon must be sent unencoded.
    """
    rows = table_rows(live_session.indices.bars(index_symbol, dm.Timespan.DAY).fetch())
    evidence(f"library fetch() of /prev for {index_symbol} (sent encoded): {rows}")
    assert [row["Symbol"] for row in rows] == [index_symbol]


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("index ticker spelling")
@pytest.mark.requests(14)
@pytest.mark.timeout(110)
def test_index_ticker_spelling_on_every_index_endpoint(
    live_session: dm.MassiveSession,
    *,
    index_symbol: str,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which index endpoints spell an index with its `I:` prefix and which without.

    Sends one raw request of each REST endpoint that serves the index (`I:SPX`, or `I:NDX` on
    Indices Basic): `/prev`, the range, open-close, the indices snapshot, the unified
    snapshot, and an indicator. Records the ticker keys of the body and of the first row,
    then the library's `Symbol` for the same datasets. The two snapshots need Indices
    Starter; on Basic their lines say "not entitled" and the rest still settle. Reading it: an
    endpoint whose rows drop `I:` is one the library must read back with the prefix; the
    library's `Symbol` column shows whether it already does.
    """
    answered = 0
    for path, path_params, query in _raw_requests(index_symbol):
        try:
            page = live_rest.get(source(Transport.REST, path), path_params, query)
        except WireEntitlementError as error:
            evidence(f"{path}: not entitled: needs {_SNAPSHOTS_PLAN} ({error})")
            continue
        except WireError as error:
            evidence(f"{path}: {type(error).__name__}: {error}")
            continue
        answered += 1
        body = page.body if isinstance(page.body, dict) else {}
        results = body.get("results")
        first = page.rows[0] if page.rows else None
        evidence(
            f"{path}: body {_spellings(body)}; results {_spellings(results)}; "
            f"first row {_spellings(first)}"
        )
        if page.rows:
            evidence(f"    first row keys and values: {page.rows[0]}")
    queries = {
        "indices.values fetch()": live_session.indices.values(index_symbol).fetch,
        "indices.bars(DAY) fetch()": live_session.indices.bars(index_symbol, dm.Timespan.DAY).fetch,
        "indices.snapshots fetch()": live_session.indices.snapshots(index_symbol).fetch,
        "indices.daily_open_close fetch()": live_session.indices.daily_open_close(
            index_symbol, _DAY
        ).fetch,
        "snapshots (unified) fetch()": live_session.snapshots(index_symbol).fetch,
        "indicators.sma fetch()": live_session.indicators.sma(index_symbol, 10, limit=2).fetch,
    }
    for name, call in queries.items():
        try:
            evidence(f"library {name}: Symbol {[r['Symbol'] for r in table_rows(call())]}")
        except dm.EntitlementError as error:
            evidence(f"library {name}: not entitled: needs {_SNAPSHOTS_PLAN} ({error})")
        except dm.MassiveError as error:
            evidence(f"library {name}: {type(error).__name__}: {error}")
    assert answered


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("half-day stamps")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_half_day_index_bars_and_their_stamps(
    index_symbol: str, *, half_day_report: HalfDayReport, evidence: Evidence
) -> None:
    """How REST stamps an index's bars on a half trading day (13:00 New York close).

    For 2025-11-28 and 2025-12-24, records the index's range day bar and its first and last
    minute bars (`I:SPX`, or `I:NDX` on Indices Basic), each in UTC and New York time. Reading
    it: an index computed until 13:00 or past it shows whether the index day ends with the
    stock half day; the day bar's stamp shows the library's day label holds.
    """
    lines = half_day_report("indicesTicker", index_symbol)
    for line in lines:
        evidence(line)
    assert lines


_STAMP_INDICES = ("I:SPX", "I:AAVE100")
"""A US index computed during the stock session and one that updates around the clock."""
_DAY_FILE = "us_indices/day_aggs_v1"
_NEW_YORK = ZoneInfo("America/New_York")
_CHICAGO = ZoneInfo("America/Chicago")
_ZONES = (("New York", _NEW_YORK), ("Chicago", _CHICAGO))
_MATCH = 1e-6
"""The largest relative error at which a rebuilt bar is the bar."""
_LONGER = {"month": (2, 2), "quarter": (2, 2), "year": (1, 2)}
"""The fewest and most bars expected of each longer unit: two whole months and quarters, and
last year's bar with this year's unfinished one, if Massive serves it."""


def _rebuilt(minutes: list[dict[str, Any]], start: datetime, end: datetime) -> StampedBar | None:
    """Rebuild one bar from the raw minute bars in `[start, end)`.

    Returns:
        The window's start (epoch ms) and the rebuilt open, high, low, and close (an index has
        no volume), or None when no minute bar falls inside.
    """
    first, last = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    inside = [m for m in minutes if first <= m["t"] < last]
    if not inside:
        return None
    values = (
        float(inside[0]["o"]),
        float(max(m["h"] for m in inside)),
        float(min(m["l"] for m in inside)),
        float(inside[-1]["c"]),
        None,
    )
    return first, values


def _day_window(day: date, zone: ZoneInfo, days: int = 1) -> tuple[datetime, datetime]:
    """Return midnight of `day` in a zone and the midnight `days` later.

    Midnight to midnight, not 24 hours, so a 23- or 25-hour day is right.

    Returns:
        The window's start and its exclusive end.
    """
    after = day + timedelta(days=days)
    return (
        datetime(day.year, day.month, day.day, tzinfo=zone),
        datetime(after.year, after.month, after.day, tzinfo=zone),
    )


def _day_rebuilds(
    range_bars: RangeBars, index: str, days: tuple[date, ...]
) -> dict[str, dict[date, list[StampedBar]]]:
    """Rebuild an index's stamp days from minute bars over the New York and the Chicago day.

    Returns:
        By source name (`"rebuilt over the New York day"`, `"... Chicago day"`) and day, the
        rebuilt bar, if any minute bar fell inside.
    """
    found: dict[str, dict[date, list[StampedBar]]] = {}
    for day in days:
        minutes = range_bars(index, "minute", (day - timedelta(days=1), day + timedelta(days=1)))
        for name, zone in _ZONES:
            bar = _rebuilt(minutes, *_day_window(day, zone))
            found.setdefault(f"rebuilt over the {name} day", {})[day] = [] if bar is None else [bar]
    return found


def _new_york_day(stamp_ms: int) -> date:
    """Return the New York calendar day of an epoch-millisecond stamp.

    Returns:
        The day.
    """
    return datetime.fromtimestamp(stamp_ms / 1000, UTC).astimezone(_NEW_YORK).date()


def _week_spans(stamp_ms: int) -> list[tuple[str, date, int]]:
    """Return the spans a week bar may cover, from its stamp.

    Returns:
        `(label, first_day, days)` for its Monday to Friday, its Monday to Sunday, and the
        seven days from the stamp's own New York day (a week that starts on the weekend
        before its Monday).
    """
    local = _new_york_day(stamp_ms)
    monday = local + timedelta(days=(7 - local.weekday()) % 7)
    return [
        ("Monday to Friday", monday, 5),
        ("Monday to Sunday", monday, 7),
        ("seven days from the stamp's day", local, 7),
    ]


def _week_minutes(
    range_bars: RangeBars, index: str, spans: Mapping[int, list[tuple[str, date, int]]]
) -> list[dict[str, Any]]:
    """Read the minute bars of every span the week bars may cover, in one range.

    Returns:
        The raw minute bars, from the day before the earliest span to the end of the latest;
        none when there are no spans.
    """
    windows = [
        (first, first + timedelta(days=n)) for found in spans.values() for _, first, n in found
    ]
    if not windows:
        return []
    bounds = (min(w[0] for w in windows) - timedelta(days=1), max(w[1] for w in windows))
    return range_bars(index, "minute", bounds)


def _week_lines(
    index: str,
    weeks: tuple[date, date],
    *,
    range_bars: RangeBars,
    largest_error: LargestError,
) -> tuple[list[str], int]:
    """Show which days an index's week bars cover, weekend included or not.

    Reads the week bars of two calendar weeks (stamped from the Saturday before the first
    Monday to the last Saturday, so a bar stamped on its Monday or the weekend before is
    counted once; Massive may widen the range to whole weeks, so bars stamped outside are
    left out and counted either way),
    then the minute bars of every span those bars may cover, and rebuilds each span (see
    `_week_spans`) in New York and in Chicago time, reporting each rebuild's largest relative
    error against the week bar.

    Returns:
        The evidence lines and how many week bars were read.
    """
    bounds = (weeks[0] - timedelta(days=2), weeks[1] - timedelta(days=1))
    raw = range_bars(index, "week", bounds)
    bars = [b for b in raw if bounds[0] <= _new_york_day(int(b["t"])) <= bounds[1]]
    spans = {int(bar["t"]): _week_spans(int(bar["t"])) for bar in bars}
    minutes = _week_minutes(range_bars, index, spans)
    header = (
        f"{index}: {len(bars)} week bars stamped {bounds[0]} to {bounds[1]} "
        f"({len(raw) - len(bars)} of {len(raw)} outside, left out; more than 0 means the range "
        "widened to whole weeks), "
        f"{len(minutes)} minute bars"
    )
    lines = [header]
    for bar in bars:
        stamp = datetime.fromtimestamp(int(bar["t"]) / 1000, UTC)
        values = tuple(float(bar[k]) for k in ("o", "h", "l", "c"))
        rebuilds = []
        for label, first, days in spans[int(bar["t"])]:
            for name, zone in _ZONES:
                rebuilt = _rebuilt(minutes, *_day_window(first, zone, days))
                error = None if rebuilt is None else largest_error(values, rebuilt[1][:4])
                verdict = "no minutes" if error is None else f"{error:.2e}"
                mark = " MATCH" if error is not None and error <= _MATCH else ""
                rebuilds.append(f"{label} in {name} time: {verdict}{mark}")
        lines.append(
            f"{index} week bar t={stamp:%Y-%m-%dT%H:%M:%SZ} = "
            f"{stamp.astimezone(_NEW_YORK):%a %H:%M} New York, ohlc={values}; "
            f"largest relative error of each rebuild: {'; '.join(rebuilds)}"
        )
    return lines, len(bars)


def _record_library_index_stamps(
    session: dm.MassiveSession,
    index: str,
    *,
    stamp_days: StampDays,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """Record the library's windows of an index's 1-day and longer bars, call by call.

    A longer unit's history that the plan refuses (a range beyond the plan's history) is
    recorded rather than raised, so the evidence of the other calls is kept.
    """
    rest = dm.HistorySource.REST
    daily = session.indices.bars(index, dm.Timespan.DAY)
    evidence(
        f"library {index} history(REST): "
        + library_stamps(partial(daily.history, *stamp_days["day"], source=rest))
    )
    evidence(f"library {index} fetch() (/prev): {library_stamps(daily.fetch)}")
    for span in (dm.Timespan.WEEK, dm.Timespan.MONTH, dm.Timespan.QUARTER, dm.Timespan.YEAR):
        query = session.indices.bars(index, span)
        try:
            outcome = library_stamps(partial(query.history, *stamp_days[span.value], source=rest))
        except dm.EntitlementError as error:
            outcome = f"refused, beyond the plan's history? ({error})"
        evidence(f"library {index} {span.value} history: {outcome}")


@pytest.mark.needs("indices:starter")
@pytest.mark.settles("52", "60")
@pytest.mark.timeout(100)
def test_index_day_and_longer_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    range_bars: RangeBars,
    largest_error: LargestError,
    day_bar_report: DayBarReport,
    prev_bar_report: PrevBarReport,
    period_bar_report: PeriodBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How REST stamps index 1-day and longer bars, and which day and week they cover.

    The days are those of the most recent finished Thanksgiving week: its Wednesday, a full trading
    day, and its Friday, a half day. For `I:SPX` and `I:AAVE100` (which updates around the clock) it
    records, each stamp in UTC and New York time: the range endpoint's 1-day bars around both days
    beside each day rebuilt from minute bars over the New York day and over the Chicago day (an hour
    later), with which day's bar each rebuild matches and its largest relative error; `/prev`, which
    serves only the most recent completed trading day (a short-day stamp shows only on a run the
    trading day after a half day), beside the range bars of that day; the week bars of Thanksgiving
    week and the week before, each against its weekdays, its whole calendar week, and the seven days
    from its stamp, rebuilt in both zones; the bars of the two whole months and quarters before the
    current ones and of the years from January 1 of last year, each read refused for the plan's
    history recorded as such; and the library's windows for the same requests. Reading it: a day bar
    matching the Chicago rebuild means index days are Chicago days, and the library's 01:00 New York
    `TimestampEnd` is right; matching the New York rebuild means the extra hour can go. A week bar
    matching the whole-week rebuild of `I:AAVE100` means weekend values count, as the library
    assumes; longer bars stamped 00:00 or 01:00 New York time on their period's first day are ones
    the library accepts. For `I:SPX` both day rebuilds can agree, since it is computed only during
    the stock session.
    """
    days = stamp_days["day"]
    complete = {}
    for index in _STAMP_INDICES:
        rebuilds = _day_rebuilds(range_bars, index, days)
        missing = day_bar_report(index, days, rebuilds)
        prevs = prev_bar_report(index)
        week_lines, weeks = _week_lines(
            index, stamp_days["week"], range_bars=range_bars, largest_error=largest_error
        )
        for line in week_lines:
            evidence(line)
        periods = period_bar_report(index, {s: stamp_days[s] for s in _LONGER}, _LONGER)
        _record_library_index_stamps(
            live_session,
            index,
            stamp_days=stamp_days,
            library_stamps=library_stamps,
            evidence=evidence,
        )
        complete[index] = {
            "a range bar on both days": not missing,
            "every minute-bar rebuild": all(all(d.values()) for d in rebuilds.values()),
            "a /prev bar": prevs > 0,
            "one bar for each of two weeks": weeks == 2,
            "the expected month, quarter, and year bars": periods,
        }
    gaps = {index: [k for k, ok in found.items() if not ok] for index, found in complete.items()}
    assert not any(gaps.values()), f"missing evidence, by index: {gaps}"


@pytest.mark.needs("indices:starter", "s3")
@pytest.mark.settles("52", "60")
@pytest.mark.timeout(100)
def test_index_day_bar_stamps_of_the_day_file(
    live_session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    flat_file_tickers: FlatFileTickers,
    stamped_bar: ToStampedBar,
    day_bar_report: DayBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How `us_indices/day_aggs_v1` stamps `I:SPX`'s and `I:AAVE100`'s 1-day bars.

    Records each index's file row of the most recent finished Thanksgiving week's Wednesday
    (a full day) and Friday (a half day), its `window_start` in UTC and New York time, beside
    the range endpoint's bars around those days and which day's bar it matches with the
    largest relative error, and the library's `history(source=FLAT_FILES)` windows. Reading
    it: a stamp at 01:00 New York time (midnight in Chicago) matching the range bar of the
    same day confirms one day label for the files and REST; which day that label covers is
    the REST test's minute rebuild.
    """
    days = stamp_days["day"]
    files: dict[str, dict[date, list[StampedBar]]] = {i: {} for i in _STAMP_INDICES}
    for day in days:
        result = flat_file_tickers(_DAY_FILE, day, _STAMP_INDICES)
        for row in [] if result is None else result[0]:
            files[str(row["ticker"])].setdefault(day, []).append(stamped_bar(row))
    for index, by_day in files.items():
        day_bar_report(index, days, {_DAY_FILE: by_day})
        daily = live_session.indices.bars(index, dm.Timespan.DAY)
        outcome = library_stamps(partial(daily.history, *days, source=dm.HistorySource.FLAT_FILES))
        evidence(f"library {index} history(FLAT_FILES): {outcome}")
    missing = [(i, d) for i in _STAMP_INDICES for d in days if not files[i].get(d)]
    assert not missing, f"the day files lack {missing}"
