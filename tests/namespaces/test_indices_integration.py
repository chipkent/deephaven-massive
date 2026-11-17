"""Live checks of index data against Massive: `I:` encoding, ticker spellings, and bar stamps.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw responses
come from the wire REST client, because the library rewrites the very spellings in question; the
library's own answer is recorded beside them. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.wire_symbols import wire_spellings
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)
from tests._live import index_day_sources, refusals

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
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
DayBars: TypeAlias = "dict[date, list[StampedBar]]"
DayBarReport: TypeAlias = (
    "Callable[[str, tuple[date, ...], Mapping[str, Mapping[date, list[StampedBar]]]], DayBars]"
)
PrevBarReport: TypeAlias = "Callable[[str], list[StampedBar]]"
PeriodBarReport: TypeAlias = (
    "Callable[[str, Mapping[str, tuple[date, date]], Mapping[str, tuple[int, int]]], bool]"
)
WeekBars: TypeAlias = "Callable[[str, tuple[date, date]], list[dict[str, Any]]]"
LargestError: TypeAlias = (
    "Callable[[tuple[float | None, ...], tuple[float | None, ...]], float | None]"
)
LibraryStamps: TypeAlias = "Callable[[Callable[[], Table]], str]"

_INDEX = "I:NDX"
"""The index the spelling checks read: any index serves, so the Nasdaq-100."""


def _raw_requests(
    index: str, day: date
) -> tuple[tuple[str, dict[str, object], dict[str, object]], ...]:
    """Return one request of each REST endpoint that serves an index.

    The endpoints that take a date read `day`.

    Returns:
        Each request's path template, path values, and query.
    """
    return (
        ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": index}, {}),
        (
            "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}",
            {"indicesTicker": index, "multiplier": 1, "timespan": "day", "from": day, "to": day},
            {},
        ),
        ("/v1/open-close/{indicesTicker}/{date}", {"indicesTicker": index, "date": day}, {}),
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


@pytest.mark.verifies("26")
@pytest.mark.requests(2)
def test_a_percent_encoded_index_ticker_in_a_path_reads_as_the_index(
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive reads a percent-encoded colon in a path (`I%3ANDX`) as `I:NDX`.

    The library percent-encodes every path value whole, so `/prev` of an index is requested
    as `/v2/aggs/ticker/I%3A<index>/prev`. Any index serves the question, so it reads the
    Nasdaq-100. Passes when the library's `fetch()` returns one row for the index. A failure
    (no row, or a 404) means a colon must be sent unencoded.
    """
    rows = table_rows(live_session.indices.previous_day_bar(_INDEX).fetch(wait=True))
    evidence(f"library fetch() of /prev for {_INDEX} (sent encoded): {rows}")
    assert [row["Symbol"] for row in rows] == [_INDEX]


@pytest.mark.verifies("44")
@pytest.mark.requests(11)
@pytest.mark.timeout(110)
def test_index_ticker_spelling_on_every_index_endpoint(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which index endpoints spell an index with its `I:` prefix and which without.

    Sends one raw request of each REST endpoint that serves the index (`I:NDX`, since any
    index serves), the dated ones for the most recent published Monday: `/prev`, the range,
    open-close, the indices snapshot, the unified snapshot, and an indicator, 6 requests.
    Records the ticker keys of the body and of the first row, then the library's `Symbol` for
    the same datasets, 5 more. Each request or call Massive refuses is recorded with Massive's
    refusal and the rest still run; the test skips, quoting the first refusal, when Massive
    answered no raw request and refused at least one, and no other raw request failed. Passes
    when every row the library returns carries the index with its `I:` prefix in `Symbol`,
    whichever spelling the endpoint sent; the raw lines show which endpoints drop the prefix.
    """
    answered = 0
    refused: list[str] = []
    failed: list[str] = []
    for path, path_params, query in _raw_requests(_INDEX, recent_monday):
        try:
            page = live_rest.get(source(Transport.REST, path), path_params, query, bucket="indices")
        except WireEntitlementError as error:
            refused.append(refusals.refusal_line(error))
            evidence(f"{path}: not entitled: {refused[-1]}")
            continue
        except WireError as error:
            failed.append(f"{path}: {type(error).__name__}: {error}")
            evidence(failed[-1])
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
        "indices.previous_day_bar fetch()": partial(
            live_session.indices.previous_day_bar(_INDEX).fetch, wait=True
        ),
        "indices.snapshots fetch()": partial(
            live_session.indices.snapshots(_INDEX).fetch, wait=True
        ),
        "indices.daily_open_close fetch()": partial(
            live_session.indices.daily_open_close(_INDEX, recent_monday).fetch, wait=True
        ),
        "snapshots (unified) fetch()": partial(live_session.snapshots(_INDEX).fetch, wait=True),
        "indicators.sma fetch()": partial(
            live_session.indicators.sma(_INDEX, 10, limit=2).fetch, wait=True
        ),
    }
    misspelled: dict[str, list[object]] = {}
    for name, call in queries.items():
        try:
            symbols = [r["Symbol"] for r in table_rows(call())]
            evidence(f"library {name}: Symbol {symbols}")
            if any(symbol != _INDEX for symbol in symbols):
                misspelled[name] = symbols
        except dm.EntitlementError as error:
            evidence(f"library {name}: not entitled: {refusals.refusal_line(error)}")
        except dm.MassiveError as error:
            evidence(f"library {name}: {type(error).__name__}: {error}")
    if not answered and refused and not failed:
        pytest.skip(f"refused: every raw request; the first: {refused[0]}")
    assert answered, f"no raw request was answered; failed: {failed}"
    assert not misspelled, f"library rows without {_INDEX} in Symbol: {misspelled}"


_STAMP_INDICES = ("I:SPX", "I:AAVE100")
"""A US index computed during the stock session and one that updates around the clock."""
_DAY_FILE = "us_indices/day_aggs_v1"
_NEW_YORK = ZoneInfo("America/New_York")
_CHICAGO = ZoneInfo("America/Chicago")
_ZONES = (("New York", _NEW_YORK), ("Chicago", _CHICAGO))
_MATCH = 1e-6
"""The largest relative error at which a rebuilt bar is the bar."""
_LONGER = {"month": (2, 2), "quarter": (2, 2), "year": (1, 1)}
"""The fewest and most bars expected of each longer unit: two whole months and quarters, and
this year's unfinished bar."""


def _rebuilt(minutes: list[dict[str, Any]], start: datetime, end: datetime) -> StampedBar | None:
    """Rebuild one bar from the raw minute bars in `[start, end)`.

    Returns:
        The window's start (epoch ms) and the rebuilt open, high, low, and close (an index has
        no volume), or None when no minute bar falls inside.
    """
    first, last = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    found = index_day_sources.rebuilt(minutes, first, last)
    return None if found is None else (first, (*found, None))


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
    range_bars: RangeBars, index: str, days: tuple[date, ...], *, evidence: Evidence
) -> dict[str, dict[date, list[StampedBar]]]:
    """Rebuild an index's stamp days from minute bars over the New York and the Chicago day.

    Also records the first and last minute bar of each New York day, in UTC and New York time,
    which show where a half day's values end.

    Returns:
        By source name (`"rebuilt over the New York day"`, `"... Chicago day"`) and day, the
        rebuilt bar, if any minute bar fell inside.
    """
    found: dict[str, dict[date, list[StampedBar]]] = {}
    for day in days:
        minutes = range_bars(index, "minute", (day - timedelta(days=1), day + timedelta(days=1)))
        start, end = (int(edge.timestamp() * 1000) for edge in _day_window(day, _NEW_YORK))
        own = [m for m in minutes if start <= m["t"] < end]
        ends = [_wall_clock(int(m["t"])) for m in own[:1] + own[-1:]]
        evidence(f"{index} {day}: {len(own)} minute bars; first and last {ends}")
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


def _record_weeks(
    index: str,
    weeks: tuple[date, date],
    *,
    week_bars: WeekBars,
    range_bars: RangeBars,
    largest_error: LargestError,
    evidence: Evidence,
) -> int:
    """Record which days an index's week bars cover, weekend included or not.

    Reads the week bars of two calendar weeks through `week_bars`, which records each bar
    outside the span and the count at once; then reads the minute bars of every span those
    bars may cover and rebuilds each span (see `_week_spans`) in New York and in Chicago time,
    recording each rebuild's largest relative error against the week bar.

    Returns:
        How many week bars were kept.
    """
    bars = week_bars(index, weeks)
    spans = {int(bar["t"]): _week_spans(int(bar["t"])) for bar in bars}
    minutes = _week_minutes(range_bars, index, spans)
    evidence(f"{index}: {len(minutes)} minute bars for the week rebuilds")
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
        evidence(
            f"{index} week bar t={stamp:%Y-%m-%dT%H:%M:%SZ} = "
            f"{stamp.astimezone(_NEW_YORK):%a %H:%M} New York, ohlc={values}; "
            f"largest relative error of each rebuild: {'; '.join(rebuilds)}"
        )
    return len(bars)


def _longer_units(stamp_days: StampDays, evidence: Evidence) -> dict[str, tuple[int, int]]:
    """Return the longer units to read and the bars expected of each, for this run's day.

    The year is read from January 1 to the run's day and expects this year's unfinished
    bar, which exists only from the year's first trading day (the first weekday after
    January 1); before it the year unit is recorded as skipped and left out.

    Returns:
        The fewest and most bars expected, by unit.
    """
    units = dict(_LONGER)
    today = stamp_days["year"][1]
    first = date(today.year, 1, 2)
    while first.weekday() >= 5:
        first += timedelta(days=1)
    if today < first:
        evidence(f"year bars skipped: {today} is before the year's first trading day, {first}")
        del units["year"]
    return units


def _record_library_index_stamps(
    session: dm.MassiveSession,
    index: str,
    *,
    units: Collection[str],
    stamp_days: StampDays,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """Record the library's windows of an index's 1-day, week, and longer bars, call by call.

    A longer unit's history that the plan refuses is recorded and the other calls still run;
    the first refusal is then raised, for the run to judge (a day outside the plan's history
    skips the test).
    """
    rest = dm.HistorySource.REST
    daily = session.indices.bars(index, dm.Timespan.DAY)
    evidence(
        f"library {index} history(REST): "
        + library_stamps(partial(daily.history, *stamp_days["day"], source=rest, wait=True))
    )
    prev_bar = session.indices.previous_day_bar(index)
    prev_stamps = library_stamps(partial(prev_bar.fetch, wait=True))
    evidence(f"library {index} previous_day_bar (/prev): {prev_stamps}")
    refused: list[dm.EntitlementError] = []
    for span in map(dm.Timespan, ("week", *units)):
        query = session.indices.bars(index, span)
        try:
            outcome = library_stamps(
                partial(query.history, *stamp_days[span.value], source=rest, wait=True)
            )
        except dm.EntitlementError as error:
            outcome = f"refused ({error})"
            refused.append(error)
        evidence(f"library {index} {span.value} history: {outcome}")
    if refused:
        raise refused[0]


@pytest.mark.settles("52", "60")
@pytest.mark.timeout(100)
def test_index_day_and_longer_bar_stamps_from_rest(
    live_session: dm.MassiveSession,
    *,
    stamp_days: StampDays,
    range_bars: RangeBars,
    week_bars: WeekBars,
    largest_error: LargestError,
    day_bar_report: DayBarReport,
    prev_bar_report: PrevBarReport,
    period_bar_report: PeriodBarReport,
    library_stamps: LibraryStamps,
    evidence: Evidence,
) -> None:
    """How REST stamps index 1-day and longer bars, and which day and week they cover.

    The days are the most recent half day whose week has ended (the Friday after Thanksgiving, July
    3, or December 24) and the full trading day before it. For `I:SPX` and `I:AAVE100` (which
    updates around the clock) it records, each stamp in UTC and New York time: the range endpoint's
    1-day bars around both days beside each day rebuilt from minute bars over the New York day and
    over the Chicago day (an hour later), with which day's bar each rebuild matches and its largest
    relative error, and each day's first and last minute bar; `/prev`, which serves only the most
    recent completed trading day (a short-day stamp shows only on a run the trading day after a half
    day), beside the range bars of that day; the week bars of the half day's week and the week
    before, each against its weekdays, its whole calendar week, and the seven days from its stamp,
    rebuilt in both zones; the bars of the two whole months and quarters before the current ones and
    of the years from January 1 of last year, each read refused for the plan's history recorded as
    such; and the library's windows for the same requests. Reading it: a day bar matching the
    Chicago rebuild means index days are Chicago days, and the library's 01:00 New York
    `TimestampEnd` is right; matching the New York rebuild means the extra hour can go. A week bar
    matching the whole-week rebuild of `I:AAVE100` means weekend values count, as the library
    assumes; longer bars stamped 00:00 or 01:00 New York time on their period's first day are ones
    the library accepts. For `I:SPX` both day rebuilds can agree, since it is computed only during
    the stock session.
    """
    days = stamp_days["day"]
    complete = {}
    for index in _STAMP_INDICES:
        rebuilds = _day_rebuilds(range_bars, index, days, evidence=evidence)
        ranged = day_bar_report(index, days, rebuilds)
        prevs = prev_bar_report(index)
        weeks = _record_weeks(
            index,
            stamp_days["week"],
            week_bars=week_bars,
            range_bars=range_bars,
            largest_error=largest_error,
            evidence=evidence,
        )
        units = _longer_units(stamp_days, evidence)
        periods = period_bar_report(index, {u: stamp_days[u] for u in units}, units)
        _record_library_index_stamps(
            live_session,
            index,
            units=units,
            stamp_days=stamp_days,
            library_stamps=library_stamps,
            evidence=evidence,
        )
        complete[index] = {
            "a range bar on both days": all(ranged.values()),
            "every minute-bar rebuild": all(all(d.values()) for d in rebuilds.values()),
            "a /prev bar": bool(prevs),
            "one bar for each of two weeks": weeks == 2,
            "the expected month, quarter, and year bars": periods,
        }
    gaps = {index: [k for k, ok in found.items() if not ok] for index, found in complete.items()}
    assert not any(gaps.values()), f"missing evidence, by index: {gaps}"


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

    Records each index's file row of the most recent half day whose week has ended and of
    the full trading day before it, its `window_start` in UTC and New York time, beside
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
        outcome = library_stamps(
            partial(daily.history, *days, source=dm.HistorySource.FLAT_FILES, wait=True)
        )
        evidence(f"library {index} history(FLAT_FILES): {outcome}")
    missing = [(i, d) for i in _STAMP_INDICES for d in days if not files[i].get(d)]
    assert not missing, f"the day files lack {missing}"


_MINUTE_FILE = "us_indices/minute_aggs_v1"
_ROUND_THE_CLOCK = "I:AAVE100"
"""An index that updates around the clock, so its minute file has rows near every midnight."""


@pytest.mark.timeout(160)
@pytest.mark.verifies("145")
def test_an_index_minute_file_holds_its_chicago_day(
    *,
    recent_monday: date,
    flat_file_tickers: FlatFileTickers,
    rest_rows: RestRows,
    evidence: Evidence,
) -> None:
    """Whether an index minute file holds its date's Chicago day, as `history()` reads it.

    `history()` reads an index minute file of a date as the minutes from midnight to midnight
    in Chicago, 01:00 to 01:00 New York time, so a window of New York days also reads the file
    of the day before its first, which holds that day's hour from 00:00 to 01:00. A file cut at
    another midnight would lose or double an hour at each seam. Reads the `I:AAVE100` rows of
    the most recent published Monday's minute file (Monday follows a weekend, when only such an
    index updates) and the REST minute bars from the day before to the day after, and records
    the file's first and last stamps in UTC and New York time, its rows outside the Chicago
    day, and the REST minutes of that day the file lacks or adds. Passes when every file row
    falls inside the Chicago day and the file adds no minute REST lacks. Minutes REST has and
    the file lacks do not fail it: they are recorded as a gap in Massive's file, which the
    library cannot fill, since the file's rows still fall on the right day.
    """
    day = recent_monday
    read = flat_file_tickers(
        _MINUTE_FILE, day, wire_spellings(dm.AssetClass.INDICES, _ROUND_THE_CLOCK)
    )
    assert read is not None, f"{_MINUTE_FILE} has no file for {day}"
    stamps = sorted(int(str(row["window_start"])) // 1_000_000 for row in read[0])
    assert stamps, f"{_MINUTE_FILE} {day} holds no {_ROUND_THE_CLOCK} row"
    start = datetime(day.year, day.month, day.day, tzinfo=_CHICAGO)
    end = start + timedelta(days=1)
    first, last = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    evidence(f"{_MINUTE_FILE} {day} {_ROUND_THE_CLOCK}: {len(stamps)} rows")
    evidence(f"first {_wall_clock(stamps[0])}; last {_wall_clock(stamps[-1])}")
    outside = [s for s in stamps if not first <= s < last]
    evidence(f"rows outside the Chicago day: {[_wall_clock(s) for s in outside[:10]]}")
    minutes = rest_rows(
        "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}",
        {
            "indicesTicker": _ROUND_THE_CLOCK,
            "multiplier": 1,
            "timespan": "minute",
            "from": day - timedelta(days=1),
            "to": day + timedelta(days=1),
        },
        {"sort": "asc", "limit": 50000},
    )
    rest_day = {int(m["t"]) for m in minutes if first <= int(m["t"]) < last}
    lacks, adds = sorted(rest_day - set(stamps)), sorted(set(stamps) - rest_day)
    evidence(f"REST minutes of the Chicago day: {len(rest_day)}")
    evidence(
        f"the file lacks {len(lacks)} (a gap in Massive's file, not asserted): "
        f"{[_wall_clock(s) for s in lacks[:10]]}"
    )
    evidence(f"the file adds {len(adds)}: {[_wall_clock(s) for s in adds[:10]]}")
    assert not outside, f"the file holds {len(outside)} rows outside its Chicago day"
    assert not adds, f"the file holds {len(adds)} minutes REST does not"


def _wall_clock(stamp_ms: int) -> str:
    """Write an epoch-millisecond stamp as UTC and New York wall time.

    Returns:
        Such as `"2025-11-28T18:00:00Z = 13:00 New York"`.
    """
    utc = datetime.fromtimestamp(stamp_ms / 1000, UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%SZ} = {utc.astimezone(_NEW_YORK):%H:%M} New York"


_WELL_KNOWN = ("I:SPX", "I:NDX", "I:DJI", "I:RUT", "I:VIX", index_day_sources.ROUND_THE_CLOCK)
"""The indices the many-index check always reads: four computed during the stock session,
the volatility index, which is computed in extended hours too, and the crypto index whose
files disagreed with REST."""

_CRYPTO_FAMILIES = ("AAVE", "BTC", "ETH")
"""The coins whose indices the many-index check finds in Massive's index ticker list."""

_PER_FAMILY = 2
"""The most indices read per coin."""

_TICKERS = "/v3/reference/tickers"
_PREV = "/v2/aggs/ticker/{indicesTicker}/prev"
_OPEN_CLOSE = "/v1/open-close/{indicesTicker}/{date}"
_NS_PER_MS = 1_000_000

FileRead: TypeAlias = "tuple[list[dict[str, str | None]], frozenset[str]] | None"
FileRows: TypeAlias = "list[dict[str, str | None]]"


def _file_rows(read: FileRead, index: str) -> FileRows | None:
    """Return one index's rows of a flat file read for many, whatever spelling the file uses.

    Returns:
        The rows, oldest first; None when the file was not published.
    """
    if read is None:
        return None
    spellings = set(wire_spellings(dm.AssetClass.INDICES, index))
    rows = [row for row in read[0] if row.get("ticker") in spellings]
    return sorted(rows, key=lambda row: int(str(row["window_start"])))


def _optional_row(
    rest_rows: RestRows, path: str, path_params: Mapping[str, object], *, evidence: Evidence
) -> dict[str, Any] | None:
    """Read a one-row REST answer, recording an error answer instead of failing on it.

    Returns:
        The first row, or None when Massive answered with an error or no row.
    """
    try:
        rows = rest_rows(path, path_params, {})
    except WireError as error:
        evidence(f"{path} {dict(path_params)}: {type(error).__name__}: {error}")
        return None
    return rows[0] if rows else None


def _record_minute_file(
    index: str,
    day: date,
    rows: FileRows | None,
    *,
    minutes: list[dict[str, Any]],
    evidence: Evidence,
) -> index_day_sources.MinuteFileComparison | None:
    """Record how one day's minute-file rows of an index compare with its REST minutes.

    Returns:
        The comparison; None when the file was not published.
    """
    if rows is None:
        evidence(f"{index} {day}: no minute file published")
        return None
    found = index_day_sources.compare_minute_file(rows, minutes, day)
    ends = [_wall_clock(s) for s in (found.first_ms, found.last_ms) if s is not None]
    evidence(
        f"{index} {day} minute file: {found.rows} rows, first and last {ends}; lacks "
        f"{len(found.lacks)} REST minutes {[_wall_clock(s) for s in found.lacks[:5]]}; adds "
        f"{len(found.adds)} {[_wall_clock(s) for s in found.adds[:5]]}; "
        f"{len(found.outside)} outside the New York day; prices differ at "
        f"{len(found.differing)} minutes {list(found.differing[:3])}"
    )
    return found


def _record_day(
    tally: index_day_sources.IndexTally,
    day: date,
    *,
    minutes: list[dict[str, Any]],
    sources: Mapping[str, dict[str, Any] | None],
    files: tuple[FileRows | None, FileRows | None],
    evidence: Evidence,
) -> None:
    """Record one index-day: the window each source covers, and the files against REST.

    Args:
        tally: The index's tally, which it adds to.
        day: The day.
        minutes: The index's REST minute bars around the day.
        sources: Each REST source's row of the day (range, `/prev`, open-close), None for none.
        files: The day file's and the minute file's rows of the index, each None when that
            file was not published.
        evidence: Records one evidence line.
    """
    index, (day_file, minute_file) = tally.index, files
    rebuilds = _rebuilds(tally, day, minutes, evidence=evidence)
    bars = {
        name: None if row is None else index_day_sources.prices(row)
        for name, row in sources.items()
    }
    if day_file is not None:
        stamps = [_wall_clock(int(str(r["window_start"])) // _NS_PER_MS) for r in day_file]
        evidence(f"{index} {day} day file rows stamped {stamps}")
        bars[index_day_sources.FILE_SOURCE] = (
            index_day_sources.prices(day_file[0]) if day_file else None
        )
    verdicts = {
        name: _judge_source(tally, day, name, values=values, rebuilds=rebuilds, evidence=evidence)
        for name, values in bars.items()
    }
    ranged = bars.get(index_day_sources.RANGE_SOURCE)
    filed = bars.get(index_day_sources.FILE_SOURCE)
    if ranged is not None and filed is not None:
        error = index_day_sources.relative_error(ranged, filed)
        evidence(f"{index} {day} day file against the range bar: {error:.2e}")
    compared = _record_minute_file(index, day, minute_file, minutes=minutes, evidence=evidence)
    tally.add_files(
        day,
        range_verdict=verdicts[index_day_sources.RANGE_SOURCE],
        day_file=verdicts.get(index_day_sources.FILE_SOURCE),
        minute_file=compared,
    )


def _rebuilds(
    tally: index_day_sources.IndexTally,
    day: date,
    minutes: list[dict[str, Any]],
    *,
    evidence: Evidence,
) -> dict[str, index_day_sources.Prices | None]:
    """Rebuild one index day over each window, and record whether the two windows differ.

    Returns:
        Each window's rebuilt bar, by window name; None where no minute bar fell inside.
    """
    rebuilds = {
        name: index_day_sources.rebuilt(minutes, *index_day_sources.day_window_ms(day, zone))
        for name, zone in index_day_sources.WINDOWS
    }
    if index_day_sources.windows_differ(minutes, day):
        tally.windows_differ_days.add(day)
    differ = "differ" if day in tally.windows_differ_days else "hold the same minutes"
    evidence(f"{tally.index} {day}: the New York and Chicago days {differ}; rebuilds {rebuilds}")
    return rebuilds


def _judge_source(
    tally: index_day_sources.IndexTally,
    day: date,
    name: str,
    *,
    values: index_day_sources.Prices | None,
    rebuilds: Mapping[str, index_day_sources.Prices | None],
    evidence: Evidence,
) -> str:
    """Judge which window one source's bar of a day covers; count and record the verdict.

    Returns:
        The verdict (`window_verdict`).
    """
    verdict, error = index_day_sources.window_verdict(values, rebuilds)
    tally.add_verdict(name, verdict, error)
    shown = "" if error is None else f" (closest {error:.2e})"
    evidence(f"{tally.index} {day} {name}: {values}: {verdict}{shown}")
    return verdict


def _read_index(
    index: str,
    days: tuple[date, ...],
    files: Mapping[date, tuple[FileRead, FileRead]],
    *,
    rest_rows: RestRows,
    range_bars: RangeBars,
    evidence: Evidence,
) -> tuple[index_day_sources.IndexTally, bool]:
    """Read and record one index over every day.

    Args:
        index: The index.
        days: The days, oldest first.
        files: The day file and minute file read of each day, for every index at once.
        rest_rows: Reads raw REST rows.
        range_bars: Reads raw range bars.
        evidence: Records one evidence line.

    Returns:
        The index's tally, and whether REST sent it both day and minute bars.
    """
    tally = index_day_sources.IndexTally(index)
    ranged = range_bars(index, "day", (days[0] - timedelta(days=3), days[-1] + timedelta(days=3)))
    by_day = {_new_york_day(int(bar["t"])): bar for bar in ranged}
    prev = _optional_row(rest_rows, _PREV, {"indicesTicker": index}, evidence=evidence)
    prev_day = None if prev is None else _new_york_day(int(prev["t"]))
    evidence(f"{index} /prev: {prev}; its New York day {prev_day}")
    # A crypto index's /prev serves the calendar day before the run (a Sunday on a Monday run),
    # which is not among the trading days: its minutes are read too, so /prev is still judged.
    extra = () if prev_day is None or prev_day in days else (prev_day,)
    spans = index_day_sources.minute_read_spans((*days, *extra))
    minutes = [m for span in spans for m in range_bars(index, "minute", span)]
    minutes.sort(key=lambda m: int(m["t"]))
    evidence(f"{index}: {len(ranged)} range day bars, {len(minutes)} REST minute bars")
    for day in days:
        open_close = _optional_row(
            rest_rows, _OPEN_CLOSE, {"indicesTicker": index, "date": day}, evidence=evidence
        )
        sources = {
            index_day_sources.RANGE_SOURCE: by_day.get(day),
            "open-close": open_close,
            **({"/prev": prev} if prev_day == day else {}),
        }
        day_file, minute_file = files[day]
        _record_day(
            tally,
            day,
            minutes=minutes,
            sources=sources,
            files=(_file_rows(day_file, index), _file_rows(minute_file, index)),
            evidence=evidence,
        )
    if extra and prev is not None:
        rebuilds = _rebuilds(tally, extra[0], minutes, evidence=evidence)
        values = index_day_sources.prices(prev)
        _judge_source(tally, extra[0], "/prev", values=values, rebuilds=rebuilds, evidence=evidence)
    return tally, bool(ranged and minutes)


@pytest.mark.settles("60", "80")
@pytest.mark.requests(110)
@pytest.mark.timeout(900)
def test_index_day_windows_and_files_across_many_indices(
    *,
    index_check_days: tuple[date, ...],
    rest_rows: RestRows,
    range_bars: RangeBars,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Which day each index day source covers, and whether the index files match REST, broadly.

    One odd index on two days must not drive a library change, so this measures about ten indices
    over four or five recent trading days (the three before the run, the most recent Monday, and the
    day after the most recent holiday): `I:SPX`, `I:NDX`, `I:DJI`, and `I:RUT`, computed during the
    stock session; `I:VIX`, computed in extended hours too; `I:AAVE100`; and up to two indices each
    of AAVE, BTC, and ETH found in Massive's index ticker list (the `100` index first). For each
    index and day it records the range's 1-day bar, the daily open and close, and the
    `us_indices/day_aggs_v1` row, each with the window it matches: the day rebuilt from REST minute
    bars over the New York day or over the Chicago day (an hour later), both when they hold the same
    minutes, or neither. `/prev` is judged the same way on its own day, which for a crypto index is
    the calendar day before the run, read for it alone. Then it records the
    `us_indices/minute_aggs_v1` rows against REST's minutes: count, first and last stamp, the REST
    minutes it lacks, the minutes it adds or holds outside its New York day, and the minutes whose
    prices differ. It ends with a line per index (each source's windows, its largest relative error
    to the closest window, and its file findings by kind) and a verdict line per question: (a) do
    REST day bars of indices cover the Chicago day, generally; (b) for regular-hours and
    around-the-clock indices apart, which day files cover another window than the range bar, which
    match no window, and which minute files have gaps or other prices; (c) are `I:AAVE100`'s wrong
    values (a day file in no window, or other minute prices) isolated.

    It asserts only that each index has REST day and minute bars and that at least one day file and
    one minute file were read; everything else is evidence. Cost: 3 ticker-list searches, then per
    index one 1-day range, one `/prev`, one open-close per day, and one minute range per run of
    adjacent days (two or three for the usual days): at most about 110 REST requests, and 2
    flat files per day.
    """
    days = index_check_days
    found = {
        family: rest_rows(
            _TICKERS, {}, {"market": "indices", "search": family, "active": True, "limit": 1000}
        )
        for family in _CRYPTO_FAMILIES
    }
    for family, rows in found.items():
        shown = [(row.get("ticker"), row.get("name")) for row in rows[:10]]
        evidence(f"index tickers found for {family!r}: {len(rows)}, such as {shown}")
    chosen = index_day_sources.crypto_choices(found, each=_PER_FAMILY)
    indices = list(dict.fromkeys((*_WELL_KNOWN, *chosen)))
    evidence(f"indices: {indices}; days: {[str(day) for day in days]}")
    spellings = sorted({s for i in indices for s in wire_spellings(dm.AssetClass.INDICES, i)})
    files = {
        day: (
            flat_file_tickers(_DAY_FILE, day, spellings),
            flat_file_tickers(_MINUTE_FILE, day, spellings),
        )
        for day in days
    }
    for day, (day_file, minute_file) in files.items():
        evidence(
            f"{day}: day file published: {day_file is not None}; minute file: "
            f"{minute_file is not None}"
        )
    reads = [
        _read_index(
            index, days, files, rest_rows=rest_rows, range_bars=range_bars, evidence=evidence
        )
        for index in indices
    ]
    for line in index_day_sources.summary_lines([tally for tally, _ in reads]):
        evidence(line)
    empty = [tally.index for tally, complete in reads if not complete]
    assert not empty, f"no REST day or minute bars for {empty}"
    assert any(day_file for day_file, _ in files.values()), f"no {_DAY_FILE} file of {days}"
    assert any(minute for _, minute in files.values()), f"no {_MINUTE_FILE} file of {days}"
