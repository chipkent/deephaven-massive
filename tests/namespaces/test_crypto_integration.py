"""Live checks of crypto bars against Massive: day cuts, minute rebuilds, weeks, and pair sets.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST and flat-file clients, because the library moves the very stamps in question;
the library's own answer to the same request is recorded beside them. How to run these
tests is in `docs/live-tests.md`.

A test marked `massive_issue` checks an open Massive problem: it asserts what Massive's fix
brings, so it fails as expected while the problem lasts, and a pass means Massive fixed it
(`tests/_live/massive_problems.py`).

The library's rule under test: a crypto bar of one day covers one UTC day and is stamped at
00:00 UTC of it, from every source; a 1-day bar stamped anywhere else in its day but its first
or last millisecond is still served, labelled by the UTC day its stamp falls on, and logs a
WARNING that it may cover other days.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.wire_symbols import canonical_from_wire, wire_spellings
from deephaven_massive._massive import RestClient, Transport, source
from tests._live import ticker_lists
from tests._live.massive_problems import expect_fixed

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
FlatFileTickers: TypeAlias = (
    "Callable[[str, date, Collection[str] | None], "
    "tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession], None]"
CryptoReport: TypeAlias = "Callable[..., list[str]]"

_PAIR = "X:BTCUSD"
_RANGE = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{cryptoTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/crypto/{date}"
_DAY_FILE = "global_crypto/day_aggs_v1"
_TICKERS = "/v3/reference/tickers"
_TICKERS_PAGE = 1000
"""Rows per page of the ticker list: its largest page, the one the library reads."""
_MS = 1_000_000
_DAY_NS = 86_400 * 1_000_000_000
_STREAM_WAIT = 60.0
"""Seconds the FMV test waits for its first frame."""
_NEW_YORK = ZoneInfo("America/New_York")
_CLOSER = 10.0
"""How many times closer to `/prev` the UTC rebuild must be than the New York rebuild."""

_TOLERANCE = 1e-6
"""The relative error within which a high, low, or volume read from text equals a JSON number."""


def _ohlcv(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return a raw REST bar's open, high, low, close, and volume.

    Returns:
        The five values, None where the bar lacks one.
    """
    return row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v")


def _prices(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return a raw REST bar's open, high, low, and close, without its volume.

    The day's prices are what tell which window a bar covers. Its volume is not: on 2026-11-03
    the range's `X:BTCUSD` bar of the day before carried a volume 0.15% above that of `/prev`,
    grouped daily, and the day's minute bars, which all agreed, with the same prices.

    Returns:
        The four prices, None where the bar lacks one.
    """
    return _ohlcv(row)[:4]


def _utc_day(stamp_ms: int) -> date:
    """Return the UTC calendar day an epoch-millisecond stamp falls on.

    Returns:
        The day.
    """
    return datetime.fromtimestamp(stamp_ms / 1000, UTC).date()


def _errors(left: Sequence[Any], right: Sequence[Any]) -> list[float | None]:
    """Return the relative error of each field of two OHLCV tuples.

    Returns:
        One error per field; None where either side lacks the field.
    """
    return [
        None if a is None or b is None else abs(a - b) / max(abs(a), abs(b), 1e-12)
        for a, b in zip(left, right, strict=True)
    ]


def _largest(left: Sequence[Any], right: Sequence[Any]) -> float:
    """Return the largest relative error of two OHLCV tuples' fields.

    Returns:
        The error; infinity when either lacks a field, as an empty rebuild does.
    """
    if len(left) != len(right):
        return float("inf")
    errors = _errors(left, right)
    return float("inf") if None in errors else max(e for e in errors if e is not None)


def _same(left: Sequence[Any], right: Sequence[Any]) -> bool:
    """Compare two OHLCV or OHLC tuples: open and close exactly, the rest within the tolerance.

    Returns:
        Whether they describe the same bar.
    """
    if len(left) != len(right) or len(left) not in {4, 5}:
        return False
    errors = [error for error in _errors(left, right) if error is not None]
    if len(errors) != len(left):
        return False
    return left[0] == right[0] and left[3] == right[3] and max(errors) <= _TOLERANCE


def _library_rows(call: Callable[[], Table], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Run one library call and describe its outcome.

    A refusal is not an outcome to record: it propagates, and the test skips, quoting Massive,
    rather than reporting it as an answer.

    Returns:
        The result's rows, or the text of the `MassiveError` (other than a refusal) it raised.

    Raises:
        EntitlementError: When the call is refused; the live tests' hook skips the test.
    """
    try:
        return table_rows(call())
    except dm.EntitlementError:
        raise
    except dm.MassiveError as error:
        return f"MassiveError: {error}"


def _rebuilt(minutes: list[dict[str, Any]], start: datetime, end: datetime) -> tuple[Any, ...]:
    """Rebuild one bar from the minute bars in `[start, end)`.

    Returns:
        The rebuilt open, high, low, close, and volume; empty when no minute bar falls inside.
    """
    first, last = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    inside = [m for m in minutes if first <= m["t"] < last]
    if not inside:
        return ()
    return (
        inside[0]["o"],
        max(m["h"] for m in inside),
        min(m["l"] for m in inside),
        inside[-1]["c"],
        sum(m["v"] for m in inside),
    )


def _at_day_edge(stamp_ms: int, day: date) -> bool:
    """Tell whether a stamp is the first or the last millisecond of a UTC day.

    Args:
        stamp_ms: The stamp, in milliseconds since the epoch.
        day: The UTC day.

    Returns:
        True for 00:00:00.000 or 23:59:59.999 UTC of `day`.
    """
    first = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)
    return stamp_ms in {first, first + 86_400_000 - 1}


def _assert_one_utc_day(
    day: date,
    prev: Mapping[str, Any],
    *,
    ranged: Sequence[Mapping[str, Any]],
    grouped: Sequence[Mapping[str, Any]],
) -> None:
    """Assert that `/prev`, the range, and grouped daily send one bar for one UTC day.

    The bars must share their prices; their volumes may differ, as Massive's sources sometimes
    do, and the test records them as evidence instead.

    Args:
        day: The UTC day of `/prev`'s stamp.
        prev: The `/prev` bar.
        ranged: The range day bars around `day`.
        grouped: The grouped-daily rows of the pair for `day`.

    Raises:
        AssertionError: If a source lacks the bar, stamps it off the day's first or last
            millisecond, or sends other prices.
    """
    assert _at_day_edge(int(prev["t"]), day), f"/prev stamped {prev['t']}"
    same_day = [b for b in ranged if _utc_day(int(b["t"])) == day]
    assert len(same_day) == 1, f"range bars of {day}: {same_day}"
    assert _at_day_edge(int(same_day[0]["t"]), day), f"range bar stamped {same_day[0]['t']}"
    assert _same(_prices(same_day[0]), _prices(prev)), "the range bar is not /prev's day"
    assert len(grouped) == 1, f"grouped rows of {_PAIR} on {day}: {grouped}"
    assert _at_day_edge(int(grouped[0]["t"]), day), f"grouped bar stamped {grouped[0]['t']}"
    assert _same(_prices(grouped[0]), _prices(prev)), "the grouped bar is not /prev's day"


@pytest.mark.verifies("1")
@pytest.mark.requests(6)
@pytest.mark.timeout(100)
def test_crypto_day_bars_of_every_rest_source_share_one_day_cut(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Where `/prev`, the aggregates range, and grouped daily stamp a crypto day bar.

    Records the raw `X:BTCUSD` day bar of `/prev`, the range bars around that day, and the
    grouped-daily row, each `t` as a UTC instant, whether they agree, and what the library
    makes of the same requests. Passes when all three sources send a bar with the same prices
    (open and close equal, high and low within 1e-6) for one UTC day, each stamped at its first
    or last millisecond, which the library labels with that day; the range bar is stamped at
    00:00 UTC, and `/prev` and grouped daily at 23:59:59.999. A failure means a source cuts or
    stamps days otherwise: a stamp at midnight New York time (04:00 or 05:00 UTC) or at no day
    boundary is served with a WARNING, and the minute-rebuild test says which day it covers. The
    volumes are evidence, not asserted: on 2026-11-03 the range bar of 2026-11-02 carried 12811.42
    against 12791.71 from `/prev` and grouped daily, which the day's minute bars sum to; a
    difference is Massive's to explain. The library's `previous_day_bar` and `dm.ALL` history,
    which read the same `/prev` and grouped daily as the raw requests, are recorded too.
    """
    prevs = rest_rows(_PREV, {"cryptoTicker": _PAIR}, {})
    assert len(prevs) == 1, f"/prev returned {prevs}"
    (prev,) = prevs
    day = _utc_day(int(prev["t"]))
    evidence(f"/prev {_PAIR}: t={instant_text(prev['t'] * _MS)} ohlcv={_ohlcv(prev)}")
    ranged = rest_rows(
        _RANGE,
        {
            "cryptoTicker": _PAIR,
            "multiplier": 1,
            "timespan": "day",
            "from": day - timedelta(days=1),
            "to": day + timedelta(days=1),
        },
        {"sort": "asc"},
    )
    for bar in ranged:
        evidence(f"range day bar: t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    grouped = [r for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _PAIR]
    for bar in grouped:
        evidence(f"grouped {day}: t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    equal = [instant_text(b["t"] * _MS) for b in ranged if _same(_ohlcv(b), _ohlcv(prev))]
    evidence(
        f"range bars equal to /prev: {equal}; "
        f"grouped equal to /prev: {[_same(_ohlcv(b), _ohlcv(prev)) for b in grouped]}"
    )
    for name, day_bars in (
        ("range", [b for b in ranged if _utc_day(int(b["t"])) == day]),
        ("grouped", grouped),
    ):
        for bar in day_bars:
            evidence(
                f"{name} volume {bar.get('v')} against /prev's {prev.get('v')}: relative error "
                f"{_errors(_ohlcv(bar), _ohlcv(prev))[4]}"
            )
    bars = live_session.crypto.bars(_PAIR, dm.Timespan.DAY)
    rest = partial(bars.history, day, day, source=dm.HistorySource.REST, wait=True)
    evidence(f"library history(REST): {_library_rows(rest, table_rows)}")
    _assert_one_utc_day(day, prev, ranged=ranged, grouped=grouped)
    prev_bar = live_session.crypto.previous_day_bar(_PAIR)
    prev_rows = _library_rows(partial(prev_bar.fetch, wait=True), table_rows)
    evidence(f"library previous_day_bar (/prev): {prev_rows}")
    every = live_session.crypto.bars(dm.ALL, dm.Timespan.DAY)
    outcome = _library_rows(
        lambda: every.history(day, day, source=dm.HistorySource.REST, wait=True), table_rows
    )
    shown = outcome if isinstance(outcome, str) else f"{len(outcome)} rows"
    evidence(f"library dm.ALL history(REST) (grouped daily): {shown}")


@pytest.mark.verifies("1")
@pytest.mark.timeout(100)
def test_crypto_day_bar_of_the_flat_file_matches_the_rest_range(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the `global_crypto/day_aggs_v1` row of a day is the REST range bar of that day.

    Records the file's `X:BTCUSD` row of the most recent published Monday with its `window_start`,
    the REST range bars around that day, and, for each, whether it is the same bar (open and close
    equal, the rest within 1e-6) with each field's relative error. The file writes the pair
    `X:BTC-USD`, so the read keeps every spelling the library's own file filter keeps. Passes
    when the file holds one row, stamped at 00:00 or 23:59:59.999 UTC of its day, and equal to the
    range bar of the same UTC day, so both sources cut days alike; a file row equal to no REST
    bar, or to a bar of another day, means the two sources cut days differently.
    """
    read = flat_file_tickers(_DAY_FILE, recent_monday, wire_spellings(dm.AssetClass.CRYPTO, _PAIR))
    assert read is not None, f"{_DAY_FILE} has no file for {recent_monday}"
    rows, extra = read
    for row in rows:
        stamp = row["window_start"]
        shown = "<missing>" if stamp is None else instant_text(int(stamp))
        evidence(f"{_DAY_FILE} {recent_monday}: window_start={shown} row={row}")
    evidence(f"{_DAY_FILE} undocumented headers: {sorted(extra)}")
    ranged = rest_rows(
        _RANGE,
        {
            "cryptoTicker": _PAIR,
            "multiplier": 1,
            "timespan": "day",
            "from": recent_monday - timedelta(days=1),
            "to": recent_monday + timedelta(days=1),
        },
        {"sort": "asc"},
    )
    for row in rows:
        values = tuple(
            None if row[k] is None else float(str(row[k]))
            for k in ("open", "high", "low", "close", "volume")
        )
        for bar in ranged:
            evidence(
                f"file row vs range bar {instant_text(bar['t'] * _MS)}: "
                f"same={_same(values, _ohlcv(bar))} "
                f"relative errors (o, h, l, c, v) {_errors(values, _ohlcv(bar))}"
            )
    flat = live_session.crypto.bars(_PAIR, dm.Timespan.DAY)
    outcome = _library_rows(
        lambda: flat.history(
            recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES, wait=True
        ),
        table_rows,
    )
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert len(rows) == 1, f"{_DAY_FILE} holds {len(rows)} rows of {_PAIR}"
    (row,) = rows
    stamp = row["window_start"]
    assert stamp is not None, f"{_DAY_FILE} row without window_start"
    assert _at_day_edge(int(stamp) // _MS, recent_monday), f"file row stamped {stamp}"
    values = tuple(float(str(row[k])) for k in ("open", "high", "low", "close", "volume"))
    same_day = [b for b in ranged if _utc_day(int(b["t"])) == recent_monday]
    assert any(_same(values, _ohlcv(b)) for b in same_day), "the file row is not the range bar"


@pytest.mark.verifies("38")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_crypto_day_bars_rebuilt_from_minute_bars_over_the_utc_and_new_york_days(
    *,
    rest_rows: RestRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the REST crypto day bar covers the UTC day, as the library assumes.

    Rebuilds `/prev`'s day of `X:BTCUSD` from REST minute bars twice, over the UTC day and over the
    New York day (midnight to the next midnight in New York, so a 23- or 25-hour day is right), and
    records both beside `/prev` and the range bar. Decides by comparison, not by an absolute
    tolerance: passes when the UTC rebuild's largest relative error against `/prev` is finite and at
    most a tenth of the New York rebuild's, so `/prev` covers the UTC day, the library's day cut. A
    failure means another cut: `/prev` closer to the New York rebuild means REST crypto days are New
    York days, a different bar from the flat file's, and the source of crypto day bars needs an
    owner decision; close to neither means another cut (compare the stamps and the minute coverage).
    """
    prevs = rest_rows(_PREV, {"cryptoTicker": _PAIR}, {})
    assert len(prevs) == 1, f"/prev returned {prevs}"
    (prev,) = prevs
    day = _utc_day(int(prev["t"]))
    same_day = [
        bar
        for bar in rest_rows(
            _RANGE,
            {"cryptoTicker": _PAIR, "multiplier": 1, "timespan": "day", "from": day, "to": day},
            {},
        )
        if _utc_day(int(bar["t"])) == day
    ]
    utc_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    ny_start = datetime(day.year, day.month, day.day, tzinfo=_NEW_YORK)
    following = day + timedelta(days=1)
    ny_end = datetime(following.year, following.month, following.day, tzinfo=_NEW_YORK)
    minutes = rest_rows(
        _RANGE,
        {
            "cryptoTicker": _PAIR,
            "multiplier": 1,
            "timespan": "minute",
            "from": utc_start - timedelta(hours=6),
            "to": utc_start + timedelta(hours=31),
        },
        {"sort": "asc", "limit": 50_000},
    )
    assert minutes, f"no minute bars of {_PAIR} around {day}"
    evidence(
        f"{len(minutes)} minute bars from {instant_text(minutes[0]['t'] * _MS)} "
        f"to {instant_text(minutes[-1]['t'] * _MS)}"
    )
    by_utc = _rebuilt(minutes, utc_start, utc_start + timedelta(days=1))
    by_new_york = _rebuilt(minutes, ny_start, ny_end)
    evidence(f"/prev t={instant_text(prev['t'] * _MS)} ohlcv={_ohlcv(prev)}")
    for bar in same_day:
        evidence(f"range t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    evidence(f"rebuilt over the UTC day {day}: {by_utc}")
    evidence(f"rebuilt over the New York day {day} ({ny_start} to {ny_end}): {by_new_york}")
    utc, new_york = _largest(by_utc, _ohlcv(prev)), _largest(by_new_york, _ohlcv(prev))
    evidence(
        f"largest relative error against /prev: UTC rebuild {utc:.2e}, "
        f"New York rebuild {new_york:.2e}"
    )
    assert math.isfinite(utc), "the UTC rebuild lacks a field of /prev"
    assert utc <= new_york / _CLOSER, "/prev is not much closer to the UTC day's rebuild"


_VOLUME_DAYS = 5
"""How many UTC days, ending with `/prev`'s, the volume check compares: the range bar's volume has
differed on one day and agreed on another, so one day alone could pass by luck."""


def _volume_error(left: Mapping[str, Any], right: Mapping[str, Any]) -> float | None:
    """Return the relative error of two raw REST bars' volumes.

    Returns:
        The error, or None when either bar lacks a volume.
    """
    return _errors(_ohlcv(left), _ohlcv(right))[4]


@pytest.mark.massive_issue(803)
@pytest.mark.requests(7)
@pytest.mark.timeout(100)
def test_crypto_range_day_bar_volume_equals_prev_and_grouped_daily(
    *, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether `X:BTCUSD`'s range day bar has the volume of `/prev` and grouped daily.

    Reads `/prev`, the range day bars of `/prev`'s UTC day and the four days before it, and
    grouped daily of each of those five days: 7 requests. `/prev` serves one day only, so it is
    compared on its day, and grouped daily on all five. Today the range bar's volume sometimes
    runs above the other sources, which agree with each other and with the day's minute bars:
    12,811.42 against 12,791.71 (0.15%) for 2026-11-02, while 2026-10-26 agreed everywhere.
    Five days make one lucky day no fix. The evidence first asserts that every source sent one
    bar of each day with the same prices, so the volumes compared belong to one bar. A pass
    means every volume agrees within 1e-6 on every day: Massive fixed it, so close the issue.
    """
    prevs = rest_rows(_PREV, {"cryptoTicker": _PAIR}, {})
    assert len(prevs) == 1, f"/prev returned {prevs}"
    (prev,) = prevs
    last = _utc_day(int(prev["t"]))
    days = [last - timedelta(days=n) for n in reversed(range(_VOLUME_DAYS))]
    ranged = rest_rows(
        _RANGE,
        {"cryptoTicker": _PAIR, "multiplier": 1, "timespan": "day", "from": days[0], "to": last},
        {"sort": "asc"},
    )
    differ: list[str] = []
    for day in days:
        same_day = [b for b in ranged if _utc_day(int(b["t"])) == day]
        assert len(same_day) == 1, f"range bars of {day}: {same_day}"
        (bar,) = same_day
        grouped = [r for r in rest_rows(_GROUPED, {"date": day}, {}) if r.get("T") == _PAIR]
        assert len(grouped) == 1, f"grouped rows of {_PAIR} on {day}: {grouped}"
        others = {f"GET {_GROUPED} {day}": grouped[0]}
        if day == last:
            others[f"GET {_PREV}"] = prev
        for request, other in others.items():
            assert _same(_prices(bar), _prices(other)), f"{day}: range {bar} vs {request} {other}"
            error = _volume_error(bar, other)
            evidence(
                f"{day}: range t={instant_text(bar['t'] * _MS)} v={bar.get('v')}, "
                f"{request} v={other.get('v')}, relative error {error}"
            )
            # A missing volume is no measurement, so it never reads as the volumes disagreeing.
            assert error is not None, f"{day}: no volume in range {bar} or {request} {other}"
            if error > _TOLERANCE:
                differ.append(f"{day}: GET {_RANGE} v={bar.get('v')}, {request} v={other.get('v')}")
    expect_fixed(
        evidence=evidence,
        fixed=not differ,
        shown=f"{_PAIR} range day-bar volumes unlike the other sources': {differ}",
    )


def _canonical(raw: Iterable[str]) -> set[str]:
    """Read crypto pairs as Massive spelled them into the canonical form, for a comparison.

    Args:
        raw: The pairs as one source wrote them, such as `X:BTC-USD`.

    Returns:
        Each pair's canonical form, such as `X:BTCUSD`; a spelling the library cannot read is
        kept as written.
    """
    return {canonical_from_wire(dm.AssetClass.CRYPTO, text) or text for text in raw}


@pytest.mark.verifies("39")
@pytest.mark.timeout(100)
def test_crypto_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    *,
    recent_monday: date,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Whether grouped daily and `global_crypto/day_aggs_v1` list the same pairs on a day.

    The two sources spell a pair differently (grouped daily `X:BTCUSD` or `C:EURUSD`, the file
    `X:BTC-USD` or `C:EUR-USD`), so both are compared in the canonical form the library reads
    them into; a spelling the library cannot read is kept as written, and so shows up as a
    difference. Records the pairs only one of them holds on the most recent published Monday.
    Passes when the two sets are equal, so `dm.ALL` crypto day bars hold the same pairs from either
    source; a failure means the difference belongs in the documentation of `dm.ALL` day bars.
    """
    grouped = _canonical(str(row["T"]) for row in rest_rows(_GROUPED, {"date": recent_monday}, {}))
    read = flat_file_tickers(_DAY_FILE, recent_monday, None)
    assert read is not None, f"{_DAY_FILE} has no file for {recent_monday}"
    filed = _canonical(str(row["ticker"]) for row in read[0])
    evidence(f"{recent_monday}: grouped daily {len(grouped)} pairs, {_DAY_FILE} {len(filed)} pairs")
    evidence(f"only in grouped daily: {sorted(grouped - filed)}")
    evidence(f"only in the flat file: {sorted(filed - grouped)}")
    assert grouped
    assert grouped == filed, "grouped daily and the day file hold different pairs"


@pytest.mark.verifies("33")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_crypto_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    recent_week: tuple[date, date],
    week_bar_report: Callable[[str, str, str], tuple[list[str], int, list[str]]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a crypto week bar covers the calendar week from Sunday 00:00 UTC.

    For each raw week bar of `X:BTCUSD` over the four weeks ending with the most recent week without
    a holiday, records its stamp and the candidate windows whose hour-bar volume is closest to its
    own; `MATCH` marks the bucket. Decides by comparison, not by an absolute tolerance: passes when,
    for every bar of those weeks, the week from Sunday 00:00 UTC, where Massive stamps it and the
    library starts its crypto week, is the closest window, ties allowed; a failure names the bars
    whose closest window starts elsewhere, and the library's crypto week windows must start there.
    The last line shows which bars the library's `history()` keeps.
    """
    lines, checked, misses = week_bar_report("cryptoTicker", _PAIR, "UTC")
    for line in lines:
        evidence(line)
    first, last = recent_week[0] - timedelta(days=7), recent_week[1]
    kept = live_session.crypto.bars(_PAIR, dm.Timespan.WEEK).history(
        first, last, source=dm.HistorySource.REST, wait=True
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history {first}..{last} keeps the bars stamped {stamps}")
    assert checked >= 3, f"only {checked} week bars of the four weeks"
    assert not misses, f"week bars closer to another window than the library's: {misses}"


@pytest.mark.settles("49")
@pytest.mark.requests(10)
@pytest.mark.timeout(100)
def test_crypto_ticker_list_currencies_join(
    live_rest: RestClient,
    *,
    crypto_report: CryptoReport,
    evidence: Evidence,
) -> None:
    """Whether every pair of Massive's crypto ticker list names currencies that join to its ticker.

    The library splits a crypto ticker into its two currencies (for a stream topic and for the
    last-trade and daily open/close paths) only from this list: it reads
    `/v3/reference/tickers?market=crypto` once per session, every page, first with
    `active=true`, then with `active=false` so a delisted pair splits too, and refuses a pair
    the list lacks or whose `base_currency_symbol` and `currency_symbol` do not join to its
    ticker (`X:` + base + quote). Reads the same two lists, every page of 1,000 rows, one
    request a page: on 2026-11-10 Massive listed 624 pairs, one page, and the test declares
    ten requests in all.

    Records each list's size, the tickers in both lists, the tickers without the `X:` prefix,
    and every row whose currencies do not join to its ticker. Reading it: no such row confirms
    that the list splits every pair it holds; each one is a pair the library refuses on those
    paths, which needs a split from elsewhere or stays refused; a large delisted list means
    the session's first read of it is slow where Massive limits the requests a minute.
    """
    reads = []
    for active in (True, False):
        # Paced as Currencies: whether Massive charges a lookup filtered by market to its
        # class is an open question, so the run never risks a refusal over it.
        pages = live_rest.pages(
            source(Transport.REST, _TICKERS),
            {},
            {"market": "crypto", "active": active, "limit": _TICKERS_PAGE},
            bucket="currencies",
        )
        reads.append([row for page in pages for row in page.rows])
    for line in crypto_report(*reads, listed_limit=None, delisted_limit=None):
        evidence(line)
    assert reads[0], "Massive listed no crypto pair"


@pytest.mark.massive_issue(800)
@pytest.mark.requests(10)
@pytest.mark.timeout(100)
def test_every_crypto_ticker_list_row_joins_its_currencies_to_its_ticker(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether every row of Massive's crypto ticker list names currencies that join to its ticker.

    Reads `/v3/reference/tickers?market=crypto`, every page of 1,000 rows, with `active=true`
    and with `active=false`, as the library does: one page each on 2026-11-10, ten requests
    declared. Today two rows do not join (`X:` + `base_currency_symbol` + `currency_symbol` is
    not the ticker): `X:DNYUSD` has base `DNT`, and `X:TUSDEUR` has quote `USD`, so the library
    refuses those pairs. The evidence first asserts that the listed read holds rows. A pass
    means every row of both reads joins: Massive fixed the list, so close the issue; the library
    then splits the two pairs with no change.
    """
    refused: list[str] = []
    for active, read in ((True, "listed"), (False, "delisted")):
        query = {"market": "crypto", "active": active, "limit": _TICKERS_PAGE}
        pages = live_rest.pages(source(Transport.REST, _TICKERS), {}, query, bucket="currencies")
        rows = [row for page in pages for row in page.rows]
        evidence(f"GET {_TICKERS} {query}: {len(rows)} rows")
        assert rows or not active, "Massive listed no crypto pair"
        refused += ticker_lists.unjoined(rows, read)
    evidence(f"rows whose currencies do not join to the ticker: {refused}")
    expect_fixed(
        evidence=evidence,
        fixed=not refused,
        shown=f"GET {_TICKERS}?market=crypto rows whose currencies do not join: {refused}",
    )


@pytest.mark.verifies("3")
@pytest.mark.timeout(160)
def test_fmv_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    stream_report: Report,
    skip_if_not_entitled: SkipIfNotEntitled,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a WebSocket `FMV` frame's `t` converts to the current instant.

    `FMV` `t` is documented in nanoseconds while the sample shows milliseconds, so the
    library reads each value by its magnitude and logs once which unit the column arrived
    in. Streams `X:BTCUSD`'s fair market value, asserts that Massive acknowledged the topic,
    and records the first rows, their age, and that log line; a refused stream skips, quoting
    Massive. Crypto trades around the clock, so rows are required. Passes when the first
    row's instant is within a day of now; the logged unit settles which unit Massive sends.
    """
    caplog.set_level(logging.INFO, logger="deephaven_massive")
    table = live_session.crypto.fmv(_PAIR).append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    rows = wait_rows(live_session, table, time.monotonic() + _STREAM_WAIT)
    for line in stream_report(live_session):
        evidence(line)
    skip_if_not_entitled(live_session)
    live_session.release(table)
    detected = [r.getMessage() for r in caplog.records if "read by magnitude" in r.message]
    evidence(f"magnitude rule log: {detected}")
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"FMV row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    assert acked, "Massive acknowledged no FMV topic"
    assert rows, f"no FMV frame arrived in {_STREAM_WAIT:.0f} s"
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS
