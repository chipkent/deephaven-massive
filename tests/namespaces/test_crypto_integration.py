"""Live checks of crypto bars against Massive: day cuts, minute rebuilds, weeks, and pair sets.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST and flat-file clients, because the library moves or refuses the very stamps in
question; the library's own answer to the same request is recorded beside them. How to run these
tests is in `docs/live-tests.md`.

The library's rule under test: a crypto bar of one day covers one UTC day and is stamped at
00:00 UTC of it, from every source; a 1-day bar stamped anywhere else in its day but its first
or last millisecond fails the request with `MassiveError` rather than being relabelled.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping, Sequence
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

_PAIR = "X:BTCUSD"
_RANGE = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{cryptoTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/crypto/{date}"
_DAY_FILE = "global_crypto/day_aggs_v1"
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
    """Compare two OHLCV tuples: open and close exactly, the rest within the tolerance.

    Returns:
        Whether they describe the same bar.
    """
    if len(left) != len(right) or len(left) != 5:
        return False
    errors = [error for error in _errors(left, right) if error is not None]
    if len(errors) != len(left):
        return False
    return left[0] == right[0] and left[3] == right[3] and max(errors) <= _TOLERANCE


def _library_rows(call: Callable[[], Table], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Run one library call and describe its outcome.

    A refusal is not an outcome to record: it propagates, so the test fails (the declared plan
    covers the data) rather than reporting it as an answer.

    Returns:
        The result's rows, or the text of the `MassiveError` (other than a refusal) it raised.

    Raises:
        EntitlementError: When the call is refused.
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


@pytest.mark.needs("currencies:basic")
@pytest.mark.settles("1")
@pytest.mark.requests(8)
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
    makes of the same requests. Reading it: stamps at 00:00 UTC that agree everywhere confirm
    the library's day cut. Stamps at midnight New York time (04:00 or 05:00 UTC) mean REST
    uses another day cut or only another stamp; the minute-rebuild test says which, and the
    library refuses those bars with `MassiveError` (shown here) until that is decided. A
    grouped stamp at no day boundary (such as 20:48 UTC) is the same bar with a write-time
    stamp if its OHLCV equal the range bar's, otherwise another day cut.
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
    bars = live_session.crypto.bars(_PAIR, dm.Timespan.DAY)
    evidence(f"library fetch() (/prev): {_library_rows(bars.fetch, table_rows)}")
    evidence(
        "library history(REST): "
        f"{_library_rows(lambda: bars.history(day, day, source=dm.HistorySource.REST), table_rows)}"
    )
    every = live_session.crypto.bars(dm.ALL, dm.Timespan.DAY)
    outcome = _library_rows(
        lambda: every.history(day, day, source=dm.HistorySource.REST), table_rows
    )
    shown = outcome if isinstance(outcome, str) else f"{len(outcome)} rows"
    evidence(f"library dm.ALL history(REST) (grouped daily): {shown}")
    assert ranged
    assert grouped


@pytest.mark.needs("currencies:starter", "s3")
@pytest.mark.settles("1")
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
    equal, the rest within 1e-6) with each field's relative error. Reading it: a file row stamped at
    00:00 or 23:59:59.999 UTC of its day and equal to the range bar of the same UTC day confirms one
    day cut for both sources; a file row equal to no REST bar, or to a bar of another day, means the
    two sources cut days differently.
    """
    read = flat_file_tickers(_DAY_FILE, recent_monday, [_PAIR])
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
        lambda: flat.history(recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES),
        table_rows,
    )
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert rows


@pytest.mark.needs("currencies:basic")
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

    Rebuilds `/prev`'s day of `X:BTCUSD` from REST minute bars twice, over the UTC day and
    over the New York day (midnight to the next midnight in New York, so a 23- or 25-hour day
    is right), and records both beside `/prev` and the range bar. Decides by comparison, not
    by an absolute tolerance: passes when the UTC rebuild's largest relative error against
    `/prev` is at most a tenth of the New York rebuild's, so `/prev` covers the UTC day, the
    library's day cut. A failure means another cut: `/prev` closer to the New York rebuild
    means REST crypto days are New York days, a different bar from the flat file's, and the
    source of crypto day bars needs an owner decision; close to neither means another cut
    (compare the stamps and the minute coverage).
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
    assert utc <= new_york / _CLOSER, "/prev is not much closer to the UTC day's rebuild"


@pytest.mark.needs("currencies:starter", "s3")
@pytest.mark.settles("39")
@pytest.mark.timeout(100)
def test_crypto_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    *,
    recent_monday: date,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Whether grouped daily and `global_crypto/day_aggs_v1` list the same pairs on a day.

    Records the pairs only one of them holds on the most recent published Monday. Reading it: equal
    sets mean `dm.ALL` crypto day bars hold the same pairs from either source; otherwise the
    difference belongs in the documentation of `dm.ALL` day bars.
    """
    grouped = {str(row["T"]) for row in rest_rows(_GROUPED, {"date": recent_monday}, {})}
    read = flat_file_tickers(_DAY_FILE, recent_monday, None)
    assert read is not None, f"{_DAY_FILE} has no file for {recent_monday}"
    filed = {str(row["ticker"]) for row in read[0]}
    evidence(f"{recent_monday}: grouped daily {len(grouped)} pairs, {_DAY_FILE} {len(filed)} pairs")
    evidence(f"only in grouped daily: {sorted(grouped - filed)}")
    evidence(f"only in the flat file: {sorted(filed - grouped)}")
    assert grouped
    assert filed


@pytest.mark.needs("currencies:basic")
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
    """Whether a crypto week bar covers the calendar week from Monday 00:00 UTC.

    For each raw week bar of `X:BTCUSD` over the four weeks ending with the most recent week without
    a holiday, records its stamp and the candidate windows whose hour-bar volume is closest to its
    own; `MATCH` marks the bucket. Decides by comparison, not by an absolute tolerance: passes when,
    for every bar of those weeks, the week from Monday 00:00 UTC, the calendar week the library
    assumes, is the closest window, ties allowed; a failure names the bars whose closest window
    starts elsewhere (such as a Sunday start), and the library's crypto week windows must start
    there. The last line shows which bars the library's `history()` keeps.
    """
    lines, checked, misses = week_bar_report("cryptoTicker", _PAIR, "UTC")
    for line in lines:
        evidence(line)
    first, last = recent_week[0] - timedelta(days=7), recent_week[1]
    kept = live_session.crypto.bars(_PAIR, dm.Timespan.WEEK).history(
        first, last, source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history {first}..{last} keeps the bars stamped {stamps}")
    assert checked >= 3, f"only {checked} week bars of the four weeks"
    assert not misses, f"week bars closer to another window than the library's: {misses}"


@pytest.mark.needs("currencies:business")
@pytest.mark.verifies("3")
@pytest.mark.timeout(160)
def test_fmv_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a WebSocket `FMV` frame's `t` converts to the current instant.

    `FMV` `t` is documented in nanoseconds while the sample shows milliseconds, so the
    library reads it by magnitude and logs once which unit it saw. Streams `X:BTCUSD`'s fair
    market value (the cheapest Business plan is Currencies Business), asserts that Massive
    acknowledged the topic, and records the first rows, their age, and that log line. Crypto
    trades around the clock, so rows are required. Passes when the first row's instant is
    within a day of now; the logged unit settles which unit Massive sends.
    """
    caplog.set_level(logging.INFO, logger="deephaven_massive")
    table = live_session.crypto.fmv(_PAIR).append_only()
    acked = wait_acked(live_session, 1, time.monotonic() + _STREAM_WAIT)
    rows = wait_rows(live_session, table, time.monotonic() + _STREAM_WAIT)
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(table)
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps detected" in r.message]
    evidence(f"magnitude rule log: {detected}")
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"FMV row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    assert acked, "Massive acknowledged no FMV topic"
    assert rows, f"no FMV frame arrived in {_STREAM_WAIT:.0f} s"
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS
