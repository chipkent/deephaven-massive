"""Live checks of crypto bars against Massive: day cuts, minute rebuilds, weeks, and pair sets.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, answers the question. Raw rows come from the wire REST and flat-file
clients, because the library moves or refuses the very stamps in question; the library's own
answer to the same request is recorded beside them. How to run these tests is in
`docs/live-tests.md`.

The library's rule under test: a crypto bar of one day covers one UTC day and is stamped at
00:00 UTC of it, from every source; a 1-day bar stamped anywhere else in its day but its first
or last millisecond fails the request with `MassiveError` rather than being relabelled.
"""

from __future__ import annotations

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
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
NotOnBasic: TypeAlias = "Callable[[str], None]"

_PAIR = "X:BTCUSD"
_RANGE = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{cryptoTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/crypto/{date}"
_DAY_FILE = "global_crypto/day_aggs_v1"
_FILE_DAY = date(2026, 6, 1)
"""A past Monday whose flat files are published, inside a Basic plan's two years of history."""

_MS = 1_000_000
_NEW_YORK = ZoneInfo("America/New_York")
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

    Returns:
        The result's rows, or the text of the `MassiveError` the call raised.
    """
    try:
        return table_rows(call())
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


@pytest.mark.timeout(100)
def test_crypto_day_bars_of_every_rest_source_share_one_day_cut(
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
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
    not_on_basic("currencies")
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


@pytest.mark.timeout(100)
def test_crypto_day_bar_of_the_flat_file_matches_the_rest_range(
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the `global_crypto/day_aggs_v1` row of a day is the REST range bar of that day.

    Records the file's `X:BTCUSD` row of 2026-06-01 with its `window_start`, the REST range
    bars around that day, and, for each, whether it is the same bar (open and close equal, the
    rest within 1e-6) with each field's relative error. Reading it: a file row stamped at
    00:00 or 23:59:59.999 UTC of its day and equal to the range bar of the same UTC day
    confirms one day cut for both sources; a file row equal to no REST bar, or to a bar of
    another day, means the two sources cut days differently.
    """
    not_on_basic("currencies")
    read = flat_file_tickers(_DAY_FILE, _FILE_DAY, [_PAIR])
    assert read is not None, f"{_DAY_FILE} has no file for {_FILE_DAY}"
    rows, extra = read
    for row in rows:
        stamp = row["window_start"]
        shown = "<missing>" if stamp is None else instant_text(int(stamp))
        evidence(f"{_DAY_FILE} {_FILE_DAY}: window_start={shown} row={row}")
    evidence(f"{_DAY_FILE} undocumented headers: {sorted(extra)}")
    ranged = rest_rows(
        _RANGE,
        {
            "cryptoTicker": _PAIR,
            "multiplier": 1,
            "timespan": "day",
            "from": _FILE_DAY - timedelta(days=1),
            "to": _FILE_DAY + timedelta(days=1),
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
        lambda: flat.history(_FILE_DAY, _FILE_DAY, source=dm.HistorySource.FLAT_FILES),
        table_rows,
    )
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert rows


@pytest.mark.timeout(100)
def test_crypto_day_bars_rebuilt_from_minute_bars_over_the_utc_and_new_york_days(
    not_on_basic: NotOnBasic,
    *,
    rest_rows: RestRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which day the REST crypto day bar covers: the UTC day or the New York day.

    Rebuilds `/prev`'s day of `X:BTCUSD` from REST minute bars twice, over the UTC day and
    over the New York day (midnight to the next midnight in New York, so a 23- or 25-hour day
    is right), and records both beside `/prev` and the range bar. Reading it: `/prev` equal to
    the UTC rebuild confirms the UTC day; equal to the New York rebuild means REST crypto days
    are New York days, a different bar from the flat file's, and the source of crypto day bars
    needs an owner decision; equal to neither means another cut (compare the stamps and the
    minute coverage).
    """
    not_on_basic("currencies")
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
    evidence(
        f"/prev equals the UTC rebuild: {_same(by_utc, _ohlcv(prev))}; "
        f"equals the New York rebuild: {_same(by_new_york, _ohlcv(prev))}"
    )


@pytest.mark.timeout(100)
def test_crypto_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    not_on_basic: NotOnBasic,
    *,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Whether grouped daily and `global_crypto/day_aggs_v1` list the same pairs on a day.

    Records the pairs only one of them holds on 2026-06-01. Reading it: equal sets mean
    `dm.ALL` crypto day bars hold the same pairs from either source; otherwise the difference
    belongs in the documentation of `dm.ALL` day bars.
    """
    not_on_basic("currencies")
    grouped = {str(row["T"]) for row in rest_rows(_GROUPED, {"date": _FILE_DAY}, {})}
    read = flat_file_tickers(_DAY_FILE, _FILE_DAY, None)
    assert read is not None, f"{_DAY_FILE} has no file for {_FILE_DAY}"
    filed = {str(row["ticker"]) for row in read[0]}
    evidence(f"{_FILE_DAY}: grouped daily {len(grouped)} pairs, {_DAY_FILE} {len(filed)} pairs")
    evidence(f"only in grouped daily: {sorted(grouped - filed)}")
    evidence(f"only in the flat file: {sorted(filed - grouped)}")
    assert grouped
    assert filed


@pytest.mark.timeout(100)
def test_crypto_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
    week_bar_report: Callable[[str, str], list[str]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which seven days a crypto week bar covers, whatever day it is stamped on.

    For each raw week bar of `X:BTCUSD` around 2026-02-23 to 2026-03-22, records its stamp and
    the candidate windows whose hour-bar volume is closest to its own; `MATCH` marks the
    bucket. Reading it: a Monday 00:00 UTC match confirms calendar weeks, as the library
    assumes; any other start means the library's crypto week windows need that start. The last
    line shows which bars the library's `history()` keeps.
    """
    not_on_basic("currencies")
    lines = week_bar_report("cryptoTicker", _PAIR)
    for line in lines:
        evidence(line)
    kept = live_session.crypto.bars(_PAIR, dm.Timespan.WEEK).history(
        date(2026, 3, 2), date(2026, 3, 15), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["Timestamp"]) for row in table_rows(kept)]
    evidence(f"library history 2026-03-02..2026-03-15 keeps the bars stamped {stamps}")
    assert len(lines) > 1
