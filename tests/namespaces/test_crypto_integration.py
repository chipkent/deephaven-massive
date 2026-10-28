"""Live checks of crypto bars against Massive: day cuts, minute rebuilds, weeks, and pair sets.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw rows come from the wire REST and
flat-file clients, because the library moves or refuses the very stamps in question; the
library's own answer to the same request is recorded beside them.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

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

_PAIR = "X:BTCUSD"
_RANGE = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{cryptoTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/crypto/{date}"
_DAY_FILE = "global_crypto/day_aggs_v1"
_FILE_DAY = date(2025, 6, 2)
"""A past Monday whose flat files are long published, for the flat-file comparisons."""

_MS = 1_000_000
_DAY_MS = 86_400_000
_NEW_YORK = ZoneInfo("America/New_York")


def _ohlcv(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return a raw REST bar's open, high, low, close, and volume."""
    return row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v")


def _utc_day(stamp_ms: int) -> date:
    """Return the UTC calendar day an epoch-millisecond stamp falls on."""
    return datetime.fromtimestamp(stamp_ms / 1000, UTC).date()


def _library_rows(call: Callable[[], Table], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Run one library call and describe its outcome.

    Returns:
        The result's rows, or the text of the `MassiveError` the call raised.
    """
    try:
        return table_rows(call())
    except dm.MassiveError as error:
        return f"MassiveError: {error}"


@pytest.mark.timeout(110)
def test_crypto_day_bars_of_every_rest_source_share_one_day_cut(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
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
    same_day = [bar for bar in ranged if _utc_day(int(bar["t"])) == day]
    evidence(
        f"range bar on /prev's UTC day agrees with /prev: "
        f"{[_ohlcv(bar) == _ohlcv(prev) for bar in same_day]}; "
        f"grouped agrees with /prev: {[_ohlcv(bar) == _ohlcv(prev) for bar in grouped]}"
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


@pytest.mark.timeout(110)
def test_crypto_day_bar_of_the_flat_file_matches_the_rest_range(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    read = flat_file_tickers(_DAY_FILE, _FILE_DAY, [_PAIR])
    assert read is not None, f"{_DAY_FILE} has no file for {_FILE_DAY}"
    rows, extra = read
    for row in rows:
        stamp = int(row["window_start"] or 0)
        evidence(f"{_DAY_FILE} {_FILE_DAY}: window_start={instant_text(stamp)} row={row}")
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
    for bar in ranged:
        evidence(f"range day bar: t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    for row in rows:
        values = tuple(float(row[k] or "nan") for k in ("open", "high", "low", "close", "volume"))
        matches = [instant_text(b["t"] * _MS) for b in ranged if _ohlcv(b) == values]
        evidence(f"range bars whose OHLCV equal the file row: {matches}")
    flat = live_session.crypto.bars(_PAIR, dm.Timespan.DAY).history(
        _FILE_DAY, _FILE_DAY, source=dm.HistorySource.FLAT_FILES
    )
    evidence(f"library history(FLAT_FILES): {table_rows(flat)}")
    assert rows


@pytest.mark.timeout(110)
def test_crypto_day_bars_rebuilt_from_minute_bars_over_the_utc_and_new_york_days(
    rest_rows: RestRows,
    *,
    instant_text: Text,
    evidence: Evidence,
) -> None:
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
    ranged = same_day[0] if same_day else None
    utc_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    ny_start = datetime(day.year, day.month, day.day, tzinfo=_NEW_YORK)
    minutes = rest_rows(
        _RANGE,
        {
            "cryptoTicker": _PAIR,
            "multiplier": 1,
            "timespan": "minute",
            "from": utc_start - timedelta(hours=6),
            "to": utc_start + timedelta(hours=30),
        },
        {"sort": "asc", "limit": 50_000},
    )
    assert minutes, f"no minute bars of {_PAIR} around {day}"
    evidence(
        f"{len(minutes)} minute bars from {instant_text(minutes[0]['t'] * _MS)} "
        f"to {instant_text(minutes[-1]['t'] * _MS)}"
    )

    def rebuilt(start: datetime) -> tuple[Any, ...]:
        start_ms = int(start.timestamp() * 1000)
        inside = [m for m in minutes if start_ms <= m["t"] < start_ms + _DAY_MS]
        if not inside:
            return ()
        return (
            inside[0]["o"],
            max(m["h"] for m in inside),
            min(m["l"] for m in inside),
            inside[-1]["c"],
            sum(m["v"] for m in inside),
        )

    by_utc, by_new_york = rebuilt(utc_start), rebuilt(ny_start)
    evidence(f"/prev t={instant_text(prev['t'] * _MS)} ohlcv={_ohlcv(prev)}")
    if ranged is not None:
        evidence(f"range t={instant_text(ranged['t'] * _MS)} ohlcv={_ohlcv(ranged)}")
    evidence(f"rebuilt over the UTC day {day}: {by_utc}")
    evidence(f"rebuilt over the New York day {day}: {by_new_york}")
    evidence(
        f"/prev open/close match UTC day: {by_utc[:4:3] == _ohlcv(prev)[:4:3]}; "
        f"match New York day: {by_new_york[:4:3] == _ohlcv(prev)[:4:3]}"
    )
    assert minutes


@pytest.mark.timeout(110)
def test_crypto_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    rest_rows: RestRows,
    *,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    grouped = {str(row["T"]) for row in rest_rows(_GROUPED, {"date": _FILE_DAY}, {})}
    read = flat_file_tickers(_DAY_FILE, _FILE_DAY, None)
    assert read is not None, f"{_DAY_FILE} has no file for {_FILE_DAY}"
    filed = {str(row["ticker"]) for row in read[0]}
    evidence(f"{_FILE_DAY}: grouped daily {len(grouped)} pairs, {_DAY_FILE} {len(filed)} pairs")
    evidence(f"only in grouped daily: {sorted(grouped - filed)}")
    evidence(f"only in the flat file: {sorted(filed - grouped)}")
    assert grouped
    assert filed


@pytest.mark.timeout(110)
def test_crypto_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    week_bar_report: Callable[[str, str], list[str]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    lines = week_bar_report("cryptoTicker", _PAIR)
    for line in lines:
        evidence(line)
    kept = live_session.crypto.bars(_PAIR, dm.Timespan.WEEK).history(
        date(2025, 3, 3), date(2025, 3, 16), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["Timestamp"]) for row in table_rows(kept)]
    evidence(f"library history 2025-03-03..2025-03-16 keeps the bars stamped {stamps}")
    assert len(lines) > 1
