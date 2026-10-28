"""Live checks of forex data against Massive: day-bar stamps, weeks, pair sets, and topics.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw rows come from the wire REST and
flat-file clients, because the library moves the very stamps in question; the library's own
answer to the same request is recorded beside them.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

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
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_PAIR = "C:EURUSD"
_RANGE = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{forexTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/fx/{date}"
_DAY_FILE = "global_forex/day_aggs_v1"
_FILE_DAY = date(2025, 6, 2)
"""A past Monday whose flat files are long published, for the flat-file comparisons."""

_MS = 1_000_000
_STREAM_WAIT = 60.0
"""Seconds a stream test waits for its first frame; forex is closed at weekends."""


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
def test_forex_day_bars_of_every_rest_source_and_their_stamps(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    prevs = rest_rows(_PREV, {"forexTicker": _PAIR}, {})
    assert len(prevs) == 1, f"/prev returned {prevs}"
    (prev,) = prevs
    day = _utc_day(int(prev["t"]))
    evidence(f"/prev {_PAIR}: t={instant_text(prev['t'] * _MS)} ohlcv={_ohlcv(prev)}")
    ranged = rest_rows(
        _RANGE,
        {
            "forexTicker": _PAIR,
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
    evidence(
        f"range bars equal to /prev: "
        f"{[instant_text(b['t'] * _MS) for b in ranged if _ohlcv(b) == _ohlcv(prev)]}; "
        f"grouped equal to /prev: {[_ohlcv(b) == _ohlcv(prev) for b in grouped]}"
    )
    bars = live_session.forex.bars(_PAIR, dm.Timespan.DAY)
    evidence(f"library fetch() (/prev): {_library_rows(bars.fetch, table_rows)}")
    evidence(
        "library history(REST): "
        f"{_library_rows(lambda: bars.history(day, day, source=dm.HistorySource.REST), table_rows)}"
    )
    assert ranged
    assert grouped


@pytest.mark.timeout(110)
def test_forex_day_bar_of_the_flat_file_and_its_stamp(
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
            "forexTicker": _PAIR,
            "multiplier": 1,
            "timespan": "day",
            "from": _FILE_DAY - timedelta(days=1),
            "to": _FILE_DAY + timedelta(days=1),
        },
        {"sort": "asc"},
    )
    grouped = [r for r in rest_rows(_GROUPED, {"date": _FILE_DAY}, {}) if r.get("T") == _PAIR]
    for bar in ranged:
        evidence(f"range day bar: t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    for bar in grouped:
        evidence(f"grouped {_FILE_DAY}: t={instant_text(bar['t'] * _MS)} ohlcv={_ohlcv(bar)}")
    for row in rows:
        values = tuple(float(row[k] or "nan") for k in ("open", "high", "low", "close", "volume"))
        matches = [instant_text(b["t"] * _MS) for b in [*ranged, *grouped] if _ohlcv(b) == values]
        evidence(f"REST bars whose OHLCV equal the file row: {matches}")
    flat = live_session.forex.bars(_PAIR, dm.Timespan.DAY).history(
        _FILE_DAY, _FILE_DAY, source=dm.HistorySource.FLAT_FILES
    )
    evidence(f"library history(FLAT_FILES): {table_rows(flat)}")
    assert rows


@pytest.mark.timeout(110)
def test_forex_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    rest_rows: RestRows, *, flat_file_tickers: FlatFileTickers, evidence: Evidence
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
def test_forex_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    week_bar_report: Callable[[str, str], list[str]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    lines = week_bar_report("forexTicker", _PAIR)
    for line in lines:
        evidence(line)
    kept = live_session.forex.bars(_PAIR, dm.Timespan.WEEK).history(
        date(2025, 3, 3), date(2025, 3, 16), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["Timestamp"]) for row in table_rows(kept)]
    evidence(f"library history 2025-03-03..2025-03-16 keeps the bars stamped {stamps}")
    assert len(lines) > 1


@pytest.mark.timeout(110)
def test_forex_pair_topics_spelled_with_a_dash_are_acknowledged_and_deliver(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Callable[[dm.MassiveSession], list[str]],
    evidence: Evidence,
) -> None:
    quotes = live_session.forex.quotes(_PAIR).append_only()
    bars = live_session.forex.bars(_PAIR).append_only()
    quote_rows = wait_rows(live_session, quotes, _STREAM_WAIT)
    bar_rows = wait_rows(live_session, bars, _STREAM_WAIT)
    live_session.release(quotes)
    live_session.release(bars)
    for line in stream_report(live_session):
        evidence(line)
    evidence(f"C quotes received: {len(quote_rows)}; first: {quote_rows[:1]}")
    evidence(f"CA minute bars received: {len(bar_rows)}; first: {bar_rows[:1]}")
    if not quote_rows:
        pytest.skip(f"no forex quote arrived in {_STREAM_WAIT:.0f} s (is the market closed?)")
    assert {row["Symbol"] for row in quote_rows} == {_PAIR}
