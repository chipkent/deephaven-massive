"""Live checks of stock data against Massive: stream time units, bar alignment, and filings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Streams subscribe to one symbol, wait for
their first rows, and are released at once; raw bars come from the wire REST client, because
the library filters the very bars in question.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_SYMBOL = "AAPL"
_RANGE = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_MS = 1_000_000
_DAY_NS = 86_400 * 1_000_000_000
_STREAM_WAIT = 100.0
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
    wait_rows: WaitRows,
    report: Report,
    evidence: Evidence,
) -> list[dict[str, Any]]:
    """Wait for a stream's first rows, release it, and record the rows and the diagnostics.

    Returns:
        The rows received before the wait ended.
    """
    rows = wait_rows(session, table, _STREAM_WAIT)
    session.release(table)
    for line in report(session):
        evidence(line)
    return rows


@pytest.mark.timeout(110)
def test_fmv_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    caplog.set_level(logging.INFO, logger="deephaven_massive")
    table = live_session.stocks.fmv(_SYMBOL).append_only()
    rows = _stream_evidence(
        live_session, table, wait_rows=wait_rows, report=stream_report, evidence=evidence
    )
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps detected" in r.message]
    evidence(f"magnitude rule log: {detected}")
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"FMV row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    if not rows:
        pytest.skip("no FMV frame arrived (a Business key is needed; see the stream above)")
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS


@pytest.mark.timeout(110)
def test_luld_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    table = live_session.stocks.luld(_SYMBOL).append_only()
    rows = _stream_evidence(
        live_session, table, wait_rows=wait_rows, report=stream_report, evidence=evidence
    )
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"LULD row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    if not rows:
        pytest.skip(f"no LULD frame for {_SYMBOL} arrived (bands change only in market hours)")
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS


@pytest.mark.timeout(110)
def test_filings_rows_without_a_ticker(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    filings = live_session.stocks.filings
    start, end = date(2025, 3, 1), date(2025, 3, 31)
    queries = {
        "filings.index": filings.index(start=start, end=end, limit=5_000),
        "filings.sections_10k": filings.sections_10k(start=start, end=end, limit=5_000),
        "filings.text_8k": filings.text_8k(start=start, end=end, limit=5_000),
        "filings.risk_factors": filings.risk_factors(start=start, end=end, limit=5_000),
    }
    counts = {}
    for name, query in queries.items():
        rows = table_rows(query.fetch())
        lacking = [row for row in rows if row["Symbol"] is None]
        counts[name] = len(rows)
        evidence(f"{name} {start}..{end}: {len(rows)} rows, {len(lacking)} without a ticker")
        for row in lacking[:3]:
            evidence(f"    e.g. {row}")
    assert any(counts.values())


@pytest.mark.timeout(110)
def test_stock_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    week_bar_report: Callable[[str, str], list[str]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    lines = week_bar_report("stocksTicker", _SYMBOL)
    for line in lines:
        evidence(line)
    kept = live_session.stocks.bars(_SYMBOL, dm.Timespan.WEEK).history(
        date(2025, 3, 3), date(2025, 3, 16), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["Timestamp"]) for row in table_rows(kept)]
    evidence(f"library history 2025-03-03..2025-03-16 keeps the bars stamped {stamps}")
    assert len(lines) > 1


@pytest.mark.timeout(110)
def test_month_bars_from_mid_month_against_the_whole_month(
    rest_rows: RestRows, *, instant_text: Text, evidence: Evidence
) -> None:
    whole = _bars(rest_rows, "month", multiplier=1, bounds=(date(2025, 3, 1), date(2025, 6, 30)))
    partial = _bars(rest_rows, "month", multiplier=1, bounds=(date(2025, 3, 15), date(2025, 6, 30)))
    for name, bars in (("from 2025-03-01", whole), ("from 2025-03-15", partial)):
        for bar in bars:
            evidence(
                f"{name}: t={instant_text(bar['t'] * _MS)} o={bar['o']} c={bar['c']} v={bar['v']}"
            )
    assert whole
    assert partial


@pytest.mark.timeout(110)
def test_bars_of_several_units_align_to_boundaries_or_to_from(
    rest_rows: RestRows, *, instant_text: Text, evidence: Evidence
) -> None:
    five = _bars(
        rest_rows,
        "minute",
        multiplier=5,
        bounds=(datetime(2025, 3, 4, 14, 32, tzinfo=UTC), datetime(2025, 3, 4, 15, 32, tzinfo=UTC)),
    )
    two_weeks = _bars(rest_rows, "week", multiplier=2, bounds=(date(2025, 3, 5), date(2025, 5, 1)))
    evidence(f"5-minute bars from 14:32 UTC: {[instant_text(b['t'] * _MS) for b in five]}")
    evidence(
        "2-week bars from Wednesday 2025-03-05: "
        f"{[(instant_text(b['t'] * _MS), b['v']) for b in two_weeks]}"
    )
    assert five
    assert two_weeks
