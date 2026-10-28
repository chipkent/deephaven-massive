"""Live checks of stock data against Massive: stream time units, bar alignment, and filings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked,
except where the check is a plain yes or no. Streams subscribe to one symbol, wait for their
first rows, and are released at once; raw bars come from the wire REST client, because the
library filters the very bars in question. How to run these tests is in `docs/live-tests.md`.
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

# Fixture types, repeated in each live module: test modules may not import from conftest.
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
WaitRows: TypeAlias = "Callable[[dm.MassiveSession, Table, float], list[dict[str, Any]]]"
Report: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"
NotOnBasic: TypeAlias = "Callable[[str], None]"

_SYMBOL = "AAPL"
_RANGE = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_MS = 1_000_000
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
) -> list[dict[str, Any]]:
    """Wait for a stream's first rows, record the diagnostics, then release the stream.

    The diagnostics are read before the release, which the session carries out on another
    thread, so they show the stream's acknowledgement and error as they were.

    Returns:
        The rows received before the wait ended.
    """
    rows = wait_rows(session, table, time.monotonic() + _STREAM_WAIT)
    for line in report(session):
        evidence(line)
    session.release(table)
    return rows


@pytest.mark.timeout(100)
def test_fmv_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a WebSocket `FMV` frame's `t` converts to the current instant.

    `FMV` `t` is documented in nanoseconds while the sample shows milliseconds, so the
    library reads it by magnitude and logs once which unit it saw. Records the first rows,
    their age, and that log line. Reading it: an instant within a day of now and the logged
    unit settle the question; a refused stream (the key lacks the Business entitlement) skips.
    """
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


@pytest.mark.timeout(100)
def test_luld_frames_convert_to_a_current_instant(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    stream_report: Report,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a WebSocket `LULD` frame's `t` converts to the current instant.

    `LULD` `t` is documented in milliseconds while the sample has 19 digits; the library reads
    nanoseconds and fails loudly (a failed stream) on a mismatch. Run this test in the first
    minutes after the 09:30 New York open, when bands are published for every symbol; later,
    one symbol's bands change only on a large move. Reading it: an instant within a day of now
    confirms nanoseconds; a failed stream naming the bound means milliseconds.
    """
    table = live_session.stocks.luld(_SYMBOL).append_only()
    rows = _stream_evidence(
        live_session, table, wait_rows=wait_rows, report=stream_report, evidence=evidence
    )
    now = time.time_ns()
    for row in rows[:3]:
        stamp = int(row["Timestamp"])
        evidence(f"LULD row: Timestamp={instant_text(stamp)} age_s={(now - stamp) / 1e9:.1f} {row}")
    if not rows:
        pytest.skip(f"no LULD frame for {_SYMBOL} arrived (run at the 09:30 New York open)")
    assert abs(now - int(rows[0]["Timestamp"])) < _DAY_NS


@pytest.mark.timeout(100)
def test_filings_rows_without_a_ticker(
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How many rows of the four filings datasets lack `ticker`.

    Reads March 2026 of each (up to 5,000 rows, flagged when cut short) and records how many
    rows have no `Symbol`. Reading it: any count is the answer; the library keeps such rows
    with a null `Symbol` either way.
    """
    not_on_basic("stocks")
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


@pytest.mark.timeout(100)
def test_stock_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
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
    not_on_basic("stocks")
    lines = week_bar_report("stocksTicker", _SYMBOL)
    for line in lines:
        evidence(line)
    kept = live_session.stocks.bars(_SYMBOL, dm.Timespan.WEEK).history(
        date(2026, 3, 2), date(2026, 3, 15), source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["Timestamp"]) for row in table_rows(kept)]
    evidence(f"library history 2026-03-02..2026-03-15 keeps the bars stamped {stamps}")
    assert len(lines) > 1


@pytest.mark.timeout(100)
def test_month_bars_from_mid_month_against_the_whole_month(
    not_on_basic: NotOnBasic, *, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether the range returns a partial month bar for a `{from}` inside a month.

    Records `AAPL` month bars from 2026-03-01 and from 2026-03-15. Reading it: a first bar of
    April, or a March bar with the whole month's volume, means no partial bar; a March bar with
    less volume is a partial bar, which the library already avoids by starting at period
    boundaries.
    """
    not_on_basic("stocks")
    whole = _bars(rest_rows, "month", multiplier=1, bounds=(date(2026, 3, 1), date(2026, 6, 30)))
    partial = _bars(rest_rows, "month", multiplier=1, bounds=(date(2026, 3, 15), date(2026, 6, 30)))
    for name, bars in (("from 2026-03-01", whole), ("from 2026-03-15", partial)):
        for bar in bars:
            evidence(
                f"{name}: t={instant_text(bar['t'] * _MS)} o={bar['o']} c={bar['c']} v={bar['v']}"
            )
    assert whole
    assert partial


@pytest.mark.timeout(100)
def test_bars_of_several_units_align_to_boundaries_or_to_from(
    not_on_basic: NotOnBasic, *, rest_rows: RestRows, instant_text: Text, evidence: Evidence
) -> None:
    """Whether bars of several units align to clock and calendar boundaries or to `{from}`.

    Records `AAPL` 5-minute bars from 14:32 UTC and 2-week bars from Wednesday 2026-03-04.
    Reading it: stamps on :30 and :35 and on Mondays mean boundaries, as the library assumes;
    stamps at 14:32 and on Wednesdays mean `{from}`, and the library's bar windows must follow.
    """
    not_on_basic("stocks")
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
