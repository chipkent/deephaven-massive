"""Live checks of forex data against Massive: day-bar stamps and cuts, weeks, pairs, and topics.

Each test records what Massive sent (`evidence`). A test marked `settles` asserts only that the
requests worked, and its evidence, not a pass, answers the question; a test marked `verifies` also
asserts the answer the library assumes, and fails when Massive no longer gives it. Raw rows come
from the wire REST and flat-file clients, because the library moves the very stamps in question; the
library's own answer to the same request is recorded beside them. How to run these tests is in
`docs/live-tests.md`.

The library's rule under test: a forex 1-day bar stamped at 00:00 UTC or at its UTC day's last
millisecond is moved to 00:00 UTC of that day, whatever the source, and any other stamp fails
the call with `MassiveError`.
"""

from __future__ import annotations

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
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_PAIR = "C:EURUSD"
_RANGE = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_PREV = "/v2/aggs/ticker/{forexTicker}/prev"
_GROUPED = "/v2/aggs/grouped/locale/global/market/fx/{date}"
_DAY_FILE = "global_forex/day_aggs_v1"
_MS = 1_000_000
_NEW_YORK = ZoneInfo("America/New_York")
_TOLERANCE = 1e-6
"""The relative error within which a high, low, or volume read from text equals a JSON number."""

_STREAM_WAIT = 60.0
"""Seconds a stream test waits, in all, for its first frames; forex is closed at weekends."""


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


def _rebuilt(bars: list[dict[str, Any]], start: datetime, end: datetime) -> tuple[Any, ...]:
    """Rebuild one bar from the smaller bars in `[start, end)`.

    Returns:
        The rebuilt open, high, low, close, and volume; empty when no bar falls inside.
    """
    first, last = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    inside = [b for b in bars if first <= b["t"] < last]
    if not inside:
        return ()
    return (
        inside[0]["o"],
        max(b["h"] for b in inside),
        min(b["l"] for b in inside),
        inside[-1]["c"],
        sum(b["v"] for b in inside),
    )


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


@pytest.mark.needs("currencies:basic")
@pytest.mark.settles("37")
@pytest.mark.requests(6)
@pytest.mark.timeout(100)
def test_forex_day_bars_of_every_rest_source_and_their_stamps(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Where `/prev`, the aggregates range, and grouped daily stamp a forex day bar.

    Records the raw `C:EURUSD` day bar of `/prev`, the range bars around that day, and the
    grouped-daily row, each stamp, and which agree, beside the library's answers. Reading it
    together with the hour-bar rebuild: a source that stamps the bar of day D before 00:00 UTC
    of D (such as 21:00 or 22:00 UTC of D-1, the 17:00 New York open of D's trading day) is
    labelled D-1 by the library, a bug to fix by mapping that source's stamp to the day it
    closes.
    """
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
    equal = [instant_text(b["t"] * _MS) for b in ranged if _same(_ohlcv(b), _ohlcv(prev))]
    evidence(
        f"range bars equal to /prev: {equal}; "
        f"grouped equal to /prev: {[_same(_ohlcv(b), _ohlcv(prev)) for b in grouped]}"
    )
    bars = live_session.forex.bars(_PAIR, dm.Timespan.DAY)
    prev_bar = live_session.forex.previous_day_bar(_PAIR)
    evidence(f"library previous_day_bar (/prev): {_library_rows(prev_bar.fetch, table_rows)}")
    evidence(
        "library history(REST): "
        f"{_library_rows(lambda: bars.history(day, day, source=dm.HistorySource.REST), table_rows)}"
    )
    assert ranged
    assert grouped


@pytest.mark.needs("currencies:basic")
@pytest.mark.settles("37")
@pytest.mark.requests(3)
@pytest.mark.timeout(100)
def test_forex_day_bars_rebuilt_from_hour_bars_over_the_utc_and_trading_days(
    *,
    rest_rows: RestRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which day the REST forex day bar covers: the UTC day or the trading day ending 17:00 NY.

    Rebuilds `/prev`'s day D of `C:EURUSD` from hour bars twice, over the UTC day and over the
    trading day from 17:00 New York time on D-1 to 17:00 on D, and records both beside `/prev`
    and the range bars. Reading it: `/prev` equal to the UTC rebuild confirms the library's UTC
    day, whatever the stamp; equal to the trading-day rebuild means forex day bars are trading
    days, and the library's labelling and week windows need that cut; equal to neither means
    another cut.
    """
    prevs = rest_rows(_PREV, {"forexTicker": _PAIR}, {})
    assert len(prevs) == 1, f"/prev returned {prevs}"
    (prev,) = prevs
    day = _utc_day(int(prev["t"]))
    utc_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    before = day - timedelta(days=1)
    trading_start = datetime(before.year, before.month, before.day, 17, tzinfo=_NEW_YORK)
    trading_end = datetime(day.year, day.month, day.day, 17, tzinfo=_NEW_YORK)
    hours = rest_rows(
        _RANGE,
        {
            "forexTicker": _PAIR,
            "multiplier": 1,
            "timespan": "hour",
            "from": day - timedelta(days=2),
            "to": day + timedelta(days=1),
        },
        {"sort": "asc", "limit": 50_000},
    )
    assert hours, f"no hour bars of {_PAIR} around {day}"
    by_utc = _rebuilt(hours, utc_start, utc_start + timedelta(days=1))
    by_trading = _rebuilt(hours, trading_start, trading_end)
    evidence(f"/prev t={instant_text(prev['t'] * _MS)} ohlcv={_ohlcv(prev)}")
    evidence(f"rebuilt over the UTC day {day}: {by_utc}")
    evidence(f"rebuilt over {trading_start} to {trading_end}: {by_trading}")
    evidence(
        f"/prev equals the UTC rebuild: {_same(by_utc, _ohlcv(prev))} "
        f"(errors {_errors(by_utc, _ohlcv(prev)) if by_utc else None}); "
        f"equals the trading-day rebuild: {_same(by_trading, _ohlcv(prev))} "
        f"(errors {_errors(by_trading, _ohlcv(prev)) if by_trading else None})"
    )


@pytest.mark.needs("currencies:starter", "s3")
@pytest.mark.settles("37")
@pytest.mark.timeout(100)
def test_forex_day_bar_of_the_flat_file_and_its_stamp(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether `global_forex/day_aggs_v1`'s row of a day is the REST bar of the same day.

    Records the file's `C:EURUSD` row of the most recent published Monday with its `window_start`,
    and, for each REST range and grouped bar around that day, whether it is the same bar (open and
    close equal, the rest within 1e-6) and each field's error. Reading it: the file and REST bars of
    the same day being one bar, whatever their stamps, means the library's 00:00 UTC label is right
    if each stamp lies in that UTC day; a file row matching another day's REST bar means the sources
    cut or label days differently.
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
            "forexTicker": _PAIR,
            "multiplier": 1,
            "timespan": "day",
            "from": recent_monday - timedelta(days=1),
            "to": recent_monday + timedelta(days=1),
        },
        {"sort": "asc"},
    )
    grouped = [r for r in rest_rows(_GROUPED, {"date": recent_monday}, {}) if r.get("T") == _PAIR]
    for row in rows:
        values = tuple(
            None if row[k] is None else float(str(row[k]))
            for k in ("open", "high", "low", "close", "volume")
        )
        for name, bar in [*(("range", b) for b in ranged), *(("grouped", b) for b in grouped)]:
            evidence(
                f"file row vs {name} bar {instant_text(bar['t'] * _MS)}: "
                f"same={_same(values, _ohlcv(bar))} "
                f"relative errors (o, h, l, c, v) {_errors(values, _ohlcv(bar))}"
            )
    flat = live_session.forex.bars(_PAIR, dm.Timespan.DAY)
    outcome = _library_rows(
        lambda: flat.history(recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES),
        table_rows,
    )
    evidence(f"library history(FLAT_FILES): {outcome}")
    assert rows


@pytest.mark.needs("currencies:starter", "s3")
@pytest.mark.settles("39")
@pytest.mark.timeout(100)
def test_forex_grouped_daily_and_day_flat_file_hold_the_same_pairs(
    *,
    recent_monday: date,
    rest_rows: RestRows,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Whether grouped daily and `global_forex/day_aggs_v1` list the same pairs on a day.

    Records the pairs only one of them holds on the most recent published Monday. Reading it: equal
    sets mean `dm.ALL` forex day bars hold the same pairs from either source; otherwise the
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


@pytest.mark.needs("currencies:starter", "s3")
@pytest.mark.settles("64")
@pytest.mark.timeout(100)
def test_forex_flat_files_of_a_weekend(
    *,
    recent_monday: date,
    flat_file_tickers: FlatFileTickers,
    evidence: Evidence,
) -> None:
    """Whether Massive publishes forex day files for a Saturday and a Sunday.

    Reads `global_forex/day_aggs_v1` for the Saturday and Sunday before the most recent
    published Monday and records, for each, whether the file exists and how many pairs it
    holds. Reading it: the library takes a forex Saturday for a day without trading, so a
    missing Saturday file is skipped silently and a Sunday's is reported as a possible holiday;
    a Saturday file with rows, or no Sunday file at all, means that rule must change.
    """
    for day in (recent_monday - timedelta(days=2), recent_monday - timedelta(days=1)):
        read = flat_file_tickers(_DAY_FILE, day, None)
        pairs = None if read is None else len(read[0])
        evidence(f"{_DAY_FILE} {day} ({day:%A}): {'no file' if read is None else f'{pairs} rows'}")


@pytest.mark.needs("currencies:basic")
@pytest.mark.verifies("33")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_forex_week_bars_start_on_monday_or_sunday(
    live_session: dm.MassiveSession,
    *,
    recent_week: tuple[date, date],
    week_bar_report: Callable[[str, str, str], tuple[list[str], int, list[str]]],
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether a forex week bar covers the calendar week from Sunday 00:00 UTC.

    For each raw week bar of `C:EURUSD` over the four weeks ending with the most recent week without
    a holiday, records its stamp and the candidate windows whose hour-bar volume is closest to its
    own; `MATCH` marks the bucket. Decides by comparison, not by an absolute tolerance: passes when,
    for every bar of those weeks, the week from Sunday 00:00 UTC, where Massive stamps it and the
    library starts its forex week, is the closest window, ties allowed (forex does not trade from
    Friday evening to Sunday evening UTC, so every start in that gap ties); a failure names the
    bars whose closest window starts elsewhere, and the library's forex week windows must start
    there. The last line shows which bars the library's `history()` keeps.
    """
    lines, checked, misses = week_bar_report("forexTicker", _PAIR, "UTC")
    for line in lines:
        evidence(line)
    first, last = recent_week[0] - timedelta(days=7), recent_week[1]
    kept = live_session.forex.bars(_PAIR, dm.Timespan.WEEK).history(
        first, last, source=dm.HistorySource.REST
    )
    stamps = [instant_text(row["TimestampStart"]) for row in table_rows(kept)]
    evidence(f"library history {first}..{last} keeps the bars stamped {stamps}")
    assert checked >= 3, f"only {checked} week bars of the four weeks"
    assert not misses, f"week bars closer to another window than the library's: {misses}"


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("27")
@pytest.mark.timeout(160)
def test_forex_pair_topics_spelled_with_a_dash_are_acknowledged_and_deliver(
    live_session: dm.MassiveSession,
    *,
    wait_rows: WaitRows,
    wait_acked: WaitAcked,
    market_status: Callable[[str], str],
    stream_report: Callable[[dm.MassiveSession], list[str]],
    evidence: Evidence,
) -> None:
    """Whether `C.EUR-USD` and `CA.EUR-USD` are acknowledged and deliver frames.

    Streams quotes and minute bars of `C:EURUSD` (one connection, two topics) and asserts that
    Massive acknowledged both, whatever the hour. When `/v1/marketstatus/now` says the forex
    market is open it also waits up to 60 s for quotes and asserts they arrived with `Symbol`
    `C:EURUSD`; when it says closed the test skips after the acknowledgement. The session's
    diagnostics are recorded before the release. Reading it: acknowledged topics and rows
    confirm the dash spelling; a refusal in the stream's error means topics must use another
    spelling.
    """
    quotes = live_session.forex.quotes(_PAIR).append_only()
    bars = live_session.forex.bars(_PAIR).append_only()
    acked = wait_acked(live_session, 2, time.monotonic() + _STREAM_WAIT)
    status = market_status("fx")
    deadline = time.monotonic() + _STREAM_WAIT
    quote_rows = wait_rows(live_session, quotes, deadline) if status != "closed" else []
    bar_rows = wait_rows(live_session, bars, deadline) if quote_rows else []
    for line in stream_report(live_session):
        evidence(line)
    live_session.release(quotes)
    live_session.release(bars)
    evidence(f"C quotes received: {len(quote_rows)}; first: {quote_rows[:1]}")
    evidence(f"CA minute bars received: {len(bar_rows)}; first: {bar_rows[:1]}")
    assert acked >= 2, f"Massive acknowledged {acked} of the 2 topics"
    if status == "closed":
        pytest.skip("the forex market is closed (per /v1/marketstatus/now)")
    assert quote_rows, f"no forex quote arrived in {_STREAM_WAIT:.0f} s of an open market"
    assert {row["Symbol"] for row in quote_rows} == {_PAIR}
