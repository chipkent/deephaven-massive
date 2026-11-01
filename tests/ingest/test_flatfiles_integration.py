"""Live checks of Massive's S3 flat files as the library reads them: sessions, units, tickers.

They run only with the S3 credentials (`MASSIVE_S3_ACCESS_KEY_ID` and
`MASSIVE_S3_SECRET_ACCESS_KEY`) as well as `MASSIVE_API_KEY`. Raw rows come from the wire
flat-file client, every value as the file's text, because the library converts and checks the
very values in question. A file the credentials are not entitled to is recorded as refused and
the check goes on. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.timestamps import MAGNITUDE_NS_THRESHOLD
from deephaven_massive._massive import Transport, WireEntitlementError, WireError, sources
from tests._live import calendar

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
FlatFileHead: TypeAlias = (
    "Callable[[str, date, str | None], tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
FlatFileSessions: TypeAlias = "Callable[[str, date, str], Counter[str | None] | None]"
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_VENUES = ("cme", "cbot", "nymex", "comex")
_COMPARED_LAG = timedelta(minutes=16)
"""How far before the current minute the AUTO and REST comparison stops: one minute, so no
bar is still forming, and the 15 minutes a delayed plan's REST bars lag."""
_DAY_NS = 86_400 * 1_000_000_000


@pytest.mark.needs("futures:starter", "s3")
@pytest.mark.settles("4")
@pytest.mark.timeout(100)
def test_futures_files_hold_only_the_session_of_their_key(
    live_session: dm.MassiveSession,
    *,
    recent_monday: date,
    flat_file_sessions: FlatFileSessions,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether every row of a futures file belongs to the session its key names.

    The library checks each block of a futures file against the key's session and fails the
    read otherwise. Counts the `session_end_date` values of the whole `session_aggs_v1` file
    of the most recent published Monday for each venue, and of the whole CME `minute_aggs_v1` file
    (a Monday file, which holds the Sunday-evening rows), then reads the CME session bars through
    the library. Reading it: only that Monday everywhere confirms the key; any other date means the
    files are not keyed by session and the library's check would fail real reads.

    Raises:
        WireEntitlementError: When a file is refused although Futures Starter and S3 are
            declared; the test fails.
        EntitlementError: When the library's read is refused, likewise.
    """
    read = 0
    files = [("us_futures_{venue}/session_aggs_v1", venue) for venue in _VENUES]
    files.append(("us_futures_{venue}/minute_aggs_v1", "cme"))
    for prefix, venue in files:
        try:
            counts = flat_file_sessions(prefix, recent_monday, venue)
        except WireEntitlementError:
            raise
        except WireError as error:
            evidence(f"{prefix} {venue}: {type(error).__name__}: {error}")
            continue
        if counts is None:
            evidence(f"{prefix} {venue}: no file for {recent_monday}")
            continue
        read += 1
        evidence(f"{prefix} {venue} {recent_monday}: session_end_date counts {dict(counts)}")
    try:
        bars = live_session.futures.bars(
            dm.ALL, dm.Timespan.SESSION, venues={dm.FuturesVenue.CME}
        ).history(recent_monday, recent_monday, source=dm.HistorySource.FLAT_FILES)
        evidence(f"library CME session bars of {recent_monday}: {len(table_rows(bars))} rows")
    except dm.EntitlementError:
        raise
    except dm.MassiveError as error:
        evidence(f"library CME session bars of {recent_monday}: {type(error).__name__}: {error}")
    assert read


@pytest.mark.needs("indices:starter", "s3")
@pytest.mark.verifies("22")
def test_index_value_timestamps_are_nanoseconds(
    flat_file_head: FlatFileHead,
    *,
    recent_monday: date,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Which unit `us_indices/values_v1` `timestamp` is in, and that the library reads it.

    It is documented in milliseconds while the example rows have 19 digits, so the library
    reads each value by its magnitude (at least 10**17 is nanoseconds, less is milliseconds).
    Records the first rows' raw `timestamp`, its digit count, and the instant the magnitude
    rule makes of it. Passes when that instant lies within a day of the file's day; the digit
    count settles the unit, after which the field can take the fixed unit.
    """
    result = flat_file_head("us_indices/values_v1", recent_monday, None)
    assert result is not None, f"no us_indices/values_v1 file for {recent_monday}"
    rows, extra = result
    assert rows, f"the us_indices/values_v1 file for {recent_monday} has no rows"
    for row in rows[:3]:
        raw = str(row["timestamp"])
        evidence(
            f"{row['ticker']}: timestamp {raw} ({len(raw)} digits) as ns {instant_text(int(raw))}"
        )
    evidence(f"undocumented headers: {sorted(extra)}")
    first = int(str(rows[0]["timestamp"]))
    nanos = first if first >= MAGNITUDE_NS_THRESHOLD else first * 10**6
    assert _day_ns(recent_monday) - _DAY_NS <= nanos < _day_ns(recent_monday) + 2 * _DAY_NS


def _day_ns(day: date) -> int:
    """Return 00:00 UTC of a day in epoch nanoseconds.

    Returns:
        The nanoseconds.
    """
    return (day - date(1970, 1, 1)).days * _DAY_NS


@pytest.mark.needs("s3")
@pytest.mark.settles("28")
@pytest.mark.timeout(100)
def test_ticker_spellings_of_every_flat_file(
    flat_file_head: FlatFileHead,
    *,
    recent_monday: date,
    flat_files_reached: Callable[[str], bool],
    evidence: Evidence,
) -> None:
    """How each flat file spells its tickers, beside REST's canonical forms.

    Reads the first block of every flat-file prefix for the most recent published Monday (futures
    from `cme`) and records the first tickers of each, any header the generated schema lacks, and
    each prefix the credentials are refused. Reading it: every file spelling tickers as REST does
    (`C:EURUSD`, `X:BTCUSD`, `I:SPX`, `O:...`, `ESH6`) confirms that symbol filters match file rows;
    a file spelling them otherwise (the forex quotes sample shows `X:EUR-USD`) needs that spelling
    among the symbol's wire spellings.
    """
    read = 0
    for prefix in sources(Transport.FLAT_FILES):
        venue = "cme" if "{venue}" in prefix else None
        if not flat_files_reached(prefix):
            evidence(f"{prefix}: the S3 credentials do not reach it")
            continue
        try:
            result = flat_file_head(prefix, recent_monday, venue)
        except WireEntitlementError as error:
            evidence(f"{prefix}: not entitled: needs that class's flat files ({error})")
            continue
        except WireError as error:
            evidence(f"{prefix}: {type(error).__name__}: {error}")
            continue
        if result is None:
            evidence(f"{prefix}: no file for {recent_monday}")
            continue
        rows, extra = result
        read += 1
        tickers = list(dict.fromkeys(str(row["ticker"]) for row in rows))
        evidence(f"{prefix}: tickers {tickers[:8]}; undocumented headers {sorted(extra)}")
    assert read


@pytest.mark.needs("stocks:starter", "s3")
@pytest.mark.timeout(200)
def test_auto_minute_bars_through_today_match_rest(
    live_session: dm.MassiveSession,
    *,
    previous_trading_day: date,
    table_rows: TableRows,
    caplog: pytest.LogCaptureFixture,
    evidence: Evidence,
) -> None:
    """Whether `AUTO` and REST read the same stock minute bars from yesterday through today.

    Today's flat file is published only after the day ends, so `AUTO` reads the published days
    from the files and fills the rest, today at least, from REST, and warns that it did. Reads
    AAPL's minute bars as traded (`adjusted=False`, the files' basis) from the previous trading
    day through today on `AUTO` and on `REST`, and records each source's bars per day and the
    fill warning. Today's bars keep arriving between the two reads, so only the minutes that
    start before a cutoff taken before the first read are compared: the minute before the
    current one, less the delayed feed's 15 minutes, which a delayed plan's REST lags by.
    Passes when both hold the same minutes (`TimestampStart`) per day, and no minute twice:
    no day missing or doubled where the sources meet. Skips on a day that is not a trading
    day, when there is nothing to fill, and when the previous day's file is not published
    yet, when `AUTO` reads REST for both days and the test would compare REST with REST.
    """
    today = calendar.today()
    if not calendar.is_trading_day(today):
        pytest.skip(f"not exercised: {today} is not a trading day, so no day is filled from REST")
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    cutoff = int((now - _COMPARED_LAG).timestamp()) * 1_000_000_000
    query = live_session.stocks.bars("AAPL", dm.Timespan.MINUTE, adjusted=False)
    read: dict[str, list[tuple[str, int]]] = {}
    for chosen in (dm.HistorySource.AUTO, dm.HistorySource.REST):
        table = query.history(previous_trading_day, today, source=chosen)
        rows = table_rows(table.view(["Date", "TimestampStart"]))
        read[chosen.value] = sorted(
            (str(r["Date"]), int(r["TimestampStart"]))
            for r in rows
            if int(r["TimestampStart"]) < cutoff
        )
        days = Counter(day for day, _ in read[chosen.value])
        evidence(f"{chosen.value}: {len(rows)} minute bars; per day {dict(sorted(days.items()))}")
    fills = [r.getMessage() for r in caplog.records if "AUTO read REST" in r.getMessage()]
    evidence(f"fill warning: {fills or 'none'}; compared minutes before {now - _COMPARED_LAG}")
    if any(previous_trading_day.isoformat() in fill for fill in fills):
        pytest.skip(f"not exercised: the {previous_trading_day} file is not published yet")
    auto, rest = read["auto"], read["rest"]
    assert len(set(auto)) == len(auto), "AUTO holds a minute twice"
    assert auto == rest, (
        f"AUTO and REST differ: {len(set(auto) - set(rest))} minutes only in AUTO, "
        f"{len(set(rest) - set(auto))} only in REST"
    )
