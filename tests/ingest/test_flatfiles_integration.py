"""Live checks of Massive's S3 flat files as the library reads them: sessions, units, tickers.

They run only with the S3 credentials (`MASSIVE_S3_ACCESS_KEY_ID` and
`MASSIVE_S3_SECRET_ACCESS_KEY`) as well as `MASSIVE_API_KEY`. Raw rows come from the wire
flat-file client, every value as the file's text, because the library converts and checks the
very values in question. A file the credentials are not entitled to is recorded as refused and
the check goes on. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Transport, WireEntitlementError, WireError, sources

if TYPE_CHECKING:
    from collections import Counter
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
    """Whether `us_indices/values_v1` `timestamp` is in nanoseconds, as the library reads it.

    It is documented in milliseconds while the example rows have 19 digits. Records the first
    rows' raw `timestamp`, its digit count, and the instant it is as nanoseconds. Passes when
    that instant lies within a day of the file's day; a millisecond value lands in 1970.
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
    assert _day_ns(recent_monday) - _DAY_NS <= first < _day_ns(recent_monday) + 2 * _DAY_NS


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
