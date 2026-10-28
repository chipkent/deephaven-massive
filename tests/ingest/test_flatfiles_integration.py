"""Live checks of Massive's S3 flat files: keys, publication, missing files, units, and tickers.

They run only with the S3 credentials (`MASSIVE_S3_ACCESS_KEY_ID` and
`MASSIVE_S3_SECRET_ACCESS_KEY`) as well as `MASSIVE_API_KEY`. Raw rows come from the wire
flat-file client, every value as the file's text, because the library converts and checks the
very values in question; most reads take only the first block of a file.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    FlatFileClient,
    Transport,
    WireEntitlementError,
    object_key,
    sources,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

FlatFileHead: TypeAlias = (
    "Callable[[str, date, str | None], tuple[list[dict[str, str | None]], frozenset[str]] | None]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_DAY = date(2025, 6, 2)
"""A past Monday whose files are long published."""

_WEEKEND = date(2025, 6, 1)
"""The Sunday before it, which has no stock file."""

_VENUES = ("cme", "cbot", "nymex", "comex")
_NEW_YORK = ZoneInfo("America/New_York")
_DAY_FILE = "us_stocks_sip/day_aggs_v1"


def _previous_weekday(day: date) -> date:
    """Return the weekday before `day`.

    Returns:
        The day before, or the Friday before for a Monday or a weekend day.
    """
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


@pytest.mark.timeout(110)
def test_futures_files_hold_only_the_session_of_their_key(
    live_session: dm.MassiveSession,
    *,
    flat_file_head: FlatFileHead,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    read = 0
    for venue in _VENUES:
        result = flat_file_head("us_futures_{venue}/session_aggs_v1", _DAY, venue)
        if result is None:
            evidence(f"{venue}: no session_aggs_v1 file for {_DAY}")
            continue
        rows, extra = result
        read += 1
        sessions = sorted({str(row["session_end_date"]) for row in rows})
        evidence(f"{venue} {_DAY}: {len(rows)} rows read; session_end_date values {sessions}")
        evidence(f"    undocumented headers: {sorted(extra)}")
    try:
        bars = live_session.futures.bars(
            dm.ALL, dm.Timespan.SESSION, venue=dm.FuturesVenue.CME
        ).history(_DAY, _DAY, source=dm.HistorySource.FLAT_FILES)
        evidence(f"library CME session bars of {_DAY}: {len(table_rows(bars))} rows")
    except dm.MassiveError as error:
        evidence(f"library CME session bars of {_DAY}: MassiveError: {error}")
    assert read


def test_yesterdays_file_is_published_by_the_cutoff(
    live_flat_files: FlatFileClient, *, evidence: Evidence
) -> None:
    now = datetime.now(_NEW_YORK)
    day = _previous_weekday(now.date())
    file = live_flat_files.open(object_key(_DAY_FILE, day))
    exists = file is not None
    if file is not None:
        file.close()
    evidence(f"now {now:%Y-%m-%d %H:%M %Z}: {_DAY_FILE} for {day} exists: {exists}")


def test_a_weekend_key_is_missing_not_refused(
    live_flat_files: FlatFileClient, *, evidence: Evidence
) -> None:
    key = object_key(_DAY_FILE, _WEEKEND)
    try:
        file = live_flat_files.open(key)
    except WireEntitlementError as error:
        evidence(f"{key}: refused (HTTP {error.status}): {error}")
        raise
    evidence(f"{key}: {'missing (HTTP 404)' if file is None else 'exists'}")
    if file is not None:
        file.close()
    assert file is None


def test_index_value_timestamps_are_nanoseconds(
    flat_file_head: FlatFileHead, *, instant_text: Text, evidence: Evidence
) -> None:
    result = flat_file_head("us_indices/values_v1", _DAY, None)
    assert result is not None, f"no us_indices/values_v1 file for {_DAY}"
    rows, extra = result
    assert rows, f"the us_indices/values_v1 file for {_DAY} has no rows"
    for row in rows[:3]:
        raw = str(row["timestamp"])
        evidence(
            f"{row['ticker']}: timestamp {raw} ({len(raw)} digits) as ns {instant_text(int(raw))}"
        )
    evidence(f"undocumented headers: {sorted(extra)}")
    first = int(str(rows[0]["timestamp"]))
    assert instant_text(first).startswith(_DAY.isoformat())


@pytest.mark.timeout(110)
def test_ticker_spellings_of_every_flat_file(
    flat_file_head: FlatFileHead, *, evidence: Evidence
) -> None:
    read = 0
    for prefix in sources(Transport.FLAT_FILES):
        venue = "cme" if "{venue}" in prefix else None
        result = flat_file_head(prefix, _DAY, venue)
        if result is None:
            evidence(f"{prefix}: no file for {_DAY}")
            continue
        rows, extra = result
        read += 1
        tickers = list(dict.fromkeys(str(row["ticker"]) for row in rows))
        evidence(f"{prefix}: tickers {tickers[:8]}; undocumented headers {sorted(extra)}")
    assert read
