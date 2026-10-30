"""Live tests of `m.stocks.filings` against Massive: rows without a ticker, and the insider keys.

They run only when `MASSIVE_API_KEY` is set.
"""

from datetime import date
from typing import TYPE_CHECKING, Any, TypeAlias

import pytest

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table


pytestmark = pytest.mark.integration

TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"

DaySpan: TypeAlias = "tuple[date, date]"

Evidence: TypeAlias = "Callable[[str], None]"

_FILINGS_LIMIT = 5_000
"""The most rows read of each filings dataset; a result of exactly this many is cut short."""

_FILINGS_PAGE = 100
"""The largest page of the 10-K sections and 8-K text, the most of each read with Stocks on
Basic: one request each, where 5,000 rows would take up to 50."""


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("25")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_filings_rows_without_a_ticker(
    live_session: dm.MassiveSession,
    *,
    recent_month: DaySpan,
    classes_on_basic: frozenset[str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """How many rows of the four filings datasets lack `ticker`.

    Reads the most recent whole month of each (up to 5,000 rows, flagged when cut short) and
    records how many rows have no `Symbol`. With Stocks on Basic, the 10-K sections and 8-K
    text, served 100 rows a page, are read one page each, so the test sends 4 requests.
    Reading it: any count is the answer; the library keeps such rows with a null `Symbol`
    either way.
    """
    filings = live_session.stocks.filings
    start, end = recent_month
    paged = _FILINGS_PAGE if "stocks" in classes_on_basic else _FILINGS_LIMIT
    queries = {
        "filings.index": filings.index(start=start, end=end, limit=_FILINGS_LIMIT),
        "filings.sections_10k": filings.sections_10k(start=start, end=end, limit=paged),
        "filings.text_8k": filings.text_8k(start=start, end=end, limit=paged),
        "filings.risk_factors": filings.risk_factors(start=start, end=end, limit=_FILINGS_LIMIT),
    }
    counts = {}
    for name, query in queries.items():
        rows = table_rows(query.fetch())
        lacking = [row for row in rows if row["Symbol"] is None]
        counts[name] = len(rows)
        cut = " (cut short at the limit)" if len(rows) in {_FILINGS_LIMIT, paged} else ""
        evidence(f"{name} {start}..{end}: {len(rows)} rows{cut}, {len(lacking)} without a ticker")
        for row in lacking[:3]:
            evidence(f"    e.g. {row}")
    assert any(counts.values())


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("42")
@pytest.mark.requests(2)
@pytest.mark.timeout(100)
def test_insider_activity_and_holdings_keys_name_one_row_each(
    live_session: dm.MassiveSession,
    *,
    recent_month: DaySpan,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether the `poll()` keys of the insider activity and holdings identify one row each.

    Reads NVDA's insider activity over the twelve months ending with the most recent whole
    month and its current insider holdings, and records the rows and any key shared by two
    different rows: `IssuerCik` and `TransactionDate` for the activity, `IssuerCik` and
    `OwnerCik` for the holdings. Two identical rows under one key lose nothing when `poll()`
    keeps one, so only different rows count. Passes when no key names two different rows,
    the assumption that makes a `poll()` table keep every row Massive sends; a failure names
    the key that needs another column. Data that comes and goes (an empty quarter, a new
    filing) changes the counts, never the verdict.
    """
    start, end = _year_before(recent_month)
    filings = live_session.stocks.filings
    queries = {
        "filings.insider_activity": (
            filings.insider_activity("NVDA", start=start, end=end),
            ("IssuerCik", "TransactionDate"),
        ),
        "filings.insider_holdings": (filings.insider_holdings("NVDA"), ("IssuerCik", "OwnerCik")),
    }
    shared: dict[str, list[tuple[object, ...]]] = {}
    for name, (query, keys) in queries.items():
        rows = table_rows(query.fetch())
        seen: dict[tuple[object, ...], set[str]] = {}
        for row in rows:
            content = repr(sorted((k, v) for k, v in row.items() if k != "TimestampInsert"))
            seen.setdefault(tuple(row[k] for k in keys), set()).add(content)
        shared[name] = [key for key, contents in seen.items() if len(contents) > 1]
        evidence(
            f"{name} NVDA: {len(rows)} rows, {len(seen)} keys, "
            f"{len(shared[name])} keys naming two different rows"
        )
        for key in shared[name][:5]:
            evidence(f"    e.g. {dict(zip(keys, key, strict=True))}")
    assert shared == {name: [] for name in queries}


def _year_before(month: DaySpan) -> DaySpan:
    """Return the twelve whole months ending with a month.

    Returns:
        The first day of the eleventh month before it and its last day.
    """
    first, last = month
    index = first.year * 12 + first.month - 1 - 11
    return date(index // 12, index % 12 + 1, 1), last
