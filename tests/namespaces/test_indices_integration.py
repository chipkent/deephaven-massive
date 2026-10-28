"""Live checks of index data against Massive: the encoded `I:` prefix and the ticker spellings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw responses come from the wire REST
client, because the library rewrites the very spellings in question; the library's own answer
is recorded beside them. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
HalfDayReport: TypeAlias = "Callable[[str, str], list[str]]"

_DAY = date(2026, 6, 1)
"""A past Monday, inside a Basic plan's two years of history, for the endpoints that take a date."""

_SNAPSHOTS_PLAN = "Indices Starter ($49/month)"
"""The plan of the indices snapshot and the unified snapshot of an index."""


def _raw_requests(index: str) -> tuple[tuple[str, dict[str, object], dict[str, object]], ...]:
    """Return one request of each REST endpoint that serves an index.

    Returns:
        Each request's path template, path values, and query.
    """
    return (
        ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": index}, {}),
        (
            "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}",
            {"indicesTicker": index, "multiplier": 1, "timespan": "day", "from": _DAY, "to": _DAY},
            {},
        ),
        ("/v1/open-close/{indicesTicker}/{date}", {"indicesTicker": index, "date": _DAY}, {}),
        ("/v3/snapshot/indices", {}, {"ticker.any_of": [index]}),
        ("/v3/snapshot", {}, {"ticker.any_of": [index]}),
        (
            "/v1/indicators/sma/{stockTicker}",
            {"stockTicker": index},
            {"timespan": "day", "window": 10, "limit": 2},
        ),
    )


_TICKER_KEYS = ("T", "ticker", "symbol", "underlying")


def _spellings(value: object) -> dict[str, object]:
    """Collect the values of the keys that may carry a ticker in one decoded JSON object.

    Returns:
        A mapping from each such key found to its value.
    """
    if not isinstance(value, dict):
        return {}
    return {key: value[key] for key in _TICKER_KEYS if key in value}


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("26")
@pytest.mark.requests(2)
def test_a_percent_encoded_index_ticker_in_a_path_reads_as_the_index(
    live_session: dm.MassiveSession,
    *,
    index_symbol: str,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive reads a percent-encoded colon in a path (`I%3ASPX`) as `I:SPX`.

    The library percent-encodes every path value whole, so `/prev` of an index is requested
    as `/v2/aggs/ticker/I%3A<index>/prev`. Uses `I:SPX`, or `I:NDX` when Indices is on Basic,
    which excludes SPX. Passes when the library's `fetch()` returns one row for the index. A
    failure (no row, or a 404) means a colon must be sent unencoded.
    """
    rows = table_rows(live_session.indices.bars(index_symbol, dm.Timespan.DAY).fetch())
    evidence(f"library fetch() of /prev for {index_symbol} (sent encoded): {rows}")
    assert [row["Symbol"] for row in rows] == [index_symbol]


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("index ticker spelling")
@pytest.mark.requests(14)
@pytest.mark.timeout(110)
def test_index_ticker_spelling_on_every_index_endpoint(
    live_session: dm.MassiveSession,
    *,
    index_symbol: str,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which index endpoints spell an index with its `I:` prefix and which without.

    Sends one raw request of each REST endpoint that serves the index (`I:SPX`, or `I:NDX` on
    Indices Basic): `/prev`, the range, open-close, the indices snapshot, the unified
    snapshot, and an indicator. Records the ticker keys of the body and of the first row,
    then the library's `Symbol` for the same datasets. The two snapshots need Indices
    Starter; on Basic their lines say "not entitled" and the rest still settle. Reading it: an
    endpoint whose rows drop `I:` is one the library must read back with the prefix; the
    library's `Symbol` column shows whether it already does.
    """
    answered = 0
    for path, path_params, query in _raw_requests(index_symbol):
        try:
            page = live_rest.get(source(Transport.REST, path), path_params, query, bucket="indices")
        except WireEntitlementError as error:
            evidence(f"{path}: not entitled: needs {_SNAPSHOTS_PLAN} ({error})")
            continue
        except WireError as error:
            evidence(f"{path}: {type(error).__name__}: {error}")
            continue
        answered += 1
        body = page.body if isinstance(page.body, dict) else {}
        results = body.get("results")
        first = page.rows[0] if page.rows else None
        evidence(
            f"{path}: body {_spellings(body)}; results {_spellings(results)}; "
            f"first row {_spellings(first)}"
        )
        if page.rows:
            evidence(f"    first row keys and values: {page.rows[0]}")
    queries = {
        "indices.values fetch()": live_session.indices.values(index_symbol).fetch,
        "indices.bars(DAY) fetch()": live_session.indices.bars(index_symbol, dm.Timespan.DAY).fetch,
        "indices.snapshots fetch()": live_session.indices.snapshots(index_symbol).fetch,
        "indices.daily_open_close fetch()": live_session.indices.daily_open_close(
            index_symbol, _DAY
        ).fetch,
        "snapshots (unified) fetch()": live_session.snapshots(index_symbol).fetch,
        "indicators.sma fetch()": live_session.indicators.sma(index_symbol, 10, limit=2).fetch,
    }
    for name, call in queries.items():
        try:
            evidence(f"library {name}: Symbol {[r['Symbol'] for r in table_rows(call())]}")
        except dm.EntitlementError as error:
            evidence(f"library {name}: not entitled: needs {_SNAPSHOTS_PLAN} ({error})")
        except dm.MassiveError as error:
            evidence(f"library {name}: {type(error).__name__}: {error}")
    assert answered


@pytest.mark.needs("indices:basic")
@pytest.mark.settles("half-day stamps")
@pytest.mark.requests(4)
@pytest.mark.timeout(100)
def test_half_day_index_bars_and_their_stamps(
    index_symbol: str, *, half_day_report: HalfDayReport, evidence: Evidence
) -> None:
    """How REST stamps an index's bars on a half trading day (13:00 New York close).

    For 2025-11-28 and 2025-12-24, records the index's range day bar and its first and last
    minute bars (`I:SPX`, or `I:NDX` on Indices Basic), each in UTC and New York time. Reading
    it: an index computed until 13:00 or past it shows whether the index day ends with the
    stock half day; the day bar's stamp shows the library's day label holds.
    """
    lines = half_day_report("indicesTicker", index_symbol)
    for line in lines:
        evidence(line)
    assert lines
