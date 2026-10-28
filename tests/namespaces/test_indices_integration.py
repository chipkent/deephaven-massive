"""Live checks of index data against Massive: the encoded `I:` prefix and the ticker spellings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw responses come from the wire REST
client, because the library rewrites the very spellings in question; the library's own answer
is recorded beside them.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"

_INDEX = "I:SPX"
_DAY = date(2025, 6, 2)
"""A past Monday, for the endpoints that take a date."""

_RAW_REQUESTS: tuple[tuple[str, dict[str, object], dict[str, object]], ...] = (
    ("/v2/aggs/ticker/{indicesTicker}/prev", {"indicesTicker": _INDEX}, {}),
    (
        "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}",
        {
            "indicesTicker": _INDEX,
            "multiplier": 1,
            "timespan": "day",
            "from": _DAY,
            "to": _DAY,
        },
        {},
    ),
    ("/v1/open-close/{indicesTicker}/{date}", {"indicesTicker": _INDEX, "date": _DAY}, {}),
    ("/v3/snapshot/indices", {}, {"ticker.any_of": [_INDEX]}),
    ("/v3/snapshot", {}, {"ticker.any_of": [_INDEX]}),
    (
        "/v1/indicators/sma/{stockTicker}",
        {"stockTicker": _INDEX},
        {"timespan": "day", "window": 10, "limit": 2},
    ),
)
"""One request of each REST endpoint that serves an index, with its path values and query."""

_TICKER_KEYS = ("T", "ticker", "symbol", "underlying")


def _spellings(value: object) -> dict[str, object]:
    """Collect the values of the keys that may carry a ticker in one decoded JSON object.

    Returns:
        A mapping from each such key found to its value.
    """
    if not isinstance(value, dict):
        return {}
    return {key: value[key] for key in _TICKER_KEYS if key in value}


def test_a_percent_encoded_index_ticker_in_a_path_reads_as_the_index(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    rows = table_rows(live_session.indices.bars(_INDEX, dm.Timespan.DAY).fetch())
    evidence(f"library fetch() of /v2/aggs/ticker/I%3ASPX/prev: {rows}")
    assert [row["Symbol"] for row in rows] == [_INDEX]


@pytest.mark.timeout(110)
def test_index_ticker_spelling_on_every_index_endpoint(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    answered = 0
    for path, path_params, query in _RAW_REQUESTS:
        try:
            page = live_rest.get(source(Transport.REST, path), path_params, query)
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
        "indices.values fetch()": live_session.indices.values(_INDEX).fetch,
        "indices.bars(DAY) fetch()": live_session.indices.bars(_INDEX, dm.Timespan.DAY).fetch,
        "indices.snapshots fetch()": live_session.indices.snapshots(_INDEX).fetch,
        "indices.daily_open_close fetch()": live_session.indices.daily_open_close(
            _INDEX, _DAY
        ).fetch,
        "snapshots (unified) fetch()": live_session.snapshots(_INDEX).fetch,
        "indicators.sma fetch()": live_session.indicators.sma(_INDEX, 10, limit=2).fetch,
    }
    for name, call in queries.items():
        try:
            evidence(f"library {name}: Symbol {[r['Symbol'] for r in table_rows(call())]}")
        except dm.MassiveError as error:
            evidence(f"library {name}: {type(error).__name__}: {error}")
    assert answered
