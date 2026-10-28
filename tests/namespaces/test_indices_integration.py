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
from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
NotOnBasic: TypeAlias = "Callable[[str], None]"

_INDEX = "I:SPX"
_DAY = date(2026, 6, 1)
"""A past Monday, inside a Basic plan's two years of history, for the endpoints that take a date."""

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
    live_session: dm.MassiveSession,
    *,
    not_on_basic: NotOnBasic,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Massive reads `I%3ASPX` in a path as `I:SPX`.

    The library percent-encodes every path value whole, so `/prev` of `I:SPX` is requested as
    `/v2/aggs/ticker/I%3ASPX/prev`. Passes when the library's `fetch()` returns one row for
    `I:SPX`. A failure (no row, or a 404) means a colon must be sent unencoded.
    """
    not_on_basic("indices")
    rows = table_rows(live_session.indices.bars(_INDEX, dm.Timespan.DAY).fetch())
    evidence(f"library fetch() of /v2/aggs/ticker/I%3ASPX/prev: {rows}")
    assert [row["Symbol"] for row in rows] == [_INDEX]


@pytest.mark.timeout(110)
def test_index_ticker_spelling_on_every_index_endpoint(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    not_on_basic: NotOnBasic,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which index endpoints spell an index with its `I:` prefix and which without.

    Sends one raw request of each REST endpoint that serves `I:SPX` (`/prev`, the range,
    open-close, the indices snapshot, the unified snapshot, an indicator) and records the
    ticker keys of the body and of the first row, then the library's `Symbol` for the same
    datasets. Reading it: an endpoint whose rows say `SPX` is one the library must read as
    `I:SPX`; the library's `Symbol` column shows whether it already does.
    """
    not_on_basic("indices")
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
