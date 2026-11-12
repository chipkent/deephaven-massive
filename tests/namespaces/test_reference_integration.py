"""Live checks of reference data against Massive: keys and repeated rows.

Each test records what Massive sent (`evidence`). Most assert only that the requests worked,
so the evidence, not a pass, settles the open question; the condition key test asserts the
answer the library relies on. The library's tables serve where they show the answer; raw rows
from the wire REST client serve where the library renames or rejects the very keys in
question. The asset-class test sends values the library refuses
(`tests/_live/bad_values.py`) and records whether Massive refuses them too. How to run these
tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import (
    RestClient,
    Transport,
    source,
)
from tests._live import bad_values

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import TypeAlias

    from deephaven.table import Table

    import deephaven_massive as dm

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"

_EVENTS = "/vX/reference/tickers/{id}/events"
_TICKERS = "/v3/reference/tickers"
_RENAMED = "META"
"""A ticker with a ticker-change event (FB became META in 2022)."""

_LIMIT = 10_000
"""The most rows read of a listing; a result of exactly this many is cut short."""

_BASIC_LIMIT = 5_000
"""The most rows read of a listing with Stocks on Basic: five pages of 1,000, five requests."""


def _repeated(keys: Iterable[tuple[object, ...]]) -> list[tuple[tuple[object, ...], int]]:
    """Return each key that occurs more than once, with its count.

    Returns:
        The repeated keys and their counts, most frequent first.
    """
    return [(key, count) for key, count in Counter(keys).most_common() if count > 1]


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("24")
@pytest.mark.requests(2)
def test_key_of_the_ticker_event_type(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Which key the ticker events carry their event type in: `type` or `event_type`.

    The schema requires `event_type` while every sample uses `type`; the library reads `type`
    into `Type`. Records the raw keys of `META`'s events and the library's `Type`. Passes when
    every raw event carries `type` and the library fills `Type` on every row; `event_type`
    would leave `Type` null, and the library would have to read that key.
    """
    rows = live_rest.get(source(Transport.REST, _EVENTS), {"id": _RENAMED}, {}, bucket="other").rows
    for row in rows:
        evidence(f"raw event keys {sorted(row)}: {row}")
    library = table_rows(live_session.reference.ticker_events(_RENAMED).fetch(wait=True))
    evidence(f"library ticker_events({_RENAMED}): Type {[r['Type'] for r in library]}")
    assert rows
    assert all("type" in row for row in rows), f"events without `type`: {rows}"
    assert library, f"the library read no event of {_RENAMED}"
    assert all(r["Type"] is not None for r in library), f"a null Type: {library}"


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("30")
@pytest.mark.requests(5)
@pytest.mark.timeout(110)
def test_inactive_tickers_on_several_rows(
    live_session: dm.MassiveSession,
    *,
    classes_on_basic: frozenset[str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether a delisted ticker can appear on several rows of the inactive tickers listing.

    `reference.tickers` polls keyed by `Symbol`. Records inactive tickers from `A` to `AC`
    (up to 10,000 rows, or 5,000 with Stocks on Basic, five requests) that occur on more than
    one row. Passes when no ticker is on several rows, so the key holds; a repeated ticker
    means a polled table keeps one of its rows, and the key needs another column.
    """
    limit = _BASIC_LIMIT if "stocks" in classes_on_basic else _LIMIT
    rows = table_rows(
        live_session.reference.tickers(
            active=False, filters={"ticker_gte": "A", "ticker_lt": "AC"}, limit=limit
        ).fetch(wait=True)
    )
    repeated = _repeated((row["Symbol"],) for row in rows)
    cut = " (cut short at the limit)" if len(rows) == limit else ""
    evidence(
        f"tickers(active=False) A..AC: {len(rows)} rows{cut}, {len(repeated)} repeated tickers"
    )
    for key, count in repeated[:20]:
        evidence(f"    {key[0]} on {count} rows")
    assert rows
    assert not repeated, f"inactive tickers on several rows: {repeated[:20]}"


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("31")
@pytest.mark.requests(1)
def test_exchanges_are_keyed_by_asset_class_and_id(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether `(AssetClass, Id)` identifies one exchange, the key `reference.exchanges` polls by.

    Records every repeated key. Passes when no key repeats; a repeated one means the key
    needs another column.
    """
    rows = table_rows(live_session.reference.exchanges().fetch(wait=True))
    repeated = _repeated((row["AssetClass"], row["Id"]) for row in rows)
    evidence(f"exchanges: {len(rows)} rows; repeated (AssetClass, Id): {repeated}")
    assert rows
    assert not repeated, f"exchanges sharing (AssetClass, Id): {repeated}"


@pytest.mark.needs("stocks:basic")
@pytest.mark.verifies("32")
@pytest.mark.requests(5)
def test_conditions_are_keyed_by_every_column(
    live_session: dm.MassiveSession,
    *,
    classes_on_basic: frozenset[str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether every condition differs from the others in some column, the key it polls by.

    Reads up to 10,000 conditions, or 5,000 with Stocks on Basic (five requests at most). For
    each `(AssetClass, Type, Id)` Massive lists more than once, records the columns whose
    values differ between its rows. Passes when no two rows are equal in every column but
    `TimestampInsert`, so a `poll()` table, keyed by all of them, keeps every condition; a
    failure names the repeated rows.
    """
    limit = _BASIC_LIMIT if "stocks" in classes_on_basic else _LIMIT
    rows = table_rows(live_session.reference.conditions(limit=limit).fetch(wait=True))
    cut = " (cut short at the limit)" if len(rows) == limit else ""
    triples = _repeated((row["AssetClass"], row["Type"], row["Id"]) for row in rows)
    evidence(f"conditions: {len(rows)} rows{cut}; repeated (AssetClass, Type, Id): {triples}")
    for triple, _count in triples:
        same = [row for row in rows if (row["AssetClass"], row["Type"], row["Id"]) == triple]
        differ = sorted(k for k in same[0] if k != "TimestampInsert" and _differs(same, k))
        evidence(f"    {triple}: the rows differ in {differ}")
    whole = _repeated(
        tuple(_hashable(v) for k, v in sorted(row.items()) if k != "TimestampInsert")
        for row in rows
    )
    assert rows
    assert not whole, f"conditions equal in every column: {whole}"


def _differs(rows: list[dict[str, Any]], column: str) -> bool:
    """Tell whether some rows hold different values in a column.

    Returns:
        True when the rows' values of `column` are not all equal.
    """
    return len({_hashable(row[column]) for row in rows}) > 1


def _hashable(value: object) -> object:
    """Return a value that can be counted: a list (`DataTypes`) as a tuple, else the value.

    Returns:
        The value, a list made a tuple.
    """
    return tuple(value) if isinstance(value, list) else value


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("92")
@pytest.mark.requests(3)
def test_massive_answers_an_asset_class_its_endpoint_does_not_list(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """What the reference endpoints answer to an `asset_class` their specification does not list.

    The library refuses one: futures on the conditions and the ticker types, indices on the
    exchanges. DATA counts the rows of the class sent.
    """
    assert bad_values.ask_all(live_rest, bad_values.ASSET_CLASS, evidence)
