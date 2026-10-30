"""Live checks of reference data against Massive: keys, repeated rows, and ticker spellings.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. The library's tables serve where they
show the answer; raw rows from the wire REST client serve where the library renames or rejects
the very keys or spellings in question. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import (
    RestClient,
    Transport,
    WireEntitlementError,
    WireError,
    source,
)

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

_STOCK_TICKER = re.compile(r"[A-Z]+(?:\.[A-Z]|p[A-Z]?|r|w)?")
"""An upper-case root and at most one of the suffixes `.X` (class), `pX` (preferred), `r`
(rights), and `w` (warrants)."""

_TYPES = ("WARRANT", "UNIT", "RIGHT", "PFD")
_LIMIT = 10_000
"""The most rows read of a listing; a result of exactly this many is cut short."""

_BASIC_LIMIT = 5_000
"""The most rows read of a listing with Stocks on Basic: five pages of 1,000, five requests."""
_PARTNER_PAGES = ("/benzinga/v1/ratings", "/etf-global/v1/constituents", "/tmx/v1/corporate-events")


def _repeated(keys: Iterable[tuple[object, ...]]) -> list[tuple[tuple[object, ...], int]]:
    """Return each key that occurs more than once, with its count.

    Returns:
        The repeated keys and their counts, most frequent first.
    """
    return [(key, count) for key, count in Counter(keys).most_common() if count > 1]


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("24")
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
    into `Type`. Records the raw keys of `META`'s events and the library's `Type`. Reading it:
    `type` with `Type` filled confirms the library; `event_type` means `Type` is null and the
    library must read that key.
    """
    rows = live_rest.get(source(Transport.REST, _EVENTS), {"id": _RENAMED}, {}, bucket="other").rows
    for row in rows:
        evidence(f"raw event keys {sorted(row)}: {row}")
    library = table_rows(live_session.reference.ticker_events(_RENAMED).fetch())
    evidence(f"library ticker_events({_RENAMED}): Type {[r['Type'] for r in library]}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("30")
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
    one row. Reading it: none confirms the key; any means a polled table keeps one of them,
    and the key needs an owner decision.
    """
    limit = _BASIC_LIMIT if "stocks" in classes_on_basic else _LIMIT
    rows = table_rows(
        live_session.reference.tickers(
            active=False, filters={"ticker_gte": "A", "ticker_lt": "AC"}, limit=limit
        ).fetch()
    )
    repeated = _repeated((row["Symbol"],) for row in rows)
    cut = " (cut short at the limit)" if len(rows) == limit else ""
    evidence(
        f"tickers(active=False) A..AC: {len(rows)} rows{cut}, {len(repeated)} repeated tickers"
    )
    for key, count in repeated[:20]:
        evidence(f"    {key[0]} on {count} rows")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("31")
@pytest.mark.requests(1)
def test_exchanges_are_keyed_by_asset_class_and_id(
    live_session: dm.MassiveSession, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Whether `(AssetClass, Id)` identifies one exchange, the key `reference.exchanges` polls by.

    Records every repeated key. Reading it: none confirms the key; any means it needs another
    column.
    """
    rows = table_rows(live_session.reference.exchanges().fetch())
    repeated = _repeated((row["AssetClass"], row["Id"]) for row in rows)
    evidence(f"exchanges: {len(rows)} rows; repeated (AssetClass, Id): {repeated}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("32")
@pytest.mark.requests(5)
def test_conditions_are_keyed_by_asset_class_type_and_id(
    live_session: dm.MassiveSession,
    *,
    classes_on_basic: frozenset[str],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether `(AssetClass, Type, Id)` identifies one condition, the key it polls by.

    Reads up to 10,000 conditions, or 5,000 with Stocks on Basic (five requests at most), and
    records every repeated key. Reading it: none confirms the key; any means it needs another
    column.
    """
    limit = _BASIC_LIMIT if "stocks" in classes_on_basic else _LIMIT
    rows = table_rows(live_session.reference.conditions(limit=limit).fetch())
    repeated = _repeated((row["AssetClass"], row["Type"], row["Id"]) for row in rows)
    cut = " (cut short at the limit)" if len(rows) == limit else ""
    evidence(f"conditions: {len(rows)} rows{cut}; repeated (AssetClass, Type, Id): {repeated}")
    assert rows


@pytest.mark.needs("stocks:basic")
@pytest.mark.settles("stock ticker spellings")
@pytest.mark.requests(4)
@pytest.mark.timeout(110)
def test_stock_ticker_spellings_outside_the_documented_forms(
    live_rest: RestClient, *, classes_on_basic: frozenset[str], evidence: Evidence
) -> None:
    """Which stock ticker spellings fall outside an upper-case root and one documented suffix.

    Reads one page of stock tickers of each type `WARRANT`, `UNIT`, `RIGHT`, and `PFD`, and
    one page of three partner datasets, and records every ticker outside the root-and-suffix
    forms (`.X`, `pX`, `r`, `w`). Reading it: none confirms the symbol grammar; each other
    spelling must join the grammar or be left to the handling of unknown symbols. A refused
    partner page (no entitlement) is recorded and skipped. With Stocks on Basic, which allows
    5 requests a minute, the partner pages are left out, so the test sends 4 requests.
    """
    seen = 0
    for ticker_type in _TYPES:
        rows = live_rest.get(
            source(Transport.REST, _TICKERS),
            {},
            {"market": "stocks", "type": ticker_type, "limit": 1000},
            bucket="other",
        ).rows
        seen += len(rows)
        tickers = sorted({str(row.get("ticker")) for row in rows})
        odd = [ticker for ticker in tickers if not _STOCK_TICKER.fullmatch(ticker)]
        evidence(f"{ticker_type}: {len(tickers)} tickers, e.g. {tickers[:10]}")
        evidence(f"    outside the root-and-suffix forms: {odd}")
    if "stocks" in classes_on_basic:
        evidence("Stocks on Basic: the partner pages are not read")
    for path in () if "stocks" in classes_on_basic else _PARTNER_PAGES:
        try:
            rows = live_rest.get(
                source(Transport.REST, path), {}, {"limit": 100}, bucket="other"
            ).rows
        except WireEntitlementError as error:
            evidence(f"{path}: not entitled: needs the partner's $99/month add-on ({error})")
            continue
        except WireError as error:
            evidence(f"{path}: {type(error).__name__}: {error}")
            continue
        values = sorted(
            {str(v) for row in rows for k, v in row.items() if "ticker" in k and v is not None}
        )
        odd = [value for value in values if not _STOCK_TICKER.fullmatch(value)]
        evidence(f"{path}: {len(values)} ticker values; outside the forms: {odd}")
    assert seen
