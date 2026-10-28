"""Live checks of options reference data against Massive: underlyings and deliverables.

Each test records what Massive sent (`evidence`) and asserts only that the requests worked:
the evidence, not a pass, settles the open question. Raw rows come from the wire REST client,
because "a key is missing" and "a key holds an empty list" look the same once the library has
read them; the library's own answer is recorded beside them.
"""

from __future__ import annotations

from datetime import date
from functools import partial
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RestClient, Transport, WireError, source

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive._query import Query

pytestmark = pytest.mark.integration

TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"

_CONTRACTS = "/v3/reference/options/contracts"
_CHAIN = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT = "/v3/snapshot"

_UNDERLYINGS = ("SPX", "I:SPX", "SPXW", "NDX", "NDXP", "VIX", "VIXW", "BRK.B", "BRKB")
"""Underlyings whose spelling the options endpoints may not agree on."""

_DELIVERABLE_FIELDS = ("additional_underlyings", "cfi", "correction", "shares_per_contract")
_DELIVERABLE_COLUMNS = ("AdditionalUnderlyings", "Cfi", "Correction", "SharesPerContract")

_ADJUSTED: tuple[tuple[str, str, date], ...] = (
    ("DELL1", "DELL", date(2021, 11, 15)),
    ("FF1", "FF", date(2024, 4, 15)),
    ("AAPL7", "AAPL", date(2014, 6, 10)),
    ("AAPL", "AAPL", date(2025, 6, 2)),
    ("SPX", "SPX", date(2025, 6, 2)),
)
"""(contract root, underlying, a day the contracts were listed): three adjusted roots after a
spin-off, a special dividend, and a split, then a standard equity root and an index root."""

_OCC_TAIL = 15
"""Characters of an OCC symbol after its root: the expiry (6), the type (1), the strike (8)."""


def _root(ticker: str) -> str:
    """Return an OCC option symbol's root, such as `"DELL1"` for `"O:DELL1211119C00050000"`.

    Returns:
        The root.
    """
    return ticker.removeprefix("O:")[:-_OCC_TAIL]


def _raw(
    client: RestClient, path: str, *, params: Mapping[str, object], evidence: Evidence
) -> list[dict[str, Any]]:
    """Read the raw rows of a request's first page; record a refusal as evidence.

    Only the first page is read: a listing such as every contract of AAPL runs to many pages,
    and the first shows the spelling. A refusal (an unknown underlying, a missing entitlement)
    is itself evidence, so it is recorded and the check goes on.

    Returns:
        The rows; empty after a refusal.
    """
    path_params = {"underlyingAsset": params["underlyingAsset"]} if "{" in path else {}
    query = {key: value for key, value in params.items() if key != "underlyingAsset"}
    try:
        return client.get(source(Transport.REST, path), path_params, query).rows
    except WireError as error:
        evidence(f"{path} {dict(params)}: {type(error).__name__}: {error}")
        return []


def _library(build: Callable[[], Query], table_rows: TableRows) -> list[dict[str, Any]] | str:
    """Build one library query, fetch it, and describe the outcome.

    Returns:
        The result's rows, or the text of the error building or fetching raised.
    """
    try:
        return table_rows(build().fetch())
    except (dm.MassiveError, ValueError) as error:
        return f"{type(error).__name__}: {error}"


@pytest.mark.timeout(110)
def test_underlying_spelling_on_the_options_endpoints(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    answered = 0
    options = live_session.options
    for underlying in _UNDERLYINGS:
        contracts = _raw(
            live_rest,
            _CONTRACTS,
            params={"underlying_ticker": underlying, "limit": 3},
            evidence=evidence,
        )
        chain = _raw(
            live_rest,
            _CHAIN,
            params={"underlyingAsset": underlying, "limit": 3},
            evidence=evidence,
        )
        answered += bool(contracts) + bool(chain)
        pairs = [(r.get("ticker"), r.get("underlying_ticker")) for r in contracts]
        evidence(f"{underlying}: contracts (ticker, underlying_ticker) {pairs}")
        chained_raw = [
            (r.get("details", {}).get("ticker"), r.get("underlying_asset", {}).get("ticker"))
            for r in chain
        ]
        evidence(f"{underlying}: chain (details.ticker, underlying_asset.ticker) {chained_raw}")
        if contracts:
            unified = _raw(
                live_rest,
                _SNAPSHOT,
                params={"ticker.any_of": [contracts[0]["ticker"]]},
                evidence=evidence,
            )
            spelled = [r.get("underlying_asset", {}).get("ticker") for r in unified]
            evidence(
                f"{underlying}: unified snapshot of {contracts[0]['ticker']}: "
                f"underlying_asset.ticker {spelled}"
            )
        chained = _library(partial(options.chain, underlying, limit=3), table_rows)
        listed = _library(partial(options.contracts, underlying=underlying, limit=3), table_rows)
        for name, outcome in (("chain", chained), ("contracts", listed)):
            shown = (
                outcome
                if isinstance(outcome, str)
                else [
                    {k: r.get(k) for k in ("Symbol", "Underlying", "OptionRoot")} for r in outcome
                ]
            )
            evidence(f"{underlying}: library {name}: {shown}")
    assert answered


@pytest.mark.timeout(110)
def test_deliverable_fields_of_adjusted_and_standard_contracts(
    live_session: dm.MassiveSession,
    *,
    live_rest: RestClient,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    found = 0
    for root, underlying, listed in _ADJUSTED:
        matching = [
            row
            for expired in (False, True)
            for lookup in (underlying, root)
            for row in _raw(
                live_rest,
                _CONTRACTS,
                params={
                    "underlying_ticker": lookup,
                    "as_of": listed,
                    "expired": expired,
                    "limit": 1000,
                },
                evidence=evidence,
            )
            if _root(str(row.get("ticker", ""))) == root
        ]
        rows = list({str(row["ticker"]): row for row in matching}.values())
        found += bool(rows)
        evidence(f"{root} (underlying {underlying}, as of {listed}): {len(rows)} contracts")
        for row in rows[:3]:
            present = {key: row.get(key, "<missing>") for key in _DELIVERABLE_FIELDS}
            evidence(f"    {row.get('ticker')} underlying={row.get('underlying_ticker')} {present}")
        if rows:
            ticker = str(rows[0]["ticker"])
            outcome = _library(
                partial(live_session.options.contracts, symbols=ticker, as_of=listed), table_rows
            )
            shown = (
                outcome
                if isinstance(outcome, str)
                else [{k: r.get(k) for k in _DELIVERABLE_COLUMNS} for r in outcome]
            )
            evidence(f"    library contracts({ticker}): {shown}")
    assert found
