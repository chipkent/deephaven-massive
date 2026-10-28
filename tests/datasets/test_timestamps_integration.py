"""Live checks of the unit of the REST "last" timestamps, which the library reads as milliseconds.

Each test records the raw value Massive sent and the instant the library made of it
(`evidence`). A wrong unit lands decades away or fails the library's [1990, 2100] bound, so an
instant within a few days of now (forex is closed at weekends) settles the unit. How to run
these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import TypeAlias

    from deephaven.table import Table

    import deephaven_massive as dm

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
RestRows: TypeAlias = (
    "Callable[[str, Mapping[str, object], Mapping[str, object]], list[dict[str, Any]]]"
)
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Text: TypeAlias = "Callable[[int], str]"
Evidence: TypeAlias = "Callable[[str], None]"

_RECENT_NS = 4 * 86_400 * 1_000_000_000
"""How far from now a last quote or trade may lie: a forex weekend and a holiday."""


def _check(
    name: str, rows: list[dict[str, Any]], *, column: str, text: Text, evidence: Evidence
) -> None:
    """Record a library result's instant and its age, and require it to be recent."""
    assert rows, f"{name}: no row"
    stamp = int(rows[0][column])
    age = (time.time_ns() - stamp) / 1e9
    evidence(f"{name}: {column}={text(stamp)} age_s={age:.0f} row={rows[0]}")
    assert abs(age) * 1e9 < _RECENT_NS


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("2")
def test_forex_last_quote_timestamp_is_milliseconds(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the library's milliseconds fit the timestamp of `/v1/last_quote/currencies/EUR/USD`.

    Records the raw value (13 digits for milliseconds) and the instant the library's
    `forex.quotes(...).fetch()` makes of it. Passes when that instant is within four days of now (a
    forex weekend and a holiday); a wrong unit lands decades away or fails the library's
    bound.
    """
    raw = rest_rows("/v1/last_quote/currencies/{from}/{to}", {"from": "EUR", "to": "USD"}, {})
    evidence(f"raw /v1/last_quote/currencies/EUR/USD: {raw}")
    rows = table_rows(live_session.forex.quotes("C:EURUSD").fetch())
    _check("forex.quotes fetch()", rows, column="Timestamp", text=instant_text, evidence=evidence)


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("2")
def test_crypto_last_trade_timestamp_is_milliseconds(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the library's milliseconds fit the timestamp of `/v1/last/crypto/BTC/USD`.

    Records the raw value (13 digits for milliseconds) and the instant the library's
    `crypto.trades(...).fetch()` makes of it. Passes when that instant is within four days of now (a
    forex weekend and a holiday); a wrong unit lands decades away or fails the library's
    bound.
    """
    raw = rest_rows("/v1/last/crypto/{from}/{to}", {"from": "BTC", "to": "USD"}, {})
    evidence(f"raw /v1/last/crypto/BTC/USD: {raw}")
    rows = table_rows(live_session.crypto.trades("X:BTCUSD").fetch())
    _check("crypto.trades fetch()", rows, column="Timestamp", text=instant_text, evidence=evidence)


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("2")
def test_forex_conversion_last_timestamp_is_milliseconds(
    live_session: dm.MassiveSession,
    *,
    rest_rows: RestRows,
    table_rows: TableRows,
    instant_text: Text,
    evidence: Evidence,
) -> None:
    """Whether the library's milliseconds fit `last.timestamp` of `/v1/conversion/EUR/USD`.

    Records the raw value (13 digits for milliseconds) and the instant the library's
    `forex.conversion(...).fetch()` makes of it. Passes when that instant is within four days
    of now (a forex weekend and a holiday); a wrong unit lands decades away or fails the
    library's bound.
    """
    raw = rest_rows("/v1/conversion/{from}/{to}", {"from": "EUR", "to": "USD"}, {})
    evidence(f"raw /v1/conversion/EUR/USD: {raw}")
    rows = table_rows(live_session.forex.conversion("EUR", "USD").fetch())
    column = "Timestamp" if rows and "Timestamp" in rows[0] else "LastTimestamp"
    _check("forex.conversion fetch()", rows, column=column, text=instant_text, evidence=evidence)
