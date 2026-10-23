"""Tests for symbol normalization (DESIGN § 10)."""

from typing import TYPE_CHECKING, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass
from deephaven_massive._symbols import SymbolTable, canonical, normalize, underlying, ws_symbol

if TYPE_CHECKING:
    from deephaven_massive._types import Symbols

STK, OPT, FUT, IDX, FX, CRY = (
    AssetClass.STOCKS,
    AssetClass.OPTIONS,
    AssetClass.FUTURES,
    AssetClass.INDICES,
    AssetClass.FOREX,
    AssetClass.CRYPTO,
)


@pytest.mark.parametrize(
    ("asset_class", "raw", "expected"),
    [
        (STK, "AAPL", "AAPL"),
        (STK, " brk.a ", "BRK.A"),
        (OPT, "AAPL250117C00150000", "O:AAPL250117C00150000"),
        (OPT, "O:AAPL250117C00150000", "O:AAPL250117C00150000"),
        (OPT, "o:spxw251219p05000000", "O:SPXW251219P05000000"),
        (FUT, "ESZ5", "ESZ5"),
        (IDX, "I:SPX", "I:SPX"),
        (IDX, "SPX", "I:SPX"),
        (FX, "EUR/USD", "C:EURUSD"),
        (FX, "EUR-USD", "C:EURUSD"),
        (FX, "EURUSD", "C:EURUSD"),
        (FX, "C:EURUSD", "C:EURUSD"),
        (FX, "C:EUR-USD", "C:EURUSD"),
        (CRY, "BTC-USD", "X:BTCUSD"),
        (CRY, "BTC/USD", "X:BTCUSD"),
        (CRY, "X:BTCUSD", "X:BTCUSD"),
        (CRY, "doge-usdt", "X:DOGEUSDT"),
    ],
)
def test_canonical_forms(asset_class: AssetClass, raw: str, expected: str) -> None:
    assert canonical(asset_class, raw) == expected


@pytest.mark.parametrize(
    ("asset_class", "raw"),
    [
        (STK, ""),
        (STK, "AAPL MSFT"),
        (STK, "AAPL,MSFT"),
        (OPT, "AAPL"),
        (OPT, "O:AAPL250117X00150000"),
        (FX, "EURUSDX"),
        (FX, "EUR/USD/JPY"),
        (FX, "EUR/"),
        (CRY, "BTC--USD"),
    ],
)
def test_invalid_symbols(asset_class: AssetClass, raw: str) -> None:
    with pytest.raises(ValueError, match=r"symbol|pair"):
        canonical(asset_class, raw)


def test_non_string_symbol() -> None:
    with pytest.raises(TypeError, match="must be a str"):
        canonical(STK, cast("str", 5))


@pytest.mark.parametrize(
    ("asset_class", "symbol", "expected"),
    [
        (FX, "C:EURUSD", "EUR/USD"),
        (CRY, "X:BTCUSD", "BTC-USD"),
        (CRY, "X:SHIBUSDT", "SHIB-USDT"),
        (CRY, "X:ETHBTC", "ETH-BTC"),
        (OPT, "O:AAPL250117C00150000", "O:AAPL250117C00150000"),
        (IDX, "I:SPX", "I:SPX"),
        (STK, "AAPL", "AAPL"),
        (FUT, "ESZ5", "ESZ5"),
    ],
)
def test_ws_symbol(asset_class: AssetClass, symbol: str, expected: str) -> None:
    assert ws_symbol(asset_class, symbol) == expected


def test_ws_symbol_unknown_crypto_quote() -> None:
    with pytest.raises(ValueError, match="BASE-QUOTE"):
        ws_symbol(CRY, "X:BTCXYZ")


def test_underlying() -> None:
    assert underlying("O:AAPL250117C00150000") == "AAPL"
    assert underlying("O:SPXW251219P05000000") == "SPXW"
    with pytest.raises(ValueError, match="OCC"):
        underlying("AAPL")


def test_normalize_lists_sorts_and_deduplicates() -> None:
    assert normalize(STK, ["msft", "AAPL", "MSFT"]) == ("AAPL", "MSFT")
    assert normalize(STK, "aapl") == ("AAPL",)
    assert normalize(FX, ("EUR/USD", "C:EURUSD")) == ("C:EURUSD",)


def test_normalize_materializes_generators() -> None:
    assert normalize(STK, cast("Symbols", (s for s in ["B", "A"]))) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    symbols = (s for s in ["A"])
    list(symbols)
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, cast("Symbols", symbols))
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, [])


def test_normalize_all_passes_through() -> None:
    assert normalize(STK, dm.ALL) is dm.ALL


def test_normalize_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(TypeError, match="symbols must be"):
        normalize(STK, cast("Symbols", 5))
    with pytest.raises(TypeError, match="must be a str"):
        normalize(STK, cast("Symbols", ["AAPL", 5]))


def test_normalize_tables(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["AAPL"])])
    boxed = normalize(STK, table)
    assert isinstance(boxed, SymbolTable)
    assert boxed.table is table
    # Boxes compare by identity, unlike Deephaven wrappers of one Java table.
    again = normalize(STK, table)
    assert boxed != again
    with pytest.raises(ValueError, match="Symbol column"):
        normalize(STK, new_table([string_col("Ticker", ["AAPL"])]))
