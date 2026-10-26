"""Tests for symbol normalization."""

import csv
import io
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _errors
from deephaven_massive._enums import AssetClass
from deephaven_massive._symbols import (
    CRYPTO_QUOTES,
    SymbolTable,
    canonical,
    canonical_view,
    normalize,
    symbol_canonicalizer,
    underlying,
    ws_symbol,
)

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


CANONICAL_FORMS = [
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
]


@pytest.mark.parametrize(("asset_class", "raw", "expected"), CANONICAL_FORMS)
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
        (CRY, "BTC.USD"),
        (CRY, "C:BTCUSD"),
        (IDX, "X:SPX"),
        (STK, "O:AAPL"),
        (FUT, "X:ESZ5"),
        (CRY, "X:X:BTCUSD"),
        (FX, "EU:USD"),
        (CRY, "BTC-XYZ"),
        (CRY, "X:BTCXYZ"),
        (CRY, "USD"),
        (FX, "EURO/USD"),
        (FX, "EU/USD"),
    ],
)
def test_invalid_symbols(asset_class: AssetClass, raw: str) -> None:
    with pytest.raises(ValueError, match=r"symbol|pair|prefix"):
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
    # Boxes of one wrapper are equal; boxes of two wrappers of one Java table are not.
    again = normalize(STK, table)
    assert boxed == again
    assert hash(boxed) == hash(again)
    twin = Table(j_table=table.j_table)
    assert twin == table
    other = normalize(STK, twin)
    assert other != boxed
    with pytest.raises(ValueError, match="Symbol column"):
        normalize(STK, new_table([string_col("Ticker", ["AAPL"])]))


def test_ambiguous_crypto_pairs_are_rejected() -> None:
    # "ABC-USDT" joins to X:ABCUSDT, which splits back as ABC-USDT; but a base ending in a
    # quote currency written with a shorter quote would split differently.
    assert canonical(CRY, "ABC-USDT") == "X:ABCUSDT"
    with pytest.raises(ValueError, match="does not split back unambiguously"):
        canonical(CRY, "ABCUS-DT")


def test_crypto_quotes_cover_the_vendored_tickers(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # The fixture is assembled from well-known pairs, not from a live listing (no API key was
    # available). Regenerate it from /v3/reference/tickers?market=crypto with a live key, and
    # widen CRYPTO_QUOTES if this test then fails.
    text = load_fixture("symbols", "crypto_tickers.csv").decode()
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) > 20
    for row in rows:
        assert canonical(CRY, row["ticker"]) == row["ticker"]
        assert ws_symbol(CRY, row["ticker"]) == row["websocket"]
        assert canonical(CRY, row["websocket"]) == row["ticker"]
    quotes = {row["websocket"].split("-")[1] for row in rows}
    assert quotes <= set(CRYPTO_QUOTES)
    assert list(CRYPTO_QUOTES) == sorted(CRYPTO_QUOTES, key=len, reverse=True)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("aapl", "AAPL"),
        ("O:AAPL250117C00150000", "O:AAPL250117C00150000"),
        ("i:spx", "I:SPX"),
        ("C:EUR/USD", "C:EURUSD"),
        ("x:btc-usd", "X:BTCUSD"),
        ("ESZ5", "ESZ5"),
    ],
)
def test_canonical_without_a_class_selects_it_by_prefix(raw: str, expected: str) -> None:
    assert canonical(None, raw) == expected
    assert normalize(None, [raw]) == (expected,)


def test_the_prefix_message_names_the_rule() -> None:
    with pytest.raises(
        ValueError,
        match=r"^'X:BTCUSD' carries the prefix of another asset class; stocks symbols have no "
        r"prefix$",
    ):
        canonical(AssetClass.STOCKS, "X:BTCUSD")
    with pytest.raises(
        ValueError,
        match=r"^'C:EURUSD' carries the prefix of another asset class; crypto symbols use the "
        r"X: prefix$",
    ):
        canonical(AssetClass.CRYPTO, "C:EURUSD")
    with pytest.raises(ValueError, match="crypto symbols use the X: prefix"):
        canonical(None, "X:C:BTCUSD")


@pytest.mark.parametrize(("asset_class", "raw", "expected"), CANONICAL_FORMS)
def test_canonical_is_idempotent(asset_class: AssetClass, raw: str, expected: str) -> None:
    assert canonical(asset_class, canonical(asset_class, raw)) == expected


def test_symbol_canonicalizer(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    to_canonical = symbol_canonicalizer(FX)
    assert to_canonical("EUR/USD") == "C:EURUSD"
    assert to_canonical(None) is None
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._symbols"):
        assert to_canonical("not a pair") is None
        assert to_canonical("EURUSDX") is None
    assert len(caplog.records) == 1
    assert "'not a pair', which is not a valid forex symbol" in caplog.text


def test_canonical_view(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["btc-usd", "X:ETHUSD", "??"])])
    view = canonical_view(CRY, table)
    assert dh_arrow.to_arrow(view).column("Symbol").to_pylist() == ["X:BTCUSD", "X:ETHUSD", None]
