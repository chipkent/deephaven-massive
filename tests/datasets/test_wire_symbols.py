"""Tests for reading the symbols each transport sends, and writing the forms it takes."""

import csv
import io
import logging
from collections.abc import Callable

import pyarrow as pa
import pytest

from deephaven_massive._datasets import wire_symbols
from deephaven_massive._datasets.symbols import is_canonical
from deephaven_massive._datasets.wire_symbols import (
    WireSymbols,
    canonical_from_wire,
    is_index_option,
    unreadable_symbol_reason,
    wire_spellings,
    ws_symbol,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix
from deephaven_massive._errors import LIBRARY_BUG

STK, OPT, FUT, IDX, FX, CRY = (
    AssetClass.STOCKS,
    AssetClass.OPTIONS,
    AssetClass.FUTURES,
    AssetClass.INDICES,
    AssetClass.FOREX,
    AssetClass.CRYPTO,
)


@pytest.mark.parametrize(
    ("asset_class", "symbol", "expected"),
    [
        (FX, "C:EURUSD", "EUR-USD"),
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
    # A crypto split comes from Massive's crypto ticker list, given here as `pairs`.
    pairs: dict[str, tuple[str, str]] = {}
    if asset_class is CRY:
        base, quote = expected.split("-")
        pairs = {symbol: (base, quote)}
    assert ws_symbol(asset_class, symbol, pairs) == expected


@pytest.mark.parametrize(
    ("asset_class", "raw", "expected"),
    [
        (FX, "EUR/USD", "C:EURUSD"),  # WebSocket quote and aggregate frames
        (FX, "EUR-USD", "C:EURUSD"),  # the topic form
        (FX, "C:EURUSD", "C:EURUSD"),
        (FX, "C:EUR-USD", "C:EURUSD"),
        (FX, "X:EUR-USD", "C:EURUSD"),  # the forex quotes flat-file sample
        (FX, "EURUSD", "C:EURUSD"),
        (FX, "eur/usd", None),  # case is never folded on the wire
        (FX, "EUR/US", None),
        (CRY, "BTC-USD", "X:BTCUSD"),  # WebSocket frames
        (CRY, "X:BTC-USD", "X:BTCUSD"),  # FMV frames
        (CRY, "X:BTCUSD", "X:BTCUSD"),
        (CRY, "BTC/USD", "X:BTCUSD"),
        (CRY, "BTC-XYZ", "X:BTCXYZ"),  # any quote currency
        (CRY, "X:BTC-DAI", "X:BTCDAI"),
        (CRY, "X:BTCDAI", "X:BTCDAI"),
        (CRY, "ABCUS-DT", "X:ABCUSDT"),
        (CRY, "BTC--USD", None),
        (IDX, "I:SPX", "I:SPX"),
        (IDX, "SPX", "I:SPX"),  # the endpoint's class adds the prefix
        (IDX, "spx", None),  # case is never folded, and lower case is no index ticker
        (IDX, "S P X", None),
        (FUT, "BOB 02-01 U6", "BOB 02-01 U6"),
        (FUT, "ZZ:NEW COMBO Z6", "ZZ:NEW COMBO Z6"),  # a row keeps a form not seen yet
        (FUT, " ESZ5", None),
        (OPT, "O:AAPL250117C00150000", "O:AAPL250117C00150000"),
        (OPT, "AAPL250117C00150000", None),
        (STK, "KIMpL", "KIMpL"),
        (STK, "BRK/B", None),
        (FUT, "ESZ5", "ESZ5"),
        (ClassFromPrefix.ANY, "X:BTC-USD", "X:BTCUSD"),
        (ClassFromPrefix.ANY, "C:EURUSD", "C:EURUSD"),
        (ClassFromPrefix.ANY, "X:EUR-USD", "X:EURUSD"),  # without the forex class, X: is crypto
        (ClassFromPrefix.ANY, "I:SPX", "I:SPX"),
        (ClassFromPrefix.ANY, "AAPL", "AAPL"),
        (ClassFromPrefix.ANY, "aapl", None),
    ],
)
def test_canonical_from_wire(
    asset_class: AssetClass | ClassFromPrefix, raw: str, expected: str | None
) -> None:
    assert canonical_from_wire(asset_class, raw) == expected


@pytest.mark.parametrize(
    ("asset_class", "symbol"),
    [
        (FX, "C:EURUSD"),
        (CRY, "X:BTCUSDT"),
        (IDX, "I:SPX"),
        (STK, "BRK.B"),
        (OPT, "O:AAPL250117C00150000"),
    ],
)
def test_wire_spellings_are_the_inverse_of_canonical_from_wire(
    asset_class: AssetClass, symbol: str
) -> None:
    spellings = wire_spellings(asset_class, symbol)
    assert spellings[0] == symbol
    assert len(set(spellings)) == len(spellings)
    assert all(canonical_from_wire(asset_class, s) == symbol for s in spellings)


def test_wire_spellings_cover_the_flat_file_and_websocket_forms() -> None:
    assert {"X:EUR-USD", "EUR/USD", "EUR-USD"} <= set(wire_spellings(FX, "C:EURUSD"))
    assert {"BTC-USD", "X:BTC-USD"} <= set(wire_spellings(CRY, "X:BTCUSD"))
    assert "C:EUR-USD" in wire_spellings(FX, "C:EURUSD")  # the knowledge base's file spelling
    assert wire_spellings(IDX, "I:SPX") == ("I:SPX", "SPX")


def test_the_vendored_crypto_tickers_read_back_from_their_websocket_form(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # The fixture pairs well-known tickers with Massive's WebSocket form of each; the split
    # comes from Massive's crypto ticker list at run time, never from the ticker text.
    text = load_fixture("datasets/wire_symbols", "crypto_tickers.csv").decode()
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) > 20
    for row in rows:
        ticker, websocket = row["ticker"], row["websocket"]
        assert is_canonical(CRY, ticker)
        base, quote = websocket.split("-")
        split = (base, quote)
        assert ws_symbol(CRY, ticker, {ticker: split}) == websocket
        assert canonical_from_wire(CRY, websocket) == ticker


def test_the_unreadable_symbol_reason_names_the_spelling_and_the_bug() -> None:
    assert unreadable_symbol_reason("a") == (
        f"Massive sent the symbol 'a', a spelling the library cannot read. {LIBRARY_BUG}"
    )


def test_wire_symbols_read_a_column_and_log_nothing(caplog: pytest.LogCaptureFixture) -> None:
    # What a spelling it cannot read does is the reader's setting, applied by the caller.
    reader = WireSymbols("forex.quotes", FX)
    raw = pa.array(["EUR/USD", None, "??", "X:EUR-USD", "??", "!!", "##"])
    cache: dict[str, str | None] = {}
    with caplog.at_level(logging.DEBUG, logger=wire_symbols.__name__):
        read = reader.read(raw, cache)
    assert read.symbols.to_pylist() == ["C:EURUSD", None, None, "C:EURUSD", None, None, None]
    assert read.unreadable_rows.to_pylist() == [False, False, True, False, True, True, True]
    assert cache == {
        "EUR/USD": "C:EURUSD",
        "??": None,
        "X:EUR-USD": "C:EURUSD",
        "!!": None,
        "##": None,
    }
    assert caplog.records == []


def test_a_crypto_topic_takes_its_split_only_from_the_pairs_given() -> None:
    # The ticker text never decides the split: X:ETHWBTC ends in BTC but quotes in WBTC, and
    # X:DOTUSD could be DOT-USD or DO-TUSD.
    pairs = {"X:ETHWBTC": ("ETH", "WBTC"), "X:DOTUSD": ("DOT", "USD"), "X:BTCUSD": ("BTC", "USD")}
    assert ws_symbol(CRY, "X:ETHWBTC", pairs) == "ETH-WBTC"
    assert ws_symbol(CRY, "X:DOTUSD", pairs) == "DOT-USD"
    assert ws_symbol(CRY, "X:BTCUSD", pairs) == "BTC-USD"
    with pytest.raises(KeyError, match="X:BTCUSD"):
        ws_symbol(CRY, "X:BTCUSD", {})  # no split given: none is guessed
    assert ws_symbol(FX, "C:EURUSD", {}) == "EUR-USD"
    assert ws_symbol(STK, "AAPL", {}) == "AAPL"
    # A file may split a pair anywhere: every cut is looked for, and each reads back.
    for symbol in ("X:BTCXYZ", "X:BTCPYUSD", "X:BTCUSD"):
        spellings = wire_spellings(CRY, symbol)
        assert spellings[:2] == (symbol, symbol[2:])
        assert all(canonical_from_wire(CRY, s) == symbol for s in spellings)
    assert {"X:BTC-PYUSD", "BTC/PYUSD", "X:BTCPY-USD"} <= set(wire_spellings(CRY, "X:BTCPYUSD"))


# --- Option underlyings ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [("I:SPX", "SPX"), ("I:NDX", "NDX"), ("AAPL", "AAPL"), ("BRK.B", "BRK.B"), ("KIMpL", "KIMpL")],
)
def test_the_contracts_list_takes_an_index_underlying_bare(symbol: str, expected: str) -> None:
    assert wire_symbols.contracts_underlying(symbol) == expected


def test_an_option_on_an_index_gets_its_prefix_by_its_cfi_code() -> None:
    # An SPX call is `OCEICS` (fourth letter I, an index); an AAPL call `OCASPS` (S, a stock).
    underlyings = pa.array(["SPX", "I:NDX", "AAPL", "VIX", None, "SPX"], pa.string())
    cfi = pa.array(["OCEICS", "OPEICS", "OCASPS", None, "OCEICS", "FFICSX"], pa.string())
    assert wire_symbols.index_underlyings(underlyings, cfi).to_pylist() == [
        "I:SPX",
        "I:NDX",
        "AAPL",
        "VIX",  # no code: kept as written
        None,
        "SPX",  # a futures code, not an option's
    ]


def test_a_class_with_no_spellings_is_a_defect() -> None:
    # Every class names its file spellings; a new one fails until it does, never reads as stocks.
    with pytest.raises(AssertionError):
        wire_spellings("bonds", "X:Y")  # type: ignore[arg-type]  # the point: refused at run time


@pytest.mark.parametrize(
    ("cfi", "expected"), [("OCEICS", True), ("OPEICS", True), ("OCASPS", False), ("ESVUFR", False)]
)
def test_a_cfi_code_marks_an_option_on_an_index_by_its_fourth_letter(
    cfi: str, *, expected: bool
) -> None:
    assert is_index_option(cfi) is expected
