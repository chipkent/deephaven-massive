"""Tests for symbol normalization."""

import csv
import io
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive import _errors
from deephaven_massive._datasets.symbols import (
    CRYPTO_QUOTES,
    SymbolTable,
    canonical,
    is_forex_currency,
    normalize,
    symbol_canonicalizer,
    symbol_class,
    underlying,
    ws_symbol,
)
from deephaven_massive._enums import AssetClass

if TYPE_CHECKING:
    from collections.abc import Iterable

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
        (FX, "EU1/USD"),  # a forex code is three letters, never a digit
        (FX, "EU1USD"),
        (FX, "C:EURUS1"),
    ],
)
def test_invalid_symbols(asset_class: AssetClass, raw: str) -> None:
    with pytest.raises(ValueError, match=r"symbol|pair|prefix"):
        canonical(asset_class, raw)


def test_forex_currencies_are_three_letters() -> None:
    assert is_forex_currency("EUR")
    for code in ("EU1", "EU", "EURO", "eur", "1NC"):
        assert not is_forex_currency(code)
    # Crypto codes keep their digits.
    assert canonical(CRY, "1INCH-USD") == "X:1INCHUSD"


def test_non_string_symbol() -> None:
    with pytest.raises(TypeError, match="must be a str"):
        canonical(STK, cast("str", 5))


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
    assert ws_symbol(asset_class, symbol) == expected


@pytest.mark.parametrize("frame_symbol", ["USD/CNH", "USD-CNH", "C:USDCNH"])
def test_every_forex_wire_spelling_reads_back_to_the_topic_symbol(frame_symbol: str) -> None:
    # Topics use the documented `{from}-{to}` form; quote and aggregate frames carry `USD/CNH`
    # and FMV frames `C:USDCNH`. All of them must name the pair the topic subscribed.
    assert ws_symbol(FX, "C:USDCNH") == "USD-CNH"
    assert canonical(FX, frame_symbol) == "C:USDCNH"
    assert canonical(FX, ws_symbol(FX, "C:USDCNH")) == "C:USDCNH"


@pytest.mark.parametrize(
    ("asset_class", "raw"),
    [
        (None, "I:"),
        (None, "O:"),
        (None, "X:"),
        (None, " c: "),
        (IDX, "I:"),
        (OPT, "O:"),
        (FX, "C:"),
        (CRY, "X:"),
    ],
)
def test_a_prefix_without_a_symbol_is_rejected(asset_class: AssetClass | None, raw: str) -> None:
    with pytest.raises(ValueError, match="is a prefix without a symbol"):
        canonical(asset_class, raw)


@pytest.mark.parametrize(
    "raw",
    [
        "O:AAPL250132C00150000",  # day 32
        "O:AAPL251301C00150000",  # month 13
        "O:AAPL250229C00150000",  # 2025 is no leap year
        "O:AAPL250100C00150000",  # day 0
        "O:AAPL1234567C00150000",  # root AAPL1, expiry 234567
    ],
)
def test_an_option_expiry_must_be_a_real_date(raw: str) -> None:
    with pytest.raises(ValueError, match="OCC"):
        canonical(OPT, raw)
    with pytest.raises(ValueError, match="OCC"):
        underlying(raw)


def test_a_leap_day_expiry_is_a_real_date() -> None:
    assert canonical(OPT, "AAPL280229C00150000") == "O:AAPL280229C00150000"


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
    assert normalize(STK, cast("Iterable[str]", (s for s in ["B", "A"]))) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    symbols = (s for s in ["A"])
    list(symbols)
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, cast("Iterable[str]", symbols))
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, [])


def test_normalize_all_passes_through() -> None:
    assert normalize(STK, dm.ALL) is dm.ALL


def test_symbol_tables_compare_by_wrapper(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["AAPL"])])
    boxed = SymbolTable(table)
    assert boxed.table is table
    # Boxes of one wrapper are equal; boxes of two wrappers of one Java table are not.
    again = SymbolTable(table)
    assert boxed == again
    assert hash(boxed) == hash(again)
    twin = Table(j_table=table.j_table)
    assert twin == table
    assert SymbolTable(twin) != boxed


def test_normalize_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(TypeError, match="symbols must be"):
        normalize(STK, cast("Iterable[str]", 5))
    with pytest.raises(TypeError, match="must be a str"):
        normalize(STK, cast("Iterable[str]", ["AAPL", 5]))


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
    text = load_fixture("datasets/symbols", "crypto_tickers.csv").decode()
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
    with caplog.at_level(logging.WARNING, logger="deephaven_massive._datasets.symbols"):
        assert to_canonical("not a pair") is None
        assert to_canonical("EURUSDX") is None
    assert len(caplog.records) == 1
    assert "'not a pair', which is not a valid forex symbol" in caplog.text


@pytest.mark.parametrize(
    ("symbol", "asset_class"),
    [
        ("AAPL", AssetClass.STOCKS),
        ("O:SPY250117C00600000", AssetClass.OPTIONS),
        ("I:SPX", AssetClass.INDICES),
        ("C:EURUSD", AssetClass.FOREX),
        ("X:BTCUSD", AssetClass.CRYPTO),
    ],
)
def test_symbol_class(symbol: str, asset_class: AssetClass) -> None:
    assert symbol_class(symbol) is asset_class
