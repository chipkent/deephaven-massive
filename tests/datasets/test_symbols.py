"""Tests for the canonical symbol grammar, the converters, and the wire spellings."""

import csv
import io
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets import symbols
from deephaven_massive._datasets.symbols import (
    CRYPTO_QUOTES,
    SymbolTable,
    WireSymbols,
    canonical_from_wire,
    check_symbol,
    is_canonical,
    is_forex_currency,
    listed_pair,
    normalize,
    option_roots,
    symbol_class,
    topic_problem,
    unknown_symbol_message,
    wire_spellings,
    ws_symbol,
)
from deephaven_massive._enums import AssetClass, SymbolConvention, UnknownSymbols
from deephaven_massive._errors import BoundedWarnings

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

CANONICAL = [
    (STK, "AAPL"),
    (STK, "BRK.B"),
    (STK, "KIMpL"),  # Kimco's series L preferred: the lower-case p is meaningful
    (STK, "DCOMp"),
    (STK, "NTEST.Q"),
    (STK, "AAC.WS"),
    (STK, "CELGr"),
    (STK, "AAPLw"),
    (STK, "TSLAAPL"),
    (OPT, "O:AAPL250117C00150000"),
    (OPT, "O:BRKB250117C00400000"),
    (OPT, "O:AAPL1250117C00150000"),  # an adjusted root keeps its digit
    (OPT, "O:SPXW251219P05000000"),
    (OPT, "O:AAPL280229C00150000"),  # a leap day
    (FUT, "ESZ5"),
    (FUT, "6CH5"),
    (FUT, "0NGH39"),
    (FUT, "ESZ25"),  # a year is never rewritten (and ESZ25 is not ESZ5)
    (FUT, "10YN6-10YQ6"),  # spreads, as Massive's CBOT, COMEX, NYMEX, and CME files write them
    (FUT, "GCQ6-GCZ6"),
    (FUT, "BOB 02-01 U6"),
    (FUT, "CL:BF Q6-U6-V6"),
    (FUT, "CL:BZ U6-V6"),
    (FUT, "6A:XF:AUDUSD:Q6"),
    (FUT, "esz5"),  # a dataset takes any ticker as written; only the converters check forms
    (FUT, "BOB 2-1 U6"),
    (IDX, "I:SPX"),
    (IDX, "I:AAVE10RP"),
    (IDX, "I:APy"),
    (IDX, "I:spx"),  # Massive's pattern allows lower case
    (FX, "C:EURUSD"),
    (CRY, "X:BTCUSD"),
    (CRY, "X:1INCHUSDT"),
    (CRY, "X:BTCXYZ"),  # a quote the library does not know: canonical, though not streamable
    (CRY, "X:BTCPYUSD"),  # ends in PYUSD and in USD: canonical, though not streamable
]

NOT_CANONICAL = [
    (STK, ""),
    (STK, "aapl"),
    (STK, "Aapl"),
    (STK, "BRK-B"),
    (STK, "BRK/B"),
    (STK, "BRK B"),
    (STK, " AAPL"),
    (STK, "AAPL\n"),  # a trailing newline never passes
    (FX, "C:EURUSD\n"),
    (STK, "BRK."),
    (STK, "BRK..B"),
    (STK, "X:BTCUSD"),
    (OPT, "AAPL250117C00150000"),
    (OPT, "o:AAPL250117C00150000"),
    (OPT, "O:BRK.B250117C00400000"),  # an OCC root has no dot
    (OPT, "O:AAPL250132C00150000"),
    (OPT, "O:AAPL251301C00150000"),
    (OPT, "O:AAPL250229C00150000"),  # 2025 is no leap year
    (OPT, "O:AAPL250117X00150000"),
    (FUT, ""),
    (FUT, " ESZ5"),
    (FUT, "ESZ5\n"),
    (FUT, "X:ESZ5"),
    (IDX, "SPX"),
    (IDX, "i:SPX"),
    (IDX, "I:"),
    (FX, "C:EURUS1"),
    (FX, "C:EUR/USD"),
    (FX, "EURUSD"),
    (CRY, "X:BTC-USD"),
    (CRY, "X:"),
    (CRY, "x:BTCUSD"),
]


@pytest.mark.parametrize(("asset_class", "text"), CANONICAL)
def test_canonical_text_is_canonical_and_converts_to_itself(
    asset_class: AssetClass, text: str
) -> None:
    assert is_canonical(asset_class, text)
    assert dm.canonical_symbol(text, asset_class) == text
    assert check_symbol(asset_class, text) == text
    assert canonical_from_wire(asset_class, text) == text


@pytest.mark.parametrize(("asset_class", "text"), NOT_CANONICAL)
def test_other_text_is_not_canonical(asset_class: AssetClass, text: str) -> None:
    assert not is_canonical(asset_class, text)
    with pytest.raises(ValueError, match=r"Massive|ticker"):
        check_symbol(asset_class, text)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("AAPL", True),
        ("KIMpL", True),
        ("O:AAPL250117C00150000", True),
        ("I:SPX", True),
        ("C:EURUSD", True),
        ("X:BTCUSD", True),
        ("aapl", False),
        ("O:AAPL", False),
        ("C:EUR/USD", False),
    ],
)
def test_without_a_class_the_prefix_selects_the_grammar(text: str, expected: bool) -> None:
    assert is_canonical(None, text) is expected


CONVERSIONS = [
    (dm.canonical_symbol_option, "AAPL250117C00150000", "O:AAPL250117C00150000"),
    (dm.canonical_symbol_option, "o:spxw251219p05000000", "O:SPXW251219P05000000"),
    (dm.canonical_symbol_option, "BRKB  250117C00400000", "O:BRKB250117C00400000"),
    (dm.canonical_symbol_option, "SPXW  251219P05000000", "O:SPXW251219P05000000"),
    (dm.canonical_symbol_index, "SPX", "I:SPX"),
    (dm.canonical_symbol_index, "NDX", "I:NDX"),
    (dm.canonical_symbol_forex, "EUR/USD", "C:EURUSD"),
    (dm.canonical_symbol_forex, "EUR-USD", "C:EURUSD"),
    (dm.canonical_symbol_forex, "EURUSD", "C:EURUSD"),
    (dm.canonical_symbol_forex, "eur/usd", "C:EURUSD"),
    (dm.canonical_symbol_forex, "C:EUR-USD", "C:EURUSD"),
    (dm.canonical_symbol_crypto, "BTC-USD", "X:BTCUSD"),
    (dm.canonical_symbol_crypto, "BTC/USD", "X:BTCUSD"),
    (dm.canonical_symbol_crypto, "BTCUSD", "X:BTCUSD"),
    (dm.canonical_symbol_crypto, "x:btc-usd", "X:BTCUSD"),
    (dm.canonical_symbol_crypto, "doge-usdt", "X:DOGEUSDT"),
    (dm.canonical_symbol_crypto, "1INCH-USD", "X:1INCHUSD"),
    (dm.canonical_symbol_crypto, "BTC-DAI", "X:BTCDAI"),
    (dm.canonical_symbol_crypto, "BTC-PYUSD", "X:BTCPYUSD"),
    (dm.canonical_symbol_future, "ESZ5", "ESZ5"),
    (dm.canonical_symbol_future_spread, "10YN6-10YQ6", "10YN6-10YQ6"),
    (dm.canonical_symbol_future_spread, "TEX 07-05 U6", "TEX 07-05 U6"),
    (dm.canonical_symbol_future_spread, "CL:BF Z6-H7-M7", "CL:BF Z6-H7-M7"),
    (dm.canonical_symbol_future_spread, "6A:XF:AUDUSD:U6", "6A:XF:AUDUSD:U6"),  # any quote currency
    (dm.canonical_symbol_crypto, "ABCUS-DT", "X:ABCUSDT"),  # the codes joined as written
    (dm.canonical_symbol_stock_yahoo, "BRK-B", "BRK.B"),
    (dm.canonical_symbol_stock_yahoo, "BAC-PL", "BACpL"),
    (dm.canonical_symbol_stock_yahoo, "KIM-PL", "KIMpL"),
    (dm.canonical_symbol_stock_yahoo, "AAPL", "AAPL"),
    (dm.canonical_symbol_stock_nasdaq, "KIM-L", "KIMpL"),
    (dm.canonical_symbol_stock_nasdaq, "DCOM-", "DCOMp"),
    (dm.canonical_symbol_stock_nasdaq, "BRK.B", "BRK.B"),
    (dm.canonical_symbol_stock_nasdaq, "MSFT", "MSFT"),
    (dm.canonical_symbol_stock_cms, "BRK B", "BRK.B"),
    (dm.canonical_symbol_stock_cms, "BAC PRL", "BACpL"),
    (dm.canonical_symbol_stock_cms, "DCOM PR", "DCOMp"),
    (dm.canonical_symbol_stock_cms, "MSFT", "MSFT"),
]


@pytest.mark.parametrize(("convert", "text", "expected"), CONVERSIONS)
def test_the_converters_convert_their_spellings(
    convert: Callable[[str | None], str | None], text: str, expected: str
) -> None:
    assert convert(text) == expected
    if "_stock_" not in convert.__name__:  # a convention reads only its own grammar
        assert convert(expected) == expected  # idempotent
    assert convert(None) is None


@pytest.mark.parametrize(
    ("convention", "text", "expected"),
    [
        (SymbolConvention.YAHOO, "BRK-B", "BRK.B"),
        (SymbolConvention.NASDAQ, "KIM-L", "KIMpL"),
        (SymbolConvention.CMS, "BAC PRL", "BACpL"),
    ],
)
def test_canonical_symbol_takes_a_stock_convention(
    convention: SymbolConvention, text: str, expected: str
) -> None:
    assert dm.canonical_symbol(text, STK, convention=convention) == expected


@pytest.mark.parametrize(
    ("convention", "text"),
    [
        (SymbolConvention.YAHOO, "BBAI-WT"),  # warrants: Massive's spelling is undocumented
        (SymbolConvention.YAHOO, "AAC-UN"),  # units
        (SymbolConvention.YAHOO, "CELG-RI"),  # rights
        (SymbolConvention.YAHOO, "DCOM-P"),  # a generic preferred and class P read alike
        (SymbolConvention.YAHOO, "ABC-U"),  # class U collides with units
        (SymbolConvention.YAHOO, "brk-b"),
        (SymbolConvention.YAHOO, "BRK-B-C"),
        (SymbolConvention.NASDAQ, "AAC+"),
        (SymbolConvention.NASDAQ, "AAC="),
        (SymbolConvention.NASDAQ, "CELG^"),
        (SymbolConvention.NASDAQ, "ABC#"),
        (SymbolConvention.NASDAQ, "ABC-LL"),
        (SymbolConvention.NASDAQ, "ABC.U"),  # class U, never Massive's unit ABC.U
        (SymbolConvention.NASDAQ, "KIMpL"),  # Massive's spelling is not Nasdaq's
        (SymbolConvention.CMS, "BRK.B"),
        (SymbolConvention.YAHOO, "KIMpL"),
        (SymbolConvention.NASDAQ, "BAC$L"),  # the ACT symbology, which is ambiguous
        (SymbolConvention.CMS, "AAC WS"),
        (SymbolConvention.CMS, "AAC U"),
        (SymbolConvention.CMS, "X RT"),
        (SymbolConvention.CMS, "X WI"),
    ],
)
def test_a_convention_converts_only_its_plain_class_and_preferred_spellings(
    convention: SymbolConvention, text: str
) -> None:
    with pytest.raises(ValueError, match=rf"of the {convention.name} convention\. Massive"):
        dm.canonical_symbol(text, STK, convention=convention)


@pytest.mark.parametrize("text", ["VOD.L", "SHOP.TO", "BMW.DE", "BRK.B"])
def test_a_yahoo_ticker_with_a_dot_is_a_foreign_listing(text: str) -> None:
    with pytest.raises(ValueError, match=rf"^'{text}' is a Yahoo ticker of a listing outside"):
        dm.canonical_symbol_stock_yahoo(text)


def test_class_u_converts_under_no_convention() -> None:
    # CQS and CMS write units as U, so a class U would read as a unit.
    for convention, text in (
        (SymbolConvention.YAHOO, "ABC-U"),
        (SymbolConvention.NASDAQ, "ABC.U"),
        (SymbolConvention.CMS, "ABC U"),
    ):
        with pytest.raises(ValueError, match="is not a plain, class, or preferred ticker"):
            dm.canonical_symbol(text, STK, convention=convention)
    assert dm.canonical_symbol("ABC.T", STK, convention=SymbolConvention.NASDAQ) == "ABC.T"
    assert dm.canonical_symbol("ABC.V", STK, convention=SymbolConvention.NASDAQ) == "ABC.V"


def test_a_convention_refuses_another_class_prefix() -> None:
    with pytest.raises(ValueError, match="is a crypto ticker"):
        dm.canonical_symbol("X:BTC-USD", STK, convention=SymbolConvention.YAHOO)


def test_a_convention_applies_to_stocks_only() -> None:
    with pytest.raises(ValueError, match=r"^a convention applies to stocks only, not forex$"):
        dm.canonical_symbol("EUR/USD", FX, convention=SymbolConvention.YAHOO)


def test_converter_enums_reject_text() -> None:
    with pytest.raises(TypeError, match=r"^asset_class must be dm\.AssetClass"):
        dm.canonical_symbol("AAPL", cast("AssetClass", "stocks"))
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonical_symbol("BRK-B", STK, convention=cast("SymbolConvention", "yahoo"))


@pytest.mark.parametrize(
    "convert",
    [
        dm.canonical_symbol_stock,
        dm.canonical_symbol_option,
        dm.canonical_symbol_future,
        dm.canonical_symbol_future_spread,
        dm.canonical_symbol_index,
        dm.canonical_symbol_forex,
        dm.canonical_symbol_crypto,
        dm.canonical_symbol_stock_yahoo,
        dm.canonical_symbol_stock_nasdaq,
        dm.canonical_symbol_stock_cms,
    ],
)
def test_a_converter_takes_only_text_or_none(convert: Callable[[object], str | None]) -> None:
    with pytest.raises(TypeError, match=r"^a symbol must be a str or None, not int$"):
        convert(5)


@pytest.mark.parametrize(
    ("convert", "text", "message"),
    [
        (
            dm.canonical_symbol_stock,
            "BRK-B",
            (
                r"^'BRK-B' is not a Massive stock ticker: it uses another "
                r"convention's separators\. Name the convention it comes from: "
                r".*dm\.SymbolConvention\.YAHOO, "
                r"dm\.SymbolConvention\.NASDAQ, dm\.SymbolConvention\.CMS"
            ),
        ),
        (dm.canonical_symbol_stock, "BRK/B", "another convention's separators"),
        (dm.canonical_symbol_stock, "BRK B", "another convention's separators"),
        (
            dm.canonical_symbol_stock,
            "kimpl",
            (
                r"^'kimpl' is not a Massive stock ticker: tickers are case-sensitive, with an "
                r"upper-case root and possibly a lower-case suffix \(KIMpL is a preferred\); "
                r"write Massive's ticker exactly$"
            ),
        ),
        (
            dm.canonical_symbol_stock,
            " AAPL ",
            r"^' AAPL ' has surrounding whitespace; write 'AAPL'$",
        ),
        (dm.canonical_symbol_stock, "Aapl", r"^'Aapl' is not a Massive stock ticker such as"),
        (dm.canonical_symbol_stock, "", r"^'' is not a Massive stock ticker such as"),
        (
            dm.canonical_symbol_stock,
            "C:EURUSD",
            (
                r"^'C:EURUSD' is a forex ticker \(prefix C:\), not a stock one; call "
                r"dm\.canonical_symbol_forex$"
            ),
        ),
        (
            dm.canonical_symbol_crypto,
            "C:EURUSD",
            (
                r"^'C:EURUSD' is a forex ticker \(prefix C:\), not a crypto one; call "
                r"dm\.canonical_symbol_forex$"
            ),
        ),
        (
            dm.canonical_symbol_forex,
            "X:BTCUSD",
            r"^'X:BTCUSD' is a crypto ticker \(prefix X:\), not a forex one",
        ),
        (
            dm.canonical_symbol_stock,
            "o:AAPL250117C00150000",
            r"is an option ticker \(prefix O:\), not a stock one; call dm\.canonical_symbol_option",
        ),
        (
            dm.canonical_symbol_option,
            "I:SPX",
            r"^'I:SPX' is an index ticker \(prefix I:\), not an option one",
        ),
        (
            dm.canonical_symbol_option,
            "AAPL",
            r"^'AAPL' is an underlying, not an OCC option symbol such as",
        ),
        (dm.canonical_symbol_option, "O:AAPL250132C00150000", "with a real expiry date"),
        (dm.canonical_symbol_option, "BRKB 250117C00400000", "with a real expiry date"),
        (dm.canonical_symbol_future, " ESZ5 ", r"^' ESZ5 ' has surrounding whitespace; write"),
        (dm.canonical_symbol_future, "", r"^'' is not a Massive futures ticker: a contract is"),
        (dm.canonical_symbol_future, "esz5", "a contract is upper-case letters and digits"),
        (
            dm.canonical_symbol_future,
            "GCQ6-GCZ6",
            (
                r"^'GCQ6-GCZ6' is a futures spread, not a single contract; call "
                r"dm\.canonical_symbol_future_spread$"
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "ESZ5",
            (
                r"^'ESZ5' is a single futures contract, not a spread; call "
                r"dm\.canonical_symbol_future$"
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "BOB 2-1 U6",
            r"^'BOB 2-1 U6' is not one of Massive's futures spread spellings: a calendar",
        ),
        (dm.canonical_symbol_future_spread, "X:BTCUSD", "is a crypto ticker"),
        (dm.canonical_symbol_future, "X:ESZ5", "is a crypto ticker"),
        (dm.canonical_symbol_index, "S P X", r"^'S P X' is not a Massive index ticker"),
        (dm.canonical_symbol_index, "C:EURUSD", "is a forex ticker"),
        (dm.canonical_symbol_forex, "EU1/USD", "two three-letter codes"),
        (dm.canonical_symbol_forex, "EURO/USD", "two three-letter codes"),
        (dm.canonical_symbol_forex, "EUR/USD/JPY", "two three-letter codes"),
        (dm.canonical_symbol_forex, "EUR USD", "two three-letter codes"),
        (dm.canonical_symbol_crypto, "BTC.USD", r"^cannot read 'BTC.USD' as a crypto pair"),
        (dm.canonical_symbol_crypto, "BTC--USD", r"^cannot read 'BTC--USD' as a crypto pair"),
    ],
)
def test_a_converter_refuses_with_a_reason(
    convert: Callable[[str], str | None], text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        convert(text)


def test_no_conversion_looks_a_name_up() -> None:
    # The index prefix comes from the class the caller or the endpoint names, never from the
    # ticker: a made-up name converts like a real one, and a bare name is a stock elsewhere.
    for name in ("SPX", "ZZQX", "AAPL"):
        assert dm.canonical_symbol_index(name) == f"I:{name}"
        assert canonical_from_wire(IDX, name) == f"I:{name}"
        assert canonical_from_wire(None, name) == name
        assert canonical_from_wire(STK, name) == name
        with pytest.raises(ValueError, match="use 'I:"):
            check_symbol(IDX, name)


def test_check_symbol_names_the_fix() -> None:
    with pytest.raises(
        ValueError,
        match=r"^'EUR/USD' is not Massive's ticker; use 'C:EURUSD' \(symbols must be Massive's "
        r"exact tickers, and dm\.canonical_symbol_forex converts\)$",
    ):
        check_symbol(FX, "EUR/USD")
    with pytest.raises(
        ValueError, match=r"tickers are case-sensitive.*; symbols must be Massive's"
    ):
        check_symbol(STK, "aapl")
    with pytest.raises(ValueError, match=r"use 'I:SPX'"):
        check_symbol(IDX, "SPX")
    # A canonical ticker of another class names the class the dataset takes, not a converter.
    with pytest.raises(
        ValueError, match=r"^'X:BTCUSD' is a crypto ticker; this dataset takes stock tickers$"
    ):
        check_symbol(STK, "X:BTCUSD")
    with pytest.raises(
        ValueError, match=r"^'I:SPX' is an index ticker; this dataset takes stock tickers$"
    ):
        check_symbol(STK, "I:SPX")
    with pytest.raises(ValueError, match=r"is a crypto ticker \(prefix X:\)"):
        check_symbol(STK, "X:BTC-USD")  # not canonical crypto either: the converter's reason
    with pytest.raises(TypeError, match=r"^a symbol must be a str, not int$"):
        check_symbol(STK, 5)


def test_forex_currencies_are_three_letters() -> None:
    assert is_forex_currency("EUR")
    for code in ("EU1", "EU", "EURO", "eur", "1NC"):
        assert not is_forex_currency(code)


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
        (IDX, "spx", "I:spx"),  # never re-cased
        (IDX, "S P X", None),
        (FUT, "BOB 02-01 U6", "BOB 02-01 U6"),
        (FUT, "ZZ:NEW COMBO Z6", "ZZ:NEW COMBO Z6"),  # a row keeps a form not seen yet
        (FUT, " ESZ5", None),
        (OPT, "O:AAPL250117C00150000", "O:AAPL250117C00150000"),
        (OPT, "AAPL250117C00150000", None),
        (STK, "KIMpL", "KIMpL"),
        (STK, "BRK/B", None),
        (FUT, "ESZ5", "ESZ5"),
        (None, "X:BTC-USD", "X:BTCUSD"),
        (None, "C:EURUSD", "C:EURUSD"),
        (None, "X:EUR-USD", "X:EURUSD"),  # without the forex class, X: is crypto
        (None, "I:SPX", "I:SPX"),
        (None, "AAPL", "AAPL"),
        (None, "aapl", None),
    ],
)
def test_canonical_from_wire(
    asset_class: AssetClass | None, raw: str, expected: str | None
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
        (None, "C:EURUSD"),
        (None, "X:ETHBTC"),
        (None, "AAPL"),
    ],
)
def test_wire_spellings_are_the_inverse_of_canonical_from_wire(
    asset_class: AssetClass | None, symbol: str
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


def test_crypto_quotes_cover_the_vendored_tickers(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # The fixture is assembled from well-known pairs, not from a live listing (no API key was
    # available). Regenerate it from /v3/reference/tickers?market=crypto with a live key, and
    # widen CRYPTO_QUOTES if this test then fails.
    text = load_fixture("datasets/symbols", "crypto_tickers.csv").decode()
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) > 20
    ambiguous = set()
    for row in rows:
        ticker, websocket = row["ticker"], row["websocket"]
        assert is_canonical(CRY, ticker)
        split = cast("tuple[str, str]", tuple(websocket.split("-")))
        if listed_pair(ticker) is None:  # two listed quotes end it: Massive's data splits it
            ambiguous.add(ticker)
        else:
            assert listed_pair(ticker) == split
        assert ws_symbol(CRY, ticker, {ticker: split}) == websocket
        assert canonical_from_wire(CRY, websocket) == ticker
    assert ambiguous == {"X:DOTUSD", "X:SHIBUSD", "X:USDTUSD"}
    quotes = {row["websocket"].split("-")[1] for row in rows}
    assert quotes <= set(CRYPTO_QUOTES)
    assert list(CRYPTO_QUOTES) == sorted(CRYPTO_QUOTES, key=len, reverse=True)


def test_option_roots() -> None:
    roots = option_roots(
        pa.array(["O:SPXW251219C06000000", "O:AAPL1250117C00150000", "O:F250117P00010000", None])
    )
    assert roots.to_pylist() == ["SPXW", "AAPL1", "F", None]


def test_normalize_sorts_and_deduplicates_canonical_symbols() -> None:
    assert normalize(STK, ["MSFT", "AAPL", "MSFT"]) == ("AAPL", "MSFT")
    assert normalize(STK, "KIMpL") == ("KIMpL",)
    assert normalize(None, ["X:BTCUSD", "I:SPX", "AAPL"]) == ("AAPL", "I:SPX", "X:BTCUSD")


@pytest.mark.parametrize(
    ("asset_class", "value"),
    [(STK, "aapl"), (FX, ["C:EURUSD", "EUR/USD"]), (IDX, "SPX"), (None, ["X:BTC-USD"])],
)
def test_normalize_refuses_what_is_not_canonical(
    asset_class: AssetClass | None, value: str | list[str]
) -> None:
    with pytest.raises(ValueError, match="symbols must be Massive's exact tickers"):
        normalize(asset_class, value)


def test_normalize_materializes_generators() -> None:
    assert normalize(STK, cast("Iterable[str]", (s for s in ["B", "A"]))) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    items = (s for s in ["A"])
    list(items)
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, cast("Iterable[str]", items))
    with pytest.raises(ValueError, match=r"^no symbols$"):
        normalize(STK, [])


def test_normalize_all_passes_through() -> None:
    assert normalize(STK, dm.ALL) is dm.ALL


def test_normalize_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(TypeError, match="symbols must be"):
        normalize(STK, cast("Iterable[str]", 5))
    with pytest.raises(TypeError, match="must be a str"):
        normalize(STK, cast("Iterable[str]", ["AAPL", 5]))


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


def test_the_unknown_symbol_message_names_the_spellings_and_the_switch() -> None:
    message = unknown_symbol_message("forex.quotes", ["b", "a"])
    assert message.startswith("forex.quotes: Massive sent the symbols 'a', 'b', spellings the")
    one = unknown_symbol_message("forex.quotes", ["a"])
    assert one.startswith("forex.quotes: Massive sent the symbol 'a', a spelling the library")
    assert "MASSIVE_UNKNOWN_SYMBOLS=warn" in message
    assert "MassiveConfig(unknown_symbols=dm.UnknownSymbols.WARN)" in message


@pytest.fixture
def fresh_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        symbols, "_unknown_spellings", BoundedWarnings(logging.getLogger(symbols.__name__), limit=2)
    )


@pytest.mark.usefixtures("fresh_warnings")
def test_wire_symbols_read_a_column_and_keep_what_they_cannot(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = WireSymbols("forex.quotes", FX, UnknownSymbols.WARN)
    raw = pa.array(["EUR/USD", None, "??", "X:EUR-USD", "??", "!!", "##"])
    cache: dict[str, str | None] = {}
    with caplog.at_level(logging.WARNING, logger=symbols.__name__):
        read = reader.read(raw, cache)
        reader.read(raw)  # a fresh read warns no spelling twice
    assert read.symbols.to_pylist() == ["C:EURUSD", None, None, "C:EURUSD", None, None, None]
    assert read.raw.to_pylist() == [None, None, "??", None, "??", "!!", "##"]
    assert read.unknown == ("!!", "##", "??")
    assert cache == {
        "EUR/USD": "C:EURUSD",
        "??": None,
        "X:EUR-USD": "C:EURUSD",
        "!!": None,
        "##": None,
    }
    # Two spellings are reported, then one record says the rest are not.
    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        (
            "forex.quotes: Massive sent the symbol '!!', a spelling the library cannot read; its "
            "rows have a null Symbol and keep the text in RawSymbol"
        ),
        (
            "forex.quotes: Massive sent the symbol '##', a spelling the library cannot read; its "
            "rows have a null Symbol and keep the text in RawSymbol"
        ),
        "2 values were reported; further ones are not logged",
    ]


@pytest.mark.usefixtures("fresh_warnings")
def test_wire_symbols_log_nothing_under_fail(caplog: pytest.LogCaptureFixture) -> None:
    reader = WireSymbols("forex.quotes", FX, UnknownSymbols.FAIL)
    with caplog.at_level(logging.WARNING, logger=symbols.__name__):
        read = reader.read(pa.array(["??", "C:EURUSD"]))
    assert read.unknown == ("??",)
    assert read.symbols.to_pylist() == [None, "C:EURUSD"]
    assert caplog.records == []


def test_the_quote_list_splits_a_crypto_ticker_only_without_a_guess() -> None:
    assert listed_pair("X:BTCUSD") == ("BTC", "USD")
    assert listed_pair("X:BTCDAI") == ("BTC", "DAI")  # DAI is a listed quote
    assert ws_symbol(CRY, "X:ETHUSDE") == "ETH-USDE"
    assert topic_problem(CRY, "X:BTCUSD") is None
    assert topic_problem(STK, "AAPL") is None
    assert topic_problem(None, "X:BTCXYZ") == (
        "'X:BTCXYZ' cannot be split into its two currencies by the quote list: its quote "
        f"currency is none of {', '.join(CRYPTO_QUOTES)}"
    )
    # Two quotes end these, so the quote list gives no split and the transport asks Massive.
    for symbol, candidates in (
        ("X:BTCPYUSD", "BTC-PYUSD or BTCPY-USD"),
        ("X:DOTUSD", "DO-TUSD or DOT-USD"),
        ("X:SHIBUSD", "SHI-BUSD or SHIB-USD"),
        ("X:USDTUSD", "USD-TUSD or USDT-USD"),
    ):
        assert listed_pair(symbol) is None
        problem = topic_problem(CRY, symbol)
        assert problem is not None
        assert problem.endswith(f"its pair splits ambiguously ({candidates})")
        with pytest.raises(ValueError, match=rf"^'{symbol}' cannot be split"):
            ws_symbol(CRY, symbol)
    # Given Massive's split, the topic follows it.
    assert ws_symbol(CRY, "X:BTCPYUSD", {"X:BTCPYUSD": ("BTC", "PYUSD")}) == "BTC-PYUSD"
    assert ws_symbol(CRY, "X:DOTUSD", {"X:DOTUSD": ("DOT", "USD")}) == "DOT-USD"
    # A file may split a pair anywhere: every cut is looked for, and each reads back.
    for symbol in ("X:BTCXYZ", "X:BTCPYUSD", "X:BTCUSD"):
        spellings = wire_spellings(CRY, symbol)
        assert spellings[:2] == (symbol, symbol[2:])
        assert all(canonical_from_wire(CRY, s) == symbol for s in spellings)
    assert {"X:BTC-PYUSD", "BTC/PYUSD", "X:BTCPY-USD"} <= set(wire_spellings(CRY, "X:BTCPYUSD"))


def test_a_dataset_kept_as_written_takes_any_ticker_text() -> None:
    for text in ("ERIC B", "7203", "VOD.L", "kimpl"):
        assert is_canonical(STK, text, as_written=True)
        assert check_symbol(STK, text, as_written=True) == text
        assert canonical_from_wire(STK, text, as_written=True) == text
    assert normalize(STK, ["ERIC B", "7203"], as_written=True) == ("7203", "ERIC B")
    for bad in ("", " ERIC B", "X:BTCUSD", "A\tB"):
        assert not is_canonical(STK, bad, as_written=True)
        assert canonical_from_wire(STK, bad, as_written=True) is None
        with pytest.raises(ValueError, match="is not a ticker as Massive writes it"):
            check_symbol(STK, bad, as_written=True)
    reader = WireSymbols("partners.tmx.corporate_events", STK, UnknownSymbols.FAIL, as_written=True)
    read = reader.read(pa.array(["ERIC B", None, " x"]))
    assert read.symbols.to_pylist() == ["ERIC B", None, None]
    assert read.unknown == (" x",)


CONVERTERS = [
    ("canonical_symbol_stock", "KIMpL", "KIMpL"),
    ("canonical_symbol_stock_yahoo", "BRK-B", "BRK.B"),
    ("canonical_symbol_stock_nasdaq", "KIM-L", "KIMpL"),
    ("canonical_symbol_stock_cms", "BAC PRL", "BACpL"),
    ("canonical_symbol_option", "AAPL250117C00150000", "O:AAPL250117C00150000"),
    ("canonical_symbol_future", "ESZ5", "ESZ5"),
    ("canonical_symbol_future_spread", "BOB 02-01 U6", "BOB 02-01 U6"),
    ("canonical_symbol_index", "SPX", "I:SPX"),
    ("canonical_symbol_forex", "EUR/USD", "C:EURUSD"),
    ("canonical_symbol_crypto", "BTC-USD", "X:BTCUSD"),
]


@pytest.mark.parametrize("case", CONVERTERS, ids=[c[0] for c in CONVERTERS])
def test_each_converter_runs_in_a_query_string(
    dh_exec_ctx: object, case: tuple[str, str, str]
) -> None:
    # Imported by bare name (here: a local name), the function makes a String column; a null
    # stays null, and a bad row raises the converter's message through Deephaven.
    name, text, expected = case
    convert = getattr(dm, name)
    assert convert.__name__ == name  # the formulas below find it as `convert`
    table = new_table([string_col("P", [text, None])]).update(["S = convert(P)"])
    (column,) = [c for c in table.columns if c.name == "S"]
    assert column.data_type == dtypes.string
    assert [row[0] for row in table.iter_tuple(cols="S")] == [expected, None]
    bad = new_table([string_col("P", ["X:BTC-USD" if "crypto" not in name else "C:EURUSD"])])
    with pytest.raises(DHError, match="ticker \\(prefix"):
        bad.update(["S = convert(P)"])


def test_a_converter_called_through_the_module_needs_a_cast(dh_exec_ctx: object) -> None:
    pairs = new_table([string_col("P", ["EUR/USD"])])
    plain = pairs.update(["S = dm.canonical_symbol_forex(P)"])
    cast_ = pairs.update(["S = (String) dm.canonical_symbol_forex(P)"])
    types = {t: next(c.data_type for c in t.columns if c.name == "S") for t in (plain, cast_)}
    assert types[plain] != dtypes.string  # an Object column
    assert types[cast_] == dtypes.string
    assert [row[0] for row in cast_.iter_tuple(cols="S")] == ["C:EURUSD"]


def test_the_spread_converter_names_surrounding_whitespace() -> None:
    with pytest.raises(
        ValueError, match=r"^' GCQ6-GCZ6 ' has surrounding whitespace; write 'GCQ6-GCZ6'$"
    ):
        dm.canonical_symbol_future_spread(" GCQ6-GCZ6 ")
