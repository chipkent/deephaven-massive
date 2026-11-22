"""Tests for the canonical symbol grammar, its checks, and the converters."""

import copy
import functools
import json
import logging
import re
import typing
from collections.abc import Callable, Sequence

import pyarrow as pa
import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets import symbols
from deephaven_massive._datasets.symbols import (
    ALL,
    AllSymbols,
    SymbolsLike,
    SymbolTable,
    as_written_fault,
    check_symbol,
    check_symbols,
    forex_codes,
    is_canonical,
    is_forex_currency,
    is_futures_contract,
    option_roots,
    refusal,
    strict_converter,
    symbol_class,
)
from deephaven_massive._datasets.wire_symbols import WireSymbols, canonical_from_wire
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError, SymbolConvention
from deephaven_massive._errors import LIBRARY_BUG, BrokenGuaranteeError
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._tables.watchlist import CurrentSymbols, SymbolRowRule, read_symbols

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
    (STK, "AAC.WS"),  # the NYSE warrant forms Massive lists
    (STK, "NE.WS.A"),
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
    (FUT, "0AF704"),  # daily contracts: three digits after the month code, as Massive lists
    (FUT, "0AX627"),
    (FUT, "GCBX602"),
    (FUT, "10YN6-10YQ6"),  # spreads, as Massive's CBOT, COMEX, NYMEX, and CME files write them
    (FUT, "GCQ6-GCZ6"),
    (FUT, "BOB 02-01 U6"),
    (FUT, "CL:BF Q6-U6-V6"),
    (FUT, "CL:BZ U6-V6"),
    (FUT, "6A:XF:AUDUSD:Q6"),
    (IDX, "I:SPX"),
    (IDX, "I:AAVE10RP"),
    (FX, "C:EURUSD"),
    (CRY, "X:BTCUSD"),
    (CRY, "X:1INCHUSDT"),
    (CRY, "X:BTCXYZ"),  # any pair is canonical; a stream needs it in Massive's ticker list
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
    (FUT, "0AF7041"),  # four digits after the month code
    (IDX, "SPX"),
    (IDX, "i:SPX"),
    (IDX, "I:"),
    (IDX, "I:spx"),  # Massive's WebSocket pattern allows lower case; its tickers are upper case
    (IDX, "I:SPx"),
    (FX, "C:EURUS1"),
    (FX, "C:EUR/USD"),
    (FX, "EURUSD"),
    (CRY, "X:BTC-USD"),
    (CRY, "X:"),
    (CRY, "x:BTCUSD"),
    (CRY, "X:BTC"),  # one currency, not a pair
]


FUTURES_FORMS = (
    "A contract looks like 'ESZ5' or 'NGZ26' and a spread like 'GCQ6-GCZ6', in upper case. Pass "
    "the ticker as m.futures.contracts() lists it."
)
"""The sentences after the first of every refusal of text that is no futures ticker."""


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("esz5", f"'esz5' is not a Massive futures ticker. {FUTURES_FORMS}"),
        ("ESZ", f"'ESZ' is not a Massive futures ticker. {FUTURES_FORMS}"),
        ("hello world", f"'hello world' is not a Massive futures ticker. {FUTURES_FORMS}"),
        ("GC  Z6", f"'GC  Z6' is not a Massive futures ticker. {FUTURES_FORMS}"),
        (" ESZ5 ", "' ESZ5 ' has surrounding whitespace. Pass 'ESZ5'."),
        (
            "X:ESZ5",
            (
                "'X:ESZ5' is a crypto ticker, not a futures one. "
                "Call dm.canonical_symbol_crypto('X:ESZ5')."
            ),
        ),
    ],
)
def test_the_general_converter_is_as_strict_for_futures_as_the_contract_one(
    text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$") as general:
        dm.canonical_symbol(text, FUT)
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$") as contract:
        dm.canonical_symbol_future(text)
    assert str(general.value) == str(contract.value)


@pytest.mark.parametrize("text", ["esz5", "ESZ", "hello world", "ESZ5-", "GC  Z6", "GCQ6--GCZ6"])
def test_a_dataset_refuses_futures_text_the_converter_refuses(text: str) -> None:
    # A dataset takes a contract or a combo, the forms dm.canonical_symbol checks, and refuses
    # everything else with the converter's reason: lower case, a contract with no year, doubled
    # or dangling separators, another class's prefix.
    with pytest.raises(ValueError, match="is not a Massive futures ticker") as converter:
        dm.canonical_symbol(text, FUT)
    with pytest.raises(ValueError, match="is not a Massive futures ticker") as dataset:
        check_symbol(FUT, text)
    assert str(dataset.value) == str(converter.value)
    assert not is_canonical(FUT, text)


@pytest.mark.parametrize("text", [" ESZ5", "ESZ5 "])
def test_a_futures_ticker_with_surrounding_whitespace_is_refused(text: str) -> None:
    message = f"{text!r} has surrounding whitespace. Pass 'ESZ5'."
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        check_symbol(FUT, text)


def test_every_futures_ticker_massive_writes_round_trips(
    load_fixture: Callable[[str, str], bytes], dh_exec_ctx: object
) -> None:
    # Every ticker of the vendored samples and of the live evidence (combo strips, packs, and
    # bundles included) is taken as symbols= input, as a symbol-table row, and from a row, as
    # written; so is a combo form made up here, since combo forms are not enumerated.
    tickers = json.loads(load_fixture("datasets/symbols", "futures_tickers.json"))
    every = [*tickers["vendor_samples"], *tickers["live_evidence"], "SR3:PK H7"]
    assert check_symbols(FUT, every) == tuple(sorted(set(every)))
    rule = SymbolRowRule("futures.trades", FUT, as_written=False, noncanonical=OnError.RAISE)
    rows = read_symbols(rule, new_table([string_col("Symbol", every)]), stream_id=None)
    assert rows == CurrentSymbols(tuple(sorted(set(every))))
    for text in every:
        assert canonical_from_wire(FUT, text) == text
        assert dm.canonical_symbol(text, FUT) == text


@pytest.mark.parametrize("text", ["0AF704", "0AF729", "0AX602", "0AX627", "GCBX602"])
def test_a_daily_futures_contract_massive_lists_is_a_single_contract(text: str) -> None:
    # Massive's active contracts list writes these daily singles with three digits after the
    # month code; each is a contract, so the calendar looks it up, and no combo.
    assert is_futures_contract(text)
    assert dm.canonical_symbol_future(text) == text
    with pytest.raises(ValueError, match=r"is a single futures contract, not a combo"):
        dm.canonical_symbol_future_spread(text)


@pytest.mark.parametrize(
    "text",
    [
        # Every crypto ticker the vendored documentation writes, and the short pairs the
        # library's other tests use; a pair needs a base and a three-letter quote.
        *("X:ADAUSD", "X:ALGOUSD", "X:ARDRUSD", "X:BOBAUSD", "X:BTCEUR", "X:BTCGBP"),
        *("X:BTCUSD", "X:DRNUSD", "X:ETHBTC", "X:FSNUSD", "X:LCXUSD", "X:LINKUSD"),
        *("X:LTCUSD", "X:MOBUSD", "X:NGCUSD", "X:RNDRUSD", "X:SOLUSD", "X:ZSCUSD"),
        *("X:BTC-USD", "BCD-USD", "BTC-USD", "USD/EUR", "X:EUR-USD", "B-USD"),
    ],
)
def test_every_crypto_pair_massive_writes_is_canonical(text: str) -> None:
    symbol = dm.canonical_symbol_crypto(text)
    assert symbol is not None
    assert is_canonical(CRY, symbol)


@pytest.mark.parametrize(("asset_class", "text"), CANONICAL)
def test_canonical_text_is_canonical_and_converts_to_itself(
    asset_class: AssetClass, text: str
) -> None:
    assert is_canonical(asset_class, text)
    assert dm.canonical_symbol(text, asset_class) == text
    assert check_symbol(asset_class, text) == text
    assert canonical_from_wire(asset_class, text) == text
    if asset_class is STK:  # the converter Deephaven query strings call agrees
        assert dm.canonical_symbol_stock(text) == text


@pytest.mark.parametrize(("asset_class", "text"), NOT_CANONICAL)
def test_other_text_is_not_canonical(asset_class: AssetClass, text: str) -> None:
    assert not is_canonical(asset_class, text)
    with pytest.raises(ValueError, match=r"Massive|ticker|surrounding whitespace|OCC option|pair"):
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
    assert is_canonical(ClassFromPrefix.ANY, text) is expected


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
    (dm.canonical_symbol_future, "0AF704", "0AF704"),
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


CONVENTIONS = [SymbolConvention.YAHOO, SymbolConvention.NASDAQ, SymbolConvention.CMS]


@pytest.mark.parametrize("convention", CONVENTIONS)
@pytest.mark.parametrize(
    ("text", "message"),
    [
        (" ABC ", r"^' ABC ' has surrounding whitespace\. Pass 'ABC'\.$"),
        ("", r"^'' is empty, not a ticker\.$"),
        ("  ", r"^'  ' is only whitespace, not a ticker\.$"),
        (
            "abc",
            (
                r"^'abc' is not a {name} ticker\. The {name} convention writes tickers in upper "
                r"case\. If you mean 'ABC', pass 'ABC'\.$"
            ),
        ),
    ],
)
def test_a_convention_names_whitespace_emptiness_and_case(
    convention: SymbolConvention, text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message.format(name=convention.name)):
        dm.canonical_symbol(text, STK, convention=convention)


def test_a_convention_refuses_another_class_prefix() -> None:
    with pytest.raises(ValueError, match="is a crypto ticker"):
        dm.canonical_symbol("X:BTC-USD", STK, convention=SymbolConvention.YAHOO)


def test_a_stock_convention_applies_to_stocks_only() -> None:
    for convention in (SymbolConvention.YAHOO, SymbolConvention.NASDAQ, SymbolConvention.CMS):
        message = (
            f"convention=dm.SymbolConvention.{convention.name} is for stock tickers, not forex. "
            "Pass asset_class=dm.AssetClass.STOCKS, or leave convention out."
        )
        with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
            dm.canonical_symbol("EUR/USD", FX, convention=convention)
    # The advice works: without the convention, the forex pair converts.
    assert dm.canonical_symbol("EUR/USD", FX) == "C:EURUSD"


def test_the_massive_convention_is_the_default_for_every_class() -> None:
    # Massive's own spelling: canonical text unchanged, and a class's unambiguous spellings.
    massive = SymbolConvention.MASSIVE
    assert dm.canonical_symbol("EUR/USD", FX, convention=massive) == "C:EURUSD"
    assert dm.canonical_symbol("EUR/USD", FX) == "C:EURUSD"
    assert dm.canonical_symbol("KIMpL", STK, convention=massive) == "KIMpL"
    assert dm.canonical_symbol("SPX", IDX, convention=massive) == "I:SPX"
    with pytest.raises(ValueError, match=re.escape(stock_conventions_fix("BRK-B"))):
        dm.canonical_symbol("BRK-B", STK, convention=massive)  # Yahoo's or Nasdaq's dash
    # The call the refusal suggests converts it.
    assert (
        dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO)
        == "BRK.B"
    )


def test_converter_enums_reject_text() -> None:
    with pytest.raises(TypeError, match=r"^asset_class must be dm\.AssetClass"):
        dm.canonical_symbol("AAPL", "stocks")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonical_symbol("BRK-B", STK, convention="yahoo")  # type: ignore[arg-type]  # the point: refused at run time
    # No convention is not a default: SymbolConvention.MASSIVE names Massive's own spelling.
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonical_symbol("AAPL", STK, convention=None)  # type: ignore[arg-type]  # the point: refused at run time


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


def stock_conventions_fix(text: str) -> str:
    # The fix that ends the refusal of a stock ticker with another convention's separator.
    return (
        f"If it is Yahoo's spelling, convert it with dm.canonical_symbol({text!r}, "
        "dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO), or name "
        "dm.SymbolConvention.NASDAQ or dm.SymbolConvention.CMS for those spellings."
    )


STOCK_FORMS = (
    "is not a Massive stock ticker, which looks like 'AAPL', 'BRK.B', or 'KIMpL'. Pass the "
    "ticker as m.reference.tickers() lists it."
)
"""The refusal of text that is no stock ticker in any form the library names, after the text."""


@pytest.mark.parametrize(
    ("convert", "text", "message"),
    [
        (
            dm.canonical_symbol_stock,
            "BRK-B",
            f"'BRK-B' is not a Massive stock ticker. {stock_conventions_fix('BRK-B')}",
        ),
        (
            dm.canonical_symbol_stock,
            "BRK/B",
            f"'BRK/B' is not a Massive stock ticker. {stock_conventions_fix('BRK/B')}",
        ),
        (
            dm.canonical_symbol_stock,
            "BRK B",
            f"'BRK B' is not a Massive stock ticker. {stock_conventions_fix('BRK B')}",
        ),
        (
            dm.canonical_symbol_stock,
            "kimpl",
            (
                "'kimpl' is not a Massive stock ticker, because Massive's tickers are "
                "case-sensitive. If you mean 'KIMPL', pass 'KIMPL'."
            ),
        ),
        (dm.canonical_symbol_stock, " AAPL ", "' AAPL ' has surrounding whitespace. Pass 'AAPL'."),
        (dm.canonical_symbol_stock, "Aapl", f"'Aapl' {STOCK_FORMS}"),
        (dm.canonical_symbol_stock, "", f"'' {STOCK_FORMS}"),
        (
            dm.canonical_symbol_stock,
            "C:EURUSD",
            (
                "'C:EURUSD' is a forex ticker, not a stock one. "
                "Call dm.canonical_symbol_forex('C:EURUSD')."
            ),
        ),
        (
            dm.canonical_symbol_crypto,
            "C:EURUSD",
            (
                "'C:EURUSD' is a forex ticker, not a crypto one. "
                "Call dm.canonical_symbol_forex('C:EURUSD')."
            ),
        ),
        (
            dm.canonical_symbol_forex,
            "X:BTCUSD",
            (
                "'X:BTCUSD' is a crypto ticker, not a forex one. "
                "Call dm.canonical_symbol_crypto('X:BTCUSD')."
            ),
        ),
        (
            dm.canonical_symbol_stock,
            "o:AAPL250117C00150000",
            (
                "'o:AAPL250117C00150000' is an option ticker, not a stock one. "
                "Call dm.canonical_symbol_option('o:AAPL250117C00150000')."
            ),
        ),
        (
            dm.canonical_symbol_option,
            "I:SPX",
            (
                "'I:SPX' is an index ticker, not an option one. "
                "Call dm.canonical_symbol_index('I:SPX')."
            ),
        ),
        (
            dm.canonical_symbol_option,
            "AAPL",
            (
                "'AAPL' is an underlying, not an OCC option symbol such as "
                "'O:AAPL250117C00150000'. "
                "Pass a contract's symbol from m.options.contracts('AAPL')."
            ),
        ),
        (
            dm.canonical_symbol_option,
            "O:AAPL250132C00150000",
            (
                "'O:AAPL250132C00150000' has the shape of an OCC option symbol, but its expiry "
                "250132, read as YYMMDD, is not a real date. Pass the symbol as "
                "m.options.contracts() lists it."
            ),
        ),
        (
            dm.canonical_symbol_option,
            "AAPL250230C00150000",
            (
                "'AAPL250230C00150000' has the shape of an OCC option symbol, but its expiry "
                "250230, read as YYMMDD, is not a real date. Pass the symbol as "
                "m.options.contracts() lists it."
            ),
        ),
        (
            dm.canonical_symbol_option,
            "BRKB 250117C00400000",
            (
                "'BRKB 250117C00400000' is not an OCC option symbol, which looks like "
                "'O:AAPL250117C00150000'. Pass the symbol as m.options.contracts() lists it."
            ),
        ),
        (dm.canonical_symbol_future, " ESZ5 ", "' ESZ5 ' has surrounding whitespace. Pass 'ESZ5'."),
        (dm.canonical_symbol_future, "", f"'' is not a Massive futures ticker. {FUTURES_FORMS}"),
        (
            dm.canonical_symbol_future,
            "esz5",
            f"'esz5' is not a Massive futures ticker. {FUTURES_FORMS}",
        ),
        (
            dm.canonical_symbol_future,
            "ESZ",
            f"'ESZ' is not a Massive futures ticker. {FUTURES_FORMS}",
        ),
        (
            dm.canonical_symbol_future,
            "GCQ6-GCZ6",
            (
                "'GCQ6-GCZ6' is a futures combo, such as a spread, not a single contract. "
                "Call dm.canonical_symbol_future_spread('GCQ6-GCZ6')."
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "ESZ5",
            (
                "'ESZ5' is a single futures contract, not a combo. "
                "Call dm.canonical_symbol_future('ESZ5')."
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "GCQ6--GCZ6",
            (
                "'GCQ6--GCZ6' is not a Massive futures combo ticker, which looks like 'GCQ6-GCZ6', "
                "'BOB 02-01 U6', or 'CL:BF Q6-U6-V6'. Pass the ticker as m.futures.contracts() "
                "lists it."
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "X:BTCUSD",
            (
                "'X:BTCUSD' is a crypto ticker, not a futures one. "
                "Call dm.canonical_symbol_crypto('X:BTCUSD')."
            ),
        ),
        (
            dm.canonical_symbol_future,
            "X:ESZ5",
            (
                "'X:ESZ5' is a crypto ticker, not a futures one. "
                "Call dm.canonical_symbol_crypto('X:ESZ5')."
            ),
        ),
        (
            dm.canonical_symbol_index,
            "S P X",
            (
                "'S P X' is not a Massive index ticker, which looks like 'I:SPX'. Pass the ticker "
                "as "
                "m.reference.tickers(market=dm.Market.INDICES) lists it."
            ),
        ),
        (
            dm.canonical_symbol_index,
            "spx",
            (
                "'spx' is not a Massive index ticker. Index tickers are upper case. If you mean "
                "'I:SPX', pass 'I:SPX'."
            ),
        ),
        (
            dm.canonical_symbol_index,
            "C:EURUSD",
            (
                "'C:EURUSD' is a forex ticker, not an index one. "
                "Call dm.canonical_symbol_forex('C:EURUSD')."
            ),
        ),
        *(
            (
                dm.canonical_symbol_forex,
                pair,
                (
                    f"{pair!r} is not a forex pair of two three-letter currency codes. Write it as "
                    "BASE/QUOTE, such as 'EUR/USD'."
                ),
            )
            for pair in ("EU1/USD", "EURO/USD", "EUR/USD/JPY", "EUR USD")
        ),
        *(
            (
                dm.canonical_symbol_crypto,
                pair,
                f"{pair!r} is not a crypto pair. Write it as BASE-QUOTE, such as 'BTC-USD'.",
            )
            for pair in ("BTC.USD", "BTC--USD")
        ),
        *(
            (
                dm.canonical_symbol_crypto,
                pair,
                (
                    f"{pair!r} is too short to hold a base currency and a quote currency. "
                    "Write the "
                    "pair as BASE-QUOTE, such as 'BTC-USD'."
                ),
            )
            for pair in ("BTC", "X:ETH")
        ),
    ],
)
def test_a_converter_refuses_with_a_reason(
    convert: Callable[[str], str | None], text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        convert(text)


@pytest.mark.parametrize(
    ("convert", "text", "expected"),
    [
        # Each call or value a refusal above names, run on the case it describes.
        (functools.partial(dm.canonical_symbol_stock_yahoo), "BRK-B", "BRK.B"),
        (dm.canonical_symbol_stock, "KIMPL", "KIMPL"),
        (dm.canonical_symbol_stock, "AAPL", "AAPL"),
        (dm.canonical_symbol_forex, "C:EURUSD", "C:EURUSD"),
        (dm.canonical_symbol_crypto, "X:BTCUSD", "X:BTCUSD"),
        (dm.canonical_symbol_option, "o:AAPL250117C00150000", "O:AAPL250117C00150000"),
        (dm.canonical_symbol_index, "I:SPX", "I:SPX"),
        (dm.canonical_symbol_future, "ESZ5", "ESZ5"),
        (dm.canonical_symbol_future_spread, "GCQ6-GCZ6", "GCQ6-GCZ6"),
        (dm.canonical_symbol_crypto, "X:ESZ5", "X:ESZ5"),
        (dm.canonical_symbol_forex, "EUR/USD", "C:EURUSD"),
        (dm.canonical_symbol_crypto, "BTC-USD", "X:BTCUSD"),
    ],
)
def test_the_fix_a_refusal_names_converts(
    convert: Callable[[str], str | None], text: str, expected: str
) -> None:
    assert convert(text) == expected


@pytest.mark.parametrize("text", ["KIMpL", "KIMr", "KIMw", "AAPL"])
def test_a_stock_ticker_with_its_lower_case_marker_is_canonical(text: str) -> None:
    # The refusal of all lower-case text says Massive's tickers are case-sensitive: a
    # preferred's `p`, rights' `r`, and when-issued `w` pass as Massive writes them.
    assert check_symbol(STK, text) == text


@pytest.fixture
def fresh_converter_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    # The scalar converters' logs remember each value for the process: start with none seen.
    for name, level in (("_logged_values", logging.WARNING), ("_ignored_values", logging.DEBUG)):
        monkeypatch.setattr(symbols, name, BoundedLog(symbols.logger, limit=3, level=level))


@pytest.mark.usefixtures("fresh_converter_logs")
@pytest.mark.parametrize(
    ("convert", "text", "caller"),
    [
        # The record names the function that judged the text: `canonical_symbol` for every
        # class and convention function that converts through it, and each futures form's own.
        (dm.canonical_symbol_stock, "BRK-B", "canonical_symbol"),
        (dm.canonical_symbol_future, "esz5", "canonical_symbol_future"),
        (dm.canonical_symbol_future_spread, "ESZ5", "canonical_symbol_future_spread"),
        (dm.canonical_symbol_forex, "EURUSD=X", "canonical_symbol"),
        (
            functools.partial(dm.canonical_symbol, asset_class=AssetClass.CRYPTO),
            "BTC",
            "canonical_symbol",
        ),
    ],
)
def test_a_converter_raises_by_default_and_returns_none_under_log_and_ignore(
    caplog: pytest.LogCaptureFixture, *, convert: Callable[..., str | None], text: str, caller: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(text)) as refused:
        convert(text)
    with pytest.raises(ValueError, match=re.escape(str(refused.value))):
        convert(text, on_error=OnError.RAISE)
    with caplog.at_level(logging.DEBUG, logger=symbols.logger.name):
        assert convert(text, on_error=OnError.LOG) is None
        assert convert(text, on_error=OnError.IGNORE) is None
        assert convert(None, on_error=OnError.LOG) is None
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, f"{caller}: {text!r} becomes None. {refused.value}"),
        (logging.DEBUG, f"{caller}: {text!r} becomes None. {refused.value}"),
    ]


@pytest.mark.usefixtures("fresh_converter_logs")
def test_a_converter_log_record_names_the_value_its_result_and_the_fix(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger=symbols.logger.name):
        assert dm.canonical_symbol_stock("BRK-B", on_error=OnError.LOG) is None
    assert caplog.messages == [
        (
            "canonical_symbol: 'BRK-B' becomes None. 'BRK-B' is not a Massive stock ticker. If it "
            "is Yahoo's spelling, convert it with dm.canonical_symbol('BRK-B', "
            "dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO), or name "
            "dm.SymbolConvention.NASDAQ or dm.SymbolConvention.CMS for those spellings."
        )
    ]


@pytest.mark.usefixtures("fresh_converter_logs")
def test_a_converter_logs_each_value_once_per_process_up_to_a_limit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger=symbols.logger.name):
        for text in ("b1", "b2", "b1", "b3", "b4"):
            assert dm.canonical_symbol_forex(text, on_error=OnError.LOG) is None
    assert [m[:30] for m in caplog.messages] == [
        "canonical_symbol: 'b1' becomes",
        "canonical_symbol: 'b2' becomes",
        "canonical_symbol: 'b3' becomes",
        "3 values were reported. Furthe",  # the limit: b4 is not logged
    ]


@pytest.mark.parametrize(
    "convert",
    [
        functools.partial(dm.canonical_symbol, asset_class=AssetClass.STOCKS),
        dm.canonical_symbol_crypto,
        dm.canonical_symbol_forex,
        dm.canonical_symbol_future,
        dm.canonical_symbol_future_spread,
        dm.canonical_symbol_index,
        dm.canonical_symbol_option,
        dm.canonical_symbol_stock,
        dm.canonical_symbol_stock_cms,
        dm.canonical_symbol_stock_nasdaq,
        dm.canonical_symbol_stock_yahoo,
    ],
)
def test_a_converter_refuses_an_on_error_that_is_no_member(convert: Callable[..., object]) -> None:
    with pytest.raises(TypeError, match=r"^on_error must be dm\.OnError"):
        convert("AAPL", on_error="log")


@pytest.mark.parametrize("on_error", list(OnError))
@pytest.mark.parametrize(("asset_class", "text"), NOT_CANONICAL)
def test_a_scalar_and_a_table_cell_refuse_a_value_the_same_way(
    dh_exec_ctx: object,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    *,
    asset_class: AssetClass,
    text: str,
    on_error: OnError,
) -> None:
    # The scalar converter and `canonicalize_symbols` share one verdict and one effect: for
    # every text that is not canonical and every setting, the scalar's result or error and the
    # cell's are the same, and so are the reason text and the log level.
    monkeypatch.setattr(
        symbols, "_logged_values", BoundedLog(symbols.logger, limit=50, level=logging.WARNING)
    )
    monkeypatch.setattr(
        symbols, "_ignored_values", BoundedLog(symbols.logger, limit=50, level=logging.DEBUG)
    )
    table = new_table([string_col("Symbol", [text])])
    reason = refusal(text, strict_converter(asset_class, SymbolConvention.MASSIVE))
    table_logger = "deephaven_massive._tables.symbols"
    if reason is None:  # not canonical, but a spelling the class converts: no record
        converted = dm.canonical_symbol(text, asset_class, on_error=on_error)
        cells = dm.canonicalize_symbols(table, asset_class, on_error=on_error)
        assert [row[0] for row in cells.iter_tuple(cols="Symbol")] == [converted]
        assert converted is not None
        return
    if on_error is OnError.RAISE:
        with pytest.raises(ValueError, match=f"^{re.escape(reason)}$"):
            dm.canonical_symbol(text, asset_class, on_error=on_error)
        with pytest.raises(ValueError, match=re.escape(reason)):
            dm.canonicalize_symbols(table, asset_class, on_error=on_error)
        return
    with (
        caplog.at_level(logging.DEBUG, logger=symbols.logger.name),
        caplog.at_level(logging.DEBUG, logger=table_logger),
    ):
        assert dm.canonical_symbol(text, asset_class, on_error=on_error) is None
        cells = dm.canonicalize_symbols(table, asset_class, on_error=on_error)
    assert [row[0] for row in cells.iter_tuple(cols="Symbol")] == [None]
    level = logging.WARNING if on_error is OnError.LOG else logging.DEBUG
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (level, f"canonical_symbol: {text!r} becomes None. {reason}"),
        (level, f"canonicalize_symbols: {text!r} becomes a null Symbol. {reason}"),
    ]


def test_no_conversion_looks_a_name_up() -> None:
    # The index prefix comes from the class the caller or the endpoint names, never from the
    # ticker: a made-up name converts like a real one, and a bare name is a stock elsewhere.
    for name in ("SPX", "ZZQX", "AAPL"):
        assert dm.canonical_symbol_index(name) == f"I:{name}"
        assert canonical_from_wire(IDX, name) == f"I:{name}"
        assert canonical_from_wire(ClassFromPrefix.ANY, name) == name
        assert canonical_from_wire(STK, name) == name
        message = (
            f"{name!r} is not Massive's index ticker. Pass 'I:{name}', which "
            f"dm.canonical_symbol_index({name!r}) returns."
        )
        with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
            check_symbol(IDX, name)
        assert check_symbol(IDX, f"I:{name}") == f"I:{name}"


def test_check_symbol_names_the_fix() -> None:
    forex = (
        "'EUR/USD' is not Massive's forex ticker. Pass 'C:EURUSD', which "
        "dm.canonical_symbol_forex('EUR/USD') returns."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(forex)}$"):
        check_symbol(FX, "EUR/USD")
    assert dm.canonical_symbol_forex("EUR/USD") == "C:EURUSD"
    assert check_symbol(FX, "C:EURUSD") == "C:EURUSD"
    # Text the converter refuses gets the converter's reason and fix, unchanged.
    lower = (
        "'aapl' is not a Massive stock ticker, because Massive's tickers are case-sensitive. "
        "If you mean 'AAPL', pass 'AAPL'."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(lower)}$"):
        check_symbol(STK, "aapl")
    assert check_symbol(STK, "AAPL") == "AAPL"
    # A canonical ticker of another class names the class the dataset takes, not a converter.
    crypto = (
        "'X:BTCUSD' is a crypto ticker, and this dataset takes stock tickers. Pass a stock "
        "ticker, or read crypto data through m.crypto."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(crypto)}$"):
        check_symbol(STK, "X:BTCUSD")
    index = (
        "'I:SPX' is an index ticker, and this dataset takes stock tickers. Pass a stock "
        "ticker, or read index data through m.indices."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(index)}$"):
        check_symbol(STK, "I:SPX")
    assert check_symbol(IDX, "I:SPX") == "I:SPX"
    assert check_symbol(CRY, "X:BTCUSD") == "X:BTCUSD"
    # Not canonical crypto either: the converter's reason.
    other = (
        "'X:BTC-USD' is a crypto ticker, not a stock one. "
        "Call dm.canonical_symbol_crypto('X:BTC-USD')."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(other)}$"):
        check_symbol(STK, "X:BTC-USD")
    with pytest.raises(TypeError, match=r"^a symbol must be a str, not int$"):
        check_symbol(STK, 5)


def test_forex_currencies_are_three_letters() -> None:
    assert is_forex_currency("EUR")
    for code in ("EU1", "EU", "EURO", "eur", "1NC"):
        assert not is_forex_currency(code)


def test_forex_codes_split_a_forex_ticker_by_position() -> None:
    assert forex_codes("C:EURUSD") == ("EUR", "USD")
    assert forex_codes("C:USDJPY") == ("USD", "JPY")


def test_option_roots() -> None:
    roots = option_roots(
        pa.array(["O:SPXW251219C06000000", "O:AAPL1250117C00150000", "O:F250117P00010000", None])
    )
    assert roots.to_pylist() == ["SPXW", "AAPL1", "F", None]


def test_check_symbols_sorts_and_deduplicates_canonical_symbols() -> None:
    assert check_symbols(STK, ["MSFT", "AAPL", "MSFT"]) == ("AAPL", "MSFT")
    assert check_symbols(STK, "KIMpL") == ("KIMpL",)
    assert check_symbols(ClassFromPrefix.ANY, ["X:BTCUSD", "I:SPX", "AAPL"]) == (
        "AAPL",
        "I:SPX",
        "X:BTCUSD",
    )


@pytest.mark.parametrize(
    ("asset_class", "value"),
    [
        (STK, "aapl"),
        (FX, ["C:EURUSD", "EUR/USD"]),
        (IDX, "SPX"),
        (ClassFromPrefix.ANY, ["X:BTC-USD"]),
    ],
)
def test_check_symbols_refuses_what_is_not_canonical(
    asset_class: AssetClass | ClassFromPrefix, value: str | list[str]
) -> None:
    with pytest.raises(ValueError, match=r"is not (a Massive|Massive's)|is a crypto ticker"):
        check_symbols(asset_class, value)


def test_check_symbols_materializes_generators() -> None:
    assert check_symbols(STK, (s for s in ["B", "A"])) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    items = (s for s in ["A"])
    list(items)
    message = (
        "symbols holds no tickers. Pass at least one ticker, or dm.ALL for every ticker where "
        "the dataset takes it."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        check_symbols(STK, items)
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        check_symbols(STK, [])
    assert check_symbols(STK, ALL) is ALL


def test_check_symbols_all_passes_through() -> None:
    assert check_symbols(STK, dm.ALL) is dm.ALL


def test_check_symbols_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(
        TypeError,
        match=r"^symbols must be a str, a sequence of str, a Deephaven Table, or dm\.ALL, not int$",
    ):
        check_symbols(STK, 5)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="must be a str"):
        check_symbols(STK, ["AAPL", 5])  # type: ignore[list-item]  # the point: refused at run time


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


def test_a_dataset_kept_as_written_takes_any_ticker_text() -> None:
    for text in ("ERIC B", "7203", "VOD.L", "kimpl"):
        assert is_canonical(STK, text, as_written=True)
        assert check_symbol(STK, text, as_written=True) == text
        assert canonical_from_wire(STK, text, as_written=True) == text
    assert check_symbols(STK, ["ERIC B", "7203"], as_written=True) == ("7203", "ERIC B")
    faults = {
        "": "'' is empty",
        " ERIC B": "' ERIC B' has surrounding whitespace",
        "ERIC B ": "'ERIC B ' has surrounding whitespace",
        "X:BTCUSD": "'X:BTCUSD' starts with the class prefix 'X:'",
        "A\tB": "'A\\tB' holds a control character",
        "\tAB": "'\\tAB' holds a control character",
    }
    for bad, fault in faults.items():
        assert not is_canonical(STK, bad, as_written=True)
        assert canonical_from_wire(STK, bad, as_written=True) is None
        message = (
            f"{fault}, and Massive never writes a ticker that way. Pass the value as Massive "
            "lists it."
        )
        with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
            check_symbol(STK, bad, as_written=True)
    # A text the grammar takes breaks no rule: asking for its fault is a defect of the caller.
    with pytest.raises(BrokenGuaranteeError, match=re.escape(LIBRARY_BUG)):
        as_written_fault("ERIC B")
    reader = WireSymbols("partners.tmx.corporate_events", STK, as_written=True)
    read = reader.read(pa.array(["ERIC B", None, " x"]), {})
    assert read.symbols.to_pylist() == ["ERIC B", None, None]
    assert read.unreadable_rows.to_pylist() == [False, False, True]


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
    with pytest.raises(DHError, match=r"is a (crypto|forex) ticker, not an? \w+ one\. Call dm\."):
        bad.update(["S = convert(P)"])
    # A partial that binds on_error, in scope by a bare name, still makes a String column, and
    # turns the bad row into a null instead of failing the formula.
    lenient = functools.partial(convert, on_error=OnError.IGNORE)
    assert lenient.keywords == {"on_error": OnError.IGNORE}  # the formula finds it as `lenient`
    nulled = bad.update(["S = lenient(P)"])
    (column,) = [c for c in nulled.columns if c.name == "S"]
    assert column.data_type == dtypes.string
    assert [row[0] for row in nulled.iter_tuple(cols="S")] == [None]


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
        ValueError, match=r"^' GCQ6-GCZ6 ' has surrounding whitespace\. Pass 'GCQ6-GCZ6'\.$"
    ):
        dm.canonical_symbol_future_spread(" GCQ6-GCZ6 ")


def test_yahoo_forex_spelling_is_refused_with_massive_form() -> None:
    message = (
        "'EURUSD=X' is Yahoo Finance's spelling of a forex pair, which the library does not "
        "read. Write the pair as 'EUR/USD', 'EUR-USD', or 'EURUSD', or pass Massive's ticker, "
        "such as 'C:EURUSD'."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        dm.canonical_symbol_forex("EURUSD=X")
    with pytest.raises(ValueError, match="Yahoo Finance"):
        dm.canonical_symbol_forex("eurusd=x")
    for spelling in ("EUR/USD", "EUR-USD", "EURUSD", "C:EURUSD"):
        assert dm.canonical_symbol_forex(spelling) == "C:EURUSD"


def test_all_is_the_exported_singleton() -> None:
    assert dm.ALL is ALL
    assert dm.AllSymbols is AllSymbols
    assert AllSymbols("ALL") is AllSymbols.ALL is ALL
    assert copy.copy(ALL) is ALL
    assert copy.deepcopy(ALL) is ALL
    assert not isinstance(ALL, str)


def test_all_shows_its_public_name() -> None:
    assert repr(ALL) == "dm.ALL"
    assert str(ALL) == "ALL"
    assert f"{ALL}" == "ALL"


def test_symbols_like_names_every_form_symbols_takes() -> None:
    assert set(typing.get_args(SymbolsLike)) == {str, Sequence[str], Table, AllSymbols}


def test_all_is_an_instance_of_the_symbols_like_member() -> None:
    assert isinstance(ALL, AllSymbols)
