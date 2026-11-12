"""Tests for the canonical symbol grammar, its checks, and the converters."""

import copy
import json
import re
import typing
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.symbols import (
    ALL,
    AllSymbols,
    SymbolsLike,
    SymbolTable,
    check_symbol,
    check_symbols,
    forex_codes,
    is_canonical,
    is_forex_currency,
    option_roots,
    symbol_class,
)
from deephaven_massive._datasets.wire_symbols import WireSymbols, canonical_from_wire
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError, SymbolConvention
from deephaven_massive._tables.watchlist import CurrentSymbols, SymbolRowRule, read_symbols

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
    (FUT, "10YN6-10YQ6"),  # spreads, as Massive's CBOT, COMEX, NYMEX, and CME files write them
    (FUT, "GCQ6-GCZ6"),
    (FUT, "BOB 02-01 U6"),
    (FUT, "CL:BF Q6-U6-V6"),
    (FUT, "CL:BZ U6-V6"),
    (FUT, "6A:XF:AUDUSD:Q6"),
    (IDX, "I:SPX"),
    (IDX, "I:AAVE10RP"),
    (IDX, "I:APy"),
    (IDX, "I:spx"),  # Massive's pattern allows lower case
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
    (IDX, "SPX"),
    (IDX, "i:SPX"),
    (IDX, "I:"),
    (FX, "C:EURUS1"),
    (FX, "C:EUR/USD"),
    (FX, "EURUSD"),
    (CRY, "X:BTC-USD"),
    (CRY, "X:"),
    (CRY, "x:BTCUSD"),
    (CRY, "X:BTC"),  # one currency, not a pair
]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("esz5", r"^'esz5' is not a Massive futures ticker: a contract is a root of upper-case"),
        ("ESZ", r"^'ESZ' is not a Massive futures ticker: a contract is"),
        ("hello world", r"^'hello world' is not a Massive futures ticker: a contract is"),
        ("GC  Z6", r"^'GC  Z6' is not a Massive futures ticker: a contract is"),
        (" ESZ5 ", r"^' ESZ5 ' has surrounding whitespace; write"),
        ("X:ESZ5", "is a crypto ticker"),
    ],
)
def test_the_general_converter_is_as_strict_for_futures_as_the_contract_one(
    text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message) as general:
        dm.canonical_symbol(text, FUT)
    with pytest.raises(ValueError, match=message) as contract:
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
    assert str(dataset.value) == f"{converter.value}; symbols must be Massive's exact tickers"
    assert not is_canonical(FUT, text)


@pytest.mark.parametrize("text", [" ESZ5", "ESZ5 "])
def test_a_futures_ticker_with_surrounding_whitespace_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match="has surrounding whitespace; write 'ESZ5'"):
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


def test_a_stock_convention_applies_to_stocks_only() -> None:
    for convention in (SymbolConvention.YAHOO, SymbolConvention.NASDAQ, SymbolConvention.CMS):
        message = rf"^the {convention.name} convention applies to stocks only, not forex$"
        with pytest.raises(ValueError, match=message):
            dm.canonical_symbol("EUR/USD", FX, convention=convention)


def test_the_massive_convention_is_the_default_for_every_class() -> None:
    # Massive's own spelling: canonical text unchanged, and a class's unambiguous spellings.
    massive = SymbolConvention.MASSIVE
    assert dm.canonical_symbol("EUR/USD", FX, convention=massive) == "C:EURUSD"
    assert dm.canonical_symbol("EUR/USD", FX) == "C:EURUSD"
    assert dm.canonical_symbol("KIMpL", STK, convention=massive) == "KIMpL"
    assert dm.canonical_symbol("SPX", IDX, convention=massive) == "I:SPX"
    with pytest.raises(ValueError, match="another convention"):
        dm.canonical_symbol("BRK-B", STK, convention=massive)  # Yahoo's or Nasdaq's dash


def test_converter_enums_reject_text() -> None:
    with pytest.raises(TypeError, match=r"^asset_class must be dm\.AssetClass"):
        dm.canonical_symbol("AAPL", cast("AssetClass", "stocks"))
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonical_symbol("BRK-B", STK, convention=cast("SymbolConvention", "yahoo"))
    # No convention is not a default: SymbolConvention.MASSIVE names Massive's own spelling.
    with pytest.raises(TypeError, match=r"^convention must be dm\.SymbolConvention"):
        dm.canonical_symbol("AAPL", STK, convention=cast("SymbolConvention", None))


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
                r"write Massive's ticker exactly: use 'KIMPL' if that is the ticker you mean$"
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
        (dm.canonical_symbol_future, "esz5", "a contract is a root of upper-case letters"),
        (dm.canonical_symbol_future, "ESZ", "a contract is a root of upper-case letters"),
        (
            dm.canonical_symbol_future,
            "GCQ6-GCZ6",
            (
                r"^'GCQ6-GCZ6' is a futures combo \(a spread, strip, pack, or bundle\), not a "
                r"single contract; call dm\.canonical_symbol_future_spread$"
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "ESZ5",
            (
                r"^'ESZ5' is a single futures contract, not a combo; call "
                r"dm\.canonical_symbol_future$"
            ),
        ),
        (
            dm.canonical_symbol_future_spread,
            "GCQ6--GCZ6",
            r"^'GCQ6--GCZ6' is not a Massive futures combo ticker: runs of upper-case letters",
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
        (dm.canonical_symbol_crypto, "BTC", r"^cannot read .BTC. as a crypto pair: it is too"),
        (dm.canonical_symbol_crypto, "X:ETH", r"^cannot read .X:ETH. as a crypto pair: it is"),
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
        assert canonical_from_wire(ClassFromPrefix.ANY, name) == name
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
    with pytest.raises(ValueError, match="symbols must be Massive's exact tickers"):
        check_symbols(asset_class, value)


def test_check_symbols_materializes_generators() -> None:
    assert check_symbols(STK, cast("Iterable[str]", (s for s in ["B", "A"]))) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    items = (s for s in ["A"])
    list(items)
    with pytest.raises(ValueError, match=r"^no symbols: the list is empty"):
        check_symbols(STK, cast("Iterable[str]", items))
    with pytest.raises(ValueError, match=r"^no symbols: the list is empty"):
        check_symbols(STK, [])


def test_check_symbols_all_passes_through() -> None:
    assert check_symbols(STK, dm.ALL) is dm.ALL


def test_check_symbols_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(TypeError, match="symbols must be"):
        check_symbols(STK, cast("Iterable[str]", 5))
    with pytest.raises(TypeError, match="must be a str"):
        check_symbols(STK, cast("Iterable[str]", ["AAPL", 5]))


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
    for bad in ("", " ERIC B", "X:BTCUSD", "A\tB"):
        assert not is_canonical(STK, bad, as_written=True)
        assert canonical_from_wire(STK, bad, as_written=True) is None
        with pytest.raises(ValueError, match="is not a ticker as Massive writes it"):
            check_symbol(STK, bad, as_written=True)
    reader = WireSymbols("partners.tmx.corporate_events", STK, OnError.RAISE, as_written=True)
    read = reader.read(pa.array(["ERIC B", None, " x"]), {})
    assert read.symbols.to_pylist() == ["ERIC B", None, None]
    assert read.unreadable == (" x",)


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


def test_yahoo_forex_spelling_is_refused_with_massive_form() -> None:
    message = (
        "cannot read 'EURUSD=X' as a forex pair: it looks like Yahoo Finance's spelling; "
        "Massive's forex tickers look like 'C:EURUSD', two three-letter currency codes after "
        "C:, and this function reads 'EUR/USD', 'EUR-USD', or 'EURUSD'"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        dm.canonical_symbol_forex("EURUSD=X")
    with pytest.raises(ValueError, match="Yahoo Finance"):
        dm.canonical_symbol_forex("eurusd=x")


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
