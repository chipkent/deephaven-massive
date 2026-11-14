"""Tests for the `symbols` argument, its check, and the converters to Massive's spellings."""

import copy
import json
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
from deephaven_massive._datasets.symbols import (
    ALL,
    AllSymbols,
    SymbolsLike,
    SymbolTable,
    check_symbol,
    check_symbols,
    forex_codes,
    is_forex_currency,
    is_sendable,
    option_roots,
    symbol_class,
    unsendable_reason,
)
from deephaven_massive._datasets.wire_symbols import canonical_from_wire
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError, SymbolConvention
from deephaven_massive._tables.watchlist import CurrentSymbols, SymbolRowRule, read_symbols

STK, OPT, FUT, IDX, FX, CRY = (
    AssetClass.STOCKS,
    AssetClass.OPTIONS,
    AssetClass.FUTURES,
    AssetClass.INDICES,
    AssetClass.FOREX,
    AssetClass.CRYPTO,
)

SENT_AS_WRITTEN = [
    # Massive's tickers, and text that is none: Massive answers each, so each is sent as is.
    "AAPL",
    "BRK.B",
    "KIMpL",
    "aapl",
    "O:AAPL250117C00150000",
    "O:AAPL",
    "AAPL250117C00150000",
    "ESZ5",
    "esz5",
    "GCQ6-GCZ6",
    "BOB 02-01 U6",
    " ESZ5 ",
    "I:SPX",
    "SPX",
    "C:EURUSD",
    "EURUSD=X",
    "X:BTCUSD",
    "BTC-USD",
    "EUR/USD",
    "X:BTC",
    "ERIC B",
    "7203",
]


@pytest.mark.parametrize("text", SENT_AS_WRITTEN)
def test_a_symbol_is_sent_as_written_whatever_its_shape(text: str) -> None:
    # No ticker's shape is judged: Massive decides, and a ticker it has no data for gives no
    # rows. The only refusals are of text no request can carry.
    assert is_sendable(text)
    assert check_symbol(text) == text
    assert check_symbols([text]) == (text,)
    rule = SymbolRowRule("stocks.trades", OnError.RAISE)
    assert rule.accepts(text)


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("", "an empty symbol names no ticker"),
        ("AAPL,MSFT", "split on commas, so it would be sent as several tickers"),
        ("*", "reads '\\*' as every ticker"),
    ],
)
def test_text_no_request_can_carry_is_refused(text: str, reason: str) -> None:
    assert not is_sendable(text)
    assert unsendable_reason(text) is not None
    with pytest.raises(
        ValueError, match=rf"^cannot send the symbol {re.escape(repr(text))} to Massive: .*{reason}"
    ):
        check_symbol(text)
    with pytest.raises(ValueError, match="cannot send the symbol"):
        check_symbols(["AAPL", text])


def test_check_symbol_refuses_a_value_that_is_not_text() -> None:
    with pytest.raises(TypeError, match=r"^a symbol must be a str, not int$"):
        check_symbol(5)


def test_every_futures_ticker_massive_writes_round_trips(
    load_fixture: Callable[[str, str], bytes], dh_exec_ctx: object
) -> None:
    # Every ticker of the vendored samples and of the live evidence (combo strips, packs, and
    # bundles included) is taken as symbols= input, as a symbol-table row, and from a row, as
    # written.
    tickers = json.loads(load_fixture("datasets/symbols", "futures_tickers.json"))
    every = [*tickers["vendor_samples"], *tickers["live_evidence"], "SR3:PK H7"]
    assert check_symbols(every) == tuple(sorted(set(every)))
    rule = SymbolRowRule("futures.trades", unsendable=OnError.RAISE)
    rows = read_symbols(rule, new_table([string_col("Symbol", every)]), stream_id=None)
    assert rows == CurrentSymbols(tuple(sorted(set(every))))
    for text in every:
        assert canonical_from_wire(FUT, text) == text
        assert dm.canonical_symbol(text, FUT) == text


@pytest.mark.parametrize(
    "text",
    [
        # Every crypto ticker the vendored documentation writes, and the spellings the
        # transports send; each converts to the joined pair.
        *("X:ADAUSD", "X:ALGOUSD", "X:ARDRUSD", "X:BOBAUSD", "X:BTCEUR", "X:BTCGBP"),
        *("X:BTCUSD", "X:DRNUSD", "X:ETHBTC", "X:FSNUSD", "X:LCXUSD", "X:LINKUSD"),
        *("X:LTCUSD", "X:MOBUSD", "X:NGCUSD", "X:RNDRUSD", "X:SOLUSD", "X:ZSCUSD"),
        *("X:BTC-USD", "BCD-USD", "BTC-USD", "USD/EUR", "X:EUR-USD", "B-USD"),
    ],
)
def test_every_crypto_pair_massive_writes_converts_to_its_joined_pair(text: str) -> None:
    symbol = dm.canonical_symbol_crypto(text)
    assert symbol is not None
    assert symbol.startswith("X:")
    assert "-" not in symbol
    assert "/" not in symbol
    assert dm.canonical_symbol_crypto(symbol) == symbol


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
    (dm.canonical_symbol_crypto, "ABCUS-DT", "X:ABCUSDT"),  # the codes joined as written
    (dm.canonical_symbol_stock_yahoo, "BRK-B", "BRK.B"),
    (dm.canonical_symbol_stock_yahoo, "BAC-PL", "BACpL"),
    (dm.canonical_symbol_stock_yahoo, "KIM-PL", "KIMpL"),
    (dm.canonical_symbol_stock_nasdaq, "KIM-L", "KIMpL"),
    (dm.canonical_symbol_stock_nasdaq, "DCOM-", "DCOMp"),
    (dm.canonical_symbol_stock_cms, "BRK B", "BRK.B"),
    (dm.canonical_symbol_stock_cms, "BAC PRL", "BACpL"),
    (dm.canonical_symbol_stock_cms, "DCOM PR", "DCOMp"),
]


@pytest.mark.parametrize(("convert", "text", "expected"), CONVERSIONS)
def test_the_converters_convert_their_spellings(
    convert: Callable[[str | None], str | None], text: str, expected: str
) -> None:
    assert convert(text) == expected
    if convert is not dm.canonical_symbol_stock_yahoo:  # Yahoo's dot is a foreign listing
        assert convert(expected) == expected  # Massive's form needs no conversion
    assert convert(None) is None


@pytest.mark.parametrize(
    ("convert", "text"),
    [
        # Text no spelling of the class matches comes back as it is, for Massive to answer.
        (dm.canonical_symbol_option, "AAPL"),
        (dm.canonical_symbol_option, "O:AAPL250229X00150000"),
        (dm.canonical_symbol_option, "X:BTCUSD"),
        (dm.canonical_symbol_index, "I:SPX"),
        (dm.canonical_symbol_index, "X:BTCUSD"),
        (dm.canonical_symbol_index, ""),
        (dm.canonical_symbol_index, " SPX"),
        (dm.canonical_symbol_forex, "EURUSD=X"),
        (dm.canonical_symbol_forex, "EU1/USD"),
        (dm.canonical_symbol_forex, "EUR/USD/JPY"),
        (dm.canonical_symbol_forex, "X:BTCUSD"),
        (dm.canonical_symbol_crypto, "BTC"),
        (dm.canonical_symbol_crypto, "X:ETH"),
        (dm.canonical_symbol_crypto, "BTC.USD"),
        (dm.canonical_symbol_crypto, "C:EURUSD"),
        (dm.canonical_symbol_stock_yahoo, "AAPL"),
        (
            dm.canonical_symbol_stock_yahoo,
            "BBAI-WT",
        ),  # warrants: Massive's spelling is undocumented
        (dm.canonical_symbol_stock_yahoo, "DCOM-P"),  # a generic preferred and class P read alike
        (dm.canonical_symbol_stock_yahoo, "ABC-U"),  # class U collides with units
        (dm.canonical_symbol_stock_yahoo, "brk-b"),
        (dm.canonical_symbol_stock_nasdaq, "BRK.B"),  # Nasdaq writes a class as Massive does
        (dm.canonical_symbol_stock_nasdaq, "AAC+"),
        (dm.canonical_symbol_stock_nasdaq, "BAC$L"),  # the ACT symbology, which is ambiguous
        (dm.canonical_symbol_stock_cms, "MSFT"),
        (dm.canonical_symbol_stock_cms, "AAC WS"),
        (dm.canonical_symbol_stock_cms, "AAC U"),
    ],
)
def test_a_converter_returns_text_it_does_not_recognize_unchanged(
    convert: Callable[[str | None], str | None], text: str
) -> None:
    assert convert(text) == text


@pytest.mark.parametrize("asset_class", [STK, FUT])
def test_a_class_without_another_spelling_returns_every_text_unchanged(
    asset_class: AssetClass,
) -> None:
    for text in ("AAPL", "BRK-B", "aapl", "ESZ5", "GCQ6-GCZ6", ""):
        assert dm.canonical_symbol(text, asset_class) == text


@pytest.mark.parametrize(
    ("convention", "text"),
    [
        (SymbolConvention.YAHOO, "VOD.L"),
        (SymbolConvention.YAHOO, "SHOP.TO"),
        (SymbolConvention.YAHOO, "BRK.B"),
        (SymbolConvention.NASDAQ, "ABC.U"),
    ],
)
def test_a_convention_refuses_a_spelling_massive_reads_as_another_instrument(
    convention: SymbolConvention, text: str
) -> None:
    with pytest.raises(
        ValueError, match=rf"^'{text}' names another instrument in Massive's spelling than in"
    ):
        dm.canonical_symbol(text, STK, convention=convention)


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


def test_a_stock_convention_applies_to_stocks_only() -> None:
    for convention in (SymbolConvention.YAHOO, SymbolConvention.NASDAQ, SymbolConvention.CMS):
        message = rf"^the {convention.name} convention applies to stocks only, not forex$"
        with pytest.raises(ValueError, match=message):
            dm.canonical_symbol("EUR/USD", FX, convention=convention)


def test_the_massive_convention_is_the_default_for_every_class() -> None:
    massive = SymbolConvention.MASSIVE
    assert dm.canonical_symbol("EUR/USD", FX, convention=massive) == "C:EURUSD"
    assert dm.canonical_symbol("EUR/USD", FX) == "C:EURUSD"
    assert dm.canonical_symbol("KIMpL", STK, convention=massive) == "KIMpL"
    assert dm.canonical_symbol("SPX", IDX, convention=massive) == "I:SPX"
    # A dash means a class to Yahoo and a preferred to Nasdaq, so without one it is kept.
    assert dm.canonical_symbol("BRK-B", STK, convention=massive) == "BRK-B"


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
        dm.canonical_symbol_option,
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


def test_no_conversion_looks_a_name_up() -> None:
    # The index prefix comes from the class the caller or the endpoint names, never from the
    # ticker: a made-up name converts like a real one, and a bare name is a stock elsewhere.
    for name in ("SPX", "ZZQX", "AAPL"):
        assert dm.canonical_symbol_index(name) == f"I:{name}"
        assert canonical_from_wire(IDX, name) == f"I:{name}"
        assert canonical_from_wire(ClassFromPrefix.ANY, name) == name
        assert canonical_from_wire(STK, name) == name


def test_forex_currencies_are_three_letters() -> None:
    assert is_forex_currency("EUR")
    for code in ("EU1", "EU", "EURO", "eur", "1NC"):
        assert not is_forex_currency(code)


def test_forex_codes_split_a_forex_ticker_by_position() -> None:
    assert forex_codes("C:EURUSD") == ("EUR", "USD")
    assert forex_codes("C:USDJPY") == ("USD", "JPY")
    # Text with the prefix splits the same way, with no check: Massive answers what is sent.
    assert forex_codes("C:EURO") == ("EUR", "O")
    # Text without it has no codes the library can derive: it goes on the wire as written.
    assert forex_codes("EURUSD") is None


def test_option_roots() -> None:
    roots = option_roots(
        pa.array(["O:SPXW251219C06000000", "O:AAPL1250117C00150000", "O:F250117P00010000", None])
    )
    assert roots.to_pylist() == ["SPXW", "AAPL1", "F", None]


def test_check_symbols_sorts_and_deduplicates() -> None:
    assert check_symbols(["MSFT", "AAPL", "MSFT"]) == ("AAPL", "MSFT")
    assert check_symbols("KIMpL") == ("KIMpL",)
    assert check_symbols(["X:BTCUSD", "I:SPX", "AAPL"]) == ("AAPL", "I:SPX", "X:BTCUSD")


def test_check_symbols_materializes_generators() -> None:
    assert check_symbols(s for s in ["B", "A"]) == ("A", "B")


def test_exhausted_generator_raises() -> None:
    items = (s for s in ["A"])
    list(items)
    with pytest.raises(ValueError, match=r"^no symbols: the list is empty"):
        check_symbols(items)
    with pytest.raises(ValueError, match=r"^no symbols: the list is empty"):
        check_symbols([])


def test_check_symbols_all_passes_through() -> None:
    assert check_symbols(dm.ALL) is dm.ALL


def test_check_symbols_rejects_non_iterables_and_non_strings() -> None:
    with pytest.raises(TypeError, match="symbols must be"):
        check_symbols(5)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="must be a str"):
        check_symbols(["AAPL", 5])  # type: ignore[list-item]  # the point: refused at run time


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


CONVERTERS = [
    ("canonical_symbol_stock_yahoo", "BRK-B", "BRK.B"),
    ("canonical_symbol_stock_nasdaq", "KIM-L", "KIMpL"),
    ("canonical_symbol_stock_cms", "BAC PRL", "BACpL"),
    ("canonical_symbol_option", "AAPL250117C00150000", "O:AAPL250117C00150000"),
    ("canonical_symbol_index", "SPX", "I:SPX"),
    ("canonical_symbol_forex", "EUR/USD", "C:EURUSD"),
    ("canonical_symbol_crypto", "BTC-USD", "X:BTCUSD"),
]


@pytest.mark.parametrize("case", CONVERTERS, ids=[c[0] for c in CONVERTERS])
def test_each_converter_runs_in_a_query_string(
    dh_exec_ctx: object, case: tuple[str, str, str]
) -> None:
    # Imported by bare name (here: a local name), the function makes a String column; a null
    # stays null, and text it does not recognize stays as it is.
    name, text, expected = case
    convert = getattr(dm, name)
    assert convert.__name__ == name  # the formulas below find it as `convert`
    table = new_table([string_col("P", [text, None, "#?"])]).update(["S = convert(P)"])
    (column,) = [c for c in table.columns if c.name == "S"]
    assert column.data_type == dtypes.string
    assert [row[0] for row in table.iter_tuple(cols="S")] == [expected, None, "#?"]


def test_a_refusing_converter_fails_a_query_string_with_its_message(dh_exec_ctx: object) -> None:
    convert = dm.canonical_symbol_stock_yahoo
    assert convert.__name__ == "canonical_symbol_stock_yahoo"  # the formula finds `convert`
    bad = new_table([string_col("P", ["VOD.L"])])
    with pytest.raises(DHError, match="names another instrument"):
        bad.update(["S = convert(P)"])


def test_a_converter_called_through_the_module_needs_a_cast(dh_exec_ctx: object) -> None:
    pairs = new_table([string_col("P", ["EUR/USD"])])
    plain = pairs.update(["S = dm.canonical_symbol_forex(P)"])
    cast_ = pairs.update(["S = (String) dm.canonical_symbol_forex(P)"])
    types = {t: next(c.data_type for c in t.columns if c.name == "S") for t in (plain, cast_)}
    assert types[plain] != dtypes.string  # an Object column
    assert types[cast_] == dtypes.string
    assert [row[0] for row in cast_.iter_tuple(cols="S")] == ["C:EURUSD"]


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
