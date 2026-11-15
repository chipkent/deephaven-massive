"""Symbols: the `symbols` argument, and the converters to Massive's ticker spellings.

Every symbol the library takes is sent to Massive as written, and Massive decides whether it
names an instrument: a ticker Massive has no data for gives no rows, and `m.symbol_reads()`
shows it. Case is never folded or judged (`KIMpL` is a Massive ticker). The library judges
a ticker's shape only by its class's spelling; this module owns three things around that:

- **The argument.** `check_symbols` reads a `symbols` argument other than a table (`dm.ALL`, one
  `str`, or a sequence of them) and `check_symbol` one value of it. A value of the wrong Python
  type raises `TypeError`, and a value the library cannot put in a request raises `ValueError`
  (`is_sendable`): empty text; whitespace at either end, which Massive's WebSocket drops from
  the topics it confirms; a control character; or `,` or `*`, which Massive's requests read as
  a separator or a wildcard instead of a ticker. A dataset of a class whose tickers carry a
  prefix (`O:`, `I:`, `C:`, `X:`) also refuses a symbol without it, and a forex or crypto
  dataset a pair written with `-` or `/` (`class_spelling_reason`).
- **The converters**, the public way to turn another spelling into Massive's:
  `canonical_symbol(text, asset_class, convention=)` for Python code, and one function per class
  that has another spelling (`canonical_symbol_option`, `canonical_symbol_index`,
  `canonical_symbol_forex`, `canonical_symbol_crypto`) plus one per stock convention
  (`canonical_symbol_stock_yahoo`, `canonical_symbol_stock_nasdaq`, `canonical_symbol_stock_cms`)
  for Deephaven query strings, which cannot pass an enum. Each maps None to None, converts the
  spellings its class or convention defines, and returns any other text unchanged, for Massive
  to answer. A convention converter refuses only the spellings that Massive reads as another
  instrument (Yahoo's `VOD.L`, Nasdaq's `ABC.U`), since sending them would read that
  instrument's data with no error.
- **The wire helpers**: `symbol_class` routes a symbol of a dataset that spans classes by its
  prefix, `forex_codes`, `read_forex_pair`, and `read_crypto_pair` split and join currency pairs
  for the transports (`wire_symbols.py`), and `option_roots` computes the `OptionRoot` column.

| Class   | Massive's spelling                     | Converters also accept         |
|---------|----------------------------------------|--------------------------------|
| stocks  | `AAPL`, `BRK.B`, `KIMpL`               | a named convention's spelling  |
| options | `O:` + OCC (`O:AAPL250117C00150000`)   | no `O:`, padded OSI, any case  |
| futures | `ESZ5`, `GCQ6-GCZ6`                    | nothing                        |
| indices | `I:` + the root (`I:SPX`)              | no `I:` (`SPX`)                |
| forex   | `C:` + two three-letter codes          | `EUR/USD`, `EURUSD`, any case  |
| crypto  | `X:` + the joined pair (`X:BTCUSD`)    | `BTC-USD`, `BTCUSD`, any case  |

A stock's case is meaningful (`KIMpL` is Kimco's series L preferred), so stock and index text
is never re-cased; Massive's option, forex, and crypto tickers are upper case, so their
converters fold case. A joined crypto ticker is never split by guessing where its quote starts
(`X:DOTUSD` reads as `DOT-USD` and as `DO-TUSD`): the transports take every split from Massive's
crypto ticker list and pass it in (`wire_symbols.ws_symbol`). Nothing maps one instrument to
another: an option root (`SPXW`, `AAPL1`) is never turned into its underlying, and `BRKB` never
into `BRK.B`.

Every function here is pure and thread-safe, makes no Deephaven call, and imports Deephaven for
type checking only.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Final, TypeAlias, assert_never

import pyarrow as pa
import pyarrow.compute as pc
from deephaven.table import Table
from typing_extensions import override

from deephaven_massive._enums import AssetClass, ClassFromPrefix, SymbolConvention, _require_enum
from deephaven_massive._errors import raises_through

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


__all__ = [
    "ALL",
    "PAIR_CLASSES",
    "AllSymbols",
    "NormalizedSymbols",
    "SymbolTable",
    "SymbolsLike",
    "canonical_symbol",
    "canonical_symbol_crypto",
    "canonical_symbol_forex",
    "canonical_symbol_index",
    "canonical_symbol_option",
    "canonical_symbol_stock_cms",
    "canonical_symbol_stock_nasdaq",
    "canonical_symbol_stock_yahoo",
    "check_symbol",
    "check_symbols",
    "class_spelling_reason",
    "forex_codes",
    "is_forex_currency",
    "is_sendable",
    "option_roots",
    "read_crypto_pair",
    "read_forex_pair",
    "symbol_class",
    "unsendable_reason",
]


class AllSymbols(Enum):
    """The type of `dm.ALL`, the value that asks a dataset for every symbol.

    Pass `dm.ALL` itself, as in `m.stocks.trades(dm.ALL)`; name `dm.AllSymbols` only in a type
    hint, as `dm.SymbolsLike` does. `dm.ALL` is its one member, and an enum member stays itself
    through a copy or an unpickle, so code tests for it with `symbols is dm.ALL`. It is not a
    `str`, so it is never mistaken for a ticker.
    """

    ALL = "ALL"

    @override
    def __repr__(self) -> str:
        """Show the public spelling, so the console prints `dm.ALL`, not `<AllSymbols.ALL: 'ALL'>`.

        Returns:
            `"dm.ALL"`.
        """
        return "dm.ALL"

    @override
    def __str__(self) -> str:
        """Render as `ALL` in messages and logs, as a query's `repr` does.

        Returns:
            `"ALL"`.
        """
        return "ALL"


ALL: Final = AllSymbols.ALL
"""Every symbol: a stream of the whole market, a whole flat file, or a full-market snapshot.

`ALL` is accepted by the stream methods, by every per-class `snapshots()`, and by `history()`
when a flat file or a grouped-daily REST path exists. Everywhere else it raises `ValueError`
naming the alternative.
"""

SymbolsLike: TypeAlias = str | Sequence[str] | Table | AllSymbols
"""What a market-data dataset method takes as its `symbols`: the instruments it reads.

Annotate your own code with it when you pass symbols through to a dataset method, such as a
helper that takes either a list of tickers or a watchlist table. It is one of:

- a `str`: one symbol, such as `"AAPL"`;
- a sequence of `str`: several symbols, read when the query is built;
- a Deephaven `Table` with a `Symbol` column: a static table is read when a query method runs,
  and a live table works as a watchlist, which the query's methods follow as you add and
  delete rows;
- `dm.ALL`: every symbol, in the datasets that take it.

Each symbol, and each row of a table, is sent to Massive as written, so write Massive's
ticker, such as `"C:EURUSD"`, not `"EUR/USD"`: a ticker Massive has no data for gives no rows.
`dm.canonical_symbol` and `dm.canonicalize_symbols` convert other spellings.

Example:
    ::

        from deephaven.table import Table

        import deephaven_massive as dm


        def latest_quotes(m: dm.MassiveSession, symbols: dm.SymbolsLike) -> Table:
            return m.stocks.quotes(symbols).last_by()
"""


_OCC: Final = re.compile(r"^O:[A-Z0-9]{1,6}\d{6}[CP]\d{8}$")
"""An OCC option symbol after `O:`: the root, `YYMMDD`, `C` or `P`, and the strike in
thousandths of a dollar, eight digits. Only the converter reads it, to recognize the spelling
it converts."""
_INDEX_ROOT: Final = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*$")
"""An index root the converter prefixes with `I:`: letters and digits, with dotted parts."""
_CRYPTO: Final = re.compile(r"^X:[A-Z0-9]{4,}$")
"""A joined crypto pair: `X:`, then at least a base of one character and a quote of three, so
the converter leaves a lone currency such as `BTC` unchanged."""
_FOREX_CURRENCY: Final = re.compile(r"^[A-Z]{3}$")
_CURRENCIES: Final = re.compile(r"^[A-Z0-9]+$")
_PAIR: Final = re.compile(r"^(?P<base>[A-Z0-9]+)[/-](?P<quote>[A-Z0-9]+)$")
_PADDED_OSI: Final = re.compile(r"^(?P<root>[A-Z0-9]{1,5}) +(?P<rest>\d{6}[CP]\d{8})$")
_OSI_LENGTH: Final = 21
_ROOT: Final = r"(?P<root>[A-Z]+)"
_CURRENCY_CODE_LENGTH: Final = 3  # ISO 4217
_OCC_TAIL: Final = 15
"""The characters after an OCC root: `YYMMDD`, `C` or `P`, and eight strike digits."""

PAIR_CLASSES: Final = frozenset({AssetClass.FOREX, AssetClass.CRYPTO})
"""The classes of currency and crypto pairs, which the options contracts list answers with no
error and no rows."""

_FOREX_PREFIX: Final = "C:"
"""The prefix of Massive's forex tickers."""

_CLASS_BY_PREFIX: Final[dict[str, AssetClass]] = {
    "O:": AssetClass.OPTIONS,
    "I:": AssetClass.INDICES,
    "C:": AssetClass.FOREX,
    "X:": AssetClass.CRYPTO,
}

_PREFIXED: Final[dict[AssetClass, tuple[str, str, str]]] = {
    AssetClass.OPTIONS: ("O:", "options contract", "O:AAPL250117C00150000"),
    AssetClass.INDICES: ("I:", "index", "I:SPX"),
    AssetClass.FOREX: ("C:", "forex", "C:EURUSD"),
    AssetClass.CRYPTO: ("X:", "crypto", "X:BTCUSD"),
}
"""The classes whose every ticker carries a prefix: each prefix, the ticker's name for a message,
and an example. Stocks and futures have none."""

_PAIR_SEPARATORS: Final = "-/"
"""The separators another source writes between a pair's currencies, which Massive's tickers
join (`C:EURUSD`, `X:BTCUSD`)."""

_UNSENDABLE: Final[dict[str, str]] = {
    ",": "Massive's list parameters and WebSocket subscriptions split on commas, so it would "
    "be sent as several tickers",
    "*": "Massive's WebSocket reads '*' as every ticker, so it would subscribe the whole market",
}
"""The characters no symbol can be sent with, each with the reason a message gives."""


@dataclass(frozen=True, slots=True)
class SymbolTable:
    """A table passed as `symbols`, compared and hashed by the identity of its Python wrapper.

    A refreshing table makes a reactive stream, which follows the table's rows as they change,
    and `history()` follows it too, reading each symbol added; the stream verbs read a static
    table once, as a fixed symbol list, `history()` reads a static table once, and `fetch()`
    reads either kind once.

    Deephaven `Table` wrappers compare equal when they wrap the same Java table, but a reactive
    stream is keyed by `id(table)`: two wrappers of one Java table are two streams.
    Two boxes are equal exactly when they hold the same wrapper, which keeps query equality and
    the stream key in agreement. The box holds the wrapper strongly, so its `id` stays valid
    for as long as a query or stream key holds the box.

    Attributes:
        table: The user's table; it has a `Symbol` column.
        table_id: `id(table)`, the box's identity for equality and hashing.
    """

    table: Table = field(compare=False)
    table_id: int = field(init=False)

    def __post_init__(self) -> None:
        """Record the wrapper's identity."""
        object.__setattr__(self, "table_id", id(self.table))


NormalizedSymbols: TypeAlias = tuple[str, ...] | AllSymbols | SymbolTable
"""A `symbols` argument after `check_symbols`: a sorted tuple of symbols, `ALL`, or a boxed
symbol table."""


# --- what a request can carry --------------------------------------------------------------------


def unsendable_reason(text: str) -> str | None:
    """Say why a symbol cannot be put in a request to Massive, if it cannot.

    Args:
        text: The symbol.

    Returns:
        None for text the library can send; otherwise the reason, such as `'A,B' holds ',':
        Massive's list parameters ... several tickers`: empty text, whitespace at either end,
        a control character, `,`, or `*`.
    """
    if not text:
        return "an empty symbol names no ticker, and a request without one asks for no ticker"
    if text != text.strip():
        return (
            f"{text!r} starts or ends with whitespace, which Massive's WebSocket drops from the "
            "topics it confirms, so a stream of it would never be confirmed"
        )
    control = next((char for char in text if unicodedata.category(char) == "Cc"), None)
    if control is not None:
        return f"{text!r} holds the control character {control!r}, which no ticker holds"
    for char, reason in _UNSENDABLE.items():
        if char in text:
            return f"{text!r} holds {char!r}: {reason}"
    return None


def is_sendable(text: str) -> bool:
    """Tell whether a symbol can be put in a request to Massive (`unsendable_reason`).

    Args:
        text: The symbol.

    Returns:
        True for any non-empty text without `,` or `*`.
    """
    return unsendable_reason(text) is None


def class_spelling_reason(
    asset_class: AssetClass | ClassFromPrefix | None, symbol: str
) -> str | None:
    """Say how a symbol is not spelled as its dataset's class writes tickers, if it is not.

    The two checks of a ticker's shape the library makes. An options, index, forex, or crypto
    dataset refuses a symbol without `O:`, `I:`, `C:`, or `X:`, so `AAPL` passed to an options
    dataset fails with the prefix to add instead of reading nothing. A forex or crypto dataset
    also refuses a pair written with `-` or `/` after its prefix (`C:EUR-USD`): REST would send
    it as written, but a flat-file filter and a WebSocket topic read Massive's joined ticker
    (`C:EURUSD`), so they would drop it silently.

    Args:
        asset_class: The dataset's class; `ClassFromPrefix.ANY` (a dataset of every class) or
            None (a table of underlyings or product codes) checks nothing.
        symbol: The symbol.

    Returns:
        None when the symbol has the prefix (and, for a pair, no separator) or the class has
        none (stocks, futures); otherwise the reason, such as `lacks the O: prefix of every
        options contract ticker, such as 'O:AAPL250117C00150000'`.
    """
    if asset_class is None or asset_class is ClassFromPrefix.ANY:
        return None  # a table of underlyings or product codes, or a dataset of every class
    if asset_class is AssetClass.STOCKS or asset_class is AssetClass.FUTURES:
        return None  # their tickers carry no prefix
    if not (
        asset_class is AssetClass.OPTIONS
        or asset_class is AssetClass.INDICES
        or asset_class is AssetClass.FOREX
        or asset_class is AssetClass.CRYPTO
    ):
        assert_never(asset_class)  # a new class needs its spelling here
    prefix, name, example = _PREFIXED[asset_class]
    if not symbol.startswith(prefix):
        return f"lacks the {prefix} prefix of every {name} ticker, such as {example!r}"
    separator = next((char for char in _PAIR_SEPARATORS if char in symbol[len(prefix) :]), None)
    if asset_class in PAIR_CLASSES and separator is not None:
        joined = prefix + "".join(
            char for char in symbol[len(prefix) :] if char not in _PAIR_SEPARATORS
        )
        return (
            f"writes a {name} pair with {separator!r}; Massive's ticker joins the two "
            f"currencies, as {joined!r}, which flat files and streams match"
        )
    return None


def symbol_class(symbol: str) -> AssetClass:
    """Return the asset class a symbol's prefix names, to route it in a dataset of every class.

    Args:
        symbol: A symbol, such as `X:BTCUSD` or `AAPL`.

    Returns:
        The class of its prefix (`O:` options, `I:` indices, `C:` forex, `X:` crypto); stocks
        for a symbol without one.
    """
    return _CLASS_BY_PREFIX.get(symbol[:2], AssetClass.STOCKS)


def is_forex_currency(code: str) -> bool:
    """Tell whether text is a forex currency code: three upper-case letters, such as `EUR`.

    Massive's forex pairs are ISO 4217 codes, so a digit (`EU1`) or another length is no
    currency; crypto codes, which may hold digits, follow their own rule.

    Args:
        code: The code.

    Returns:
        True for three letters `A` to `Z`.
    """
    return _FOREX_CURRENCY.fullmatch(code) is not None


# --- the converters ------------------------------------------------------------------------------


def _convert_option(text: str) -> str:
    """Convert an OCC option symbol to Massive's `O:` form, or return other text unchanged.

    Converts the symbol with or without `O:` and the 21-character OSI form whose root is padded
    with spaces (`BRKB  250117C00400000`), in any case.

    Args:
        text: The text.

    Returns:
        `O:` and the OCC symbol, upper case; the text unchanged when it is no OCC symbol.
    """
    body = text.upper().removeprefix("O:")
    padded = _PADDED_OSI.fullmatch(body)
    if padded is not None and len(body) == _OSI_LENGTH:
        body = padded["root"] + padded["rest"]
    symbol = f"O:{body}"
    return symbol if _OCC.fullmatch(symbol) is not None else text


def _convert_index(text: str) -> str:
    """Convert an index root to Massive's `I:` form; the caller named the class.

    Args:
        text: The text, with or without `I:`; its case is kept.

    Returns:
        `I:` and the root, such as `I:SPX`; the text unchanged when it already has `I:` or is
        no root of letters and digits (another class's ticker, say).
    """
    return f"I:{text}" if _INDEX_ROOT.fullmatch(text) is not None else text


def _pair(text: str, prefixes: tuple[str, ...]) -> tuple[str, str] | None:
    """Split a currency pair written with or without a prefix and a separator.

    Args:
        text: The text; no case is folded here.
        prefixes: The prefixes accepted and dropped.

    Returns:
        `(base, quote)`, the quote empty when the pair has no separator; None for text that is
        no pair of letters and digits.
    """
    body = text
    for prefix in prefixes:
        if text.startswith(prefix):
            body = text[len(prefix) :]
            break
    match = _PAIR.fullmatch(body)
    if match is not None:
        return match["base"], match["quote"]
    if _CURRENCIES.fullmatch(body) is not None:
        return body, ""
    return None


def read_forex_pair(text: str, prefixes: tuple[str, ...]) -> str | None:
    """Read a forex pair, without folding case.

    Args:
        text: The text.
        prefixes: The prefixes accepted.

    Returns:
        `C:` and the two codes, or None when the text is not two three-letter codes.
    """
    pair = _pair(text, prefixes)
    if pair is None:
        return None
    base, quote = pair
    if not quote:
        base, quote = base[:_CURRENCY_CODE_LENGTH], base[_CURRENCY_CODE_LENGTH:]
    if is_forex_currency(base) and is_forex_currency(quote):
        return f"C:{base}{quote}"
    return None


def read_crypto_pair(text: str, prefixes: tuple[str, ...]) -> str | None:
    """Read a crypto pair, without folding case: the two codes joined, whatever the quote.

    Args:
        text: The text.
        prefixes: The prefixes accepted.

    Returns:
        `X:` and the joined pair, or None for text that is no pair of letters and digits.
    """
    pair = _pair(text, prefixes)
    return None if pair is None else f"X:{pair[0]}{pair[1]}"


def _convert_forex(text: str) -> str:
    """Convert a forex pair to Massive's `C:` form, or return other text unchanged.

    Converts `EUR/USD`, `EUR-USD`, `EURUSD`, each with or without `C:`, in any case.

    Args:
        text: The text.

    Returns:
        `C:` and the two codes, such as `C:EURUSD`; the text unchanged when it is not two
        three-letter codes.
    """
    symbol = read_forex_pair(text.upper(), ("C:",))
    return text if symbol is None else symbol


def _convert_crypto(text: str) -> str:
    """Convert a crypto pair to Massive's `X:` form, or return other text unchanged.

    Converts `BTC-USD`, `BTC/USD`, `BTCUSD`, each with or without `X:`, in any case, and joins
    the two codes, whatever the quote currency.

    Args:
        text: The text.

    Returns:
        `X:` and the joined pair, such as `X:BTCUSD`; the text unchanged when it is no pair of
        letters and digits or too short to hold a base and a three-letter quote (`BTC`).
    """
    symbol = read_crypto_pair(text.upper(), ("X:",))
    if symbol is None or _CRYPTO.fullmatch(symbol) is None:
        return text
    return symbol


def _unchanged(text: str) -> str:
    """Return text unchanged: a class with no other spelling to convert (stocks, futures).

    Args:
        text: The text.

    Returns:
        The text.
    """
    return text


_CONVERTERS: Final[dict[AssetClass, Callable[[str], str]]] = {
    AssetClass.STOCKS: _unchanged,
    AssetClass.OPTIONS: _convert_option,
    AssetClass.FUTURES: _unchanged,
    AssetClass.INDICES: _convert_index,
    AssetClass.FOREX: _convert_forex,
    AssetClass.CRYPTO: _convert_crypto,
}

_STOCK_CONVENTIONS: Final[dict[SymbolConvention, tuple[tuple[re.Pattern[str], str], ...]]] = {
    SymbolConvention.YAHOO: (
        (re.compile(rf"^{_ROOT}-P(?P<series>[A-Z])$"), r"\g<root>p\g<series>"),
        (re.compile(rf"^{_ROOT}-(?P<cls>[A-OQ-TV-Z])$"), r"\g<root>.\g<cls>"),
    ),
    SymbolConvention.NASDAQ: (
        (re.compile(rf"^{_ROOT}-(?P<series>[A-Z]?)$"), r"\g<root>p\g<series>"),
    ),
    SymbolConvention.CMS: (
        (re.compile(rf"^{_ROOT} PR(?P<series>[A-Z]?)$"), r"\g<root>p\g<series>"),
        (re.compile(rf"^{_ROOT} (?P<cls>[A-TV-Z])$"), r"\g<root>.\g<cls>"),
    ),
}
"""Each stock convention's spellings of a preferred and a share class, and Massive's form of
each (`BACpL`, `BRK.B`); any other text, a plain ticker included, is returned unchanged. Class
`U` is left unchanged in Yahoo's and CMS's spellings, since CQS writes units as `U`; Yahoo's
bare `-P` is left unchanged, since it reads as a generic preferred and as class P alike."""

_ANOTHER_INSTRUMENT: Final[dict[SymbolConvention, re.Pattern[str]]] = {
    SymbolConvention.YAHOO: re.compile(r"\."),
    SymbolConvention.NASDAQ: re.compile(r"\.U$"),
}
"""The spellings of a convention that Massive reads as another instrument: Yahoo's dot names
a listing outside the US (`VOD.L`) where Massive's names a share class, and Nasdaq's `.U` is
class U where Massive's is a unit. Returned unchanged, they would read that instrument's data
with no error, so the converter refuses them."""


def _refuse_another_instrument(text: str, convention: SymbolConvention) -> None:
    """Refuse a convention's spelling that Massive reads as another instrument.

    Args:
        text: The ticker as the convention writes it.
        convention: The convention it comes from.

    Raises:
        ValueError: If Massive reads the text as another instrument than the convention does
            (`_ANOTHER_INSTRUMENT`): a Yahoo ticker with a dot, or a Nasdaq class U.
    """
    other = _ANOTHER_INSTRUMENT.get(convention)
    if other is not None and other.search(text) is not None:
        raise ValueError(
            f"{text!r} names another instrument in Massive's spelling than in the "
            f"{convention.name} convention (a Yahoo dot names a listing outside the US, a "
            "Nasdaq '.U' a class U; Massive's dot names a share class and its '.U' a unit), "
            "so it is not converted; pass Massive's own ticker, as m.reference.tickers lists it"
        )


def _convert_stock_from(text: str, convention: SymbolConvention) -> str:
    """Convert a stock ticker written in a named convention to Massive's form.

    Args:
        text: The ticker as the convention writes it, upper case.
        convention: The convention it comes from.

    Returns:
        Massive's ticker: a class as `.X`, a preferred as `p` and its series; any other text
        unchanged, for Massive to answer.
    """
    for pattern, template in _STOCK_CONVENTIONS[convention]:
        match = pattern.fullmatch(text)
        if match is not None:
            return match.expand(template)
    return text


def _require_text(text: object) -> str | None:
    """Check a converter's input: a `str` or None.

    Args:
        text: The input.

    Returns:
        The text, or None.

    Raises:
        TypeError: If the input is neither a `str` nor None.
    """
    if text is None or isinstance(text, str):
        return text
    raise TypeError(f"a symbol must be a str or None, not {type(text).__name__}")


@raises_through(_require_enum, _require_text, _refuse_another_instrument)
def canonical_symbol(
    text: str | None,
    asset_class: AssetClass,
    *,
    convention: SymbolConvention = SymbolConvention.MASSIVE,
) -> str | None:
    """Convert a symbol of a named asset class to Massive's ticker spelling.

    Use it to turn symbols from another source into the form Massive's tickers take, before
    passing them as `symbols=`. Under `SymbolConvention.MASSIVE`, the default, another spelling
    of the class converts (`EUR/USD` to `C:EURUSD`, `SPX` to `I:SPX`), and any other text comes
    back unchanged, so Massive answers it: a ticker Massive has no data for gives no rows.
    Stock and futures tickers have no other spelling under this convention, so they always come
    back unchanged. Under another convention, for stocks only, that convention's spellings of a
    preferred and a share class convert. The class is never guessed: `SPX` is an index only
    because the caller says so. In a Deephaven query string, call the per-class functions
    instead (`canonical_symbol_forex`), which need no enum argument; a function called through
    the module (`dm.canonical_symbol(...)`) makes an `Object` column unless cast with
    `(String)`. Pure and thread-safe.

    Example:
        ::

            import deephaven_massive as dm

            dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX)  # 'C:EURUSD'
            dm.canonical_symbol(
                "BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
            )  # 'BRK.B'

    Args:
        text: The symbol, or None.
        asset_class: The symbol's class, whose spellings apply. Required: nothing is inferred.
        convention: The convention the text comes from. `SymbolConvention.MASSIVE`, the
            default, converts the unambiguous spellings of the class; `YAHOO`, `NASDAQ`, and
            `CMS`, for stocks only, convert from that convention's spellings. A stock ticker
            converts only from a named convention, because `-` means a class in Yahoo's and a
            preferred in Nasdaq's.

    Returns:
        Massive's ticker, the text unchanged when no spelling of the class or convention
        matches it, or None for None.

    Raises:
        TypeError: If `asset_class` is not a `dm.AssetClass`, `convention` is not a
            `dm.SymbolConvention`, or `text` is neither a `str` nor None.
        ValueError: If `convention` is a stock convention and the class is not stocks, or the
            text is a convention's spelling that Massive reads as another instrument (Yahoo's
            `VOD.L`, Nasdaq's `ABC.U`).
    """
    resolved = _require_enum(asset_class, AssetClass, "asset_class")
    chosen = _require_enum(convention, SymbolConvention, "convention")
    if chosen is not SymbolConvention.MASSIVE and resolved is not AssetClass.STOCKS:
        raise ValueError(
            f"the {chosen.name} convention applies to stocks only, not {resolved.value}"
        )
    value = _require_text(text)
    if value is None:
        return None
    if chosen is not SymbolConvention.MASSIVE:
        _refuse_another_instrument(value, chosen)
        return _convert_stock_from(value, chosen)
    return _CONVERTERS[resolved](value)


@raises_through(canonical_symbol)
def canonical_symbol_stock_yahoo(text: str | None) -> str | None:
    """Convert a Yahoo Finance stock ticker to Massive's: `BRK-B` to `BRK.B`, `BAC-PL` to `BACpL`.

    Yahoo writes a share class as `-X` and a preferred as `-P` and its series; any other text,
    a plain ticker included, comes back unchanged, so Massive answers it. Yahoo's bare `-P`, a
    generic preferred and class P alike, and its warrants (`-WT`), units (`-UN`), and rights
    (`-RI`), for which Massive documents no spelling, come back unchanged too. A ticker with a
    dot, Yahoo's mark of a listing outside the US (`VOD.L`), is refused, since Massive reads the
    dot as a share class. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_yahoo`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_yahoo(Col)`.

    Args:
        text: The Yahoo ticker, or None.

    Returns:
        Massive's ticker, the text unchanged when it is no Yahoo class or preferred, or None
        for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text holds a dot.
    """
    return canonical_symbol(text, AssetClass.STOCKS, convention=SymbolConvention.YAHOO)


@raises_through(canonical_symbol)
def canonical_symbol_stock_nasdaq(text: str | None) -> str | None:
    """Convert a Nasdaq Integrated Platform stock ticker to Massive's: `KIM-L` to `KIMpL`.

    Nasdaq's own symbology (the "NASDAQ Symbol" column of `nasdaqtraded.txt`) writes a class as
    `.X`, as Massive does, and a preferred as `-` and its optional series. Any other text comes
    back unchanged, so Massive answers it: its other suffixes (`+` warrants, `=` units, `^`
    rights, `#` when issued) included, for which Massive documents no spelling. A class U
    (`ABC.U`) is refused, since Massive reads `.U` as a unit. The ACT symbology of the same
    files writes `.W` for warrants and `.V` for both a class and when-issued, so it is ambiguous
    and not supported. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_nasdaq`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_nasdaq(Col)`.

    Args:
        text: The Nasdaq ticker, or None.

    Returns:
        Massive's ticker, the text unchanged when it is no Nasdaq preferred, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text ends in `.U`.
    """
    return canonical_symbol(text, AssetClass.STOCKS, convention=SymbolConvention.NASDAQ)


@raises_through(_require_text)
def canonical_symbol_stock_cms(text: str | None) -> str | None:
    """Convert a CMS stock ticker to Massive's: `BRK B` to `BRK.B`, `BAC PRL` to `BACpL`.

    The CMS convention (the root, a space, and a suffix, as many broker and FIX systems write
    it) spells a class as ` X` and a preferred as ` PR` and its optional series. Any other
    text comes back unchanged, so Massive answers it: its other suffixes (` WS`, ` U`, ` RT`,
    ` WI`) included, for which Massive documents no spelling. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_cms`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_cms(Col)`.

    Args:
        text: The CMS ticker, or None.

    Returns:
        Massive's ticker, the text unchanged when it is no CMS class or preferred, or None for
        None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
    """
    value = _require_text(text)
    return None if value is None else _convert_stock_from(value, SymbolConvention.CMS)


@raises_through(_require_text)
def canonical_symbol_option(text: str | None) -> str | None:
    """Convert an OCC option symbol to Massive's: `AAPL250117C00150000` to `O:AAPL250117C00150000`.

    Converts the symbol with or without `O:` and the 21-character OSI form whose root is padded
    with spaces, in any case; any other text comes back unchanged, so Massive answers it. The
    root is never changed: `SPXW` stays `SPXW` and an adjusted root such as `AAPL1` stays
    `AAPL1`. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_option`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_option(Col)`.

    Args:
        text: The option symbol, or None.

    Returns:
        `O:` and the OCC symbol, the text unchanged when it is no OCC symbol, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
    """
    value = _require_text(text)
    return None if value is None else _convert_option(value)


@raises_through(_require_text)
def canonical_symbol_index(text: str | None) -> str | None:
    """Convert an index ticker to Massive's: `SPX` to `I:SPX`.

    The prefix is added because the caller names the class by calling this function; no
    ticker is ever recognized as an index by its name. Case is kept, and text that already has
    `I:` or is no root of letters and digits comes back unchanged. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_index`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_index(Col)`.

    Args:
        text: The index ticker, with or without `I:`, or None.

    Returns:
        `I:` and the ticker, the text unchanged when it is no root, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
    """
    value = _require_text(text)
    return None if value is None else _convert_index(value)


@raises_through(_require_text)
def canonical_symbol_forex(text: str | None) -> str | None:
    """Convert a currency pair to Massive's forex ticker: `EUR/USD` to `C:EURUSD`.

    Converts `EUR/USD`, `EUR-USD`, and `EURUSD`, with or without `C:`, in any case; any other
    text, such as Yahoo Finance's `EURUSD=X`, comes back unchanged, so Massive answers it. Pure
    and thread-safe.

    Example:
        ::

            from deephaven import new_table
            from deephaven.column import string_col

            from deephaven_massive import canonical_symbol_forex

            pairs = new_table([string_col("Pair", ["EUR/USD", "GBP-USD"])])
            pairs = pairs.update(["Symbol = canonical_symbol_forex(Pair)"])

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_forex`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_forex(Col)`.

    Args:
        text: The pair, or None.

    Returns:
        `C:` and the two codes, the text unchanged when it is not two three-letter codes, or
        None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
    """
    value = _require_text(text)
    return None if value is None else _convert_forex(value)


@raises_through(_require_text)
def canonical_symbol_crypto(text: str | None) -> str | None:
    """Convert a crypto pair to Massive's ticker: `BTC-USD` to `X:BTCUSD`.

    Converts `BTC-USD`, `BTC/USD`, and `BTCUSD`, with or without `X:`, in any case, whatever the
    quote currency, and joins the two codes; any other text, such as a lone `BTC`, comes back
    unchanged, so Massive answers it. A stream, the last-trade path, and the daily open/close
    path need the pair split into its two codes, and take the split only from Massive's crypto
    ticker list, so they skip a pair that list does not hold (`MassiveConfig.unlisted_symbols`).
    Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_crypto`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_crypto(Col)`.

    Args:
        text: The pair, or None.

    Returns:
        `X:` and the joined pair, the text unchanged when it is no pair, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
    """
    value = _require_text(text)
    return None if value is None else _convert_crypto(value)


# --- checks when a query is built ----------------------------------------------------------------


def check_symbol(text: object) -> str:
    """Return a symbol passed in code, as the request will send it.

    The check behind `symbols=`, `underlying=`, and each value of a list: the library never
    converts or judges what a caller passes; Massive answers it.

    Args:
        text: The value as passed.

    Returns:
        The text, unchanged.

    Raises:
        TypeError: If the value is not a `str`.
        ValueError: If the library cannot put the text in a request (`unsendable_reason`).
    """
    if not isinstance(text, str):
        raise TypeError(f"a symbol must be a str, not {type(text).__name__}")
    reason = unsendable_reason(text)
    if reason is not None:
        raise ValueError(f"cannot send the symbol {text!r} to Massive: {reason}")
    return text


def check_symbols(symbols: str | Iterable[str] | AllSymbols) -> tuple[str, ...] | AllSymbols:
    """Check a `Symbols` argument other than a table when a query is built.

    Rules: `dm.ALL` passes through; a `str` is one symbol; any other iterable of strings,
    including a generator, is materialized now. Every symbol is checked by `check_symbol`. The
    resulting tuple is sorted and de-duplicated, so equal symbol sets give equal queries and one
    stream. A Deephaven `Table` never reaches this function: the dataset factory boxes it in a
    `SymbolTable` first.

    Args:
        symbols: The argument as the user passed it.

    Returns:
        A sorted tuple of symbols, or `ALL`.

    Raises:
        TypeError: If `symbols` is not a `str`, an iterable of `str`, or `dm.ALL`, or an
            element is not a `str`.
        ValueError: If no symbols remain (`ValueError("no symbols")`, for example from an
            exhausted generator), or a symbol cannot be sent (`check_symbol`).
    """
    if isinstance(symbols, AllSymbols):
        return symbols
    if isinstance(symbols, str):
        items: tuple[object, ...] = (symbols,)
    else:
        try:
            items = tuple(symbols)
        except TypeError:
            raise TypeError(
                "symbols must be a str, a sequence of str, a Deephaven Table, or dm.ALL; "
                f"got {type(symbols).__name__}"
            ) from None
    if not items:
        raise ValueError(
            "no symbols: the list is empty; pass at least one ticker, or dm.ALL for every "
            "ticker where the dataset takes it"
        )
    return tuple(sorted({check_symbol(i) for i in items}))


# --- the wire ------------------------------------------------------------------------------------


def forex_codes(symbol: str) -> tuple[str, str] | None:
    """Split a forex ticker into its two ISO 4217 codes, by position.

    A forex ticker is `C:` and two three-letter codes joined, so, unlike a crypto pair, it
    splits by position, with no lookup and no guess: `C:EURUSD` is `("EUR", "USD")`. Only that
    form is split: `C:` and six letters. Any other text, such as `C:EUR/USD`, has no codes the
    library can derive, and goes on the wire as written.

    Args:
        symbol: A forex ticker, such as `C:EURUSD`.

    Returns:
        `(base, quote)`, such as `("EUR", "USD")`; None for text that is not `C:` and six
        letters.
    """
    body = symbol.removeprefix(_FOREX_PREFIX)
    if body == symbol or len(body) != 2 * _CURRENCY_CODE_LENGTH or not body.isalpha():
        return None
    return body[:_CURRENCY_CODE_LENGTH], body[_CURRENCY_CODE_LENGTH:]


def option_roots(symbols: pa.Array) -> pa.Array:
    """Return the OCC root of each option symbol: the `OptionRoot` column.

    The root is the symbol without its `O:` prefix and its last 15 characters (expiry, type,
    strike): `SPXW` for `O:SPXW251219C06000000`, `AAPL1` for an adjusted contract. It is read
    from the symbol, never mapped to an underlying. A symbol of another class (`AAPL`,
    `X:BTCUSD`), which a dataset of every class mixes with options contracts, has no root.

    Args:
        symbols: Symbols, nulls allowed.

    Returns:
        A string array of the roots; null where the symbol is null or not an options contract.
    """
    slicing = pc.SliceOptions(start=2, stop=-_OCC_TAIL)
    roots = pc.call_function("utf8_slice_codeunits", [symbols], slicing)
    return pc.if_else(pc.starts_with(symbols, "O:"), roots, pa.scalar(None, pa.string()))
