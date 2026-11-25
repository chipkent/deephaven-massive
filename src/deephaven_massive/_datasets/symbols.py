"""Symbols: Massive's canonical tickers, their checks, and the converters to them.

Every symbol the library takes or shows is Massive's exact REST ticker, case kept: `AAPL`,
`BRK.B`, `KIMpL`, `O:BRKB250117C00400000`, `ESZ5`, `I:SPX`, `C:EURUSD`, `X:BTCUSD`. This module
owns that grammar and two things built on it:

- **The checks.** `check_symbols` and `check_symbol` accept only canonical text when a query is
  built (`symbols=`, `underlying=`, the rows of a symbol table); anything else raises
  `ValueError`, and the message names the canonical form when the class's converter gives one.
  `is_canonical` is the test itself.
- **The converters**, the public way to turn another spelling into Massive's:
  `canonical_symbol(text, asset_class, convention=)` for Python code, and one function per class
  (`canonical_symbol_stock`, `canonical_symbol_option`, `canonical_symbol_future`,
  `canonical_symbol_future_spread`, `canonical_symbol_index`, `canonical_symbol_forex`,
  `canonical_symbol_crypto`) plus one per stock convention (`canonical_symbol_stock_yahoo`,
  `canonical_symbol_stock_nasdaq`, `canonical_symbol_stock_cms`) for Deephaven query strings (see
  below). Each maps None to None,
  returns canonical text unchanged, converts the spellings its class or convention defines, and,
  for anything else, does what its `on_error` says: raises `ValueError` with a hint
  (`OnError.RAISE`, the default), or returns None and logs the value once (`LOG` at WARNING,
  `IGNORE` at DEBUG). Inside the module a refusal is a `SymbolRefusalError`, which holds the
  reason and the fix apart; a user meets it as a plain `ValueError`. The table converter,
  `dm.canonicalize_symbols` in `_tables/symbols.py`, takes its verdict and its effect from the
  same functions (`strict_converter`, `refusal`, `convert_under`), so a value converts, or is
  refused with one reason, the same way in both, and `m.problems()` gets the reason alone.

`option_roots` computes the `OptionRoot` column from canonical option symbols. The spellings
Massive's transports write instead (`EUR/USD` in WebSocket forex frames, say) are read and
written by `wire_symbols.py`, on the grammar's public names (`AS_WRITTEN`, `CANONICAL_GRAMMARS`,
`read_forex_pair`, `read_crypto_pair`).

In a Deephaven query string, a converter is called with positional arguments only, so a keyword
argument (`convention=`, `on_error=`) is given by putting a `functools.partial` that binds it in
scope under a bare name. How the call is written decides the column's type: a bare name in scope
(`f(Ticker)`) gives a typed `String` column, while a scoped call (`dm.canonical_symbol(...)`, or
any `obj.method(...)`) gives an `Object` column unless cast with `(String)`, because Deephaven
ignores the type hints of an attribute call. An enum member passes only by attribute access
(`dm.AssetClass.STOCKS`).

| Class   | Canonical grammar                                  | Converters also accept         |
|---------|----------------------------------------------------|--------------------------------|
| stocks  | upper-case root; `.X`, `pX`, `r`, `w` (`KIMpL`)    | a named convention's spelling  |
| options | `O:` + OCC (root of 1 to 6 characters, real date)  | no `O:`, padded OSI, any case  |
| futures | a contract (`ESZ5`) or a combo (`GCQ6-GCZ6`)       | nothing                        |
| indices | `I:` + letters and digits (`I:SPX`)                | no `I:` (`SPX`)                |
| forex   | `C:` + two three-letter codes (`C:EURUSD`)         | `EUR/USD`, `EURUSD`, any case  |
| crypto  | `X:` + the joined pair (`X:BTCUSD`)                | `BTC-USD`, `BTCUSD`, any case  |

A futures ticker is a single contract or a combo (a spread, strip, pack, or bundle, which
Massive's contracts list returns with `type: 'combo'`). A contract is strict: a root, a month
code, and one to three digits: a one- or two-digit year (`ESZ5`, `0NGH39`), or three digits as
Massive lists daily contracts (`0AF704`). A combo is any run of upper-case letters and digits
joined by Massive's separators, `-`, `:`, or one space (`GCQ6-GCZ6`, `BOB 02-01 U6`,
`CL:BF Q6-U6-V6`, `1Y:SA 03M F6`): Massive documents no grammar for combos and lists more forms
than its samples show, and every ticker Massive itself writes must round-trip, so the forms are
not enumerated, while whitespace, lower case, and a truncated contract (`ESZ`) are refused.
`canonical_symbol_future` takes contracts, `canonical_symbol_future_spread` combos, and
`canonical_symbol` and a futures dataset's `symbols` either. A dataset whose
symbols are a third party's global tickers (`DatasetSpec.symbols_as_written`) takes and keeps them
as Massive writes them. A stock's case is meaningful (`KIMpL` is Kimco's series L preferred), so
stock, index, and futures text is never re-cased; the option, forex, and crypto grammars are all
upper case, so folding case there loses nothing. A joined crypto ticker is never split by
guessing where its quote starts (`X:DOTUSD` reads as `DOT-USD` and as `DO-TUSD`): the transports
take every split from Massive's crypto ticker list and pass it in (`wire_symbols.ws_symbol`).
Nothing maps one instrument to another: an option root (`SPXW`, `AAPL1`) is never turned into
its underlying, `BRKB` never into `BRK.B`, and a futures year is never rewritten.

Every function here is thread-safe and makes no Deephaven call, and the module imports Deephaven
only for its `Table` type. Every function is pure except a converter under `OnError.LOG` or
`IGNORE`, which records the value once in a process-wide log.

The module is one subject although it runs past 1,000 lines: the checks and every converter
are the grammar applied, one class at a time, and each converter's message names the canonical
form the grammar defines.
"""

from __future__ import annotations

import functools
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from deephaven.table import Table
from typing_extensions import override

from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    OnError,
    SymbolConvention,
    _require_enum,
)
from deephaven_massive._errors import BrokenGuaranteeError, library_bug, raises_through
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._on_error import effect_of

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from deephaven_massive._on_error import OnErrorEffect


__all__ = [
    "ALL",
    "AS_WRITTEN",
    "CANONICAL_GRAMMARS",
    "CLASS_NOUNS",
    "LOGGED_VALUES",
    "UNDERLYING_CLASSES",
    "AllSymbols",
    "NormalizedSymbols",
    "SymbolTable",
    "SymbolsLike",
    "as_written_fault",
    "canonical_symbol",
    "canonical_symbol_crypto",
    "canonical_symbol_forex",
    "canonical_symbol_future",
    "canonical_symbol_future_spread",
    "canonical_symbol_index",
    "canonical_symbol_option",
    "canonical_symbol_stock",
    "canonical_symbol_stock_cms",
    "canonical_symbol_stock_nasdaq",
    "canonical_symbol_stock_yahoo",
    "check_convention",
    "check_symbol",
    "check_symbols",
    "convert_under",
    "forex_codes",
    "is_canonical",
    "is_forex_currency",
    "is_futures_contract",
    "is_lower_case",
    "option_roots",
    "read_crypto_pair",
    "read_forex_pair",
    "refusal",
    "strict_converter",
    "symbol_class",
]

logger = logging.getLogger(__name__)


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

Each symbol, and each row of a table, is Massive's exact ticker, such as `"C:EURUSD"`, not
`"EUR/USD"`; `dm.canonical_symbol` and `dm.canonicalize_symbols` convert other spellings.

Example:
    ::

        from deephaven.table import Table

        import deephaven_massive as dm


        def latest_quotes(m: dm.MassiveSession, symbols: dm.SymbolsLike) -> Table:
            return m.stocks.quotes(symbols).last_by()
"""


UNDERLYING_CLASSES: Final = frozenset({AssetClass.STOCKS, AssetClass.INDICES})
"""The classes whose tickers an option can be linked to: the ones an `underlying` takes."""

_STOCK: Final = re.compile(r"^[A-Z0-9]+(?:\.[A-Z0-9]+|p[A-Z]?|[rw])*$")
"""A canonical stock ticker: an upper-case root, then the CQS suffixes Massive keeps: `.X` (a
class or another dotted suffix, `BRK.B`, `NTEST.Q`, and one or more of them, as in the NYSE
warrants `AAC.WS` and `NE.WS.A`), `p` and an optional series (`KIMpL`), and
the lower-case markers `r` (rights) and `w` (when issued)."""
_OCC: Final = re.compile(r"^O:(?P<root>[A-Z0-9]{1,6})(?P<expiry>\d{6})[CP]\d{8}$")
"""A canonical option symbol: `O:`, the OCC root (no dot), `YYMMDD`, `C` or `P`, and the strike
in thousandths of a dollar, eight digits."""
AS_WRITTEN: Final = re.compile(
    r"^(?![OICX]:)[^\x00-\x20\x7f](?:[^\x00-\x1f\x7f]*[^\x00-\x20\x7f])?$"
)
"""Text kept exactly as Massive writes it: not empty, no control character, no surrounding
whitespace, and no class prefix. How a futures row's ticker is read, so a row Massive sends is
never lost to the grammar, and the grammar of a dataset whose symbols are a third party's global
tickers."""
_CLASS_PREFIX: Final = re.compile(r"^[OICX]:")
"""A class prefix, which `AS_WRITTEN` refuses."""
_CONTROL: Final = re.compile(r"[\x00-\x1f\x7f]")
"""A control character, which `AS_WRITTEN` refuses anywhere."""
_FUTURE: Final = re.compile(r"^[A-Z0-9]+[FGHJKMNQUVXZ][0-9]{1,3}$")
"""A single futures contract, in the forms Massive's contracts list writes: a root of upper-case
letters and digits, a month code, and one to three digits. Most contracts end in a one- or
two-digit year (`ESZ5`, `6CH5`, `NGZ26`, `0NGH39`); daily contracts end in three digits
(`0AF704`, `GCBX602`), which the library does not read as a date. `ESZ`, with no digit, and
`ESZ5310`, with four, are no contract."""
_FUTURES_COMBO: Final = re.compile(r"^(?![OICX]:)[A-Z0-9]+(?:[-: ][A-Z0-9]+)+$")
"""A futures combo (spread, strip, pack, or bundle): runs of upper-case letters and digits, each
joined to the next by one `-`, `:`, or space, as Massive writes them (`GCQ6-GCZ6`,
`BOB 02-01 U6`, `CL:BF Q6-U6-V6`, `6A:XF:AUDUSD:Q6`, `1Y:SA 03M F6`, `10YF5-10YG5XXX`).
Massive documents no grammar for combos, so the shape is checked and the forms are not listed:
a ticker Massive writes always round-trips, while surrounding or doubled whitespace, lower case,
an empty part, and another class's prefix (`X:BTC-USD`) are refused."""
_INDEX: Final = re.compile(r"^I:[A-Z0-9]+(?:\.[A-Z0-9]+)*$")
"""A canonical index ticker: `I:` and upper-case letters and digits, with dotted parts allowed.
Massive's WebSocket specification allows lower case (`^(I:[a-zA-Z0-9]+)$`), but a lower-case
index ticker is refused as not canonical, as lower case is in the other classes."""
_FOREX: Final = re.compile(r"^C:[A-Z]{3}[A-Z]{3}$")
"""A canonical forex ticker: `C:` and two three-letter ISO 4217 codes."""
_CRYPTO: Final = re.compile(r"^X:[A-Z0-9]{4,}$")
"""A canonical crypto ticker: `X:` and the pair's two codes joined, whatever the quote; only a
WebSocket topic needs the pair split into its two codes (`ws_symbol`). A pair is at least a
base of one character and a quote of three, so a lone currency such as `X:BTC` is refused."""
_FOREX_CURRENCY: Final = re.compile(r"^[A-Z]{3}$")
_CURRENCIES: Final = re.compile(r"^[A-Z0-9]+$")
_PAIR: Final = re.compile(r"^(?P<base>[A-Z0-9]+)[/-](?P<quote>[A-Z0-9]+)$")
_PADDED_OSI: Final = re.compile(r"^(?P<root>[A-Z0-9]{1,5}) +(?P<rest>\d{6}[CP]\d{8})$")
_OSI_LENGTH: Final = 21
_ROOT: Final = r"(?P<root>[A-Z]+)"
_CURRENCY_CODE_LENGTH: Final = 3  # ISO 4217
_OCC_TAIL: Final = 15
"""The characters after an OCC root: `YYMMDD`, `C` or `P`, and eight strike digits."""

_PREFIXES: Final[dict[AssetClass, str]] = {
    AssetClass.OPTIONS: "O:",
    AssetClass.INDICES: "I:",
    AssetClass.FOREX: "C:",
    AssetClass.CRYPTO: "X:",
}
_CLASS_BY_PREFIX: Final[dict[str, AssetClass]] = {p: c for c, p in _PREFIXES.items()}
_CLASS_NAMES: Final[dict[AssetClass, str]] = {
    AssetClass.STOCKS: "stock",
    AssetClass.OPTIONS: "option",
    AssetClass.FUTURES: "future",
    AssetClass.INDICES: "index",
    AssetClass.FOREX: "forex",
    AssetClass.CRYPTO: "crypto",
}
"""The singular noun of each class, as the per-class converters are named."""
CLASS_NOUNS: Final[dict[AssetClass, str]] = {**_CLASS_NAMES, AssetClass.FUTURES: "futures"}
"""The noun of each class as messages write it before "ticker" (`a futures ticker`)."""


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
"""A `symbols` argument after `check_symbols`: a sorted tuple of canonical symbols, `ALL`, or a
boxed symbol table."""


# --- the grammar ---------------------------------------------------------------------------------


def _occ_ok(symbol: str) -> bool:
    """Tell whether text is a canonical OCC option symbol whose expiry is a real date.

    Args:
        symbol: The text, with its `O:` prefix.

    Returns:
        True for the OCC shape with a real `YYMMDD` date (`250229` is not one).
    """
    match = _OCC.fullmatch(symbol)
    if match is None:
        return False
    expiry = match["expiry"]
    try:
        date(2000 + int(expiry[:2]), int(expiry[2:4]), int(expiry[4:]))
    except ValueError:
        return False
    return True


def is_futures_contract(text: str) -> bool:
    """Tell whether text is a single futures contract ticker (`ESZ5`), not a combo.

    The futures schedule calendar looks up only such tickers, by their product: a combo
    (`type: 'combo'` in Massive's contracts list) names several legs, and the calendar keeps no
    schedule for it.

    Args:
        text: The text.

    Returns:
        True for a root of upper-case letters and digits, a month code, and one to three
        digits.
    """
    return _FUTURE.fullmatch(text) is not None


def _is_spread(text: str) -> bool:
    """Tell whether text is a futures combo ticker in Massive's shape (`_FUTURES_COMBO`).

    Args:
        text: The text.

    Returns:
        True for runs of upper-case letters and digits joined by single `-`, `:`, or spaces.
    """
    return _FUTURES_COMBO.fullmatch(text) is not None


def _futures_ok(text: str) -> bool:
    """Tell whether text is a futures ticker a dataset takes: a contract or a combo.

    Args:
        text: The text.

    Returns:
        True for a single contract (`ESZ5`) or a combo in Massive's shape (`GCQ6-GCZ6`),
        exactly as written; False for anything else, `esz5` and `ESZ` included.
    """
    return is_futures_contract(text) or _is_spread(text)


def _matches(pattern: re.Pattern[str]) -> Callable[[str], bool]:
    """Return the test that text matches a pattern.

    Args:
        pattern: The class's grammar.

    Returns:
        A function from text to whether it matches.
    """
    return lambda text: pattern.fullmatch(text) is not None


CANONICAL_GRAMMARS: Final[dict[AssetClass, Callable[[str], bool]]] = {
    AssetClass.STOCKS: _matches(_STOCK),
    AssetClass.OPTIONS: _occ_ok,
    AssetClass.FUTURES: _futures_ok,
    AssetClass.INDICES: _matches(_INDEX),
    AssetClass.FOREX: _matches(_FOREX),
    AssetClass.CRYPTO: _matches(_CRYPTO),
}


def symbol_class(symbol: str) -> AssetClass:
    """Return the asset class a symbol's prefix names.

    Args:
        symbol: A symbol, such as `X:BTCUSD` or `AAPL`.

    Returns:
        The class of its prefix (`O:` options, `I:` indices, `C:` forex, `X:` crypto); stocks
        for a symbol without one.
    """
    return _CLASS_BY_PREFIX.get(symbol[:2], AssetClass.STOCKS)


def is_canonical(
    asset_class: AssetClass | ClassFromPrefix, text: str, *, as_written: bool = False
) -> bool:
    """Tell whether text is exactly a canonical Massive ticker of a class.

    Args:
        asset_class: The class whose grammar applies, or `ClassFromPrefix.ANY` for a dataset
            that spans classes: the prefix then selects the class, and text without one is a stock.
        text: The text to test.
        as_written: True for a dataset whose symbols are a third party's global tickers,
            kept as Massive writes them (`DatasetSpec.symbols_as_written`): any text of the
            `AS_WRITTEN` grammar is canonical, whatever the class.

    Returns:
        True when the text is canonical, case and all.
    """
    if as_written:
        return AS_WRITTEN.fullmatch(text) is not None
    resolved = symbol_class(text) if asset_class is ClassFromPrefix.ANY else asset_class
    return CANONICAL_GRAMMARS[resolved](text)


def is_lower_case(text: str) -> bool:
    """Tell whether text has letters and all of them are lower case.

    Massive never writes a ticker that way (every grammar of `CANONICAL_GRAMMARS` starts with
    an upper-case letter or a digit), so such text is the caller's spelling, with the
    upper-case form as its likely fix. Text with no letters, such as `'123'`, is not lower
    case; `'0p'`, with one, is.

    Args:
        text: The text.

    Returns:
        True when `text.lower()` is the text and differs from `text.upper()`.
    """
    return text == text.lower() != text.upper()


def as_written_fault(text: str) -> str:
    """Say which rule of `AS_WRITTEN` text breaks, for the message that refuses it.

    Args:
        text: Text `AS_WRITTEN` refuses.

    Returns:
        The rule it breaks, worded to follow the text, such as `"has surrounding whitespace"`.

    Raises:
        BrokenGuaranteeError: If the text breaks none of the rules, so this function and
            `AS_WRITTEN` disagree.
    """
    if not text:
        return "is empty"
    if _CLASS_PREFIX.match(text):
        return f"starts with the class prefix {text[:2]!r}"
    if _CONTROL.search(text):
        return "holds a control character"
    if text != text.strip(" "):
        return "has surrounding whitespace"
    raise BrokenGuaranteeError(
        library_bug(f"{text!r} breaks no rule of a ticker kept as Massive writes it")
    )


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


class SymbolRefusalError(ValueError):
    """A converter's refusal of text: why it does not convert, and the fix, held apart.

    A `ValueError`, so the converters' callers catch it as one. `reason` says what the text
    is, with its value (`'msft' is not a Massive stock ticker, because Massive's tickers are
    case-sensitive.`); `fix` is the imperative sentence that follows (`If you mean 'MSFT',
    pass 'MSFT'.`), or empty when the reason makes the fix obvious. The message is the two
    joined by a space, or the reason alone. A user never sees this class: `convert_under` and
    `check_symbol` raise its message as a plain `ValueError`, and `m.problems()` takes
    `reason` alone for its `Detail` (`_tables/refused_values.py`).

    Attributes:
        reason: What the text is, as sentences, with its value.
        fix: What to pass instead, as an imperative sentence, or empty.
    """

    def __init__(self, reason: str, fix: str = "") -> None:
        """Create the refusal from its two parts.

        Args:
            reason: What the text is, as sentences, with its value.
            fix: What to pass instead, as an imperative sentence; empty when the reason makes
                it obvious.
        """
        super().__init__(f"{reason} {fix}" if fix else reason)
        self.reason: Final = reason
        self.fix: Final = fix


def _converter_name(asset_class: AssetClass) -> str:
    """Return the name of a class's public converter, for messages.

    Args:
        asset_class: The class.

    Returns:
        For example `dm.canonical_symbol_forex`.
    """
    return f"dm.canonical_symbol_{_CLASS_NAMES[asset_class]}"


_VOWEL_CLASSES: Final = frozenset({AssetClass.OPTIONS, AssetClass.INDICES})
"""The classes whose singular noun takes `an`."""


def _class_noun_with_article(asset_class: AssetClass) -> str:
    """Return a class's singular noun with its article, for messages.

    Args:
        asset_class: The class.

    Returns:
        For example `"a stock"` or `"an index"`.
    """
    article = "an" if asset_class in _VOWEL_CLASSES else "a"
    return f"{article} {CLASS_NOUNS[asset_class]}"


def _refuse_surrounding_whitespace(text: str) -> None:
    """Refuse text that is a ticker only once its surrounding whitespace is removed.

    Args:
        text: The text.

    Raises:
        SymbolRefusalError: If the text holds more than whitespace and starts or ends with
            some, naming the stripped text.
    """
    if text.strip() and text != text.strip():
        raise SymbolRefusalError(f"{text!r} has surrounding whitespace.", f"Pass {text.strip()!r}.")


def _refuse_other_prefix(asset_class: AssetClass, text: str) -> None:
    """Refuse text that carries another class's prefix.

    Args:
        asset_class: The class the caller asked for.
        text: The text.

    Raises:
        SymbolRefusalError: If the text starts with the prefix of another class, for example
            `'C:EURUSD' is a forex ticker, not a stock one. Call
            dm.canonical_symbol_forex('C:EURUSD').`
    """
    prefix = text[:2].upper()
    other = _CLASS_BY_PREFIX.get(prefix)
    if other is not None and other is not asset_class:
        found, wanted = _class_noun_with_article(other), _class_noun_with_article(asset_class)
        raise SymbolRefusalError(
            f"{text!r} is {found} ticker, not {wanted} one.",
            f"Call {_converter_name(other)}({text!r}).",
        )


_SEPARATORS: Final = frozenset("-/ +=^#$*%@!~")
"""Characters other stock conventions use as separators and Massive never does."""


def _convert_stock(text: str) -> str:
    """Say why text is not a Massive stock ticker: without a convention, nothing converts.

    Args:
        text: Text that is not canonical (the caller checked).

    Returns:
        Nothing: it always raises; the return type is the converters' common one.

    Raises:
        SymbolRefusalError: Always. Separators of another convention (`-`, `/`, a space) lead
            with the fix, Yahoo's convention, and name the others; all lower-case text says
            that case matters and names the upper-case ticker to use if that is the one
            meant. It is only offered, never converted to: Massive's tickers are
            case-sensitive, so `kimpl` may mean `KIMpL`.
    """
    _refuse_other_prefix(AssetClass.STOCKS, text)
    _refuse_surrounding_whitespace(text)
    if text.strip() and _SEPARATORS.intersection(text):
        raise SymbolRefusalError(
            f"{text!r} is not a Massive stock ticker.",
            f"If it is Yahoo's spelling, convert it with dm.canonical_symbol({text!r}, "
            "dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO), or name "
            "dm.SymbolConvention.NASDAQ or dm.SymbolConvention.CMS for those spellings.",
        )
    if is_lower_case(text):
        raise SymbolRefusalError(
            f"{text!r} is not a Massive stock ticker, because Massive's tickers are "
            "case-sensitive.",
            f"If you mean {text.upper()!r}, pass {text.upper()!r}.",
        )
    raise SymbolRefusalError(
        f"{text!r} is not a Massive stock ticker, which looks like 'AAPL', 'BRK.B', or 'KIMpL'.",
        "Pass the ticker as m.reference.tickers() lists it.",
    )


def _convert_option(text: str) -> str:
    """Convert an OCC option symbol to Massive's `O:` form.

    Accepts the canonical form, the OCC symbol without `O:`, and the 21-character OSI form
    whose root is padded with spaces (`BRKB  250117C00400000`), in any case.

    Args:
        text: The text.

    Returns:
        `O:` and the OCC symbol, upper case.

    Raises:
        SymbolRefusalError: If the text is no OCC symbol with a real expiry date: an
            OCC-shaped symbol whose date does not exist is refused for its date, and an
            underlying such as `AAPL` is named as one; or if it names another class.
    """
    _refuse_other_prefix(AssetClass.OPTIONS, text)
    body = text.upper().removeprefix("O:")
    padded = _PADDED_OSI.fullmatch(body)
    if padded is not None and len(body) == _OSI_LENGTH:
        body = padded["root"] + padded["rest"]
    symbol = f"O:{body}"
    if _occ_ok(symbol):
        return symbol
    shaped = _OCC.fullmatch(symbol)
    if shaped is not None:
        raise SymbolRefusalError(
            f"{text!r} has the shape of an OCC option symbol, but its expiry "
            f"{shaped['expiry']}, read as YYMMDD, is not a real date.",
            "Pass the symbol as m.options.contracts() lists it.",
        )
    if _STOCK.fullmatch(text) is not None:
        raise SymbolRefusalError(
            f"{text!r} is an underlying, not an OCC option symbol such as 'O:AAPL250117C00150000'.",
            f"Pass a contract's symbol from m.options.contracts({text!r}).",
        )
    raise SymbolRefusalError(
        f"{text!r} is not an OCC option symbol, which looks like 'O:AAPL250117C00150000'.",
        "Pass the symbol as m.options.contracts() lists it.",
    )


def _convert_future(text: str) -> str:
    """Say why text is not a Massive futures ticker: a futures ticker converts from nothing.

    Args:
        text: Text that `canonical_symbol_future` refused as a single contract, or that
            `canonical_symbol` refused as a futures ticker as Massive writes it.

    Returns:
        Nothing: it always raises; the return type is the converters' common one.

    Raises:
        SymbolRefusalError: Always, naming the reason: another class's prefix names that
            class's function, surrounding whitespace names the stripped text, and any other
            text gets the forms a contract and a spread take; nothing is re-cased.
    """
    _refuse_other_prefix(AssetClass.FUTURES, text)
    _refuse_surrounding_whitespace(text)
    raise SymbolRefusalError(
        f"{text!r} is not a Massive futures ticker. A contract looks like 'ESZ5' or 'NGZ26' and "
        "a spread like 'GCQ6-GCZ6', in upper case.",
        "Pass the ticker as m.futures.contracts() lists it.",
    )


def _convert_single_future(text: str) -> str:
    """Return a single futures contract ticker, refusing a spread with the function to use.

    Args:
        text: The text.

    Returns:
        The ticker unchanged.

    Raises:
        SymbolRefusalError: If the text is a spread (`canonical_symbol_future_spread` takes
            it) or no futures ticker at all (`_convert_future`).
    """
    if is_futures_contract(text):
        return text
    if _is_spread(text):
        raise SymbolRefusalError(
            f"{text!r} is a futures combo, such as a spread, not a single contract.",
            f"Call dm.canonical_symbol_future_spread({text!r}).",
        )
    return _convert_future(text)


def _convert_spread(text: str) -> str:
    """Return a futures spread ticker, refusing a single contract with the function to use.

    Args:
        text: The text.

    Returns:
        The ticker unchanged.

    Raises:
        SymbolRefusalError: If the text is a single contract (`canonical_symbol_future` takes
            it), or not in Massive's combo shape; the message describes it.
    """
    if _is_spread(text):
        return text
    if is_futures_contract(text):
        raise SymbolRefusalError(
            f"{text!r} is a single futures contract, not a combo.",
            f"Call dm.canonical_symbol_future({text!r}).",
        )
    _refuse_other_prefix(AssetClass.FUTURES, text)
    _refuse_surrounding_whitespace(text)
    raise SymbolRefusalError(
        f"{text!r} is not a Massive futures combo ticker, which looks like 'GCQ6-GCZ6', "
        "'BOB 02-01 U6', or 'CL:BF Q6-U6-V6'.",
        "Pass the ticker as m.futures.contracts() lists it.",
    )


def _convert_index(text: str) -> str:
    """Convert an index ticker to Massive's `I:` form; the caller named the class.

    Args:
        text: The text, with or without `I:`; its case is kept.

    Returns:
        `I:` and the root, such as `I:SPX`.

    Raises:
        SymbolRefusalError: If the root is not an index ticker, or the text names another
            class. A root with a lower-case letter names the upper-case ticker to use, if that
            is the one meant; it is only offered, never converted to.
    """
    _refuse_other_prefix(AssetClass.INDICES, text)
    symbol = text if text.startswith("I:") else f"I:{text}"
    if symbol != symbol.upper() and _INDEX.fullmatch(symbol.upper()) is not None:
        raise SymbolRefusalError(
            f"{text!r} is not a Massive index ticker. Index tickers are upper case.",
            f"If you mean {symbol.upper()!r}, pass {symbol.upper()!r}.",
        )
    if _INDEX.fullmatch(symbol) is None:
        raise SymbolRefusalError(
            f"{text!r} is not a Massive index ticker, which looks like 'I:SPX'.",
            "Pass the ticker as m.reference.tickers(market=dm.Market.INDICES) lists it.",
        )
    return symbol


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
    """Convert a forex pair to Massive's `C:` form.

    Accepts `EUR/USD`, `EUR-USD`, `EURUSD`, each with or without `C:`, in any case.

    Args:
        text: The text.

    Returns:
        `C:` and the two codes, such as `C:EURUSD`.

    Raises:
        SymbolRefusalError: If the codes are not two three-letter currency codes (text ending
            in `=X`, Yahoo Finance's spelling, gets a message that shows Massive's form), or
            the text names another class.
    """
    _refuse_other_prefix(AssetClass.FOREX, text)
    symbol = read_forex_pair(text.upper(), ("C:",))
    if symbol is None and text.upper().endswith("=X"):
        # Yahoo Finance writes a pair as `EURUSD=X`; reading it would guess at another vendor's
        # grammar, so the message only shows Massive's form.
        raise SymbolRefusalError(
            f"{text!r} is Yahoo Finance's spelling of a forex pair, which the library does not "
            "read.",
            "Write the pair as 'EUR/USD', 'EUR-USD', or 'EURUSD', or pass Massive's ticker, "
            "such as 'C:EURUSD'.",
        )
    if symbol is None:
        raise SymbolRefusalError(
            f"{text!r} is not a forex pair of two three-letter currency codes.",
            "Write it as BASE/QUOTE, such as 'EUR/USD'.",
        )
    return symbol


def _convert_crypto(text: str) -> str:
    """Convert a crypto pair to Massive's `X:` form.

    Accepts `BTC-USD`, `BTC/USD`, `BTCUSD`, each with or without `X:`, in any case, and joins
    the two codes, whatever the quote currency.

    Args:
        text: The text.

    Returns:
        `X:` and the joined pair, such as `X:BTCUSD`.

    Raises:
        SymbolRefusalError: If the text is not a pair of letters and digits, is too short to
            hold a base and a three-letter quote (`BTC` names one currency), or names another
            class.
    """
    _refuse_other_prefix(AssetClass.CRYPTO, text)
    symbol = read_crypto_pair(text.upper(), ("X:",))
    if symbol is None:
        raise SymbolRefusalError(
            f"{text!r} is not a crypto pair.", "Write it as BASE-QUOTE, such as 'BTC-USD'."
        )
    if _CRYPTO.fullmatch(symbol) is None:
        raise SymbolRefusalError(
            f"{text!r} is too short to hold a base currency and a quote currency.",
            "Write the pair as BASE-QUOTE, such as 'BTC-USD'.",
        )
    return symbol


_CONVERTERS: Final[dict[AssetClass, Callable[[str], str]]] = {
    AssetClass.STOCKS: _convert_stock,
    AssetClass.OPTIONS: _convert_option,
    AssetClass.FUTURES: _convert_future,
    AssetClass.INDICES: _convert_index,
    AssetClass.FOREX: _convert_forex,
    AssetClass.CRYPTO: _convert_crypto,
}

_UNDOCUMENTED_SPELLINGS: Final = (
    "Massive documents no spelling for warrants, units, rights, when-issued, "
    "when-distributed, called, or test issues, so the library converts none of them."
)
"""The reason a convention's other spellings are refused, after the text's own sentence."""
_STOCK_CONVENTIONS: Final[dict[SymbolConvention, tuple[tuple[re.Pattern[str], str], ...]]] = {
    SymbolConvention.YAHOO: (
        (re.compile(rf"^{_ROOT}$"), r"\g<root>"),
        (re.compile(rf"^{_ROOT}-P(?P<series>[A-Z])$"), r"\g<root>p\g<series>"),
        (re.compile(rf"^{_ROOT}-(?P<cls>[A-OQ-TV-Z])$"), r"\g<root>.\g<cls>"),
    ),
    SymbolConvention.NASDAQ: (
        (re.compile(rf"^{_ROOT}$"), r"\g<root>"),
        (re.compile(rf"^{_ROOT}-(?P<series>[A-Z]?)$"), r"\g<root>p\g<series>"),
        (re.compile(rf"^{_ROOT}\.(?P<cls>[A-TV-Z])$"), r"\g<root>.\g<cls>"),
    ),
    SymbolConvention.CMS: (
        (re.compile(rf"^{_ROOT}$"), r"\g<root>"),
        (re.compile(rf"^{_ROOT} PR(?P<series>[A-Z]?)$"), r"\g<root>p\g<series>"),
        (re.compile(rf"^{_ROOT} (?P<cls>[A-TV-Z])$"), r"\g<root>.\g<cls>"),
    ),
}
"""Each stock convention's exact spellings of a plain ticker, a preferred, and a share class,
and Massive's form of each (`BACpL`, `BRK.B`). Class `U` is refused everywhere, since CQS and
CMS write units as `U`; Yahoo's bare `-P` is refused, since it reads as a generic preferred and
as class P alike."""


def _convert_stock_from(text: str, convention: SymbolConvention) -> str:
    """Convert a stock ticker written in a named convention to Massive's form.

    Only the convention's own grammar applies: text that happens to be a Massive ticker is
    not passed through, since the convention may mean something else by it (Nasdaq's `ABC.U`
    is class U, Massive's is a unit; Yahoo's `VOD.L` is a London listing).

    Args:
        text: The ticker as the convention writes it, upper case.
        convention: The convention it comes from.

    Returns:
        Massive's ticker: a plain ticker unchanged, a class as `.X`, a preferred as `p` and its
        series.

    Raises:
        SymbolRefusalError: If the text is not one of the convention's plain, class, or
            preferred spellings. Surrounding whitespace names the stripped text, empty text
            says it is empty, all lower-case text names the upper-case form, a Yahoo ticker
            with a dot is named a foreign listing, and every other refusal says which
            suffixes the library does not convert. Empty text and a foreign listing carry no
            fix: the reason makes it obvious.
    """
    _refuse_surrounding_whitespace(text)
    if not text.strip():
        raise SymbolRefusalError(
            f"{text!r} is {'only whitespace' if text else 'empty'}, not a ticker."
        )
    _refuse_other_prefix(AssetClass.STOCKS, text)
    if is_lower_case(text):
        raise SymbolRefusalError(
            f"{text!r} is not a {convention.name} ticker. The {convention.name} convention "
            "writes tickers in upper case.",
            f"If you mean {text.upper()!r}, pass {text.upper()!r}.",
        )
    if convention is SymbolConvention.YAHOO and "." in text:
        raise SymbolRefusalError(
            f"{text!r} is a Yahoo ticker of a listing outside the US, since Yahoo's dot names "
            "the exchange. Massive's stock tickers are US listings, so no Massive ticker matches "
            "it."
        )
    for pattern, template in _STOCK_CONVENTIONS[convention]:
        match = pattern.fullmatch(text)
        if match is not None:
            return match.expand(template)
    raise SymbolRefusalError(
        f"{text!r} is not a plain, class, or preferred ticker of the {convention.name} "
        f"convention. {_UNDOCUMENTED_SPELLINGS}",
        "Pass Massive's own ticker, as m.reference.tickers() lists it.",
    )


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


LOGGED_VALUES: Final = 20
"""The most distinct values that do not convert one log reports: a scalar converter's per
process (`_logged_values`, `_ignored_values`), and one `canonicalize_symbols` table's."""

_logged_values: Final = BoundedLog(logger, limit=LOGGED_VALUES, level=logging.WARNING)
"""The values a scalar converter could not convert under `OnError.LOG`: each once per process."""
_ignored_values: Final = BoundedLog(logger, limit=LOGGED_VALUES, level=logging.DEBUG)
"""The same under `OnError.IGNORE`, at DEBUG."""


def check_convention(
    asset_class: object, convention: object
) -> tuple[AssetClass, SymbolConvention]:
    """Check a converter's class and convention, as the scalar and table converters both take them.

    Args:
        asset_class: The `asset_class` argument.
        convention: The `convention` argument.

    Returns:
        The class and the convention.

    Raises:
        ValueError: If `convention` is a stock convention and the class is not stocks.

    An enum argument of the wrong type raises the `TypeError` of `_require_enum`.
    """
    resolved = _require_enum(asset_class, AssetClass, "asset_class")
    chosen = _require_enum(convention, SymbolConvention, "convention")
    if chosen is not SymbolConvention.MASSIVE and resolved is not AssetClass.STOCKS:
        raise ValueError(
            f"convention=dm.SymbolConvention.{chosen.name} is for stock tickers, not "
            f"{resolved.value}. Pass asset_class=dm.AssetClass.STOCKS, or leave convention out."
        )
    return resolved, chosen


def strict_converter(asset_class: AssetClass, convention: SymbolConvention) -> Callable[[str], str]:
    """Return the one strict verdict on text of a class: its canonical ticker, or `ValueError`.

    The scalar converters and `canonicalize_symbols` both convert through it, so a value is
    accepted, or refused with one reason, the same way in both.

    Args:
        asset_class: The class whose grammar and converter apply.
        convention: The convention the text comes from; a stock convention applies its own
            grammar alone.

    Returns:
        A function from text to its canonical ticker, which raises `ValueError`, with the
        reason and the fix where one exists, for text that does not convert.
    """
    if convention is not SymbolConvention.MASSIVE:
        return functools.partial(_convert_stock_from, convention=convention)
    grammar = CANONICAL_GRAMMARS[asset_class]
    converter = _CONVERTERS[asset_class]

    def convert(text: str) -> str:
        return text if grammar(text) else converter(text)

    return convert


def refusal(text: str, convert: Callable[[str], str]) -> SymbolRefusalError | None:
    """Return why text does not convert, or None when it does.

    Args:
        text: The text.
        convert: The strict verdict (`strict_converter`, or a futures form's converter).

    Returns:
        The refusal, with its `reason` and `fix` apart and the message they make as its
        text; None for text that converts.
    """
    try:
        convert(text)
    except SymbolRefusalError as error:
        # Returned as a value, the refusal must not keep its traceback: through the frames it
        # holds, and each finished frame's caller, it reaches the caller's own locals and
        # forms a cycle with them that only a garbage collection frees.
        return error.with_traceback(None)
    return None


def convert_under(
    text: str,
    convert: Callable[[str], str],
    *,
    effect: OnErrorEffect,
    log: BoundedLog,
    caller: str,
    consequence: str,
) -> str | None:
    """Convert text, doing what an `OnError` setting says with text that does not convert.

    The one place a converter's `on_error` takes effect, for a scalar converter and for each
    row of `canonicalize_symbols`: under `RAISE` the verdict's `ValueError` propagates; under
    `LOG` and `IGNORE` the value is logged once, at the effect's level, through the caller's
    bounded log, and None comes back.

    Args:
        text: The text.
        convert: The strict verdict (`strict_converter`, or a futures form's converter).
        effect: What the caller's `on_error` does (`effect_of`).
        log: The caller's bounded log of values that did not convert: a scalar converter's
            process-wide one, or a table's own.
        caller: The function logged as refusing the value, such as `canonical_symbol`.
        consequence: What the value becomes, as a verb phrase with the value as its
            subject, such as `"becomes None"`.

    Returns:
        The canonical ticker, or None for text that does not convert when the effect does not
        fail.

    Raises:
        ValueError: Under an effect that fails, the converter's refusal, reason and fix
            together.
    """
    try:
        return convert(text)
    except SymbolRefusalError as error:
        if effect.fails:
            # A plain ValueError, from None: the public contract, so neither a traceback nor
            # its chain shows the private class.
            raise ValueError(str(error)) from None
        log.log(text, "%s: %r %s. %s", caller, text, consequence, error)
        return None


def _convert_scalar(
    text: str | None, convert: Callable[[str], str], *, on_error: object, caller: str
) -> str | None:
    """Run a scalar converter: check its text and `on_error`, then convert under the effect.

    Args:
        text: The `text` argument.
        convert: The strict verdict.
        on_error: The `on_error` argument.
        caller: The converter's name, for its log record.

    Returns:
        The canonical ticker; None for None, or for text that does not convert under `LOG`
        and `IGNORE`.

    A `text` that is neither a `str` nor None raises the `TypeError` of `_require_text`, and an
    `on_error` that is no `OnError` member that of `_require_enum`; other errors are those of
    `convert_under`.
    """
    effect = effect_of(_require_enum(on_error, OnError, "on_error"))
    value = _require_text(text)
    if value is None:
        return None
    log = _logged_values if effect.level > logging.DEBUG else _ignored_values
    return convert_under(
        value,
        convert,
        effect=effect,
        log=log,
        caller=caller,
        consequence="becomes None",
    )


@raises_through(_require_enum, _require_text, check_convention)
def canonical_symbol(
    text: str | None,
    asset_class: AssetClass,
    *,
    convention: SymbolConvention = SymbolConvention.MASSIVE,
    on_error: OnError = OnError.RAISE,
) -> str | None:
    """Convert a symbol of a named asset class to Massive's exact REST ticker.

    Use it to turn symbols from another source into the form every dataset takes and every
    `Symbol` column holds, before passing them as `symbols=`. Under `SymbolConvention.MASSIVE`,
    the default, canonical text comes back unchanged; another spelling of the class converts
    (`EUR/USD` to `C:EURUSD`, `SPX` to `I:SPX`); anything else raises. Under another convention,
    only that convention's grammar applies, so text it does not spell raises even when it
    happens to be a Massive ticker. For futures, a single contract (`ESZ5`) or a spread in one
    of Massive's spellings (`GCQ6-GCZ6`) comes back unchanged and anything else raises, with
    the message of `canonical_symbol_future`; a futures dataset's `symbols` take the same two
    forms. The class is never guessed: `SPX` is an index only because
    the caller says so. Text that does not convert does what `on_error` says: raises, the
    default, or comes back as None with one log record per distinct value, so a column of
    mixed spellings in Python can be converted without failing on the first bad one. The same
    verdict and `on_error` apply to every row of `dm.canonicalize_symbols`. A Deephaven query
    string passes positional arguments only: call a convention's function
    (`canonical_symbol_stock_yahoo`), or give `on_error=` through a `functools.partial` of the
    function, imported under a bare name. A function called through the module
    (`dm.canonical_symbol(...)`) makes an `Object` column unless cast with `(String)`.
    Thread-safe.

    Example:
        ::

            import deephaven_massive as dm

            dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX)  # 'C:EURUSD'
            dm.canonical_symbol(
                "BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
            )  # 'BRK.B'

    Args:
        text: The symbol, or None.
        asset_class: The symbol's class, whose rules apply. Required: nothing is inferred.
        convention: The convention the text comes from. `SymbolConvention.MASSIVE`, the
            default, accepts Massive's own spelling and the unambiguous spellings of the class;
            `YAHOO`, `NASDAQ`, and `CMS`, for stocks only, convert from that convention's
            grammar alone. A stock ticker converts only from a named convention, because `-`
            means a class in Yahoo's and a preferred in Nasdaq's.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only.

    Returns:
        The canonical ticker; None for None, or, under `OnError.LOG` and `IGNORE`, for text
        that does not convert.

    Raises:
        TypeError: If `asset_class` is not a `dm.AssetClass`, `convention` is not a
            `dm.SymbolConvention`, `on_error` is not a `dm.OnError`, or `text` is neither a
            `str` nor None.
        ValueError: If `convention` is a stock convention and the class is not stocks, or,
            under `OnError.RAISE`, the default, if the text is not a symbol of the class in any
            accepted spelling. The message gives the reason and, where one exists, the fix: a
            ticker with another class's prefix names that class's function.
    """
    resolved, chosen = check_convention(asset_class, convention)
    return _convert_scalar(
        text, strict_converter(resolved, chosen), on_error=on_error, caller="canonical_symbol"
    )


@raises_through(canonical_symbol)
def canonical_symbol_stock(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Check a stock ticker in Massive's own spelling, for Python code and query strings.

    Massive's spelling is the only one accepted: a ticker from Yahoo, Nasdaq, or a CMS feed
    goes through the function of its convention (`canonical_symbol_stock_yahoo`), because the
    same `-` means a class in one and a preferred in another. Thread-safe.

    Example:
        ::

            from deephaven import new_table
            from deephaven.column import string_col

            from deephaven_massive import canonical_symbol_stock

            t = new_table([string_col("Ticker", ["AAPL", "KIMpL"])])
            t = t.update(["Symbol = canonical_symbol_stock(Ticker)"])

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_stock(Col)`.

    Args:
        text: The ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_stock, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        The ticker unchanged, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not a Massive stock ticker;
            the message names the conventions for
            text with their separators, and the upper-case form for lower-case text.
    """
    return canonical_symbol(text, AssetClass.STOCKS, on_error=on_error)


@raises_through(canonical_symbol)
def canonical_symbol_stock_yahoo(
    text: str | None, *, on_error: OnError = OnError.RAISE
) -> str | None:
    """Convert a Yahoo Finance stock ticker to Massive's: `BRK-B` to `BRK.B`, `BAC-PL` to `BACpL`.

    Yahoo writes a share class as `-X` and a preferred as `-P` and its series. Only plain
    tickers, classes, and preferreds with a series convert: Yahoo's bare `-P` is refused (a
    generic preferred and class P read alike), and warrants (`-WT`), units (`-UN`), and rights
    (`-RI`) are refused, since Massive documents no spelling for them, and so is a ticker with a
    dot, Yahoo's mark of a listing outside the US (`VOD.L`). Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_yahoo`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_yahoo(Col)`.

    Args:
        text: The Yahoo ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_stock_yahoo, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        Massive's ticker, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not a plain, class, or
            preferred Yahoo ticker.
    """
    return canonical_symbol(
        text, AssetClass.STOCKS, convention=SymbolConvention.YAHOO, on_error=on_error
    )


@raises_through(canonical_symbol)
def canonical_symbol_stock_nasdaq(
    text: str | None, *, on_error: OnError = OnError.RAISE
) -> str | None:
    """Convert a Nasdaq Integrated Platform stock ticker to Massive's: `KIM-L` to `KIMpL`.

    Nasdaq's own symbology (the "NASDAQ Symbol" column of `nasdaqtraded.txt`) writes a class as
    `.X` and a preferred as `-` and its optional series. Its other suffixes (`+` warrants, `=`
    units, `^` rights, `#` when issued) are refused, since Massive documents no spelling for
    them. The ACT symbology of the same files writes `.W` for warrants and `.V` for both a
    class and when-issued, so it is ambiguous and not supported. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_nasdaq`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_nasdaq(Col)`.

    Args:
        text: The Nasdaq ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_stock_nasdaq, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        Massive's ticker, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not a plain, class, or
            preferred Nasdaq ticker.
    """
    return canonical_symbol(
        text, AssetClass.STOCKS, convention=SymbolConvention.NASDAQ, on_error=on_error
    )


@raises_through(canonical_symbol)
def canonical_symbol_stock_cms(
    text: str | None, *, on_error: OnError = OnError.RAISE
) -> str | None:
    """Convert a CMS stock ticker to Massive's: `BRK B` to `BRK.B`, `BAC PRL` to `BACpL`.

    The CMS convention (the root, a space, and a suffix, as many broker and FIX systems write
    it) spells a class as ` X` and a preferred as ` PR` and its optional series. Its other
    suffixes (` WS`, ` U`, ` RT`, ` WI`) are refused, since Massive documents no spelling for
    them. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_cms`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_cms(Col)`.

    Args:
        text: The CMS ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_stock_cms, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        Massive's ticker, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not a plain, class, or
            preferred CMS ticker.
    """
    return canonical_symbol(
        text, AssetClass.STOCKS, convention=SymbolConvention.CMS, on_error=on_error
    )


@raises_through(canonical_symbol)
def canonical_symbol_option(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Convert an OCC option symbol to Massive's: `AAPL250117C00150000` to `O:AAPL250117C00150000`.

    Accepts the symbol with or without `O:` and the 21-character OSI form whose root is padded
    with spaces, in any case. The root is never changed: `SPXW` stays `SPXW` and an adjusted
    root such as `AAPL1` stays `AAPL1`. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_option`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_option(Col)`.

    Args:
        text: The option symbol, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_option, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        `O:` and the OCC symbol, or None for None or, under `OnError.LOG` and `IGNORE`, for text
        that does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is no OCC symbol with a real
            expiry date (an underlying such as
            `AAPL` is named as one), or carries another class's prefix.
    """
    return canonical_symbol(text, AssetClass.OPTIONS, on_error=on_error)


@raises_through(_require_enum, _require_text, _convert_single_future)
def canonical_symbol_future(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Check a single futures contract ticker, which Massive's text is exactly: `ESZ5`.

    A contract ticker is a root of upper-case letters and digits, a month code, and one to
    three digits, taken as written: nothing is converted or re-cased. Most contracts end in a
    one- or two-digit year (`ESZ5`, `NGZ26`), and daily contracts in three digits (`0AF704`,
    `GCBX602`). A spread is refused with the function that takes it
    (`canonical_symbol_future_spread`). Beware the year: Massive writes it with one digit for
    most products (`ESZ5` is December 2025) and with two for some (`NGZ26`, `0NGH39`), so
    `ESZ25` is not `ESZ5`. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_future`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_future(Col)`.

    Args:
        text: The contract ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_future, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        The ticker unchanged, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is a combo, or not a root, a
            month code, and one to three digits.
    """
    return _convert_scalar(
        text, _convert_single_future, on_error=on_error, caller="canonical_symbol_future"
    )


@raises_through(_require_enum, _require_text, _convert_spread)
def canonical_symbol_future_spread(
    text: str | None, *, on_error: OnError = OnError.RAISE
) -> str | None:
    """Check a futures combo ticker, exactly as Massive writes it: `GCQ6-GCZ6`, `BOB 02-01 U6`.

    Any combo (spread, strip, pack, or bundle) Massive writes is accepted: runs of upper-case
    letters and digits joined by single `-`, `:`, or spaces (`GCQ6-GCZ6`, `BOB 02-01 U6`,
    `CL:BF Q6-U6-V6`, `6A:XF:AUDUSD:Q6`). Massive documents no grammar for combos, so the shape
    is checked rather than a list of forms. Canonical text comes back unchanged; no ticker is
    built from its legs. A single contract is refused with `canonical_symbol_future`. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_future_spread`) for a `String` column; called through the module it makes
    an `Object` column unless cast: `(String) dm.canonical_symbol_future_spread(Col)`.

    Args:
        text: The spread ticker, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_future_spread, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        The ticker unchanged, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is a single contract, or not in
            Massive's combo shape (the
            message describes it).
    """
    return _convert_scalar(
        text, _convert_spread, on_error=on_error, caller="canonical_symbol_future_spread"
    )


@raises_through(canonical_symbol)
def canonical_symbol_index(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Convert an index ticker to Massive's: `SPX` to `I:SPX`.

    The prefix is added because the caller names the class by calling this function; no
    ticker is ever recognized as an index by its name. Case is kept. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_index`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_index(Col)`.

    Args:
        text: The index ticker, with or without `I:`, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_index, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        `I:` and the ticker, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not an index ticker or
            carries another class's prefix.
    """
    return canonical_symbol(text, AssetClass.INDICES, on_error=on_error)


@raises_through(canonical_symbol)
def canonical_symbol_forex(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Convert a currency pair to Massive's forex ticker: `EUR/USD` to `C:EURUSD`.

    Accepts `EUR/USD`, `EUR-USD`, and `EURUSD`, with or without `C:`, in any case. Thread-safe.

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
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_forex, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        `C:` and the two codes, or None for None or, under `OnError.LOG` and `IGNORE`, for text that
        does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not two three-letter codes,
            or carries another class's prefix
            (`X:` is crypto). Yahoo Finance's spelling, `EURUSD=X`, is refused with a message that
            shows Massive's form: nothing is guessed from another vendor's grammar.
    """
    return canonical_symbol(text, AssetClass.FOREX, on_error=on_error)


@raises_through(canonical_symbol)
def canonical_symbol_crypto(text: str | None, *, on_error: OnError = OnError.RAISE) -> str | None:
    """Convert a crypto pair to Massive's ticker: `BTC-USD` to `X:BTCUSD`.

    Accepts `BTC-USD`, `BTC/USD`, and `BTCUSD`, with or without `X:`, in any case, whatever the
    quote currency, and joins the two codes; any such ticker is canonical. A stream, the
    last-trade path, and the daily open/close path need the pair split into its two codes, and
    take the split only from Massive's crypto ticker list, so they refuse a pair that list does
    not hold; every other verb takes any pair. Thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_crypto`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_crypto(Col)`.

    Args:
        text: The pair, or None.
        on_error: What text that does not convert does. `OnError.RAISE`, the default,
            raises `ValueError`; `OnError.LOG` returns None and logs the value once at
            WARNING, at most `LOGGED_VALUES` values a process; `OnError.IGNORE` returns None
            and logs it at DEBUG only. A query string passes positional arguments only, so
            there a bad value fails the whole formula unless the function is in scope as a
            `functools.partial` that binds `on_error`
            (`lenient = partial(canonical_symbol_crypto, on_error=dm.OnError.LOG)`, then
            `lenient(Ticker)`); `dm.canonicalize_symbols` converts a whole table.

    Returns:
        `X:` and the joined pair, or None for None or, under `OnError.LOG` and `IGNORE`, for text
        that does not convert.

    Raises:
        TypeError: If `text` is neither a `str` nor None, or `on_error` is not a
            `dm.OnError`.
        ValueError: Under `OnError.RAISE`, the default, if the text is not a pair of letters and
            digits, is too short to hold a
            base and a three-letter quote (`BTC` names one currency, not a pair), or carries
            another class's prefix (`C:` is forex).
    """
    return canonical_symbol(text, AssetClass.CRYPTO, on_error=on_error)


# --- checks when a query is built ----------------------------------------------------------------


def check_symbol(
    asset_class: AssetClass | ClassFromPrefix,
    text: object,
    *,
    as_written: bool = False,
    underlying: bool = False,
) -> str:
    """Return a symbol that must already be canonical, or raise with the fix.

    The check behind `symbols=`, `underlying=`, and every row of a symbol table: the library
    never converts what a caller passes, it tells the caller what to pass.

    Args:
        asset_class: The class whose grammar applies, or `ClassFromPrefix.ANY` for a dataset
            that spans classes (the prefix selects the class; text without one is a stock).
        text: The value as passed.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`is_canonical`): any text of that grammar passes.
        underlying: True where the symbol is an option's underlying, which only a stock or
            index ticker can be (`UNDERLYING_CLASSES`): a canonical ticker of another class is
            refused, as a symbol table's row is (`watchlist.SymbolRowRule`), because Massive
            answers a currency or crypto pair asked for as an underlying with no error and no
            rows (the contracts list did).

    Returns:
        The text, unchanged.

    Raises:
        TypeError: If the value is not a `str`.
        ValueError: If the text is not canonical. A canonical ticker of another class says
            the dataset takes the other class and names the namespace of that class's
            datasets; otherwise the message names the canonical form when the class's
            converter gives one (`'EUR/USD' is not Massive's forex ticker. Pass 'C:EURUSD',
            ...`), and the converter's reason when it gives none. With `underlying`, a
            canonical ticker that is no stock's or index's names its class and the tickers to
            pass.
    """
    if not isinstance(text, str):
        raise TypeError(f"a symbol must be a str, not {type(text).__name__}")
    if is_canonical(asset_class, text, as_written=as_written):
        if underlying and symbol_class(text) not in UNDERLYING_CLASSES:
            raise ValueError(
                f"{text!r} is {_class_noun_with_article(symbol_class(text))} ticker, and "
                "options are listed on stocks and indices only. Pass a stock or index ticker, "
                "such as 'AAPL' or 'I:SPX'."
            )
        return text
    if as_written:
        raise ValueError(
            f"{text!r} {as_written_fault(text)}, and Massive never writes a ticker that way. "
            "Pass the value as Massive lists it."
        )
    resolved = symbol_class(text) if asset_class is ClassFromPrefix.ANY else asset_class
    other = symbol_class(text)
    if other not in {resolved, AssetClass.STOCKS} and CANONICAL_GRAMMARS[other](text):
        raise ValueError(
            f"{text!r} is {_class_noun_with_article(other)} ticker, and this dataset takes "
            f"{CLASS_NOUNS[resolved]} tickers. Pass {_class_noun_with_article(resolved)} "
            f"ticker, or read {CLASS_NOUNS[other]} data through m.{other.value}."
        )
    try:
        fixed = _CONVERTERS[resolved](text)
    except SymbolRefusalError as error:
        # A plain ValueError, from None: the public contract, so neither a traceback nor its
        # chain shows the private class.
        raise ValueError(str(error)) from None
    raise ValueError(
        f"{text!r} is not Massive's {CLASS_NOUNS[resolved]} ticker. Pass {fixed!r}, which "
        f"{_converter_name(resolved)}({text!r}) returns."
    )


def check_symbols(
    asset_class: AssetClass | ClassFromPrefix,
    symbols: str | Iterable[str] | AllSymbols,
    *,
    as_written: bool = False,
    underlying: bool = False,
) -> tuple[str, ...] | AllSymbols:
    """Check a `Symbols` argument other than a table when a query is built.

    Rules: `dm.ALL` passes through; a `str` is one symbol; any other iterable of strings,
    including a generator, is materialized now. Every symbol must be canonical
    (`check_symbol`). The resulting tuple is sorted and de-duplicated, so equal symbol sets
    give equal queries and one stream. A Deephaven `Table` never reaches this function: the
    dataset factory boxes it in a `SymbolTable` first.

    Args:
        asset_class: The class whose grammar applies, or `ClassFromPrefix.ANY` for a dataset
            that spans classes.
        symbols: The argument as the user passed it.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`check_symbol`).
        underlying: True for a dataset whose symbols are option underlyings, stock and index
            tickers only (`check_symbol`).

    Returns:
        A sorted tuple of canonical symbols, or `ALL`.

    Raises:
        TypeError: If `symbols` is not a `str`, an iterable of `str`, or `dm.ALL`, or an
            element is not a `str`.
        ValueError: If no symbols remain, for example from an exhausted generator, or a
            symbol is not canonical (`check_symbol`).
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
                "symbols must be a str, a sequence of str, a Deephaven Table, or dm.ALL, "
                f"not {type(symbols).__name__}"
            ) from None
    if not items:
        raise ValueError(
            "symbols holds no tickers. Pass at least one ticker, or dm.ALL for every ticker "
            "where the dataset takes it."
        )
    return tuple(
        sorted(
            {
                check_symbol(asset_class, i, as_written=as_written, underlying=underlying)
                for i in items
            }
        )
    )


# --- the wire ------------------------------------------------------------------------------------


def forex_codes(symbol: str) -> tuple[str, str]:
    """Split a canonical forex ticker into its two ISO 4217 codes.

    A forex ticker is `C:` and two three-letter codes joined, so, unlike a crypto pair, it
    splits by position, with no lookup and no guess: `C:EURUSD` is `("EUR", "USD")`.

    Args:
        symbol: A canonical forex ticker, such as `C:EURUSD`.

    Returns:
        `(base, quote)`, such as `("EUR", "USD")`.
    """
    body = symbol.removeprefix("C:")
    return body[:_CURRENCY_CODE_LENGTH], body[_CURRENCY_CODE_LENGTH:]


def option_roots(symbols: pa.Array) -> pa.Array:
    """Return the OCC root of each canonical option symbol: the `OptionRoot` column.

    The root is the symbol without its `O:` prefix and its last 15 characters (expiry, type,
    strike): `SPXW` for `O:SPXW251219C06000000`, `AAPL1` for an adjusted contract. It is read
    from the symbol, never mapped to an underlying. A symbol of another class (`AAPL`,
    `X:BTCUSD`), which a dataset of every class mixes with options contracts, has no root.

    Args:
        symbols: Canonical symbols, nulls allowed.

    Returns:
        A string array of the roots; null where the symbol is null or not an options contract.
    """
    slicing = pc.SliceOptions(start=2, stop=-_OCC_TAIL)
    roots = pc.call_function("utf8_slice_codeunits", [symbols], slicing)
    return pc.if_else(pc.starts_with(symbols, "O:"), roots, pa.scalar(None, pa.string()))
