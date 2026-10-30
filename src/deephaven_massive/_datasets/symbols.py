"""Symbols: Massive's canonical tickers, the converters to them, and the wire spellings.

Every symbol the library takes or shows is Massive's exact REST ticker, case kept: `AAPL`,
`BRK.B`, `KIMpL`, `O:BRKB250117C00400000`, `ESZ5`, `I:SPX`, `C:EURUSD`, `X:BTCUSD`. This module
owns that grammar and three things built on it:

- **The checks.** `normalize` and `check_symbol` accept only canonical text when a query is
  built (`symbols=`, `underlying=`, the rows of a symbol table); anything else raises
  `ValueError`, and the message names the canonical form when the class's converter gives one.
  `is_canonical` is the test itself.
- **The converters**, the public way to turn another spelling into Massive's:
  `canonical_symbol(text, asset_class, convention=)` for Python code, and one function per class
  (`canonical_symbol_stock`, `canonical_symbol_option`, `canonical_symbol_future`,
  `canonical_symbol_future_spread`, `canonical_symbol_index`, `canonical_symbol_forex`,
  `canonical_symbol_crypto`) plus one per stock convention (`canonical_symbol_stock_yahoo`,
  `canonical_symbol_stock_nasdaq`, `canonical_symbol_stock_cms`) for Deephaven query strings, which
  cannot pass an enum. Each maps None to None, returns canonical text unchanged, converts the
  spellings its class or convention defines, and raises `ValueError` with a hint for anything else.
- **The wire spellings.** Massive itself writes a few symbols differently per transport:
  `EUR/USD` in WebSocket forex frames, `BTC-USD` in WebSocket crypto frames, `X:EUR-USD` in the
  forex quotes flat-file sample. `canonical_from_wire` reads every spelling the library knows
  into the canonical form, `WireSymbols` applies it to a column and the session's
  `unknown_symbols` rule to what it cannot read, `wire_spellings` lists the spellings for a
  flat-file filter, and `ws_symbol` writes the topic form. An index endpoint may send an index
  without its `I:` prefix, so `canonical_from_wire` adds it there, from the endpoint's class
  alone: no conversion ever looks a name up. `option_roots` computes the `OptionRoot` column.

| Class   | Canonical grammar                                  | Converters also accept         |
|---------|----------------------------------------------------|--------------------------------|
| stocks  | upper-case root; `.X`, `pX`, `r`, `w` (`KIMpL`)    | a named convention's spelling  |
| options | `O:` + OCC (root of 1 to 6 characters, real date)  | no `O:`, padded OSI, any case  |
| futures | any ticker as Massive writes it (`ESZ5`)           | nothing                        |
| indices | `I:` + letters and digits (`I:SPX`)                | no `I:` (`SPX`)                |
| forex   | `C:` + two three-letter codes (`C:EURUSD`)         | `EUR/USD`, `EURUSD`, any case  |
| crypto  | `X:` + the joined pair (`X:BTCUSD`)                | `BTC-USD`, `BTCUSD`, any case  |

A futures spread is one of the spellings Massive's samples show (`_SPREADS`: `GCQ6-GCZ6`, `BOB 02-01
U6`, `CL:BF Q6-U6-V6`, `6A:XF:AUDUSD:Q6`); `canonical_symbol_future` takes single contracts and
`canonical_symbol_future_spread` spreads, strictly, while a dataset takes, and a futures row keeps,
any futures ticker as Massive writes it, so what comes out can always go back in. A dataset whose
symbols are a third party's global tickers (`DatasetSpec.symbols_as_written`) takes and keeps them
as Massive writes them. A stock's case is meaningful (`KIMpL` is Kimco's series L preferred), so
stock, index, and futures text is never re-cased; the option, forex, and crypto grammars are all
upper case, so folding case there loses nothing. A joined crypto ticker is never split by
guessing where its quote starts (`X:DOTUSD` reads as `DOT-USD` and as `DO-TUSD`): the transports
take every split from Massive's crypto ticker list and pass it in (`ws_symbol`). Nothing maps one
instrument to another: an option root (`SPXW`, `AAPL1`) is never turned into its underlying,
`BRKB` never into `BRK.B`, and a futures year is never rewritten.

Every function here is pure and thread-safe, makes no Deephaven call, and imports Deephaven for
type checking only.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    SymbolConvention,
    UnknownSymbols,
    _require_enum,
)
from deephaven_massive._errors import raises_through
from deephaven_massive._log_limits import BoundedWarnings
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven.table import Table

__all__ = [
    "NormalizedSymbols",
    "ReadSymbols",
    "SymbolTable",
    "WireSymbols",
    "canonical_from_wire",
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
    "check_symbol",
    "forex_codes",
    "is_canonical",
    "is_forex_currency",
    "is_futures_contract",
    "normalize",
    "option_roots",
    "symbol_class",
    "unknown_symbol_message",
    "wire_spellings",
    "ws_symbol",
]

logger = logging.getLogger(__name__)

_STOCK: Final = re.compile(r"^[A-Z0-9]+(?:\.[A-Z0-9]+|p[A-Z]?|[rw])*$")
"""A canonical stock ticker: an upper-case root, then the CQS suffixes Massive keeps: `.X` (a
class or another dotted suffix, `BRK.B`, `NTEST.Q`), `p` and an optional series (`KIMpL`), and
the lower-case markers `r` (rights) and `w` (when issued)."""
_OCC: Final = re.compile(r"^O:(?P<root>[A-Z0-9]{1,6})(?P<expiry>\d{6})[CP]\d{8}$")
"""A canonical option symbol: `O:`, the OCC root (no dot), `YYMMDD`, `C` or `P`, and the strike
in thousandths of a dollar, eight digits."""
_WRITTEN: Final = re.compile(r"^(?![OICX]:)[^\x00-\x20\x7f](?:[^\x00-\x1f\x7f]*[^\x00-\x20\x7f])?$")
"""Text kept exactly as Massive writes it: not empty, no control character, no surrounding
whitespace, and no class prefix. How a futures row's ticker is read (a combo form not listed in
`_SPREADS` still keeps its row), and the grammar of a dataset whose symbols are a third party's
global tickers."""
_FUTURE: Final = re.compile(r"^(?=[A-Z0-9]*[A-Z])[A-Z0-9]+$")
"""A single futures contract: upper-case letters and digits, at least one letter (`ESZ5`,
`6CH5`, `0NGH39`)."""
_MONTH: Final = r"[FGHJKMNQUVXZ][0-9]{1,2}"
_SPREADS: Final = (
    # A calendar spread of two contracts: `GCQ6-GCZ6`, `10YN6-10YQ6`, `6AU7-6AU6`
    # (vendor/massive/flat-files/futures/minute-aggregates/comex.md and cbot.md,
    # session-aggregates/cme.md).
    re.compile(r"^[A-Z0-9]+[FGHJKMNQUVXZ][0-9]{1,2}-[A-Z0-9]+[FGHJKMNQUVXZ][0-9]{1,2}$"),
    # A CBOT inter-commodity spread: `BOB 02-01 U6`, `TEX 07-05 U6`
    # (vendor/massive/flat-files/futures/minute-aggregates/cbot.md, session-aggregates/cbot.md).
    re.compile(rf"^[A-Z0-9]{{3}} [0-9]{{2}}-[0-9]{{2}} {_MONTH}$"),
    # A NYMEX strategy of one or two products over months: the butterfly `CL:BF Q6-U6-V6`,
    # the calendar `CL:BZ U6-V6` (vendor/massive/flat-files/futures/minute-aggregates/nymex.md,
    # session-aggregates/nymex.md).
    re.compile(rf"^[A-Z0-9]+:[A-Z0-9]+ {_MONTH}(?:-{_MONTH})+$"),
    # A CME currency cross: `6A:XF:AUDUSD:Q6` (vendor/massive/flat-files/futures/
    # session-aggregates/cme.md).
    re.compile(rf"^[A-Z0-9]+(?::[A-Z0-9]+)+:{_MONTH}$"),
)
"""Massive's spellings of a futures spread, each as written in its vendored samples. Massive
documents no grammar for them (only `type: 'combo'`), so this list is what the samples show,
and no ticker is ever built from its legs."""
_INDEX: Final = re.compile(r"^I:[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*$")
"""A canonical index ticker: `I:` and letters and digits, as Massive's WebSocket specification
writes the pattern (`^(I:[a-zA-Z0-9]+)$`), with dotted parts allowed."""
_FOREX: Final = re.compile(r"^C:[A-Z]{3}[A-Z]{3}$")
"""A canonical forex ticker: `C:` and two three-letter ISO 4217 codes."""
_CRYPTO: Final = re.compile(r"^X:[A-Z0-9]+$")
"""A canonical crypto ticker: `X:` and the pair's two codes joined, whatever the quote; only a
WebSocket topic needs the pair split into its two codes (`ws_symbol`)."""
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
_CLASS_NOUNS: Final[dict[AssetClass, str]] = {**_CLASS_NAMES, AssetClass.FUTURES: "futures"}
"""The noun of each class as messages write it before "ticker" (`a futures ticker`)."""


@dataclass(frozen=True, slots=True)
class SymbolTable:
    """A table passed as `symbols`, compared and hashed by the identity of its Python wrapper.

    A refreshing table makes a reactive stream, which follows the table's rows as they change;
    the stream verbs read a static table once, as a fixed symbol list, and `fetch()` and
    `history()` read either kind once.

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


NormalizedSymbols: TypeAlias = tuple[str, ...] | _All | SymbolTable
"""A `Symbols` argument after `normalize`: a sorted tuple of canonical symbols, `ALL`, or a
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
    """Tell whether text is a single futures contract ticker (`ESZ5`), not a spread.

    The futures schedule calendar looks up only such tickers: every other futures ticker is a
    spread or combination, which Massive's contracts list does not hold.

    Args:
        text: The text.

    Returns:
        True for upper-case letters and digits with at least one letter.
    """
    return _FUTURE.fullmatch(text) is not None


def _is_spread(text: str) -> bool:
    """Tell whether text is one of Massive's futures spread spellings (`_SPREADS`).

    Args:
        text: The text.

    Returns:
        True for a calendar spread, a CBOT inter-commodity spread, a NYMEX strategy, or a CME
        currency cross, each exactly as Massive writes it.
    """
    return any(pattern.fullmatch(text) is not None for pattern in _SPREADS)


def _matches(pattern: re.Pattern[str]) -> Callable[[str], bool]:
    """Return the test that text matches a pattern.

    Args:
        pattern: The class's grammar.

    Returns:
        A function from text to whether it matches.
    """
    return lambda text: pattern.fullmatch(text) is not None


_CANONICAL: Final[dict[AssetClass, Callable[[str], bool]]] = {
    AssetClass.STOCKS: _matches(_STOCK),
    AssetClass.OPTIONS: _occ_ok,
    AssetClass.FUTURES: _matches(_WRITTEN),
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
            `_WRITTEN` grammar is canonical, whatever the class.

    Returns:
        True when the text is canonical, case and all.
    """
    if as_written:
        return _WRITTEN.fullmatch(text) is not None
    resolved = symbol_class(text) if asset_class is ClassFromPrefix.ANY else asset_class
    return _CANONICAL[resolved](text)


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


def _function(asset_class: AssetClass) -> str:
    """Return the name of a class's public converter, for messages.

    Args:
        asset_class: The class.

    Returns:
        For example `dm.canonical_symbol_forex`.
    """
    return f"dm.canonical_symbol_{_CLASS_NAMES[asset_class]}"


def _refuse_other_prefix(asset_class: AssetClass, text: str) -> None:
    """Refuse text that carries another class's prefix.

    Args:
        asset_class: The class the caller asked for.
        text: The text.

    Raises:
        ValueError: If the text starts with the prefix of another class, for example
            `'C:EURUSD' is a forex ticker (prefix C:); call dm.canonical_symbol_forex`.
    """
    prefix = text[:2].upper()
    other = _CLASS_BY_PREFIX.get(prefix)
    if other is not None and other is not asset_class:
        raise ValueError(
            f"{text!r} is a{'n' if other in _VOWEL_CLASSES else ''} {_CLASS_NOUNS[other]} "
            f"ticker (prefix {prefix}), not a{'n' if asset_class in _VOWEL_CLASSES else ''} "
            f"{_CLASS_NOUNS[asset_class]} one; call {_function(other)}"
        )


_VOWEL_CLASSES: Final = frozenset({AssetClass.OPTIONS, AssetClass.INDICES})
"""The classes whose singular noun takes `an`."""
_SEPARATORS: Final = frozenset("-/ +=^#$*%@!~")
"""Characters other stock conventions use as separators and Massive never does."""
_CONVENTIONS: Final = ", ".join(f"dm.SymbolConvention.{c.name}" for c in SymbolConvention)


def _convert_stock(text: str) -> str:
    """Say why text is not a Massive stock ticker: without a convention, nothing converts.

    Args:
        text: Text that is not canonical (the caller checked).

    Returns:
        Nothing: it always raises; the return type is the converters' common one.

    Raises:
        ValueError: Always. Separators of another convention (`-`, `/`, a space) name the
            conventions that convert them; all lower-case text says that case matters, with no
            guess at the ticker, since Massive's tickers are case-sensitive.
    """
    _refuse_other_prefix(AssetClass.STOCKS, text)
    if text.strip() and text != text.strip():
        raise ValueError(f"{text!r} has surrounding whitespace; write {text.strip()!r}")
    if text.strip() and _SEPARATORS.intersection(text):
        raise ValueError(
            f"{text!r} is not a Massive stock ticker: it uses another convention's separators. "
            "Name the convention it comes from: dm.canonical_symbol(text, dm.AssetClass.STOCKS, "
            f"convention=...) with one of {_CONVENTIONS}, or in a query string "
            "dm.canonical_symbol_stock_yahoo, _nasdaq, or _cms"
        )
    if text == text.lower() != text.upper():
        raise ValueError(
            f"{text!r} is not a Massive stock ticker: tickers are case-sensitive, with an "
            "upper-case root and possibly a lower-case suffix (KIMpL is a preferred); write "
            "Massive's ticker exactly"
        )
    raise ValueError(f"{text!r} is not a Massive stock ticker such as 'AAPL', 'BRK.B', or 'KIMpL'")


def _convert_option(text: str) -> str:
    """Convert an OCC option symbol to Massive's `O:` form.

    Accepts the canonical form, the OCC symbol without `O:`, and the 21-character OSI form
    whose root is padded with spaces (`BRKB  250117C00400000`), in any case.

    Args:
        text: The text.

    Returns:
        `O:` and the OCC symbol, upper case.

    Raises:
        ValueError: If the text is no OCC symbol with a real expiry date (an underlying such
            as `AAPL` is named as one), or names another class.
    """
    _refuse_other_prefix(AssetClass.OPTIONS, text)
    body = text.upper().removeprefix("O:")
    padded = _PADDED_OSI.fullmatch(body)
    if padded is not None and len(body) == _OSI_LENGTH:
        body = padded["root"] + padded["rest"]
    symbol = f"O:{body}"
    if _occ_ok(symbol):
        return symbol
    if _STOCK.fullmatch(text) is not None:
        raise ValueError(
            f"{text!r} is an underlying, not an OCC option symbol such as 'O:AAPL250117C00150000'"
        )
    raise ValueError(
        f"{text!r} is not an OCC option symbol with a real expiry date, such as "
        "'O:AAPL250117C00150000'"
    )


def _convert_future(text: str) -> str:
    """Say why text is not a Massive futures ticker: a futures ticker converts from nothing.

    Args:
        text: Text that is neither a single contract nor a spread (the caller checked).

    Returns:
        Nothing: it always raises; the return type is the converters' common one.

    Raises:
        ValueError: Always, naming the reason: another class's prefix names that class's
            function, surrounding whitespace names the stripped text, and lower case says the
            ticker is upper case; nothing is re-cased.
    """
    _refuse_other_prefix(AssetClass.FUTURES, text)
    if text.strip() and text != text.strip():
        raise ValueError(f"{text!r} has surrounding whitespace; write {text.strip()!r}")
    raise ValueError(
        f"{text!r} is not a Massive futures ticker: a contract is upper-case letters and digits "
        "('ESZ5'), and a spread is written as Massive writes it ('GCQ6-GCZ6', 'BOB 02-01 U6', "
        "'CL:BF Q6-U6-V6', '6A:XF:AUDUSD:Q6')"
    )


def _convert_single_future(text: str) -> str:
    """Return a single futures contract ticker, refusing a spread with the function to use.

    Args:
        text: The text.

    Returns:
        The ticker unchanged.

    Raises:
        ValueError: If the text is a spread (`canonical_symbol_future_spread` takes it) or no
            futures ticker at all (`_convert_future`).
    """
    if is_futures_contract(text):
        return text
    if _is_spread(text):
        raise ValueError(
            f"{text!r} is a futures spread, not a single contract; call "
            "dm.canonical_symbol_future_spread"
        )
    return _convert_future(text)


def _convert_spread(text: str) -> str:
    """Return a futures spread ticker, refusing a single contract with the function to use.

    Args:
        text: The text.

    Returns:
        The ticker unchanged.

    Raises:
        ValueError: If the text is a single contract (`canonical_symbol_future` takes it), or
            none of Massive's spread spellings; the message lists them.
    """
    if _is_spread(text):
        return text
    if is_futures_contract(text):
        raise ValueError(
            f"{text!r} is a single futures contract, not a spread; call dm.canonical_symbol_future"
        )
    _refuse_other_prefix(AssetClass.FUTURES, text)
    if text.strip() and text != text.strip():
        raise ValueError(f"{text!r} has surrounding whitespace; write {text.strip()!r}")
    raise ValueError(
        f"{text!r} is not one of Massive's futures spread spellings: a calendar spread "
        "'GCQ6-GCZ6', a CBOT inter-commodity spread 'BOB 02-01 U6', a NYMEX strategy "
        "'CL:BF Q6-U6-V6' or 'CL:BZ U6-V6', or a CME currency cross '6A:XF:AUDUSD:Q6'"
    )


def _convert_index(text: str) -> str:
    """Convert an index ticker to Massive's `I:` form; the caller named the class.

    Args:
        text: The text, with or without `I:`; its case is kept.

    Returns:
        `I:` and the root, such as `I:SPX`.

    Raises:
        ValueError: If the root is not an index ticker, or the text names another class.
    """
    _refuse_other_prefix(AssetClass.INDICES, text)
    symbol = text if text.startswith("I:") else f"I:{text}"
    if _INDEX.fullmatch(symbol) is None:
        raise ValueError(f"{text!r} is not a Massive index ticker such as 'I:SPX'")
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


def _forex(text: str, prefixes: tuple[str, ...]) -> str | None:
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


def _crypto(text: str, prefixes: tuple[str, ...]) -> str | None:
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
        ValueError: If the codes are not two three-letter currency codes (text ending in `=X`,
            Yahoo Finance's spelling, gets a message that shows Massive's form), or the text
            names another class.
    """
    _refuse_other_prefix(AssetClass.FOREX, text)
    symbol = _forex(text.upper(), ("C:",))
    if symbol is None and text.upper().endswith("=X"):
        # Yahoo Finance writes a pair as `EURUSD=X`; reading it would guess at another vendor's
        # grammar, so the message only shows Massive's form.
        raise ValueError(
            f"cannot read {text!r} as a forex pair: it looks like Yahoo Finance's spelling; "
            "Massive's forex tickers look like 'C:EURUSD', two three-letter currency codes "
            "after C:, and this function reads 'EUR/USD', 'EUR-USD', or 'EURUSD'"
        )
    if symbol is None:
        raise ValueError(
            f"cannot read {text!r} as a forex pair of two three-letter codes; write it as "
            "BASE/QUOTE, such as 'EUR/USD'"
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
        ValueError: If the text is not a pair of letters and digits, or names another class.
    """
    _refuse_other_prefix(AssetClass.CRYPTO, text)
    symbol = _crypto(text.upper(), ("X:",))
    if symbol is None:
        raise ValueError(f"cannot read {text!r} as a crypto pair; write it as BASE-QUOTE")
    return symbol


_CONVERTERS: Final[dict[AssetClass, Callable[[str], str]]] = {
    AssetClass.STOCKS: _convert_stock,
    AssetClass.OPTIONS: _convert_option,
    AssetClass.FUTURES: _convert_future,
    AssetClass.INDICES: _convert_index,
    AssetClass.FOREX: _convert_forex,
    AssetClass.CRYPTO: _convert_crypto,
}

_UNVERIFIED: Final = (
    "Massive documents no spelling for warrants, units, rights, when-issued, "
    "when-distributed, called, or test issues, so the library converts none of them; pass "
    "Massive's own ticker, as m.reference.tickers lists it"
)
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
        ValueError: If the text is not one of the convention's plain, class, or preferred
            spellings: a Yahoo ticker with a dot is named a foreign listing, and every other
            refusal says which suffixes the library does not convert.
    """
    _refuse_other_prefix(AssetClass.STOCKS, text)
    if convention is SymbolConvention.YAHOO and "." in text:
        raise ValueError(
            f"{text!r} is a Yahoo ticker of a listing outside the US (Yahoo's dot names the "
            "exchange); Massive's US stock tickers have no such listing"
        )
    for pattern, template in _STOCK_CONVENTIONS[convention]:
        match = pattern.fullmatch(text)
        if match is not None:
            return match.expand(template)
    raise ValueError(
        f"{text!r} is not a plain, class, or preferred ticker of the {convention.name} "
        f"convention. {_UNVERIFIED}"
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


@raises_through(_require_enum, _require_text)
def canonical_symbol(
    text: str | None,
    asset_class: AssetClass,
    *,
    convention: SymbolConvention = SymbolConvention.MASSIVE,
) -> str | None:
    """Convert a symbol of a named asset class to Massive's exact REST ticker.

    Use it to turn symbols from another source into the form every dataset takes and every
    `Symbol` column holds, before passing them as `symbols=`. Under `SymbolConvention.MASSIVE`,
    the default, canonical text comes back unchanged; another spelling of the class converts
    (`EUR/USD` to `C:EURUSD`, `SPX` to `I:SPX`); anything else raises. Under another convention,
    only that convention's grammar applies, so text it does not spell raises even when it
    happens to be a Massive ticker. The class is never guessed: `SPX` is an index only because
    the caller says so. In a Deephaven query string, call the per-class functions instead
    (`canonical_symbol_forex`), which need no enum argument; a function called through the
    module (`dm.canonical_symbol(...)`) makes an `Object` column unless cast with `(String)`.
    Pure and thread-safe.

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

    Returns:
        The canonical ticker, or None for None.

    Raises:
        TypeError: If `asset_class` is not a `dm.AssetClass`, `convention` is not a
            `dm.SymbolConvention`, or `text` is neither a `str` nor None.
        ValueError: If `convention` is a stock convention and the class is not stocks, or the
            text is not a symbol of the class in any accepted spelling. The message gives the
            reason and, where one exists, the fix: a ticker with another class's prefix names
            that class's function.
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
        return _convert_stock_from(value, chosen)
    if _CANONICAL[resolved](value):
        return value
    return _CONVERTERS[resolved](value)


@raises_through(canonical_symbol)
def canonical_symbol_stock(text: str | None) -> str | None:
    """Check a stock ticker in Massive's own spelling, for Python code and query strings.

    Massive's spelling is the only one accepted: a ticker from Yahoo, Nasdaq, or a CMS feed
    goes through the function of its convention (`canonical_symbol_stock_yahoo`), because the
    same `-` means a class in one and a preferred in another. Pure and thread-safe.

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

    Returns:
        The ticker unchanged, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not a Massive stock ticker; the message names the conventions for
            text with their separators, and the upper-case form for lower-case text.
    """
    return canonical_symbol(text, AssetClass.STOCKS)


@raises_through(canonical_symbol)
def canonical_symbol_stock_yahoo(text: str | None) -> str | None:
    """Convert a Yahoo Finance stock ticker to Massive's: `BRK-B` to `BRK.B`, `BAC-PL` to `BACpL`.

    Yahoo writes a share class as `-X` and a preferred as `-P` and its series. Only plain
    tickers, classes, and preferreds with a series convert: Yahoo's bare `-P` is refused (a
    generic preferred and class P read alike), and warrants (`-WT`), units (`-UN`), and rights
    (`-RI`) are refused, since Massive documents no spelling for them, and so is a ticker with a
    dot, Yahoo's mark of a listing outside the US (`VOD.L`). Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_yahoo`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_yahoo(Col)`.

    Args:
        text: The Yahoo ticker, or None.

    Returns:
        Massive's ticker, or None for None. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not a plain, class, or preferred Yahoo ticker.
    """
    return canonical_symbol(text, AssetClass.STOCKS, convention=SymbolConvention.YAHOO)


@raises_through(canonical_symbol)
def canonical_symbol_stock_nasdaq(text: str | None) -> str | None:
    """Convert a Nasdaq Integrated Platform stock ticker to Massive's: `KIM-L` to `KIMpL`.

    Nasdaq's own symbology (the "NASDAQ Symbol" column of `nasdaqtraded.txt`) writes a class as
    `.X` and a preferred as `-` and its optional series. Its other suffixes (`+` warrants, `=`
    units, `^` rights, `#` when issued) are refused, since Massive documents no spelling for
    them. The ACT symbology of the same files writes `.W` for warrants and `.V` for both a
    class and when-issued, so it is ambiguous and not supported. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_nasdaq`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_nasdaq(Col)`.

    Args:
        text: The Nasdaq ticker, or None.

    Returns:
        Massive's ticker, or None for None. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not a plain, class, or preferred Nasdaq ticker.
    """
    return canonical_symbol(text, AssetClass.STOCKS, convention=SymbolConvention.NASDAQ)


@raises_through(canonical_symbol)
def canonical_symbol_stock_cms(text: str | None) -> str | None:
    """Convert a CMS stock ticker to Massive's: `BRK B` to `BRK.B`, `BAC PRL` to `BACpL`.

    The CMS convention (the root, a space, and a suffix, as many broker and FIX systems write
    it) spells a class as ` X` and a preferred as ` PR` and its optional series. Its other
    suffixes (` WS`, ` U`, ` RT`, ` WI`) are refused, since Massive documents no spelling for
    them. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_stock_cms`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_stock_cms(Col)`.

    Args:
        text: The CMS ticker, or None.

    Returns:
        Massive's ticker, or None for None. Only the convention's grammar is read: text it
        does not spell is refused even when it is a Massive ticker.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not a plain, class, or preferred CMS ticker.
    """
    return canonical_symbol(text, AssetClass.STOCKS, convention=SymbolConvention.CMS)


@raises_through(canonical_symbol)
def canonical_symbol_option(text: str | None) -> str | None:
    """Convert an OCC option symbol to Massive's: `AAPL250117C00150000` to `O:AAPL250117C00150000`.

    Accepts the symbol with or without `O:` and the 21-character OSI form whose root is padded
    with spaces, in any case. The root is never changed: `SPXW` stays `SPXW` and an adjusted
    root such as `AAPL1` stays `AAPL1`. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_option`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_option(Col)`.

    Args:
        text: The option symbol, or None.

    Returns:
        `O:` and the OCC symbol, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is no OCC symbol with a real expiry date (an underlying such as
            `AAPL` is named as one), or carries another class's prefix.
    """
    return canonical_symbol(text, AssetClass.OPTIONS)


@raises_through(_require_text, _convert_single_future)
def canonical_symbol_future(text: str | None) -> str | None:
    """Check a single futures contract ticker, which Massive's text is exactly: `ESZ5`.

    A contract ticker is upper-case letters and digits, taken as written: nothing is converted
    or re-cased. A spread is refused with the function that takes it
    (`canonical_symbol_future_spread`). Beware the year: Massive writes it with one digit
    (`ESZ5` is December 2025) unless the contract is ten or more years out (`0NGH39`), so
    `ESZ25` is not `ESZ5`. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_future`) for a `String` column; called through the module it makes an
    `Object` column unless cast: `(String) dm.canonical_symbol_future(Col)`.

    Args:
        text: The contract ticker, or None.

    Returns:
        The ticker unchanged, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is a spread, or not upper-case letters and digits.
    """
    value = _require_text(text)
    return None if value is None else _convert_single_future(value)


@raises_through(_require_text, _convert_spread)
def canonical_symbol_future_spread(text: str | None) -> str | None:
    """Check a futures spread ticker, exactly as Massive writes it: `GCQ6-GCZ6`, `BOB 02-01 U6`.

    Massive documents no grammar for spreads, so the spellings accepted are those its samples
    show: a calendar spread of two contracts (`GCQ6-GCZ6`, `10YN6-10YQ6`), a CBOT
    inter-commodity spread (`BOB 02-01 U6`), a NYMEX strategy of one or two products over
    months (the butterfly `CL:BF Q6-U6-V6`, the calendar `CL:BZ U6-V6`), and a CME currency
    cross (`6A:XF:AUDUSD:Q6`). Canonical text comes back unchanged; no ticker is built from its
    legs. A single contract is refused with `canonical_symbol_future`. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_future_spread`) for a `String` column; called through the module it makes
    an `Object` column unless cast: `(String) dm.canonical_symbol_future_spread(Col)`.

    Args:
        text: The spread ticker, or None.

    Returns:
        The ticker unchanged, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is a single contract, or none of the spread spellings (the message
            lists them).
    """
    value = _require_text(text)
    return None if value is None else _convert_spread(value)


@raises_through(canonical_symbol)
def canonical_symbol_index(text: str | None) -> str | None:
    """Convert an index ticker to Massive's: `SPX` to `I:SPX`.

    The prefix is added because the caller names the class by calling this function; no
    ticker is ever recognized as an index by its name. Case is kept. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_index`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_index(Col)`.

    Args:
        text: The index ticker, with or without `I:`, or None.

    Returns:
        `I:` and the ticker, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not an index ticker or carries another class's prefix.
    """
    return canonical_symbol(text, AssetClass.INDICES)


@raises_through(canonical_symbol)
def canonical_symbol_forex(text: str | None) -> str | None:
    """Convert a currency pair to Massive's forex ticker: `EUR/USD` to `C:EURUSD`.

    Accepts `EUR/USD`, `EUR-USD`, and `EURUSD`, with or without `C:`, in any case. Pure and
    thread-safe.

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
        `C:` and the two codes, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not two three-letter codes, or carries another class's prefix
            (`X:` is crypto). Yahoo Finance's spelling, `EURUSD=X`, is refused with a message that
            shows Massive's form: nothing is guessed from another vendor's grammar.
    """
    return canonical_symbol(text, AssetClass.FOREX)


@raises_through(canonical_symbol)
def canonical_symbol_crypto(text: str | None) -> str | None:
    """Convert a crypto pair to Massive's ticker: `BTC-USD` to `X:BTCUSD`.

    Accepts `BTC-USD`, `BTC/USD`, and `BTCUSD`, with or without `X:`, in any case, whatever the
    quote currency, and joins the two codes; any such ticker is canonical. A stream, the
    last-trade path, and the daily open/close path need the pair split into its two codes, and
    take the split only from Massive's crypto ticker list, so they refuse a pair that list does
    not hold; every other verb takes any pair. Pure and thread-safe.

    In a Deephaven query string, import it by bare name (`from deephaven_massive import
    canonical_symbol_crypto`) for a `String` column; called through the module it makes an `Object`
    column unless cast: `(String) dm.canonical_symbol_crypto(Col)`.

    Args:
        text: The pair, or None.

    Returns:
        `X:` and the joined pair, or None for None.

    Raises:
        TypeError: If `text` is neither a `str` nor None.
        ValueError: If the text is not a pair of letters and digits, or carries another class's
            prefix (`C:` is forex).
    """
    return canonical_symbol(text, AssetClass.CRYPTO)


# --- checks when a query is built ----------------------------------------------------------------


def check_symbol(
    asset_class: AssetClass | ClassFromPrefix, text: object, *, as_written: bool = False
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

    Returns:
        The text, unchanged.

    Raises:
        TypeError: If the value is not a `str`.
        ValueError: If the text is not canonical. A canonical ticker of another class says
            the dataset takes the other class (`'I:SPX' is an index ticker; this dataset takes
            stock tickers`); otherwise the message names the canonical form when the class's
            converter gives one (`'EUR/USD' is not Massive's ticker; use 'C:EURUSD'`), and
            the converter's reason when it gives none.
    """
    if not isinstance(text, str):
        raise TypeError(f"a symbol must be a str, not {type(text).__name__}")
    if as_written:
        if _WRITTEN.fullmatch(text) is not None:
            return text
        raise ValueError(
            f"{text!r} is not a ticker as Massive writes it (not empty, no surrounding "
            "whitespace, no control character, no class prefix)"
        )
    resolved = symbol_class(text) if asset_class is ClassFromPrefix.ANY else asset_class
    if _CANONICAL[resolved](text):
        return text
    other = symbol_class(text)
    if other not in {resolved, AssetClass.STOCKS} and _CANONICAL[other](text):
        raise ValueError(
            f"{text!r} is a{'n' if other in _VOWEL_CLASSES else ''} {_CLASS_NOUNS[other]} "
            f"ticker; this dataset takes {_CLASS_NOUNS[resolved]} tickers"
        )
    try:
        fixed = _CONVERTERS[resolved](text)
    except ValueError as error:
        raise ValueError(f"{error}; symbols must be Massive's exact tickers") from None
    raise ValueError(
        f"{text!r} is not Massive's ticker; use {fixed!r} (symbols must be Massive's exact "
        f"tickers, and {_function(resolved)} converts)"
    )


def normalize(
    asset_class: AssetClass | ClassFromPrefix,
    symbols: str | Iterable[str] | _All,
    *,
    as_written: bool = False,
) -> tuple[str, ...] | _All:
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

    Returns:
        A sorted tuple of canonical symbols, or `ALL`.

    Raises:
        TypeError: If `symbols` is not a `str`, an iterable of `str`, or `dm.ALL`, or an
            element is not a `str`.
        ValueError: If no symbols remain (`ValueError("no symbols")`, for example from an
            exhausted generator), or a symbol is not canonical (`check_symbol`).
    """
    if isinstance(symbols, _All):
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
    return tuple(sorted({check_symbol(asset_class, i, as_written=as_written) for i in items}))


# --- the wire ------------------------------------------------------------------------------------


def canonical_from_wire(
    asset_class: AssetClass | ClassFromPrefix, raw: str, *, as_written: bool = False
) -> str | None:
    """Read a symbol as a Massive transport sent it, in the canonical form.

    The spellings the library knows: forex pairs as `C:EURUSD`, `EUR/USD`, `EUR-USD`,
    `EURUSD`, or `X:EUR-USD` (the forex quotes flat-file sample); crypto pairs as `X:BTCUSD`,
    `BTC-USD`, `BTC/USD`, or `X:BTC-USD` (FMV frames), whatever the quote currency; an index
    without its prefix (`SPX`) on a dataset of the indices class, which then gets `I:` from the
    dataset's class, never from the name. A futures row keeps any ticker as Massive writes it,
    spreads of a form not yet seen included, and so does a dataset kept as written; every other
    class takes only its canonical text. Case is never
    folded.

    Args:
        asset_class: The dataset's class, or `ClassFromPrefix.ANY` for a dataset that spans classes:
            the prefix then selects the class, and text without one is read as a stock.
        raw: The wire text.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`is_canonical`).

    Returns:
        The canonical ticker, or None for a spelling the library does not know.
    """
    if as_written:
        return raw if _WRITTEN.fullmatch(raw) is not None else None
    resolved = symbol_class(raw) if asset_class is ClassFromPrefix.ANY else asset_class
    if resolved is AssetClass.FOREX:
        return _forex(raw, ("C:", "X:"))
    if resolved is AssetClass.CRYPTO:
        return _crypto(raw, ("X:",))
    if resolved is AssetClass.FUTURES:
        return raw if _WRITTEN.fullmatch(raw) is not None else None
    if asset_class is AssetClass.INDICES and not raw.startswith("I:"):
        raw = f"I:{raw}"
    return raw if _CANONICAL[resolved](raw) else None


def wire_spellings(asset_class: AssetClass, symbol: str) -> tuple[str, ...]:
    """Return every spelling `canonical_from_wire` reads as a canonical symbol, for a file filter.

    A flat file is filtered on its raw `ticker` column before any row is read, so the filter
    must hold every way a file may write the requested symbols: Massive's knowledge base says
    the crypto and forex files write `X:BTC-USD` and `C:EUR-USD`, and the forex quotes sample
    shows `X:EUR-USD`, so `X:BTCUSD` and `C:EURUSD` are also looked for in those spellings. A
    crypto pair is looked for split at every point, since its joined ticker does not say where
    the quote starts (`X:BTCPYUSD` may be written `X:BTC-PYUSD`). The canonical text comes
    first.

    Args:
        asset_class: The dataset's class; every flat-file dataset has one (`DatasetSpec`
            refuses `ClassFromPrefix.ANY` with flat files).
        symbol: A canonical symbol of the class.

    Returns:
        The spellings, each of which `canonical_from_wire(asset_class, spelling)` reads as
        `symbol`.
    """
    prefixes: tuple[str, ...]
    if asset_class is AssetClass.FOREX:
        base, quote = forex_codes(symbol)
        prefixes = ("C:", "X:", "")
    elif asset_class is AssetClass.CRYPTO:
        prefixes = ("X:", "")
        # The joined ticker does not say where the pair splits, so the file may separate it at
        # any point: every cut is looked for, and each reads back as this one symbol.
        body = symbol[2:]
        cuts = [(body[:i], body[i:]) for i in range(1, len(body))]
        joined = tuple(f"{p}{body}" for p in prefixes)
        dashed = (f"{p}{b}{s}{q}" for p in prefixes for b, q in cuts for s in ("/", "-"))
        return (*joined, *dashed)
    elif asset_class is AssetClass.INDICES:
        return symbol, symbol[2:]
    else:
        return (symbol,)
    return tuple(f"{p}{base}{s}{quote}" for p in prefixes for s in ("", "/", "-"))


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


def ws_symbol(asset_class: AssetClass, symbol: str, pairs: Mapping[str, tuple[str, str]]) -> str:
    """Return the WebSocket spelling of a canonical symbol, as topics use it.

    Forex and crypto become `BASE-QUOTE` (`C:EURUSD` to `EUR-USD`, `X:BTCUSD` to `BTC-USD`);
    every other class uses the canonical form unchanged. A crypto pair splits where `pairs`
    says: Massive's crypto ticker list, read by the transport, since a joined ticker such as
    `X:DOTUSD` could split as `DOT-USD` or `DO-TUSD` and is never split by a guess.

    Massive's WebSocket documentation gives every forex channel's `ticker` parameter (`C`,
    `CA`, `CAS`, `FMV`) as `{from}-{to}`, so a forex topic is `C.EUR-USD`, even though forex
    quote and aggregate frames spell the pair `EUR/USD` and FMV frames `C:EURUSD`. Frames are
    read back with `canonical_from_wire`, so every inbound spelling still reaches its stream.

    Args:
        asset_class: The symbol's class.
        symbol: A canonical symbol.
        pairs: The base and quote of crypto symbols by canonical symbol, from Massive's crypto
            ticker list; every crypto symbol must be in it. Empty for a class other than
            crypto, which needs none.

    Returns:
        The WebSocket form.

    A crypto symbol not in `pairs` raises `KeyError`; the transports resolve every crypto
    symbol before they build its topic.
    """
    if asset_class is AssetClass.FOREX:
        # The `ticker` parameter of every forex channel is "{from}-{to}": see
        # vendor/massive/websocket/forex/quotes.md, aggregates-per-minute.md,
        # aggregates-per-second.md, and fair-market-value.md.
        base, quote = forex_codes(symbol)
        return f"{base}-{quote}"
    if asset_class is AssetClass.CRYPTO:
        base, quote = pairs[symbol]
        return f"{base}-{quote}"
    return symbol


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


_UNKNOWN_LIMIT: Final = 100
"""The most unknown wire spellings logged per process; one record then says the rest are not."""
_unknown_spellings: Final = BoundedWarnings(logger, limit=_UNKNOWN_LIMIT)


def unknown_symbol_message(dataset: str, spellings: Iterable[str]) -> str:
    """Say which symbols Massive sent in a spelling the library cannot read, and the remedy.

    Args:
        dataset: The dataset that received them, such as `forex.quotes`.
        spellings: The texts, as sent.

    Returns:
        The message a failed request or stream carries; it names the session setting that
        keeps such rows instead.
    """
    texts = sorted(spellings)
    listed = ", ".join(repr(s) for s in texts)
    sent = f"the symbols {listed}, spellings" if texts[1:] else f"the symbol {listed}, a spelling"
    return (
        f"{dataset}: Massive sent {sent} the library cannot read as Massive's ticker. "
        "Set MASSIVE_UNKNOWN_SYMBOLS=warn, or pass "
        "MassiveConfig(unknown_symbols=dm.UnknownSymbols.WARN), to keep such rows with a null "
        "Symbol and the text in RawSymbol"
    )


@dataclass(frozen=True, slots=True)
class ReadSymbols:
    """One batch's wire symbols, read into the canonical form by `WireSymbols.read`.

    Attributes:
        symbols: The canonical symbols, one per row; null for a null wire symbol and for a
            spelling the library cannot read.
        raw: The wire text where the spelling could not be read, null elsewhere: the
            `RawSymbol` column of a session that keeps such rows.
        unknown: The distinct spellings that could not be read, sorted; empty for a clean
            batch.
    """

    symbols: pa.Array
    raw: pa.Array
    unknown: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WireSymbols:
    """How one dataset reads the symbols its rows carry, and what an unreadable one does.

    Built once per transport plan (REST converter, WebSocket channel plan, flat-file
    transform) from the dataset and the session's `MassiveConfig.unknown_symbols`; immutable
    and shared by every thread of the plan.

    Attributes:
        dataset: The dataset name, for messages.
        asset_class: The dataset's class (`canonical_from_wire`), or `ClassFromPrefix.ANY`
            for one that spans classes.
        unknown: The session's rule for a spelling `canonical_from_wire` cannot read: under
            `UnknownSymbols.WARN` each new spelling is logged once at WARNING (a bounded
            number per process) and its rows keep the text in `ReadSymbols.raw`; under
            `FAIL` the caller fails the request or stream with `unknown_symbol_message`.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`canonical_from_wire`).
    """

    dataset: str
    asset_class: AssetClass | ClassFromPrefix
    unknown: UnknownSymbols
    as_written: bool = False

    def read(self, raw: pa.Array, cache: dict[str, str | None]) -> ReadSymbols:
        """Read a column of wire symbols, calling `canonical_from_wire` once per distinct text.

        Args:
            raw: The rows' symbols as sent; nulls allowed.
            cache: A map from wire text to canonical symbol (None for unreadable), read and
                extended here: one the caller keeps between batches and owns on one thread, or
                an empty one to read a batch afresh.

        Returns:
            The canonical symbols, the unreadable texts, and the distinct unreadable spellings.
            Under `WARN` each new unreadable spelling is logged once at WARNING.
        """
        known = cache
        distinct = pc.call_function("unique", [raw])
        texts = distinct.to_pylist()
        for text in texts:
            if text is not None and text not in known:
                known[text] = canonical_from_wire(
                    self.asset_class, text, as_written=self.as_written
                )
        mapped = pa.array([None if t is None else known[t] for t in texts], pa.string())
        index = pc.call_function("index_in", [raw], pc.SetLookupOptions(value_set=distinct))
        symbols = pc.take(mapped, index)
        unknown = tuple(sorted(t for t in texts if t is not None and known[t] is None))
        missing = pc.call_function("is_null", [symbols])
        unread = pc.call_function("and", [missing, pc.call_function("is_valid", [raw])])
        kept = pc.call_function("if_else", [unread, raw, pa.scalar(None, pa.string())])
        if self.unknown is UnknownSymbols.WARN:
            for text in unknown:
                _unknown_spellings.warn(
                    f"{self.dataset}\0{text}",
                    "%s: Massive sent the symbol %r, a spelling the library cannot read; its "
                    "rows have a null Symbol and keep the text in RawSymbol",
                    self.dataset,
                    text,
                )
        return ReadSymbols(symbols, kept, unknown)
