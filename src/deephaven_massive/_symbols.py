"""Symbol normalization across WebSocket, REST, and flat-file forms (DESIGN § 10).

Massive spells one instrument differently per transport: a forex pair is `EUR/USD` in
WebSocket frames and topics but `C:EURUSD` on REST, and a crypto pair is `BTC-USD` on the
WebSocket but `X:BTCUSD` on REST. Users may pass any of these forms. This module maps every
input form to the *canonical* form, which is Massive's REST ticker, and that is what the
`Symbol` column always holds:

| Class   | Inputs accepted                            | Canonical               |
|---------|--------------------------------------------|-------------------------|
| stocks  | `AAPL`, `brk.a`                            | `AAPL`, `BRK.A`         |
| options | `AAPL250117C00150000`, `O:AAPL250117C00150000` | `O:AAPL250117C00150000` |
| futures | `ESZ5`                                     | `ESZ5`                  |
| indices | `SPX`, `I:SPX`                             | `I:SPX`                 |
| forex   | `EUR/USD`, `EUR-USD`, `EURUSD`, `C:EURUSD` | `C:EURUSD`              |
| crypto  | `BTC-USD`, `BTC/USD`, `X:BTCUSD`           | `X:BTCUSD`              |

`ws_symbol` turns a canonical symbol back into the WebSocket form, and `underlying` parses the
underlying ticker out of an OCC option symbol (the `Underlying` column of options rows).

`normalize` applies these rules to a whole `Symbols` argument when a query is built: it
materializes sequences to a sorted, de-duplicated tuple (the stream key), checks that a
reactive `Table` has a `Symbol` column, and passes `dm.ALL` through. Every function here is pure
and thread-safe.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven.table import Table

from deephaven_massive._enums import AssetClass
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from deephaven_massive._types import Symbols

__all__ = [
    "NormalizedSymbols",
    "SymbolTable",
    "canonical",
    "normalize",
    "underlying",
    "ws_symbol",
]

logger = logging.getLogger(__name__)

_OCC: Final = re.compile(r"^O:(?P<root>[A-Z0-9.]{1,6})(?P<expiry>\d{6})[CP]\d{8}$")
_PLAIN: Final = re.compile(r"^[A-Z0-9.:/\-]+$")
_PAIR_SEPARATORS: Final = re.compile(r"[/\-]")
_CURRENCY: Final = re.compile(r"^[A-Z0-9]+$")
_PAIR_PARTS: Final = 2
_FOREX_PAIR_LENGTH: Final = 6  # two three-letter ISO 4217 codes
# Quote currencies of Massive's crypto pairs, longest first so `USDT` wins over `USD`. Used only
# to split a canonical `X:` ticker, which has no separator, back into the WebSocket form.
_CRYPTO_QUOTES: Final = ("USDT", "USDC", "USD", "EUR", "GBP", "JPY", "BTC", "ETH")
_PREFIXES: Final[dict[AssetClass, str]] = {
    AssetClass.OPTIONS: "O:",
    AssetClass.INDICES: "I:",
    AssetClass.FOREX: "C:",
    AssetClass.CRYPTO: "X:",
}


@dataclass(frozen=True, eq=False, slots=True)
class SymbolTable:
    """A reactive symbol table, compared and hashed by the identity of its Python wrapper.

    Deephaven `Table` wrappers compare equal when they wrap the same Java table, but DESIGN
    § 14 keys a reactive stream by `id(table)`: two wrappers of one Java table are two streams.
    Holding the table in this identity-keyed box keeps query equality and the stream key in
    agreement. The box holds the wrapper strongly, so its `id` stays valid.

    Attributes:
        table: The user's table; it has a `Symbol` column.
    """

    table: Table


NormalizedSymbols: TypeAlias = tuple[str, ...] | _All | SymbolTable
"""A `Symbols` argument after `normalize`: a sorted tuple of canonical symbols, `ALL`, or a
reactive table."""


def _split_pair(body: str, raw: str) -> tuple[str, str]:
    """Split a currency pair written with a separator into its base and quote currencies.

    Args:
        body: The pair with any class prefix removed, upper case.
        raw: The symbol as the user wrote it, for the error message.

    Returns:
        `(base, quote)`.

    Raises:
        ValueError: If the pair does not have exactly two non-empty alphanumeric parts.
    """
    parts = _PAIR_SEPARATORS.split(body)
    if len(parts) != _PAIR_PARTS or not all(_CURRENCY.match(part) for part in parts):
        raise ValueError(f"cannot read {raw!r} as a currency pair; write it as BASE/QUOTE")
    return parts[0], parts[1]


def canonical(asset_class: AssetClass, raw: object) -> str:
    """Return the canonical Massive form of one symbol of an asset class.

    Args:
        asset_class: The class whose rules apply.
        raw: The symbol in any accepted input form (see the module table). Surrounding
            whitespace is ignored and letters are upper-cased.

    Returns:
        The canonical symbol, the form the `Symbol` column holds.

    Raises:
        TypeError: If `raw` is not a `str`.
        ValueError: If `raw` is empty, contains characters no Massive symbol has (spaces,
            commas), is not a valid OCC symbol for options, or is not a readable pair for
            forex or crypto.
    """
    if not isinstance(raw, str):
        raise TypeError(f"a symbol must be a str, not {type(raw).__name__}")
    text = raw.strip().upper()
    if not _PLAIN.match(text):
        raise ValueError(f"{raw!r} is not a symbol")
    prefix = _PREFIXES.get(asset_class, "")
    body = text.removeprefix(prefix) if prefix else text
    if asset_class is AssetClass.OPTIONS:
        symbol = f"O:{body}"
        if not _OCC.match(symbol):
            raise ValueError(f"{raw!r} is not an OCC option symbol such as O:AAPL250117C00150000")
        return symbol
    if asset_class is AssetClass.FOREX and _PAIR_SEPARATORS.search(body) is None:
        if len(body) != _FOREX_PAIR_LENGTH:
            raise ValueError(f"cannot read {raw!r} as a currency pair; write it as BASE/QUOTE")
        return f"C:{body}"
    if asset_class in {AssetClass.FOREX, AssetClass.CRYPTO}:
        if _PAIR_SEPARATORS.search(body) is None:
            return f"{prefix}{body}"
        base, quote = _split_pair(body, raw)
        return f"{prefix}{base}{quote}"
    return f"{prefix}{body}"


def ws_symbol(asset_class: AssetClass, symbol: str) -> str:
    """Return the WebSocket spelling of a canonical symbol, as topics and frames use it.

    Forex becomes `BASE/QUOTE` (`C:EURUSD` to `EUR/USD`) and crypto `BASE-QUOTE` (`X:BTCUSD`
    to `BTC-USD`); every other class uses the canonical form unchanged.

    Args:
        asset_class: The symbol's class.
        symbol: A canonical symbol, as `canonical` returns it.

    Returns:
        The WebSocket form.

    Raises:
        ValueError: If a crypto ticker does not end in a known quote currency, so it cannot
            be split into base and quote.
    """
    if asset_class is AssetClass.FOREX:
        body = symbol.removeprefix("C:")
        half = _FOREX_PAIR_LENGTH // 2
        return f"{body[:half]}/{body[half:]}"
    if asset_class is AssetClass.CRYPTO:
        body = symbol.removeprefix("X:")
        for quote in _CRYPTO_QUOTES:
            if body.endswith(quote) and len(body) > len(quote):
                return f"{body[: -len(quote)]}-{quote}"
        raise ValueError(
            f"cannot split {symbol!r} into base and quote currencies; write it as BASE-QUOTE"
        )
    return symbol


def underlying(option_symbol: str) -> str:
    """Return the underlying ticker of a canonical OCC option symbol.

    Args:
        option_symbol: A canonical option symbol such as `O:AAPL250117C00150000`.

    Returns:
        The OCC root, such as `AAPL`.

    Raises:
        ValueError: If the symbol is not a canonical OCC option symbol.
    """
    match = _OCC.match(option_symbol)
    if match is None:
        raise ValueError(f"{option_symbol!r} is not an OCC option symbol")
    return match["root"]


def normalize(asset_class: AssetClass, symbols: Symbols) -> NormalizedSymbols:
    """Normalize a `Symbols` argument when a query is built.

    Rules (DESIGN § 10): `dm.ALL` passes through; a Deephaven `Table` must have a `Symbol`
    column and is boxed by identity; a `str` is one symbol; any other iterable of strings,
    including a generator, is materialized now. The resulting tuple is canonical, sorted, and
    de-duplicated, so equal symbol sets give equal queries and one stream.

    Whether `ALL` is acceptable for a dataset is decided by the caller, which knows the
    alternative to name.

    Args:
        asset_class: The class whose symbol rules apply.
        symbols: The argument as the user passed it.

    Returns:
        A sorted tuple of canonical symbols, `ALL`, or a `SymbolTable`.

    Raises:
        TypeError: If `symbols` is not a `str`, an iterable of `str`, a Deephaven `Table`, or
            `dm.ALL`, or an element is not a `str`.
        ValueError: If no symbols remain (`ValueError("no symbols")`, for example from an
            exhausted generator), a `Table` has no `Symbol` column, or a symbol is invalid for
            the class.
    """
    if isinstance(symbols, _All):
        return symbols
    if isinstance(symbols, Table):
        if "Symbol" not in symbols.column_names:
            raise ValueError("a symbol table must have a Symbol column")
        return SymbolTable(symbols)
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
        raise ValueError("no symbols")
    return tuple(sorted({canonical(asset_class, item) for item in items}))
