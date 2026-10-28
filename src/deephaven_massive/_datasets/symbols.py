"""Symbol normalization across WebSocket, REST, and flat-file forms.

Massive spells one instrument differently per transport: a forex pair is `EUR/USD` in
WebSocket quote and aggregate frames, `EUR-USD` in WebSocket topics, and `C:EURUSD` on REST,
and a crypto pair is `BTC-USD` on the WebSocket but `X:BTCUSD` on REST. Users may pass any of
these forms. This module maps every input form to the *canonical* form, which is Massive's REST
ticker, and that is what the `Symbol` column always holds:

| Class   | Inputs accepted                            | Canonical               |
|---------|--------------------------------------------|-------------------------|
| stocks  | `AAPL`, `brk.a`                            | `AAPL`, `BRK.A`         |
| options | `AAPL250117C00150000`, `O:AAPL250117C00150000` | `O:AAPL250117C00150000` |
| futures | `ESZ5`                                     | `ESZ5`                  |
| indices | `SPX`, `I:SPX`                             | `I:SPX`                 |
| forex   | `EUR/USD`, `EUR-USD`, `EURUSD`, `C:EURUSD` | `C:EURUSD`              |
| crypto  | `BTC-USD`, `BTC/USD`, `X:BTCUSD`           | `X:BTCUSD`              |

Datasets that span asset classes (indicators, reference data, the unified snapshot) pass no
class: the prefix then selects the class (`O:` options, `I:` indices, `C:` forex, `X:`
crypto), a symbol without one is a stock, and that class's rules apply unchanged.

`ws_symbol` turns a canonical symbol back into the WebSocket form, and `underlying` parses the
underlying ticker out of an OCC option symbol (the `Underlying` column of options rows).

`normalize` applies these rules to a `Symbols` argument other than a table when a query is
built: it materializes sequences to a sorted, de-duplicated tuple (the stream key) and passes
`dm.ALL` through. A reactive `Table` is checked and boxed in a `SymbolTable` by the dataset
factory (`build_query` in `_namespaces/base.py`), and its canonical view is built by
`canonical_view` in `_tables/watchlist.py`: this module makes no Deephaven call and imports
Deephaven for type checking only. Every function here is pure and thread-safe.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._enums import AssetClass
from deephaven_massive._errors import log_once
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from deephaven.table import Table

__all__ = [
    "CRYPTO_QUOTES",
    "NormalizedSymbols",
    "SymbolTable",
    "canonical",
    "is_forex_currency",
    "normalize",
    "symbol_canonicalizer",
    "symbol_class",
    "underlying",
    "ws_symbol",
]

logger = logging.getLogger(__name__)

_OCC: Final = re.compile(r"^O:(?P<root>[A-Z0-9.]{1,6})(?P<expiry>\d{6})[CP]\d{8}$")
_PLAIN: Final = re.compile(r"^[A-Z0-9.:/\-]+$")
_PAIR_SEPARATORS: Final = re.compile(r"[/\-]")
_ALPHANUMERIC: Final = re.compile(r"[A-Z0-9]")
_CURRENCY: Final = re.compile(r"^[A-Z0-9]+$")
"""A crypto currency code: letters and digits of any length (`1INCH`, `USDT`)."""
_FOREX_CURRENCY: Final = re.compile(r"^[A-Z]{3}$")
"""A forex currency code: three letters, as ISO 4217 writes them (`EUR`)."""
_PAIR_PARTS: Final = 2
_CURRENCY_CODE_LENGTH: Final = 3  # ISO 4217
CRYPTO_QUOTES: Final = (
    "USDT",
    "USDC",
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "CAD",
    "CHF",
    "SGD",
    "HKD",
    "BTC",
    "ETH",
)
"""The quote currencies of Massive's crypto pairs, longest first so `USDT` wins over `USD`.

A canonical crypto ticker (`X:BTCUSD`) has no separator, so the WebSocket form (`BTC-USD`) is
recovered by matching one of these suffixes. `canonical` accepts only pairs that split back
unambiguously, so `ws_symbol` cannot fail. The list is checked against the vendored ticker
fixture `tests/data/datasets/symbols/crypto_tickers.csv`."""
_PREFIXES: Final[dict[AssetClass, str]] = {
    AssetClass.OPTIONS: "O:",
    AssetClass.INDICES: "I:",
    AssetClass.FOREX: "C:",
    AssetClass.CRYPTO: "X:",
}
_CLASS_BY_PREFIX: Final[dict[str, AssetClass]] = {p: c for c, p in _PREFIXES.items()}


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


def canonical(asset_class: AssetClass | None, raw: object) -> str:
    """Return the canonical Massive form of one symbol of an asset class.

    Args:
        asset_class: The class whose rules apply, or None for a dataset that spans classes:
            the symbol's prefix then selects the class (`O:`, `I:`, `C:`, `X:`), and a symbol
            without one is a stock.
        raw: The symbol in any accepted input form (see the module table). Surrounding
            whitespace is ignored and letters are upper-cased.

    Returns:
        The canonical symbol, the form the `Symbol` column holds.

    Raises:
        TypeError: If `raw` is not a `str`.
        ValueError: If `raw` is empty or a prefix with nothing after it (`I:`), contains
            characters no Massive symbol has (spaces, commas), carries another class's prefix
            (`X:` on a stock, `C:` on crypto; the message says which prefix, if any, the class
            uses), carries an unknown prefix when no class is given, is not a valid OCC symbol
            with a real `YYMMDD` expiry date for options, is not a pair of three-letter codes
            for forex (`is_forex_currency`), or is not a crypto pair that ends in one of
            `CRYPTO_QUOTES` (checked here, so the WebSocket form can always be recovered).
    """
    if not isinstance(raw, str):
        raise TypeError(f"a symbol must be a str, not {type(raw).__name__}")
    asset_class, prefix, body = _class_and_body(asset_class, raw)
    if asset_class is AssetClass.OPTIONS:
        symbol = f"O:{body}"
        if _occ_match(symbol) is None:
            raise ValueError(f"{raw!r} is not an OCC option symbol such as O:AAPL250117C00150000")
        return symbol
    if asset_class is AssetClass.FOREX:
        pair = _split_pair(body, raw) if _PAIR_SEPARATORS.search(body) else (body[:3], body[3:])
        if not all(is_forex_currency(code) for code in pair):
            raise ValueError(f"cannot read {raw!r} as a currency pair; write it as BASE/QUOTE")
        return f"C:{pair[0]}{pair[1]}"
    if asset_class is AssetClass.CRYPTO:
        if _PAIR_SEPARATORS.search(body) is None:
            if not _CURRENCY.match(body):
                raise ValueError(f"{raw!r} is not a crypto pair; write it as BASE-QUOTE")
            _split_crypto(body, raw)
            return f"X:{body}"
        base, quote = _split_pair(body, raw)
        if _split_crypto(base + quote, raw) != (base, quote):
            raise ValueError(f"{raw!r} does not split back unambiguously; write it as BASE-QUOTE")
        return f"X:{base}{quote}"
    return f"{prefix}{body}"


def is_forex_currency(code: str) -> bool:
    """Tell whether text is a forex currency code: three upper-case letters, such as `EUR`.

    Massive's forex pairs are ISO 4217 codes, so a digit (`EU1`) or another length is no
    currency; crypto codes, which may hold digits, follow their own rule in `canonical`.

    Args:
        code: The code, already stripped and upper-cased.

    Returns:
        True for three letters `A` to `Z`.
    """
    return _FOREX_CURRENCY.match(code) is not None


def symbol_class(symbol: str) -> AssetClass:
    """Return the asset class a canonical symbol's prefix names.

    Args:
        symbol: A canonical symbol, such as `X:BTCUSD` or `AAPL`.

    Returns:
        The class of its prefix (`O:` options, `I:` indices, `C:` forex, `X:` crypto); stocks
        for a symbol without one.
    """
    return _CLASS_BY_PREFIX.get(symbol[:2], AssetClass.STOCKS)


def _class_and_body(asset_class: AssetClass | None, raw: str) -> tuple[AssetClass, str, str]:
    """Check a raw symbol's characters and prefix, and split it into class, prefix, and body.

    Args:
        asset_class: The dataset's class, or None to select it by the symbol's prefix.
        raw: The symbol as the caller wrote it.

    Returns:
        `(class, prefix, body)`: the class whose rules apply, its prefix (empty for classes
        without one), and the upper-cased symbol without that prefix.

    Raises:
        ValueError: If `raw` is not a plain symbol, carries another class's prefix, is a
            prefix with nothing after it (`I:`), or has no letter or digit (`-`, `.`).
    """
    text = raw.strip().upper()
    if not _PLAIN.match(text):
        raise ValueError(f"{raw!r} is not a symbol")
    if asset_class is None:
        asset_class = _CLASS_BY_PREFIX.get(text[:2], AssetClass.STOCKS)
    prefix = _PREFIXES.get(asset_class, "")
    body = text.removeprefix(prefix) if prefix else text
    if not body:
        raise ValueError(f"{raw!r} is a prefix without a symbol")
    if not _ALPHANUMERIC.search(body):
        raise ValueError(f"{raw!r} is not a symbol: it has no letter or digit")
    if ":" in body:
        rule = f"use the {prefix} prefix" if prefix else "have no prefix"
        raise ValueError(
            f"{raw!r} carries the prefix of another asset class; {asset_class.value} symbols {rule}"
        )
    return asset_class, prefix, body


def _split_crypto(body: str, raw: str) -> tuple[str, str]:
    """Split a separator-free crypto pair at its quote currency.

    Args:
        body: The pair without prefix or separator, such as `BTCUSDT`.
        raw: The symbol as the caller wrote it, for the error message.

    Returns:
        `(base, quote)`, with the longest matching quote currency.

    Raises:
        ValueError: If no quote currency of `CRYPTO_QUOTES` ends the pair.
    """
    for quote in CRYPTO_QUOTES:
        if body.endswith(quote) and len(body) > len(quote):
            return body[: -len(quote)], quote
    raise ValueError(
        f"cannot read {raw!r} as a crypto pair whose quote currency is one of "
        f"{', '.join(CRYPTO_QUOTES)}; write it as BASE-QUOTE"
    )


def ws_symbol(asset_class: AssetClass, symbol: str) -> str:
    """Return the WebSocket spelling of a canonical symbol, as topics and frames use it.

    Forex and crypto become `BASE-QUOTE` (`C:EURUSD` to `EUR-USD`, `X:BTCUSD` to `BTC-USD`);
    every other class uses the canonical form unchanged. Every symbol `canonical` returns
    converts; a crypto ticker it would reject raises its `ValueError`.

    Massive's WebSocket documentation gives every forex channel's `ticker` parameter (`C`,
    `CA`, `CAS`, `FMV`) as `{from}-{to}`, so a forex topic is `C.EUR-USD`, even though forex
    quote and aggregate frames spell the pair `EUR/USD` and FMV frames `C:EURUSD`. Frames are
    matched by their canonical form, so every inbound spelling still reaches its stream.

    Args:
        asset_class: The symbol's class.
        symbol: A canonical symbol, as `canonical` returns it.

    Returns:
        The WebSocket form.
    """
    if asset_class is AssetClass.FOREX:
        # The `ticker` parameter of every forex channel is "{from}-{to}": see
        # vendor/massive/websocket/forex/quotes.md, aggregates-per-minute.md,
        # aggregates-per-second.md, and fair-market-value.md.
        body = symbol.removeprefix("C:")
        return f"{body[:_CURRENCY_CODE_LENGTH]}-{body[_CURRENCY_CODE_LENGTH:]}"
    if asset_class is AssetClass.CRYPTO:
        base, quote = _split_crypto(symbol.removeprefix("X:"), symbol)
        return f"{base}-{quote}"
    return symbol


def _occ_match(symbol: str) -> re.Match[str] | None:
    """Match a canonical OCC option symbol whose expiry is a real calendar date.

    Args:
        symbol: The symbol with its `O:` prefix, upper case.

    Returns:
        The match, with the `root` and `expiry` groups; None if the symbol has not the OCC
        shape or its `YYMMDD` expiry is no date (`250132`, `251301`).
    """
    match = _OCC.match(symbol)
    if match is None:
        return None
    expiry = match["expiry"]
    try:
        date(2000 + int(expiry[:2]), int(expiry[2:4]), int(expiry[4:]))
    except ValueError:
        return None
    return match


def underlying(option_symbol: str) -> str:
    """Return the underlying ticker of a canonical OCC option symbol.

    Args:
        option_symbol: A canonical option symbol such as `O:AAPL250117C00150000`.

    Returns:
        The OCC root, such as `AAPL`.

    Raises:
        ValueError: If the symbol is not a canonical OCC option symbol with a real expiry
            date.
    """
    match = _occ_match(option_symbol)
    if match is None:
        raise ValueError(f"{option_symbol!r} is not an OCC option symbol")
    return match["root"]


def symbol_canonicalizer(
    asset_class: AssetClass | None,
) -> Callable[[str | None], str | None]:
    """Return the per-row canonicalizer applied to a reactive symbol table's `Symbol` column.

    The returned function maps each row's symbol to its canonical form with `canonical`, so a
    watchlist may hold any accepted spelling (`EUR/USD`, `BTC-USD`, `SPX`). A null stays null;
    a row that is not a valid symbol of the class becomes null, so it matches nothing, and is
    logged once per class at WARNING. It runs on update-graph threads, so it does no I/O beyond
    that one log record and takes no lock but the log-once guard.

    Args:
        asset_class: The class whose rules apply, or None to select it by each symbol's
            prefix (see `canonical`).

    Returns:
        A function from a raw symbol to its canonical form or None.
    """
    kind = "any-class" if asset_class is None else asset_class.value

    def canonical_symbol(symbol: str | None) -> str | None:
        if symbol is None:
            return None
        try:
            return canonical(asset_class, symbol)
        except (TypeError, ValueError):
            log_once(
                logger,
                f"invalid-table-symbol:{kind}",
                "a symbol table holds %r, which is not a valid %s symbol; such rows are ignored",
                symbol,
                kind,
                level=logging.WARNING,
            )
            return None

    return canonical_symbol


def normalize(
    asset_class: AssetClass | None, symbols: str | Iterable[str] | _All
) -> tuple[str, ...] | _All:
    """Normalize a `Symbols` argument other than a table when a query is built.

    Rules: `dm.ALL` passes through; a `str` is one symbol; any other iterable of strings,
    including a generator, is materialized now. The resulting tuple is canonical, sorted, and
    de-duplicated, so equal symbol sets give equal queries and one stream. A Deephaven `Table`
    never reaches this function: the dataset factory boxes it in a `SymbolTable` first.

    Whether `ALL` is acceptable for a dataset is decided by the caller, which knows the
    alternative to name.

    Args:
        asset_class: The class whose symbol rules apply, or None for a dataset that spans
            classes (each symbol's prefix selects its class; see `canonical`).
        symbols: The argument as the user passed it.

    Returns:
        A sorted tuple of canonical symbols, or `ALL`.

    Raises:
        TypeError: If `symbols` is not a `str`, an iterable of `str`, or `dm.ALL`, or an
            element is not a `str`.
        ValueError: If no symbols remain (`ValueError("no symbols")`, for example from an
            exhausted generator), or a symbol is invalid for the class.
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
        raise ValueError("no symbols")
    return tuple(sorted({canonical(asset_class, item) for item in items}))
