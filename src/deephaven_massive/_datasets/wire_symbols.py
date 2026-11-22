"""A symbol's transport spellings, both ways: reading what Massive sends, writing what it takes.

Every symbol the library takes or shows is Massive's canonical REST ticker (`symbols.py`), but
Massive itself writes a few symbols differently per transport: `EUR/USD` in WebSocket forex
frames, `BTC-USD` in WebSocket crypto frames, `X:EUR-USD` in the forex quotes flat-file sample.
This module holds both directions of that mapping, which every transport applies:

- **Reading.** `canonical_from_wire` reads every spelling the library knows into the canonical
  form, and `WireSymbols` applies it to a column of rows (`ReadSymbols`). A row whose spelling
  it cannot read is withheld by its reader (`withheld.withhold_rows`), whose
  `unreadable_symbols` setting decides whether that fails the read; the row is an
  `unreadable_symbols` problem of the table, which the session's problem reporter logs. An
  index endpoint may send an index without its
  `I:` prefix, so `canonical_from_wire` adds it there, from the endpoint's class alone: no
  conversion ever looks a name up.
- **Writing.** `wire_spellings` lists every spelling a flat file may write, for its row
  filter, and `ws_symbol` writes the WebSocket topic form. A joined crypto ticker is never
  split by guessing where its quote starts: the transports take every split from Massive's
  crypto ticker list and pass it in.
- **Option underlyings.** Massive's options contracts list writes an index underlying
  without its `I:` (`SPX`) and finds no contract for `I:SPX`, while the option chain takes
  and writes `I:SPX`. `contracts_underlying` writes the spelling the contracts list takes, and
  `index_underlyings` reads its rows' underlyings back into the canonical form, by the CFI
  code each row carries; `is_index_option` reads one CFI code.

The grammar these read and write, and its checks and converters, are `symbols.py`'s. Every
function here is pure and thread-safe and makes no Deephaven call.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, assert_never

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.symbols import (
    AS_WRITTEN,
    CANONICAL_GRAMMARS,
    forex_codes,
    read_crypto_pair,
    read_forex_pair,
    symbol_class,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix

if TYPE_CHECKING:
    from collections.abc import Mapping


__all__ = [
    "ReadSymbols",
    "WireSymbols",
    "canonical_from_wire",
    "contracts_underlying",
    "index_underlyings",
    "is_index_option",
    "unreadable_symbol_reason",
    "wire_spellings",
    "ws_symbol",
]


logger = logging.getLogger(__name__)


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
        return raw if AS_WRITTEN.fullmatch(raw) is not None else None
    resolved = symbol_class(raw) if asset_class is ClassFromPrefix.ANY else asset_class
    if resolved is AssetClass.FOREX:
        return read_forex_pair(raw, ("C:", "X:"))
    if resolved is AssetClass.CRYPTO:
        return read_crypto_pair(raw, ("X:",))
    if resolved is AssetClass.FUTURES:
        return raw if AS_WRITTEN.fullmatch(raw) is not None else None
    if asset_class is AssetClass.INDICES and not raw.startswith("I:"):
        raw = f"I:{raw}"
    return raw if CANONICAL_GRAMMARS[resolved](raw) else None


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
    elif (
        asset_class is AssetClass.STOCKS
        or asset_class is AssetClass.OPTIONS
        or asset_class is AssetClass.FUTURES
    ):
        return (symbol,)  # the files write these classes' canonical text
    else:
        assert_never(asset_class)  # a new class needs its spellings here
    return tuple(f"{p}{base}{s}{quote}" for p in prefixes for s in ("", "/", "-"))


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


_INDEX_PREFIX: Final = "I:"
"""The prefix of a canonical index ticker."""

_OPTION_ON_AN_INDEX: Final = r"^O..I"
"""The CFI codes (ISO 10962) of an option whose underlying is an index: an `O` (an option),
then its type and exercise style, then `I`, the underlying's kind (`OCEICS` for an SPX call;
`OCASPS`, with `S` for a stock, for an AAPL call)."""


def is_index_option(cfi: str) -> bool:
    """Tell whether a contract's CFI code (ISO 10962) marks an option on an index.

    Args:
        cfi: The contract's CFI code, such as `"OCEICS"` for an SPX call.

    Returns:
        True for an option on an index (`_OPTION_ON_AN_INDEX`); False for any other code,
        such as `"OCASPS"` for a stock's call.
    """
    return re.match(_OPTION_ON_AN_INDEX, cfi) is not None


def contracts_underlying(symbol: str) -> str:
    """Return an underlying as Massive's options contracts list takes it: an index bare.

    The list's `underlying_ticker` finds no contract for `I:SPX` and every contract of `SPX`,
    and the list writes `SPX`; any other underlying is sent canonical (`AAPL`, `BRK.B`).

    Args:
        symbol: A canonical stock or index ticker.

    Returns:
        The ticker without its `I:` prefix for an index, otherwise unchanged.
    """
    return symbol.removeprefix(_INDEX_PREFIX)


def index_underlyings(underlyings: pa.Array, cfi: pa.Array) -> pa.Array:
    """Read the underlyings of options rows into the canonical form, by each row's CFI code.

    Massive's options contracts list writes an index underlying bare (`SPX`), while the option
    chain writes it `I:SPX`; the row's CFI code says which underlyings are indices. A row
    whose CFI code marks an option on an index (`_OPTION_ON_AN_INDEX`) gets `I:` unless it has
    it; every other row, a null code included, is unchanged.

    Args:
        underlyings: The rows' underlyings, as text.
        cfi: The rows' CFI codes, as text.

    Returns:
        The underlyings, every index one with its `I:` prefix.
    """
    on_index = pc.and_(
        pc.fill_null(pc.match_substring_regex(cfi, _OPTION_ON_AN_INDEX), fill_value=False),
        pc.invert(pc.starts_with(underlyings, _INDEX_PREFIX)),
    )
    prefixed = pc.binary_join_element_wise(_INDEX_PREFIX, underlyings, "")
    return pc.if_else(on_index, prefixed, underlyings)


def unreadable_symbol_reason(text: str) -> str:
    """Say why a row whose symbol the library cannot read is withheld.

    Args:
        text: The symbol as Massive sent it.

    Returns:
        The reason, which names the spelling as Massive sent it; whether Massive's lists hold
        it, which would make it the library's to read, the site cannot know.
    """
    return (
        f"Massive sent the symbol {text!r}, a spelling the library cannot read as Massive's ticker"
    )


@dataclass(frozen=True, slots=True)
class ReadSymbols:
    """One batch's wire symbols, read into the canonical form by `WireSymbols.read`.

    Attributes:
        symbols: The canonical symbols, one per row; null for a null wire symbol and for a
            spelling the library cannot read.
        unreadable_rows: The rows whose wire symbol is a spelling the library cannot read.
    """

    symbols: pa.Array
    unreadable_rows: pa.Array


@dataclass(frozen=True, slots=True)
class WireSymbols:
    """How one dataset reads the symbols its rows carry.

    Built once per transport plan (REST converter, WebSocket channel plan, flat-file
    transform); immutable and shared by every thread of the plan. Its reader withholds a row
    whose spelling it cannot read, and each stream or read applies its own policy to it, since
    one WebSocket plan serves streams of several policies.

    Attributes:
        dataset: The dataset name, for messages.
        asset_class: The dataset's class (`canonical_from_wire`), or `ClassFromPrefix.ANY`
            for one that spans classes.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`canonical_from_wire`).
    """

    dataset: str
    asset_class: AssetClass | ClassFromPrefix
    as_written: bool = False

    def read(self, raw: pa.Array, cache: dict[str, str | None]) -> ReadSymbols:
        """Read a column of wire symbols, calling `canonical_from_wire` once per distinct text.

        Args:
            raw: The rows' symbols as sent; nulls allowed.
            cache: A map from wire text to canonical symbol (None for unreadable), read and
                extended here: one the caller keeps between batches and owns on one thread, or
                an empty one to read a batch afresh.

        Returns:
            The canonical symbols, and the rows whose spelling cannot be read. Nothing is
            logged: the caller withholds those rows.
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
        missing = pc.call_function("is_null", [symbols])
        unread = pc.call_function("and", [missing, pc.call_function("is_valid", [raw])])
        return ReadSymbols(symbols, unread)
