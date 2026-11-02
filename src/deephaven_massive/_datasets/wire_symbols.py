"""A symbol's transport spellings, both ways: reading what Massive sends, writing what it takes.

Every symbol the library takes or shows is Massive's canonical REST ticker (`symbols.py`), but
Massive itself writes a few symbols differently per transport: `EUR/USD` in WebSocket forex
frames, `BTC-USD` in WebSocket crypto frames, `X:EUR-USD` in the forex quotes flat-file sample.
This module holds both directions of that mapping, which every transport applies:

- **Reading.** `canonical_from_wire` reads every spelling the library knows into the canonical
  form, and `WireSymbols` applies it to a column of rows (`ReadSymbols`) and the session's
  `unreadable_symbols` setting to what it cannot read: a bounded ERROR record per spelling
  under `LOG`, a bounded DEBUG record under `IGNORE`, or the failure
  `unreadable_symbol_message` words under `RAISE`. An index endpoint may send an index without its
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
  code each row carries.

The grammar these read and write, and its checks and converters, are `symbols.py`'s. Every
function here is pure and thread-safe and makes no Deephaven call; the one shared state is the
process-wide bound on the records of unreadable spellings.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

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
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._log_limits import BoundedLog

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


__all__ = [
    "ReadSymbols",
    "WireSymbols",
    "canonical_from_wire",
    "contracts_underlying",
    "index_underlyings",
    "unreadable_symbol_message",
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
    else:
        return (symbol,)
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


_UNREADABLE_LIMIT: Final = 100
"""The most unreadable wire spellings logged per process; one record then says the rest are
not."""

_logged_spellings: Final = BoundedLog(logger, limit=_UNREADABLE_LIMIT, level=logging.ERROR)
"""The spellings logged under `OnError.LOG`: at ERROR, since each is a gap in the library."""

_ignored_spellings: Final = BoundedLog(logger, limit=_UNREADABLE_LIMIT, level=logging.DEBUG)
"""The spellings logged under `OnError.IGNORE`: at DEBUG only."""


def unreadable_symbol_message(dataset: str, spellings: Iterable[str]) -> str:
    """Say which symbols Massive sent in a spelling the library cannot read, and what to do.

    Such a spelling is a gap in the library's grammar, never the caller's fault, so the message
    asks for a bug report and names the setting that keeps the rows meanwhile.

    Args:
        dataset: The dataset that received them, such as `forex.quotes`.
        spellings: The texts, as sent.

    Returns:
        The message a failed request or stream carries under `OnError.RAISE`.
    """
    texts = sorted(spellings)
    listed = ", ".join(repr(s) for s in texts)
    sent = f"the symbols {listed}, spellings" if texts[1:] else f"the symbol {listed}, a spelling"
    return (
        f"{dataset}: Massive sent {sent} the library cannot read as Massive's ticker. To keep "
        "such rows with a null Symbol and the text in RawSymbol meanwhile, pass "
        "MassiveConfig(unreadable_symbols=dm.OnError.LOG), or write 'unreadable_symbols: log' "
        f"in the settings file. {LIBRARY_BUG}"
    )


@dataclass(frozen=True, slots=True)
class ReadSymbols:
    """One batch's wire symbols, read into the canonical form by `WireSymbols.read`.

    Attributes:
        symbols: The canonical symbols, one per row; null for a null wire symbol and for a
            spelling the library cannot read.
        raw: The wire text where the spelling could not be read, null elsewhere: the
            `RawSymbol` column of a session that keeps such rows.
        unreadable: The distinct spellings that could not be read, sorted; empty for a clean
            batch.
    """

    symbols: pa.Array
    raw: pa.Array
    unreadable: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WireSymbols:
    """How one dataset reads the symbols its rows carry, and what an unreadable one does.

    Built once per transport plan (REST converter, WebSocket channel plan, flat-file
    transform) from the dataset and the session's `MassiveConfig.unreadable_symbols`;
    immutable and shared by every thread of the plan.

    Attributes:
        dataset: The dataset name, for messages.
        asset_class: The dataset's class (`canonical_from_wire`), or `ClassFromPrefix.ANY`
            for one that spans classes.
        unreadable: The session's setting for a spelling `canonical_from_wire` cannot read:
            under `OnError.LOG` each new spelling is logged once at ERROR with the library-bug
            text, and under `IGNORE` at DEBUG (a bounded number per process); under both its
            rows keep the text in `ReadSymbols.raw`. Under `RAISE` the caller fails the request
            or stream with `unreadable_symbol_message`.
        as_written: True for a dataset whose symbols are kept as Massive writes them
            (`canonical_from_wire`).
    """

    dataset: str
    asset_class: AssetClass | ClassFromPrefix
    unreadable: OnError
    as_written: bool = False

    @property
    def keeps(self) -> bool:
        """Tell whether a row whose spelling cannot be read is kept, with its text in `RawSymbol`.

        Returns:
            True under `OnError.LOG` and `IGNORE`; False under `RAISE`, which fails it.
        """
        return self.unreadable is not OnError.RAISE

    def read(self, raw: pa.Array, cache: dict[str, str | None]) -> ReadSymbols:
        """Read a column of wire symbols, calling `canonical_from_wire` once per distinct text.

        Args:
            raw: The rows' symbols as sent; nulls allowed.
            cache: A map from wire text to canonical symbol (None for unreadable), read and
                extended here: one the caller keeps between batches and owns on one thread, or
                an empty one to read a batch afresh.

        Returns:
            The canonical symbols, the unreadable texts, and the distinct unreadable spellings.
            Each new unreadable spelling is logged once: at ERROR with the library-bug text
            under `LOG`, at DEBUG under `IGNORE`; never under `RAISE`, whose failure the caller
            reports.
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
        unreadable = tuple(sorted(t for t in texts if t is not None and known[t] is None))
        missing = pc.call_function("is_null", [symbols])
        unread = pc.call_function("and", [missing, pc.call_function("is_valid", [raw])])
        kept = pc.call_function("if_else", [unread, raw, pa.scalar(None, pa.string())])
        if self.keeps:
            spellings = _logged_spellings if self.unreadable is OnError.LOG else _ignored_spellings
            for text in unreadable:
                spellings.log(
                    f"{self.dataset}\0{text}",
                    "%s: Massive sent the symbol %r, a spelling the library cannot read; its "
                    "rows have a null Symbol and keep the text in RawSymbol. %s",
                    self.dataset,
                    text,
                    LIBRARY_BUG,
                )
        return ReadSymbols(symbols, kept, unreadable)
