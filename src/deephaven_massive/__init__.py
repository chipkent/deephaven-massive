"""Massive.com market data as live and static Deephaven tables.

`deephaven-massive` connects a Deephaven server to Massive.com (formerly Polygon.io). It turns
Massive's three transports into ordinary Deephaven tables:

- **WebSocket streams** (trades, quotes, bars, fair market value, index values, and more)
  become refreshing tables: blink, append-only, ring, or one row per symbol.
- **REST endpoints** (snapshots, reference data, fundamentals, filings, economy, and partner
  datasets) become static tables, or refreshing tables that re-poll on an interval.
- **S3 flat files** (full-day trades, quotes, and bars) become static historical tables.

Every dataset is a method on a namespace grouped by asset class (`stocks`, `options`,
`futures`, `indices`, `forex`, `crypto`) or by topic (`reference`, `indicators`, `economy`,
`partners`). Each method returns an immutable query, and a verb on that query picks the table
shape: `blink()`, `append_only()`, `ring(n)`, `last_by()`, `fetch()`, `poll(interval)`, or
`history(start, end=None)`. The same code runs on every Massive plan: the library finds the right
WebSocket host for your key, and a dataset your plan lacks fails the table with a message that
names the alternative. The library runs inside an existing Deephaven server (it needs an active
`ExecutionContext`) and never starts one.

Each public name is imported on first use (PEP 562), so `import deephaven_massive` is cheap
and needs no JVM. `MassiveSession`, `MassiveConfig`, `ALL`, and the symbol converters need a
running Deephaven JVM: use them inside a Deephaven worker, or after starting
`deephaven_server.Server`. The exceptions and enums do not.

Public API:
    - `MassiveSession(config=None)`: the entry point; namespaces, `snapshots()`,
      `connections()`, `streams()`, `rate_limits()`, `release(table)`, `close()`, and use as
      a context manager.
    - `MassiveConfig`: the frozen configuration; `MassiveConfig()` reads `MASSIVE_API_KEY`.
    - `datasets()`: a static table of every dataset, with its summary, verbs, transports, and
      the link to its columns; it needs no session and no API key.
    - `MassiveError`, `AuthenticationError`, `EntitlementError`: the exceptions.
    - `ALL`: every symbol on a stream, in a flat file, or in a snapshot.
    - The symbol converters. Every dataset takes, and every `Symbol` column holds, Massive's
      exact REST ticker (`AAPL`, `BRK.B`, `KIMpL`, `O:AAPL250117C00150000`, `I:SPX`,
      `C:EURUSD`, `X:BTCUSD`); these turn other spellings into it:
      `canonical_symbol(text, asset_class, convention=SymbolConvention.MASSIVE)`, one function
      per class for query strings (`canonical_symbol_stock`, `canonical_symbol_option`,
      `canonical_symbol_future`, `canonical_symbol_future_spread`, `canonical_symbol_index`,
      `canonical_symbol_forex`, `canonical_symbol_crypto`), one per stock convention
      (`canonical_symbol_stock_yahoo`, `canonical_symbol_stock_nasdaq`,
      `canonical_symbol_stock_cms`), and the table form `canonicalize_symbols`. In a query
      string, import a converter by bare name for a `String` column
      (`from deephaven_massive import canonical_symbol_forex`); called as `dm.…` it makes an
      `Object` column unless cast, `(String) dm.canonical_symbol_forex(Pair)`.
    - The enums (all `StrEnum`s; a value is Massive's wire string where the enum reaches the
      wire): `AssetClass`, `Timespan`, `PollMode`, `HistorySource`, `FuturesVenue`,
      `Direction`, `ContractType`, `SeriesType`, `Timeframe`, `Order`, `Market`, `TickerType`,
      `Locale`, `ConditionDataType`, `Sip`, `SymbolConvention`, `OnError` (what a bad symbol
      does, in `canonicalize_symbols` and in `MassiveConfig`'s `noncanonical_symbols` and
      `unreadable_symbols`); and `Subscription`, Massive's five subscriptions.
      Parameters take only enum members; convert text with, for example,
      `Timespan("minute")`.
    - `enable_debug_logging(output=LogOutput.STDERR)` and `LogOutput`: send the library's log,
      DEBUG and up, to stderr (which Deephaven shows in its log panel) or to a file; the
      environment variable `DEEPHAVEN_MASSIVE_LOG=DEBUG` (and `DEEPHAVEN_MASSIVE_LOG_FILE`)
      does the same when the first session is created. Attach that log to a bug report. It
      never holds a secret. `disable_debug_logging()` turns it off again.

    The query classes (`Query`, `StreamQuery`, `HistoryQuery`, `StreamHistoryQuery`) and the
    argument types (`Symbols`, `DateLike`, `FilterValue`) are in the public module
    `deephaven_massive.queries`, for type annotations; this package does not import it.

Status:
    The library is under active development. The public surface, configuration, session
    lifecycle, and query classes are in place; the REST, WebSocket, and flat-file transports
    and the datasets of each namespace are not implemented yet, so the verbs that need a
    transport raise `NotImplementedError`.

Example:
    A session with a pinned stocks host, inside a Deephaven worker::

        import deephaven_massive as dm

        cfg = dm.MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "delayed.massive.com"})
        m = dm.MassiveSession(cfg)
        print(m, dm.__version__)  # <MassiveSession #1 open> 0.1.0
        m.close()

Attributes:
    __version__: The installed distribution's version string, such as `"0.1.0"`. It comes from
        the package metadata (`importlib.metadata`), so it always matches what pip or uv
        installed.
"""

from importlib import import_module as _import_module
from importlib.metadata import version as _version
from typing import TYPE_CHECKING

__all__ = [
    "ALL",
    "AssetClass",
    "AuthenticationError",
    "ConditionDataType",
    "ContractType",
    "Direction",
    "EntitlementError",
    "FuturesVenue",
    "HistorySource",
    "Locale",
    "LogOutput",
    "Market",
    "MassiveConfig",
    "MassiveError",
    "MassiveSession",
    "OnError",
    "Order",
    "PollMode",
    "SeriesType",
    "Sip",
    "Subscription",
    "SymbolConvention",
    "TickerType",
    "Timeframe",
    "Timespan",
    "__version__",
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
    "canonicalize_symbols",
    "datasets",
    "disable_debug_logging",
    "enable_debug_logging",
]

__version__: str = _version("deephaven-massive")

# Each lazily resolved public name, and the private module that defines it.
_EXPORTS: dict[str, str] = {
    "ALL": "deephaven_massive._types",
    "AssetClass": "deephaven_massive._enums",
    "AuthenticationError": "deephaven_massive._errors",
    "ConditionDataType": "deephaven_massive._enums",
    "ContractType": "deephaven_massive._enums",
    "Direction": "deephaven_massive._enums",
    "EntitlementError": "deephaven_massive._errors",
    "FuturesVenue": "deephaven_massive._enums",
    "HistorySource": "deephaven_massive._enums",
    "Locale": "deephaven_massive._enums",
    "LogOutput": "deephaven_massive._debug_log",
    "Market": "deephaven_massive._enums",
    "MassiveConfig": "deephaven_massive._config",
    "MassiveError": "deephaven_massive._errors",
    "MassiveSession": "deephaven_massive._session",
    "Order": "deephaven_massive._enums",
    "PollMode": "deephaven_massive._enums",
    "SeriesType": "deephaven_massive._enums",
    "Sip": "deephaven_massive._enums",
    "Subscription": "deephaven_massive._enums",
    "TickerType": "deephaven_massive._enums",
    "Timeframe": "deephaven_massive._enums",
    "Timespan": "deephaven_massive._enums",
    "canonical_symbol": "deephaven_massive._datasets.symbols",
    "canonical_symbol_crypto": "deephaven_massive._datasets.symbols",
    "canonical_symbol_forex": "deephaven_massive._datasets.symbols",
    "canonical_symbol_future": "deephaven_massive._datasets.symbols",
    "canonical_symbol_future_spread": "deephaven_massive._datasets.symbols",
    "canonical_symbol_index": "deephaven_massive._datasets.symbols",
    "canonical_symbol_option": "deephaven_massive._datasets.symbols",
    "canonical_symbol_stock": "deephaven_massive._datasets.symbols",
    "canonical_symbol_stock_cms": "deephaven_massive._datasets.symbols",
    "canonical_symbol_stock_nasdaq": "deephaven_massive._datasets.symbols",
    "canonical_symbol_stock_yahoo": "deephaven_massive._datasets.symbols",
    "canonicalize_symbols": "deephaven_massive._tables.symbols",
    "OnError": "deephaven_massive._enums",
    "SymbolConvention": "deephaven_massive._enums",
    "datasets": "deephaven_massive._dataset_list",
    "disable_debug_logging": "deephaven_massive._debug_log",
    "enable_debug_logging": "deephaven_massive._debug_log",
}

# Type checkers read the imports, so they see each name's real type and flag a name outside
# `__all__`; at run time `__getattr__` resolves the names instead.
if TYPE_CHECKING:
    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._dataset_list import datasets
    from deephaven_massive._datasets.symbols import (
        canonical_symbol,
        canonical_symbol_crypto,
        canonical_symbol_forex,
        canonical_symbol_future,
        canonical_symbol_future_spread,
        canonical_symbol_index,
        canonical_symbol_option,
        canonical_symbol_stock,
        canonical_symbol_stock_cms,
        canonical_symbol_stock_nasdaq,
        canonical_symbol_stock_yahoo,
    )
    from deephaven_massive._debug_log import (
        LogOutput,
        disable_debug_logging,
        enable_debug_logging,
    )
    from deephaven_massive._enums import (
        AssetClass,
        ConditionDataType,
        ContractType,
        Direction,
        FuturesVenue,
        HistorySource,
        Locale,
        Market,
        OnError,
        Order,
        PollMode,
        SeriesType,
        Sip,
        Subscription,
        SymbolConvention,
        TickerType,
        Timeframe,
        Timespan,
    )
    from deephaven_massive._errors import AuthenticationError, EntitlementError, MassiveError
    from deephaven_massive._session import MassiveSession
    from deephaven_massive._tables.symbols import canonicalize_symbols
    from deephaven_massive._types import ALL
else:

    def __getattr__(name: str) -> object:
        """Resolve a public name by importing the private module that defines it.

        Nothing is cached in this module's namespace. The import system caches the defining
        module, and its per-module lock serializes concurrent first imports, so this function
        writes nothing itself and a repeated lookup costs a dictionary lookup.

        Args:
            name: The attribute requested.

        Returns:
            The object `name` refers to.

        Raises:
            AttributeError: If `name` is not a public name of the package.
        """
        try:
            module = _EXPORTS[name]
        except KeyError:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
        return getattr(_import_module(module), name)


def __dir__() -> list[str]:
    """Return the public names, for completion in IDEs and the Deephaven console.

    Returns:
        `__all__`, sorted.
    """
    return sorted(__all__)


# Keep the namespace to the public names: the helper import goes once the module is built.
del TYPE_CHECKING
