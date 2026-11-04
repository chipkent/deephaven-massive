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
`history(start, end=None)`. Every query also has `columns()`, a table of its columns and what
each one means. Live data is real-time by default: Massive's Starter and Developer plans
stream stocks, options, indices, and futures 15 minutes late (10 for futures), so on those plans
list the subscription in `MassiveConfig(delayed_subscriptions=...)`, or its live tables and its
current snapshots fail with a message that names that setting. A dataset your plan lacks fails
with a message that says what was refused and, where the library has another way to read the
data, names it. The library runs inside an existing Deephaven server (it needs an active
`ExecutionContext`) and never starts one.

Example:
    In the Deephaven console, with your key in `MASSIVE_API_KEY`::

        import deephaven_massive as dm

        m = dm.MassiveSession()

        # A history table: Apple's daily bars for one week, read once. Works on every plan,
        # the free one included.
        bars = m.stocks.bars("AAPL", dm.Timespan.DAY)
        week = bars.history("2025-06-02", "2025-06-06")

        # What each column of those bars means.
        columns = bars.columns()

        # A live table: the latest minute bar of each symbol, updated as each minute closes.
        # Needs a paid Stocks plan. On a delayed one, such as Stocks Starter, create the
        # session with
        # dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})).
        latest = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).last_by()

Status:
    Every dataset is implemented over all three transports and tested against simulated Massive
    servers built from Massive's documentation. The live tests have also run against Massive on
    a mix of paid and free plans. Questions those plans cannot settle remain open (which host a
    delayed plan streams from, how index daily bars and futures sessions are stamped, parts of
    the flat files); there the library follows its best reading of the documentation. Where
    Massive sends something unexpected, the library serves what it can: a value that doesn't
    fit is null, a row missing its symbol or time is dropped, and each is logged once.
    The library is not on PyPI yet.

Read next:
    The README, https://github.com/chipkent/deephaven-massive; the dataset finder,
    https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/README.md, whose page
    for each dataset gives its arguments, their rules, and its columns; troubleshooting,
    https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md; and
    `help()` on any object.

`import deephaven_massive` is cheap and needs no JVM. `MassiveSession`, `MassiveConfig`, `ALL`,
`datasets()`, the `canonical_symbol*` converters, and `canonicalize_symbols` need a running
Deephaven JVM: use them inside a Deephaven server, or after starting `deephaven_server.Server`.
The exceptions, the enums, `enable_debug_logging`, `disable_debug_logging`, `LogOutput`, and
`__version__` do not.

Public API:
    - `MassiveSession(config=None)`: the entry point; namespaces, `snapshots()`,
      `connections()`, `streams()`, `rate_limits()`, `symbol_reads()`, `release(table)`,
      `close()`, and use as a context manager.
    - `MassiveConfig`: the frozen configuration; `MassiveConfig()` reads `MASSIVE_API_KEY`.
    - `datasets()`: a static table of every dataset, with its summary, verbs, transports, and
      the link to its columns; it needs no session and no API key.
    - `MassiveError`, `AuthenticationError`, `EntitlementError`: the exceptions; each one's
      `cause`, an `ErrorCause`, says whose problem it is: expected, a defect in the user's code
      or input, or a defect of the library (whose message says how to report it).
    - `ALL`: every symbol on a stream, in a flat file, or in a snapshot.
    - The symbol converters. Every dataset takes, and every `Symbol` column holds, Massive's exact
      REST ticker (`AAPL`, `BRK.B`, `KIMpL`, `O:AAPL250117C00150000`, `I:SPX`, `C:EURUSD`,
      `X:BTCUSD`). A dataset's `symbols` can also be a table with a `Symbol` column, and a live one
      is followed as rows are added and deleted. The converters turn other spellings into Massive's:
      `canonical_symbol(text, asset_class, convention=SymbolConvention.MASSIVE)`, one function per
      class for query strings (`canonical_symbol_stock`, `canonical_symbol_option`,
      `canonical_symbol_future`, `canonical_symbol_future_spread`, `canonical_symbol_index`,
      `canonical_symbol_forex`, `canonical_symbol_crypto`), one per stock convention
      (`canonical_symbol_stock_yahoo`, `canonical_symbol_stock_nasdaq`,
      `canonical_symbol_stock_cms`), and the table form `canonicalize_symbols`. In a query string,
      import a converter by bare name for a `String` column
      (`from deephaven_massive import canonical_symbol_forex`); called as `dm.…` it makes an
      `Object` column unless cast, `(String) dm.canonical_symbol_forex(Pair)`.
    - The enums (all `StrEnum`s; a value is Massive's wire string where the enum reaches the
      wire): `AssetClass`, `Timespan`, `PollMode`, `HistorySource`, `Reread`, `FuturesVenue`,
      `Direction`, `ContractType`, `SeriesType`, `Timeframe`, `Order`, `Market`, `TickerType`,
      `Locale`, `ConditionDataType`, `Sip`, `SymbolConvention`, `OnError` (what a bad symbol does,
      in `canonicalize_symbols` and in `MassiveConfig`'s `noncanonical_symbols`, `unlisted_symbols`,
      `unreadable_symbols`, and `unavailable_symbols`); and `Subscription`, Massive's five
      subscriptions. Parameters take only enum members; convert text with, for example,
      `Timespan("minute")`.
    - `enable_debug_logging(output=LogOutput.STDERR)` and `LogOutput`: send the library's log,
      DEBUG and up, to stderr (which Deephaven shows in its log panel) or to a file; the
      environment variable `DEEPHAVEN_MASSIVE_LOG=DEBUG` (and `DEEPHAVEN_MASSIVE_LOG_FILE`)
      does the same when the first session is created. Attach that log to a bug report. It
      never holds a secret. `disable_debug_logging()` turns it off again.

    The query classes (`Query`, `StreamQuery`, `HistoryQuery`, `StreamHistoryQuery`) and the
    argument types (`Symbols`, `DateLike`, `FilterValue`) are in the public module
    `deephaven_massive.queries`, for type annotations; this package does not import it.

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
    "ErrorCause",
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
    "Reread",
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
    "ErrorCause": "deephaven_massive._errors",
    "FuturesVenue": "deephaven_massive._enums",
    "HistorySource": "deephaven_massive._enums",
    "Reread": "deephaven_massive._enums",
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
        Reread,
        SeriesType,
        Sip,
        Subscription,
        SymbolConvention,
        TickerType,
        Timeframe,
        Timespan,
    )
    from deephaven_massive._errors import (
        AuthenticationError,
        EntitlementError,
        ErrorCause,
        MassiveError,
    )
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
