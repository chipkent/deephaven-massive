"""Market data from Massive as live and historical Deephaven tables, for traders and quants.

Testing a trading idea usually takes one program for history and another for the live feed.
Here, one query of a streaming dataset gives both, with the same columns.

Every dataset works the same way. Create a session, call a dataset method to get a query, and
call one of the query's methods to get a Deephaven table.

Example:
    In the Deephaven console, with your API key in `MASSIVE_API_KEY`::

        import deephaven_massive as dm

        m = dm.MassiveSession()

        # Apple's daily bars for one week, on any Stocks plan.
        apple = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")

        # Each stock's latest minute bar, live and real-time on Stocks Advanced.
        latest = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).last_by()

Datasets:
    Each dataset is a method of a namespace on the session. The namespaces by asset class are
    `m.stocks`, `m.options`, `m.futures`, `m.indices`, `m.forex`, and `m.crypto`. The ones by
    topic are `m.reference`, `m.indicators`, `m.economy`, and `m.partners`. `m.snapshots()`
    reads symbols of several asset classes at once. `dm.datasets()` lists every dataset in a
    table, with each one's query methods in its `Methods` column.

    To annotate your own code, import the namespace classes, such as `StocksNamespace` for
    `m.stocks`, from `deephaven_massive.namespaces`, and the query classes and argument types
    from `deephaven_massive.typehints`.

Query methods:
    A query reads nothing until you call one of its methods. It has only the methods its
    dataset supports:

    - `history(start, end)`: a table of a past range.
    - `fetch()`: a table of what Massive has now.
    - `poll(interval)`: a live table that fetches again every `interval` seconds.
    - `blink()`: a live table of a stream's rows from the latest update cycle.
    - `append_only()`: a live table of every row a stream sends after the call.
    - `ring(capacity)`: a live table of a stream's latest `capacity` rows.
    - `last_by()`: a live table of each symbol's latest row from a stream.

    `columns()` on any query lists its columns and what each one means.

    A live table updates while anything uses it: a variable, a panel, or a table made from it.
    `m.release(table)` drops your variable's hold at once. The table then stops when no panel
    or table made from it is left. `m.close()` stops every live table of the session.

Settings:
    `dm.MassiveConfig` holds a session's settings. Pass it as `dm.MassiveSession(config)`. Two
    settings match a session to your plans:

    - `delayed_subscriptions` lets live tables and snapshots read a plan's delayed data, as on
      Stocks Starter.
    - `rest_basic_subscriptions` keeps a free Basic plan's requests within its limit from the
      first request.

    `help(dm.MassiveConfig)` lists every setting.

Symbols:
    Datasets take Massive's tickers, spelled exactly: `AAPL`, `BRK.B`, `O:SPY271217C00700000`,
    `I:SPX`, `C:EURUSD`, `X:BTCUSD`, and `ESZ6`. `dm.ALL` means every symbol, in the datasets
    that take it. A table with a `Symbol` column works too. A live one is a watchlist, which
    the library follows as you add and delete rows. `dm.canonical_symbol` and
    `dm.canonicalize_symbols` convert other spellings, such as Yahoo's `BRK-B`.

Enums:
    An argument with a fixed set of values takes an enum member, such as `dm.Timespan.MINUTE`.
    `dm.Timespan("minute")` converts text to a member.

Errors:
    Errors from Massive, failed reads, and failed live tables are all `dm.MassiveError`s.
    `dm.AuthenticationError` means Massive refused your keys. `dm.EntitlementError` means your
    plan doesn't include the data. Each error's `cause` is a `dm.ErrorCause`, which says
    whether the error is expected, a mistake in your code, or a bug in the library to report.
    A wrong argument raises `TypeError` or `ValueError` at the call instead.
    `dm.enable_debug_logging()` records what the library does, for a bug report.

Server:
    Sessions run inside a Deephaven server: in its console, in an application script, or in
    Python after starting `deephaven_server.Server`. Without a server, you can import the
    package and use its errors, enums, and logging functions.

Next steps:
    - The README and its tutorial: https://github.com/chipkent/deephaven-massive
    - Every dataset's arguments, plans, and columns:
      https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/README.md
    - Error messages and their fixes:
      https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md
    - `help()` on any object, such as `help(m.stocks.bars)`.

Attributes:
    __version__: The installed version, such as `"0.1.0"`.
"""

from importlib import import_module as _import_module
from importlib.metadata import version as _version
from typing import TYPE_CHECKING

__all__ = [
    "ALL",
    "AssetClass",
    "AuthenticationError",
    "ConditionDataType",
    "ConnectionTaken",
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
    "ALL": "deephaven_massive._datasets.symbols",
    "AssetClass": "deephaven_massive._enums",
    "AuthenticationError": "deephaven_massive._errors",
    "ConditionDataType": "deephaven_massive._enums",
    "ConnectionTaken": "deephaven_massive._enums",
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
        ALL,
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
        ConnectionTaken,
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
