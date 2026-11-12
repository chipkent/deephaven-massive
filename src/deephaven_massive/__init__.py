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

    To annotate your own code, import the class of a namespace, such as `StocksNamespace` for
    `m.stocks`, from `deephaven_massive.namespaces`, and the class of a query, such as
    `StreamHistoryQuery`, from `deephaven_massive.queries`. The types of the arguments,
    `dm.SymbolsLike`, `dm.DateLike`, and `dm.FilterValue`, are here, and so is
    `dm.AllSymbols`, the type of `dm.ALL`.

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
    Every error Massive reports, and every failure the library catches, is a
    `dm.MassiveError`. `dm.AuthenticationError` means your keys are missing or Massive refused
    them. `dm.EntitlementError` means your plan doesn't include the data.
    `dm.SessionClosedError` means the session is closed. Each error's `source`, a
    `dm.ErrorSource`, says what stopped the call, its `is_retryable` says whether the same call
    may succeed later, and its `is_bug` says whether a bug caused it.
    A wrong argument raises `TypeError` or `ValueError` at the call instead. In a read of
    several symbols, a parameter only some symbols' endpoint lacks fails those symbols with a
    `dm.MassiveError` whose `is_bug` is `True`.
    `dm.enable_debug_logging()` records what the library does, for a bug report.

Server:
    The library runs inside a Deephaven server: in its console, in an application script, or
    in a Python program that starts one first. Importing the package without a running server
    fails with Deephaven's own error. In a plain Python program, start the server before the
    import::

        from deephaven_server import Server

        Server(port=10000, jvm_args=["-Xmx4g"]).start()

        import deephaven_massive as dm

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

from importlib.metadata import version as _version

from deephaven_massive._config import MassiveConfig
from deephaven_massive._dataset_list import datasets
from deephaven_massive._datasets.dates import DateLike
from deephaven_massive._datasets.filters import FilterValue
from deephaven_massive._datasets.symbols import (
    ALL,
    AllSymbols,
    SymbolsLike,
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
from deephaven_massive._debug_log import LogOutput, disable_debug_logging, enable_debug_logging
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
    ErrorSource,
    MassiveError,
    SessionClosedError,
)
from deephaven_massive._session import MassiveSession
from deephaven_massive._tables.symbols import canonicalize_symbols

__all__ = [
    "ALL",
    "AllSymbols",
    "AssetClass",
    "AuthenticationError",
    "ConditionDataType",
    "ConnectionTaken",
    "ContractType",
    "DateLike",
    "Direction",
    "EntitlementError",
    "ErrorSource",
    "FilterValue",
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
    "SessionClosedError",
    "Sip",
    "Subscription",
    "SymbolConvention",
    "SymbolsLike",
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
