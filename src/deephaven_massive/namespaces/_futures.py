"""The `m.futures` namespace: US futures (CME, CBOT, NYMEX, COMEX).

Datasets: `trades`, `quotes`, and `bars` (streamed over the `/futures` WebSocket cluster, with
REST and flat-file history), `snapshots` (the current state of contracts), and the reference
datasets `contracts`, `products`, `exchanges`, `market_status`, and `schedules`.

Each dataset is one factory method on `FuturesNamespace` that validates its arguments through
`build_query` (`namespaces/_query_building.py`) and returns a query; nothing touches the network
until a verb is called on the query. This module owns the namespace's `DatasetSpec`s, exported as
`DATASETS` and composed into the dataset catalog (`CATALOG` in `_session.py`). A spec names the
generated sources of its dataset and the key columns of each source; the columns, their types, and
their units come from the generated schemas.

Futures differ from the other asset classes in three ways that show here:

- **Sessions.** A futures trading day is a session that ends at 17:00 America/Chicago, and every
  REST and flat-file row of trades, quotes, and bars carries its `session_end_date`, which the
  specs declare as the `date` key, so it is the row's `Date` (every flat file is checked against
  it). WebSocket messages carry none: the WebSocket transport dates a streamed row by the 17:00
  Chicago rule, corrected by Massive's futures schedules.
- **Venues.** Flat files are published per exchange (`us_futures_cme/...`), so `trades`,
  `quotes`, and `bars` take `venues`, a set that picks the flat-file prefixes. Left out, a read
  takes every venue the plan allows and skips each one S3 refuses with a warning. A venue the
  call names follows the `refused_venues` cause of its `dm.ErrorPolicy` when S3 refuses it.
  Streams always use the `/futures` cluster path, whatever the venues.
- **Bars.** The aggregates endpoint documents a `session` unit and no `day` unit, but Massive
  serves `1day` bars in the shape of one-session buckets, so `bars` sends `Timespan.DAY` as `day`
  and dates and keeps its bars as it does `Timespan.SESSION` bars.

Every `product_code=` takes one code, a list of codes (sent as `product_code.any_of`), or a table
of codes in a `ProductCode` column (`value_table`), each code read as if passed alone; a
refreshing table is followed by `fetch()` and re-read by each `poll()` cycle.

The reference datasets keep the `FUTURES` asset class, which selects the session-day rule for
their dates. `contracts` describes one contract per row, so its `Symbol` is the row's ticker
and a contracts table feeds the market-data datasets directly; the others describe no single
instrument: they take no symbols and have no `Symbol` column.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven.table import Table

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, FuturesVenue, NotBars, Timespan
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    build_query,
    day_argument,
    list_params,
    one_or_any_of,
    value_table,
)
from deephaven_massive.queries import FetchQuery, HistoryQuery, StreamHistoryQuery

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.request import ValueTable
    from deephaven_massive._datasets.symbols import SymbolsLike
    from deephaven_massive._enums import Order
    from deephaven_massive._error_policy import ErrorPolicy

__all__ = ["DATASETS", "FuturesNamespace"]


_REST, _WS, _FLAT_FILES = Transport.REST, Transport.WS, Transport.FLAT_FILES

_TRADES_PATH: Final = "/futures/v1/trades/{ticker}"
_TRADES_FILES: Final = "us_futures_{venue}/trades_v1"
_QUOTES_PATH: Final = "/futures/v1/quotes/{ticker}"
_QUOTES_FILES: Final = "us_futures_{venue}/quotes_v1"
_BARS_PATH: Final = "/futures/v1/aggs/{ticker}"
_MINUTE_FILES: Final = "us_futures_{venue}/minute_aggs_v1"
_SESSION_FILES: Final = "us_futures_{venue}/session_aggs_v1"
_SNAPSHOT_PATH: Final = "/futures/v1/snapshot"
_CONTRACTS_PATH: Final = "/futures/v1/contracts"
_PRODUCTS_PATH: Final = "/futures/v1/products"
_EXCHANGES_PATH: Final = "/futures/v1/exchanges"
_MARKET_STATUS_PATH: Final = "/futures/v1/market-status"
_SCHEDULES_PATH: Final = "/futures/v1/schedules"

_TICKER: Final = KeySpec("Ticker")
_SESSION: Final = KeySpec("SessionEndDate")


def _tick_keys(time: str) -> SourceKeys:
    """Return the keys of a REST or flat-file tick or bar source: ticker, time, and session.

    Args:
        time: The source's column that holds the row's primary instant.

    Returns:
        The keys; every such source carries its ticker and session in its rows, the session
        (`session_end_date`) as the row's `Date`.
    """
    return SourceKeys(symbol=_TICKER, time=KeySpec(time), date=_SESSION)


_PRODUCT_CODE_ARGUMENT: Final = {"product_code.any_of": "product_code"}
"""The argument that sets `product_code.any_of`, for the message that rejects it in `filters`."""

_REFERENCE_ARGUMENTS: Final = {**_PRODUCT_CODE_ARGUMENT, "date": "as_of"}
"""The arguments of `contracts` and `products` named otherwise than their wire parameters."""


@raises_through(one_or_any_of, value_table)
def _product_codes(product_code: object) -> tuple[dict[str, object], ValueTable | None]:
    """Return the wire parameters that carry a `product_code` argument, or its table.

    Every futures path that takes a product code also takes `product_code.any_of`, so one
    request serves several codes; `one_or_any_of` picks the form and checks the argument. A
    table of codes, in a `ProductCode` column, is boxed instead (`value_table`) and read when a
    verb runs: each code is sent as `product_code`, as if passed alone.

    Args:
        product_code: The argument as passed: one code, a sequence of codes, a table, or None.

    Returns:
        The two wire parameters, `product_code` and `product_code.any_of`, the unset one None
        (both None for a table), and the boxed table or None.

    Raises:
        TypeError: If `product_code` is neither text, a sequence of text, a table, nor None
            (`one_or_any_of`).
        ValueError: If `product_code` is an empty sequence (`one_or_any_of`), or a table
            without a `ProductCode` column (`value_table`).
    """
    if isinstance(product_code, Table):
        values = value_table(
            product_code, argument="product_code", parameter="product_code", underlying=False
        )
        return one_or_any_of("product_code", None, every="every product"), values
    return one_or_any_of("product_code", product_code, every="every product"), None


def _stream_keys(time: str, *, end: str | None = None) -> SourceKeys:
    """Return the keys of a WebSocket channel, whose messages carry no session date.

    Args:
        time: The channel's column that holds the row's primary instant.
        end: For a bars channel, the column of the window's end `e`; None for ticks.

    Returns:
        The ticker and time keys, and a bar's end.
    """
    return SourceKeys(symbol=_TICKER, time=KeySpec(time), end=None if end is None else KeySpec(end))


TRADES: Final = DatasetSpec(
    name="futures.trades",
    summary="Trades of futures contracts, tick by tick.",
    asset_class=AssetClass.FUTURES,
    kind="trades",
    history_path=_TRADES_PATH,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: _TRADES_FILES},
    keys={
        (_REST, _TRADES_PATH): _tick_keys("Timestamp"),
        (_WS, "futures/T"): _stream_keys("Timestamp"),
        (_FLAT_FILES, _TRADES_FILES): _tick_keys("Timestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)

QUOTES: Final = DatasetSpec(
    name="futures.quotes",
    summary="Best bid and offer quotes of futures contracts, tick by tick.",
    asset_class=AssetClass.FUTURES,
    kind="quotes",
    history_path=_QUOTES_PATH,
    channels={NotBars.ANY: "Q"},
    flat_file_prefixes={NotBars.ANY: _QUOTES_FILES},
    keys={
        (_REST, _QUOTES_PATH): _tick_keys("Timestamp"),
        (_WS, "futures/Q"): _stream_keys("Timestamp"),
        (_FLAT_FILES, _QUOTES_FILES): _tick_keys("Timestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)

BARS: Final = DatasetSpec(
    name="futures.bars",
    summary="Open, high, low, close, and volume bars of futures contracts.",
    asset_class=AssetClass.FUTURES,
    is_bars=True,
    kind="bars",
    history_path=_BARS_PATH,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.SESSION: _SESSION_FILES},
    keys={
        (_REST, _BARS_PATH): _tick_keys("WindowStart"),
        (_WS, "futures/A"): _stream_keys("WindowStart", end="EndTimestamp"),
        (_WS, "futures/AM"): _stream_keys("WindowStart", end="EndTimestamp"),
        (_FLAT_FILES, _MINUTE_FILES): _tick_keys("WindowStart"),
        (_FLAT_FILES, _SESSION_FILES): _tick_keys("WindowStart"),
    },
    time_axis=("window_start.gte", "window_start.lt"),
)

SNAPSHOTS: Final = DatasetSpec(
    name="futures.snapshots",
    summary="The latest trade, quote, minute bar, and session of each futures contract.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_SNAPSHOT_PATH,
    serves_current_data=True,
    keys={(_REST, _SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    # The contract's last quote and trade times: an explicit APPEND poll appends each new
    # state of a contract. The snapshot carries no single update time of its own.
    revision_keys=("LastQuoteTimestamp", "LastTradeTimestamp"),
)

CONTRACTS: Final = DatasetSpec(
    name="futures.contracts",
    summary="The specification of every listed futures contract.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_CONTRACTS_PATH,
    # The row's own ticker is its `Symbol`, so a contracts table is a symbol table of the
    # market-data datasets; explicit symbols travel in `ticker.any_of`, a list per request.
    keys={(_REST, _CONTRACTS_PATH): SourceKeys(symbol=_TICKER)},
    symbol_param="ticker.any_of",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    poll_keys=("Symbol", "Date"),
)

PRODUCTS: Final = DatasetSpec(
    name="futures.products",
    summary="The specification of every futures product.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_PRODUCTS_PATH,
    poll_keys=("ProductCode", "TradingVenue", "Date"),
)

EXCHANGES: Final = DatasetSpec(
    name="futures.exchanges",
    summary="The exchanges and trading venues of US futures.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_EXCHANGES_PATH,
    poll_keys=("Id",),
)

MARKET_STATUS: Final = DatasetSpec(
    name="futures.market_status",
    summary="Whether each futures product is trading right now.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_MARKET_STATUS_PATH,
    # The time of the product's latest market event is the row's time: `Timestamp` is the
    # library's name, so it comes from a declared key.
    keys={(_REST, _MARKET_STATUS_PATH): SourceKeys(time=KeySpec("Timestamp"))},
    poll_keys=("ProductCode", "TradingVenue"),
    # The time of the product's latest market event: an explicit APPEND poll appends each
    # status change.
    revision_keys=("Timestamp",),
)

SCHEDULES: Final = DatasetSpec(
    name="futures.schedules",
    summary="The timed events of each futures product's trading sessions.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_SCHEDULES_PATH,
    # The event's time is the row's time: `Timestamp` is the library's name, so it comes from
    # a declared key.
    keys={(_REST, _SCHEDULES_PATH): SourceKeys(time=KeySpec("Timestamp"))},
    time_axis=("session_end_date.gte", "session_end_date.lte"),
    poll_keys=("ProductCode", "TradingVenue", "Timestamp", "Event"),
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    QUOTES,
    BARS,
    SNAPSHOTS,
    CONTRACTS,
    PRODUCTS,
    EXCHANGES,
    MARKET_STATUS,
    SCHEDULES,
)
"""Every dataset of `m.futures`, one `DatasetSpec` each, in the order of the namespace's methods."""


class FuturesNamespace(Namespace):
    """The US futures datasets, reached as `m.futures`, for CME, CBOT, NYMEX, and COMEX contracts.

    Each method returns a query of one dataset, and together they cover trades, quotes, bars,
    snapshots, and the reference datasets of contracts, products, exchanges, market status, and
    session schedules. A method reads nothing, so it never blocks, and you can call it from
    any thread.
    A symbol is a contract ticker, such as `"ESZ5"` for the December 2025 E-mini S&P 500. A futures
    trading day is a session that ends at 17:00 Chicago time, and `Date` is that session.

    A contract trades only until its last trade date, so a live table needs a current contract.
    `contracts()` lists them. The tickers in this namespace's examples have expired. Their live
    calls need a current contract in their place. Their `history()` calls read past dates and work
    as written.

    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import FuturesNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            active = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
            es = m.futures.trades(active).last_by()  # live
            daily = m.futures.bars("ESZ5", dm.Timespan.SESSION).history("2025-01-02", "2025-03-31")
    """

    __slots__ = ()
    _path: ClassVar[str] = "futures"

    @raises_through(build_query)
    def trades(
        self,
        symbols: SymbolsLike,
        *,
        venues: AbstractSet[FuturesVenue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> StreamHistoryQuery:
        """Trades of futures contracts, tick by tick.

        Each row holds a trade's price, size, and the exchange's sequence numbers. `Date` is the
        trading session, and a date bound of `history()` is a whole session. `snapshots()` gives
        each contract's latest trade. `history()` of `dm.ALL` reads the flat files.

        Args:
            symbols: Contract tickers such as `"ESZ5"`, a contracts table, or `dm.ALL` for every
                contract.
            venues: The exchanges whose flat files `history()` reads. Left out, it reads
                every exchange your plan allows.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.trades.md.

        Raises:
            TypeError: If `symbols` is of another type, `venues` isn't a set of
                `dm.FuturesVenue`, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `venues` is empty;
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                past = m.futures.trades("ESZ5").history("2025-10-01", "2025-10-03")
        """
        return build_query(
            self._core,
            TRADES,
            query_type=StreamHistoryQuery,
            symbols=symbols,
            venues=venues,
            on_error=on_error,
        )

    @raises_through(build_query)
    def quotes(
        self,
        symbols: SymbolsLike,
        *,
        venues: AbstractSet[FuturesVenue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> StreamHistoryQuery:
        """Best bid and offer quotes of futures contracts, tick by tick.

        Each row holds a top-of-book update: bid and ask prices, sizes, and the time of each side.
        `Date` is the trading session, and a date bound of `history()` is a whole session.
        `snapshots()` gives each contract's latest quote. `history()` of `dm.ALL` reads the flat
        files.

        Args:
            symbols: Contract tickers such as `"ESZ5"`, a contracts table, or `dm.ALL` for every
                contract.
            venues: The exchanges whose flat files `history()` reads. Left out, it reads
                every exchange your plan allows.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.quotes.md.

        Raises:
            TypeError: If `symbols` is of another type, `venues` isn't a set of
                `dm.FuturesVenue`, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `venues` is empty;
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                active = m.futures.contracts(product_code="CL", filters={"active": True}).fetch()
                book = m.futures.quotes(active).last_by()
        """
        return build_query(
            self._core,
            QUOTES,
            query_type=StreamHistoryQuery,
            symbols=symbols,
            venues=venues,
            on_error=on_error,
        )

    @raises_through(build_query)
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        venues: AbstractSet[FuturesVenue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> StreamHistoryQuery | HistoryQuery:
        """Open, high, low, close, and volume bars of futures contracts.

        Each row is one bar of `multiplier` times `timespan`. Daily bars are
        `dm.Timespan.SESSION` or `dm.Timespan.DAY`; both give one bar per trading session.
        `DAY` bars are read from REST only, so for `dm.ALL` use `SESSION`, which flat files
        serve. There is no `previous_day_bar()`. With dates, `history()` keeps a bar of a
        session or longer only when all its sessions are in the range. The dataset's page,
        linked below, says how each bar's window and `Date` are set.

        Massive refuses a multiplier above its unit's maximum, such as 60 minutes, and the call
        raises `MassiveError`. Use the next unit instead.

        Args:
            symbols: Contract tickers, a contracts table, or `dm.ALL` for every contract.
            timespan: The bar's unit. `DAY` bars are read from REST only; for `dm.ALL`, use
                `SESSION`.
            multiplier: Units per bar.
            venues: The exchanges whose flat files `history()` reads. Left out, it reads
                every exchange your plan allows.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A `StreamHistoryQuery` for 1-second and 1-minute bars at `multiplier=1`, which
            Massive streams, with the stream methods and `history()`; a `HistoryQuery`, with
            `history()` only, for any other bars. A type checker allows a stream method on the
            result only after `isinstance(q, StreamHistoryQuery)`. Its columns, plans, and
            methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `venues` is empty;
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                sessions = m.futures.bars("ESZ5", dm.Timespan.SESSION).history(
                    "2025-10-01", "2025-10-31"
                )
        """
        return build_query(
            self._core,
            BARS,
            query_type=HistoryQuery,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            venues=venues,
            on_error=on_error,
        )

    @raises_through(build_query, list_params, _product_codes)
    def snapshots(
        self,
        symbols: SymbolsLike,
        *,
        product_code: str | Sequence[str] | Table | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The latest trade, quote, minute bar, and session of each futures contract.

        Each row holds one contract's last trade, quote, and minute bar, and the open, high, low,
        close, volume, and settlement of its current session. `dm.ALL` with `product_code` reads
        every contract of those products.

        A table of product codes is read value by value, each as if passed alone. A live table is
        followed. In a static table, one failed code fails the call.

        Args:
            symbols: Contract tickers, a contracts table, or `dm.ALL` for every contract.
            product_code: Only contracts of this product, such as `"ES"`, or of these, such as
                `["ES", "NQ"]`. A table of them in a `ProductCode` column works too. Leave it out
                for every product.
            sort: A field of the rows, such as `"ticker"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead, and a table of product codes caps every code's requests.
            filters: Further endpoint parameters, such as `{"ticker_gte": "ES"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.snapshots.md.

        Raises:
            TypeError: If `sort` isn't a `str`, `order` isn't a `dm.Order`, or `product_code` isn't
                a `str`, a sequence of `str`, or a table, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` holds None, or a ticker that isn't Massive's exact ticker;
                - `order` is given without `sort`;
                - `product_code` is empty, or a table without a `ProductCode` column;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                es_curve = m.futures.snapshots(dm.ALL, product_code="ES").poll(5)
        """
        codes, values = _product_codes(product_code)
        return build_query(
            self._core,
            SNAPSHOTS,
            query_type=FetchQuery,
            symbols=symbols,
            params={**codes, **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
            values=values,
            on_error=on_error,
        )

    @raises_through(build_query, list_params, _product_codes, day_argument)
    def contracts(
        self,
        symbols: SymbolsLike | None = None,
        *,
        product_code: str | Sequence[str] | Table | None = None,
        as_of: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The specification of every listed futures contract.

        Each row holds one contract as of a date: its product, trading venue, first and last trade
        dates, settlement date, tick sizes, and order-size limits. `Symbol` is the contract's
        ticker, and a combo, such as a spread, has the `Type` `combo`. The table works as `symbols`
        for `trades()`, `quotes()`, `bars()`, and `snapshots()`.

        A table of product codes is read value by value, each as if passed alone. A live table is
        followed. In a static table, one failed code fails the call.

        Args:
            symbols: Only these contracts; leave it out for every contract. `dm.ALL` is refused;
                leave `symbols` out instead.
            product_code: Only contracts of this product, such as `"ES"`, or of these, such as
                `["ES", "NQ"]`. A table of them in a `ProductCode` column works too. Leave it out
                for every product.
            as_of: The day whose specifications to list; leave it out for today.
            sort: A field of the rows, such as `"ticker"`. Without it, Massive sorts by
                `product_code`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead, and a table of product codes caps every code's requests.
            filters: Further endpoint parameters, such as `{"active": True}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.contracts.md.

        Raises:
            TypeError: If `sort` isn't a `str`, `order` isn't a `dm.Order`, `product_code` isn't a
                `str`, a sequence of `str`, or a table, or `symbols` or `as_of` is of another type,
                or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, or holds a ticker that isn't Massive's exact ticker;
                - `as_of` is a `datetime` or isn't a date;
                - `order` is given without `sort`;
                - `product_code` is empty, or a table without a `ProductCode` column;
                - a filter isn't one the dataset takes, or its value doesn't fit;
                - `filters` holds `ticker_any_of`, which `symbols` sets.

        Example:
            ::

                active = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
                es = m.futures.trades(active).last_by()
        """
        codes, values = _product_codes(product_code)
        params = {
            **codes,
            "date": None if as_of is None else day_argument(CONTRACTS, as_of, "as_of"),
            **list_params(sort=sort, order=order),
        }
        return build_query(
            self._core,
            CONTRACTS,
            query_type=FetchQuery,
            symbols=symbols,
            symbols_optional=True,
            all_alternative="leave symbols out for every contract",
            params=params,
            filters=filters,
            arguments=_REFERENCE_ARGUMENTS,
            limit=limit,
            values=values,
            on_error=on_error,
        )

    @raises_through(build_query, list_params, _product_codes, day_argument)
    def products(
        self,
        *,
        product_code: str | Sequence[str] | Table | None = None,
        as_of: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The specification of every futures product.

        Each row holds one product as of a date, such as `ES`, the E-mini S&P 500. A row gives the
        product's name, trading and clearing venues, sector and asset class, settlement method and
        currency, price quotation, and unit of measure. The dataset takes no symbols and has no
        `Symbol` column.

        A table of product codes is read value by value, each as if passed alone. A live table is
        followed. In a static table, one failed code fails the call.

        Args:
            product_code: Only this product, such as `"ES"`, or these, such as `["ES", "NQ"]`. A
                table of them in a `ProductCode` column works too. Leave it out for every product.
            as_of: The day whose specifications to list; leave it out for today.
            sort: A field of the rows, such as `"name"`. Without it, Massive sorts by `date`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A table of product codes caps every code's
                requests instead.
            filters: Further endpoint parameters, such as `{"sector": "precious"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.products.md.

        Raises:
            TypeError: If `sort` isn't a `str`, `order` isn't a `dm.Order`, `product_code` isn't a
                `str`, a sequence of `str`, or a table, or `as_of` is of another type, or `on_error`
                isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `as_of` is a `datetime` or isn't a date;
                - `order` is given without `sort`;
                - `product_code` is empty, or a table without a `ProductCode` column;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                energy = m.futures.products(filters={"asset_sub_class": "energy"}).fetch()
        """
        codes, values = _product_codes(product_code)
        params = {
            **codes,
            "date": None if as_of is None else day_argument(PRODUCTS, as_of, "as_of"),
            **list_params(sort=sort, order=order),
        }
        return build_query(
            self._core,
            PRODUCTS,
            query_type=FetchQuery,
            params=params,
            filters=filters,
            arguments=_REFERENCE_ARGUMENTS,
            limit=limit,
            values=values,
            on_error=on_error,
        )

    @raises_through(build_query)
    def exchanges(
        self, *, limit: int | None = None, on_error: ErrorPolicy | None = None
    ) -> FetchQuery:
        """The exchanges and trading venues of US futures.

        Each row holds one venue, such as the Chicago Mercantile Exchange: its id, name, acronym,
        market identifier codes, type, locale, and web site. Massive offers no filter or sort here,
        so the method takes none. The dataset takes no symbols and has no `Symbol` column.

        Args:
            limit: The most rows to return, in total.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.exchanges.md.

        Raises:
            TypeError: If `limit` isn't an `int`, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If `limit` isn't positive.

        Example:
            ::

                venues = m.futures.exchanges().fetch()
        """
        return build_query(
            self._core, EXCHANGES, query_type=FetchQuery, limit=limit, on_error=on_error
        )

    @raises_through(build_query, _product_codes)
    def market_status(
        self,
        *,
        product_code: str | Sequence[str] | Table | None = None,
        limit: int | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """Whether each futures product is trading right now.

        Each row holds one product's current market event, such as `open`, `pause`, or `close`, in
        `MarketEvent`. It also gives the trading venue, the session, and when Massive evaluated it.
        `poll()` keeps it current. Massive's only filter here is the product code, so the method
        takes no `filters`, `sort`, or `order`. The dataset takes no symbols and has no `Symbol`
        column.

        A table of product codes is read value by value, each as if passed alone. A live table is
        followed. In a static table, one failed code fails the call.

        Args:
            product_code: Only this product, such as `"ES"`, or these, such as `["ES", "NQ"]`. A
                table of them in a `ProductCode` column works too. Leave it out for every product.
            limit: The most rows to return, in total. A table of product codes caps every code's
                requests instead.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.market_status.md.

        Raises:
            TypeError: If `product_code` isn't a `str`, a sequence of `str`, or a table, or `limit`
                isn't an `int`, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `product_code` is an empty sequence, or a table without a `ProductCode` column;
                - `limit` isn't positive.

        Example:
            ::

                status = m.futures.market_status(product_code=["ES", "CL"]).poll(30)
        """
        codes, values = _product_codes(product_code)
        return build_query(
            self._core,
            MARKET_STATUS,
            query_type=FetchQuery,
            params=codes,
            limit=limit,
            values=values,
            on_error=on_error,
        )

    @raises_through(build_query, list_params, _product_codes)
    def schedules(
        self,
        *,
        product_code: str | Sequence[str] | Table | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The timed events of each futures product's trading sessions.

        Each row holds one scheduled event of a product's session, such as `pre_open`, `open`, or
        `close`, in `Event`. It also gives the event's time, its session's `SessionEndDate`, and the
        trading venue. The events cover session hours, intraday breaks, and holiday changes. The
        dataset takes no symbols and has no `Symbol` column.

        A table of product codes is read value by value, each as if passed alone. A live table is
        followed. In a static table, one failed code fails the call.

        Args:
            product_code: Only this product, such as `"ES"`, or these, such as `["ES", "NQ"]`. A
                table of them in a `ProductCode` column works too. Leave it out for every product.
            start: The first session, by `SessionEndDate`, as a date.
            end: The last session, as a date. Leave it out to include sessions still to come.
            sort: A field of the rows. Without it, Massive sorts by `product_code`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A table of product codes caps every code's
                requests instead.
            filters: Further endpoint parameters, such as `{"trading_venue": "XCME"}`. Don't pass
                `session_end_date`, which `start` and `end` set.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`).

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.schedules.md.

        Raises:
            TypeError: If `sort` isn't a `str`, `order` isn't a `dm.Order`, or `product_code` isn't
                a `str`, a sequence of `str`, or a table, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `end` is before `start`, or a bound is a `datetime` or can't be read;
                - `order` is given without `sort`;
                - `product_code` is empty, or a table without a `ProductCode` column;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                week = m.futures.schedules(
                    product_code="ES", start="2025-12-22", end="2025-12-26"
                ).fetch()
        """
        codes, values = _product_codes(product_code)
        return build_query(
            self._core,
            SCHEDULES,
            query_type=FetchQuery,
            start=start,
            end=end,
            params={**codes, **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
            values=values,
            on_error=on_error,
        )
