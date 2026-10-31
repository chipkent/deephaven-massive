"""The `m.futures` namespace: US futures (CME, CBOT, NYMEX, COMEX).

Datasets: `trades`, `quotes`, and `bars` (streamed over the `/futures` WebSocket cluster, with
REST and flat-file history), `snapshots` (the current state of contracts), and the reference
datasets `contracts`, `products`, `exchanges`, `market_status`, and `schedules`.

Each dataset is one factory method on `_Futures` that validates its arguments through
`build_query` (`_namespaces/query_building.py`) and returns a query; nothing touches the network
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
  `quotes`, and `bars` take `venues`, a set that picks the flat-file prefixes (all four by
  default). Streams always use the `/futures` cluster path, whatever the venues.
- **Bars.** The aggregates endpoint has a `session` unit and no `day` unit, so `bars` accepts
  `Timespan.SESSION` and rejects `Timespan.DAY`.

The reference datasets keep the `FUTURES` asset class, which selects the session-day rule for
their dates. `contracts` describes one contract per row, so its `Symbol` is the row's ticker
and a contracts table feeds the market-data datasets directly; the others describe no single
instrument: they take no symbols and have no `Symbol` column.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, FuturesVenue, NotBars, Timespan
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    Namespace,
    build_query,
    day_argument,
    list_params,
    one_or_any_of,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._enums import Order
    from deephaven_massive._query import HistoryQuery, Query, StreamHistoryQuery
    from deephaven_massive._types import DateLike, FilterValue, FuturesSpan, StreamSpan, Symbols

__all__ = ["DATASETS", "_Futures"]

logger = logging.getLogger(__name__)

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


@raises_through(one_or_any_of)
def _product_codes(product_code: object) -> dict[str, object]:
    """Return the wire parameters that carry a `product_code` argument.

    Every futures path that takes a product code also takes `product_code.any_of`, so one
    request serves several codes; `one_or_any_of` picks the form and checks the argument.

    Args:
        product_code: The argument as passed: one code, a sequence of codes, or None.

    Returns:
        The two wire parameters, `product_code` and `product_code.any_of`, the unset one None.

    Raises:
        TypeError: If `product_code` is neither text, a sequence of text, nor None
            (`one_or_any_of`).
        ValueError: If `product_code` is an empty sequence (`one_or_any_of`).
    """
    return one_or_any_of("product_code", product_code, every="every product")


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
    summary="Quotes of futures contracts: every change of the best bid and offer.",
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
    summary="Snapshots of futures contracts: the latest trade, quote, minute bar, and session.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_SNAPSHOT_PATH,
    keys={(_REST, _SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    # The contract's last quote and trade times: an explicit APPEND poll appends each new
    # state of a contract. The snapshot carries no single update time of its own.
    revision_keys=("LastQuoteLastUpdated", "LastTradeLastUpdated"),
)

CONTRACTS: Final = DatasetSpec(
    name="futures.contracts",
    summary="Futures contracts: the specification of every listed contract.",
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
    summary="Futures products: the specification of every product, the family of its contracts.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_PRODUCTS_PATH,
    poll_keys=("ProductCode", "TradingVenue", "Date"),
)

EXCHANGES: Final = DatasetSpec(
    name="futures.exchanges",
    summary="Futures exchanges: the exchanges and trading venues of US futures.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_EXCHANGES_PATH,
    poll_keys=("Id",),
)

MARKET_STATUS: Final = DatasetSpec(
    name="futures.market_status",
    summary="Futures market status: whether each futures product is trading right now.",
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
    summary="Futures schedules: the timed events of each product's trading sessions.",
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


class _Futures(Namespace):
    """`m.futures`: the US futures (CME, CBOT, NYMEX, COMEX) datasets of a session.

    Created once by `MassiveSession` and reached as `m.futures`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.

    Futures symbols are contract tickers, such as `"ESZ5"` (the December 2025 E-mini S&P 500)
    or `"GCJ5"` (April 2025 gold). Every streamed dataset uses the `/futures` WebSocket cluster
    path; `venues` only picks the exchanges whose flat files `history()` reads.

    A contract trades only until its last trade date, so a live call needs a current one:
    `contracts()` lists them. The tickers in the examples of this namespace's methods have
    expired; their live calls need a current contract in their place, while their `history()`
    calls read past dates and work as written.

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
        self, symbols: Symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)
    ) -> StreamHistoryQuery:
        """Trades of futures contracts, tick by tick.

        Each row is one trade: price, size, and the exchange's sequence numbers. `Date` is the
        trading session (README.md#time): Massive's `session_end_date`, or on the WebSocket, which
        sends none, the session ending at the next 17:00 Chicago time, corrected by Massive's
        futures schedules (a row they cannot place gets a null `Date`, with a WARNING in the
        log, since the rule can be wrong around a holiday; its other columns are kept). A date
        bound of `history()` is a whole session. There is no `fetch()`: `snapshots(symbols)`
        gives each contract's latest trade. REST `history()` takes explicit contracts; `dm.ALL`
        streams every contract and reads the flat files.

        Args:
            symbols: Contract tickers such as `"ESZ5"`, a contracts table, or `dm.ALL`. See
                README.md#symbols.
            venues: The exchanges whose flat files `history()` reads. See README.md#venues.

        Returns:
            A query with the stream verbs and `history()`. Columns: docs/datasets/futures.trades.md.

        Raises:
            TypeError: If `symbols` is of another type or `venues` is not a set of
                `dm.FuturesVenue`.
            ValueError: If `venues` is empty, `symbols` is None or empty, a ticker is not Massive's
                exact ticker, or a table has no `Symbol` column.

        Example:
            ::

                past = m.futures.trades("ESZ5").history("2025-10-01", "2025-10-03")
        """
        return cast(
            "StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols, venues=venues)
        )

    @raises_through(build_query)
    def quotes(
        self, symbols: Symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)
    ) -> StreamHistoryQuery:
        """Quotes of futures contracts: every change of the best bid and offer.

        Each row is one top-of-book update: bid and ask prices, sizes, and the time of each side.
        `Date` is the trading session, as for `trades()`, and a date bound of `history()` is a whole
        session. There is no `fetch()`: `snapshots(symbols)` gives each contract's latest quote.
        REST `history()` takes explicit contracts; `dm.ALL` streams every contract and reads the
        flat files.

        Args:
            symbols: Contract tickers such as `"ESZ5"`, a contracts table, or `dm.ALL`. See
                README.md#symbols.
            venues: The exchanges whose flat files `history()` reads. See README.md#venues.

        Returns:
            A query with the stream verbs and `history()`. Columns: docs/datasets/futures.quotes.md.

        Raises:
            TypeError: If `symbols` is of another type or `venues` is not a set of
                `dm.FuturesVenue`.
            ValueError: If `venues` is empty, `symbols` is None or empty, a ticker is not Massive's
                exact ticker, or a table has no `Symbol` column.

        Example:
            ::

                active = m.futures.contracts(product_code="CL", filters={"active": True}).fetch()
                book = m.futures.quotes(active).last_by()
        """
        return cast(
            "StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols, venues=venues)
        )

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan,
        *,
        multiplier: Literal[1] = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: FuturesSpan,
        *,
        multiplier: int = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> HistoryQuery: ...
    @raises_through(build_query)
    def bars(
        self,
        symbols: Symbols,
        timespan: FuturesSpan,
        *,
        multiplier: int = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> HistoryQuery:
        """Open, high, low, close, and volume bars of futures contracts.

        Each row is one bar of `multiplier` times `timespan`. A futures trading day is a session
        ending at 17:00 Chicago time, so daily bars are `Timespan.SESSION`: there is no `DAY` and no
        `previous_day_bar()`. One-second and one-minute bars also stream; every other length is
        history only. A session bar ends at 17:00 Chicago time on its session date; a bar of several
        sessions or a week or longer ends as late as either reading of its label and holidays allow,
        and `history()` keeps it only when it fits the range under both (README.md#bars). REST
        rejects a multiplier above its unit's maximum (`60` minutes; use one hour) with
        `MassiveError`.

        Args:
            symbols: Contract tickers, a contracts table, or `dm.ALL`. See README.md#symbols.
            timespan: Any `dm.Timespan` but `DAY`; required. See README.md#timespan-and-multiplier.
            multiplier: Units per bar; only 1 streams.
            venues: The exchanges whose flat files `history()` reads. See README.md#venues.

        Returns:
            A query of the class the timespan picks. Columns: docs/datasets/futures.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `DAY` (the message names `SESSION`), `venues` is empty,
                `multiplier` is below 1, `symbols` is None or empty, a ticker is not Massive's exact
                ticker, or a table has no `Symbol` column.

        Example:
            ::

                sessions = m.futures.bars("ESZ5", dm.Timespan.SESSION).history(
                    "2025-10-01", "2025-10-31"
                )
        """
        query = build_query(
            self._core,
            BARS,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            venues=venues,
        )
        return cast("HistoryQuery", query)

    @raises_through(build_query, list_params, _product_codes)
    def snapshots(
        self,
        symbols: Symbols,
        *,
        product_code: str | Sequence[str] | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Snapshots of futures contracts: the latest trade, quote, minute bar, and session.

        Each row is one contract's current state: its last trade, quote, and minute bar, and the
        open, high, low, close, volume, and settlement of its current session. It is the one-request
        way to read the current state of a set of contracts; `dm.ALL` with `product_code` reads
        every contract of those products.

        Args:
            symbols: Contract tickers, a contracts table, or `dm.ALL`. See README.md#symbols.
            product_code: Only contracts of this product (`"ES"`) or these (`["ES", "NQ"]`); None
                for every product.
            sort: Such as `"ticker"`. See README.md#sort-order-limit.
            order: Needs `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Such as `{"ticker_gte": "ES"}`. See README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.snapshots.md.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or `product_code` is
                neither a `str` nor a sequence of `str`.
            ValueError: If the symbols are invalid (None among them), a filter key or value does not
                fit the endpoint, `order` is given without `sort`, or `product_code` is empty.

        Example:
            ::

                es_curve = m.futures.snapshots(dm.ALL, product_code="ES").poll(5)
        """
        query = build_query(
            self._core,
            SNAPSHOTS,
            symbols=symbols,
            params={**_product_codes(product_code), **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query, list_params, _product_codes, day_argument)
    def contracts(
        self,
        symbols: Symbols | None = None,
        *,
        product_code: str | Sequence[str] | None = None,
        as_of: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Futures contracts: the specification of every listed contract.

        Each row is one contract as of a date: its product, trading venue, first and last trade
        dates, settlement date, tick sizes, and order-size limits. Its `Symbol` is the contract's
        ticker, so the table, or part of it, passes straight to `trades()`, `quotes()`, `bars()`, or
        `snapshots()`. Explicit symbols go 250 to a request.

        Args:
            symbols: Only these contracts; None, the default, lists every contract. `dm.ALL` is
                refused. See README.md#symbols.
            product_code: Only contracts of this product (`"ES"`) or these (`["ES", "NQ"]`); None
                for every product.
            as_of: The day whose specifications to list; None for today. See README.md#as-of.
            sort: Such as `"ticker"`; the endpoint sorts by `product_code`. See
                README.md#sort-order-limit.
            order: Needs `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Such as `{"active": True}` or `{"last_trade_date_gte": "2026-01-01"}`. See
                README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.contracts.md.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, `product_code` is
                neither a `str` nor a sequence of `str`, or `symbols` or `as_of` is of another type.
            ValueError: If `symbols` is `dm.ALL` or holds an invalid ticker, `as_of` is a `datetime`
                or not a date, a filter key or value does not fit the endpoint (`ticker_any_of` is
                set through `symbols`), `order` is given without `sort`, or `product_code` is empty.

        Example:
            ::

                active = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
                es = m.futures.trades(active).last_by()
        """
        params = {
            **_product_codes(product_code),
            "date": None if as_of is None else day_argument(CONTRACTS, as_of, "as_of"),
            **list_params(sort=sort, order=order),
        }
        query = build_query(
            self._core,
            CONTRACTS,
            symbols=symbols,
            symbols_optional=True,
            all_alternative="leave symbols out for every contract",
            params=params,
            filters=filters,
            arguments=_REFERENCE_ARGUMENTS,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query, list_params, _product_codes, day_argument)
    def products(
        self,
        *,
        product_code: str | Sequence[str] | None = None,
        as_of: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Futures products: the specification of every product, the family of its contracts.

        Each row is one product as of a date, such as `ES` (E-mini S&P 500): its name, trading and
        clearing venues, sector and asset class, settlement method and currency, price quotation,
        and unit of measure. It takes no symbols and has no `Symbol` column.

        Args:
            product_code: Only this product (`"ES"`) or these (`["ES", "NQ"]`); None for every
                product.
            as_of: The day whose specifications to list; None for today. See README.md#as-of.
            sort: Such as `"name"`; the endpoint sorts by `date`. See README.md#sort-order-limit.
            order: Needs `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Such as `{"sector": "precious"}` or `{"trading_venue": "XCME"}`. See
                README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.products.md.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, `product_code` is
                neither a `str` nor a sequence of `str`, or `as_of` is of another type.
            ValueError: If `as_of` is a `datetime` or not a date, a filter key or value does not fit
                the endpoint, `order` is given without `sort`, or `product_code` is empty.

        Example:
            ::

                energy = m.futures.products(filters={"asset_sub_class": "energy"}).fetch()
        """
        params = {
            **_product_codes(product_code),
            "date": None if as_of is None else day_argument(PRODUCTS, as_of, "as_of"),
            **list_params(sort=sort, order=order),
        }
        query = build_query(
            self._core,
            PRODUCTS,
            params=params,
            filters=filters,
            arguments=_REFERENCE_ARGUMENTS,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query)
    def exchanges(self, *, limit: int | None = None) -> Query:
        """Futures exchanges: the exchanges and trading venues of US futures.

        Each row is one venue, such as the Chicago Mercantile Exchange: its id, name, acronym,
        market identifier codes, type, locale, and web site. The endpoint has no filter or sort, so
        neither has this method. It takes no symbols and has no `Symbol` column.

        Args:
            limit: The most rows to return, in total. See README.md#sort-order-limit.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.exchanges.md.

        Raises:
            TypeError: If `limit` is not an `int`.
            ValueError: If `limit` is not positive.

        Example:
            ::

                venues = m.futures.exchanges().fetch()
        """
        return cast("Query", build_query(self._core, EXCHANGES, limit=limit))

    @raises_through(build_query, _product_codes)
    def market_status(
        self,
        *,
        product_code: str | Sequence[str] | None = None,
        limit: int | None = None,
    ) -> Query:
        """Futures market status: whether each futures product is trading right now.

        Each row is one product's current market event (`MarketEvent`, such as `open`, `pause`, or
        `close`), its trading venue, its session, and when the status was evaluated; `poll()` keeps
        it current. The endpoint's only filter is the product code and it has no sort, so the method
        takes no `filters`, `sort`, or `order`. It takes no symbols and has no `Symbol` column.

        Args:
            product_code: Only this product (`"ES"`) or these (`["ES", "NQ"]`); None for every
                product.
            limit: The most rows to return, in total. See README.md#sort-order-limit.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.market_status.md.

        Raises:
            TypeError: If `product_code` is not a `str` or a sequence of `str`, or `limit` is not an
                `int`.
            ValueError: If `product_code` is an empty sequence, or `limit` is not positive.

        Example:
            ::

                status = m.futures.market_status(product_code=["ES", "CL"]).poll(30)
        """
        query = build_query(
            self._core, MARKET_STATUS, params=_product_codes(product_code), limit=limit
        )
        return cast("Query", query)

    @raises_through(build_query, list_params, _product_codes)
    def schedules(
        self,
        *,
        product_code: str | Sequence[str] | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Futures schedules: the timed events of each product's trading sessions.

        Each row is one scheduled event of one product's session (`Event`, such as `pre_open`,
        `open`, or `close`) at its instant, with its session (`SessionEndDate`) and trading venue:
        session hours, intraday breaks, and holiday changes. It takes no symbols and has no `Symbol`
        column.

        Args:
            product_code: Only this product (`"ES"`) or these (`["ES", "NQ"]`); None for every
                product.
            start: The first session, by `SessionEndDate`, inclusive; a calendar date. See
                README.md#start-and-end.
            end: The last session, inclusive; None also returns future sessions.
            sort: The endpoint sorts by `product_code`. See README.md#sort-order-limit.
            order: Needs `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Such as `{"trading_venue": "XCME"}`; never `session_end_date`, which `start`
                and `end` set. See README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/futures.schedules.md.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or `product_code` is
                neither a `str` nor a sequence of `str`.
            ValueError: If `end` is before `start`, a bound is a `datetime` or cannot be read, a
                filter key or value does not fit the endpoint, `order` is given without `sort`, or
                `product_code` is empty.

        Example:
            ::

                week = m.futures.schedules(
                    product_code="ES", start="2025-12-22", end="2025-12-26"
                ).fetch()
        """
        query = build_query(
            self._core,
            SCHEDULES,
            start=start,
            end=end,
            params={**_product_codes(product_code), **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
        )
        return cast("Query", query)
