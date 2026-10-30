"""The `m.futures` namespace: US futures (CME, CBOT, NYMEX, COMEX).

Datasets: `trades`, `quotes`, and `bars` (streamed over the `/futures` WebSocket cluster, with
REST and flat-file history), `snapshots` (the current state of contracts), and the reference
datasets `contracts`, `products`, `exchanges`, `market_status`, and `schedules`.

Each dataset is one factory method on `_Futures` that validates its arguments through
`build_query` (`_namespaces/base.py`) and returns a query; nothing touches the network until a
verb is called on the query. This module owns the namespace's `DatasetSpec`s, exported as
`DATASETS` and composed into the dataset catalog (`CATALOG` in `_session.py`). A spec names the
generated sources of its dataset and the key columns of each source; the columns, their types,
and their units come from the generated schemas.

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
their dates, but describe no single instrument: they take no symbols and have no `Symbol`
column.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, FuturesVenue, NotBars, Timespan
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import Namespace, build_query, list_params, one_or_any_of

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
    page_size=50_000,
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
    page_size=50_000,
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
    page_size=50_000,
)

SNAPSHOTS: Final = DatasetSpec(
    name="futures.snapshots",
    summary="Snapshots of futures contracts: the latest trade, quote, minute bar, and session.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_SNAPSHOT_PATH,
    keys={(_REST, _SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    page_size=50_000,
    # The contract's last quote and trade times: an explicit APPEND poll appends each new
    # state of a contract. The snapshot carries no single update time of its own.
    revision_keys=("LastQuoteLastUpdated", "LastTradeLastUpdated"),
)

CONTRACTS: Final = DatasetSpec(
    name="futures.contracts",
    summary="Futures contracts: the specification of every listed contract.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_CONTRACTS_PATH,
    poll_keys=("Ticker", "Date"),
    page_size=1_000,
)

PRODUCTS: Final = DatasetSpec(
    name="futures.products",
    summary="Futures products: the specification of every product, the family of its contracts.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_PRODUCTS_PATH,
    poll_keys=("ProductCode", "TradingVenue", "Date"),
    page_size=50_000,
)

EXCHANGES: Final = DatasetSpec(
    name="futures.exchanges",
    summary="Futures exchanges: the exchanges and trading venues of US futures.",
    asset_class=AssetClass.FUTURES,
    fetch_path=_EXCHANGES_PATH,
    poll_keys=("Id",),
    page_size=1_000,
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
    page_size=100,
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
    page_size=1_000,
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

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            es = m.futures.trades("ESZ5").last_by()  # the latest trade, live
            daily = m.futures.bars("ESZ5", dm.Timespan.SESSION).history("2025-01-02", "2025-03-31")
    """

    __slots__ = ()
    _path: ClassVar[str] = "futures"

    @raises_through(build_query)
    def trades(
        self, symbols: Symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)
    ) -> StreamHistoryQuery:
        """Trades of futures contracts, tick by tick.

        Each row is one trade: its price, its size, and the exchange's sequence numbers. Each
        row also carries its time, `Timestamp`, its session, `Date` (ISO `yyyy-MM-dd` text:
        Massive's `session_end_date`; on the WebSocket, which sends none, the session that ends
        at the next 17:00 Chicago time, a weekend's moving to Monday, corrected by Massive's
        futures schedules, a row they cannot place keeping that date and showing the miss in its
        stream's `m.streams()` row), and `TimestampInsert`, when the library received it. Live
        trades stream over the WebSocket `T` channel of the `/futures` cluster. `history()`
        reads the REST endpoint `/futures/v1/trades/{ticker}`, one contract per request, or the
        flat files `us_futures_<venue>/trades_v1` of each venue of `venues`, all four by default.
        A date bound of `history()` is a whole trading session, the session whose `Date` is that
        date, requested over REST one session at a time (`session_end_date=<date>`, weekdays
        only, whatever hours a holiday added to it). Datetime bounds are exact instants on
        `Timestamp`. Futures have no "last trade" endpoint, so there is no `fetch()`;
        `m.futures.snapshots(symbols).fetch()` gives the latest trade of each contract in one
        request.

        `dm.ALL` streams every contract and reads every contract from flat files; REST history
        needs explicit symbols. There is no `limit`: `history()` pages internally.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#trades,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            symbols: The contracts: one ticker such as `"ESZ5"`, a sequence of tickers, a
                Deephaven table with a `Symbol` column (streams follow its rows as they
                change), or `dm.ALL`.
            venues: The exchanges whose flat files `history()` reads, a set of
                `dm.FuturesVenue`, such as `{dm.FuturesVenue.CME}`; every venue (CME, CBOT,
                NYMEX, COMEX) by default. It does not affect the stream.

        Returns:
            A query with the stream verbs (`blink()`, `append_only()`, `ring()`,
            `last_by()`) and `history()`.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`,
                or `venues` is not a set of `dm.FuturesVenue`.
            ValueError: If `venues` is empty, or for invalid symbols: none at all (for example
                from an exhausted generator), None, a ticker that is not Massive's exact
                ticker, or a table without a `Symbol` column.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                live = m.futures.trades(["ESZ5", "NQZ5"]).append_only()
                past = m.futures.trades("ESZ5", venues={dm.FuturesVenue.CME}).history(
                    "2025-10-01", "2025-10-03"
                )
        """
        return cast(
            "StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols, venues=venues)
        )

    @raises_through(build_query)
    def quotes(
        self, symbols: Symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)
    ) -> StreamHistoryQuery:
        """Quotes of futures contracts: every change of the best bid and offer.

        Each row is one top-of-book update: bid and ask prices, sizes, and the time of each
        side. Each row also carries its time, `Timestamp`, its session, `Date` (ISO `yyyy-MM-dd`
        text: Massive's `session_end_date`; on the WebSocket, which sends none, the session that
        ends at the next 17:00 Chicago time, a weekend's moving to Monday, corrected by
        Massive's futures schedules, a row they cannot place keeping that date and showing the
        miss in its stream's `m.streams()` row), and `TimestampInsert`, when the library
        received it. Live quotes stream over the WebSocket `Q` channel of the `/futures`
        cluster. `history()` reads the REST endpoint `/futures/v1/quotes/{ticker}`, one contract
        per request, or the flat files `us_futures_<venue>/quotes_v1` of each venue of `venues`,
        all four by default. A date bound of `history()` is a whole trading session,
        requested over REST one session at a time as for `trades`. Datetime bounds are exact
        instants on `Timestamp`. There is no `fetch()`: `m.futures.snapshots(symbols).fetch()`
        gives the latest quote of each contract in one request.

        `dm.ALL` streams every contract and reads every contract from flat files; REST history
        needs explicit symbols. There is no `limit`: `history()` pages internally.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#quotes,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            symbols: The contracts: one ticker such as `"ESZ5"`, a sequence of tickers, a
                Deephaven table with a `Symbol` column (streams follow its rows as they
                change), or `dm.ALL`.
            venues: The exchanges whose flat files `history()` reads, a set of
                `dm.FuturesVenue`, such as `{dm.FuturesVenue.CME}`; every venue (CME, CBOT,
                NYMEX, COMEX) by default. It does not affect the stream.

        Returns:
            A query with the stream verbs (`blink()`, `append_only()`, `ring()`,
            `last_by()`) and `history()`.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`,
                or `venues` is not a set of `dm.FuturesVenue`.
            ValueError: If `venues` is empty, or for invalid symbols: none at all (for example
                from an exhausted generator), None, a ticker that is not Massive's exact
                ticker, or a table without a `Symbol` column.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                book = m.futures.quotes("CLZ5").last_by()  # the current top of book, live
        """
        return cast(
            "StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols, venues=venues)
        )

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan = Timespan.MINUTE,
        *,
        multiplier: Literal[1] = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: FuturesSpan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> HistoryQuery: ...
    @raises_through(build_query)
    def bars(
        self,
        symbols: Symbols,
        timespan: FuturesSpan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue),
    ) -> HistoryQuery:
        """Open, high, low, close, and volume bars of futures contracts.

        Each row is one bar spanning `multiplier` units of `timespan`: its window, from
        `TimestampStart` (Massive's window start) to the exclusive `TimestampEnd`, its session
        `Date` (ISO `yyyy-MM-dd` text: Massive's `session_end_date`, or on the WebSocket the
        session as for `trades`), and `TimestampInsert`, when the library received it.
        `TimestampEnd` is the first instant at which the bar is complete, so join on it
        (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only closed bars:
        an intraday bar ends its length after its start (a one-second WebSocket bar at the
        later of that and Massive's `e`); a session bar at 17:00 Chicago time on its session
        date. Massive does not document whether a bar of several sessions, or of a week or
        longer, is labelled with its first session or its last, so such a bar ends at the
        later of the two readings: 17:00 Chicago time on the last weekday its label allows,
        plus four days for a holiday that moves a session for a bar of several sessions.
        `Date` is null for a bar of several sessions, a week or longer, or an intraday bar that
        crosses 17:00 Chicago time. Futures have no corporate actions, so their bars are never
        adjusted. The timespan decides the transports, and so the query class:

        - `Timespan.SECOND` or `Timespan.MINUTE` with `multiplier=1` stream over the
          WebSocket `A` (per second) or `AM` (per minute) channel of the `/futures` cluster,
          and also have `history()`: a `StreamHistoryQuery`.
        - Every other timespan and multiplier is history only: a `HistoryQuery`.

        `history()` reads the REST endpoint `/futures/v1/aggs/{ticker}`, one contract per
        request, with `resolution` sent as `<multiplier><unit>` (`5min`, `1session`); the
        server rejects a multiplier above its unit's maximum (for example `60min`; use
        `1hour`) with HTTP 400, raised as `MassiveError`. 1-minute and 1-session bars can also
        come from the flat files `us_futures_<venue>/minute_aggs_v1` and
        `us_futures_<venue>/session_aggs_v1`. A futures trading day is a session ending at
        17:00 America/Chicago, so daily bars are `Timespan.SESSION`; there is no
        `Timespan.DAY` and no previous-session `fetch()`.

        A date bound of `history()` is a whole trading session. Bars are requested from four
        days before the first session's regular open (a holiday can open a session early) to
        18:00 Chicago time on the last date, and kept by their session label: an intraday bar
        when its session is in the range and it has ended, a bar of a session or a longer period
        (`WEEK`, Monday to Friday, to `YEAR`) when every session of its period is in the range
        and has ended. A bar of several sessions or periods is kept only when it fits the range
        under both readings of its label, as its first session's and as its last's. Every bar
        must also have ended, by its `TimestampEnd`, by 17:00 Chicago time on the last date and
        by now. A bar of several sessions ends as late as holidays could push its sessions (one
        weekday more, and one more for every five further sessions), so
        `bars(..., Timespan.SESSION, multiplier=2).history("2025-02-03", "2025-02-07")` keeps
        the bars opening Monday and Tuesday evening: the one opening Wednesday evening could
        end on the next Monday. Datetimes keep the bars whose whole window, from Massive's
        window start to `TimestampEnd`, lies in the range.

        `dm.ALL` streams every contract and reads every contract from flat files; REST history
        needs explicit symbols. There is no `limit`: `history()` pages internally.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#bars,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            symbols: The contracts: one ticker such as `"ESZ5"`, a sequence of tickers, a
                Deephaven table with a `Symbol` column, or `dm.ALL`.
            timespan: The bar unit, a `dm.Timespan` other than `DAY`: `SECOND`, `MINUTE`
                (the default), `HOUR`, `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
            multiplier: How many units one bar spans; a positive `int`, 1 by default.
            venues: The exchanges whose flat files `history()` reads, a set of
                `dm.FuturesVenue`, such as `{dm.FuturesVenue.CME}`; every venue (CME, CBOT,
                NYMEX, COMEX) by default. It does not affect the stream.

        Returns:
            A `StreamHistoryQuery` for 1-second and 1-minute bars, otherwise a `HistoryQuery`.

        Raises:
            TypeError: If `timespan` is not a `dm.Timespan`, `venues` is not a set of
                `dm.FuturesVenue`, `multiplier` is not an `int`, or `symbols` is not a `str`, a
                sequence of `str`, a table, or `dm.ALL`.
            ValueError: If `timespan` is `Timespan.DAY` (the message names
                `Timespan.SESSION`), `venues` is empty, `multiplier` is not positive, or the
                symbols are invalid: none at all, None, a ticker that is not Massive's exact
                ticker, or a table without a `Symbol` column.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                minute = m.futures.bars("ESZ5").ring(390)  # the latest 390 1-minute bars, live
                five = m.futures.bars("ESZ5", dm.Timespan.MINUTE, multiplier=5).history(
                    "2025-10-01", "2025-10-03"
                )
                sessions = m.futures.bars(
                    dm.ALL, dm.Timespan.SESSION, venues={dm.FuturesVenue.CME}
                ).history("2025-01-02", "2025-03-31")
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

        Each row is one contract's current state from the REST endpoint
        `/futures/v1/snapshot`: its last trade, its last quote, its last minute bar, and the
        open, high, low, close, volume, and settlement of its current session. Explicit
        symbols are sent together as `ticker.any_of`; `dm.ALL` asks for every contract, all
        pages of it, which with `product_code` is every contract of the products. `fetch()`
        reads it once, `poll()` re-reads it on an interval; it is the one-request way to get
        the current state of a set of contracts.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#snapshots,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            symbols: The contracts: one ticker such as `"ESZ5"`, a sequence of tickers, a
                Deephaven table with a `Symbol` column (`poll()` re-reads it each cycle), or
                `dm.ALL`.
            product_code: Only contracts of this product, such as `"ES"` (sent as
                `product_code`), or of these products, such as `["ES", "NQ"]` (sent as
                `product_code.any_of`); None for every product.
            sort: The field to sort by, such as `"ticker"`; None for the endpoint's default.
            order: The sort direction, a `dm.Order`, sent as the `.asc` or `.desc` suffix of
                `sort`; it needs `sort`. None for the endpoint's default direction.
            limit: The most rows to return in total; None for every row.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"ticker_gte": "ES"}` for an alphabetical range
                of tickers. The `product_code` forms are set through `product_code`.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or
                `product_code` is neither a `str` nor a sequence of `str`.
            ValueError: For invalid symbols (None among them), an unknown filter, a filter
                value of the wrong kind, an `order` without `sort`, or an empty `product_code`
                sequence.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                now = m.futures.snapshots(["ESZ5", "NQZ5"]).fetch()
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

    @raises_through(build_query, list_params, _product_codes)
    def contracts(
        self,
        *,
        product_code: str | Sequence[str] | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Futures contracts: the specification of every listed contract.

        Each row is one contract as of a date (`Date`): its ticker, product, trading venue,
        first and last trade dates, settlement date, tick sizes, and order-size limits, from
        the REST endpoint `/futures/v1/contracts`. Use it to find the tickers of a product's
        contracts, for example the active `ES` contracts. The dataset takes no symbols and has
        no `Symbol` column: the contract's ticker is the `Ticker` column, and the
        `ticker_any_of` filter selects contracts.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#contracts,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            product_code: Only contracts of this product, such as `"ES"` (sent as
                `product_code`), or of these products, such as `["ES", "NQ"]` (sent as
                `product_code.any_of`); None for every product.
            sort: The field to sort by, such as `"ticker"`; None for the endpoint's default
                (`product_code`).
            order: The sort direction, a `dm.Order`, sent as the `.asc` or `.desc` suffix of
                `sort`; it needs `sort`. None for the endpoint's default direction.
            limit: The most rows to return in total; None for every row.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"active": True}` or
                `{"last_trade_date_gte": "2026-01-01"}`.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or
                `product_code` is neither a `str` nor a sequence of `str`.
            ValueError: For an unknown filter, a filter value of the wrong kind, an `order`
                without `sort`, or an empty `product_code` sequence.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                es = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
        """
        query = build_query(
            self._core,
            CONTRACTS,
            params={**_product_codes(product_code), **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query, list_params, _product_codes)
    def products(
        self,
        *,
        product_code: str | Sequence[str] | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Futures products: the specification of every product, the family of its contracts.

        Each row is one product as of a date (`Date`), such as `ES` (E-mini S&P 500): its
        name, trading and clearing venues, sector and asset class, settlement method and
        currency, price quotation, and unit of measure, from the REST endpoint
        `/futures/v1/products`. The dataset takes no symbols and has no `Symbol` column.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#products,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            product_code: Only this product, such as `"ES"` (sent as `product_code`), or
                these products, such as `["ES", "NQ"]` (sent as `product_code.any_of`);
                None for every product.
            sort: The field to sort by, such as `"name"`; None for the endpoint's default
                (`date`).
            order: The sort direction, a `dm.Order`, sent as the `.asc` or `.desc` suffix of
                `sort`; it needs `sort`. None for the endpoint's default direction.
            limit: The most rows to return in total; None for every row.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"sector": "precious"}` or
                `{"trading_venue": "XCME"}`.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or
                `product_code` is neither a `str` nor a sequence of `str`.
            ValueError: For an unknown filter, a filter value of the wrong kind (a `sector`
                outside the endpoint's values, for example), an `order` without `sort`, or an
                empty `product_code` sequence.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
                energy = m.futures.products(filters={"asset_sub_class": "energy"}).fetch()
        """
        query = build_query(
            self._core,
            PRODUCTS,
            params={**_product_codes(product_code), **list_params(sort=sort, order=order)},
            filters=filters,
            arguments=_PRODUCT_CODE_ARGUMENT,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query)
    def exchanges(self, *, limit: int | None = None) -> Query:
        """Futures exchanges: the exchanges and trading venues of US futures.

        Each row is one venue, such as the Chicago Mercantile Exchange: its id, name, acronym,
        market identifier codes (MIC and operating MIC), type, locale, and web site, from the
        REST endpoint `/futures/v1/exchanges`. The endpoint has no filter or sort parameter,
        so the method takes no `filters`, `sort`, or `order`. The dataset takes no symbols and
        has no `Symbol` column.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#exchanges,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            limit: The most rows to return in total; None for every row.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `limit` is not an `int`.
            ValueError: If `limit` is not positive.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
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

        Each row is one product's current market event (`MarketEvent`, such as `open`,
        `pause`, or `close`), its trading venue, the session it belongs to, and the time the
        status was evaluated, from the REST endpoint `/futures/v1/market-status`. `poll()`
        keeps the table current. The endpoint's only filter is the product code, which
        `product_code` sets, and it has no sort parameter, so the method takes no `filters`,
        `sort`, or `order`. The dataset takes no symbols and has no `Symbol` column.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#market_status,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            product_code: Only this product, such as `"ES"` (sent as `product_code`), or
                these products, such as `["ES", "NQ"]` (sent as `product_code.any_of`);
                None for every product.
            limit: The most rows to return in total; None for every row.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `product_code` is not a `str` or a sequence of `str`, or `limit` is
                not an `int`.
            ValueError: If `product_code` is an empty sequence, or `limit` is not positive.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
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
        `open`, or `close`), at its instant (`Timestamp`), with the session it belongs to
        (`SessionEndDate`, ISO `yyyy-MM-dd` text) and the trading venue, from the REST endpoint
        `/futures/v1/schedules`. It shows session hours, intraday breaks, and holiday
        adjustments. The dataset takes no symbols and has no `Symbol` column.

        `start` and `end` select sessions by their `SessionEndDate`, both inclusive (sent as
        `session_end_date.gte` and `session_end_date.lte`); None leaves that side open. Every
        other form of `session_end_date` is set through them, never through `filters`.

        `limit` caps the total number of rows across pages; the page size is internal.

        The columns are described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/futures.md#schedules,
        in the web UI's column-header tooltips, and by `columns()` on the query.

        Args:
            product_code: Only this product, such as `"ES"` (sent as `product_code`), or
                these products, such as `["ES", "NQ"]` (sent as `product_code.any_of`);
                None for every product.
            start: The first session, as an ISO date string or a `date` (a `datetime` raises
                `ValueError`); None for no lower bound.
            end: The last session, in the same forms. None (the default) sends no upper bound, so
                rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by; None for the endpoint's default (`product_code`).
            order: The sort direction, a `dm.Order`, sent as the `.asc` or `.desc` suffix of
                `sort`; it needs `sort`. None for the endpoint's default direction.
            limit: The most rows to return in total; None for every row.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"trading_venue": "XCME"}`.

        Returns:
            A query with `fetch()` and `poll()`.

        Raises:
            TypeError: If `sort` is not a `str`, `order` is not a `dm.Order`, or
                `product_code` is neither a `str` nor a sequence of `str`.
            ValueError: For an `end` before `start`, an unparseable bound, a `datetime` bound
                (`session_end_date` is a calendar date), a `session_end_date` filter, an unknown
                filter, a filter value of the wrong kind, an `order` without `sort`, or an empty
                `product_code` sequence.

        Example:
            ::

                import deephaven_massive as dm

                m = dm.MassiveSession()
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
