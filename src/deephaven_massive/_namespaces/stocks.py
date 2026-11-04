"""The `m.stocks` namespace: US equities.

Datasets: trades, quotes, bars, each symbol's last trade, last quote, and previous day's bar, LULD
bands, net order imbalances, fair market value, snapshots, movers, daily open/close, dividends,
splits, IPOs, short interest and volume, and share float. The nested `m.stocks.financials` and
`m.stocks.filings` namespaces are `stock_financials.py` and `stock_filings.py`, which `_Stocks`
creates.

Each dataset is one factory method that validates its arguments and returns a query; nothing
touches the network until a verb is called on the query. This module owns the namespace's
`DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in
`_session.py`). A spec names the dataset's generated sources, its key columns per source, the
wire parameters behind `start`, `end`, and `symbols`, its `poll()` defaults, and the REST page
size (the largest `limit` the endpoint documents); columns, types, and units come from the
generated schemas.

The REST list datasets (dividends through share float) share one shape: `symbols` is optional and
only filters the request, `dm.ALL` is refused in favour of `symbols=None` (`EVERY_TICKER`), `start`
and `end` bound the dataset's date axis where it has one, and `sort`, `order`, `limit`, and
`filters` work as each dataset's page describes.

The module is one subject although it runs past 1,000 lines: it is one namespace, and each
dataset's spec and its factory method, whose docstring is the dataset's documentation, belong
together.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    NotBars,
    Order,
    PollMode,
    Timespan,
    _require_enum,
)
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    EVERY_TICKER,
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    Namespace,
    adjustment_notes,
    build_query,
    day_argument,
    list_query,
    require_bool,
    ticker_key,
)
from deephaven_massive._namespaces.stock_filings import _StockFilings
from deephaven_massive._namespaces.stock_financials import _StockFinancials

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._query import HistoryQuery, Query, StreamHistoryQuery, StreamQuery
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, StreamSpan, Symbols

__all__ = [
    "DATASETS",
    "_Stocks",
]


_REST: Final = Transport.REST
_WS: Final = Transport.WS
_FLAT_FILES: Final = Transport.FLAT_FILES
_STOCKS: Final = AssetClass.STOCKS


def _row(column: str) -> KeySpec:
    """Return a key the source's rows carry in `column`."""
    return KeySpec(column)


def _request(parameter: str) -> KeySpec:
    """Return a key filled from the request parameter `parameter`."""
    return KeySpec(parameter, from_request=True)


def _stream_bar_keys() -> SourceKeys:
    """Return the keys of a WebSocket bars channel: its ticker, window start, and end `e`."""
    return SourceKeys(symbol=_row("Ticker"), time=_row("WindowStart"), end=_row("EndTimestamp"))


# --- Market data ------------------------------------------------------------------------------

_LAST_TRADE: Final = "/v2/last/trade/{stocksTicker}"
_TRADES: Final = "/v3/trades/{stockTicker}"
_LAST_QUOTE: Final = "/v2/last/nbbo/{stocksTicker}"
_QUOTES: Final = "/v3/quotes/{stockTicker}"
_PREV: Final = "/v2/aggs/ticker/{stocksTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_GROUPED: Final = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
_MINUTE_FILES: Final = "us_stocks_sip/minute_aggs_v1"
_DAY_FILES: Final = "us_stocks_sip/day_aggs_v1"

_AGGREGATES_PAGE: Final = 50_000
"""The aggregates range endpoint's largest `limit`, which counts the base aggregates
behind a page. Its specification states this maximum only in prose, so it is typed here."""

TRADES: Final = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=_STOCKS,
    kind="trades",
    history_path=_TRADES,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (_REST, _TRADES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_WS, "stocks/T"): ticker_key("SipTimestamp"),
        (_FLAT_FILES, "us_stocks_sip/trades_v1"): ticker_key("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
# Each one-row dataset joins the kind of its stream and history dataset, so its rows have the
# same columns; an explicit APPEND poll() records each new row by its time.
LAST_TRADE: Final = DatasetSpec(
    name="stocks.last_trade",
    summary="The latest trade of each US-listed stock.",
    asset_class=_STOCKS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    serves_current_data=True,
    refused_alternative="stocks.snapshots",
    keys={(_REST, _LAST_TRADE): ticker_key("SipTimestamp")},
    per_symbol=True,
    revision_keys=("Timestamp",),
)
QUOTES: Final = DatasetSpec(
    name="stocks.quotes",
    summary="National best bid and offer quotes of US-listed stocks, tick by tick.",
    asset_class=_STOCKS,
    kind="quotes",
    history_path=_QUOTES,
    channels={NotBars.ANY: "Q"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/quotes_v1"},
    keys={
        (_REST, _QUOTES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_WS, "stocks/Q"): ticker_key("SipTimestamp"),
        (_FLAT_FILES, "us_stocks_sip/quotes_v1"): ticker_key("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
LAST_QUOTE: Final = DatasetSpec(
    name="stocks.last_quote",
    summary="The latest national best bid and offer of each US-listed stock.",
    asset_class=_STOCKS,
    kind="quotes",
    fetch_path=_LAST_QUOTE,
    serves_current_data=True,
    refused_alternative="stocks.snapshots",
    keys={(_REST, _LAST_QUOTE): ticker_key("SipTimestamp")},
    per_symbol=True,
    revision_keys=("Timestamp",),
)
_BAR_NOTES: Final = adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",))
"""The split-adjustment notes of the stock bars' price and volume columns."""

BARS: Final = DatasetSpec(
    name="stocks.bars",
    summary="Aggregate bars (open, high, low, close, volume) of US-listed stocks.",
    asset_class=_STOCKS,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    grouped_daily_path=_GROUPED,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_REST, _RANGE): SourceKeys(symbol=_request("stocksTicker"), time=_row("WindowStart")),
        (_REST, _GROUPED): ticker_key("WindowStart"),
        (_WS, "stocks/A"): _stream_bar_keys(),
        (_WS, "stocks/AM"): _stream_bar_keys(),
        (_FLAT_FILES, _MINUTE_FILES): ticker_key("WindowStart"),
        (_FLAT_FILES, _DAY_FILES): ticker_key("WindowStart"),
    },
    page_size=_AGGREGATES_PAGE,
    column_notes=_BAR_NOTES,
)
PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="stocks.previous_day_bar",
    summary="The previous trading day's bar (open, high, low, close, volume) of US stocks.",
    asset_class=_STOCKS,
    kind="bars",
    fetch_path=_PREV,
    keys={(_REST, _PREV): ticker_key("WindowStart")},
    per_symbol=True,
    revision_keys=("TimestampStart",),
    column_notes=_BAR_NOTES,
)
LULD: Final = DatasetSpec(
    name="stocks.luld",
    summary="Limit up-limit down price bands of US-listed stocks, as they change.",
    asset_class=_STOCKS,
    kind="luld",
    channels={NotBars.ANY: "LULD"},
    keys={(_WS, "stocks/LULD"): ticker_key("UnixTimestamp")},
)
IMBALANCES: Final = DatasetSpec(
    name="stocks.imbalances",
    summary="Net order imbalances of US-listed stocks ahead of opening and closing auctions.",
    asset_class=_STOCKS,
    kind="imbalances",
    channels={NotBars.ANY: "NOI"},
    keys={(_WS, "stocks/NOI"): ticker_key("UnixTimestamp")},
)
FMV: Final = DatasetSpec(
    name="stocks.fmv",
    summary="Massive's fair market value estimate of US-listed stocks, as it updates.",
    asset_class=_STOCKS,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    channel_family=ChannelFamily.FMV,
    keys={(_WS, "stocks/FMV"): ticker_key("FmvLastUpdated")},
)

# --- Snapshots and daily summaries -------------------------------------------------------------

_SNAPSHOTS: Final = "/v2/snapshot/locale/us/markets/stocks/tickers"
_MOVERS: Final = "/v2/snapshot/locale/us/markets/stocks/{direction}"
_OPEN_CLOSE: Final = "/v1/open-close/{stocksTicker}/{date}"

# The snapshots declare no time key: a ticker that has not traded can carry `updated` 0,
# which as the declared time column would fail the whole response instead of reading as null.
SNAPSHOTS: Final = DatasetSpec(
    name="stocks.snapshots",
    summary="Current market snapshots of US-listed stocks, one row per ticker.",
    asset_class=_STOCKS,
    fetch_path=_SNAPSHOTS,
    serves_current_data=True,
    keys={(_REST, _SNAPSHOTS): ticker_key()},
    symbol_param="tickers",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    revision_keys=("Updated",),
)
MOVERS: Final = DatasetSpec(
    name="stocks.movers",
    summary="The twenty US stocks with the largest percentage move today, up or down.",
    asset_class=_STOCKS,
    fetch_path=_MOVERS,
    serves_current_data=True,
    keys={(_REST, _MOVERS): ticker_key()},
    revision_keys=("Updated",),
)
DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="stocks.daily_open_close",
    summary="The open, close, and pre- and after-market prices of US stocks on one day.",
    asset_class=_STOCKS,
    fetch_path=_OPEN_CLOSE,
    keys={(_REST, _OPEN_CLOSE): SourceKeys(symbol=_row("Symbol"), date=_row("From"))},
    per_symbol=True,
    row_times=RowTimes.DAY,
    column_notes=adjustment_notes(
        ("Open", "High", "Low", "Close", "PreMarket", "AfterHours"), ("Volume",)
    ),
)

# --- Corporate actions and short data ----------------------------------------------------------

_DIVIDENDS: Final = "/stocks/v1/dividends"
_SPLITS: Final = "/stocks/v1/splits"
_IPOS: Final = "/vX/reference/ipos"
_SHORT_INTEREST: Final = "/stocks/v1/short-interest"
_SHORT_VOLUME: Final = "/stocks/v1/short-volume"
_FLOAT: Final = "/stocks/vX/float"

DIVIDENDS: Final = DatasetSpec(
    name="stocks.dividends",
    summary="Cash dividends of US-listed stocks, one row per distribution.",
    asset_class=_STOCKS,
    fetch_path=_DIVIDENDS,
    keys={(_REST, _DIVIDENDS): ticker_key()},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
)
SPLITS: Final = DatasetSpec(
    name="stocks.splits",
    summary="Stock splits, reverse splits, and stock dividends of US-listed stocks.",
    asset_class=_STOCKS,
    fetch_path=_SPLITS,
    keys={(_REST, _SPLITS): ticker_key()},
    time_axis=("execution_date.gte", "execution_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
)
# The IPO path filters one `ticker` per request, so the REST layer sends one request per symbol.
IPOS: Final = DatasetSpec(
    name="stocks.ipos",
    summary="Initial public offerings on US exchanges, upcoming and past, one row per offering.",
    asset_class=_STOCKS,
    fetch_path=_IPOS,
    keys={(_REST, _IPOS): ticker_key()},
    time_axis=("listing_date.gte", "listing_date.lte"),
    symbol_param="ticker",
    poll_mode=PollMode.UPSERT,
    # One offering is one issuer listing under one ticker; both are required on every row, and
    # a ticker an earlier, different issuer used (a recycled SPAC ticker) stays apart. The key
    # assumes Massive keeps an offering's issuer name unchanged through its statuses, which no
    # vendor source states: a renamed issuer would leave its old row behind. The listing date
    # and ISIN are no key: an offering can be postponed to a new date, and either can be
    # missing before the listing.
    poll_keys=("Symbol", "IssuerName"),
)
SHORT_INTEREST: Final = DatasetSpec(
    name="stocks.short_interest",
    summary="Short interest of US-listed stocks, one row per ticker and FINRA settlement date.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_INTEREST,
    keys={(_REST, _SHORT_INTEREST): ticker_key()},
    time_axis=("settlement_date.gte", "settlement_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "SettlementDate"),
)
SHORT_VOLUME: Final = DatasetSpec(
    name="stocks.short_volume",
    summary="Daily short sale volume of US-listed stocks, one row per ticker and trading day.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_VOLUME,
    keys={(_REST, _SHORT_VOLUME): ticker_key()},
    time_axis=("date.gte", "date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
)
SHARE_FLOAT: Final = DatasetSpec(
    name="stocks.share_float",
    summary="The free float of US-listed stocks: the latest count of shares the public can trade.",
    asset_class=_STOCKS,
    fetch_path=_FLOAT,
    keys={(_REST, _FLOAT): ticker_key()},
    symbol_param="ticker.any_of",
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    LAST_TRADE,
    QUOTES,
    LAST_QUOTE,
    BARS,
    PREVIOUS_DAY_BAR,
    LULD,
    IMBALANCES,
    FMV,
    SNAPSHOTS,
    MOVERS,
    DAILY_OPEN_CLOSE,
    DIVIDENDS,
    SPLITS,
    IPOS,
    SHORT_INTEREST,
    SHORT_VOLUME,
    SHARE_FLOAT,
)
"""Every dataset of `m.stocks` itself, one `DatasetSpec` each (for example `stocks.trades`);
its nested namespaces have their own (`stock_financials.py`, `stock_filings.py`)."""


# --- Namespaces ---------------------------------------------------------------------------------


class _Stocks(Namespace):
    """`m.stocks`: the US equities datasets of a session.

    Created once by `MassiveSession` and reached as `m.stocks`, together with its nested
    `m.stocks.financials` and `m.stocks.filings` namespaces. It holds no mutable state, so it
    is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            live = m.stocks.trades(["AAPL", "MSFT"]).last_by()
            daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-01-02", "2025-06-30")
    """

    __slots__ = ("_filings", "_financials")
    _path: ClassVar[str] = "stocks"

    def __init__(self, core: SessionCore) -> None:
        """Create the namespace and its two nested namespaces.

        Args:
            core: The session's internal core.
        """
        super().__init__(core)
        self._financials = _StockFinancials(core)
        self._filings = _StockFilings(core)

    @property
    def financials(self) -> _StockFinancials:
        """The financial statements and ratios datasets (`m.stocks.financials`)."""
        return self._financials

    @property
    def filings(self) -> _StockFilings:
        """The SEC filings and taxonomy datasets (`m.stocks.filings`)."""
        return self._filings

    @raises_through(build_query)
    def trades(self, symbols: Symbols) -> StreamHistoryQuery:
        """Trades of US-listed stocks, tick by tick.

        Each row is one trade reported to the SIP; `Timestamp` is the SIP time. Trades stream from
        the WebSocket, and `history()` reads REST or the flat files. With `dm.ALL`, streams and
        flat-file history serve every ticker, while REST history refuses it. Each symbol's latest
        trade alone is `last_trade()`.

        Args:
            symbols: Stock tickers, or `dm.ALL`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.trades.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                recent = m.stocks.trades(["AAPL", "MSFT"]).ring(10_000)
                friday = m.stocks.trades("AAPL").history("2025-06-06", "2025-06-06")
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    @raises_through(build_query)
    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each US-listed stock.

        Each row is one symbol's most recent trade, read by REST one request per symbol; a symbol
        with no trade (HTTP 404) is left out, with a WARNING. `poll()` replaces the table each
        cycle, keyed by `Symbol`; `mode=dm.PollMode.APPEND` keeps each new last trade, but a trade
        between two cycles is never seen (`trades()` streams every one). Some plans lack the
        endpoint and raise `EntitlementError`; `snapshots()` also carries each ticker's last trade.

        Args:
            symbols: Stock tickers; `dm.ALL` is refused (use `m.stocks.snapshots(dm.ALL)`).

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.last_trade.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is `dm.ALL`, None, empty, or holds an invalid ticker, or a
                `Table` has no `Symbol` column.

        Example:
            ::

                last = m.stocks.last_trade(["AAPL", "MSFT"]).fetch()
        """
        query = build_query(
            self._core,
            LAST_TRADE,
            symbols=symbols,
            all_alternative="use m.stocks.snapshots(dm.ALL).fetch()",
        )
        return cast("Query", query)

    @raises_through(build_query)
    def quotes(self, symbols: Symbols) -> StreamHistoryQuery:
        """National best bid and offer quotes of US-listed stocks, tick by tick.

        Each row is one NBBO update; `Timestamp` is the SIP time. Quotes stream from the WebSocket,
        and `history()` reads REST or the flat files. With `dm.ALL`, streams and flat-file history
        serve every ticker, while REST history refuses it. Each symbol's latest NBBO alone is
        `last_quote()`.

        Args:
            symbols: Stock tickers, or `dm.ALL`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.quotes.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                top = m.stocks.quotes(["AAPL", "SPY"]).last_by()
        """
        return cast("StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols))

    @raises_through(build_query)
    def last_quote(self, symbols: Symbols) -> Query:
        """The latest national best bid and offer of each US-listed stock.

        Each row is one symbol's most recent NBBO, read by REST one request per symbol; a symbol
        with no quote (HTTP 404) is left out, with a WARNING. `poll()` replaces the table each
        cycle, keyed by `Symbol`; `mode=dm.PollMode.APPEND` keeps each new last quote, but a quote
        between two cycles is never seen (`quotes()` streams every one). Some plans lack the
        endpoint and raise `EntitlementError`; `snapshots()` also carries each ticker's last quote.

        Args:
            symbols: Stock tickers; `dm.ALL` is refused (use `m.stocks.snapshots(dm.ALL)`).

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.last_quote.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is `dm.ALL`, None, empty, or holds an invalid ticker, or a
                `Table` has no `Symbol` column.

        Example:
            ::

                spreads = m.stocks.last_quote(["AAPL", "SPY"]).poll(5)
        """
        query = build_query(
            self._core,
            LAST_QUOTE,
            symbols=symbols,
            all_alternative="use m.stocks.snapshots(dm.ALL).fetch()",
        )
        return cast("Query", query)

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan,
        *,
        multiplier: Literal[1] = 1,
        adjusted: bool = True,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery: ...
    @raises_through(build_query, require_bool)
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed stocks.

        Each row is one bar of `multiplier` times `timespan`. One-second and one-minute bars also
        stream from the WebSocket; every other length is history only. Windows, stamps, `Date`, and
        which bars `history()` returns follow the rules its columns page states. Daily `dm.ALL`
        history reads Massive's grouped daily bars, one request per day. Intraday `dm.ALL` history
        needs `adjusted=False`, since only REST adjusts and its intraday bars need named symbols.
        The previous session's bar alone is `previous_day_bar()`.

        Args:
            symbols: Stock tickers, or `dm.ALL`.
            timespan: The bar unit, any but `SESSION`; required.
            multiplier: Units per bar, 1 by default; only 1 streams.
            adjusted: Split-adjusted prices, served by REST only.

        Returns:
            A query of the class the timespan picks. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, or `symbols` is None,
                empty, or holds an invalid ticker.

        Example:
            ::

                daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
                live = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).append_only()
        """
        query = build_query(
            self._core,
            BARS,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            adjusted=require_bool("adjusted", adjusted),
        )
        return cast("HistoryQuery", query)

    @raises_through(build_query, require_bool)
    def previous_day_bar(self, symbols: Symbols, *, adjusted: bool = True) -> Query:
        """The previous trading day's bar (open, high, low, close, volume) of US stocks.

        Each row is one symbol's 1-day bar of the previous completed session, read by REST one
        request per symbol; a symbol with no bar (HTTP 404) is left out, with a WARNING. `poll()`
        replaces the table each cycle, keyed by `Symbol`; `mode=dm.PollMode.APPEND` keeps each new
        day's bar. Massive stamps the bar at the session close, which is moved to the day's start,
        as its columns page describes; an unexpected stamp is served with a WARNING.

        Args:
            symbols: Stock tickers; `dm.ALL` is refused (use `m.stocks.bars(dm.ALL,
                dm.Timespan.DAY).history(day, day)`).
            adjusted: Split-adjusted prices and volume.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.previous_day_bar.md.

        Raises:
            TypeError: If `adjusted` is not a `bool`, or `symbols` is not an accepted form.
            ValueError: If `symbols` is `dm.ALL`, None, empty, or holds an invalid ticker, or a
                `Table` has no `Symbol` column.

        Example:
            ::

                prev = m.stocks.previous_day_bar(["AAPL", "MSFT"]).fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "use m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
            adjusted=require_bool("adjusted", adjusted),
        )
        return cast("Query", query)

    @raises_through(build_query)
    def luld(self, symbols: Symbols) -> StreamQuery:
        """Limit up-limit down price bands of US-listed stocks, as they change.

        Each row is one band update from the SIP: the upper and lower price limits, the indicators,
        and the tape. The bands only stream from the WebSocket; there is no history.

        Args:
            symbols: Stock tickers, or `dm.ALL`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.luld.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                bands = m.stocks.luld(dm.ALL).last_by()
        """
        return cast("StreamQuery", build_query(self._core, LULD, symbols=symbols))

    @raises_through(build_query)
    def imbalances(self, symbols: Symbols) -> StreamQuery:
        """Net order imbalances of US-listed stocks ahead of opening and closing auctions.

        Each row is one imbalance message: the auction type and time, the imbalance and paired
        quantities, the book clearing price, and the exchange. Imbalances only stream from the
        WebSocket; there is no history.

        Args:
            symbols: Stock tickers, or `dm.ALL`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.imbalances.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                closing = m.stocks.imbalances(["AAPL", "MSFT"]).append_only()
        """
        return cast("StreamQuery", build_query(self._core, IMBALANCES, symbols=symbols))

    @raises_through(build_query)
    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value estimate of US-listed stocks, as it updates.

        Each row is one fair market value with the time it was computed. It streams only from
        Massive's Business host, so it needs a Business plan; there is no history.

        Args:
            symbols: Stock tickers, or `dm.ALL`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.fmv.md.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                fair = m.stocks.fmv(["AAPL"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query, require_bool)
    def snapshots(self, symbols: Symbols, *, include_otc: bool = False) -> Query:
        """Current market snapshots of US-listed stocks, one row per ticker.

        Each row is a ticker's state now: the day's and previous day's bars, the latest minute bar,
        last trade and quote, today's change, and the fair market value where the plan includes it.
        `poll()` replaces the table each cycle, keyed by `Symbol`; `mode=dm.PollMode.APPEND` keeps
        each new state by its `Updated` time. Snapshots need a paid plan
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#what-your-plan-allows).

        Args:
            symbols: Stock tickers, or `dm.ALL` for the full market.
            include_otc: True to include OTC securities; False, the default, leaves them out.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.snapshots.md.

        Raises:
            TypeError: If `include_otc` is not a `bool`, or `symbols` is not an accepted form.
            ValueError: If `symbols` is None, empty, or holds an invalid ticker, or a `Table` has no
                `Symbol` column.

        Example:
            ::

                board = m.stocks.snapshots(["AAPL", "MSFT", "NVDA"]).poll(5)
        """
        query = build_query(
            self._core,
            SNAPSHOTS,
            symbols=symbols,
            params={"include_otc": require_bool("include_otc", include_otc)},
        )
        return cast("Query", query)

    @raises_through(require_bool, _require_enum)
    def movers(self, direction: Direction, *, include_otc: bool = False) -> Query:
        """The twenty US stocks with the largest percentage move today, up or down.

        Each row is the full snapshot of one mover, as `snapshots()` returns it, for the tickers
        with the largest gain or loss since the previous close. `poll()` replaces the table each
        cycle, keyed by `Symbol`, so a ticker that leaves the list is removed.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.
            include_otc: True to include OTC securities; False, the default, leaves them out.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.movers.md.

        Raises:
            TypeError: If `direction` is not a `dm.Direction` or `include_otc` is not a `bool`.

        Example:
            ::

                gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
        """
        query = build_query(
            self._core,
            MOVERS,
            params={
                "direction": _require_enum(direction, Direction, "direction"),
                "include_otc": require_bool("include_otc", include_otc),
            },
        )
        return cast("Query", query)

    @raises_through(build_query, require_bool, day_argument)
    def daily_open_close(self, symbols: Symbols, date: DateLike, *, adjusted: bool = True) -> Query:
        """The open, close, and pre- and after-market prices of US stocks on one day.

        Each row is one ticker's session on `date`, with the day's window as a 1-day bar's, read by
        REST one request per symbol; a symbol with no session that day (a holiday) is left out, with
        a WARNING. `poll()` replaces the table each cycle, keyed by `Symbol`.

        Args:
            symbols: Stock tickers; `dm.ALL` is refused (use `m.stocks.bars(dm.ALL,
                dm.Timespan.DAY).history(day, day)`).
            date: The trading day, a calendar date; a `datetime` or an instant raises.
            adjusted: Split-adjusted prices and volume. Massive does not echo the basis here, so it
                cannot be checked.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.daily_open_close.md.

        Raises:
            TypeError: If `symbols` or `date` is of another type, or `adjusted` is not a `bool`.
            ValueError: If `symbols` is `dm.ALL`, None, empty, or holds an invalid ticker, a `Table`
                has no `Symbol` column, or `date` is a `datetime`, another time, or text that is not
                a date.

        Example:
            ::

                t = m.stocks.daily_open_close(["AAPL", "MSFT"], "2025-06-06").fetch()
        """
        query = build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            symbols=symbols,
            all_alternative=(
                "use m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            adjusted=require_bool("adjusted", adjusted),
            params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
        )
        return cast("Query", query)

    @raises_through(list_query)
    def dividends(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Cash dividends of US-listed stocks, one row per distribution.

        Each row is one dividend: its ex-dividend, record, pay, and declaration dates, the cash
        amount as declared and split-adjusted, the currency, frequency, and distribution type.
        `poll()` replaces the table each cycle, keyed by `Id`.

        Args:
            symbols: Stock tickers; None, the default, for every ticker.
            start: The first ex-dividend date.
            end: The last ex-dividend date; without it, announced dividends still to come are
                returned too.
            sort: The field to sort by, such as `"ex_dividend_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"distribution_type": "special"}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.dividends.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                t = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
        """
        return list_query(
            self._core,
            DIVIDENDS,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def splits(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Stock splits, reverse splits, and stock dividends of US-listed stocks.

        Each row is one split: its `ExecutionDate`, the ratio (`SplitFrom` to `SplitTo`), the
        adjustment type, and `HistoricalAdjustmentFactor`, the cumulative factor that adjusts
        earlier prices. `poll()` replaces the table each cycle, keyed by `Id`. Use it to adjust
        as-traded bars yourself, as
        https://github.com/chipkent/deephaven-massive/blob/main/README.md#split-adjustment shows.

        Args:
            symbols: Stock tickers; None, the default, for every ticker.
            start: The first execution date.
            end: The last execution date.
            sort: The field to sort by, such as `"execution_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"adjustment_type": "reverse_split"}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.splits.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                t = m.stocks.splits("NVDA", start="2024-01-01").fetch()
        """
        return list_query(
            self._core,
            SPLITS,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def ipos(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Initial public offerings on US exchanges, upcoming and past, one row per offering.

        Each row is one IPO: the issuer, ticker, dates, status (rumored, pending, new, withdrawn,
        and so on), price range, final price, and shares offered. The endpoint takes one ticker per
        request, so a symbol list fans out. `poll()` updates each offering in place, keyed by
        `Symbol` and `IssuerName`, as it moves through its statuses; rows are never deleted. The key
        assumes Massive keeps an issuer's name through an offering's statuses, which it does not
        document: a renamed issuer would leave its old row beside the new one.

        Args:
            symbols: Stock tickers; None, the default, for every offering.
            start: The first listing date.
            end: The last listing date.
            sort: The field to sort by, such as `"listing_date"`.
            order: The sort direction; here it works without `sort` too.
            limit: The most rows: in total for `symbols=None`, per ticker when tickers are named.
            filters: Further endpoint parameters, such as `{"ipo_status": "pending"}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.ipos.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of
                the wrong kind.

        Example:
            ::

                upcoming = m.stocks.ipos(filters={"ipo_status": "pending"}).fetch()
        """
        return list_query(
            self._core,
            IPOS,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def short_interest(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Short interest of US-listed stocks, one row per ticker and FINRA settlement date.

        Each row is the number of shares sold short and not yet covered on a settlement date, with
        the average daily volume and the days to cover, as reported to FINRA twice a month. `poll()`
        replaces the table each cycle, keyed by `Symbol` and `SettlementDate`.

        Args:
            symbols: Stock tickers; None, the default, for every ticker.
            start: The first settlement date.
            end: The last settlement date.
            sort: The field to sort by, such as `"settlement_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"days_to_cover_gte": 5}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.short_interest.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                t = m.stocks.short_interest("GME", start="2024-01-01").fetch()
        """
        return list_query(
            self._core,
            SHORT_INTEREST,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def short_volume(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily short sale volume of US-listed stocks, one row per ticker and trading day.

        Each row is a day's short volume, exempt and non-exempt volume, total volume, and short
        volume ratio, with the per-venue breakdown FINRA publishes. `poll()` replaces the table each
        cycle, keyed by `Symbol` and `Date`.

        Args:
            symbols: Stock tickers; None, the default, for every ticker.
            start: The first trading day.
            end: The last trading day.
            sort: The field to sort by, such as `"date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"short_volume_ratio_gte": 50}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.short_volume.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                t = m.stocks.short_volume(["AAPL", "TSLA"], start="2025-06-01").fetch()
        """
        return list_query(
            self._core,
            SHORT_VOLUME,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def share_float(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The free float of US-listed stocks: the latest count of shares the public can trade.

        Each row is one ticker's most recent free float, its percentage of shares outstanding, and
        the date it took effect. The endpoint has no date filter, so there is no `start` or `end`.
        `poll()` replaces the table each cycle, keyed by `Symbol`.

        Args:
            symbols: Stock tickers; None, the default, for every ticker.
            sort: The field to sort by, such as `"free_float_percent"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"free_float_percent_lt": 20}`.

        Returns:
            A query. Columns:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.share_float.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, `order` is
                given without `sort`, or a filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                t = m.stocks.share_float(["AAPL", "MSFT"]).fetch()
        """
        return list_query(
            self._core,
            SHARE_FLOAT,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )
