"""The `m.stocks` namespace: US equities.

Datasets: trades, quotes, bars, each symbol's last trade, last quote, and previous day's bar, LULD
bands, net order imbalances, fair market value, snapshots, movers, daily open/close, dividends,
splits, IPOs, short interest and volume, and share float. The nested `m.stocks.financials` and
`m.stocks.filings` namespaces are `_stock_financials.py` and `_stock_filings.py`, which
`StocksNamespace` creates.

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

from typing import TYPE_CHECKING, ClassVar, Final, Literal, overload

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
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    EVERY_TICKER,
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    adjustment_notes,
    build_query,
    day_argument,
    list_query,
    require_bool,
    ticker_key,
)
from deephaven_massive.namespaces._stock_filings import StocksFilingsNamespace
from deephaven_massive.namespaces._stock_financials import StocksFinancialsNamespace
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    StreamHistoryQuery,
    SymbolStreamQuery,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._symbols_like import SymbolsLike
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "DATASETS",
    "StocksNamespace",
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
    real_time_only=True,
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
its nested namespaces have their own (`_stock_financials.py`, `_stock_filings.py`)."""


# --- Namespaces ---------------------------------------------------------------------------------


class StocksNamespace(Namespace):  # noqa: PLR0904 - a facade: one delegating method per dataset or dataset namespace, no state beyond the core
    """The US stock datasets, reached as `m.stocks`.

    Each method returns a query of one dataset, and together they cover trades, quotes, bars, each
    stock's last trade and quote, previous day's bar, and daily open and close, limit up-limit
    down bands, imbalances, fair market value, snapshots, movers, dividends, splits, IPOs, short
    interest, short volume, and free float. `m.stocks.financials` holds the financial
    statements, and `m.stocks.filings` the SEC filings. A method reads nothing, so it never
    blocks, and you can call it from any thread.
    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import StocksNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            live = m.stocks.trades(["AAPL", "MSFT"]).last_by()
            daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-01-02", "2026-06-30")
    """

    __slots__ = ("_filings", "_financials")
    _path: ClassVar[str] = "stocks"

    def __init__(self, _core: SessionCore) -> None:
        """Create the namespace and its two nested namespaces.

        Args:
            _core: The session's internal core.
        """
        super().__init__(_core)
        self._financials = StocksFinancialsNamespace(_core)
        self._filings = StocksFilingsNamespace(_core)

    @property
    def financials(self) -> StocksFinancialsNamespace:
        """The financial statements and valuation ratios, reached as `m.stocks.financials`."""
        return self._financials

    @property
    def filings(self) -> StocksFilingsNamespace:
        """The SEC filings and the insider data built from them, reached as `m.stocks.filings`."""
        return self._filings

    @raises_through(build_query)
    def trades(self, symbols: SymbolsLike) -> StreamHistoryQuery:
        """Trades of US-listed stocks, tick by tick.

        Each row holds one trade reported to the SIP. `Timestamp` is the SIP's time. Streams and
        flat-file history take `dm.ALL`. REST history needs named tickers. For only each symbol's
        latest trade, use `last_trade()`.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.trades.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                recent = m.stocks.trades(["AAPL", "MSFT"]).ring(10_000)
                friday = m.stocks.trades("AAPL").history("2026-10-30", "2026-10-30")
        """
        return build_query(self._core, TRADES, query_type=StreamHistoryQuery, symbols=symbols)

    @raises_through(build_query)
    def last_trade(self, symbols: SymbolsLike) -> FetchQuery:
        """The latest trade of each US-listed stock.

        Each row holds one symbol's most recent trade. A symbol with no trade is left out, and the
        library logs a WARNING. Each poll sees only the trade that is latest at that moment. For
        every trade, stream `trades()`.

        Args:
            symbols: Stock tickers. `dm.ALL` is refused; use `m.stocks.snapshots(dm.ALL)` for every
                ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.last_trade.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                last = m.stocks.last_trade(["AAPL", "MSFT"]).fetch()
        """
        return build_query(
            self._core,
            LAST_TRADE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="use m.stocks.snapshots(dm.ALL).fetch()",
        )

    @raises_through(build_query)
    def quotes(self, symbols: SymbolsLike) -> StreamHistoryQuery:
        """National best bid and offer quotes of US-listed stocks, tick by tick.

        Each row holds one NBBO update. `Timestamp` is the SIP's time. Streams and flat-file history
        take `dm.ALL`. REST history needs named tickers. For only each symbol's latest NBBO, use
        `last_quote()`.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.quotes.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                top = m.stocks.quotes(["AAPL", "SPY"]).last_by()
        """
        return build_query(self._core, QUOTES, query_type=StreamHistoryQuery, symbols=symbols)

    @raises_through(build_query)
    def last_quote(self, symbols: SymbolsLike) -> FetchQuery:
        """The latest national best bid and offer of each US-listed stock.

        Each row holds one symbol's most recent NBBO. A symbol with no quote is left out, and the
        library logs a WARNING. Each poll sees only the quote that is latest at that moment. For
        every quote, stream `quotes()`.

        Args:
            symbols: Stock tickers. `dm.ALL` is refused; use `m.stocks.snapshots(dm.ALL)` for every
                ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.last_quote.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                spreads = m.stocks.last_quote(["AAPL", "SPY"]).poll(5)
        """
        return build_query(
            self._core,
            LAST_QUOTE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="use m.stocks.snapshots(dm.ALL).fetch()",
        )

    @overload
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Literal[Timespan.SECOND, Timespan.MINUTE],
        *,
        multiplier: Literal[1] = 1,
        adjusted: bool = True,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery: ...
    @raises_through(build_query, require_bool)
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed stocks.

        Each row is one bar of `multiplier` times `timespan`. The dataset's page, linked below, says
        how each bar's window and `Date` are set.

        - History is split-adjusted unless you pass `adjusted=False`. Streams and flat files serve
          bars as traded.
        - With `dm.ALL`, `history()` reads one-day bars. It also reads one-minute bars from flat
          files when `adjusted=False`. `history()` of any other bar of `dm.ALL` raises `ValueError`.
        - For only the previous day's bar, use `previous_day_bar()`.

        Args:
            symbols: Stock tickers, or `dm.ALL`.
            timespan: The bar's unit, any but `dm.Timespan.SESSION`.
            multiplier: Units per bar.
            adjusted: Adjusts volume as well as prices.

        Returns:
            A query. One-second and one-minute bars with `multiplier=1` give a `StreamHistoryQuery`,
            which streams and reads history. Every other bar gives a `HistoryQuery`, which reads
            history only. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `timespan` is `SESSION`, or `multiplier` is below 1;
                - `symbols` is None or empty, or holds a ticker that isn't Massive's exact ticker.

        Example:
            ::

                week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
                live = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE).append_only()
        """
        return build_query(
            self._core,
            BARS,
            query_type=HistoryQuery,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            adjusted=require_bool("adjusted", adjusted),
        )

    @raises_through(build_query, require_bool)
    def previous_day_bar(self, symbols: SymbolsLike, *, adjusted: bool = True) -> FetchQuery:
        """The previous trading day's bar (open, high, low, close, volume) of US stocks.

        Each row holds one symbol's bar of the previous completed session. A symbol with no bar is
        left out, and the library logs a WARNING. The dataset's page says how the bar is stamped.

        Args:
            symbols: Stock tickers. `dm.ALL` is refused; for every ticker's bar of one day, use
                `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.
            adjusted: Adjusts volume as well as prices.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.previous_day_bar.md.

        Raises:
            TypeError: If `adjusted` isn't a `bool`, or `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                previous = m.stocks.previous_day_bar(["AAPL", "MSFT"]).fetch()
        """
        return build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "use m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
            adjusted=require_bool("adjusted", adjusted),
        )

    @raises_through(build_query)
    def luld(self, symbols: SymbolsLike) -> SymbolStreamQuery:
        """Limit up-limit down price bands of US-listed stocks, as they change.

        Each row holds one band update from the SIP, with its upper and lower price limits. The
        bands only stream, so there's no history.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.luld.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                bands = m.stocks.luld(dm.ALL).last_by()
        """
        return build_query(self._core, LULD, query_type=SymbolStreamQuery, symbols=symbols)

    @raises_through(build_query)
    def imbalances(self, symbols: SymbolsLike) -> SymbolStreamQuery:
        """Net order imbalances of US-listed stocks ahead of opening and closing auctions.

        Each row holds one imbalance message, with the auction's type and time and the imbalance
        quantity. Imbalances only stream, so there's no history.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.imbalances.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                closing = m.stocks.imbalances(["AAPL", "MSFT"]).append_only()
        """
        return build_query(self._core, IMBALANCES, query_type=SymbolStreamQuery, symbols=symbols)

    @raises_through(build_query)
    def fmv(self, symbols: SymbolsLike) -> SymbolStreamQuery:
        """Massive's fair market value estimate of US-listed stocks, as it updates.

        Each row holds one fair market value and the time Massive computed it. It streams only on a
        Business plan, and there's no history.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.fmv.md.

        Raises:
            TypeError: If `symbols` isn't one of the accepted forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                fair = m.stocks.fmv(["AAPL"]).last_by()
        """
        return build_query(self._core, FMV, query_type=SymbolStreamQuery, symbols=symbols)

    @raises_through(build_query, require_bool)
    def snapshots(self, symbols: SymbolsLike, *, include_otc: bool = False) -> FetchQuery:
        """Current market snapshots of US-listed stocks, one row per ticker.

        Each row holds a ticker's state now: the day's bar, the previous day's bar, the latest
        minute bar, the last trade and quote, and the day's change.

        Args:
            symbols: Stock tickers, or `dm.ALL` for every ticker.
            include_otc: True to include OTC securities.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.snapshots.md.

        Raises:
            TypeError: If `include_otc` isn't a `bool`, or `symbols` isn't one of the accepted
                forms.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column.

        Example:
            ::

                latest = m.stocks.snapshots(["AAPL", "MSFT", "NVDA"]).poll(5)
        """
        return build_query(
            self._core,
            SNAPSHOTS,
            query_type=FetchQuery,
            symbols=symbols,
            params={"include_otc": require_bool("include_otc", include_otc)},
        )

    @raises_through(require_bool, _require_enum)
    def movers(self, direction: Direction, *, include_otc: bool = False) -> FetchQuery:
        """The twenty US stocks with the largest percentage move today, up or down.

        Each row holds one mover's full snapshot, as `snapshots()` returns it. In a `poll()` table,
        a ticker that leaves the list leaves the table.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.
            include_otc: True to include OTC securities.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.movers.md.

        Raises:
            TypeError: If `direction` isn't a `dm.Direction`, or `include_otc` isn't a `bool`.

        Example:
            ::

                gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
        """
        return build_query(
            self._core,
            MOVERS,
            query_type=FetchQuery,
            params={
                "direction": _require_enum(direction, Direction, "direction"),
                "include_otc": require_bool("include_otc", include_otc),
            },
        )

    @raises_through(build_query, require_bool, day_argument)
    def daily_open_close(
        self, symbols: SymbolsLike, date: DateLike, *, adjusted: bool = True
    ) -> FetchQuery:
        """The open, close, and pre- and after-market prices of US stocks on one day.

        Each row holds one ticker's session on `date`. A symbol with no session that day, such as on
        a holiday, is left out, and the library logs a WARNING.

        Args:
            symbols: Stock tickers. `dm.ALL` is refused; for every ticker's bar of one day, use
                `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.
            date: The trading day, as a date.
            adjusted: Adjusts volume as well as prices. Massive doesn't report the basis of these
                prices, so the library can't check it.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.daily_open_close.md.

        Raises:
            TypeError: If `symbols` or `date` is of another type, or `adjusted` isn't a `bool`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker;
                - a table has no `Symbol` column;
                - `date` is a `datetime`, another time, or text that isn't a date.

        Example:
            ::

                friday = m.stocks.daily_open_close(["AAPL", "MSFT"], "2026-10-30").fetch()
        """
        return build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "use m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            adjusted=require_bool("adjusted", adjusted),
            params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
        )

    @raises_through(list_query)
    def dividends(
        self,
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Cash dividends of US-listed stocks, one row per distribution.

        Each row holds a dividend's ex-dividend, record, pay, and declaration dates, its cash amount
        as declared and split-adjusted, its currency, its frequency, and its distribution type.

        Args:
            symbols: Stock tickers; leave it out for every ticker.
            start: The first ex-dividend date.
            end: The last ex-dividend date.
            sort: Any of Massive's field names, such as `"pay_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. With a watchlist, `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"distribution_type": "special"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.dividends.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty, or holds a ticker that isn't Massive's exact
                  ticker;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                dividends = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
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
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Stock splits, reverse splits, and stock dividends of US-listed stocks.

        Each row holds one split's `ExecutionDate`, its ratio of `SplitFrom` to `SplitTo`, its
        adjustment type, and its `HistoricalAdjustmentFactor`. To adjust a price from day D,
        multiply it by the factor of the first split executed after D. Bars' `history()` already
        returns split-adjusted prices unless you pass `adjusted=False`.

        Args:
            symbols: Stock tickers; leave it out for every ticker.
            start: The first execution date.
            end: The last execution date.
            sort: Any of Massive's field names, such as `"execution_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"adjustment_type": "reverse_split"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.splits.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                nvidia = m.stocks.splits("NVDA", start="2024-01-01").fetch()
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
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Initial public offerings on US exchanges, upcoming and past, one row per offering.

        Each row holds an offering's issuer, ticker, dates, status, price range, final price, and
        shares offered. A `poll()` table updates each offering in place as its status changes. If
        Massive renames an issuer during an offering, the old row stays beside the new one.

        Args:
            symbols: Stock tickers; leave it out for every offering.
            start: The first listing date.
            end: The last listing date.
            sort: Any of Massive's field names, such as `"listing_date"`.
            order: The direction of the rows. Here it works without `sort` too.
            limit: The most rows: in total for `symbols=None`, per ticker when tickers are named.
            filters: Further endpoint parameters, such as `{"ipo_status": "pending"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.ipos.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date, or `end` is before `start`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

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
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Short interest of US-listed stocks, one row per ticker and FINRA settlement date.

        Each row holds the shares sold short and not yet covered on a settlement date, the average
        daily volume, and the days to cover. Short interest is reported to FINRA twice a month.

        Args:
            symbols: Stock tickers; leave it out for every ticker.
            start: The first settlement date.
            end: The last settlement date.
            sort: Any of Massive's field names, such as `"settlement_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"days_to_cover_gte": 5}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.short_interest.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                gamestop = m.stocks.short_interest("GME", start="2025-01-01").fetch()
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
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Daily short sale volume of US-listed stocks, one row per ticker and trading day.

        Each row holds a day's short volume, the total volume, the short volume ratio, and the
        breakdown by venue that FINRA publishes.

        Args:
            symbols: Stock tickers; leave it out for every ticker.
            start: The first trading day.
            end: The last trading day.
            sort: Any of Massive's field names, such as `"short_volume_ratio"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"short_volume_ratio_gte": 50}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.short_volume.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                shorts = m.stocks.short_volume(["AAPL", "TSLA"], start="2026-10-01").fetch()
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
        symbols: SymbolsLike | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """The free float of US-listed stocks: the latest count of shares the public can trade.

        Each row holds one ticker's most recent free float, its percentage of shares outstanding,
        and the date it took effect. Massive offers no date filter here, so there's no `start` or
        `end`.

        Args:
            symbols: Stock tickers; leave it out for every ticker.
            sort: Any of Massive's field names, such as `"free_float_percent"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"free_float_percent_lt": 20}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.share_float.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                floats = m.stocks.share_float(["AAPL", "MSFT"]).fetch()
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
