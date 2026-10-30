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

The REST list datasets (dividends through share float) share one shape: `symbols` is optional
and only filters the request, `dm.ALL` is refused in favour of `symbols=None` (`EVERY_TICKER`),
`start` and `end` bound the dataset's date axis where it has one, and the `sort`, `order`,
`limit`, and `filters` block works as described in the package documentation.

The module is one subject although it runs past 1,000 lines: it is one namespace, and each
dataset's spec and its factory method, whose docstring is the dataset's documentation, belong
together.
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

_R: Final = Transport.REST
_W: Final = Transport.WS
_F: Final = Transport.FLAT_FILES
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

TRADES: Final = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=_STOCKS,
    kind="trades",
    history_path=_TRADES,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (_R, _TRADES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_W, "stocks/T"): ticker_key("SipTimestamp"),
        (_F, "us_stocks_sip/trades_v1"): ticker_key("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=50_000,
)
# Each one-row dataset joins the kind of its stream and history dataset, so its rows have the
# same columns; an explicit APPEND poll() records each new row by its time.
LAST_TRADE: Final = DatasetSpec(
    name="stocks.last_trade",
    summary="The latest trade of each US-listed stock.",
    asset_class=_STOCKS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    keys={(_R, _LAST_TRADE): ticker_key("SipTimestamp")},
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
        (_R, _QUOTES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_W, "stocks/Q"): ticker_key("SipTimestamp"),
        (_F, "us_stocks_sip/quotes_v1"): ticker_key("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=50_000,
)
LAST_QUOTE: Final = DatasetSpec(
    name="stocks.last_quote",
    summary="The latest national best bid and offer of each US-listed stock.",
    asset_class=_STOCKS,
    kind="quotes",
    fetch_path=_LAST_QUOTE,
    keys={(_R, _LAST_QUOTE): ticker_key("SipTimestamp")},
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
        (_R, _RANGE): SourceKeys(symbol=_request("stocksTicker"), time=_row("WindowStart")),
        (_R, _GROUPED): ticker_key("WindowStart"),
        (_W, "stocks/A"): _stream_bar_keys(),
        (_W, "stocks/AM"): _stream_bar_keys(),
        (_F, _MINUTE_FILES): ticker_key("WindowStart"),
        (_F, _DAY_FILES): ticker_key("WindowStart"),
    },
    page_size=50_000,
    column_notes=_BAR_NOTES,
)
PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="stocks.previous_day_bar",
    summary="The previous trading day's bar (open, high, low, close, volume) of US stocks.",
    asset_class=_STOCKS,
    kind="bars",
    fetch_path=_PREV,
    keys={(_R, _PREV): ticker_key("WindowStart")},
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
    keys={(_W, "stocks/LULD"): ticker_key("UnixTimestamp")},
)
IMBALANCES: Final = DatasetSpec(
    name="stocks.imbalances",
    summary="Net order imbalances of US-listed stocks ahead of opening and closing auctions.",
    asset_class=_STOCKS,
    kind="imbalances",
    channels={NotBars.ANY: "NOI"},
    keys={(_W, "stocks/NOI"): ticker_key("UnixTimestamp")},
)
FMV: Final = DatasetSpec(
    name="stocks.fmv",
    summary="Massive's fair market value estimate of US-listed stocks, as it updates.",
    asset_class=_STOCKS,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(_W, "stocks/FMV"): ticker_key("FmvLastUpdated")},
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
    keys={(_R, _SNAPSHOTS): ticker_key()},
    symbol_param="tickers",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    revision_keys=("Updated",),
)
MOVERS: Final = DatasetSpec(
    name="stocks.movers",
    summary="The twenty US stocks with the largest percentage move today, up or down.",
    asset_class=_STOCKS,
    fetch_path=_MOVERS,
    keys={(_R, _MOVERS): ticker_key()},
    revision_keys=("Updated",),
)
DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="stocks.daily_open_close",
    summary="The open, close, and pre- and after-market prices of US stocks on one day.",
    asset_class=_STOCKS,
    fetch_path=_OPEN_CLOSE,
    keys={(_R, _OPEN_CLOSE): SourceKeys(symbol=_row("Symbol"), date=_row("From"))},
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
    keys={(_R, _DIVIDENDS): ticker_key()},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
    page_size=5_000,
)
SPLITS: Final = DatasetSpec(
    name="stocks.splits",
    summary="Stock splits, reverse splits, and stock dividends of US-listed stocks.",
    asset_class=_STOCKS,
    fetch_path=_SPLITS,
    keys={(_R, _SPLITS): ticker_key()},
    time_axis=("execution_date.gte", "execution_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
    page_size=5_000,
)
# The IPO path filters one `ticker` per request, so the REST layer sends one request per symbol.
IPOS: Final = DatasetSpec(
    name="stocks.ipos",
    summary="Initial public offerings on US exchanges, upcoming and past, one row per offering.",
    asset_class=_STOCKS,
    fetch_path=_IPOS,
    keys={(_R, _IPOS): ticker_key()},
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
    page_size=1_000,
)
SHORT_INTEREST: Final = DatasetSpec(
    name="stocks.short_interest",
    summary="Short interest of US-listed stocks, one row per ticker and FINRA settlement date.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_INTEREST,
    keys={(_R, _SHORT_INTEREST): ticker_key()},
    time_axis=("settlement_date.gte", "settlement_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "SettlementDate"),
    page_size=50_000,
)
SHORT_VOLUME: Final = DatasetSpec(
    name="stocks.short_volume",
    summary="Daily short sale volume of US-listed stocks, one row per ticker and trading day.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_VOLUME,
    keys={(_R, _SHORT_VOLUME): ticker_key()},
    time_axis=("date.gte", "date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
    page_size=50_000,
)
SHARE_FLOAT: Final = DatasetSpec(
    name="stocks.share_float",
    summary="The free float of US-listed stocks: the latest count of shares the public can trade.",
    asset_class=_STOCKS,
    fetch_path=_FLOAT,
    keys={(_R, _FLOAT): ticker_key()},
    symbol_param="ticker.any_of",
    page_size=5_000,
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

        Each row is one trade reported to the SIP: price, size, exchange, conditions, and the
        SIP, participant, and TRF timestamps (`Timestamp` is the SIP time). Each row also
        carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of
        `Timestamp`), and `TimestampInsert`, when the library received it. Streams over the
        WebSocket (`T`) through `blink()`, `append_only()`, `ring()`, and `last_by()`;
        `history()` reads REST (`/v3/trades/{stockTicker}`) or the flat files
        (`us_stocks_sip/trades_v1`). Each symbol's latest trade is `last_trade()`.

        `history()` has no `limit`: it pages internally. `dm.ALL` streams every ticker and
        reads every ticker from the flat files; REST `history()` refuses it.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#trades,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.

        Returns:
            The query; nothing is requested or subscribed until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                q = m.stocks.trades(["AAPL", "MSFT"])
                recent = q.ring(10_000)
                friday = q.history("2025-06-06", "2025-06-06")
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    @raises_through(build_query)
    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each US-listed stock.

        Each row is one symbol's most recent trade reported to the SIP, with the columns of its kind
        that its endpoint fills: price, size, exchange, conditions, and the SIP, participant, and
        TRF timestamps (`Timestamp` is the SIP time), its trading day `Date` (ISO `yyyy-MM-dd` text,
        the New York date of `Timestamp`), and `TimestampInsert`, when the library received it.
        Served by REST (`/v2/last/trade/{stocksTicker}`) through `fetch()` and `poll()`, one request
        per symbol; `poll()` replaces the table each cycle, keyed by `Symbol`, and an explicit
        `mode=dm.PollMode.APPEND` appends each cycle's last trade when it is new, so a trade between
        two cycles is never seen (`trades()` streams every one). Some plans lack the endpoint (HTTP
        403 raises `EntitlementError`); `snapshots(symbols)` carries each ticker's last trade on
        every plan. A symbol with no trade (HTTP 404) is left out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each symbol gives one row. `dm.ALL` is refused,
        since the endpoint takes one ticker per request: use `m.stocks.snapshots(dm.ALL).fetch()`.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#last_trade,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, or a `Table` with a `Symbol`
                column (re-read each `poll()` cycle).

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a `Table`
                has no `Symbol` column. Also for `symbols` of None, with a message that names the
                forms it takes.

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

        Each row is one NBBO update: bid and ask prices, sizes, and exchanges, conditions,
        indicators, and the SIP and participant timestamps (`Timestamp` is the SIP time). Each
        row also carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of
        `Timestamp`), and `TimestampInsert`, when the library received it. Streams over the
        WebSocket (`Q`) through `blink()`, `append_only()`, `ring()`, and `last_by()`;
        `history()` reads REST (`/v3/quotes/{stockTicker}`) or the flat files
        (`us_stocks_sip/quotes_v1`). Each symbol's latest NBBO is `last_quote()`.

        `history()` has no `limit`: it pages internally. `dm.ALL` streams every ticker and
        reads every ticker from the flat files; REST `history()` refuses it.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#quotes,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.

        Returns:
            The query; nothing is requested or subscribed until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                top = m.stocks.quotes(["AAPL", "SPY"]).last_by()
        """
        return cast("StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols))

    @raises_through(build_query)
    def last_quote(self, symbols: Symbols) -> Query:
        """The latest national best bid and offer of each US-listed stock.

        Each row is one symbol's most recent NBBO, with the columns of its kind that its endpoint
        fills: bid and ask prices, sizes, and exchanges, conditions, indicators, and the SIP and
        participant timestamps (`Timestamp` is the SIP time), its trading day `Date` (ISO
        `yyyy-MM-dd` text, the New York date of `Timestamp`), and `TimestampInsert`, when the
        library received it. Served by REST (`/v2/last/nbbo/{stocksTicker}`) through `fetch()` and
        `poll()`, one request per symbol; `poll()` replaces the table each cycle, keyed by `Symbol`,
        and an explicit `mode=dm.PollMode.APPEND` appends each cycle's last quote when it is new, so
        a quote between two cycles is never seen (`quotes()` streams every one). Some plans lack the
        endpoint (HTTP 403 raises `EntitlementError`); `snapshots(symbols)` carries each ticker's
        last quote on every plan. A symbol with no quote (HTTP 404) is left out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each symbol gives one row. `dm.ALL` is refused,
        since the endpoint takes one ticker per request: use `m.stocks.snapshots(dm.ALL).fetch()`.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#last_quote,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, or a `Table` with a `Symbol`
                column (re-read each `poll()` cycle).

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a `Table`
                has no `Symbol` column. Also for `symbols` of None, with a message that names the
                forms it takes.

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
        timespan: StreamSpan = Timespan.MINUTE,
        *,
        multiplier: Literal[1] = 1,
        adjusted: bool = True,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery: ...
    @raises_through(build_query, require_bool)
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed stocks.

        Each row is one bar of `multiplier` times `timespan`: its window, from `TimestampStart`
        to the exclusive `TimestampEnd`, its trading day `Date` (ISO `yyyy-MM-dd` text, New York
        time), its open, high, low, close, volume, VWAP, and trade count, and `TimestampInsert`,
        when the library received it. The timespan picks the transports and the query class:
        one-second and one-minute bars also stream over the WebSocket (`A`, `AM`) as a
        `StreamHistoryQuery`; every other combination is history only, and the previous
        completed session's bar is `previous_day_bar()`. `history()` reads REST
        (`/v2/aggs/ticker/{stocksTicker}/range/...`) or, for unadjusted one-minute and one-day
        bars, the flat files (`us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1`); with
        `dm.ALL`, one-day REST history reads the grouped daily bars
        (`/v2/aggs/grouped/locale/us/market/stocks/{date}`), one request per day.

        `TimestampEnd` is the first instant at which the bar is complete, so join on it
        (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only bars whose
        window had closed; it is not when Massive published the bar. An intraday bar ends
        its length after its start. A bar of a day or longer runs from 00:00 New York time
        of its window's first day to 00:00 after its last, whatever the source: the range
        stamps a 1-day bar at the day's start, while grouped daily and the day
        files stamp it at the 16:00 session close (13:00 on a short trading day), which is
        moved to the day's start. A week runs Monday to Friday and a month, quarter, or year
        from its first to its last weekday. A bar of several days spans that many calendar
        days, weekends included, as Massive counts them: a 2-day bar stamped on a Friday
        holds Friday's trading alone, and the next is stamped on the Sunday. A longer bar
        must be stamped at 00:00 on the day its period starts: a week on its Sunday, where
        Massive starts it, a month, quarter, or year on its 1st. Any other stamp fails the call
        with `MassiveError` naming the source, since the bar may cover other days than its
        label. `Date` is null for a bar that spans more than one date: several days, a week
        or longer, or an intraday bar across midnight. Joining an intraday row to the same
        day's daily bar on `Date` attaches values not known until that day ended.

        Prices are split-adjusted only when `adjusted` is True, the default, and only REST
        serves adjusted bars: a price from before a split is restated in the split's terms, so
        it is not the price that traded, and volumes are restated the other way. Massive
        adjusts for splits only, never for cash dividends. The flat files and the WebSocket are
        as traded, so `history()` reads the flat files only with `adjusted=False`, and one
        `history()` never mixes the two bases: every page's own `adjusted` must match the
        request, or the call raises `MassiveError`. So the default
        `bars(dm.ALL, dm.Timespan.MINUTE).history(...)` raises `ValueError`: the flat files need
        `adjusted=False`, and REST needs explicit symbols. Live bars are never adjusted after
        the fact, so a live table, or a merge of adjusted history and live bars, shows a jump
        at a split's execution date; rebuild from history after a split, or adjust
        unadjusted bars yourself with the factors of `splits()`.

        `history()` has no `limit`: it pages internally. It returns only the bars whose
        whole window lies inside the range and has ended, judged by `TimestampEnd`: a week
        runs Monday to Friday and a month from its first to its last weekday, so the current
        day's, week's, or month's bar never appears, and a 2-day bar from a Thursday is left
        out of a range that ends on the Friday, since a holiday could carry it into Monday.
        `dm.ALL` streams every ticker and works with `history()` where a flat file or the
        grouped daily path serves it. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#bars,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.
            timespan: The bar unit, a `dm.Timespan` other than `SESSION`; defaults to
                `MINUTE`.
            multiplier: The number of `timespan` units per bar, a positive `int`; defaults to
                1. Only 1 streams.
            adjusted: True (the default) for bars adjusted for splits, from REST only; False
                for bars as traded, which the flat files can also serve. Streams are always as
                traded.

        Returns:
            The query, of the class the timespan and multiplier give; nothing is requested
            or subscribed until a verb runs.

        Raises:
            TypeError: If `timespan` is not a `dm.Timespan`, `multiplier` is not an `int`,
                `adjusted` is not a `bool`, or `symbols` is not an accepted form.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, or `symbols` is empty
                or holds an invalid ticker. Also for `symbols` of None, with a message that names
                the forms it takes.

        Example:
            ::

                live = m.stocks.bars(["AAPL"]).append_only()  # one-minute bars, streamed
                daily = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
                hourly = m.stocks.bars("SPY", dm.Timespan.HOUR, multiplier=4).history(
                    "2025-01-01", "2025-03-31"
                )
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

        Each row is one symbol's 1-day bar of the previous completed session, which Massive
        documents as "the prior session's open, high, low, close, and volume", with the columns of
        its kind that its endpoint fills: its window, from `TimestampStart`, 00:00 New York time of
        the day, to the exclusive `TimestampEnd`, 00:00 of the next day, its trading day `Date` (ISO
        `yyyy-MM-dd` text), open, high, low, close, volume, VWAP, and trade count, and
        `TimestampInsert`, when the library received it. Served by REST
        (`/v2/aggs/ticker/{stocksTicker}/prev`) through `fetch()` and `poll()`, one request per
        symbol; `poll()` replaces the table each cycle, keyed by `Symbol`, and an explicit
        `mode=dm.PollMode.APPEND` appends each new day's bar. Massive stamps the bar at the 16:00
        session close (13:00 on a short day), which is moved to the day's start; any other stamp
        raises `MassiveError` naming `bars(symbols, dm.Timespan.DAY).history()` for that day. A
        symbol with no bar (HTTP 404) is left out, with a WARNING.

        Prices are split-adjusted when `adjusted` is True, the default, as `bars()` describes,
        and a page whose own `adjusted` differs from the request raises `MassiveError`.

        `fetch()` and `poll()` take no `limit`: each symbol gives one row. `dm.ALL` is refused,
        since the endpoint takes one ticker per request: use
        `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(day, day)` for every ticker's bar of a
        day. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#previous_day_bar,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, or a `Table` with a `Symbol`
                column (re-read each `poll()` cycle).
            adjusted: True (the default) for prices and volume adjusted for splits; False for
                the bar as traded.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If `adjusted` is not a `bool`, or `symbols` is not an accepted form.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a `Table`
                has no `Symbol` column. Also for `symbols` of None, with a message that names the
                forms it takes.

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

        Each row is one band update from the SIP: the upper and lower price limits, the
        indicators, and the tape. Streams over the WebSocket (`LULD`) through `blink()`,
        `append_only()`, `ring()`, and `last_by()`; there is no REST or flat-file source, so the
        query has no `fetch()` or `history()`. Each row also carries its time, `Timestamp`, its
        trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of `Timestamp`), and
        `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#luld,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                bands = m.stocks.luld(dm.ALL).last_by()
        """
        return cast("StreamQuery", build_query(self._core, LULD, symbols=symbols))

    @raises_through(build_query)
    def imbalances(self, symbols: Symbols) -> StreamQuery:
        """Net order imbalances of US-listed stocks ahead of opening and closing auctions.

        Each row is one imbalance message: the auction type and time, the imbalance and paired
        quantities, the book clearing price, and the exchange. Streams over the WebSocket
        (`NOI`) through `blink()`, `append_only()`, `ring()`, and `last_by()`; there is no REST
        or flat-file source, so the query has no `fetch()` or `history()`. Each row also carries
        its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date
        of `Timestamp`), and `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#imbalances,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                closing = m.stocks.imbalances(["AAPL", "MSFT"]).append_only()
        """
        return cast("StreamQuery", build_query(self._core, IMBALANCES, symbols=symbols))

    @raises_through(build_query)
    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value estimate of US-listed stocks, as it updates.

        Each row is one fair market value with the time it was computed. Streams over the
        WebSocket (`FMV`) from the Business host (`business.massive.com/stocks`), which needs a
        Business plan, through `blink()`, `append_only()`, `ring()`, and `last_by()`; there is
        no REST or flat-file source, so the query has no `fetch()` or `history()`. Each row also
        carries its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the New
        York date of `Timestamp`), and `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#fmv,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Raises:
            TypeError: If `symbols` is not one of the accepted forms.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                fair = m.stocks.fmv(["AAPL"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query, require_bool)
    def snapshots(self, symbols: Symbols, *, include_otc: bool = False) -> Query:
        """Current market snapshots of US-listed stocks, one row per ticker.

        Each row is a ticker's state now: the day's and previous day's bars, the latest
        minute bar, last trade, and last quote, today's change, and the fair market value
        where the plan includes it. Served by REST
        (`/v2/snapshot/locale/us/markets/stocks/tickers`) through `fetch()` and `poll()`, one
        request for every symbol; `poll()` replaces the table each cycle, keyed by `Symbol`.
        `snapshots(symbols).poll(n)` is the way to get current state on every plan.

        `fetch()` and `poll()` take no `limit`: the endpoint returns one row per ticker.
        `dm.ALL` returns the full market. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#snapshots,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (re-read each `poll()` cycle), or `dm.ALL` for the full market.
            include_otc: True to include OTC securities; False (the default) leaves them out.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If `include_otc` is not a `bool`, or `symbols` is not an accepted form.
            ValueError: If `symbols` is empty or holds an invalid ticker, or a `Table` has no
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

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

        Each row is the full snapshot of one mover (as `snapshots()` returns it), for the
        tickers with the largest gain or loss since the previous close. Served by REST
        (`/v2/snapshot/locale/us/markets/stocks/{direction}`) through `fetch()` and `poll()`;
        `poll()` replaces the table each cycle, keyed by `Symbol`, so tickers that leave the
        list are removed.

        `fetch()` and `poll()` take no `limit`: the endpoint returns at most twenty rows. The
        dataset takes no `symbols`, so `dm.ALL` does not apply. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#movers,
        the web UI's column tooltips, or `columns()`.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.
            include_otc: True to include OTC securities; False (the default) leaves them out.

        Returns:
            The query; nothing is requested until a verb runs.

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

        Each row is one ticker's session on `date`: its `Date` (ISO `yyyy-MM-dd` text), the
        day's window as a 1-day bar's (`TimestampStart` and `TimestampEnd`, the New York
        midnights around it), open, high, low, close, volume, and the pre-market and after-hours
        prices. Served by REST (`/v1/open-close/{stocksTicker}/{date}`) through `fetch()` and
        `poll()`, one request per symbol; a symbol with no session that day (a holiday) is
        dropped with a warning. `poll()` replaces the table each cycle, keyed by `Symbol`.

        `fetch()` and `poll()` take no `limit`: each symbol gives one row. `dm.ALL` is
        refused: use `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(date, date)`. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#daily_open_close,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers: one `str`, a sequence of them, or a `Table` with a `Symbol`
                column (re-read each `poll()` cycle).
            date: The trading day: an ISO-8601 date, a `date`, Deephaven date text such as
                `"2025-3-3"`, a Java `LocalDate`, or a day-precision `numpy.datetime64`. It is a
                calendar date, so a `datetime` or an instant raises `ValueError`.
            adjusted: True (the default) for prices and volume adjusted for splits (never for
                dividends); False for the prices as traded. Massive does not echo the basis
                here, so it cannot be checked.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If `date` is not a `str`, `date`, or `datetime`, `adjusted` is not a `bool`,
                or `symbols` is not an accepted form.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or `date` is a
                `datetime` or not an ISO-8601 date. Also for `symbols` of None, with a message that
                names the forms it takes.

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

        Each row is one dividend: its ex-dividend, record, pay, and declaration dates, the
        cash amount (as declared and split-adjusted), the currency, frequency, and
        distribution type. Served by REST (`/stocks/v1/dividends`) through `fetch()` and
        `poll()`; `poll()` replaces the table each cycle, keyed by `Id`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#dividends,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first ex-dividend date to include, inclusive; None for no lower bound.
            end: The last ex-dividend date to include, inclusive. None (the default) sends no upper
                bound, so an announced dividend whose ex-date is still to come is returned too; pass
                `end=` to stop at a date.
            sort: The field to sort by, such as `"ex_dividend_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"distribution_type": "special"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of
                the wrong kind.

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

        Each row is one split: its execution date (`ExecutionDate`, ISO `yyyy-MM-dd` text), the
        split ratio (`SplitFrom` to `SplitTo`), the adjustment type, and the factor that adjusts
        earlier prices (`HistoricalAdjustmentFactor`). Served by REST (`/stocks/v1/splits`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by `Id`.

        Use it to adjust bars as traded (`bars(..., adjusted=False)`, the flat files, or a
        stream) yourself. The factor is cumulative: for a bar dated D, take the first split
        whose execution date is after D and multiply the bar's prices by its factor and divide
        its volume by it; a bar with no later split is already on today's basis. Both dates are
        ISO text, which orders as dates do, so a reverse as-of join finds that split:
        `bars.raj(splits, on=["Symbol", "Date<ExecutionDate"],
        joins=["F=HistoricalAdjustmentFactor"])`, then `Close * F` and `Volume / F`, with a
        null `F` meaning 1. A split takes effect overnight: on its execution date all trading,
        the pre-market included, is already on the new basis.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#splits,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first execution date to include, inclusive; None for no lower bound.
            end: The last execution date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"execution_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"adjustment_type": "reverse_split"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of
                the wrong kind.

        Example:
            ::

                t = m.stocks.splits(start="2025-01-01", end="2025-06-30").fetch()

                # NVDA's daily bars as traded, adjusted for its splits by hand.
                bars = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
                    "2024-06-03", "2024-06-14"
                )
                splits = m.stocks.splits("NVDA").fetch()
                adjusted = bars.raj(
                    splits,
                    on=["Symbol", "Date<ExecutionDate"],
                    joins=["F=HistoricalAdjustmentFactor"],
                ).update(
                    ["F = isNull(F) ? 1.0 : F", "AdjClose = Close * F", "AdjVolume = Volume / F"]
                )
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

        Each row is one IPO: the issuer, ticker, listing and announcement dates, status (rumored,
        pending, new, withdrawn, and so on), offer price range, final issue price, shares offered,
        and the listing exchange. Served by REST (`/vX/reference/ipos`) through `fetch()` and
        `poll()`; the endpoint filters one ticker per request, so a symbol list fans out to one
        request per symbol. By default `poll()` updates each offering in place, keyed by
        `Symbol` and `IssuerName`, as it moves from rumor or pending to new and history (or is
        postponed or withdrawn); rows are never deleted. Two issuers that used one ticker stay
        two rows, while an issuer's withdrawn offering and its later offering under the same
        ticker share one row, which shows the latest. The key assumes Massive keeps an
        offering's issuer name the same through its statuses, which Massive does not
        document: if it revised the name (a legal suffix added, say), the row under the old
        name would stay, showing the offering's earlier state beside the new row.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every offering. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#ipos,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every offering.
            start: The first listing date to include, inclusive; None for no lower bound.
            end: The last listing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"listing_date"`; None for the server default.
            order: The sort direction, sent as the IPO path's own `order` parameter, so it works
                with or without `sort`; None for the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"ipo_status": "pending"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

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

        Each row is the number of shares sold short and not yet covered on a settlement
        date, with the average daily volume and the days to cover, as reported to FINRA
        twice a month. Served by REST (`/stocks/v1/short-interest`) through `fetch()` and
        `poll()`; `poll()` replaces the table each cycle, keyed by `Symbol` and
        `SettlementDate`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#short_interest,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first settlement date to include, inclusive; None for no lower bound.
            end: The last settlement date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"settlement_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"days_to_cover_gte": 5}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of
                the wrong kind.

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

        Each row is a day's short volume, exempt and non-exempt volume, total volume, and
        short volume ratio, with the per-venue breakdown FINRA publishes (NYSE, Nasdaq
        Carteret and Chicago, and the ADF). Served by REST (`/stocks/v1/short-volume`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by
        `Symbol` and `Date`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#short_volume,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first trading day to include, inclusive; None for no lower bound.
            end: The last trading day to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"short_volume_ratio_gte": 50}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of
                the wrong kind.

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

        Each row is one ticker's most recent free float, its percentage of shares
        outstanding, and the date it took effect (`EffectiveDate`). The endpoint has no date
        filter, so the dataset has no `start` or `end`. Served by REST (`/stocks/vX/float`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by
        `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#share_float,
        the web UI's column tooltips, or `columns()`.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            sort: The field to sort by, such as `"free_float_percent"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"free_float_percent_lt": 20}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a filter is
                unknown, reserved, or of the wrong kind.

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
