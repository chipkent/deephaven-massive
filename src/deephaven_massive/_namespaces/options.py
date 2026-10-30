"""The `m.options` namespace: US-listed options contracts, as reported by OPRA.

Datasets: `trades`, `last_trade` (each contract's latest trade), `quotes`, `bars`,
`previous_day_bar` (each contract's bar of the previous session), `fmv` (fair market value),
`chain` (one underlying's
option chain), `snapshots` (the current state of chosen contracts), `contracts` (contract
reference data), and `daily_open_close`.

Each dataset is one factory method on `_Options`. The method validates its arguments and returns
a query; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`). A spec names the dataset's sources and key columns, never its
columns: those come from the generated wire schemas.

Option symbols are Massive's exact tickers: `O:` and the OCC symbol (`O:AAPL250117C00150000`).
Every method takes only that form (`check_symbol` in `_datasets/symbols.py`), and
`dm.canonical_symbol_option` converts the bare or space-padded OCC forms. Every options dataset
also has `OptionRoot`, the OCC root read from the symbol (`SPXW`, `AAPL1`). The rows of the
chain, the snapshots, and the contract reference data carry the instrument Massive links the
contract to as `Underlying`, exactly as Massive sends it: it names the linked instrument, not
the deliverable, so an adjusted contract (root `AAPL1`) may deliver more or less than 100
shares of it. Neither column is ever mapped to the other.

One `/options` WebSocket connection allows 1,000 contracts. The stream verbs count topics
against that cap and raise `ValueError` before subscribing a static list that would cross it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.symbols import check_symbol, symbol_class
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    ClassFromPrefix,
    ContractType,
    NotBars,
    Order,
    Timespan,
    _require_enum,
)
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import (
    Namespace,
    adjustment_notes,
    build_query,
    day_argument,
    list_params,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._query import (
        HistoryQuery,
        Query,
        StreamHistoryQuery,
        StreamQuery,
    )
    from deephaven_massive._types import DateLike, FilterValue, StreamSpan, Symbols

__all__ = ["DATASETS", "_Options"]

logger = logging.getLogger(__name__)

_REST, _WS, _FLAT = Transport.REST, Transport.WS, Transport.FLAT_FILES

_SNAPSHOT_TYPE: Final = "options"
"""The `type` the unified snapshot is sent, so `dm.ALL` lists options contracts only."""

_LAST_TRADE: Final = "/v2/last/trade/{optionsTicker}"
_TRADES: Final = "/v3/trades/{optionsTicker}"
_QUOTES: Final = "/v3/quotes/{optionsTicker}"
_PREV: Final = "/v2/aggs/ticker/{optionsTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_CHAIN: Final = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT: Final = "/v3/snapshot"
_CONTRACTS: Final = "/v3/reference/options/contracts"
_CONTRACT: Final = "/v3/reference/options/contracts/{options_ticker}"
_OPEN_CLOSE: Final = "/v1/open-close/{optionsTicker}/{date}"

_TRADE_FILES: Final = "us_options_opra/trades_v1"
_QUOTE_FILES: Final = "us_options_opra/quotes_v1"
_MINUTE_FILES: Final = "us_options_opra/minute_aggs_v1"
_DAY_FILES: Final = "us_options_opra/day_aggs_v1"

_TICK_PAGE: Final = 50_000
"""The largest page the trades, quotes, and aggregates endpoints return."""

_SNAPSHOT_PAGE: Final = 250
"""The largest page the chain and the unified snapshot return; also the most tickers one
unified-snapshot request may name."""

_CONTRACTS_PAGE: Final = 1_000
"""The largest page the contracts endpoint returns."""


def _keys(symbol: str, time: str | None = None, *, fanout: str | None = None) -> SourceKeys:
    """Build the key columns of one source from the source's own column names.

    Args:
        symbol: The column holding the contract, renamed to `Symbol`.
        time: The column holding the row's time, renamed to `Timestamp`; None for none.
        fanout: The column holding the underlying, renamed to `Underlying`; None for none.

    Returns:
        The source's keys.
    """
    return SourceKeys(
        symbol=KeySpec(symbol),
        time=None if time is None else KeySpec(time),
        fanout=None if fanout is None else KeySpec(fanout),
    )


def _stream_bar_keys() -> SourceKeys:
    """Build the keys of a WebSocket bars channel: its contract, window start, and end `e`.

    Returns:
        The channel's keys.
    """
    return SourceKeys(
        symbol=KeySpec("Ticker"), time=KeySpec("WindowStart"), end=KeySpec("EndTimestamp")
    )


def _from_path(time: str) -> SourceKeys:
    """Build the keys of a source that takes the contract as its `{optionsTicker}` segment.

    Args:
        time: The column holding the row's time, renamed to `Timestamp`.

    Returns:
        The source's keys; `Symbol` is filled from the request.
    """
    return SourceKeys(symbol=KeySpec("optionsTicker", from_request=True), time=KeySpec(time))


TRADES: Final = DatasetSpec(
    name="options.trades",
    summary="Trades of US-listed options contracts (OPRA), tick by tick.",
    asset_class=AssetClass.OPTIONS,
    kind="trades",
    history_path=_TRADES,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: _TRADE_FILES},
    keys={
        (_REST, _TRADES): _from_path("SipTimestamp"),
        (_WS, "options/T"): _keys("Ticker", "SipTimestamp"),
        (_FLAT, _TRADE_FILES): _keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=_TICK_PAGE,
)

# Each one-row dataset joins the kind of its stream and history dataset, so its rows have the
# same columns; an explicit APPEND poll() records each new row by its time.
LAST_TRADE: Final = DatasetSpec(
    name="options.last_trade",
    summary="The latest trade of each US-listed options contract.",
    asset_class=AssetClass.OPTIONS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    keys={(_REST, _LAST_TRADE): _keys("Ticker", "SipTimestamp")},
    per_symbol=True,
    revision_keys=("Timestamp",),
)

QUOTES: Final = DatasetSpec(
    name="options.quotes",
    summary="Quotes of US-listed options contracts (OPRA), tick by tick.",
    asset_class=AssetClass.OPTIONS,
    kind="quotes",
    history_path=_QUOTES,
    channels={NotBars.ANY: "Q"},
    flat_file_prefixes={NotBars.ANY: _QUOTE_FILES},
    keys={
        (_REST, _QUOTES): _from_path("SipTimestamp"),
        (_WS, "options/Q"): _keys("Ticker", "SipTimestamp"),
        (_FLAT, _QUOTE_FILES): _keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=_TICK_PAGE,
)

BARS: Final = DatasetSpec(
    name="options.bars",
    summary="Aggregate bars (open, high, low, close, volume) of US-listed options contracts.",
    asset_class=AssetClass.OPTIONS,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_REST, _RANGE): _from_path("WindowStart"),
        (_WS, "options/A"): _stream_bar_keys(),
        (_WS, "options/AM"): _stream_bar_keys(),
        (_FLAT, _MINUTE_FILES): _keys("Ticker", "WindowStart"),
        (_FLAT, _DAY_FILES): _keys("Ticker", "WindowStart"),
    },
    page_size=_TICK_PAGE,
    column_notes=adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",)),
)

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="options.previous_day_bar",
    summary="The previous trading day's open, high, low, close, and volume of options contracts.",
    asset_class=AssetClass.OPTIONS,
    kind="bars",
    fetch_path=_PREV,
    keys={(_REST, _PREV): _keys("Ticker", "WindowStart")},
    per_symbol=True,
    revision_keys=("TimestampStart",),
    column_notes=adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",)),
)

FMV: Final = DatasetSpec(
    name="options.fmv",
    summary="Massive's fair market value of US-listed options contracts, streamed in real time.",
    asset_class=AssetClass.OPTIONS,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(_WS, "options/FMV"): _keys("Ticker", "FmvLastUpdated")},
)

_SNAPSHOT_REVISION: Final = ("LastQuoteLastUpdated", "LastTradeSipTimestamp")
"""The update times of a contract's snapshot: a new quote or trade is a new row of an explicit
`poll(mode=PollMode.APPEND)`. The snapshot carries no single update time of its own."""

CHAIN: Final = DatasetSpec(
    name="options.chain",
    summary="The snapshot of every contract in one underlying's option chain.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_CHAIN,
    keys={(_REST, _CHAIN): _keys("DetailsTicker", fanout="UnderlyingAssetTicker")},
    page_size=_SNAPSHOT_PAGE,
    revision_keys=_SNAPSHOT_REVISION,
)

SNAPSHOTS: Final = DatasetSpec(
    name="options.snapshots",
    summary="The current snapshot of each requested options contract, with its greeks.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_SNAPSHOT,
    keys={(_REST, _SNAPSHOT): _keys("Ticker", fanout="UnderlyingAssetTicker")},
    symbol_param="ticker.any_of",
    symbols_per_request=_SNAPSHOT_PAGE,
    page_size=_SNAPSHOT_PAGE,
    revision_keys=_SNAPSHOT_REVISION,
)

CONTRACTS: Final = DatasetSpec(
    name="options.contracts",
    summary="Reference data on options contracts: type, exercise style, expiration, and strike.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_CONTRACTS,
    detail_path=_CONTRACT,
    keys={
        (_REST, _CONTRACTS): _keys("Ticker", fanout="UnderlyingTicker"),
        # The per-contract rows carry their ticker, although the request names it too.
        (_REST, _CONTRACT): _keys("Ticker", fanout="UnderlyingTicker"),
    },
    symbol_param="ticker",
    page_size=_CONTRACTS_PAGE,
)

DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="options.daily_open_close",
    summary="The open, high, low, close, and volume of each options contract on one day.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_OPEN_CLOSE,
    keys={(_REST, _OPEN_CLOSE): SourceKeys(symbol=KeySpec("Symbol"), date=KeySpec("From"))},
    # One contract a request, which makes this an instrument dataset: its `Symbol` is required.
    per_symbol=True,
    row_times=RowTimes.DAY,
    column_notes=adjustment_notes(
        ("Open", "High", "Low", "Close", "PreMarket", "AfterHours"), ("Volume",)
    ),
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    LAST_TRADE,
    QUOTES,
    BARS,
    PREVIOUS_DAY_BAR,
    FMV,
    CHAIN,
    SNAPSHOTS,
    CONTRACTS,
    DAILY_OPEN_CLOSE,
)
"""Every dataset of `m.options`, one `DatasetSpec` each, in the order of the namespace's methods."""


def _underlying(value: object) -> str:
    """Check an `underlying` argument: Massive's exact ticker of a stock or an index.

    Args:
        value: The `underlying` argument as passed, such as `"AAPL"`, `"BRK.B"`, or `"I:SPX"`.

    Returns:
        The ticker, unchanged: case is kept (`KIMpL`), and nothing is converted.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is the ticker of an option, a currency pair, or a crypto pair,
            or is not canonical; `check_symbol` names the canonical form when it can.
    """
    if not isinstance(value, str):
        raise TypeError(f"underlying must be a str, not {type(value).__name__}")
    if symbol_class(value) not in {AssetClass.STOCKS, AssetClass.INDICES}:
        raise ValueError(
            f"underlying must be a stock or index ticker such as 'AAPL' or 'I:SPX', not {value!r}"
        )
    return check_symbol(ClassFromPrefix.ANY, value)


def _contract_type(value: object) -> ContractType | None:
    """Check the optional `contract_type` argument of the chain and the contracts.

    Args:
        value: The argument as passed.

    Returns:
        The member, or None when the argument is None.

    A value that is not a `dm.ContractType` raises the `TypeError` of `_require_enum`.
    """
    return None if value is None else _require_enum(value, ContractType, "contract_type")


def _expired(value: object) -> bool | None:
    """Check the optional `expired` argument of the contracts.

    Args:
        value: The argument as passed.

    Returns:
        The value: True, False, or None.

    Raises:
        TypeError: If the value is neither None nor a `bool`.
    """
    if value is not None and not isinstance(value, bool):
        raise TypeError(f"expired must be a bool, not {type(value).__name__}")
    return value


class _Options(Namespace):
    """`m.options`: the US options (OPRA) datasets of a session.

    Created once by `MassiveSession` and reached as `m.options`. It holds no state of its own,
    so it is safe to use from any thread; each method validates its arguments synchronously,
    builds a query, and never blocks or touches the network.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            calls = m.options.chain("AAPL", contract_type=dm.ContractType.CALL).fetch()
            live = m.options.trades(["O:AAPL250117C00150000"]).append_only()
    """

    __slots__ = ()
    _path: ClassVar[str] = "options"

    def trades(self, symbols: Symbols) -> StreamHistoryQuery:
        """Trades of US-listed options contracts (OPRA), tick by tick.

        One row per trade: its price, size, exchange, condition codes, and SIP timestamp
        (`Timestamp`). Each row also carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the
        New York date of `Timestamp`), and `TimestampInsert`, when the library received it. The
        stream verbs read the WebSocket `T` channel. `history()` reads the REST trades endpoint
        (`/v3/trades/{optionsTicker}`, one request series per contract) or the
        `us_options_opra/trades_v1` flat files; each contract's latest trade is `last_trade()`.
        There is no `limit`: `history()` pages internally and returns the whole range.

        `dm.ALL` streams every contract (the `T.*` topic) and reads every contract of a day's
        flat file; REST `history()` takes explicit contracts and raises `ValueError` for
        `dm.ALL`. One `/options` connection carries at most 1,000 contract topics.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#trades,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if a symbol is not an OCC option symbol,
        a sequence is empty or an exhausted generator, or a table has no `Symbol` column;
        `TypeError` if `symbols` is of another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol)
                (`"O:AAPL250117C00150000"`); one `str`, a sequence, a Deephaven table with a
                `Symbol` column that the stream follows as rows are added and removed, or
                `dm.ALL`.

        Returns:
            A query with `history()` and the stream verbs.

        Example:
            ::

                q = m.options.trades("O:AAPL250117C00150000")
                ticks = q.blink()
                day = q.history("2025-01-02", "2025-01-02")
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each US-listed options contract.

        One row per contract: its most recent trade, with the columns of its kind that its endpoint
        fills: price, size, exchange, condition codes, and SIP timestamp (`Timestamp`), its trading
        day `Date` (ISO `yyyy-MM-dd` text, the New York date of `Timestamp`), and `TimestampInsert`,
        when the library received it. Served by REST (`/v2/last/trade/{optionsTicker}`) through
        `fetch()` and `poll()`, one request per contract; `poll()` replaces the table each cycle,
        keyed by `Symbol`, and an explicit `mode=dm.PollMode.APPEND` appends each cycle's last trade
        when it is new, so a trade between two cycles is never seen (`trades()` streams every one).
        Some plans lack the endpoint (HTTP 403 raises `EntitlementError`); `snapshots(symbols)`
        carries each contract's last trade on every plan. A contract with no trade (HTTP 404) is
        left out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each contract gives one row. `dm.ALL` is
        refused, since the endpoint takes one contract per request: use
        `m.options.chain(underlying).fetch()` for every contract of an underlying.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#last_trade,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if `symbols` is `dm.ALL`, a symbol
        is not an OCC option symbol, a sequence is empty or an exhausted generator, or a table
        has no `Symbol` column; `TypeError` if `symbols` is of another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, or a Deephaven table with a `Symbol` column, re-read each `poll()`
                cycle.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                last = m.options.last_trade(["O:AAPL250117C00150000"]).fetch()
        """
        query = build_query(
            self._core,
            LAST_TRADE,
            symbols=symbols,
            all_alternative="m.options.chain(underlying).fetch()",
        )
        return cast("Query", query)

    def quotes(self, symbols: Symbols) -> StreamHistoryQuery:
        """Quotes of US-listed options contracts (OPRA), tick by tick.

        One row per quote: bid and ask prices, sizes, and exchanges, and the SIP timestamp
        (`Timestamp`). Each row also carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the
        New York date of `Timestamp`), and `TimestampInsert`, when the library received it. The
        stream verbs read the WebSocket `Q` channel. `history()` reads the REST quotes endpoint
        (`/v3/quotes/{optionsTicker}`, one request series per contract) or the
        `us_options_opra/quotes_v1` flat files. Massive has no last-quote endpoint for options,
        so there is no `fetch()`: `m.options.snapshots(symbols)` gives each contract's latest
        quote. There is no `limit`: `history()` pages internally and returns the whole range.

        `dm.ALL` streams every contract (the `Q.*` topic) and reads every contract of a day's
        flat file; REST `history()` takes explicit contracts and raises `ValueError` for
        `dm.ALL`. One `/options` connection carries at most 1,000 contract topics.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#quotes,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if a symbol is not an OCC option symbol,
        a sequence is empty or an exhausted generator, or a table has no `Symbol` column;
        `TypeError` if `symbols` is of another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, a Deephaven table with a `Symbol` column, or `dm.ALL`.

        Returns:
            A query with `history()` and the stream verbs.

        Example:
            ::

                book = m.options.quotes(["O:AAPL250117C00150000"]).last_by()
        """
        return cast("StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols))

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
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed options contracts.

        One row per contract and bar: the bar's window, from `TimestampStart` to the exclusive
        `TimestampEnd`, its trading day `Date` (ISO `yyyy-MM-dd` text, New York time), its
        prices and volume, and `TimestampInsert`, when the library received it. The timespan and
        multiplier decide the transports and so the query class:

        - `Timespan.SECOND` or `Timespan.MINUTE` with `multiplier=1`: the WebSocket `A` (one
          second) or `AM` (one minute) channel, plus `history()`.
        - Anything else: `history()` only. Each contract's bar of the previous completed
          session is `previous_day_bar()`.

        `history()` reads the REST aggregates endpoint
        (`/v2/aggs/ticker/{optionsTicker}/range/...`, one request series per contract) or, for
        one-minute and one-day bars with `adjusted=False`, the `us_options_opra/minute_aggs_v1`
        and `us_options_opra/day_aggs_v1` flat files. There is no `limit`: `history()` pages
        internally and returns the whole range. It returns only the bars whose whole window
        lies inside the range and has ended, judged by `TimestampEnd`, so the current day's bar
        never appears.

        `TimestampEnd` is the first instant at which the bar is complete, so join on it
        (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only closed
        bars; it is not when Massive published the bar. An intraday bar ends its length
        after its start. A bar of a day or longer runs from 00:00 New York time of its
        window's first day to 00:00 after its last, whatever the source: the range and the
        day files stamp a 1-day bar at the day's start. A week runs Monday to Friday and a
        longer period from its first to its last weekday. A bar of several days counts
        trading days, ending as late as holidays could push it (one weekday more, and one
        more for every five further days), since there is no holiday calendar. A longer bar
        must be stamped at 00:00 on the day its period starts: a week on its Monday or the
        Saturday or Sunday before, a month, quarter, or year on its 1st. Any other stamp
        fails the call with `MassiveError` naming the source, since the bar may cover other
        days than its label. `Date` is null for a bar that spans more than one date.

        Prices and volumes are adjusted for splits of the underlying only when `adjusted` is
        True, the default, and only REST serves adjusted bars; Massive never adjusts for cash
        dividends. The flat files and the WebSocket are as traded, so `history()` reads the
        flat files only with `adjusted=False`, and one `history()` never mixes the two bases:
        every page's own `adjusted` must match the request, or the call raises `MassiveError`.
        Live bars are never adjusted after the fact, so a live table shows a jump at a split.

        `dm.ALL` streams every contract and reads every contract of a day's flat file; REST
        `history()` takes explicit contracts and raises `ValueError` for `dm.ALL`. One
        `/options` connection carries at most 1,000 contract topics.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#bars,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if `timespan` is `Timespan.SESSION`,
        `multiplier` is not positive, a symbol is not an OCC option symbol, a sequence is empty or
        an exhausted generator, or a table has no `Symbol` column; `TypeError` if `timespan` is not
        a `dm.Timespan`, `multiplier` is not an `int`, `adjusted` is not a `bool`, or `symbols` is
        of another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, a Deephaven table with a `Symbol` column, or `dm.ALL`.
            timespan: The bar unit; `Timespan.MINUTE` by default. `Timespan.SESSION` is for
                futures only.
            multiplier: How many units one bar spans, a positive `int`; 1 by default.
            adjusted: Whether REST bars are adjusted for splits of the underlying; True by
                default. The flat files and the streams are as traded, so `history()` reads the
                flat files only when this is False.

        Returns:
            A `StreamHistoryQuery` or `HistoryQuery`, as listed above.

        Example:
            ::

                minute = m.options.bars("O:AAPL250117C00150000").append_only()
                hourly = m.options.bars(
                    "O:AAPL250117C00150000", dm.Timespan.HOUR, multiplier=4
                ).history("2025-01-02", "2025-01-10")
        """
        return cast(
            "HistoryQuery",
            build_query(
                self._core,
                BARS,
                symbols=symbols,
                timespan=timespan,
                multiplier=multiplier,
                adjusted=adjusted,
            ),
        )

    def previous_day_bar(self, symbols: Symbols, *, adjusted: bool = True) -> Query:
        """The previous trading day's open, high, low, close, and volume of options contracts.

        One row per contract: its 1-day bar of the previous completed session, which Massive
        documents as "the prior session's open, high, low, close, and volume", with the columns of
        its kind that its endpoint fills: its window, from `TimestampStart`, 00:00 New York time of
        the day, to the exclusive `TimestampEnd`, 00:00 of the next day, its trading day `Date` (ISO
        `yyyy-MM-dd` text), its prices and volume, and `TimestampInsert`, when the library received
        it. Served by REST (`/v2/aggs/ticker/{optionsTicker}/prev`) through `fetch()` and `poll()`,
        one request per contract; `poll()` replaces the table each cycle, keyed by `Symbol`, and an
        explicit `mode=dm.PollMode.APPEND` appends each new day's bar. Massive stamps the bar at the
        16:00 session close (13:00 on a short trading day; 16:15 or 13:15 for index options such as
        SPXW), which is moved to the day's start; any other stamp raises `MassiveError` naming
        `bars(symbols, dm.Timespan.DAY).history()` for that day. A contract with no bar (HTTP 404)
        is left out, with a WARNING.

        Prices and volumes are adjusted for splits of the underlying when `adjusted` is True,
        the default, as `bars()` describes, and a page whose own `adjusted` differs from the
        request raises `MassiveError`.

        `fetch()` and `poll()` take no `limit`: each contract gives one row. `dm.ALL` is
        refused, since the endpoint takes one contract per request: use
        `m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day)`, which reads
        every contract's bar of a day from the flat files.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#previous_day_bar,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if `symbols` is `dm.ALL`, a symbol
        is not an OCC option symbol, a sequence is empty or an exhausted generator, or a table
        has no `Symbol` column; `TypeError` if `adjusted` is not a `bool` or `symbols` is of
        another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, or a Deephaven table with a `Symbol` column, re-read each `poll()`
                cycle.
            adjusted: Whether the bar is adjusted for splits of the underlying; True by
                default.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                prev = m.options.previous_day_bar("O:AAPL250117C00150000").fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day)"
            ),
            timespan=Timespan.DAY,
            adjusted=adjusted,
        )
        return cast("Query", query)

    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value of US-listed options contracts, streamed in real time.

        One row per update: the contract's fair market value and the time it was computed
        (`Timestamp`), with its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of
        `Timestamp`), and `TimestampInsert`, when the library received it. It streams only, over
        the WebSocket `FMV` channel, which Massive serves on its Business plans from
        `business.massive.com/options`; there is no history, and so no `limit`: `ring(capacity)`
        bounds how many rows a live table keeps.

        `dm.ALL` streams every contract (the `FMV.*` topic). One `/options` connection carries
        at most 1,000 contract topics.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#fmv,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if a symbol is not an OCC option symbol,
        a sequence is empty or an exhausted generator, or a table has no `Symbol` column;
        `TypeError` if `symbols` is of another type.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, a Deephaven table with a `Symbol` column, or `dm.ALL`.

        Returns:
            A query with the stream verbs.

        Example:
            ::

                fair = m.options.fmv(["O:AAPL250117C00150000"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    def chain(
        self,
        underlying: str,
        *,
        contract_type: ContractType | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The snapshot of every contract in one underlying's option chain.

        One row per contract (`Symbol`) of the underlying (`Underlying`): its contract
        details, the session's prices, the last quote and trade, the greeks, the implied
        volatility, and the open interest. It reads the REST chain snapshot
        (`/v3/snapshot/options/{underlyingAsset}`), paginated internally; `poll()` re-reads
        it each interval.

        `limit` caps the total number of rows across pages (None, the default, returns every
        contract); it is not the page size. The chain is one underlying, so it takes no
        `symbols` and no `dm.ALL`; `m.options.snapshots(dm.ALL)` lists every contract.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#chain,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if `underlying` is not Massive's
        exact stock or index ticker, `limit` is not positive, a filter key is not a parameter
        of the endpoint or is `contract_type`, or a filter value does not fit its parameter;
        `TypeError` if `underlying` or `sort` is not a `str`, `contract_type` is not a
        `dm.ContractType`, `order` is not a `dm.Order`, `limit` is not an `int`, or `filters`
        is not a mapping.

        `Underlying` is the instrument Massive links each contract to, exactly as it sends it;
        it names that instrument, not what the contract delivers. An adjusted contract (a root
        such as `AAPL1` after a merger or a special dividend) may deliver another number of
        shares, cash, or other securities, and is listed here beside the standard ones. The
        chain carries only `DetailsSharesPerContract`; `m.options.contracts` adds
        `SharesPerContract`, `AdditionalUnderlyings`, and `Correction`, so a chain row can be
        kept only when its contract is standard by joining them on `Symbol` (see the example).

        Args:
            underlying: The underlying's exact Massive ticker, such as `"AAPL"`, `"BRK.B"`, or
                `"I:SPX"`; case is kept, and nothing is converted.
            contract_type: Only calls or only puts; None, the default, returns both.
            sort: The field to sort by: `"ticker"`, `"expiration_date"`, or
                `"strike_price"`; None for the endpoint's order.
            order: The sort direction; None for the endpoint's default.
            limit: The most rows to return, a positive `int`; None for every row.
            filters: Further parameters of the endpoint, by wire name with dots written as
                underscores: `strike_price`, `expiration_date`, and their `_gt`, `_gte`,
                `_lt`, and `_lte` forms, such as `{"strike_price_gte": 150}`.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                near = m.options.chain(
                    "AAPL",
                    contract_type=dm.ContractType.CALL,
                    filters={"expiration_date_lte": "2025-01-17"},
                ).fetch()
                # Keep the contracts that deliver 100 shares and nothing else.
                terms = m.options.contracts(underlying="AAPL").fetch()
                standard = near.natural_join(
                    terms, "Symbol", "SharesPerContract, AdditionalUnderlyings"
                ).where(
                    [
                        "SharesPerContract == 100",
                        "isNull(AdditionalUnderlyings) || AdditionalUnderlyings == `[]`",
                    ]
                )
        """
        params: dict[str, object] = {
            "underlyingAsset": _underlying(underlying),
            "contract_type": _contract_type(contract_type),
        }
        params.update(list_params(sort=sort, order=order))
        return cast(
            "Query", build_query(self._core, CHAIN, params=params, filters=filters, limit=limit)
        )

    def snapshots(
        self,
        symbols: Symbols,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The current snapshot of each requested options contract, with its greeks.

        One row per contract (`Symbol`), with its underlying (`Underlying`): the contract
        details, the session's prices, the last quote, trade, and minute bar, the greeks, the
        implied volatility, the open interest, and the fair market value. It reads Massive's
        unified snapshot (`/v3/snapshot`) restricted to options, at most 250 contracts per
        request; `poll()` re-reads it each interval. Massive includes this endpoint from the
        Options Starter plan up, and the last-trade endpoint only from Options Developer,
        so polling the snapshots follows a contract's current state on more plans.

        `dm.ALL` lists every options contract Massive has, paginated: that is millions of rows
        and many requests, so it is slow. `limit` caps the total number of rows across pages
        (None, the default, returns every row); it is not the page size.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#snapshots,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if a symbol is not an OCC option symbol,
        a sequence is empty or an exhausted generator, a table has no `Symbol` column, `limit` is
        not positive, a filter key is not a parameter of the endpoint or is `type` (the library
        sends `type=options`), or a filter value does not fit its parameter; `TypeError` if
        `symbols` is of another type, `sort` is not a `str`, `order` is not a `dm.Order`, `limit` is
        not an `int`, or `filters` is not a mapping.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, a Deephaven table with a `Symbol` column (read again each `poll()`
                cycle), or `dm.ALL`.
            sort: The field to sort by (`"ticker"`); None for the endpoint's order.
            order: The sort direction; None for the endpoint's default.
            limit: The most rows to return, a positive `int`; None for every row.
            filters: Further parameters of the endpoint, by wire name with dots written as
                underscores: the alphabetical range forms `ticker_gt`, `ticker_gte`,
                `ticker_lt`, and `ticker_lte`.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                watch = ["O:AAPL250117C00150000", "O:AAPL250117P00150000"]
                greeks = m.options.snapshots(watch).poll(5)
        """
        params: dict[str, object] = {"type": _SNAPSHOT_TYPE}
        params.update(list_params(sort=sort, order=order))
        return cast(
            "Query",
            build_query(
                self._core, SNAPSHOTS, symbols=symbols, params=params, filters=filters, limit=limit
            ),
        )

    def contracts(
        self,
        *,
        symbols: Symbols | None = None,
        underlying: str | None = None,
        contract_type: ContractType | None = None,
        as_of: DateLike | None = None,
        expired: bool | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Reference data on options contracts: type, exercise style, expiration, and strike.

        One row per contract (`Symbol`), with its underlying (`Underlying`), shares per
        contract, primary exchange, and CFI code. Without `symbols` it lists contracts from the
        REST contracts endpoint (`/v3/reference/options/contracts`), paginated internally and
        narrowed by the other arguments; with `symbols` it reads each contract's own endpoint
        (`/v3/reference/options/contracts/{options_ticker}`), one request per contract, which
        takes only `as_of`: with `symbols`, a verb raises `ValueError` before any request if
        `underlying`, `contract_type`, `expired`, `sort`, `order`, or a filter is set.
        `poll()` re-reads it each interval.

        `limit` caps the total number of rows across pages (None, the default, returns every
        row); it is not the page size. `dm.ALL` is not accepted: leave `symbols` out to list
        every contract.

        The deliverable: `Underlying` is the instrument Massive links the contract to, exactly
        as sent, not what the contract delivers. `SharesPerContract` is the number of shares
        one contract delivers (100 for a standard contract), `AdditionalUnderlyings` the
        JSON text of any further deliverables (other securities or cash, as Massive lists
        them; null or `[]` when there are none), `Cfi` the contract's six-letter ISO 10962
        classification code, and `Correction` Massive's correction number of the contract
        record. An adjusted contract keeps its own root (`AAPL1`) and its
        own `Symbol`; the library never maps it to `AAPL`.

        It raises at once, before any request: `ValueError` if `symbols` is `dm.ALL`, a
        symbol is not an OCC option symbol, a sequence is empty or an exhausted generator, a
        table has no `Symbol` column, `underlying` is not Massive's exact stock or index
        ticker, `as_of` is a `datetime` or not an ISO-8601 date, `limit` is not positive, a
        filter key is not a parameter of the endpoints or is set by a typed argument, or a
        filter value does not fit its parameter;
        `TypeError` if `underlying` or `sort` is not a `str`, `contract_type` is not a
        `dm.ContractType`, `expired` is not a `bool`, `order` is not a `dm.Order`, `limit` is
        not an `int`, or `filters` is not a mapping.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#contracts,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The contracts to describe, as Massive's tickers (`O:` and the OCC symbol); one
                `str`, a sequence, or a Deephaven table with a `Symbol` column. None, the
                default, lists contracts instead.
            underlying: Only contracts linked to this exact Massive ticker, such as `"AAPL"`
                or `"I:SPX"`; None for every underlying.
            contract_type: Only calls or only puts; None for both.
            as_of: The day whose contracts to list, as an ISO-8601 date, a `date`, Deephaven
                date text such as `"2025-3-3"`, a Java `LocalDate`, or a day-precision
                `numpy.datetime64` (a `datetime` or an instant raises `ValueError`: it is a
                calendar date); None for today.
            expired: True for expired contracts only, False for active ones only; None for
                the endpoint's default, active contracts.
            sort: The field to sort by: `"ticker"`, `"underlying_ticker"`,
                `"expiration_date"`, or `"strike_price"`; None for the endpoint's order.
            order: The sort direction; None for the endpoint's default.
            limit: The most rows to return, a positive `int`; None for every row.
            filters: Further parameters of the endpoints, by wire name with dots written as
                underscores: `strike_price`, `expiration_date`, and their `_gt`, `_gte`,
                `_lt`, and `_lte` forms, such as `{"expiration_date_lte": "2026-01-16"}`.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                listed = m.options.contracts(
                    underlying="NVDA",
                    filters={"strike_price_gte": 100, "expiration_date_lte": "2026-01-16"},
                ).fetch()
        """
        params: dict[str, object] = {
            "underlying_ticker": None if underlying is None else _underlying(underlying),
            "contract_type": _contract_type(contract_type),
            "as_of": None if as_of is None else day_argument(CONTRACTS, as_of, "as_of"),
            "expired": _expired(expired),
        }
        params.update(list_params(sort=sort, order=order))
        return cast(
            "Query",
            build_query(
                self._core,
                CONTRACTS,
                symbols=symbols,
                all_alternative="m.options.contracts() without symbols",
                params=params,
                filters=filters,
                limit=limit,
            ),
        )

    def daily_open_close(self, symbols: Symbols, date: DateLike, *, adjusted: bool = True) -> Query:
        """The open, high, low, close, and volume of each options contract on one day.

        One row per contract (`Symbol`) for the day (`Date`, ISO `yyyy-MM-dd` text), with the
        day's window as a 1-day bar's (`TimestampStart` and `TimestampEnd`, the New York
        midnights around it) and its pre-market and after-hours prices. It reads the REST daily
        open/close endpoint (`/v1/open-close/{optionsTicker}/{date}`), one request per contract;
        `poll()` re-reads it each interval.

        It has no `limit`: each contract gives one row. `dm.ALL` is not accepted, because the
        endpoint takes one contract per request; `m.options.snapshots(dm.ALL)` gives every
        contract's session prices.

        The columns, their types, and what each holds are listed in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.md#daily_open_close,
        in the web UI's column tooltips, and by `columns()`.

        It raises at once, before any request: `ValueError` if `symbols` is `dm.ALL`, a symbol is
        not an OCC option symbol, a sequence is empty or an exhausted generator, a table has no
        `Symbol` column, or `date` is a `datetime` or not an ISO-8601 date; `TypeError` if `symbols`
        or `date` is of another type, or `adjusted` is not a `bool`.

        Args:
            symbols: The contracts, as Massive's tickers (`O:` and the OCC symbol); one `str`, a
                sequence, or a Deephaven table with a `Symbol` column.
            date: The trading day, as an ISO-8601 date, a `date`, Deephaven date text such as
                `"2025-3-3"`, a Java `LocalDate`, or a day-precision `numpy.datetime64`. It is a
                calendar date, so a `datetime` or an instant raises `ValueError`.
            adjusted: Whether the prices and volume are adjusted for splits of the underlying
                (never for dividends); True by default. Massive does not echo the basis here,
                so it cannot be checked.

        Returns:
            A query with `fetch()` and `poll()`.

        Example:
            ::

                day = m.options.daily_open_close("O:AAPL250117C00150000", "2025-01-02").fetch()
        """
        return cast(
            "Query",
            build_query(
                self._core,
                DAILY_OPEN_CLOSE,
                symbols=symbols,
                all_alternative="m.options.snapshots(dm.ALL)",
                adjusted=adjusted,
                params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
