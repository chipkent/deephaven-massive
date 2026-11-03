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
contract to as `Underlying`, a canonical ticker: it names the linked instrument, not the
deliverable, so an adjusted contract (root `AAPL1`) may deliver more or less than 100 shares
of it. Neither column is ever mapped to the other. `underlying=` takes the canonical ticker
too, or a table of them in a `Symbol` column (`value_table`), each read as if passed alone.
The chain takes and writes an index as `I:SPX`, but the contracts list takes and writes it bare
(`SPX`, and no contract for `I:SPX`), so the REST transport sends it bare
(`wire_symbols.contracts_underlying`) and the rows' index underlyings get their `I:` back by
their CFI code (`wire_symbols.index_underlyings`).

One `/options` WebSocket connection allows 1,000 contracts. The stream verbs count topics
against that cap and raise `ValueError` before subscribing a static list that would cross it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven.table import Table

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.symbols import (
    UNDERLYING_CLASSES,
    check_symbol,
    is_canonical,
    symbol_class,
)
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
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    Namespace,
    adjustment_notes,
    build_query,
    day_argument,
    list_params,
    require_bool,
    value_table,
)
from deephaven_massive._types import ALL

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
"""The `type` the unified snapshot is sent for `dm.ALL`, so it lists options contracts only.

Tickers never travel with it: Massive answers `type` beside `ticker.any_of` with HTTP 400
("Cannot specify tickers and type."), and each contract's `O:` prefix already names its class.
"""

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

_AGGREGATES_PAGE: Final = 50_000
"""The aggregates range endpoint's largest `limit`, which counts the base aggregates
behind a page. Its specification states this maximum only in prose, so it is typed here."""


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
)

# Each one-row dataset joins the kind of its stream and history dataset, so its rows have the
# same columns; an explicit APPEND poll() records each new row by its time.
LAST_TRADE: Final = DatasetSpec(
    name="options.last_trade",
    summary="The latest trade of each US-listed options contract.",
    asset_class=AssetClass.OPTIONS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    serves_current_data=True,
    refused_alternative="options.snapshots",
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
    page_size=_AGGREGATES_PAGE,
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
    channel_family=ChannelFamily.FMV,
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
    serves_current_data=True,
    keys={(_REST, _CHAIN): _keys("DetailsTicker", fanout="UnderlyingAssetTicker")},
    revision_keys=_SNAPSHOT_REVISION,
)

SNAPSHOTS: Final = DatasetSpec(
    name="options.snapshots",
    summary="The current snapshot of each requested options contract, with its greeks.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_SNAPSHOT,
    serves_current_data=True,
    keys={(_REST, _SNAPSHOT): _keys("Ticker", fanout="UnderlyingAssetTicker")},
    symbol_param="ticker.any_of",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
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
        The ticker, unchanged: case is kept (`KIMpL`), and nothing is converted. The REST
        transport sends an index bare to the contracts list (`contracts_underlying`).

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is the ticker of an option, a currency pair, or a crypto pair,
            or is not canonical; `check_symbol` names the canonical form when it can.
    """
    if not isinstance(value, str):
        kind = "dm.ALL" if value is ALL else type(value).__name__
        raise TypeError(f"underlying must be one ticker, a str, not {kind}")
    if symbol_class(value) not in UNDERLYING_CLASSES:
        raise ValueError(
            f"underlying must be a stock or index ticker such as 'AAPL' or 'I:SPX', not {value!r}"
        )
    return check_symbol(ClassFromPrefix.ANY, value)


@raises_through(_require_enum)
def _contract_type(value: object) -> ContractType | None:
    """Check the optional `contract_type` argument of the chain and the contracts.

    Args:
        value: The argument as passed.

    Returns:
        The member, or None when the argument is None.

    Raises:
        TypeError: If the value is neither None nor a `dm.ContractType` (`_require_enum`).
    """
    return None if value is None else _require_enum(value, ContractType, "contract_type")


def _expired(value: object) -> bool | None:
    """Check the `expired` argument of the contracts and return what is sent.

    `False` is Massive's default, so it is not sent: the request is then the same as one that
    never mentions `expired`, and the per-contract path, which has no such parameter, accepts
    it.

    Args:
        value: The argument as passed.

    Returns:
        True to send `expired=true`; None for False, which sends nothing.

    Raises:
        TypeError: If the value is not a `bool`.
    """
    if not isinstance(value, bool):
        raise TypeError(f"expired must be a bool, not {type(value).__name__}")
    return True if value else None


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
            live = m.options.trades(calls.head(10)).append_only()  # ten current contracts
    """

    __slots__ = ()
    _path: ClassVar[str] = "options"

    @raises_through(build_query)
    def trades(self, symbols: Symbols) -> StreamHistoryQuery:
        """Trades of US-listed options contracts (OPRA), tick by tick.

        One row per trade: price, size, exchange, condition codes, and SIP timestamp. REST
        `history()` takes explicit contracts; `dm.ALL` streams every contract and reads a day's flat
        file. One `/options` connection carries at most 1,000 contract topics.

        Args:
            symbols: Contracts as Massive's tickers, `O:` and the OCC symbol
                (`"O:AAPL250117C00150000"`), or `dm.ALL`. See README.md#symbols.

        Returns:
            A query with `history()` and the stream verbs. Columns: docs/datasets/options.trades.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If a symbol is not an OCC option symbol, `symbols` is None or empty, or a
                table has no `Symbol` column.

        Example:
            ::

                from datetime import UTC, datetime, timedelta

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                hour = m.options.trades(contract).history(datetime.now(UTC) - timedelta(hours=1))
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    @raises_through(build_query)
    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each US-listed options contract.

        One row per contract, one request per contract. The endpoint needs Options Developer or
        above (a lower plan raises `EntitlementError`); `snapshots(symbols)` also carries each
        contract's last trade, from Options Starter up (README.md#plans-and-errors). A contract with
        no trade (HTTP 404) is left out, with a WARNING. `dm.ALL` is refused: use
        `chain(underlying)`. An explicit `poll(mode=dm.PollMode.APPEND)` appends each cycle's trade
        when it is new, so a trade between two cycles is missed; `trades()` streams every one.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol). See
                README.md#symbols.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.last_trade.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If `symbols` is `dm.ALL`, None, or empty, a symbol is not an OCC option
                symbol, or a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                last = m.options.last_trade(contract).fetch()
        """
        query = build_query(
            self._core,
            LAST_TRADE,
            symbols=symbols,
            all_alternative="use m.options.chain(underlying).fetch()",
        )
        return cast("Query", query)

    @raises_through(build_query)
    def quotes(self, symbols: Symbols) -> StreamHistoryQuery:
        """Quotes of US-listed options contracts (OPRA), tick by tick.

        One row per quote: bid and ask prices, sizes, and exchanges, with the SIP timestamp. Massive
        has no last-quote endpoint for options: `snapshots(symbols)` gives each contract's latest
        quote. REST `history()` takes explicit contracts; `dm.ALL` streams every contract and reads
        a day's flat file. One `/options` connection carries at most 1,000 contract topics.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol), or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query with `history()` and the stream verbs. Columns: docs/datasets/options.quotes.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If a symbol is not an OCC option symbol, `symbols` is None or empty, or a
                table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                book = m.options.quotes(contract).last_by()
        """
        return cast("StreamHistoryQuery", build_query(self._core, QUOTES, symbols=symbols))

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
    @raises_through(build_query)
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed options contracts.

        Each row is one contract's bar of `multiplier` times `timespan`. One-second and one-minute
        bars also stream from the WebSocket; every other length is history only. Bar windows,
        stamps, and which bars `history()` returns follow README.md#bars. REST `history()` takes
        explicit contracts; `dm.ALL` reads a day's flat file. One `/options` connection carries at
        most 1,000 contract topics.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol), or `dm.ALL`. See
                README.md#symbols.
            timespan: The bar unit; required. See README.md#timespan-and-multiplier.
            multiplier: Units per bar; only 1 streams.
            adjusted: Adjust for splits of the underlying, served by REST only. See
                README.md#adjusted.

        Returns:
            A query of the class the timespan picks. Columns: docs/datasets/options.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, a symbol is not an OCC
                option symbol, `symbols` is None or empty, or a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                minute = m.options.bars(contract, dm.Timespan.MINUTE).append_only()
        """
        return cast(
            "HistoryQuery",
            build_query(
                self._core,
                BARS,
                symbols=symbols,
                timespan=timespan,
                multiplier=multiplier,
                adjusted=require_bool("adjusted", adjusted),
            ),
        )

    @raises_through(build_query)
    def previous_day_bar(self, symbols: Symbols, *, adjusted: bool = True) -> Query:
        """The previous trading day's open, high, low, close, and volume of options contracts.

        One row per contract: its 1-day bar of the previous completed session, one request per
        contract. A contract with no bar (HTTP 404) is left out, with a WARNING. `dm.ALL` is
        refused: `bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day)` reads every
        contract's bar of a day from the flat files. Stamps follow README.md#bars.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol). See
                README.md#symbols.
            adjusted: Adjust for splits of the underlying. See README.md#adjusted.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.previous_day_bar.md.

        Raises:
            TypeError: If `adjusted` is not a `bool` or `symbols` is of another type.
            ValueError: If `symbols` is `dm.ALL`, None, or empty, a symbol is not an OCC option
                symbol, or a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                prev = m.options.previous_day_bar(contract).fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "use m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day), "
                "every contract's bar of that day"
            ),
            timespan=Timespan.DAY,
            adjusted=require_bool("adjusted", adjusted),
        )
        return cast("Query", query)

    @raises_through(build_query)
    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value of US-listed options contracts, streamed in real time.

        One row per update: the contract's fair market value and when it was computed. It streams
        only, from Massive's Business plans, so it has no history. One `/options` connection carries
        at most 1,000 contract topics.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol), or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query with the stream verbs. Columns: docs/datasets/options.fmv.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If a symbol is not an OCC option symbol, `symbols` is None or empty, or a
                table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                fair = m.options.fmv(contract).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query, list_params, _contract_type, _underlying, value_table)
    def chain(
        self,
        underlying: str | Table,
        *,
        contract_type: ContractType | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The snapshot of every contract in one underlying's option chain.

        One row per contract: its details, the session's prices, the last quote and trade, the
        greeks, the implied volatility, and the open interest. `Underlying` names the instrument
        Massive links the contract to, not what it delivers: an adjusted contract (root `AAPL1`) is
        listed beside the standard ones. To keep standard contracts only, join `contracts()` on
        `Symbol` and keep `SharesPerContract == 100` with a null or `[]` `AdditionalUnderlyings`.
        `snapshots(dm.ALL)` lists every contract of every underlying. A table of underlyings reads
        each one's chain; a refreshing one is followed by `fetch()` and re-read by each `poll()`
        cycle (README.md#option-chain).

        Args:
            underlying: The underlying's exact Massive ticker, such as `"AAPL"`, `"BRK.B"`, or
                `"I:SPX"`; case is kept and nothing is converted. Or a table of them in a
                `Symbol` column, each read as if passed alone.
            contract_type: `dm.ContractType.CALL` or `PUT`; None, the default, for both.
            sort: `"ticker"`, `"expiration_date"`, or `"strike_price"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: `strike_price`, `expiration_date`, and their range forms. See
                README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.chain.md.

        Raises:
            TypeError: If `underlying` is not a `str` or a table, `sort` is not a `str`,
                `contract_type` is not a `dm.ContractType`, `order` is not a `dm.Order`, `limit`
                is not an `int`, or `filters` is not a mapping.
            ValueError: If `underlying` is `dm.ALL`, not Massive's exact stock or index ticker,
                or a table without a `Symbol` column, `limit` is not positive, or a filter key
                or value does not fit the endpoint.

        Example:
            ::

                calls = m.options.chain("AAPL", contract_type=dm.ContractType.CALL).fetch()
        """
        if cast("object", underlying) is ALL:  # the type says str; a console caller may pass it
            # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
            logger.debug("options.chain refused: underlying=dm.ALL")
            raise ValueError(
                "options.chain reads one underlying's chain and does not take dm.ALL; "
                "m.options.snapshots(dm.ALL) lists every contract"
            )
        values = (
            value_table(
                underlying, argument="underlying", parameter="underlyingAsset", underlying=True
            )
            if isinstance(underlying, Table)
            else None
        )
        params: dict[str, object] = {
            "underlyingAsset": None if values is not None else _underlying(underlying),
            "contract_type": _contract_type(contract_type),
        }
        params.update(list_params(sort=sort, order=order))
        query = build_query(
            self._core, CHAIN, params=params, filters=filters, limit=limit, values=values
        )
        return cast("Query", query)

    @raises_through(build_query, list_params)
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

        One row per contract: its details, the session's prices, the last quote, trade, and minute
        bar, the greeks, the implied volatility, the open interest, and the fair market value, at
        most 250 contracts a request. It needs Options Starter or above, while `last_trade()` needs
        Options Developer, so polling the snapshots follows a contract on more plans. `dm.ALL` lists
        every contract Massive has: millions of rows and many requests.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol), or `dm.ALL`. See
                README.md#symbols.
            sort: `"ticker"`. See README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: The ticker range forms `ticker_gt`, `ticker_gte`, `ticker_lt`, and
                `ticker_lte`. See README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.snapshots.md.

        Raises:
            TypeError: If `symbols` is of another type, `sort` is not a `str`, `order` is not a
                `dm.Order`, `limit` is not an `int`, or `filters` is not a mapping.
            ValueError: If a symbol is not an OCC option symbol, `symbols` is None or empty, a table
                has no `Symbol` column, `limit` is not positive, or a filter key or value does not
                fit the endpoint (`type` is refused: the library sends it for `dm.ALL`).

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                greeks = m.options.snapshots(contract).poll(5)
        """
        # Massive refuses `type` beside tickers, so only `dm.ALL` sends it. The key stays when
        # its value is None, which keeps `type` refused as a filter either way.
        listing = symbols is ALL
        logger.debug(
            "options.snapshots: %s, so type=%s is %s",
            "dm.ALL" if listing else "tickers given",
            _SNAPSHOT_TYPE,
            "sent" if listing else "left out",
        )
        params: dict[str, object] = {"type": _SNAPSHOT_TYPE if listing else None}
        params.update(list_params(sort=sort, order=order))
        return cast(
            "Query",
            build_query(
                self._core, SNAPSHOTS, symbols=symbols, params=params, filters=filters, limit=limit
            ),
        )

    @raises_through(
        build_query, list_params, day_argument, _contract_type, _expired, _underlying, value_table
    )
    def contracts(
        self,
        symbols: Symbols | None = None,
        *,
        underlying: str | Table | None = None,
        contract_type: ContractType | None = None,
        as_of: DateLike | None = None,
        expired: bool = False,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Reference data on options contracts: type, exercise style, expiration, and strike.

        One row per contract, with its underlying, shares per contract, primary exchange, and CFI
        code. Without `symbols` it lists contracts, narrowed by the other arguments; with `symbols`
        it reads each contract, one request each, and takes only `as_of` (any other argument raises
        `ValueError` when a verb runs). `SharesPerContract` is 100 for a standard contract and
        `AdditionalUnderlyings` is JSON text of any further deliverables (null or `[]` when none).
        An adjusted contract keeps its own root (`AAPL1`) and `Symbol`.

        Args:
            symbols: Contracts to describe, as Massive's tickers; None, the default, lists
                contracts. `dm.ALL` is refused. See README.md#symbols.
            underlying: Only contracts of this exact Massive ticker, such as `"AAPL"` or `"I:SPX"`;
                the rows' `Underlying` is the same canonical ticker. Or a table of them in a
                `Symbol` column, each listed as if passed alone; a refreshing one is followed
                by `fetch()` and re-read by each `poll()` cycle. See
                README.md#option-chain.
            contract_type: `dm.ContractType.CALL` or `PUT`; None for both.
            as_of: The day whose contracts to list; None for today. See README.md#as-of.
            expired: True lists expired contracts only; False, the default, those still trading.
            sort: `"ticker"`, `"underlying_ticker"`, `"expiration_date"`, or `"strike_price"`.
            order: See README.md#sort-order-limit.
            limit: The most rows in total without `symbols`; per contract with them, so no
                effect there. See README.md#sort-order-limit.
            filters: `strike_price`, `expiration_date`, and their range forms. See
                README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.contracts.md.

        Raises:
            TypeError: If `underlying` is not a `str` or a table, `sort` is not a `str`,
                `contract_type` is not a `dm.ContractType`, `expired` is not a `bool`, `order` is
                not a `dm.Order`, `limit` is not an `int`, or `filters` is not a mapping.
            ValueError: If `symbols` is `dm.ALL`, empty, or a stock or index ticker (the message
                names `underlying=`), a symbol is not an OCC option symbol, a table has no `Symbol`
                column, `underlying` is not Massive's exact ticker, `as_of` is a `datetime` or not
                an ISO date, `limit` is not positive, or a filter key or value does not fit.

        Example:
            ::

                listed = m.options.contracts(underlying="NVDA", expired=True).fetch()
        """
        if (
            isinstance(symbols, str)
            and symbol_class(symbols) in UNDERLYING_CLASSES
            and is_canonical(ClassFromPrefix.ANY, symbols)
        ):
            # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
            logger.debug("options.contracts refused: symbols=%r is an underlying", symbols)
            raise ValueError(
                f"{symbols!r} is an underlying, not an options contract: list its contracts "
                f"with m.options.contracts(underlying={symbols!r})"
            )
        values = (
            value_table(
                underlying, argument="underlying", parameter="underlying_ticker", underlying=True
            )
            if isinstance(underlying, Table)
            else None
        )
        params: dict[str, object] = {
            "underlying_ticker": None
            if underlying is None or values is not None
            else _underlying(underlying),
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
                symbols_optional=True,
                all_alternative="use m.options.contracts() without symbols",
                params=params,
                filters=filters,
                limit=limit,
                values=values,
            ),
        )

    @raises_through(build_query, day_argument)
    def daily_open_close(self, symbols: Symbols, date: DateLike, *, adjusted: bool = True) -> Query:
        """The open, high, low, close, and volume of each options contract on one day.

        One row per contract for the day, with its pre-market and after-hours prices and the day's
        window as a 1-day bar's, one request per contract. `dm.ALL` is refused, since Massive has
        no grouped daily endpoint for options: `bars(dm.ALL, dm.Timespan.DAY,
        adjusted=False).history(day, day)` reads every contract's bar of that day from the flat
        files. Massive does not echo the adjustment basis here, so
        it cannot be checked.

        Args:
            symbols: Contracts as Massive's tickers (`O:` and the OCC symbol). See
                README.md#symbols.
            date: The trading day, a calendar date (a `datetime` raises). See
                README.md#start-and-end.
            adjusted: Adjust for splits of the underlying. See README.md#adjusted.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/options.daily_open_close.md.

        Raises:
            TypeError: If `symbols` or `date` is of another type, or `adjusted` is not a `bool`.
            ValueError: If `symbols` is `dm.ALL`, None, or empty, a symbol is not an OCC option
                symbol, a table has no `Symbol` column, or `date` is a `datetime` or not an ISO
                date.

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
                all_alternative=(
                    "use m.options.bars(dm.ALL, dm.Timespan.DAY, adjusted=False)"
                    ".history(day, day), every contract's bar of that day"
                ),
                adjusted=require_bool("adjusted", adjusted),
                params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
