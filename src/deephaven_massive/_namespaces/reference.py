"""The `m.reference` namespace: cross-asset reference data.

Datasets: tickers, ticker details, ticker types, ticker events, related companies, news,
exchanges, condition codes, market status, and market holidays. They declare no asset class:
the reference endpoints span every class, and a symbol's prefix selects its class (`X:`
crypto, `C:` forex, `I:` indices, `O:` options, none for stocks).

Each dataset is one factory method on `_Reference` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._datasets.catalog import NO_NAMED_TICKER, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import (
    AssetClass,
    ConditionDataType,
    Locale,
    Market,
    Order,
    PollMode,
    Sip,
    TickerType,
    _require_enum,
)
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    Namespace,
    accepted_values,
    build_query,
    day_argument,
    list_params,
    optional_text,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "_Reference"]


_REST: Final = Transport.REST

TICKERS: Final = DatasetSpec(
    name="reference.tickers",
    summary="Every ticker Massive supports, across asset classes, with its listing details.",
    fetch_path="/v3/reference/tickers",
    keys={(_REST, "/v3/reference/tickers"): SourceKeys(symbol=KeySpec("Ticker"))},
    # `ticker` takes one value, so a symbol list fans out one request per symbol, while
    # no symbols at all is one request that lists every ticker.
    symbol_param="ticker",
)
TICKER_DETAILS: Final = DatasetSpec(
    name="reference.ticker_details",
    summary="One ticker's company and listing details: address, branding, shares, and more.",
    fetch_path="/v3/reference/tickers/{ticker}",
    keys={(_REST, "/v3/reference/tickers/{ticker}"): SourceKeys(symbol=KeySpec("Ticker"))},
    per_symbol=True,
)
TICKER_TYPES: Final = DatasetSpec(
    name="reference.ticker_types",
    summary="The ticker type codes Massive uses, such as `CS` for common stock, with their names.",
    fetch_path="/v3/reference/tickers/types",
    poll_keys=("AssetClass", "Locale", "Code"),
)
TICKER_EVENTS: Final = DatasetSpec(
    name="reference.ticker_events",
    summary="The timeline of a ticker's events, such as ticker changes, one row per event.",
    fetch_path="/vX/reference/tickers/{id}/events",
    keys={
        (_REST, "/vX/reference/tickers/{id}/events"): SourceKeys(
            symbol=KeySpec("id", from_request=True)
        )
    },
    poll_keys=("Symbol", "Date", "Type"),
    per_symbol=True,
)
RELATED_COMPANIES: Final = DatasetSpec(
    name="reference.related_companies",
    summary="The companies Massive relates to a ticker, from news and returns data, one row each.",
    fetch_path="/v1/related-companies/{ticker}",
    keys={
        # The rows name the peer; the company asked about is the path's ticker.
        (_REST, "/v1/related-companies/{ticker}"): SourceKeys(
            symbol=KeySpec("Ticker"), fanout=KeySpec("ticker", from_request=True)
        )
    },
    poll_keys=("Underlying", "Symbol"),
    per_symbol=True,
)
NEWS: Final = DatasetSpec(
    name="reference.news",
    summary="News articles about tickers, with their publishers and sentiment insights.",
    fetch_path="/v2/reference/news",
    time_axis=("published_utc.gte", "published_utc.lt"),
    # `ticker` takes one value, so a watchlist fans out one request per ticker, and an
    # article that mentions two of its tickers comes back once for each, under each.
    symbol_param="ticker",
    ticker_list="Tickers",
    poll_mode=PollMode.APPEND,
    poll_keys=("Id", "Symbol"),
    nullable_poll_keys={"Symbol": NO_NAMED_TICKER},
)
EXCHANGES: Final = DatasetSpec(
    name="reference.exchanges",
    summary="The exchanges, trade reporting facilities, and SIPs Massive knows, with their codes.",
    fetch_path="/v3/reference/exchanges",
    poll_keys=("AssetClass", "Id"),
)
_UPDATE_RULES: Final = tuple(
    f"UpdateRules{scope}Updates{what}"
    for scope in ("Consolidated", "MarketCenter")
    for what in ("HighLow", "OpenClose", "Volume")
)
"""The condition columns that say whether a trade with the condition updates a bar's values."""
CONDITIONS: Final = DatasetSpec(
    name="reference.conditions",
    summary="The trade and quote condition codes, with what each means and how it updates bars.",
    fetch_path="/v3/reference/conditions",
    # Massive lists two different conditions as stocks `quote_condition` 30, and no column is
    # known to tell them apart, so a row is identified by every column Massive sends: a keyed
    # poll() table keeps every distinct condition and merges only exact repeats.
    poll_keys=(
        "AssetClass",
        "Type",
        "Id",
        "Name",
        "DataTypes",
        "Abbreviation",
        "Description",
        "Exchange",
        "Legacy",
        "SipMappingCta",
        "SipMappingOpra",
        "SipMappingUtp",
        "SipMappingFinraTdds",
        *_UPDATE_RULES,
    ),
    nullable_poll_keys={
        **dict.fromkeys(
            ("Abbreviation", "Description", "Legacy", *_UPDATE_RULES),
            "Massive marks the field optional",
        ),
        "Exchange": "present only for a condition whose data comes from one exchange alone",
        **dict.fromkeys(
            ("SipMappingCta", "SipMappingOpra", "SipMappingUtp", "SipMappingFinraTdds"),
            "empty for a SIP that has no code for the condition",
        ),
    },
)
MARKET_STATUS: Final = DatasetSpec(
    name="reference.market_status",
    summary="Whether the markets are open now, by exchange, currency market, and index group.",
    fetch_path="/v1/marketstatus/now",
    # One row and no key: a `poll()` table keeps the latest status, never a row per
    # `ServerTime`, which changes on every call. An explicit APPEND poll appends one row per
    # new `ServerTime`, a history of the status.
    poll_keys=(),
    revision_keys=("ServerTime",),
)
MARKET_HOLIDAYS: Final = DatasetSpec(
    name="reference.market_holidays",
    summary="The upcoming market holidays and early closes, one row per exchange and day.",
    fetch_path="/v1/marketstatus/upcoming",
    poll_keys=("Exchange", "Date"),
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TICKERS,
    TICKER_DETAILS,
    TICKER_TYPES,
    TICKER_EVENTS,
    RELATED_COMPANIES,
    NEWS,
    EXCHANGES,
    CONDITIONS,
    MARKET_STATUS,
    MARKET_HOLIDAYS,
)
"""Every dataset of `m.reference`, one `DatasetSpec` each (for example `reference.news`)."""


_REST_ASSET_CLASS: Final = {
    member: "fx" if member is AssetClass.FOREX else member.value for member in AssetClass
}
"""The `asset_class` value of each class on the reference endpoints, which spell forex `fx`."""


@raises_through(_require_enum)
def _asset_class(core: SessionCore, spec: DatasetSpec, value: object) -> str | None:
    """Translate an `asset_class` argument to the wire value of the dataset's endpoint.

    The reference endpoints spell forex `fx`, and each accepts its own set of classes, which
    the generated `asset_class` parameter of the path lists: the exchanges accept futures but
    not indices, the ticker types indices but not futures, and the conditions neither.

    Args:
        core: The session core, whose catalog holds the generated paths.
        spec: The dataset; its fetch path has an `asset_class` enum parameter.
        value: The argument as passed, or None.

    Returns:
        The wire value, or None when `value` is None.

    Raises:
        TypeError: If `value` is neither None nor a `dm.AssetClass` (`_require_enum`).
        ValueError: If the endpoint does not accept the class; the message lists those it does.
    """
    if value is None:
        return None
    member = _require_enum(value, AssetClass, "asset_class")
    accepted = accepted_values(core, spec, "asset_class")
    if _REST_ASSET_CLASS[member] not in accepted:
        names = [m.name for m in AssetClass if _REST_ASSET_CLASS[m] in accepted]
        raise ValueError(
            f"{spec.name} does not accept AssetClass.{member.name}; it accepts {names}"
        )
    return _REST_ASSET_CLASS[member]


def _event_types(types: object) -> str | None:
    """Join the `types` argument of `ticker_events` into its comma-separated wire form.

    Args:
        types: None, one event type as a `str`, or a sequence of them.

    Returns:
        The comma-separated types, or None for every type.

    Raises:
        TypeError: If `types` is not a `str` or an iterable of `str`.
        ValueError: If the sequence is empty: pass None for every type.
    """
    if types is None or isinstance(types, str):
        return types
    try:
        items = tuple(cast("Sequence[object]", types))
    except TypeError:
        raise TypeError(f"types must be a sequence of str, not {type(types).__name__}") from None
    if not items:
        raise ValueError("types is empty; pass None for every event type")
    if not all(isinstance(item, str) for item in items):
        raise TypeError("types must hold only str")
    return ",".join(cast("tuple[str, ...]", items))


class _Reference(Namespace):
    """`m.reference`: the cross-asset reference data datasets of a session.

    Created once by `MassiveSession` and reached as `m.reference`. It holds no state of its
    own, so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            details = m.reference.ticker_details(["AAPL", "MSFT"]).fetch()
            news = m.reference.news("AAPL", start="2025-06-01").poll(60)
    """

    __slots__ = ()
    _path: ClassVar[str] = "reference"

    @raises_through(build_query, list_params, day_argument, _require_enum, optional_text)
    def tickers(
        self,
        symbols: Symbols | None = None,
        *,
        market: Market | None = None,
        ticker_type: TickerType | None = None,
        exchange: str | None = None,
        cusip: str | None = None,
        cik: str | None = None,
        as_of: DateLike | None = None,
        active: bool = True,
        search: str | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Every ticker Massive supports, across asset classes, with its listing details.

        Each row is one ticker. `poll()` re-fetches the list, keyed by `Symbol`. The endpoint takes
        one ticker per request, so named tickers are one request each and `limit` then caps each
        ticker's rows; `symbols=None` lists every ticker in one request series.

        Args:
            symbols: Tickers of any classes; None for every ticker (`dm.ALL` is refused). See
                README.md#symbols.
            market: The market to list; None for every market.
            ticker_type: The type, such as `dm.TickerType.CS` (`ticker_types()` lists them),
                sent as the wire parameter `type`; None for every type.
            exchange: The primary exchange's MIC, such as `"XNAS"`; None for every exchange.
            cusip: The CUSIP to look up; None for any.
            cik: The SEC Central Index Key to look up; None for any.
            as_of: The day the listing is as of, sent as `date`; None for today. See
                README.md#as-of.
            active: True (the default) lists tickers traded on `as_of`; False lists delisted ones.
            search: Text matched against the ticker and the company name; None for no search.
            sort: See README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: Per ticker when tickers are named, else in total. See README.md#sort-order-limit.
            filters: See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/reference.tickers.md.

        Raises:
            TypeError: If `active` is not a `bool`, or another argument has the wrong type.
            ValueError: For `dm.ALL`, an invalid symbol, a `datetime` as `as_of`, or an unknown
                filter.

        Example:
            ::

                etfs = m.reference.tickers(ticker_type=dm.TickerType.ETF, limit=100).fetch()
        """
        if not isinstance(active, bool):
            raise TypeError(f"active must be a bool, not {type(active).__name__}")
        params: dict[str, ParamValue | None] = {
            "market": None if market is None else _require_enum(market, Market, "market"),
            "type": None
            if ticker_type is None
            else _require_enum(ticker_type, TickerType, "ticker_type"),
            "exchange": optional_text("exchange", exchange),
            "cusip": optional_text("cusip", cusip),
            "cik": optional_text("cik", cik),
            "date": None if as_of is None else day_argument(TICKERS, as_of, "as_of"),
            "active": active,
            "search": optional_text("search", search),
            **list_params(sort=sort, order=order),
        }
        query = build_query(
            self._core,
            TICKERS,
            symbols=symbols,
            symbols_optional=True,
            all_alternative="leave symbols out for every ticker",
            params=params,
            arguments={"type": "ticker_type", "date": "as_of"},
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query, day_argument)
    def ticker_details(self, symbols: Symbols, *, as_of: DateLike | None = None) -> Query:
        """One ticker's company and listing details: address, branding, shares, and more.

        One row per symbol, one request each; a symbol Massive does not know is left out with a
        WARNING. `poll()` re-fetches them, keyed by `Symbol`.

        Args:
            symbols: Tickers of any classes (`dm.ALL` is refused: `tickers()` lists every
                ticker). See README.md#symbols.
            as_of: The day the details are as of, sent as `date`; None for the latest. See
                README.md#as-of.

        Returns:
            A `Query`. Columns: docs/datasets/reference.ticker_details.md.

        Raises:
            TypeError: For an argument of the wrong type.
            ValueError: For `dm.ALL`, an invalid symbol, or a `datetime` as `as_of`. Also for
                `symbols` of None, with a message that names the forms it takes.

        Example:
            ::

                details = m.reference.ticker_details(["AAPL", "MSFT"]).fetch()
        """
        query = build_query(
            self._core,
            TICKER_DETAILS,
            symbols=symbols,
            all_alternative="use m.reference.tickers() for every ticker",
            params={
                "date": None if as_of is None else day_argument(TICKER_DETAILS, as_of, "as_of")
            },
            arguments={"date": "as_of"},
        )
        return cast("Query", query)

    @raises_through(build_query, _require_enum, _asset_class)
    def ticker_types(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> Query:
        """The ticker type codes Massive uses, such as `CS` for common stock, with their names.

        One row per type; `poll()` re-fetches them, keyed by `AssetClass`, `Locale`, and `Code`.

        Args:
            asset_class: The class whose types to list (forex is sent as `fx`); None for every
                class. The endpoint has no futures types.
            locale: The locale whose types to list; None for both.

        Returns:
            A `Query`. Columns: docs/datasets/reference.ticker_types.md.

        Raises:
            TypeError: For an argument of the wrong type.
            ValueError: For an `asset_class` the endpoint does not accept (`FUTURES`).

        Example:
            ::

                types = m.reference.ticker_types(asset_class=dm.AssetClass.STOCKS).fetch()
        """
        params = {
            "asset_class": _asset_class(self._core, TICKER_TYPES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return cast("Query", build_query(self._core, TICKER_TYPES, params=params))

    @raises_through(build_query, _event_types)
    def ticker_events(self, symbols: Symbols, *, types: Sequence[str] | None = None) -> Query:
        """The timeline of a ticker's events, such as ticker changes, one row per event.

        One request per symbol; the symbol may also be a CUSIP or a Composite FIGI, and `Symbol` is
        the identifier asked about. `poll()` re-fetches them, keyed by `Symbol`, `Date`, and `Type`.

        Args:
            symbols: Tickers of any classes (`dm.ALL` is refused). See README.md#symbols.
            types: The event types to include, such as `["ticker_change"]` (one `str` is one
                type); None for every type.

        Returns:
            A `Query`. Columns: docs/datasets/reference.ticker_events.md.

        Raises:
            TypeError: For an argument of the wrong type.
            ValueError: For `dm.ALL`, an invalid symbol, or an empty `types`. Also for `symbols` of
                None, with a message that names the forms it takes.

        Example:
            ::

                events = m.reference.ticker_events("META").fetch()
        """
        query = build_query(
            self._core,
            TICKER_EVENTS,
            symbols=symbols,
            all_alternative="pass a list of symbols",
            params={"types": _event_types(types)},
        )
        return cast("Query", query)

    @raises_through(build_query)
    def related_companies(self, symbols: Symbols) -> Query:
        """The companies Massive relates to a ticker, from news and returns data, one row each.

        One request per symbol. Each row names one peer: `Symbol` is the related company and
        `Underlying` the ticker asked about. `poll()` re-fetches them, keyed by `Underlying` and
        `Symbol`.

        Args:
            symbols: The tickers to find peers of (`dm.ALL` is refused). See README.md#symbols.

        Returns:
            A `Query`. Columns: docs/datasets/reference.related_companies.md.

        Raises:
            TypeError: For an argument of the wrong type.
            ValueError: For `dm.ALL` or an invalid symbol. Also for `symbols` of None, with a
                message that names the forms it takes.

        Example:
            ::

                peers = m.reference.related_companies("AAPL").fetch()
        """
        query = build_query(
            self._core, RELATED_COMPANIES, symbols=symbols, all_alternative="pass a list of symbols"
        )
        return cast("Query", query)

    @raises_through(build_query, list_params)
    def news(
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
        """News articles about tickers, with their publishers and sentiment insights.

        One row per article. An article names several tickers, in its `Tickers` array, and is listed
        under each ticker it was read for, in `Symbol` (README.md#symbols). The endpoint takes one
        ticker per request, so each ticker is its own request, `limit`, `sort`, and `order` apply to
        each ticker's request (newest first by default). `poll()` appends new articles, keyed by
        `Id` and `Symbol`, and needs `start` or `limit`; it re-reads a watchlist table every cycle,
        and a fixed `start` widens the window of a poll that runs for days.

        Args:
            symbols: The tickers whose articles to read; None for every article (`dm.ALL` is
                refused). See README.md#symbols.
            start: The earliest publication. See README.md#start-and-end.
            end: The end of the range. See README.md#start-and-end.
            sort: Each ticker's sort field, `"published_utc"` by default. See
                README.md#sort-order-limit.
            order: Each ticker's order, newest first by default. See README.md#sort-order-limit.
            limit: Articles per ticker, or in total without `symbols`. See
                README.md#sort-order-limit.
            filters: See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/reference.news.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: For `dm.ALL`, an empty list, an invalid symbol, a naive `datetime`, a
                range that ends before it starts, or an unknown filter.

        Example:
            ::

                headlines = m.reference.news(["AAPL", "MSFT"], limit=50).poll(60)
        """
        query = build_query(
            self._core,
            NEWS,
            symbols=symbols,
            all_alternative="leave symbols out for every article",
            start=start,
            end=end,
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(build_query, _require_enum, _asset_class)
    def exchanges(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> Query:
        """The exchanges, trade reporting facilities, and SIPs Massive knows, with their codes.

        One row per exchange; `poll()` re-fetches them, keyed by `AssetClass` and `Id`.

        Args:
            asset_class: The class whose exchanges to list (forex is sent as `fx`); None for every
                class. The endpoint has no indices exchanges.
            locale: The locale whose exchanges to list; None for both.

        Returns:
            A `Query`. Columns: docs/datasets/reference.exchanges.md.

        Raises:
            TypeError: For an argument of the wrong type.
            ValueError: For an `asset_class` the endpoint does not accept (`INDICES`).

        Example:
            ::

                venues = m.reference.exchanges(asset_class=dm.AssetClass.STOCKS).fetch()
        """
        params = {
            "asset_class": _asset_class(self._core, EXCHANGES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return cast("Query", build_query(self._core, EXCHANGES, params=params))

    @raises_through(build_query, list_params, _require_enum, _asset_class)
    def conditions(
        self,
        *,
        asset_class: AssetClass | None = None,
        data_type: ConditionDataType | None = None,
        condition_id: int | None = None,
        sip: Sip | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The trade and quote condition codes, with what each means and how it updates bars.

        One row per condition. Massive lists two different conditions under one stocks quote id,
        so a `poll()` table is keyed by every column Massive sends and merges only exact repeats.

        Args:
            asset_class: The class whose conditions to list (forex is sent as `fx`); None for every
                class. The endpoint has no futures or indices conditions.
            data_type: The kind of data the conditions apply to; None for every kind.
            condition_id: One condition's id, sent as the wire parameter `id`; None for every
                condition.
            sip: The SIP whose condition mapping to list; None for every SIP.
            sort: See README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/reference.conditions.md.

        Raises:
            TypeError: If `condition_id` is neither None nor an `int`, or another argument has the
                wrong type.
            ValueError: For an `asset_class` the endpoint does not accept, or an unknown filter.

        Example:
            ::

                codes = m.reference.conditions(asset_class=dm.AssetClass.STOCKS).fetch()
        """
        if condition_id is not None and (
            isinstance(condition_id, bool) or not isinstance(condition_id, int)
        ):
            raise TypeError(f"condition_id must be an int, not {type(condition_id).__name__}")
        params: dict[str, ParamValue | None] = {
            "asset_class": _asset_class(self._core, CONDITIONS, asset_class),
            "data_type": None
            if data_type is None
            else _require_enum(data_type, ConditionDataType, "data_type"),
            "id": condition_id,
            "sip": None if sip is None else _require_enum(sip, Sip, "sip"),
            **list_params(sort=sort, order=order),
        }
        query = build_query(
            self._core,
            CONDITIONS,
            params=params,
            arguments={"id": "condition_id"},
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    def market_status(self) -> Query:
        """Whether the markets are open now, by exchange, currency market, and index group.

        One row, with the nested exchange, currency, and index-group maps flattened into columns.
        `poll()` keeps only the latest status in its one row.

        Returns:
            A `Query`. Columns: docs/datasets/reference.market_status.md.

        Example:
            ::

                status = m.reference.market_status().poll(60)
        """
        return cast("Query", build_query(self._core, MARKET_STATUS))

    def market_holidays(self) -> Query:
        """The upcoming market holidays and early closes, one row per exchange and day.

        `poll()` re-fetches them, keyed by `Exchange` and `Date`.

        Returns:
            A `Query`. Columns: docs/datasets/reference.market_holidays.md.

        Example:
            ::

                holidays = m.reference.market_holidays().fetch()
        """
        return cast("Query", build_query(self._core, MARKET_HOLIDAYS))
