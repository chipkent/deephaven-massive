"""The `m.reference` namespace: cross-asset reference data.

Datasets: tickers, ticker details, ticker types, ticker events, related companies, news,
exchanges, condition codes, market status, and market holidays. They declare no asset class:
the reference endpoints span every class, and a symbol's prefix selects its class (`X:`
crypto, `C:` forex, `I:` indices, `O:` options, none for stocks).

Each dataset is one factory method on `ReferenceNamespace` that validates its arguments and returns
a `FetchQuery`; nothing touches the network until a verb is called on the query. This module owns
the namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, ClassVar, Final

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
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    accepted_values,
    build_query,
    day_argument,
    list_params,
    optional_text,
)
from deephaven_massive.queries import FetchQuery

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._datasets.symbols import SymbolsLike
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["DATASETS", "ReferenceNamespace"]


_REST: Final = Transport.REST

TICKERS: Final = DatasetSpec(
    name="reference.tickers",
    summary="Tickers of stocks, OTC securities, indices, forex, and crypto, with listing details.",
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
    if not isinstance(types, Iterable):
        raise TypeError(f"types must be a sequence of str, not {type(types).__name__}")
    items = tuple(types)
    if not items:
        raise ValueError("types is empty; pass None for every event type")
    texts = [item for item in items if isinstance(item, str)]
    if len(texts) != len(items):
        raise TypeError("types must hold only str")
    return ",".join(texts)


class ReferenceNamespace(Namespace):
    """The reference datasets across asset classes, reached as `m.reference`.

    Each method returns a query of one dataset, and together they cover tickers and their details,
    types, events, and related companies, news, the exchanges, the trade and quote condition codes,
    the market status, and the market holidays. A method reads nothing, so it never blocks.
    You can call it from any thread.
    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import ReferenceNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            details = m.reference.ticker_details(["AAPL", "MSFT"]).fetch()
            headlines = m.reference.news("AAPL", start="2026-11-01").poll(60)
    """

    __slots__ = ()
    _path: ClassVar[str] = "reference"

    @raises_through(build_query, list_params, day_argument, _require_enum, optional_text)
    def tickers(
        self,
        symbols: SymbolsLike | None = None,
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
    ) -> FetchQuery:
        """Tickers of stocks, OTC securities, indices, forex, and crypto, with listing details.

        Each row holds one ticker's name, market, type, primary exchange, and identifiers. Massive
        lists no options or futures here. With `symbols` left out, the call lists every ticker that
        matches the other arguments.

        Args:
            symbols: Tickers to look up. Leave it out for every ticker; `dm.ALL` is refused.
            market: The market to list, a `dm.Market` member such as `dm.Market.STOCKS`. Leave it
                out for every market.
            ticker_type: The type, a `dm.TickerType` member such as `dm.TickerType.ETF`.
                `ticker_types()` lists them. Leave it out for every type.
            exchange: The primary exchange's MIC, such as `"XNAS"`. Leave it out for every exchange.
            cusip: The CUSIP to look up.
            cik: The SEC Central Index Key to look up.
            as_of: The day the listing is as of. Leave it out for today.
            active: True, the default, lists tickers traded on `as_of`. False lists delisted ones.
            search: Text to match against the ticker and the company name.
            sort: Any of Massive's field names, such as `"name"`.
            order: The direction of `sort`.
            limit: Per ticker when tickers are named, else in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.tickers.md.

        Raises:
            TypeError: If `active` isn't a `bool`, or another argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - `as_of` is a time instead of a date;
                - a filter isn't one the dataset takes, or its value doesn't fit.

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
        return build_query(
            self._core,
            TICKERS,
            query_type=FetchQuery,
            symbols=symbols,
            symbols_optional=True,
            all_alternative="leave symbols out for every ticker",
            params=params,
            arguments={"type": "ticker_type", "date": "as_of"},
            filters=filters,
            limit=limit,
        )

    @raises_through(build_query, day_argument)
    def ticker_details(self, symbols: SymbolsLike, *, as_of: DateLike | None = None) -> FetchQuery:
        """One ticker's company and listing details: address, branding, shares, and more.

        Each row holds one ticker's company details. A ticker Massive doesn't know gets no row, and
        the library logs a WARNING.

        Args:
            symbols: Tickers of any asset class. `dm.ALL` is refused; `tickers()` lists every
                ticker.
            as_of: The day the details are as of. Leave it out for the latest.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.ticker_details.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - `as_of` is a time instead of a date.

        Example:
            ::

                details = m.reference.ticker_details(["AAPL", "MSFT"]).fetch()
        """
        return build_query(
            self._core,
            TICKER_DETAILS,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="use m.reference.tickers() for every ticker",
            params={
                "date": None if as_of is None else day_argument(TICKER_DETAILS, as_of, "as_of")
            },
            arguments={"date": "as_of"},
        )

    @raises_through(build_query, _require_enum, _asset_class)
    def ticker_types(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> FetchQuery:
        """The ticker type codes Massive uses, such as `CS` for common stock, with their names.

        Each row holds one type's code, its description, its asset class, and its locale. The codes
        are the values of `dm.TickerType`, which `tickers()` filters by.

        Args:
            asset_class: The asset class whose types to list. Leave it out for every class. Massive
                lists no futures types.
            locale: The locale whose types to list. Leave it out for both.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.ticker_types.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `asset_class` is `dm.AssetClass.FUTURES`, which the endpoint doesn't
                take.

        Example:
            ::

                types = m.reference.ticker_types(asset_class=dm.AssetClass.STOCKS).fetch()
        """
        params = {
            "asset_class": _asset_class(self._core, TICKER_TYPES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return build_query(self._core, TICKER_TYPES, query_type=FetchQuery, params=params)

    @raises_through(build_query, _event_types)
    def ticker_events(
        self, symbols: SymbolsLike, *, types: Sequence[str] | None = None
    ) -> FetchQuery:
        """The timeline of a ticker's events, such as ticker changes, one row per event.

        Each row holds the event's date and type. A symbol may also be a CUSIP or a Composite FIGI,
        and `Symbol` holds the identifier you asked about.

        Args:
            symbols: Tickers of any asset class, CUSIPs, or Composite FIGIs. `dm.ALL` is refused.
            types: The event types to include, such as `["ticker_change"]`. One `str` is one type.
                Leave it out for every type.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.ticker_events.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - `types` is empty.

        Example:
            ::

                events = m.reference.ticker_events("META").fetch()
        """
        return build_query(
            self._core,
            TICKER_EVENTS,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="pass a list of symbols",
            params={"types": _event_types(types)},
        )

    @raises_through(build_query)
    def related_companies(self, symbols: SymbolsLike) -> FetchQuery:
        """The companies Massive relates to a ticker, from news and returns data, one row each.

        `Symbol` is the related company, and `Underlying` is the ticker you asked about.

        Args:
            symbols: The tickers to find related companies of. `dm.ALL` is refused.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.related_companies.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker.

        Example:
            ::

                peers = m.reference.related_companies("AAPL").fetch()
        """
        return build_query(
            self._core,
            RELATED_COMPANIES,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="pass a list of symbols",
        )

    @raises_through(build_query, list_params)
    def news(
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
        """News articles about tickers, with their publishers and sentiment insights.

        Each row holds one article's headline, publisher, publication time, and Massive's sentiment
        insights. An article lists every ticker it names in `Tickers`. It appears once under each of
        your tickers it names, in `Symbol`. When you name tickers, `sort`, `order`, and `limit`
        apply to each ticker's articles. The default order is newest first.

        Args:
            symbols: The tickers whose articles to read. Leave it out for every article; `dm.ALL` is
                refused.
            start: The earliest publication time.
            end: The latest publication time.
            sort: Each ticker's sort field. `"published_utc"`, the default, is the only one.
            order: Each ticker's direction of `sort`, newest first by default.
            limit: Articles per ticker, or in total without `symbols`.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.news.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date or a time with a time zone;
                - the range ends before it starts;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                headlines = m.reference.news(["AAPL", "MSFT"], limit=50).poll(60)
        """
        return build_query(
            self._core,
            NEWS,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="leave symbols out for every article",
            start=start,
            end=end,
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
        )

    @raises_through(build_query, _require_enum, _asset_class)
    def exchanges(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> FetchQuery:
        """The exchanges, trade reporting facilities, and SIPs Massive knows, with their codes.

        Each row holds one venue's id, name, type, MIC, and asset class. The ids are the exchange
        codes that trades and quotes carry.

        Args:
            asset_class: The asset class whose exchanges to list. Leave it out for every class.
                Massive lists no index exchanges.
            locale: The locale whose exchanges to list. Leave it out for both.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.exchanges.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `asset_class` is `dm.AssetClass.INDICES`, which the endpoint doesn't
                take.

        Example:
            ::

                venues = m.reference.exchanges(asset_class=dm.AssetClass.STOCKS).fetch()
        """
        params = {
            "asset_class": _asset_class(self._core, EXCHANGES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return build_query(self._core, EXCHANGES, query_type=FetchQuery, params=params)

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
    ) -> FetchQuery:
        """The trade and quote condition codes, with what each means and how it updates bars.

        Each row holds one condition's id, name, and the SIP codes it maps to. It also says whether
        a trade with the condition updates a bar's high and low, open and close, and volume. Massive
        lists two different conditions under one stocks quote id, so a `poll()` table keeps every
        distinct row.

        Args:
            asset_class: The asset class whose conditions to list. Leave it out for every class.
                Massive lists no futures or index conditions.
            data_type: The kind of data the conditions apply to. Leave it out for every kind.
            condition_id: One condition's id. Leave it out for every condition.
            sip: The SIP whose condition codes to list. Leave it out for every SIP.
            sort: Any of Massive's field names, such as `"name"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.conditions.md.

        Raises:
            TypeError: If `condition_id` isn't an `int`, or another argument has the wrong type.
            ValueError: If:
                - `asset_class` is one the endpoint doesn't take;
                - a filter isn't one the dataset takes, or its value doesn't fit.

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
        return build_query(
            self._core,
            CONDITIONS,
            query_type=FetchQuery,
            params=params,
            arguments={"id": "condition_id"},
            filters=filters,
            limit=limit,
        )

    def market_status(self) -> FetchQuery:
        """Whether the markets are open now, by exchange, currency market, and index group.

        The table has one row, with a column for each exchange, currency market, and index group. A
        `poll()` table keeps the latest status in that row.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.market_status.md.

        Example:
            ::

                status = m.reference.market_status().poll(60)
        """
        return build_query(
            self._core,
            MARKET_STATUS,
            query_type=FetchQuery,
        )

    def market_holidays(self) -> FetchQuery:
        """The upcoming market holidays and early closes, one row per exchange and day.

        Each row holds the holiday's name and the exchange's status that day, with its open and
        close times on an early close.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.market_holidays.md.

        Example:
            ::

                holidays = m.reference.market_holidays().fetch()
        """
        return build_query(
            self._core,
            MARKET_HOLIDAYS,
            query_type=FetchQuery,
        )
