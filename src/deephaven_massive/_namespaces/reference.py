"""The `m.reference` namespace: cross-asset reference data, and the unified snapshot's record.

Datasets: tickers, ticker details, ticker types, ticker events, related companies, news,
exchanges, condition codes, market status, and market holidays. They declare no asset class:
the reference endpoints span every class, and a symbol's prefix selects its class (`X:`
crypto, `C:` forex, `I:` indices, `O:` options, none for stocks).

The module also declares `reference.snapshots`, the catalog record of the unified snapshot
(`/v3/snapshot`) that `MassiveSession.snapshots()` serves: the session method takes tickers of
any classes in one call, so it hangs off the session rather than a namespace, but its dataset
is reference data like the rest and is named, shown in `m.streams()`, and documented here.

Each dataset is one factory method on `_Reference` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import calendar_day
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
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import Namespace, build_query, list_params

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._massive import RestPath
    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "SNAPSHOTS", "_Reference"]

logger = logging.getLogger(__name__)

_REST: Final = Transport.REST
_LIST_PAGE: Final = 1000
"""The largest page of the tickers, news, and conditions endpoints (their `limit` maximum)."""

TICKERS: Final = DatasetSpec(
    name="reference.tickers",
    summary="Every ticker Massive supports, across asset classes, with its listing details.",
    fetch_path="/v3/reference/tickers",
    keys={(_REST, "/v3/reference/tickers"): SourceKeys(symbol=KeySpec("Ticker"))},
    # `ticker` takes one value, so a symbol list fans out one request per symbol, while
    # no symbols at all is one request that lists every ticker.
    symbol_param="ticker",
    page_size=_LIST_PAGE,
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
    # article that mentions two of its tickers comes back twice.
    symbol_param="ticker",
    symbols_overlap=True,
    poll_mode=PollMode.APPEND,
    poll_keys=("Id",),
    page_size=_LIST_PAGE,
)
EXCHANGES: Final = DatasetSpec(
    name="reference.exchanges",
    summary="The exchanges, trade reporting facilities, and SIPs Massive knows, with their codes.",
    fetch_path="/v3/reference/exchanges",
    poll_keys=("AssetClass", "Id"),
)
CONDITIONS: Final = DatasetSpec(
    name="reference.conditions",
    summary="The trade and quote condition codes, with what each means and how it updates bars.",
    fetch_path="/v3/reference/conditions",
    poll_keys=("AssetClass", "Type", "Id"),
    page_size=_LIST_PAGE,
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
SNAPSHOTS: Final = DatasetSpec(
    name="reference.snapshots",
    summary="The current state of tickers of any asset classes, from Massive's unified snapshot.",
    fetch_path="/v3/snapshot",
    keys={(_REST, "/v3/snapshot"): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    page_size=250,
    symbols_per_request=250,
    # The update times that cover every asset class: an index's own, and a ticker's last quote
    # and trade. An explicit APPEND poll appends each new state of a ticker.
    revision_keys=("LastUpdated", "LastQuoteLastUpdated", "LastTradeLastUpdated"),
)
"""The unified snapshot, served by `MassiveSession.snapshots()`; at most 250 tickers a page."""

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
    SNAPSHOTS,
)
"""Every dataset of `m.reference`, one `DatasetSpec` each (for example `reference.news`), and
`reference.snapshots`, the record of `MassiveSession.snapshots()`."""


_REST_ASSET_CLASS: Final = {
    member: "fx" if member is AssetClass.FOREX else member.value for member in AssetClass
}
"""The `asset_class` value of each class on the reference endpoints, which spell forex `fx`."""


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
        ValueError: If the endpoint does not accept the class; the message lists those it does.

    A `value` that is not a `dm.AssetClass` raises the `TypeError` of `_require_enum`.
    """
    if value is None:
        return None
    member = _require_enum(value, AssetClass, "asset_class")
    path = cast("RestPath", core.catalog.source(_REST, cast("str", spec.fetch_path)))
    accepted = next(param.values for param in path.params if param.name == "asset_class")
    if _REST_ASSET_CLASS[member] not in accepted:
        names = [m.name for m in AssetClass if _REST_ASSET_CLASS[m] in accepted]
        raise ValueError(
            f"{spec.name} does not accept AssetClass.{member.name}; it accepts {names}"
        )
    return _REST_ASSET_CLASS[member]


def _text(name: str, value: object) -> str | None:
    """Check an optional text argument.

    Args:
        name: The parameter's name, for the message.
        value: The argument as passed.

    Returns:
        The text, or None.

    Raises:
        TypeError: If `value` is neither None nor a `str`.
    """
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a str, not {type(value).__name__}")
    return value


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
            news = m.reference.news(symbols="AAPL", start="2025-06-01").poll(60)
    """

    __slots__ = ()
    _path: ClassVar[str] = "reference"

    def tickers(
        self,
        *,
        symbols: Symbols | None = None,
        market: Market | None = None,
        ticker_type: TickerType | None = None,
        exchange: str | None = None,
        cusip: str | None = None,
        cik: str | None = None,
        date: DateLike | None = None,
        active: bool = True,
        search: str | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Every ticker Massive supports, across asset classes, with its listing details.

        Served over REST (`/v3/reference/tickers`); `fetch()` returns every matching ticker
        and `poll()` re-fetches them, keyed by `Symbol`. The endpoint takes one ticker per
        request, so a symbol list fans out to one request per symbol; `symbols=None` lists
        every ticker, and `dm.ALL` is not accepted (use `symbols=None`). Its columns, and what
        each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#tickers,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers to look up, of any asset classes, or a Deephaven table with a
                `Symbol` column; None for every ticker.
            market: The market to list; None for every market.
            ticker_type: The ticker type, such as `dm.TickerType.CS` for common stock
                (`ticker_types()` lists them with descriptions), sent as the wire
                parameter `type`; None for every type.
            exchange: The primary exchange's MIC, such as `"XNAS"`; None for every exchange.
            cusip: The CUSIP to look up; None for any.
            cik: The SEC Central Index Key to look up; None for any.
            date: The day the listing is as of: a `date` or ISO-8601 date text (a `datetime`
                raises `ValueError`: it is a calendar date); None for today.
            active: True lists tickers traded on `date`; False lists delisted ones.
            search: Text matched against the ticker and the company name; None for no search.
            sort: The field to sort by, such as `"ticker"`; None for the endpoint's default.
            order: The sort direction; None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores, such as `{"ticker_gte": "A", "ticker_lt": "B"}`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        Raises:
            TypeError: If `active` is not a `bool`.

        `dm.ALL`, an invalid symbol, a `datetime` as `date`, or an unknown filter raises
        `ValueError`; another argument of the wrong type raises `TypeError`.
        """
        if not isinstance(active, bool):
            raise TypeError(f"active must be a bool, not {type(active).__name__}")
        params: dict[str, ParamValue | None] = {
            "market": None if market is None else _require_enum(market, Market, "market"),
            "type": None
            if ticker_type is None
            else _require_enum(ticker_type, TickerType, "ticker_type"),
            "exchange": _text("exchange", exchange),
            "cusip": _text("cusip", cusip),
            "cik": _text("cik", cik),
            "date": None if date is None else calendar_day(TICKERS, date, "date"),
            "active": active,
            "search": _text("search", search),
            **list_params(sort=sort, order=order),
        }
        query = build_query(
            self._core,
            TICKERS,
            symbols=symbols,
            all_alternative="symbols=None for every ticker",
            params=params,
            arguments={"type": "ticker_type"},
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    def ticker_details(self, symbols: Symbols, *, date: DateLike | None = None) -> Query:
        """One ticker's company and listing details: address, branding, shares, and more.

        Served over REST (`/v3/reference/tickers/{ticker}`), one request per symbol; a symbol
        Massive does not know is dropped with a WARNING. `fetch()` returns one row per symbol
        and `poll()` re-fetches them, keyed by `Symbol`. `dm.ALL` is not accepted (use
        `tickers()` to list every ticker). Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#ticker_details,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes, or a Deephaven table with a `Symbol`
                column.
            date: The day the details are as of: a `date` or ISO-8601 date text (a
                `datetime` raises `ValueError`: it is a calendar date); None for the latest.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, an invalid symbol, or a `datetime` as `date` raises `ValueError`; an argument of
        the wrong type raises `TypeError`.
        """
        query = build_query(
            self._core,
            TICKER_DETAILS,
            symbols=symbols,
            all_alternative="m.reference.tickers() for every ticker",
            params={"date": None if date is None else calendar_day(TICKER_DETAILS, date, "date")},
        )
        return cast("Query", query)

    def ticker_types(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> Query:
        """The ticker type codes Massive uses, such as `CS` for common stock, with their names.

        Served over REST (`/v3/reference/tickers/types`); `fetch()` returns every type and
        `poll()` re-fetches them, keyed by `AssetClass`, `Locale`, and `Code`. The dataset
        takes no symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#ticker_types,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            asset_class: The class whose types to list (forex is sent as `fx`); None for
                every class. The endpoint has no futures types.
            locale: The locale whose types to list; None for both.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        An `asset_class` the endpoint does not accept (`FUTURES`) raises `ValueError`; an
        argument of the wrong type raises `TypeError`.
        """
        params = {
            "asset_class": _asset_class(self._core, TICKER_TYPES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return cast("Query", build_query(self._core, TICKER_TYPES, params=params))

    def ticker_events(self, symbols: Symbols, *, types: Sequence[str] | None = None) -> Query:
        """The timeline of a ticker's events, such as ticker changes, one row per event.

        Served over REST (`/vX/reference/tickers/{id}/events`), one request per symbol; the
        symbol may also be a CUSIP or a Composite FIGI, and `Symbol` is the identifier asked
        about. `fetch()` returns every event and `poll()` re-fetches them, keyed by `Symbol`,
        `Date`, and `Type`. `dm.ALL` is not accepted: the endpoint takes one ticker per
        request. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#ticker_events,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes, or a Deephaven table with a `Symbol`
                column.
            types: The event types to include, such as `["ticker_change"]` (one `str` is one
                type); None for every type.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, an invalid symbol, or an empty `types` raises `ValueError`; an argument of
        the wrong type raises `TypeError`.
        """
        query = build_query(
            self._core,
            TICKER_EVENTS,
            symbols=symbols,
            all_alternative="a list of symbols",
            params={"types": _event_types(types)},
        )
        return cast("Query", query)

    def related_companies(self, symbols: Symbols) -> Query:
        """The companies Massive relates to a ticker, from news and returns data, one row each.

        Served over REST (`/v1/related-companies/{ticker}`), one request per symbol. Each row
        names one peer: `Symbol` is the related company and `Underlying` the ticker asked
        about. `fetch()` returns every peer and `poll()` re-fetches them, keyed by
        `Underlying` and `Symbol`. `dm.ALL` is not accepted: the endpoint takes one ticker per
        request. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#related_companies,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers to find peers of, or a Deephaven table with a `Symbol` column.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL` or an invalid symbol raises `ValueError`; an argument of the wrong type
        raises `TypeError`.
        """
        query = build_query(
            self._core, RELATED_COMPANIES, symbols=symbols, all_alternative="a list of symbols"
        )
        return cast("Query", query)

    def news(
        self,
        *,
        symbols: Symbols | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """News articles about tickers, with their publishers and sentiment insights.

        Served over REST (`/v2/reference/news`); `fetch()` returns every article in range and
        `poll()` appends the articles each cycle returns. Each cycle re-reads the whole range,
        so pass `start`: a poll without it re-reads the news archive every cycle. An article
        lists several tickers, so the table has no `Symbol`: it keeps the article's `Tickers`
        (JSON text), and `symbols` selects the articles that mention any of its tickers.

        The endpoint takes one ticker per request, so the library sends one request per
        ticker and keeps an article that mentions several of them once, by its `Id`. Rows
        come ticker by ticker, in ticker order. `limit`, `sort`, and `order` apply to each
        ticker's request, not to the whole table: `limit=10` reads up to ten articles per
        ticker, and `order` sorts within each ticker's articles; sort the table itself for one
        order. A watchlist table is followed: `fetch()` reads its rows once, and `poll()`
        re-reads them every cycle, so a ticker added to the table is read from the next cycle.
        Each ticker costs one request a cycle or more: on a plan limited to 5 requests a
        minute, `poll(60)` of a 50-ticker watchlist cannot keep up, and the table logs the
        slow-poll WARNING. `symbols=None` reads every article in one request series;
        `dm.ALL` is not accepted (use `symbols=None`). Its columns, and what each means, are
        in https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#news,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers whose articles to read: one symbol, a list, or a Deephaven
                table with a `Symbol` column (a watchlist); None for every article.
            start: The earliest publication: a date is its 00:00 in New York, a `datetime`
                is used as is; None for no lower bound, which reads the whole archive.
            end: The end of the range, of the same kind as `start` (mixing a date with a
                `datetime` raises `ValueError`): a date includes that whole day, a
                `datetime` is exclusive; None for no upper bound.
            sort: The field each ticker's request sorts by (`"published_utc"`); None for the
                endpoint's default.
            order: The sort direction of each ticker's request; None for the endpoint's
                default.
            limit: The maximum number of articles per ticker, or in total without `symbols`;
                None for every article. The page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the publication axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, an empty list, an invalid symbol, a naive `datetime`, a range that ends
        before it starts, or an unknown filter raises `ValueError`; an argument of the wrong
        type raises `TypeError`.
        """
        query = build_query(
            self._core,
            NEWS,
            symbols=symbols,
            all_alternative="symbols=None for every article",
            start=start,
            end=end,
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    def exchanges(
        self, *, asset_class: AssetClass | None = None, locale: Locale | None = None
    ) -> Query:
        """The exchanges, trade reporting facilities, and SIPs Massive knows, with their codes.

        Served over REST (`/v3/reference/exchanges`); `fetch()` returns every exchange and
        `poll()` re-fetches them, keyed by `AssetClass` and `Id`. The dataset takes no
        symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#exchanges,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            asset_class: The class whose exchanges to list (forex is sent as `fx`); None for
                every class. The endpoint has no indices exchanges.
            locale: The locale whose exchanges to list; None for both.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        An `asset_class` the endpoint does not accept (`INDICES`) raises `ValueError`; an
        argument of the wrong type raises `TypeError`.
        """
        params = {
            "asset_class": _asset_class(self._core, EXCHANGES, asset_class),
            "locale": None if locale is None else _require_enum(locale, Locale, "locale"),
        }
        return cast("Query", build_query(self._core, EXCHANGES, params=params))

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

        Served over REST (`/v3/reference/conditions`); `fetch()` returns every condition and
        `poll()` re-fetches them, keyed by `AssetClass`, `Type`, and `Id`. The dataset takes no
        symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#conditions,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            asset_class: The class whose conditions to list (forex is sent as `fx`); None for
                every class. The endpoint has no futures or indices conditions.
            data_type: The kind of data the conditions apply to; None for every kind.
            condition_id: One condition's id, sent as the wire parameter `id`; None for
                every condition.
            sip: The SIP whose condition mapping to list; None for every SIP.
            sort: The field to sort by, such as `"id"`; None for the endpoint's default.
            order: The sort direction; None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        Raises:
            TypeError: If `condition_id` is neither None nor an `int`.

        An `asset_class` the endpoint does not accept or an unknown filter raises
        `ValueError`; another argument of the wrong type raises `TypeError`.
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

        One row, with the nested exchange, currency, and index-group maps flattened into
        columns. Served over REST (`/v1/marketstatus/now`); `fetch()` returns the current
        status and `poll()` keeps only the latest in one row (`REPLACE`, `UPSERT`). It takes no
        symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#market_status,
        in the web UI's column tooltips, and in the query's `columns()`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.
        """
        return cast("Query", build_query(self._core, MARKET_STATUS))

    def market_holidays(self) -> Query:
        """The upcoming market holidays and early closes, one row per exchange and day.

        Served over REST (`/v1/marketstatus/upcoming`); `fetch()` returns every upcoming
        holiday and `poll()` re-fetches them, keyed by `Exchange` and `Date`. The dataset takes
        no symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/reference.md#market_holidays,
        in the web UI's column tooltips, and in the query's `columns()`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.
        """
        return cast("Query", build_query(self._core, MARKET_HOLIDAYS))
