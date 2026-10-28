"""The `m.partners` namespace: datasets from Massive's data partners.

Partner datasets stay nested by vendor, one class each:

- `m.partners.benzinga` (`_Benzinga`): analysts, analyst insights, ratings, consensus
  ratings, bulls-and-bears summaries, guidance, earnings, firms, and news;
- `m.partners.etf_global` (`_EtfGlobal`): ETF analytics, constituents, fund flows, profiles,
  and taxonomies;
- `m.partners.tmx` (`_Tmx`): corporate events;
- `m.partners.fable` (`_Fable`): European consumer-spending merchant aggregates and hierarchy.

Every partner dataset is a REST list endpoint, so every factory returns a `Query`: `fetch()`
reads every page once into a static table, and `poll()` re-reads it on an interval into a live
one. Nothing streams over the WebSocket and nothing has flat files. Each factory validates its
arguments synchronously and touches no network; the request runs only when a verb is called.

This module owns the partners' `DatasetSpec`s, exported as `DATASETS` and composed into the
dataset catalog (`CATALOG` in `_session.py`). A spec names its REST path and the facts the
generated wire schemas cannot give: which column holds the row's instrument (`Symbol`) and, for
news, its time (`Timestamp`), the wire parameters behind `symbols`, `start`, and `end`, the
keys of a keyed `poll()` table, and the page size, which is each endpoint's documented maximum.
Columns, types, and descriptions come from the generated schemas, never from this module.

Instruments. The Benzinga datasets that take tickers, the ETF Global datasets, and the TMX
events take US-style stock symbols (`AAPL`, `brk.b`), so their specs carry
`AssetClass.STOCKS` and a symbol with another class's prefix (`X:BTCUSD`) is rejected when the
query is built. Their rows each describe one instrument, which becomes the `Symbol` column (an
ETF Global row's ETF, its `composite_ticker`; for consensus ratings, the symbol requested; for
a TMX event of a company without a ticker, null). Benzinga news names several tickers per article,
so it has no `Symbol`, keeps its `Tickers` column (JSON text), and uses `symbols` only to filter
the request. Benzinga analysts and firms, and both Fable datasets, take no symbols at all.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, PollMode
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import Namespace, build_query, list_params

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import Query
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "_Benzinga", "_EtfGlobal", "_Fable", "_Partners", "_Tmx"]

logger = logging.getLogger(__name__)

_REST = Transport.REST

_NO_TICKER_FILTER: Final = "symbols=None, which applies no ticker filter"
"""The alternative a list dataset names when it is given `dm.ALL`."""

_ETF_KEYS: Final = ("Symbol", "EffectiveDate", "ProcessedDate")
"""The keys of an ETF Global row: the fund, the date its data is valid for, and the date ETF
Global processed it (an issuer can restate one effective date in a later processing)."""


def _list_spec(
    name: str,
    path: str,
    *,
    summary: str,
    symbol: str | KeySpec | None = None,
    symbol_param: str | None = None,
    poll_keys: tuple[str, ...],
    page_size: int,
    asset_class: AssetClass | None = None,
    time: str | None = None,
    time_axis: tuple[str, str] | None = None,
    poll_mode: PollMode = PollMode.REPLACE,
    per_symbol: bool = False,
) -> DatasetSpec:
    """Declare one partner dataset served by a single REST list path.

    Args:
        name: The dataset's dotted name, such as `"partners.benzinga.ratings"`.
        path: The REST path template behind `fetch()` and `poll()`.
        summary: The dataset's one-sentence summary, the first line of its factory's docstring.
        symbol: The path's column that holds the row's instrument, renamed to `Symbol`, or
            a `KeySpec` that fills `Symbol` from the request; None for a dataset whose rows
            describe no single instrument.
        symbol_param: The query parameter that carries the requested symbols, or None.
        poll_keys: The key columns of a keyed `poll()` table, unique per row.
        page_size: The page size requested: the endpoint's documented maximum `limit`.
        asset_class: The class whose symbol rules apply to `symbols`, or None.
        time: The path's instant column that holds the row's time, renamed to `Timestamp`.
        time_axis: The wire parameter pair behind `start` and `end`, or None.
        poll_mode: The default `poll()` mode.
        per_symbol: True when the path takes one ticker per request.

    Returns:
        The spec; `DatasetSpec` raises its `ValueError` for an inconsistent
        declaration.
    """
    keys = SourceKeys(
        symbol=KeySpec(symbol) if isinstance(symbol, str) else symbol,
        time=None if time is None else KeySpec(time),
    )
    return DatasetSpec(
        name=name,
        summary=summary,
        asset_class=asset_class,
        fetch_path=path,
        keys={} if keys == SourceKeys() else {(_REST, path): keys},
        time_axis=time_axis,
        symbol_param=symbol_param,
        poll_mode=poll_mode,
        poll_keys=poll_keys,
        per_symbol=per_symbol,
        page_size=page_size,
    )


def _benzinga(
    name: str, path: str, *, summary: str, page_size: int, tickers: bool = True
) -> DatasetSpec:
    """Declare a Benzinga v1 list dataset whose rows each carry a unique `benzinga_id`.

    Args:
        name: The dataset's name below `partners.benzinga`, such as `"ratings"`.
        path: The REST path template.
        summary: The dataset's one-sentence summary.
        page_size: The endpoint's documented maximum `limit`.
        tickers: True when the endpoint filters by `ticker` and each row names one stock.

    Returns:
        The spec, keyed for `poll()` by `BenzingaId`.
    """
    return _list_spec(
        f"partners.benzinga.{name}",
        path,
        summary=summary,
        asset_class=AssetClass.STOCKS if tickers else None,
        symbol="Ticker" if tickers else None,
        symbol_param="ticker.any_of" if tickers else None,
        poll_keys=("BenzingaId",),
        page_size=page_size,
    )


def _etf_global(
    name: str, path: str, *, summary: str, poll_keys: tuple[str, ...] = _ETF_KEYS
) -> DatasetSpec:
    """Declare an ETF Global dataset: one fund per row, filtered by `composite_ticker`.

    Args:
        name: The dataset's name below `partners.etf_global`, such as `"profiles"`.
        path: The REST path template.
        summary: The dataset's one-sentence summary.
        poll_keys: The key columns of a keyed `poll()` table.

    Returns:
        The spec; every ETF Global path pages at most 5,000 rows.
    """
    return _list_spec(
        f"partners.etf_global.{name}",
        path,
        summary=summary,
        asset_class=AssetClass.STOCKS,
        symbol="CompositeTicker",
        symbol_param="composite_ticker.any_of",
        poll_keys=poll_keys,
        page_size=5_000,
    )


BENZINGA_ANALYSTS: Final = _benzinga(
    "analysts",
    "/benzinga/v1/analysts",
    summary="Benzinga's profiles of sell-side analysts, with each analyst's firm and track record.",
    page_size=50_000,
    tickers=False,
)
BENZINGA_ANALYST_INSIGHTS: Final = _benzinga(
    "analyst_insights",
    "/benzinga/v1/analyst-insights",
    summary="Benzinga analyst insights on US stocks: a rating, a price target, and the reason.",
    page_size=50_000,
)
BENZINGA_RATINGS: Final = _benzinga(
    "ratings",
    "/benzinga/v1/ratings",
    summary="Analyst rating and price-target actions on US stocks, as reported by Benzinga.",
    page_size=50_000,
)
BENZINGA_CONSENSUS_RATINGS: Final = _list_spec(
    "partners.benzinga.consensus_ratings",
    "/benzinga/v1/consensus-ratings/{ticker}",
    summary="The consensus of analyst ratings and price targets for each US stock, from Benzinga.",
    asset_class=AssetClass.STOCKS,
    # The symbol requested fills `Symbol`, so it is canonical even when the row's own `ticker`
    # echo is missing or spelled another way; `Ticker` keeps the echo.
    symbol=KeySpec("ticker", from_request=True),
    poll_keys=("Symbol",),
    page_size=50_000,
    per_symbol=True,
)
BENZINGA_BULLS_BEARS_SAY: Final = _benzinga(
    "bulls_bears_say",
    "/benzinga/v1/bulls-bears-say",
    summary="Benzinga's bull case and bear case summaries for US stocks.",
    page_size=5_000,
)
BENZINGA_GUIDANCE: Final = _benzinga(
    "guidance",
    "/benzinga/v1/guidance",
    summary="Earnings and revenue guidance issued by US companies, as reported by Benzinga.",
    page_size=50_000,
)
BENZINGA_EARNINGS: Final = _benzinga(
    "earnings",
    "/benzinga/v1/earnings",
    summary="Earnings announcements of US companies from Benzinga, with estimates and surprises.",
    page_size=50_000,
)
BENZINGA_FIRMS: Final = _benzinga(
    "firms",
    "/benzinga/v1/firms",
    summary="The research firms behind Benzinga's analyst ratings.",
    page_size=50_000,
    tickers=False,
)
BENZINGA_NEWS: Final = _list_spec(
    "partners.benzinga.news",
    "/benzinga/v2/news",
    summary="Benzinga news articles, with headline, teaser, body, and the tickers each one names.",
    time="Published",
    time_axis=("published.gte", "published.lt"),
    symbol_param="tickers.any_of",
    poll_mode=PollMode.APPEND,
    poll_keys=("BenzingaId",),
    page_size=50_000,
)

ETF_GLOBAL_ANALYTICS: Final = _etf_global(
    "analytics",
    "/etf-global/v1/analytics",
    summary="ETF Global's quantitative risk, reward, and quality scores for US-listed ETFs.",
)
ETF_GLOBAL_CONSTITUENTS: Final = _etf_global(
    "constituents",
    "/etf-global/v1/constituents",
    summary="The holdings of US-listed ETFs from ETF Global, one row per fund, date, and holding.",
    # The rank orders a fund's holdings of one effective date without ties.
    poll_keys=(*_ETF_KEYS, "ConstituentRank"),
)
ETF_GLOBAL_FUND_FLOWS: Final = _etf_global(
    "fund_flows",
    "/etf-global/v1/fund-flows",
    summary="Daily fund flows, net asset value, and shares outstanding of US-listed ETFs.",
)
ETF_GLOBAL_PROFILES: Final = _etf_global(
    "profiles",
    "/etf-global/v1/profiles",
    summary="ETF Global's profiles of US ETFs: issuer, fees, assets, trading, and exposures.",
)
ETF_GLOBAL_TAXONOMIES: Final = _etf_global(
    "taxonomies",
    "/etf-global/v1/taxonomies",
    summary="ETF Global's classification of US-listed ETFs by strategy, structure, and exposure.",
)

TMX_CORPORATE_EVENTS: Final = _list_spec(
    "partners.tmx.corporate_events",
    "/tmx/v1/corporate-events",
    summary="Corporate events from TMX: earnings dates, dividends, conferences, and more.",
    asset_class=AssetClass.STOCKS,
    # The calendar is global and the vendor marks `ticker` optional: an event of a company
    # without a ticker has a null `Symbol` instead of failing the request.
    symbol=KeySpec("Ticker", required=False),
    symbol_param="ticker.any_of",
    time_axis=("date.gte", "date.lte"),
    poll_keys=("TmxRecordId",),
    page_size=50_000,
)

FABLE_MERCHANT_AGGREGATES: Final = _list_spec(
    "partners.fable.merchant_aggregates",
    "/consumer-spending/eu/v1/merchant-aggregates",
    summary="Daily European consumer spending per merchant, from Fable Data's transaction panel.",
    time_axis=("transaction_date.gte", "transaction_date.lte"),
    # The vendor documents no row key, so the key is every field that is not a measure or an
    # attribute of the merchant: the day, the data version (a restated day comes back with a
    # later publish date), the merchant or processor, and the panel slice (consumer country,
    # channel, panel, merchant category, and account currency).
    poll_keys=(
        "TransactionDate",
        "PublishedDate",
        "Name",
        "Type",
        "UserCountry",
        "Channel",
        "ConsumerType",
        "MccGroup",
        "TransactionCurrency",
    ),
    page_size=5_000,
)
FABLE_MERCHANT_HIERARCHY: Final = _list_spec(
    "partners.fable.merchant_hierarchy",
    "/consumer-spending/eu/v1/merchant-hierarchy",
    summary="Fable Data's map from merchants to parent companies, tickers, and industries.",
    # One mapping per merchant and the date it became applicable.
    poll_keys=("LookupName", "ActiveFrom"),
    page_size=50_000,
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    BENZINGA_ANALYSTS,
    BENZINGA_ANALYST_INSIGHTS,
    BENZINGA_RATINGS,
    BENZINGA_CONSENSUS_RATINGS,
    BENZINGA_BULLS_BEARS_SAY,
    BENZINGA_GUIDANCE,
    BENZINGA_EARNINGS,
    BENZINGA_FIRMS,
    BENZINGA_NEWS,
    ETF_GLOBAL_ANALYTICS,
    ETF_GLOBAL_CONSTITUENTS,
    ETF_GLOBAL_FUND_FLOWS,
    ETF_GLOBAL_PROFILES,
    ETF_GLOBAL_TAXONOMIES,
    TMX_CORPORATE_EVENTS,
    FABLE_MERCHANT_AGGREGATES,
    FABLE_MERCHANT_HIERARCHY,
)
"""Every partner dataset, one `DatasetSpec` each, in the order of the namespace tree."""


def _list_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None = None,
    all_alternative: str = _NO_TICKER_FILTER,
    start: DateLike | None = None,
    end: DateLike | None = None,
    sort: str | None = None,
    order: Order | None = None,
    limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None,
) -> Query:
    """Build the query of a partner dataset from its factory's arguments.

    Args:
        core: The session core.
        spec: The dataset.
        symbols: The requested tickers, or None for none.
        all_alternative: What the `ValueError` for `dm.ALL` tells the caller to pass instead.
        start: The lower bound of the dataset's time axis, or None.
        end: The upper bound of the dataset's time axis, or None.
        sort: The `sort` parameter, or None for the endpoint's default.
        order: The `order` parameter, or None for the endpoint's default.
        limit: The cap on the total number of rows, or None for every row.
        filters: The `filters` mapping, or None.

    Returns:
        The query. Invalid arguments raise the `TypeError` or `ValueError` of `build_query`
        and `list_params`.
    """
    query = build_query(
        core,
        spec,
        symbols=symbols,
        all_alternative=all_alternative,
        start=start,
        end=end,
        params=list_params(sort=sort, order=order),
        filters=filters,
        limit=limit,
    )
    # A partner spec has only a fetch path, so `build_query` builds its `Query`.
    return cast("Query", query)


class _Benzinga(Namespace):
    """`m.partners.benzinga`: Benzinga's analyst, earnings, guidance, and news datasets.

    Benzinga is a financial news and data vendor; Massive resells its analyst ratings and their
    consensus, analyst and firm profiles, analyst insights, bull and bear cases, corporate
    guidance, earnings, and news, each in its own Benzinga expansion plan. Every dataset here
    is a REST list endpoint and returns a `Query`.

    Created once per session by `_Partners`, together with the other vendors, and reached as
    `m.partners.benzinga`. It holds no state of its own, so it is safe to use from any thread;
    each method validates its arguments and returns a query without touching the network.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            upgrades = m.partners.benzinga.ratings(
                ["AAPL", "MSFT"], filters={"rating_action": "upgrades"}
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.benzinga"

    def analysts(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Benzinga's profiles of sell-side analysts, with each analyst's firm and track record.

        One row per analyst: name, firm, the number of ratings issued, and performance
        measures such as success rate, average return, and their percentiles. The rows
        describe people, not instruments, so the dataset takes no symbols and has no `Symbol`
        column; select analysts or firms through `filters` (`full_name`, `firm_name`,
        `benzinga_firm_id`, ...).

        Served by REST only, from `GET /benzinga/v1/analysts`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-analysts,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            sort: The column to sort by, by its wire name (`"smart_score"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="smart_score", order=dm.Order.DESC` sends `sort=smart_score.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"firm_name_any_of": ["Wedbush", "Mizuho"]}` sends
                `firm_name.any_of=Wedbush,Mizuho`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type (an `order` that is not a `dm.Order`), and
            `ValueError` for an unknown filter, a filter value of the wrong kind, or a
            `limit` that is not positive.

        Example:
            ::

                top = m.partners.benzinga.analysts(
                    sort="smart_score", order=dm.Order.DESC, limit=50
                ).fetch()
        """
        return _list_query(
            self._core, BENZINGA_ANALYSTS, sort=sort, order=order, limit=limit, filters=filters
        )

    def analyst_insights(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Benzinga analyst insights on US stocks: a rating, a price target, and the reason.

        One row per insight: the stock, the firm, the rating and rating action, the price
        target, and the analyst's rationale as text, dated by `Date`. Narrow the dates through
        `filters` (`date_gte`, `date_lt`, ...), since the dataset has no `start` and `end`.

        Served by REST only, from `GET /benzinga/v1/analyst-insights`: `fetch()` reads every
        page into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-analyst_insights,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks to return: one ticker (`"AAPL"`), a sequence of tickers, or a
                Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="date", order=dm.Order.DESC` sends `sort=date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"date_gte": "2025-01-01"}` sends
                `date.gte=2025-01-01`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                q = m.partners.benzinga.analyst_insights("NVDA", filters={"date_gte": "2025-01-01"})
                insights = q.fetch()
        """
        return _list_query(
            self._core,
            BENZINGA_ANALYST_INSIGHTS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def ratings(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Analyst rating and price-target actions on US stocks, as reported by Benzinga.

        One row per action by one analyst at one firm: an upgrade, a downgrade, a
        reiteration, or a price-target change, with the previous and new rating and target,
        dated by `Date`. Narrow the dates through `filters` (`date_gte`, `date_lt`, ...),
        since the dataset has no `start` and `end`.

        Served by REST only, from `GET /benzinga/v1/ratings`: `fetch()` reads every page into
        a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-ratings,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks to return: one ticker (`"AAPL"`), a sequence of tickers, or a
                Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="date", order=dm.Order.DESC` sends `sort=date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"rating_action": "upgrades"}`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                live = m.partners.benzinga.ratings(["AAPL", "MSFT"]).poll(60)
        """
        return _list_query(
            self._core,
            BENZINGA_RATINGS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def consensus_ratings(
        self, symbols: Symbols, *, filters: Mapping[str, FilterValue] | None = None
    ) -> Query:
        """The consensus of analyst ratings and price targets for each US stock, from Benzinga.

        One row per requested stock: the counts of strong-buy, buy, hold, sell, and
        strong-sell ratings, the consensus rating and its numeric value, the consensus, high,
        and low price targets, and the number of contributing analysts. `Symbol` is the
        symbol requested, in its canonical form; `Ticker` is Benzinga's echo of it. By
        default the consensus aggregates every rating Benzinga holds for the stock; the
        `date` filters restrict it to the ratings published in a range
        (`{"date_gte": "2024-10-01", "date_lt": "2025-01-01"}` aggregates the fourth quarter
        of 2024).

        Served by REST only, from `GET /benzinga/v1/consensus-ratings/{ticker}`, which takes
        one ticker as a path segment, so a list of symbols sends one request per symbol, at
        most `max_concurrency` at a time. `fetch()` returns a static table, and
        `poll(interval)` re-reads it into a live table keyed by `Symbol`. There is no stream
        and no `history()`, and no `limit`: the endpoint returns one row per stock. The
        columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-consensus_ratings,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks: one ticker (`"AAPL"`), a sequence of tickers, or a Deephaven
                table with a `Symbol` column, which `poll()` re-reads every cycle. Required:
                the endpoint has no form without a ticker, so `dm.ALL` raises `ValueError`.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores: the `date` range of the ratings to aggregate
                (`date`, `date_gte`, `date_lt`, ...); None, the default, aggregates every
                rating.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            a value that is not a ticker, a sequence of tickers, or a table, or for a
            `filters` that is not a mapping, and `ValueError` for `dm.ALL`, no symbols (an
            empty list or an exhausted generator), a symbol that is not a valid stock ticker,
            a table without a `Symbol` column, an unknown filter, or a filter value that is
            not a date.

        Example:
            ::

                consensus = m.partners.benzinga.consensus_ratings(["AAPL", "MSFT"]).fetch()
                q4 = m.partners.benzinga.consensus_ratings(
                    "AAPL", filters={"date_gte": "2024-10-01", "date_lt": "2025-01-01"}
                ).fetch()
        """
        return _list_query(
            self._core,
            BENZINGA_CONSENSUS_RATINGS,
            symbols=symbols,
            all_alternative="a list of symbols; the endpoint takes one ticker per request",
            filters=filters,
        )

    def bulls_bears_say(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Benzinga's bull case and bear case summaries for US stocks.

        One row per summary: the stock, the bull case and the bear case as text, and when the
        summary was last updated (`LastUpdated`, filterable as `last_updated_gte` and so on).

        Served by REST only, from `GET /benzinga/v1/bulls-bears-say`: `fetch()` reads every
        page into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-bulls_bears_say,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks to return: one ticker (`"AAPL"`), a sequence of tickers, or a
                Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"last_updated"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="last_updated", order=dm.Order.DESC` sends `sort=last_updated.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"last_updated_gte": "2025-06-01"}`); None sends
                none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                cases = m.partners.benzinga.bulls_bears_say(["TSLA"]).fetch()
        """
        return _list_query(
            self._core,
            BENZINGA_BULLS_BEARS_SAY,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def guidance(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Earnings and revenue guidance issued by US companies, as reported by Benzinga.

        One row per guidance release: the stock, the fiscal period, the guided EPS and revenue
        ranges with the previous ones, and whether the figure is the primary or a secondary
        one (`Positioning`), dated by
        `Date`. Narrow the dates through `filters` (`date_gte`, `date_lt`, ...), since the
        dataset has no `start` and `end`.

        Served by REST only, from `GET /benzinga/v1/guidance`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-guidance,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks to return: one ticker (`"AAPL"`), a sequence of tickers, or a
                Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="date", order=dm.Order.DESC` sends `sort=date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"fiscal_year": 2025, "positioning": "primary"}`); None
                sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                outlook = m.partners.benzinga.guidance("AAPL", filters={"fiscal_year": 2025})
                rows = outlook.fetch()
        """
        return _list_query(
            self._core,
            BENZINGA_GUIDANCE,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def earnings(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Earnings announcements of US companies from Benzinga, with estimates and surprises.

        One row per announcement, past or scheduled: the stock, the fiscal period, the
        announcement date and time and whether they are confirmed, and the estimated, actual,
        and previous EPS and revenue with their surprises. Narrow the dates through `filters`
        (`date_gte`, `date_lt`, ...), since the dataset has no `start` and `end`.

        Served by REST only, from `GET /benzinga/v1/earnings`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-earnings,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The stocks to return: one ticker (`"AAPL"`), a sequence of tickers, or a
                Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="date", order=dm.Order.ASC` sends `sort=date.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"date_gte": "2025-07-01", "importance_gte": 3}`);
                None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                calendar = m.partners.benzinga.earnings(filters={"date_gte": "2025-07-01"})
                upcoming = calendar.fetch()
        """
        return _list_query(
            self._core,
            BENZINGA_EARNINGS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def firms(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The research firms behind Benzinga's analyst ratings.

        One row per firm: its Benzinga identifier, name, currency, and when the record was
        last updated. Join `BenzingaId` to the `BenzingaFirmId` of `ratings()`,
        `analyst_insights()`, or `analysts()`. The rows describe firms, not instruments, so the
        dataset takes no symbols and has no `Symbol` column.

        Served by REST only, from `GET /benzinga/v1/firms`: `fetch()` reads every page into a
        static table, and `poll(interval)` re-reads it into a live table keyed by
        `BenzingaId`. There is no stream and no `history()`. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-firms,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            sort: The column to sort by, by its wire name (`"name"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="name", order=dm.Order.ASC` sends `sort=name.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"benzinga_id_any_of": ["5f3ac1", "60a1b2"]}`);
                None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for an unknown filter, a filter
            value of the wrong kind, or a `limit` that is not positive.

        Example:
            ::

                firms = m.partners.benzinga.firms().fetch()
        """
        return _list_query(
            self._core, BENZINGA_FIRMS, sort=sort, order=order, limit=limit, filters=filters
        )

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
        """Benzinga news articles, with headline, teaser, body, and the tickers each one names.

        One row per article: its publication time as `Timestamp`, author, title, teaser, body,
        URL, and the JSON lists of tickers, channels, tags, and images. An article names any
        number of tickers, so the rows have no `Symbol` column: `Tickers` holds the list as
        JSON text, and `symbols` only selects the articles that name at least one of them.

        Served by REST only, from `GET /benzinga/v2/news`: `fetch()` reads every page into a
        static table, and `poll(interval)` re-reads the endpoint into a live table that, by
        default (`PollMode.APPEND`), only grows: each cycle appends the articles it has not
        seen before, keyed by `BenzingaId`, so every article appears once. Always give
        `poll()` a `start`: every cycle reads every page of the range, and without one
        the range is the whole Benzinga archive. There is no stream and no `history()`;
        use `start` and `end` for a past range. The columns and what they mean are in
        the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#benzinga-news,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The tickers whose articles to return: one ticker (`"AAPL"`), a sequence
                of tickers, or a Deephaven table with a `Symbol` column, which `poll()`
                re-reads every cycle. An article matches when it names any of them. None, the
                default, applies no ticker filter. `dm.ALL` raises `ValueError`; pass None
                instead.
            start: The earliest publication time: an ISO date (`"2025-06-02"`) or `date`,
                meaning 00:00 America/New_York that day, or a timezone-aware `datetime` or
                ISO text with an offset. None, the default, sets no lower bound.
            end: The end of the publication range, of the same kind as `start` (mixing a date with a
                `datetime` raises `ValueError`): a date includes that whole day, and an aware
                `datetime` is the first instant excluded. None (the default) sends no upper bound,
                so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The column to sort by, by its wire name (`"published"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="published", order=dm.Order.ASC` sends `sort=published.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"channels_any_of": ["earnings"]}` sends
                `channels.any_of=earnings`); None sends none. The `published` parameters are
                set only through `start` and `end`.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, a naive `datetime` or an `end` before `start`, an unknown filter or a
            filter on `published`, a filter value of the wrong kind, or a `limit` that is not
            positive.

        Example:
            ::

                from datetime import date, timedelta

                q = m.partners.benzinga.news(symbols=["AAPL", "NVDA"], start="2025-06-02")
                headlines = q.fetch().view(["Timestamp", "Title", "Tickers"])
                yesterday = date.today() - timedelta(days=1)
                feed = m.partners.benzinga.news(start=yesterday).poll(30)
        """
        return _list_query(
            self._core,
            BENZINGA_NEWS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class _EtfGlobal(Namespace):
    """`m.partners.etf_global`: ETF Global's fund analytics, holdings, flows, and profiles.

    ETF Global covers US-listed exchange-traded funds; Massive resells its quantitative scores,
    constituents, fund flows, profiles, and taxonomies. Every row describes one fund, named by
    its ticker in `Symbol` (ETF Global's `composite_ticker`), on an `EffectiveDate` (the date
    the data is valid for) and a `ProcessedDate` (when ETF Global processed it; some issuers
    publish weeks late). Narrow the dates through `filters` (`effective_date_gte`, ...).
    Every dataset here is a REST list endpoint and returns a `Query`.

    Created once per session by `_Partners` and reached as `m.partners.etf_global`. It holds no
    state of its own, so it is safe to use from any thread; each method validates its arguments
    and returns a query without touching the network.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            spy = m.partners.etf_global.constituents(
                "SPY", filters={"effective_date": "2025-09-18"}
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.etf_global"

    def analytics(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """ETF Global's quantitative risk, reward, and quality scores for US-listed ETFs.

        One row per fund and date: the total risk and reward scores, the quant grade and total
        score, and their technical, sentiment, behavioral, fundamental, global, and quality
        components.

        Served by REST only, from `GET /etf-global/v1/analytics`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `Symbol`, `EffectiveDate`, and `ProcessedDate`. There is no stream and no `history()`.
        The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#etf_global-analytics,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The funds to return: one ETF ticker (`"SPY"`), a sequence of tickers, or
                a Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"reward_score"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="reward_score", order=dm.Order.DESC` sends `sort=reward_score.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"quant_grade": "A"}`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                scores = m.partners.etf_global.analytics(["SPY", "QQQ"]).fetch()
        """
        return _list_query(
            self._core,
            ETF_GLOBAL_ANALYTICS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def constituents(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The holdings of US-listed ETFs from ETF Global, one row per fund, date, and holding.

        Each row is one security a fund holds on a date: the fund in `Symbol`, the holding's
        ticker (`ConstituentTicker`), name, identifiers (ISIN, FIGI, SEDOL, CUSIP), rank by
        weight, weight, market value, and shares held. A fund holds hundreds of securities on
        each of many dates, so filter by date (`{"effective_date": "2025-09-18"}`).

        Served by REST only, from `GET /etf-global/v1/constituents`: `fetch()` reads every
        page into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `Symbol`, `EffectiveDate`, `ProcessedDate`, and `ConstituentRank`. There is no stream
        and no `history()`. The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#etf_global-constituents,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The funds whose holdings to return: one ETF ticker (`"SPY"`), a sequence
                of tickers, or a Deephaven table with a `Symbol` column, which `poll()`
                re-reads every cycle. None, the default, applies no fund filter. `dm.ALL`
                raises `ValueError`; pass None instead. To find the funds that hold a
                security, filter on `constituent_ticker` instead.
            sort: The column to sort by, by its wire name (`"weight"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="weight", order=dm.Order.DESC` sends `sort=weight.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"constituent_ticker": "NVDA"}`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                holders = m.partners.etf_global.constituents(
                    filters={"constituent_ticker": "NVDA", "effective_date": "2025-09-18"}
                ).fetch()
        """
        return _list_query(
            self._core,
            ETF_GLOBAL_CONSTITUENTS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def fund_flows(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily fund flows, net asset value, and shares outstanding of US-listed ETFs.

        One row per fund and date, from ETF Global: the money that flowed into or out of the
        fund, its net asset value, and its shares outstanding.

        Served by REST only, from `GET /etf-global/v1/fund-flows`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `Symbol`, `EffectiveDate`, and `ProcessedDate`. There is no stream and no `history()`.
        The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#etf_global-fund_flows,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The funds to return: one ETF ticker (`"SPY"`), a sequence of tickers, or
                a Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"effective_date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="effective_date", order=dm.Order.DESC` sends `sort=effective_date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"effective_date_gte": "2025-01-01"}`); None sends
                none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                flows = m.partners.etf_global.fund_flows(
                    ["SPY", "IVV", "VOO"], filters={"effective_date_gte": "2025-01-01"}
                ).fetch()
        """
        return _list_query(
            self._core,
            ETF_GLOBAL_FUND_FLOWS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def profiles(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """ETF Global's profiles of US ETFs: issuer, fees, assets, trading, and exposures.

        One row per fund and date: the issuer and service providers, inception date, fees and
        expenses, assets under management, holdings count, trading and options activity, and
        the fund's sector, industry, geographic, currency, coupon, and maturity exposures,
        each a JSON text column.

        Served by REST only, from `GET /etf-global/v1/profiles`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `Symbol`, `EffectiveDate`, and `ProcessedDate`. There is no stream and no `history()`.
        The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#etf_global-profiles,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The funds to return: one ETF ticker (`"SPY"`), a sequence of tickers, or
                a Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"effective_date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="effective_date", order=dm.Order.DESC` sends `sort=effective_date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"effective_date": "2025-09-18"}`); None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                profile = m.partners.etf_global.profiles("ARKK").fetch()
        """
        return _list_query(
            self._core,
            ETF_GLOBAL_PROFILES,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def taxonomies(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """ETF Global's classification of US-listed ETFs by strategy, structure, and exposure.

        One row per fund and date: the asset class, category, focus, region, objective,
        selection and weighting methodology, leverage, rebalancing, ESG, and tax
        classification that describe what the fund is.

        Served by REST only, from `GET /etf-global/v1/taxonomies`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `Symbol`, `EffectiveDate`, and `ProcessedDate`. There is no stream and no `history()`.
        The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#etf_global-taxonomies,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The funds to return: one ETF ticker (`"SPY"`), a sequence of tickers, or
                a Deephaven table with a `Symbol` column, which `poll()` re-reads every cycle.
                None, the default, applies no ticker filter. `dm.ALL` raises `ValueError`;
                pass None instead.
            sort: The column to sort by, by its wire name (`"effective_date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="effective_date", order=dm.Order.DESC` sends `sort=effective_date.desc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"processed_date_gte": "2025-09-01"}`); None sends
                none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an unknown filter, a filter value of the wrong kind, or a `limit` that is
            not positive.

        Example:
            ::

                kinds = m.partners.etf_global.taxonomies(["SPY", "TQQQ"]).fetch()
        """
        return _list_query(
            self._core,
            ETF_GLOBAL_TAXONOMIES,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class _Tmx(Namespace):
    """`m.partners.tmx`: corporate events from TMX's Wall Street Horizon calendar.

    TMX Group's Wall Street Horizon tracks scheduled corporate events worldwide: earnings
    dates and calls, dividends, splits, conferences, and shareholder meetings. Its one dataset
    is a REST list endpoint and returns a `Query`.

    Created once per session by `_Partners` and reached as `m.partners.tmx`. It holds no state
    of its own, so it is safe to use from any thread; its method validates its arguments and
    returns a query without touching the network.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            events = m.partners.tmx.corporate_events(
                ["AAPL"], start="2025-07-01", end="2025-09-30"
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.tmx"

    def corporate_events(
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
        """Corporate events from TMX: earnings dates, dividends, conferences, and more.

        One row per event: the company's ticker (`Symbol`), name, and ISIN, its trading
        venue, the event's type, name, status (confirmed, unconfirmed, canceled, ...),
        scheduled `Date`, and a link to its announcement. The calendar is global, and a
        company without a ticker has a null `Symbol`; its `Isin` still names it. Events are
        dated by day, so `Date` is a calendar date and the dataset has no `Timestamp`.

        Served by REST only, from `GET /tmx/v1/corporate-events`: `fetch()` reads every page
        into a static table, and `poll(interval)` re-reads it into a live table keyed by
        `TmxRecordId`. There is no stream and no `history()`; use `start` and `end` for a
        date range. The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#tmx-corporate_events,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            symbols: The companies to return: one ticker (`"AAPL"`), a sequence of tickers,
                or a Deephaven table with a `Symbol` column, which `poll()` re-reads every
                cycle. None, the default, applies no ticker filter. `dm.ALL` raises
                `ValueError`; pass None instead.
            start: The first event date to return, inclusive: an ISO date
                (`"2025-07-01"`) or a `date`; the axis is a calendar date, so a `datetime` raises
                `ValueError`. None, the default, sets no lower bound.
            end: The last event date to return, inclusive, in the same forms. None (the default)
                sends no upper bound, so rows dated in the future are returned too; pass `end=` to
                stop at a date.
            sort: The column to sort by, by its wire name (`"date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="date", order=dm.Order.ASC` sends `sort=date.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"type": "earnings_announcement_date"}`); None sends
                none. The `date` parameters are set only through `start` and `end`.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for `dm.ALL`, an empty or invalid
            symbol, an `end` before `start`, an unknown filter or a filter on `date`, a
            filter value of the wrong kind, or a `limit` that is not positive.

        Example:
            ::

                q = m.partners.tmx.corporate_events(
                    ["AAPL", "MSFT"],
                    start="2025-07-01",
                    end="2025-09-30",
                    filters={"type": "earnings_announcement_date"},
                )
                dates = q.fetch()
        """
        return _list_query(
            self._core,
            TMX_CORPORATE_EVENTS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class _Fable(Namespace):
    """`m.partners.fable`: Fable Data's European consumer-spending datasets.

    Fable Data aggregates card and open-banking transactions of a European consumer panel (the
    UK, Germany, France, Italy, Spain, and Austria) per merchant and day, and maps the merchants
    to their parent companies and, for about 250 US public companies, tickers. Merchant names
    join the two: an aggregate's `Name` is a hierarchy row's `LookupName`. The tickers are
    Bloomberg tickers (`"AMZN US"`), not Massive symbols, so neither dataset takes `symbols`;
    filter them through `filters`. Both are REST list endpoints and return a `Query`.

    Created once per session by `_Partners` and reached as `m.partners.fable`. It holds no
    state of its own, so it is safe to use from any thread; each method validates its arguments
    and returns a query without touching the network.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            amazon = m.partners.fable.merchant_aggregates(
                start="2025-06-01", filters={"parent_name": "Amazon Inc"}
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.fable"

    def merchant_aggregates(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily European consumer spending per merchant, from Fable Data's transaction panel.

        One row per merchant (or payment processor), transaction day, consumer country,
        channel (online, offline, buy now pay later), and panel (credit, debit, open banking):
        outbound and inbound spend, transaction and account counts, and the rolling panel sizes
        that normalize them. Amounts are in the row's `TransactionCurrency` (EUR or GBP). A
        restated day comes back with a later `PublishedDate`.

        Served by REST only, from `GET /consumer-spending/eu/v1/merchant-aggregates`:
        `fetch()` reads every page into a static table, and `poll(interval)` re-reads it into
        a live table keyed by `TransactionDate`, `PublishedDate`, `Name`, `Type`,
        `UserCountry`, `Channel`, `ConsumerType`, `MccGroup`, and `TransactionCurrency`.
        There is no stream and no `history()`;
        use `start` and `end` for a range of transaction days. The columns and what they mean
        are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#fable-merchant_aggregates,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            start: The first transaction date to return, inclusive: an ISO date
                (`"2025-06-01"`) or a `date`; the axis is a calendar date, so a `datetime` raises
                `ValueError`. None, the default, sets no lower bound.
            end: The last transaction date to return, inclusive, in the same forms. None (the
                default) sends no upper bound, so rows dated in the future are returned too; pass
                `end=` to stop at a date.
            sort: The column to sort by, by its wire name (`"transaction_date"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="transaction_date", order=dm.Order.ASC` sends `sort=transaction_date.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"name": "asda", "channel": "online"}`); None sends
                none. The `transaction_date` parameters are set only through `start` and `end`.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for an `end` before `start`, an
            unknown filter or a filter on `transaction_date`, a filter value of the wrong kind
            or outside a parameter's values (`channel` takes `online`, `offline`, or `bnpl`),
            or a `limit` that is not positive.

        Example:
            ::

                q = m.partners.fable.merchant_aggregates(
                    start="2025-06-01", end="2025-06-30", filters={"user_country": "UK"}
                )
                june = q.fetch()
        """
        return _list_query(
            self._core,
            FABLE_MERCHANT_AGGREGATES,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def merchant_hierarchy(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Fable Data's map from merchants to parent companies, tickers, and industries.

        One row per merchant and period of validity: the merchant's lookup and display names,
        its parent, grandparent, and great-grandparent companies with their Bloomberg tickers,
        whether it is listed, and its sector, industry, and category. `ActiveFrom` and
        `ActiveTo` bound when the mapping applies (`0001-01-01` and `9999-12-31` mean
        unbounded), so a join on `LookupName` within them is point in time.

        Served by REST only, from `GET /consumer-spending/eu/v1/merchant-hierarchy`: `fetch()`
        reads every page into a static table, and `poll(interval)` re-reads it into a live
        table keyed by `LookupName` and `ActiveFrom`. There is no stream and no `history()`.
        The columns and what they mean are in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.md#fable-merchant_hierarchy,
        in the web UI's column tooltips, and in `columns()`.

        Args:
            sort: The column to sort by, by its wire name (`"lookup_name"`); None keeps the
                endpoint's default order.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix
                (`sort="lookup_name", order=dm.Order.ASC` sends `sort=lookup_name.asc`);
                None leaves the direction to the endpoint. `order` without `sort` raises
                `ValueError`.
            limit: The cap on the total number of rows across all pages, a positive `int`,
                or None (the default) for every row. The page size is internal.
            filters: Further query parameters of the endpoint, keyed by wire name with dots
                written as underscores (`{"ticker": "AMZN US", "listing_status": "public"}`);
                None sends none.

        Returns:
            A `Query`. Building it checks every argument and sends nothing: `TypeError` for
            an argument of the wrong type, and `ValueError` for an unknown filter, a filter
            value of the wrong kind or outside a parameter's values (`listing_status` takes
            `public` or `private`), or a `limit` that is not positive.

        Example:
            ::

                public = m.partners.fable.merchant_hierarchy(
                    filters={"listing_status": "public"}
                ).fetch()
        """
        return _list_query(
            self._core,
            FABLE_MERCHANT_HIERARCHY,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class _Partners(Namespace):
    """`m.partners`: one attribute per data vendor.

    Created once by `MassiveSession`, together with its four vendor namespaces. Holds no
    mutable state, so it is safe to use from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            m.partners.benzinga  # <MassiveSession #1 partners.benzinga>, the Benzinga datasets
    """

    __slots__ = ("_benzinga", "_etf_global", "_fable", "_tmx")
    _path: ClassVar[str] = "partners"

    def __init__(self, core: SessionCore) -> None:
        """Create the four vendor namespaces.

        Args:
            core: The session's internal core.
        """
        super().__init__(core)
        self._benzinga = _Benzinga(core)
        self._etf_global = _EtfGlobal(core)
        self._tmx = _Tmx(core)
        self._fable = _Fable(core)

    @property
    def benzinga(self) -> _Benzinga:
        """Every Benzinga dataset: analysts, firms, ratings, insights, guidance, and news."""
        return self._benzinga

    @property
    def etf_global(self) -> _EtfGlobal:
        """Every ETF Global dataset: analytics, constituents, flows, profiles, taxonomies."""
        return self._etf_global

    @property
    def tmx(self) -> _Tmx:
        """The TMX datasets: scheduled corporate events."""
        return self._tmx

    @property
    def fable(self) -> _Fable:
        """The Fable Data datasets: European consumer spending by merchant."""
        return self._fable
