"""The `m.partners` namespace: datasets from Massive's data partners.

Partner datasets stay nested by vendor, one class each:

- `m.partners.benzinga` (`PartnersBenzingaNamespace`): analysts, analyst insights, ratings,
  consensus ratings, bulls-and-bears summaries, guidance, earnings, firms, and news;
- `m.partners.etf_global` (`PartnersEtfGlobalNamespace`): ETF analytics, constituents, fund flows,
  profiles, and taxonomies;
- `m.partners.tmx` (`PartnersTmxNamespace`): corporate events;
- `m.partners.fable` (`PartnersFableNamespace`): European consumer-spending merchant aggregates and
  hierarchy.

Every partner dataset is a REST list endpoint, so every factory returns a `FetchQuery`: `fetch()`
reads every page once, and `poll()` re-reads them on an interval. Nothing streams over the
WebSocket and nothing has flat files. Each factory validates its arguments synchronously and
touches no network; the request runs only when a query method is called.

This module owns the partners' `DatasetSpec`s, exported as `DATASETS` and composed into the
dataset catalog (`CATALOG` in `_session.py`). A spec names its REST path and the facts the
generated wire schemas cannot give: which column holds the row's instrument (`Symbol`) and, for
news, its time (`Timestamp`), the wire parameters behind `symbols`, `start`, and `end`, the
keys of a keyed `poll()` table, and the page size, which is each endpoint's documented maximum.
Columns, types, and descriptions come from the generated schemas, never from this module.

Instruments. The Benzinga datasets that take tickers and the ETF Global datasets take Massive's
stock tickers (`AAPL`, `BRK.B`), so their specs carry `AssetClass.STOCKS` and a symbol with another
class's prefix (`X:BTCUSD`) is rejected when the query is built. Their rows each describe one
instrument, which becomes the `Symbol` column (an ETF Global row's ETF, its `composite_ticker`; a
Benzinga row's `ticker`; for a TMX event or a Benzinga row without a ticker, null). Benzinga news
names several tickers per article, so it keeps its `Tickers` column (a `String[]`), and an article
is listed under each ticker it was read for, in `Symbol`. Benzinga analysts and firms, and both
Fable datasets, take no symbols at all. The TMX corporate events come from Wall Street Horizon's
global calendar, whose tickers are not US-market ones (each event names its venue's MIC and an
ISIN), so that dataset takes and keeps its tickers exactly as Massive writes them
(`DatasetSpec.symbols_as_written`).

The module is one subject although it runs past 1,000 lines: it is one namespace and its four
partner sub-namespaces, and every dataset is a REST list endpoint built the same way
(`list_query`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import NO_NAMED_TICKER, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, ClassFromPrefix, PollMode
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive.namespaces._query_building import Namespace, list_query

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._enums import Order
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive.typehints import Symbols
    from deephaven_massive.typehints._queries import FetchQuery

__all__ = [
    "DATASETS",
    "PartnersBenzingaNamespace",
    "PartnersEtfGlobalNamespace",
    "PartnersFableNamespace",
    "PartnersNamespace",
    "PartnersTmxNamespace",
]


_REST: Final = Transport.REST

_NO_TICKER_FILTER: Final = "leave symbols out, which applies no ticker filter"
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
    page_size: int | None = None,
    asset_class: AssetClass | ClassFromPrefix = ClassFromPrefix.ANY,
    time: str | None = None,
    time_axis: tuple[str, str] | None = None,
    poll_mode: PollMode = PollMode.REPLACE,
    per_symbol: bool = False,
    symbols_as_written: bool = False,
    ticker_list: str | None = None,
    nullable_poll_keys: Mapping[str, str] | None = None,
) -> DatasetSpec:
    """Declare one partner dataset served by a single REST list path.

    Args:
        name: The dataset's dotted name, such as `"partners.benzinga.ratings"`.
        path: The REST path template behind `fetch()` and `poll()`.
        summary: The dataset's one-sentence summary, the first line of its factory's docstring.
        symbol: The path's column that holds the row's instrument, renamed to `Symbol`, or
            a `KeySpec` for a ticker some rows lack (`required=False`); None for a dataset
            whose rows describe no single instrument.
        symbol_param: The query parameter that carries the requested symbols, or None.
        poll_keys: The key columns of a keyed `poll()` table, unique per row.
        page_size: A page size below the endpoint's documented maximum `limit`, for rows
            that carry long text, so one page stays at tens of megabytes; None, the default,
            requests the documented maximum.
        asset_class: The class whose symbol rules apply to `symbols`, or `ClassFromPrefix.ANY`
            for a dataset whose symbols span classes.
        time: The path's instant column that holds the row's time, renamed to `Timestamp`.
        time_axis: The wire parameter pair behind `start` and `end`, or None.
        poll_mode: The default `poll()` mode.
        per_symbol: True when the path takes one ticker per request.
        symbols_as_written: True when the symbols are a third party's global tickers, kept
            as Massive writes them (`DatasetSpec.symbols_as_written`).
        ticker_list: The column listing each row's tickers, from which the library fills
            `Symbol` (`DatasetSpec.ticker_list`); None, the default, for any other dataset.
        nullable_poll_keys: The poll keys whose null is a value, each with its reason; None,
            the default, for none.

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
        symbols_as_written=symbols_as_written,
        ticker_list=ticker_list,
        nullable_poll_keys={} if nullable_poll_keys is None else nullable_poll_keys,
    )


def _benzinga(
    name: str, path: str, *, summary: str, page_size: int | None = None, tickers: bool = True
) -> DatasetSpec:
    """Declare a Benzinga v1 list dataset whose rows each carry a unique `benzinga_id`.

    Args:
        name: The dataset's name below `partners.benzinga`, such as `"ratings"`.
        path: The REST path template.
        summary: The dataset's one-sentence summary.
        page_size: A page size below the endpoint's documented maximum `limit`, for rows
            that carry long text; None, the default, requests the documented maximum.
        tickers: True when the endpoint filters by `ticker` and each row names at most one
            stock. The vendor marks `ticker` optional on every such endpoint, and its own
            analyst-insights sample has none, so a row without one keeps a null `Symbol`
            instead of failing the request; the `poll()` key is `BenzingaId`, never `Symbol`,
            so such rows stay apart.

    Returns:
        The spec, keyed for `poll()` by `BenzingaId`.
    """
    return _list_spec(
        f"partners.benzinga.{name}",
        path,
        summary=summary,
        asset_class=AssetClass.STOCKS if tickers else ClassFromPrefix.ANY,
        symbol=KeySpec("Ticker", required=False) if tickers else None,
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
    )


BENZINGA_ANALYSTS: Final = _benzinga(
    "analysts",
    "/benzinga/v1/analysts",
    summary="Benzinga's profiles of sell-side analysts, with each analyst's firm and track record.",
    tickers=False,
)
BENZINGA_ANALYST_INSIGHTS: Final = _benzinga(
    "analyst_insights",
    "/benzinga/v1/analyst-insights",
    summary="Benzinga analyst insights on US stocks: a rating, a price target, and the reason.",
    # Below the documented 50,000: each row carries the analyst's reasoning as prose, so a page
    # of 5,000 stays at a few megabytes.
    page_size=5_000,
)
BENZINGA_RATINGS: Final = _benzinga(
    "ratings",
    "/benzinga/v1/ratings",
    summary="Analyst rating and price-target actions on US stocks, as reported by Benzinga.",
)
BENZINGA_CONSENSUS_RATINGS: Final = _list_spec(
    "partners.benzinga.consensus_ratings",
    "/benzinga/v1/consensus-ratings/{ticker}",
    summary="The consensus of analyst ratings and price targets for each US stock, from Benzinga.",
    asset_class=AssetClass.STOCKS,
    # The row's own `ticker` is its `Symbol`, as on the other Benzinga datasets; every row
    # names its stock, so a row without one is dropped and counted.
    symbol=KeySpec("Ticker"),
    poll_keys=("Symbol",),
    per_symbol=True,
)
BENZINGA_BULLS_BEARS_SAY: Final = _benzinga(
    "bulls_bears_say",
    "/benzinga/v1/bulls-bears-say",
    summary="Benzinga's bull case and bear case summaries for US stocks.",
)
BENZINGA_GUIDANCE: Final = _benzinga(
    "guidance",
    "/benzinga/v1/guidance",
    summary="Earnings and revenue guidance issued by US companies, as reported by Benzinga.",
)
BENZINGA_EARNINGS: Final = _benzinga(
    "earnings",
    "/benzinga/v1/earnings",
    summary="Earnings announcements of US companies from Benzinga, with estimates and surprises.",
)
BENZINGA_FIRMS: Final = _benzinga(
    "firms",
    "/benzinga/v1/firms",
    summary="The research firms behind Benzinga's analyst ratings.",
    tickers=False,
)
BENZINGA_NEWS: Final = _list_spec(
    "partners.benzinga.news",
    "/benzinga/v2/news",
    summary="Benzinga news articles, with headline, teaser, body, and the tickers each one names.",
    time="Published",
    time_axis=("published.gte", "published.lt"),
    symbol_param="tickers.any_of",
    ticker_list="Tickers",
    poll_mode=PollMode.APPEND,
    poll_keys=("BenzingaId", "Symbol"),
    nullable_poll_keys={"Symbol": NO_NAMED_TICKER},
    # Below the documented 50,000: each article carries its whole HTML body, often several
    # kilobytes and sometimes tens, so a full page could reach hundreds of megabytes. A page of
    # 1,000 stays at some tens of megabytes at most.
    page_size=1_000,
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
    # Wall Street Horizon's calendar is global (each event names its venue's MIC and an
    # ISIN), so its tickers are not US-market ones: they are kept as Massive writes them.
    symbols_as_written=True,
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
)
FABLE_MERCHANT_HIERARCHY: Final = _list_spec(
    "partners.fable.merchant_hierarchy",
    "/consumer-spending/eu/v1/merchant-hierarchy",
    summary="Fable Data's map from merchants to parent companies, tickers, and industries.",
    # One mapping per merchant and the date it became applicable.
    poll_keys=("LookupName", "ActiveFrom"),
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


class PartnersBenzingaNamespace(Namespace):
    """The Benzinga datasets, reached as `m.partners.benzinga`.

    Import it for annotations::

        from deephaven_massive.namespaces import PartnersBenzingaNamespace

    Its methods cover analyst profiles and their firms, analyst insights, rating actions and their
    consensus, bull and bear cases, corporate guidance, earnings, and news. Each method only builds
    a query, so it never blocks. You can call it from any thread.

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

    @raises_through(list_query)
    def analysts(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Benzinga's profiles of sell-side analysts, with each analyst's firm and track record.

        Each row holds an analyst's name, firm, ratings issued, and performance measures. The
        success rate and the average return are two of the measures. The rows describe people, so
        the dataset takes no symbols and has no `Symbol`. Select analysts with `filters`, such as by
        `full_name` or `firm_name`.

        Args:
            sort: Any field of the rows, such as `"smart_score"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"firm_name": "Wedbush"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.analysts.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                top = m.partners.benzinga.analysts(
                    sort="smart_score", order=dm.Order.DESC, limit=50
                ).fetch()
        """
        return list_query(
            self._core,
            BENZINGA_ANALYSTS,
            all_alternative=_NO_TICKER_FILTER,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def analyst_insights(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Benzinga analyst insights on US stocks: a rating, a price target, and the reason.

        Each row holds an insight's stock, firm, rating and rating action, price target, and `Date`.
        It also holds the analyst's rationale. `Symbol` is null on a row Benzinga sends without a
        ticker. The dataset has no `start` or `end`, so narrow the dates with `filters`, such as
        `date_gte`.

        Args:
            symbols: US stock tickers. Leave it out for every ticker; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"date_gte": "2026-01-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.analyst_insights.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                insights = m.partners.benzinga.analyst_insights("NVDA").fetch()
        """
        return list_query(
            self._core,
            BENZINGA_ANALYST_INSIGHTS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def ratings(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Analyst rating and price-target actions on US stocks, as reported by Benzinga.

        Each row holds one analyst's action, such as an upgrade, a downgrade, a reiteration, or a
        price-target change. The row has the previous and new rating and target, and the action's
        `Date`. `Symbol` is null on a row Benzinga sends without a ticker. The dataset has no
        `start` or `end`, so narrow the dates with `filters`, such as `date_gte`.

        Args:
            symbols: US stock tickers. Leave it out for every ticker; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"rating_action": "upgrades"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.ratings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                live = m.partners.benzinga.ratings(["AAPL", "MSFT"]).poll(60)
        """
        return list_query(
            self._core,
            BENZINGA_RATINGS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def consensus_ratings(
        self, symbols: Symbols, *, filters: Mapping[str, FilterValue] | None = None
    ) -> FetchQuery:
        """The consensus of analyst ratings and price targets for each US stock, from Benzinga.

        Each row holds one stock's count of each rating and its consensus rating. It also holds the
        stock's consensus, high, and low price targets, and its number of analysts. The consensus
        covers every rating Benzinga holds. A `date` filter limits it to the ratings of a range. The
        dataset has no `limit`.

        Args:
            symbols: US stock tickers. `dm.ALL` is refused; pass a list of tickers.
            filters: The range of the ratings to count, such as `{"date_gte": "2026-01-01"}`; leave
                it out to count every rating.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.consensus_ratings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None, `dm.ALL`, or empty;
                - a ticker isn't Massive's exact ticker;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                recent = m.partners.benzinga.consensus_ratings(
                    "AAPL", filters={"date_gte": "2026-01-01", "date_lt": "2026-07-01"}
                ).fetch()
        """
        return list_query(
            self._core,
            BENZINGA_CONSENSUS_RATINGS,
            symbols=symbols,
            all_alternative="pass a list of symbols; the endpoint takes one ticker per request",
            filters=filters,
        )

    @raises_through(list_query)
    def bulls_bears_say(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Benzinga's bull case and bear case summaries for US stocks.

        Each row holds a stock's bull case and bear case as text. `LastUpdated` says when they last
        changed. `Symbol` is null on a row Benzinga sends without a ticker.

        Args:
            symbols: US stock tickers. Leave it out for every ticker; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"last_updated"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"last_updated_gte": "2026-06-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.bulls_bears_say.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                cases = m.partners.benzinga.bulls_bears_say(["TSLA"]).fetch()
        """
        return list_query(
            self._core,
            BENZINGA_BULLS_BEARS_SAY,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def guidance(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Earnings and revenue guidance issued by US companies, as reported by Benzinga.

        Each row holds one guidance release: the stock, the fiscal period, the guided EPS and
        revenue ranges with the previous ones, and its `Date`. `Positioning` says whether the figure
        is the primary one. `Symbol` is null on a row Benzinga sends without a ticker. The dataset
        has no `start` or `end`, so narrow the dates with `filters`, such as `date_gte`.

        Args:
            symbols: US stock tickers. Leave it out for every ticker; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"positioning": "primary"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.guidance.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                outlook = m.partners.benzinga.guidance("AAPL").fetch()
        """
        return list_query(
            self._core,
            BENZINGA_GUIDANCE,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def earnings(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Earnings announcements of US companies from Benzinga, with estimates and surprises.

        Each row holds one announcement, past or scheduled: the stock, the fiscal period, and the
        date and time, with whether they're confirmed. It also holds the estimated, actual, and
        previous EPS and revenue, with their surprises. `Symbol` is null on a row Benzinga sends
        without a ticker. The dataset has no `start` or `end`, so narrow the dates with `filters`,
        such as `date_gte`.

        Args:
            symbols: US stock tickers. Leave it out for every ticker; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"importance_gte": 3}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.earnings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                upcoming = m.partners.benzinga.earnings(filters={"date_gte": "2026-11-01"}).fetch()
        """
        return list_query(
            self._core,
            BENZINGA_EARNINGS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def firms(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """The research firms behind Benzinga's analyst ratings.

        Each row holds a firm's Benzinga id, name, currency, and last update. Join its `BenzingaId`
        to the `BenzingaFirmId` of `ratings()`, `analyst_insights()`, or `analysts()` to name the
        firm of a rating, an insight, or an analyst. The rows describe firms, so the dataset takes
        no symbols and has no `Symbol`.

        Args:
            sort: Any field of the rows, such as `"name"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"benzinga_id_any_of": ["5f3ac1"]}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.firms.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                firms = m.partners.benzinga.firms().fetch()
        """
        return list_query(
            self._core,
            BENZINGA_FIRMS,
            all_alternative=_NO_TICKER_FILTER,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
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
    ) -> FetchQuery:
        """Benzinga news articles, with headline, teaser, body, and the tickers each one names.

        Each row holds one article with its publication `Timestamp`. `Tickers` lists every ticker an
        article names. `symbols` selects the articles that name any of yours. An article appears
        once for each of your tickers it names, with that ticker in `Symbol`. A `poll()` adds only
        articles it hasn't seen, so it needs a `start` or a `limit`.

        Args:
            symbols: US stock tickers. Leave it out for every article; `dm.ALL` is refused.
            start: The earliest publication time.
            end: The end of the range.
            sort: Any field of the rows, such as `"published"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"channels_any_of": ["earnings"]}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.benzinga.news.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - a bound isn't a date or a time with a time zone;
                - the range ends before it starts;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                feed = m.partners.benzinga.news(["AAPL", "NVDA"], start="2026-11-02").poll(30)
        """
        return list_query(
            self._core,
            BENZINGA_NEWS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class PartnersEtfGlobalNamespace(Namespace):
    """The ETF Global datasets, reached as `m.partners.etf_global`.

    Import it for annotations::

        from deephaven_massive.namespaces import PartnersEtfGlobalNamespace

    Its methods cover scores, holdings, fund flows, profiles, and classifications of US-listed ETFs.
    Each row describes one fund, named by its ticker in `Symbol`. `EffectiveDate` is the date the
    data is valid for. `ProcessedDate` is when ETF Global processed the row. Some issuers publish
    weeks late, and a later processing can restate an effective date. The datasets have no `start`
    or `end`, so narrow the dates with `filters`, such as `effective_date_gte`. Each method only
    builds a query, so it never blocks. You can call it from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            spy = m.partners.etf_global.constituents(
                "SPY", filters={"effective_date": "2026-09-18"}
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.etf_global"

    @raises_through(list_query)
    def analytics(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """ETF Global's quantitative risk, reward, and quality scores for US-listed ETFs.

        Each row holds a fund's risk and reward scores, its quant grade and total score, and their
        components, on one `EffectiveDate`.

        Args:
            symbols: ETF tickers. Leave it out for every fund; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"effective_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"effective_date_gte": "2026-01-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.etf_global.analytics.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                scores = m.partners.etf_global.analytics(["SPY", "QQQ"]).fetch()
        """
        return list_query(
            self._core,
            ETF_GLOBAL_ANALYTICS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def constituents(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """The holdings of US-listed ETFs from ETF Global, one row per fund, date, and holding.

        Each row holds a security the fund in `Symbol` holds on an `EffectiveDate`: its ticker in
        `ConstituentTicker`, its name and identifiers, its rank by weight, and its weight, market
        value, and shares. A fund holds hundreds of securities on many dates, so filter by date.

        Args:
            symbols: ETF tickers. Leave it out for every fund; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"weight"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"effective_date": "2026-09-18"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.etf_global.constituents.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                spy = m.partners.etf_global.constituents(
                    "SPY", filters={"effective_date": "2026-09-18"}
                ).fetch()
        """
        return list_query(
            self._core,
            ETF_GLOBAL_CONSTITUENTS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def fund_flows(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Daily fund flows, net asset value, and shares outstanding of US-listed ETFs.

        Each row holds the money that flowed into or out of a fund on an `EffectiveDate`. It also
        holds the fund's net asset value and shares outstanding that day.

        Args:
            symbols: ETF tickers. Leave it out for every fund; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"effective_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"effective_date_gte": "2026-01-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.etf_global.fund_flows.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                flows = m.partners.etf_global.fund_flows(["SPY", "IVV", "VOO"]).fetch()
        """
        return list_query(
            self._core,
            ETF_GLOBAL_FUND_FLOWS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def profiles(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """ETF Global's profiles of US ETFs: issuer, fees, assets, trading, and exposures.

        Each row holds a fund's issuer, inception, fees, assets under management, holdings count,
        and trading activity on an `EffectiveDate`. Each of its exposures, such as by sector or
        currency, is a column of JSON text.

        Args:
            symbols: ETF tickers. Leave it out for every fund; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"effective_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"effective_date_gte": "2026-01-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.etf_global.profiles.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                profile = m.partners.etf_global.profiles("ARKK").fetch()
        """
        return list_query(
            self._core,
            ETF_GLOBAL_PROFILES,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def taxonomies(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """ETF Global's classification of US-listed ETFs by strategy, structure, and exposure.

        Each row holds a fund's asset class, category, focus, region, objective, selection and
        weighting methods, leverage, rebalancing, ESG, and tax classification on an `EffectiveDate`.

        Args:
            symbols: ETF tickers. Leave it out for every fund; `dm.ALL` is refused.
            sort: Any field of the rows, such as `"effective_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"effective_date_gte": "2026-01-01"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.etf_global.taxonomies.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                kinds = m.partners.etf_global.taxonomies(["SPY", "TQQQ"]).fetch()
        """
        return list_query(
            self._core,
            ETF_GLOBAL_TAXONOMIES,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class PartnersTmxNamespace(Namespace):
    """The TMX dataset, reached as `m.partners.tmx`.

    Import it for annotations::

        from deephaven_massive.namespaces import PartnersTmxNamespace

    Its one method reads corporate events from TMX's Wall Street Horizon calendar. The calendar
    covers companies worldwide: earnings dates and calls, dividends, splits, conferences, and
    shareholder meetings. The method only builds a query, so it never blocks. You can call it from
    any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            events = m.partners.tmx.corporate_events(
                ["AAPL"], start="2026-07-01", end="2026-09-30"
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.tmx"

    @raises_through(list_query)
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
    ) -> FetchQuery:
        """Corporate events from TMX: earnings dates, dividends, conferences, and more.

        Each row holds an event's company: its ticker in `Symbol`, and its name, ISIN, and venue. It
        also holds the event's type, name, status, scheduled `Date`, and announcement link. The
        calendar is global, so tickers stay exactly as TMX writes them. A company without a ticker
        has a null `Symbol` and keeps its `Isin`.

        Args:
            symbols: Tickers as TMX writes them. Leave it out for every event; `dm.ALL` is refused.
            start: The first event day.
            end: The last event day.
            sort: Any field of the rows, such as `"type"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"type": "earnings"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.tmx.corporate_events.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a symbol is text that can't be a ticker;
                - a bound isn't a date;
                - the range ends before it starts;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                events = m.partners.tmx.corporate_events("AAPL", start="2026-07-01").fetch()
        """
        return list_query(
            self._core,
            TMX_CORPORATE_EVENTS,
            all_alternative=_NO_TICKER_FILTER,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class PartnersFableNamespace(Namespace):
    """The Fable Data datasets, reached as `m.partners.fable`.

    Import it for annotations::

        from deephaven_massive.namespaces import PartnersFableNamespace

    Fable Data totals a consumer panel's card and open-banking spending per merchant and day. The
    panel covers six European countries: the UK, Germany, France, Italy, Spain, and Austria. Fable
    maps about 3,500 merchants to their parent companies. About 250 of those are US public companies
    with tickers. To join the two datasets, match an aggregate's `Name` to a hierarchy row's
    `LookupName`. The tickers are Bloomberg's, such as `"AMZN US"`, not Massive's. So neither
    dataset takes `symbols`; filter with `filters` instead. Each method only builds a query, so it
    never blocks. You can call it from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            amazon = m.partners.fable.merchant_aggregates(
                start="2026-06-01", filters={"parent_name": "Amazon Inc"}
            ).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.fable"

    @raises_through(list_query)
    def merchant_aggregates(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Daily European consumer spending per merchant, from Fable Data's transaction panel.

        Each row holds a merchant's spending and its transaction and account counts. It also holds
        the panel sizes that normalize them. A row covers one transaction day, consumer country,
        channel, and panel. Amounts are in the row's `TransactionCurrency`, EUR or GBP. A restated
        day comes back with a later `PublishedDate`.

        Args:
            start: The first transaction day.
            end: The last transaction day.
            sort: Any field of the rows, such as `"transaction_date"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"parent_name": "Amazon Inc"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.fable.merchant_aggregates.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound isn't a date;
                - the range ends before it starts;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                uk = m.partners.fable.merchant_aggregates(
                    start="2026-06-01", filters={"user_country": "UK"}
                ).fetch()
        """
        return list_query(
            self._core,
            FABLE_MERCHANT_AGGREGATES,
            all_alternative=_NO_TICKER_FILTER,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(list_query)
    def merchant_hierarchy(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Fable Data's map from merchants to parent companies, tickers, and industries.

        Each row holds a merchant's names, its parent companies with their Bloomberg tickers,
        whether it's listed, and its sector and industry. A row covers one period, from `ActiveFrom`
        to `ActiveTo`. `0001-01-01` and `9999-12-31` mark a period with no start or no end. For a
        point-in-time map, join an aggregate's `Name` to `LookupName` on a day inside the period.

        Args:
            sort: Any field of the rows, such as `"lookup_name"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters, such as `{"listing_status": "public"}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/partners.fable.merchant_hierarchy.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                listed = m.partners.fable.merchant_hierarchy(
                    filters={"listing_status": "public"}
                ).fetch()
        """
        return list_query(
            self._core,
            FABLE_MERCHANT_HIERARCHY,
            all_alternative=_NO_TICKER_FILTER,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


class PartnersNamespace(Namespace):
    """The datasets Massive resells from its partners, reached as `m.partners`.

    Import it for annotations::

        from deephaven_massive.namespaces import PartnersNamespace

    Each partner has its own namespace:

    - `m.partners.benzinga`: analyst ratings, earnings, guidance, and news.
    - `m.partners.etf_global`: ETF analytics, holdings, flows, and profiles.
    - `m.partners.tmx`: corporate events.
    - `m.partners.fable`: European consumer spending.

    Their methods only build queries, so they never block. You can call them from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            ratings = m.partners.benzinga.ratings("AAPL").fetch()
    """

    __slots__ = ("_benzinga", "_etf_global", "_fable", "_tmx")
    _path: ClassVar[str] = "partners"

    def __init__(self, _core: SessionCore) -> None:
        """Create the four vendor namespaces.

        Args:
            _core: The session's internal core.
        """
        super().__init__(_core)
        self._benzinga = PartnersBenzingaNamespace(_core)
        self._etf_global = PartnersEtfGlobalNamespace(_core)
        self._tmx = PartnersTmxNamespace(_core)
        self._fable = PartnersFableNamespace(_core)

    @property
    def benzinga(self) -> PartnersBenzingaNamespace:
        """Every Benzinga dataset: analysts, firms, ratings, insights, guidance, and news."""
        return self._benzinga

    @property
    def etf_global(self) -> PartnersEtfGlobalNamespace:
        """Every ETF Global dataset: analytics, constituents, flows, profiles, taxonomies."""
        return self._etf_global

    @property
    def tmx(self) -> PartnersTmxNamespace:
        """The TMX datasets: scheduled corporate events."""
        return self._tmx

    @property
    def fable(self) -> PartnersFableNamespace:
        """The Fable Data datasets: European consumer spending by merchant."""
        return self._fable
