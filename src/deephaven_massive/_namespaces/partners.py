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

Instruments. The Benzinga datasets that take tickers and the ETF Global datasets take Massive's
stock tickers (`AAPL`, `BRK.B`), so their specs carry `AssetClass.STOCKS` and a symbol with another
class's prefix (`X:BTCUSD`) is rejected when the query is built. Their rows each describe one
instrument, which becomes the `Symbol` column (an ETF Global row's ETF, its `composite_ticker`; a
Benzinga row's `ticker`; for a TMX event or a Benzinga row without a ticker, null).
Benzinga news names several tickers per article, so it has no `Symbol`, keeps its `Tickers` column
(a `String[]`), and uses `symbols` only to filter the request. Benzinga analysts and firms, and both
Fable datasets, take no symbols at all. The TMX corporate events come from Wall Street Horizon's
global calendar, whose tickers are not US-market ones (each event names its venue's MIC and an
ISIN), so that dataset takes and keeps its tickers exactly as Massive writes them
(`DatasetSpec.symbols_as_written`).

The module is one subject although it runs past 1,000 lines: it is one namespace and its four
partner sub-namespaces, and every dataset is a REST list endpoint built the same way
(`list_query`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, ClassFromPrefix, PollMode
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import Namespace, list_query

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "_Benzinga", "_EtfGlobal", "_Fable", "_Partners", "_Tmx"]

logger = logging.getLogger(__name__)

_REST = Transport.REST

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
    # names its stock, so a row without one fails loudly.
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
    poll_mode=PollMode.APPEND,
    poll_keys=("BenzingaId",),
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

    @raises_through(list_query)
    def analysts(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Benzinga's profiles of sell-side analysts, with each analyst's firm and track record.

        One row per analyst: name, firm, ratings issued, and performance measures such as
        success rate and average return. The rows describe people, so the dataset takes no
        symbols and has no `Symbol`; select through `filters` (`full_name`, `firm_name`, ...).
        `poll()` re-reads it, keyed by `BenzingaId`.

        Args:
            sort: The wire field to sort by, such as `"smart_score"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"firm_name": "Wedbush"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.analysts.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `limit` is not positive, or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Benzinga analyst insights on US stocks: a rating, a price target, and the reason.

        One row per insight: the stock, the firm, the rating and its action, the price target,
        and the analyst's rationale, dated by `Date`. `Symbol` is null on a row Benzinga sends
        without a ticker. Narrow the dates through `filters` (`date_gte`, `date_lt`, ...): it
        has no `start`. `poll()` re-reads it, keyed by `BenzingaId`.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"date_gte": "2025-01-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.analyst_insights.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Analyst rating and price-target actions on US stocks, as reported by Benzinga.

        One row per action by one analyst at one firm: an upgrade, a downgrade, a reiteration,
        or a price-target change, with the previous and new rating and target, dated by `Date`.
        `Symbol` is null on a row Benzinga sends without a ticker. Narrow the dates through
        `filters` (`date_gte`, `date_lt`, ...): it has no `start`. `poll()` re-reads it, keyed
        by `BenzingaId`.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"rating_action": "upgrades"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.ratings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """The consensus of analyst ratings and price targets for each US stock, from Benzinga.

        One row per requested stock: the counts of each rating, the consensus rating, the
        consensus, high, and low price targets, and the number of analysts. The endpoint takes
        one ticker per request, so each symbol is its own request, and it has no `limit`. By
        default the consensus covers every rating Benzinga holds; the `date` filters restrict it
        to the ratings published in a range. `poll()` re-reads it, keyed by `Symbol`.

        Args:
            symbols: US stock tickers; required (`dm.ALL` is refused). See README.md#symbols.
            filters: The `date` range of the ratings to aggregate (`date_gte`, `date_lt`, ...);
                None aggregates every rating. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.consensus_ratings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is None, `dm.ALL`, empty, or holds a ticker that is not
                Massive's exact ticker; or a filter is unknown, set by a typed argument, or of
                the wrong kind.

        Example:
            ::

                q4 = m.partners.benzinga.consensus_ratings(
                    "AAPL", filters={"date_gte": "2024-10-01", "date_lt": "2025-01-01"}
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
    ) -> Query:
        """Benzinga's bull case and bear case summaries for US stocks.

        One row per summary: the stock, the bull and the bear case as text, and when it was last
        updated (`LastUpdated`, filterable as `last_updated_gte` and so on). `Symbol` is null on
        a row Benzinga sends without a ticker. `poll()` re-reads it, keyed by `BenzingaId`.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"last_updated"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"last_updated_gte": "2025-06-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.bulls_bears_say.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Earnings and revenue guidance issued by US companies, as reported by Benzinga.

        One row per guidance release: the stock, the fiscal period, the guided EPS and revenue
        ranges with the previous ones, and whether the figure is the primary one
        (`Positioning`), dated by `Date`. `Symbol` is null on a row Benzinga sends without a
        ticker. Narrow the dates through `filters` (`date_gte`, `date_lt`, ...): it has no
        `start`. `poll()` re-reads it, keyed by `BenzingaId`.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"positioning": "primary"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.guidance.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Earnings announcements of US companies from Benzinga, with estimates and surprises.

        One row per announcement, past or scheduled: the stock, the fiscal period, the date and
        time and whether they are confirmed, and the estimated, actual, and previous EPS and
        revenue with their surprises. `Symbol` is null on a row Benzinga sends without a ticker.
        Narrow the dates through `filters` (`date_gte`, `date_lt`, ...): it has no `start`.
        `poll()` re-reads it, keyed by `BenzingaId`.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"importance_gte": 3}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.earnings.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

        Example:
            ::

                upcoming = m.partners.benzinga.earnings(filters={"date_gte": "2025-07-01"}).fetch()
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
    ) -> Query:
        """The research firms behind Benzinga's analyst ratings.

        One row per firm: its Benzinga id, name, currency, and last update. Join `BenzingaId` to
        the `BenzingaFirmId` of `ratings()`, `analyst_insights()`, or `analysts()`. The rows
        describe firms, so the dataset takes no symbols and has no `Symbol`. `poll()` re-reads
        it, keyed by `BenzingaId`.

        Args:
            sort: The wire field to sort by, such as `"name"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"benzinga_id_any_of": ["5f3ac1"]}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.firms.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `limit` is not positive, or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Benzinga news articles, with headline, teaser, body, and the tickers each one names.

        One row per article, at its publication `Timestamp`. An article names any number of
        tickers, so the table has no `Symbol` but a `Tickers` array, and `symbols` selects the
        articles that name any of them: `t.where("Tickers != null && inObj(`AAPL`, Tickers)")`
        keeps one ticker's. `poll()` appends the articles it has not seen, keyed by
        `BenzingaId`, and needs `start` or `limit`, since each cycle reads the whole range; a
        fixed `start` widens the window of a poll that runs for days.

        Args:
            symbols: US stock tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            start: The earliest publication time; `poll()` needs it or a `limit`.
                See README.md#start-and-end.
            end: The end of the range. See README.md#start-and-end.
            sort: The wire field to sort by, such as `"published"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"channels_any_of": ["earnings"]}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.benzinga.news.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; a bound cannot be parsed or is naive, or the range ends before it
                starts; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

        Example:
            ::

                feed = m.partners.benzinga.news(["AAPL", "NVDA"], start="2025-06-02").poll(30)
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

    @raises_through(list_query)
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

        One row per fund and date: the risk and reward scores, the quant grade and total score,
        and their components. The fund is `Symbol`, the data's date `EffectiveDate`, and
        `ProcessedDate` when ETF Global processed it; narrow dates through `filters`. `poll()`
        re-reads it, keyed by `Symbol`, `EffectiveDate`, and `ProcessedDate`.

        Args:
            symbols: ETF tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"effective_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"effective_date_gte": "2025-01-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.etf_global.analytics.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """The holdings of US-listed ETFs from ETF Global, one row per fund, date, and holding.

        Each row is one security a fund (`Symbol`) holds on a date: its ticker
        (`ConstituentTicker`), name, identifiers, rank by weight, weight, market value, and
        shares. A fund holds hundreds of securities on many dates, so filter by date. `poll()`
        re-reads it, keyed by `Symbol`, `EffectiveDate`, `ProcessedDate`, and `ConstituentRank`.

        Args:
            symbols: ETF tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"effective_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"effective_date": "2025-09-18"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.etf_global.constituents.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

        Example:
            ::

                spy = m.partners.etf_global.constituents(
                    "SPY", filters={"effective_date": "2025-09-18"}
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
    ) -> Query:
        """Daily fund flows, net asset value, and shares outstanding of US-listed ETFs.

        One row per fund and date, from ETF Global: the money that flowed into or out of the
        fund, its net asset value, and its shares outstanding. `poll()` re-reads it, keyed by
        `Symbol`, `EffectiveDate`, and `ProcessedDate`.

        Args:
            symbols: ETF tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"effective_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"effective_date_gte": "2025-01-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.etf_global.fund_flows.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """ETF Global's profiles of US ETFs: issuer, fees, assets, trading, and exposures.

        One row per fund and date: issuer, inception, fees, assets under management, holdings
        count, trading activity, and the sector, geographic, currency, and other exposures, each
        exposure a JSON text column. `poll()` re-reads it, keyed by `Symbol`, `EffectiveDate`,
        and `ProcessedDate`.

        Args:
            symbols: ETF tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"effective_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"effective_date_gte": "2025-01-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.etf_global.profiles.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """ETF Global's classification of US-listed ETFs by strategy, structure, and exposure.

        One row per fund and date: asset class, category, focus, region, objective, selection
        and weighting method, leverage, rebalancing, ESG, and tax classification. `poll()`
        re-reads it, keyed by `Symbol`, `EffectiveDate`, and `ProcessedDate`.

        Args:
            symbols: ETF tickers; None, the default, for no ticker filter (`dm.ALL` is
                refused). See README.md#symbols.
            sort: The wire field to sort by, such as `"effective_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"effective_date_gte": "2025-01-01"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.etf_global.taxonomies.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
    ) -> Query:
        """Corporate events from TMX: earnings dates, dividends, conferences, and more.

        One row per event: the company's ticker (`Symbol`), name, ISIN, and venue, and the
        event's type, name, status, scheduled `Date`, and announcement link. The calendar is
        global: tickers are taken exactly as TMX writes them, and a company without one has a
        null `Symbol` but keeps its `Isin`. Events are dated by day, so there is no `Timestamp`.
        `poll()` re-reads it, keyed by `TmxRecordId`.

        Args:
            symbols: Tickers as TMX writes them; None, the default, for no ticker filter
                (`dm.ALL` is refused). See README.md#symbols.
            start: The first event day. See README.md#start-and-end.
            end: The end of the range. See README.md#start-and-end.
            sort: The wire field to sort by, such as `"date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"type": "earnings"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.tmx.corporate_events.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds a ticker that is not Massive's
                exact ticker; a bound cannot be parsed or is naive, or the range ends before it
                starts; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

        Example:
            ::

                events = m.partners.tmx.corporate_events("AAPL", start="2025-07-01").fetch()
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
    ) -> Query:
        """Daily European consumer spending per merchant, from Fable Data's transaction panel.

        One row per merchant, transaction day, consumer country, channel, and panel: spend,
        transaction and account counts, and the panel sizes that normalize them, in the row's
        `TransactionCurrency` (EUR or GBP). A restated day returns with a later `PublishedDate`.
        The tickers are Bloomberg's, so it takes no `symbols`: filter by name through `filters`.
        `poll()` re-reads it, keyed by every dimension column, from `TransactionDate` and
        `PublishedDate` to `TransactionCurrency`.

        Args:
            start: The first transaction day. See README.md#start-and-end.
            end: The end of the range. See README.md#start-and-end.
            sort: The wire field to sort by, such as `"transaction_date"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"parent_name": "Amazon Inc"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.fable.merchant_aggregates.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If a bound cannot be parsed or is naive, or the range ends before it
                starts; `limit` is not positive; or a filter is unknown, set by a typed
                argument, or of the wrong kind.

        Example:
            ::

                uk = m.partners.fable.merchant_aggregates(
                    start="2025-06-01", filters={"user_country": "UK"}
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
    ) -> Query:
        """Fable Data's map from merchants to parent companies, tickers, and industries.

        One row per merchant and period of validity: its names, its parent companies with their
        Bloomberg tickers, whether it is listed, and its sector and industry. Join an
        aggregate's `Name` to `LookupName` between `ActiveFrom` and `ActiveTo` (`0001-01-01` and
        `9999-12-31` mean unbounded) for a point-in-time map. `poll()` re-reads it, keyed by
        `LookupName` and `ActiveFrom`.

        Args:
            sort: The wire field to sort by, such as `"lookup_name"`. See
                README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: Such as `{"listing_status": "public"}`. See README.md#filters.

        Returns:
            A `Query`. Columns: docs/datasets/partners.fable.merchant_hierarchy.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `limit` is not positive, or a filter is unknown, set by a typed
                argument, or of the wrong kind.

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
