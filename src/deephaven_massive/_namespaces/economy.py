"""The `m.economy` namespace: US economic data from the Federal Reserve's published series.

Datasets: treasury yields, inflation, inflation expectations, labor-market indicators, and
funding conditions, one REST list endpoint each under `/fed/v1/`.

Each dataset is one factory method on `Economy` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. Every row is one
calendar date of the series, so the datasets have no `Symbol` and no `Timestamp`: they keep
their `Date` column (ISO `yyyy-MM-dd` text, as every date the library returns), and `start` and
`end` bound it through `date.gte` and `date.lte`. This module owns the namespace's
`DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in
`_session.py`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._errors import raises_through
from deephaven_massive._namespaces.query_building import Namespace, build_query, list_params

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import Query
    from deephaven_massive._types import DateLike, FilterValue

__all__ = ["DATASETS", "Economy"]


def _series(name: str, path: str, summary: str) -> DatasetSpec:
    """Declare one economic series: a REST list endpoint whose rows are calendar dates.

    Args:
        name: The dataset's name below the namespace, such as `"inflation"`.
        path: The endpoint, such as `"/fed/v1/inflation"`.
        summary: The one-sentence summary, the first line of the factory's docstring.

    Returns:
        The dataset: fetched and polled from `path`, its `start`/`end` mapped to the inclusive
        `date.gte`/`date.lte` pair, and keyed by `Date` in a `poll()` table.
    """
    return DatasetSpec(
        name=f"economy.{name}",
        summary=summary,
        fetch_path=path,
        time_axis=("date.gte", "date.lte"),
        poll_keys=("Date",),
    )


TREASURY_YIELDS: Final = _series(
    "treasury_yields",
    "/fed/v1/treasury-yields",
    "US Treasury yields at constant maturities from one month to 30 years, one row per day.",
)
INFLATION: Final = _series(
    "inflation",
    "/fed/v1/inflation",
    "US inflation: CPI, core CPI, and the PCE price indexes and spending, one row per month.",
)
INFLATION_EXPECTATIONS: Final = _series(
    "inflation_expectations",
    "/fed/v1/inflation-expectations",
    "US inflation expectations from market prices and models, one row per day.",
)
LABOR_MARKET: Final = _series(
    "labor_market",
    "/fed/v1/labor-market",
    "US labor-market indicators: unemployment, participation, earnings, and job openings.",
)
FUNDING_CONDITIONS: Final = _series(
    "funding_conditions",
    "/fed/v1/funding-conditions",
    "US short-term funding conditions: policy, overnight, and repo rates, one row per day.",
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TREASURY_YIELDS,
    INFLATION,
    INFLATION_EXPECTATIONS,
    LABOR_MARKET,
    FUNDING_CONDITIONS,
)
"""Every dataset of `m.economy`, one `DatasetSpec` each (for example `economy.inflation`)."""


class Economy(Namespace):
    """The US economic datasets, reached as `m.economy`.

    Its methods cover Treasury yields, inflation, inflation expectations, the labor market, and
    short-term funding conditions. They take no symbols, and each row is one date of the series.
    Each method only builds a query, so it never blocks. You can call it from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            yields = m.economy.treasury_yields(start="2026-01-02").fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "economy"

    @raises_through(build_query, list_params)
    def _query(
        self,
        spec: DatasetSpec,
        *,
        start: DateLike | None,
        end: DateLike | None,
        sort: str | None,
        order: Order | None,
        limit: int | None,
        filters: Mapping[str, FilterValue] | None,
    ) -> Query:
        """Validate the arguments every economy dataset shares and return its query.

        Args:
            spec: The dataset.
            start: The first date, or None.
            end: The last date, inclusive, or None.
            sort: The sort columns, or None.
            order: The sort direction, or None.
            limit: The total-row cap, or None.
            filters: Further query parameters, or None.

        Returns:
            The dataset's `Query`.

        Raises:
            TypeError: If an argument has the wrong type (`list_params`, `build_query`).
            ValueError: If an argument has a value the dataset refuses (`build_query`).
        """
        params = list_params(sort=sort, order=order)
        query = build_query(
            self._core, spec, start=start, end=end, params=params, filters=filters, limit=limit
        )
        return cast("Query", query)

    @raises_through(_query)
    def treasury_yields(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """US Treasury yields at constant maturities from one month to 30 years, one row per day.

        Each row holds one day's yields, one column per maturity, such as `Yield10Year`.

        Args:
            start: The first date.
            end: The last date.
            sort: Any of Massive's field names, such as `"yield_10_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.treasury_yields.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound is a time instead of a date, or the range ends before it starts;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                yields = m.economy.treasury_yields(start="2026-01-02").fetch()
        """
        return self._query(
            TREASURY_YIELDS,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(_query)
    def inflation(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """US inflation: CPI, core CPI, and the PCE price indexes and spending, one row per month.

        Each row holds one month's CPI, core CPI, and year-over-year CPI, and its PCE, core PCE, and
        PCE spending.

        Args:
            start: The first date.
            end: The last date.
            sort: Any of Massive's field names, such as `"cpi_year_over_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.inflation.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound is a time instead of a date, or the range ends before it starts;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                cpi = m.economy.inflation(start="2020-01-01").fetch()
        """
        return self._query(
            INFLATION,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(_query)
    def inflation_expectations(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """US inflation expectations from market prices and models, one row per day.

        Each row holds one day's expected inflation over several horizons. Market-price columns,
        such as `Market10Year`, sit beside model columns, such as `Model10Year`.

        Args:
            start: The first date.
            end: The last date.
            sort: Any of Massive's field names, such as `"market_10_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.inflation_expectations.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound is a time instead of a date, or the range ends before it starts;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                expected = m.economy.inflation_expectations(start="2026-01-02").fetch()
        """
        return self._query(
            INFLATION_EXPECTATIONS,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(_query)
    def labor_market(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """US labor-market indicators: unemployment, participation, earnings, and job openings.

        Each row holds one month's unemployment rate, labor force participation rate, average hourly
        earnings, and job openings.

        Args:
            start: The first date.
            end: The last date.
            sort: Any of Massive's field names, such as `"unemployment_rate"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.labor_market.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound is a time instead of a date, or the range ends before it starts;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                jobs = m.economy.labor_market(start="2020-01-01").fetch()
        """
        return self._query(
            LABOR_MARKET,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(_query)
    def funding_conditions(
        self,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """US short-term funding conditions: policy, overnight, and repo rates, one row per day.

        Each row holds one day's fed funds target range and effective rate, the overnight bank
        funding rate, SOFR, the tri-party general collateral rate, interest on reserve balances,
        commercial paper rates, and the Fed's repo amounts.

        Args:
            start: The first date.
            end: The last date.
            sort: Any of Massive's field names, such as `"effective_fed_funds_rate"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.funding_conditions.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - a bound is a time instead of a date, or the range ends before it starts;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                rates = m.economy.funding_conditions(start="2026-01-02").fetch()
        """
        return self._query(
            FUNDING_CONDITIONS,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )
