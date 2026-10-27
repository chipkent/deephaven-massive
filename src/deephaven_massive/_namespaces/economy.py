"""The `m.economy` namespace: US economic data from the Federal Reserve's published series.

Datasets: treasury yields, inflation, inflation expectations, labor-market indicators, and
funding conditions, one REST list endpoint each under `/fed/v1/`.

Each dataset is one factory method on `_Economy` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. Every row is one
calendar date of the series, so the datasets have no `Symbol` and no `Timestamp`: they keep
their `Date` column (a `LocalDate`), and `start` and `end` bound it through `date.gte` and
`date.lte`. This module owns the namespace's `DatasetSpec`s, exported as `DATASETS` and composed
into the dataset catalog (`CATALOG` in `_session.py`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._namespaces.base import Namespace, build_query, list_params

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import Query
    from deephaven_massive._types import DateLike, FilterValue

__all__ = ["DATASETS", "_Economy"]

logger = logging.getLogger(__name__)

_PAGE_SIZE: Final = 50_000
"""The largest page the `/fed/v1/` endpoints serve (their documented `limit` maximum)."""


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
        page_size=_PAGE_SIZE,
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


class _Economy(Namespace):
    """`m.economy`: the US economic data datasets of a session.

    Created once by `MassiveSession` and reached as `m.economy`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            yields = m.economy.treasury_yields(start="2025-01-01").fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "economy"

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

        Invalid arguments raise the `TypeError` or `ValueError` of `list_params` and
        `build_query`.
        """
        params = list_params(sort=sort, order=order)
        query = build_query(
            self._core, spec, start=start, end=end, params=params, filters=filters, limit=limit
        )
        return cast("Query", query)

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

        Served over REST (`/fed/v1/treasury-yields`); `fetch()` returns every row in range and
        `poll()` re-fetches it, keyed by `Date`. The dataset takes no symbols, so `dm.ALL`
        does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.md#treasury_yields,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            start: The first date (a `date`, `"YYYY-MM-DD"`, or an aware `datetime`'s day);
                None for the start of the series.
            end: The last date, inclusive; None for the latest.
            sort: The wire column to sort by, such as `"date"`; None sorts by date.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix; None for
                ascending. It needs `sort`.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the date axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        A naive `datetime`, a range that ends before it starts, `order` without `sort`, or
        an unknown filter raises `ValueError`; an argument of the wrong type raises
        `TypeError`.
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

        Served over REST (`/fed/v1/inflation`); `fetch()` returns every row in range and
        `poll()` re-fetches it, keyed by `Date`. The dataset takes no symbols, so `dm.ALL`
        does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.md#inflation,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            start: The first date (a `date`, `"YYYY-MM-DD"`, or an aware `datetime`'s day);
                None for the start of the series.
            end: The last date, inclusive; None for the latest.
            sort: The wire column to sort by, such as `"date"`; None sorts by date.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix; None for
                ascending. It needs `sort`.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the date axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        A naive `datetime`, a range that ends before it starts, `order` without `sort`, or
        an unknown filter raises `ValueError`; an argument of the wrong type raises
        `TypeError`.
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

        Served over REST (`/fed/v1/inflation-expectations`); `fetch()` returns every row in
        range and `poll()` re-fetches it, keyed by `Date`. The dataset takes no symbols, so
        `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.md#inflation_expectations,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            start: The first date (a `date`, `"YYYY-MM-DD"`, or an aware `datetime`'s day);
                None for the start of the series.
            end: The last date, inclusive; None for the latest.
            sort: The wire column to sort by, such as `"date"`; None sorts by date.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix; None for
                ascending. It needs `sort`.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the date axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        A naive `datetime`, a range that ends before it starts, `order` without `sort`, or
        an unknown filter raises `ValueError`; an argument of the wrong type raises
        `TypeError`.
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

        One row per month. Served over REST (`/fed/v1/labor-market`); `fetch()` returns every
        row in range and `poll()` re-fetches it, keyed by `Date`. The dataset takes no
        symbols, so `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.md#labor_market,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            start: The first date (a `date`, `"YYYY-MM-DD"`, or an aware `datetime`'s day);
                None for the start of the series.
            end: The last date, inclusive; None for the latest.
            sort: The wire column to sort by, such as `"date"`; None sorts by date.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix; None for
                ascending. It needs `sort`.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the date axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        A naive `datetime`, a range that ends before it starts, `order` without `sort`, or
        an unknown filter raises `ValueError`; an argument of the wrong type raises
        `TypeError`.
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

        Served over REST (`/fed/v1/funding-conditions`); `fetch()` returns every row in range
        and `poll()` re-fetches it, keyed by `Date`. The dataset takes no symbols, so
        `dm.ALL` does not apply. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/economy.md#funding_conditions,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            start: The first date (a `date`, `"YYYY-MM-DD"`, or an aware `datetime`'s day);
                None for the start of the series.
            end: The last date, inclusive; None for the latest.
            sort: The wire column to sort by, such as `"date"`; None sorts by date.
            order: The direction of `sort`, sent as its `.asc` or `.desc` suffix; None for
                ascending. It needs `sort`.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoint, by wire name with dots written
                as underscores; the date axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        A naive `datetime`, a range that ends before it starts, `order` without `sort`, or
        an unknown filter raises `ValueError`; an argument of the wrong type raises
        `TypeError`.
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
