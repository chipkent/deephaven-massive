"""The `m.stocks.financials` namespace: company financial statements and valuation ratios.

Datasets: balance sheets, income statements, and cash flow statements, quarterly, annual, or
(for the flow statements) trailing twelve months, and the daily valuation ratios. A statement's
rows each describe one filer's period and list the filer's tickers, so the three statements have
no `Symbol` column and keep `Tickers`; the ratios name one ticker each.

Each dataset is one factory method that validates its arguments and returns a query; nothing
touches the network until a verb is called on the query. This module owns the namespace's
`DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in
`_session.py`). Every dataset is a REST list dataset: `symbols` is optional and only filters
the request, `dm.ALL` is refused in favour of `symbols=None`, `start` and `end` bound the
statement period, and the `sort`, `order`, `limit`, and `filters` block works as described in
the package documentation. `m.stocks` (`stocks.py`) creates the namespace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, Order, Timeframe, _require_enum
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    EVERY_TICKER,
    Namespace,
    list_query,
    ticker_key,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = [
    "DATASETS",
    "_StockFinancials",
]

_R: Final = Transport.REST
_STOCKS: Final = AssetClass.STOCKS


_STATEMENT_KEYS: Final = ("Cik", "PeriodEnd", "Timeframe")
"""One filer's statement for one period: the `poll()` key of the three statements."""


def _statement(dataset: str, path: str, *, summary: str) -> DatasetSpec:
    """Return the spec of one financial statement: no `Symbol`, axis `period_end`.

    Args:
        dataset: The dataset name below `stocks.financials`, such as `"balance_sheets"`.
        path: The statement's REST path.
        summary: The dataset's one-sentence summary.

    Returns:
        The spec.
    """
    return DatasetSpec(
        name=f"stocks.financials.{dataset}",
        summary=summary,
        asset_class=_STOCKS,
        fetch_path=path,
        time_axis=("period_end.gte", "period_end.lte"),
        symbol_param="tickers.any_of",
        poll_keys=_STATEMENT_KEYS,
        # Below the documented 50,000: a statement row holds some fifty line items, over a
        # kilobyte of JSON, so a full page would pass 60 MB; 10,000 keeps it near 15 MB.
        page_size=10_000,
    )


BALANCE_SHEETS: Final = _statement(
    "balance_sheets",
    "/stocks/financials/v1/balance-sheets",
    summary="Balance sheets of US public companies, one row per filer and reporting period.",
)

INCOME_STATEMENTS: Final = _statement(
    "income_statements",
    "/stocks/financials/v1/income-statements",
    summary="Income statements of US public companies, one row per filer and reporting period.",
)

CASH_FLOW_STATEMENTS: Final = _statement(
    "cash_flow_statements",
    "/stocks/financials/v1/cash-flow-statements",
    summary="Cash flow statements of US public companies, one row per filer and reporting period.",
)

_RATIOS: Final = "/stocks/financials/v1/ratios"

RATIOS: Final = DatasetSpec(
    name="stocks.financials.ratios",
    summary="Daily valuation, profitability, liquidity, and leverage ratios of US stocks.",
    asset_class=_STOCKS,
    fetch_path=_RATIOS,
    keys={(_R, _RATIOS): ticker_key()},
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
    page_size=50_000,
)


DATASETS: Final[tuple[DatasetSpec, ...]] = (
    BALANCE_SHEETS,
    INCOME_STATEMENTS,
    CASH_FLOW_STATEMENTS,
    RATIOS,
)
"""Every dataset of `m.stocks.financials`, one `DatasetSpec` each."""


class _StockFinancials(Namespace):
    """`m.stocks.financials`: company financial statements and daily valuation ratios.

    Created once per session by `_Stocks`; holds no state of its own, so it is safe to use from
    any thread. Each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            sheets = m.stocks.financials.balance_sheets("AAPL", timeframe=dm.Timeframe.ANNUAL)
            t = sheets.fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "stocks.financials"

    def balance_sheets(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Balance sheets of US public companies, one row per filer and reporting period.

        Each row is a company's assets, liabilities, and equity at the end of a quarter or
        fiscal year, as filed with the SEC. A filing can name several share classes, so a row
        has no `Symbol`: its `Tickers` column holds the JSON list of tickers, and `symbols`
        only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/balance-sheets`) through `fetch()` and `poll()`; `poll()`
        replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-balance_sheets,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL` or `dm.Timeframe.QUARTERLY`; None for both.
                A balance sheet is a position at the period's end, so it has no trailing
                twelve months.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            ValueError: If `timeframe` is `dm.Timeframe.TRAILING_TWELVE_MONTHS`, which the
                balance sheets do not offer.

        Example:
            ::

                q = m.stocks.financials.balance_sheets(
                    ["AAPL", "MSFT"], timeframe=dm.Timeframe.QUARTERLY, start="2023-01-01"
                )
                t = q.fetch()
        """
        if timeframe is Timeframe.TRAILING_TWELVE_MONTHS:
            raise ValueError(
                "balance sheets have no trailing twelve months; "
                "use dm.Timeframe.ANNUAL or dm.Timeframe.QUARTERLY"
            )
        return _statement_query(
            self._core,
            BALANCE_SHEETS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def income_statements(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Income statements of US public companies, one row per filer and reporting period.

        Each row is a company's revenue, expenses, earnings, and per-share figures for a
        quarter or fiscal year, as filed with the SEC. A filing can name several share
        classes, so a row has no `Symbol`: its `Tickers` column holds the JSON list of
        tickers, and `symbols` only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/income-statements`) through `fetch()` and `poll()`; `poll()`
        replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-income_statements,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`;
                None for every timeframe the endpoint holds.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.financials.income_statements(
                    "NVDA", timeframe=dm.Timeframe.ANNUAL
                ).fetch()
        """
        return _statement_query(
            self._core,
            INCOME_STATEMENTS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def cash_flow_statements(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Cash flow statements of US public companies, one row per filer and reporting period.

        Each row is a company's operating, investing, and financing cash flows for a quarter
        or fiscal year, as filed with the SEC. A filing can name several share classes, so a
        row has no `Symbol`: its `Tickers` column holds the JSON list of tickers, and `symbols`
        only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/cash-flow-statements`) through `fetch()` and `poll()`;
        `poll()` replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-cash_flow_statements,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`;
                None for every timeframe the endpoint holds.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.financials.cash_flow_statements("AAPL", start="2024-01-01").fetch()
        """
        return _statement_query(
            self._core,
            CASH_FLOW_STATEMENTS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def ratios(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily valuation, profitability, liquidity, and leverage ratios of US stocks.

        Each row is one ticker's ratios (price to earnings, return on equity, debt to equity,
        and so on) as of the row's `Date`, computed by Massive from the trailing twelve months
        of filings and the daily close. The endpoint has no date filter, so the dataset has no
        `start` or `end`. Served by REST (`/stocks/financials/v1/ratios`) through `fetch()` and
        `poll()`; `symbols` is sent as one `ticker.any_of` list, and `poll()` replaces the
        table each cycle, keyed by `Symbol` and `Date`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-ratios,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a filter
        is unknown, reserved, or of the wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            sort: The field to sort by, such as `"market_cap"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"price_to_earnings_lt": 15}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                cheap = m.stocks.financials.ratios(filters={"price_to_earnings_lt": 10}).fetch()
        """
        return list_query(
            self._core,
            RATIOS,
            all_alternative=EVERY_TICKER,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


def _statement_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None,
    timeframe: object,
    start: DateLike | None,
    end: DateLike | None,
    sort: str | None,
    order: Order | None,
    limit: int | None,
    filters: Mapping[str, FilterValue] | None,
) -> Query:
    """Build the query of one financial statement, checking its `timeframe`.

    Args:
        core: The session core.
        spec: The statement's dataset.
        symbols: The tickers that filter the request, or None.
        timeframe: The `timeframe` argument as passed.
        start: The first period end date, or None.
        end: The last period end date, or None.
        sort: The field to sort by, or None.
        order: The sort direction, or None.
        limit: The cap on the total number of rows, or None.
        filters: The `filters` mapping, or None.

    Returns:
        The `Query`.

    A `timeframe` that is not a `dm.Timeframe` raises the `TypeError` of `_require_enum`; the
    errors of `list_query` propagate.
    """
    frame = None if timeframe is None else _require_enum(timeframe, Timeframe, "timeframe")
    return list_query(
        core,
        spec,
        all_alternative=EVERY_TICKER,
        symbols=symbols,
        start=start,
        end=end,
        params={"timeframe": frame},
        sort=sort,
        order=order,
        limit=limit,
        filters=filters,
    )
