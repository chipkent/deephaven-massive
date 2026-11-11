"""The `m.stocks.financials` namespace: company financial statements and valuation ratios.

Datasets: balance sheets, income statements, and cash flow statements, quarterly, annual, or (for
the flow statements) trailing twelve months, and the daily valuation ratios. A statement's rows each
describe one filer's period and list the filer's tickers, so the three statements keep `Tickers`,
and a row is listed under each named ticker it lists, in `Symbol`; the ratios name one ticker each.

Each dataset is one factory method that validates its arguments and returns a query; nothing touches
the network until a verb is called on the query. This module owns the namespace's `DatasetSpec`s,
exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in `_session.py`). Every
dataset is a REST list dataset: `symbols` is optional and only filters the request, `dm.ALL` is
refused in favour of `symbols=None`, `start` and `end` bound the statement period, and `sort`,
`order`, `limit`, and `filters` work as each dataset's page describes. `m.stocks` (`stocks.py`)
creates the namespace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import NO_NAMED_TICKER, DatasetSpec
from deephaven_massive._enums import AssetClass, Order, Timeframe, _require_enum
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    EVERY_TICKER,
    list_query,
    ticker_key,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.symbols import SymbolsLike
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive.queries import FetchQuery

__all__ = [
    "DATASETS",
    "StocksFinancialsNamespace",
]

_REST: Final = Transport.REST
_STOCKS: Final = AssetClass.STOCKS


_STATEMENT_KEYS: Final = ("Cik", "PeriodEnd", "Timeframe")
"""One filer's statement for one period: the `poll()` key of the three statements."""


def _statement(dataset: str, path: str, *, summary: str) -> DatasetSpec:
    """Return the spec of one financial statement: `Symbol` from its `Tickers`, axis `period_end`.

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
        ticker_list="Tickers",
        poll_keys=(*_STATEMENT_KEYS, "Symbol"),
        nullable_poll_keys={"Symbol": NO_NAMED_TICKER},
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
    keys={(_REST, _RATIOS): ticker_key()},
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
)


@raises_through(list_query, _require_enum)
def _statement_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: SymbolsLike | None,
    timeframe: object,
    start: DateLike | None,
    end: DateLike | None,
    sort: str | None,
    order: Order | None,
    limit: int | None,
    filters: Mapping[str, FilterValue] | None,
) -> FetchQuery:
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
        The `FetchQuery`.

    Raises:
        TypeError: If `timeframe` is not a `dm.Timeframe`, or another argument has the wrong
            type (`list_query`).
        ValueError: If an argument has a value the dataset refuses (`list_query`).
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


DATASETS: Final[tuple[DatasetSpec, ...]] = (
    BALANCE_SHEETS,
    INCOME_STATEMENTS,
    CASH_FLOW_STATEMENTS,
    RATIOS,
)
"""Every dataset of `m.stocks.financials`, one `DatasetSpec` each."""


class StocksFinancialsNamespace(Namespace):
    """The financial statement datasets, reached as `m.stocks.financials`.

    Each method returns a query of one dataset, and together they cover the balance sheets, income
    statements, and cash flow statements that companies file with the SEC, and Massive's valuation
    ratios. A method reads nothing, so it never blocks, and you can call it from any
    thread.
    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import StocksFinancialsNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            annual = m.stocks.financials.balance_sheets("AAPL", timeframe=dm.Timeframe.ANNUAL)
            sheets = annual.fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "stocks.financials"

    @raises_through(_statement_query)
    def balance_sheets(
        self,
        symbols: SymbolsLike | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Balance sheets of US public companies, one row per filer and reporting period.

        Each row holds a company's assets, liabilities, and equity at the end of a quarter or fiscal
        year, as filed with the SEC. A filing that lists several of your tickers appears once for
        each, named in `Symbol`.

        Args:
            symbols: Tickers whose filings to return; leave it out for every filing. `dm.ALL` is
                refused.
            timeframe: `dm.Timeframe.ANNUAL` or `QUARTERLY`; leave it out for both. A balance sheet
                has no trailing twelve months.
            start: The first period end date.
            end: The last period end date.
            sort: Any of Massive's field names, such as `"fiscal_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"fiscal_year_gte": 2020}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.financials.balance_sheets.md.

        Raises:
            TypeError: If an argument has the wrong type, such as a `str` timeframe.
            ValueError: If:
                - `timeframe` is `dm.Timeframe.TRAILING_TWELVE_MONTHS`;
                - `symbols` is `dm.ALL` or empty;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                annual = m.stocks.financials.balance_sheets(
                    "AAPL", timeframe=dm.Timeframe.ANNUAL
                ).fetch()
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

    @raises_through(_statement_query)
    def income_statements(
        self,
        symbols: SymbolsLike | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Income statements of US public companies, one row per filer and reporting period.

        Each row holds a company's revenue, expenses, earnings, and per-share figures, as filed with
        the SEC. The period is a quarter, a fiscal year, or the trailing twelve months. A filing
        that lists several of your tickers appears once for each, named in `Symbol`.

        Args:
            symbols: Tickers whose filings to return; leave it out for every filing. `dm.ALL` is
                refused.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`; leave it out
                for all three.
            start: The first period end date.
            end: The last period end date.
            sort: Any of Massive's field names, such as `"fiscal_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"fiscal_year_gte": 2020}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.financials.income_statements.md.

        Raises:
            TypeError: If an argument has the wrong type, such as a `str` timeframe.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                annual = m.stocks.financials.income_statements(
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

    @raises_through(_statement_query)
    def cash_flow_statements(
        self,
        symbols: SymbolsLike | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Cash flow statements of US public companies, one row per filer and reporting period.

        Each row holds a company's operating, investing, and financing cash flows, as filed with the
        SEC. The period is a quarter, a fiscal year, or the trailing twelve months. A filing that
        lists several of your tickers appears once for each, named in `Symbol`.

        Args:
            symbols: Tickers whose filings to return; leave it out for every filing. `dm.ALL` is
                refused.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`; leave it out
                for all three.
            start: The first period end date.
            end: The last period end date.
            sort: Any of Massive's field names, such as `"fiscal_year"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"fiscal_year_gte": 2020}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.financials.cash_flow_statements.md.

        Raises:
            TypeError: If an argument has the wrong type, such as a `str` timeframe.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a bound isn't a date, or `end` is before `start`;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                flows = m.stocks.financials.cash_flow_statements("AAPL", start="2024-01-01").fetch()
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

    @raises_through(list_query)
    def ratios(
        self,
        symbols: SymbolsLike | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> FetchQuery:
        """Daily valuation, profitability, liquidity, and leverage ratios of US stocks.

        Each row holds one ticker's ratios as of its `Date`, such as price to earnings, return on
        equity, and debt to equity. Massive computes them for the most recent trading day only, so
        the dataset has no `start` or `end`.

        Args:
            symbols: Stock tickers; leave it out for every ticker. `dm.ALL` is refused.
            sort: Any of Massive's field names, such as `"market_cap"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"price_to_earnings_lt": 15}`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.financials.ratios.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker;
                - `order` is given without `sort`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

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
