"""The `m.indicators` namespace: technical indicators computed by Massive from its bars.

Datasets: the simple and exponential moving averages (SMA, EMA), the relative strength index
(RSI), and the moving average convergence/divergence (MACD), for any asset class.

The indicator endpoints take one ticker per request as a path segment, so a symbol list fans
out to one request per symbol. The datasets declare no asset class: each symbol's prefix
selects its class (`X:` crypto, `C:` forex, `I:` indices, `O:` options, none for stocks). One
URL serves every class, but Massive documents the crypto form apart, without the `adjusted`
parameter, so each dataset reads two generated paths: the `{stockTicker}` template for every
class but crypto, and the `{cryptoTicker}` template for crypto (`fetch_paths_by_class`).
`adjusted` defaults to None, which sends nothing; an explicit value goes only to a path that
declares it, so it raises `ValueError` for a crypto symbol. Each row is one indicator value
at one bar's start, `Timestamp`, for the requested `Symbol`.

Each dataset is one factory method on `_Indicators` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._config import require_positive_int
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, Order, SeriesType, Timespan, _require_enum
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import Namespace, build_query

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._query import Query
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "_Indicators"]

logger = logging.getLogger(__name__)

_PAGE_SIZE: Final = 5000
"""The largest page the indicator endpoints serve (their documented `limit` maximum)."""

_NO_TIMESPAN: Final = frozenset({Timespan.SECOND, Timespan.SESSION})
"""Timespans the indicator endpoints do not accept: they compute from minute bars and up."""


def _indicator(name: str, summary: str) -> DatasetSpec:
    """Declare one indicator dataset over its two generated paths.

    Args:
        name: The indicator's path segment and dataset name, such as `"sma"`.
        summary: The one-sentence summary, the first line of the factory's docstring.

    Returns:
        The dataset: `fetch()` from `/v1/indicators/<name>/{stockTicker}` (and the
        `{cryptoTicker}` form for crypto), one request per symbol, the symbol filled from the
        request, `Timestamp` from the row's `timestamp`, and `start`/`end` on the half-open
        `timestamp.gte`/`timestamp.lt` pair.
    """
    stock = f"/v1/indicators/{name}/{{stockTicker}}"
    crypto = f"/v1/indicators/{name}/{{cryptoTicker}}"
    return DatasetSpec(
        name=f"indicators.{name}",
        summary=summary,
        fetch_path=stock,
        fetch_paths_by_class={AssetClass.CRYPTO: crypto},
        keys={
            (Transport.REST, stock): SourceKeys(
                symbol=KeySpec("stockTicker", from_request=True), time=KeySpec("Timestamp")
            ),
            (Transport.REST, crypto): SourceKeys(
                symbol=KeySpec("cryptoTicker", from_request=True), time=KeySpec("Timestamp")
            ),
        },
        time_axis=("timestamp.gte", "timestamp.lt"),
        poll_keys=("Symbol", "Timestamp"),
        per_symbol=True,
        page_size=_PAGE_SIZE,
    )


SMA: Final = _indicator("sma", "The simple moving average of a ticker's bars, one row per bar.")
EMA: Final = _indicator(
    "ema", "The exponential moving average of a ticker's bars, one row per bar."
)
RSI: Final = _indicator(
    "rsi", "The relative strength index of a ticker's bars, from 0 to 100, one row per bar."
)
MACD: Final = _indicator(
    "macd", "The moving average convergence/divergence of a ticker's bars, one row per bar."
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (SMA, EMA, RSI, MACD)
"""Every dataset of `m.indicators`, one `DatasetSpec` each (for example `indicators.sma`)."""


def _check_timespan(timespan: object) -> Timespan:
    """Validate the bar size an indicator is computed over.

    Args:
        timespan: The `timespan` argument as passed.

    Returns:
        The timespan.

    Raises:
        ValueError: If the timespan is `SECOND` or `SESSION`, which the endpoints do not
            accept.

    A `timespan` that is not a `dm.Timespan` raises the `TypeError` of `_require_enum`.
    """
    span = _require_enum(timespan, Timespan, "timespan")
    if span in _NO_TIMESPAN:
        raise ValueError(f"indicators do not accept Timespan.{span.name}; use MINUTE or longer")
    return span


class _Indicators(Namespace):
    """`m.indicators`: the technical indicator datasets of a session.

    Created once by `MassiveSession` and reached as `m.indicators`. It holds no state of its
    own, so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            sma = m.indicators.sma(["AAPL", "X:BTCUSD"], 50, start="2025-01-01").fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "indicators"

    def _query(
        self,
        spec: DatasetSpec,
        symbols: Symbols,
        windows: Mapping[str, object],
        *,
        timespan: Timespan,
        series_type: SeriesType,
        adjusted: bool | None,
        start: DateLike | None,
        end: DateLike | None,
        order: Order | None,
        limit: int | None,
        filters: Mapping[str, FilterValue] | None,
    ) -> Query:
        """Validate the arguments every indicator shares and return its query.

        Args:
            spec: The dataset.
            symbols: The tickers.
            windows: The indicator's window lengths by wire name (`window`, or the three MACD
                windows), each a positive `int`.
            timespan: The bar timespan.
            series_type: The bar price the indicator is computed from.
            adjusted: Whether the bars are adjusted for splits, or None for Massive's default.
            start: The lower bound of `Timestamp`, or None.
            end: The upper bound of `Timestamp`, or None.
            order: The timestamp order, or None.
            limit: The total-row cap, or None.
            filters: Further query parameters, or None.

        Returns:
            The dataset's `Query`.

        Invalid timespans, windows, a `series_type` that is not a `dm.SeriesType`, or other
        invalid arguments raise the `TypeError` or `ValueError` of `_check_timespan`,
        `require_positive_int`, `_require_enum`, and `build_query`.
        """
        span = _check_timespan(timespan)
        params: dict[str, ParamValue | None] = {
            name: require_positive_int(name, value) for name, value in windows.items()
        }
        params["series_type"] = _require_enum(series_type, SeriesType, "series_type")
        # The paths have their own `order` and no `sort`, so `order` stands alone.
        params["order"] = None if order is None else _require_enum(order, Order, "order")
        query = build_query(
            self._core,
            spec,
            symbols=symbols,
            all_alternative="a list of symbols",
            timespan=span,
            adjusted=adjusted,
            start=start,
            end=end,
            params=params,
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    def sma(
        self,
        symbols: Symbols,
        window: int,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The simple moving average of a ticker's bars, one row per bar.

        Served over REST (`/v1/indicators/sma/{ticker}`), one request per symbol; `fetch()`
        returns every value in range and `poll()` re-fetches it, keyed by `Symbol` and
        `Timestamp`. `dm.ALL` is not accepted: the endpoint takes one ticker per request. Its
        columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.md#sma,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes (each prefix selects the class), or a
                Deephaven table with a `Symbol` column.
            window: The number of bars averaged, a positive `int`.
            timespan: The bar size the average is computed over; `MINUTE` or longer.
            series_type: The bar price averaged.
            adjusted: Whether the bars are adjusted for splits; None sends nothing and
                leaves Massive's default. The crypto endpoint has no such parameter, so an
                explicit value fails the verb with `ValueError` for a crypto symbol.
            start: The first bar start (a date's 00:00 in the class day zone, or an aware
                `datetime`); None for the endpoint's default range.
            end: The end of the range: a date includes that whole day, a `datetime` is
                exclusive; None for the latest.
            order: The timestamp order, sent as the endpoints' own `order` parameter (they
                have no `sort`); None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoints, by wire name with dots written
                as underscores; the timestamp axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, a timespan the endpoints do not accept, a window that is not positive, a
        naive `datetime`, a range that ends before it starts, or an unknown filter raises
        `ValueError`; an argument of the wrong type raises `TypeError`.
        """
        return self._query(
            SMA,
            symbols,
            {"window": window},
            timespan=timespan,
            series_type=series_type,
            adjusted=adjusted,
            start=start,
            end=end,
            order=order,
            limit=limit,
            filters=filters,
        )

    def ema(
        self,
        symbols: Symbols,
        window: int,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The exponential moving average of a ticker's bars, one row per bar.

        Served over REST (`/v1/indicators/ema/{ticker}`), one request per symbol; `fetch()`
        returns every value in range and `poll()` re-fetches it, keyed by `Symbol` and
        `Timestamp`. `dm.ALL` is not accepted: the endpoint takes one ticker per request. Its
        columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.md#ema,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes (each prefix selects the class), or a
                Deephaven table with a `Symbol` column.
            window: The number of bars the average spans, a positive `int`.
            timespan: The bar size the average is computed over; `MINUTE` or longer.
            series_type: The bar price averaged.
            adjusted: Whether the bars are adjusted for splits; None sends nothing and
                leaves Massive's default. The crypto endpoint has no such parameter, so an
                explicit value fails the verb with `ValueError` for a crypto symbol.
            start: The first bar start (a date's 00:00 in the class day zone, or an aware
                `datetime`); None for the endpoint's default range.
            end: The end of the range: a date includes that whole day, a `datetime` is
                exclusive; None for the latest.
            order: The timestamp order, sent as the endpoints' own `order` parameter (they
                have no `sort`); None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoints, by wire name with dots written
                as underscores; the timestamp axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, a timespan the endpoints do not accept, a window that is not positive, a
        naive `datetime`, a range that ends before it starts, or an unknown filter raises
        `ValueError`; an argument of the wrong type raises `TypeError`.
        """
        return self._query(
            EMA,
            symbols,
            {"window": window},
            timespan=timespan,
            series_type=series_type,
            adjusted=adjusted,
            start=start,
            end=end,
            order=order,
            limit=limit,
            filters=filters,
        )

    def rsi(
        self,
        symbols: Symbols,
        window: int,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The relative strength index of a ticker's bars, from 0 to 100, one row per bar.

        Served over REST (`/v1/indicators/rsi/{ticker}`), one request per symbol; `fetch()`
        returns every value in range and `poll()` re-fetches it, keyed by `Symbol` and
        `Timestamp`. `dm.ALL` is not accepted: the endpoint takes one ticker per request. Its
        columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.md#rsi,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes (each prefix selects the class), or a
                Deephaven table with a `Symbol` column.
            window: The number of bars the index looks back over, a positive `int`.
            timespan: The bar size the index is computed over; `MINUTE` or longer.
            series_type: The bar price the index is computed from.
            adjusted: Whether the bars are adjusted for splits; None sends nothing and
                leaves Massive's default. The crypto endpoint has no such parameter, so an
                explicit value fails the verb with `ValueError` for a crypto symbol.
            start: The first bar start (a date's 00:00 in the class day zone, or an aware
                `datetime`); None for the endpoint's default range.
            end: The end of the range: a date includes that whole day, a `datetime` is
                exclusive; None for the latest.
            order: The timestamp order, sent as the endpoints' own `order` parameter (they
                have no `sort`); None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoints, by wire name with dots written
                as underscores; the timestamp axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, a timespan the endpoints do not accept, a window that is not positive, a
        naive `datetime`, a range that ends before it starts, or an unknown filter raises
        `ValueError`; an argument of the wrong type raises `TypeError`.
        """
        return self._query(
            RSI,
            symbols,
            {"window": window},
            timespan=timespan,
            series_type=series_type,
            adjusted=adjusted,
            start=start,
            end=end,
            order=order,
            limit=limit,
            filters=filters,
        )

    def macd(
        self,
        symbols: Symbols,
        *,
        short_window: int,
        long_window: int,
        signal_window: int,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The moving average convergence/divergence of a ticker's bars, one row per bar.

        Each row holds the MACD line (`Value`), its signal line, and their difference (the
        histogram). Served over REST (`/v1/indicators/macd/{ticker}`), one request per
        symbol; `fetch()` returns every value in range and `poll()` re-fetches it, keyed by
        `Symbol` and `Timestamp`. `dm.ALL` is not accepted: the endpoint takes one ticker per
        request. Its columns, and what each means, are in
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.md#macd,
        in the web UI's column tooltips, and in the query's `columns()`.

        Args:
            symbols: The tickers, of any asset classes (each prefix selects the class), or a
                Deephaven table with a `Symbol` column.
            short_window: The bars of the fast exponential average, a positive `int`.
            long_window: The bars of the slow exponential average, a positive `int`.
            signal_window: The bars of the signal line's average, a positive `int`.
            timespan: The bar size the averages are computed over; `MINUTE` or longer.
            series_type: The bar price the averages are computed from.
            adjusted: Whether the bars are adjusted for splits; None sends nothing and
                leaves Massive's default. The crypto endpoint has no such parameter, so an
                explicit value fails the verb with `ValueError` for a crypto symbol.
            start: The first bar start (a date's 00:00 in the class day zone, or an aware
                `datetime`); None for the endpoint's default range.
            end: The end of the range: a date includes that whole day, a `datetime` is
                exclusive; None for the latest.
            order: The timestamp order, sent as the endpoints' own `order` parameter (they
                have no `sort`); None for the endpoint's default.
            limit: The maximum total number of rows across pages; None for every row. The
                page size is internal.
            filters: Further query parameters of the endpoints, by wire name with dots written
                as underscores; the timestamp axis is set only through `start` and `end`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        `dm.ALL`, a timespan the endpoints do not accept, a window that is not positive, a
        naive `datetime`, a range that ends before it starts, or an unknown filter raises
        `ValueError`; an argument of the wrong type raises `TypeError`.
        """
        return self._query(
            MACD,
            symbols,
            {
                "short_window": short_window,
                "long_window": long_window,
                "signal_window": signal_window,
            },
            timespan=timespan,
            series_type=series_type,
            adjusted=adjusted,
            start=start,
            end=end,
            order=order,
            limit=limit,
            filters=filters,
        )
