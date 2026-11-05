"""The `m.indicators` namespace: technical indicators computed by Massive from its bars.

Datasets: the simple and exponential moving averages (SMA, EMA), the relative strength index
(RSI), and the moving average convergence/divergence (MACD), for any asset class.

The indicator endpoints take one ticker per request as a path segment, so a symbol list fans
out to one request per symbol. The datasets declare no asset class: each symbol's prefix
selects its class (`X:` crypto, `C:` forex, `I:` indices, `O:` options, none for stocks). One
URL serves every class, but Massive documents the crypto form apart, without the `adjusted`
parameter, so each dataset reads two generated paths: the `{stockTicker}` template for every
class but crypto, and the `{cryptoTicker}` template for crypto (`fetch_paths_by_class`).
Every default of a signature is the default Massive's specification gives the parameter (the
windows, `timespan` day, `series_type` close, and `adjusted=True`, bars adjusted for splits,
never for dividends). `adjusted` is sent to a path that declares it; the crypto path does
not, since a pair has no splits, so `True` is left out there and `False` raises `ValueError`
for a crypto symbol. Massive does not echo the basis of an indicator, so it cannot be
checked. Each row is one indicator value of
one bar of the requested `Symbol`: the bar's window, `TimestampStart` to the exclusive
`TimestampEnd`, and its trading day, `Date`, in the day zone of the symbol's class (UTC for
crypto and forex, New York otherwise), as for that class's bars. A value of a day or longer
carries the stamp of the aggregate it is computed over, which the aggregates range puts at
00:00 of the period's first day (01:00 New York time for indices), so it is labelled as that
class's bars are. A value stamped otherwise is still returned, as Massive stamped it: it keeps
that stamp, ends one period later, has a null `Date`, and logs one WARNING per dataset per
session, since which day it covers is not known. `fetch()` and `poll()` return what Massive
reports now, the current period's still-changing value included; a row whose `TimestampEnd`
is after its `TimestampInsert` was read before its period ended.

Each dataset is one factory method on `_Indicators` that validates its arguments and returns a
`Query`; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven_massive._config import require_positive_int
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import AssetClass, Order, SeriesType, Timespan, _require_enum
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    Namespace,
    accepted_values,
    build_query,
    list_params,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["DATASETS", "_Indicators"]


def _indicator(name: str, summary: str) -> DatasetSpec:
    """Declare one indicator dataset over its two generated paths.

    Args:
        name: The indicator's path segment and dataset name, such as `"sma"`.
        summary: The one-sentence summary, the first line of the factory's docstring.

    Returns:
        The dataset: `fetch()` from `/v1/indicators/<name>/{stockTicker}` (and the
        `{cryptoTicker}` form for crypto), one request per symbol, the symbol filled from the
        request, `TimestampStart` from the row's `timestamp`, and `start`/`end` on the half-open
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
        poll_keys=("Symbol", "TimestampStart"),
        per_symbol=True,
        # One value per bar of the query's timespan: a bar's window and trading day.
        row_times=RowTimes.BAR,
        # Its underlying bars are not rows of the table, so the library never asks for them.
        fixed_params={"expand_underlying": False},
        # The latest value is computed from the latest bars, delayed on a delayed plan.
        latest_is_current=True,
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


@raises_through(_require_enum)
def _check_timespan(core: SessionCore, spec: DatasetSpec, timespan: object) -> Timespan:
    """Validate the bar size an indicator is computed over, against its generated parameter.

    The indicator paths take `timespan` as an enum from `minute` to `year`, so `SECOND` and
    `SESSION` are refused here, before any request.

    Args:
        core: The session core, whose catalog holds the generated paths.
        spec: The indicator; its fetch path has a `timespan` enum parameter.
        timespan: The `timespan` argument as passed.

    Returns:
        The timespan.

    Raises:
        TypeError: If `timespan` is not a `dm.Timespan` (`_require_enum`).
        ValueError: If the endpoint does not accept the timespan; the message lists those it
            does.
    """
    span = _require_enum(timespan, Timespan, "timespan")
    accepted = accepted_values(core, spec, "timespan")
    if span.value not in accepted:
        names = [member.name for member in Timespan if member.value in accepted]
        raise ValueError(f"indicators do not accept Timespan.{span.name}; they accept {names}")
    return span


@raises_through(require_positive_int)
def _check_window(name: str, value: object) -> int:
    """Validate one window length, naming `timespan=` when a timespan was passed in its place.

    `bars()` takes its timespan as the second positional argument, and an indicator takes its
    window there, with `timespan` keyword-only, so `sma("AAPL", dm.Timespan.MINUTE)` puts the
    timespan in the window.

    Args:
        name: The window's wire name, such as `"window"` or `"short_window"`.
        value: The argument as passed.

    Returns:
        The window, a positive `int`.

    Raises:
        TypeError: If `value` is text, a `dm.Timespan` among it; the message says that
            `timespan` is keyword-only. If it is any other value that is not an `int`
            (`require_positive_int`).
        ValueError: If the window is not positive (`require_positive_int`).
    """
    if isinstance(value, str):
        shown = f"Timespan.{value.name}" if isinstance(value, Timespan) else repr(value)
        raise TypeError(
            f"{name} must be an int, not {shown}: timespan is keyword-only, so pass it by "
            "name, as in sma('AAPL', timespan=dm.Timespan.MINUTE)"
        )
    return require_positive_int(name, value)


class _Indicators(Namespace):
    """The technical indicator datasets, reached as `m.indicators`.

    Its methods cover the simple and exponential moving averages, the relative strength index, and
    the moving average convergence/divergence. Massive computes them from its bars, for tickers of
    any asset class. Each method only builds a query, so it never blocks. You can call it from any
    thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            averages = m.indicators.sma(["AAPL", "X:BTCUSD"], start="2026-01-02").fetch()
            hourly = m.indicators.macd("AAPL", timespan=dm.Timespan.HOUR, limit=100).fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "indicators"

    @raises_through(
        _check_timespan,
        _check_window,
        require_positive_int,
        _require_enum,
        list_params,
        build_query,
    )
    def _query(
        self,
        spec: DatasetSpec,
        symbols: Symbols,
        windows: Mapping[str, object],
        *,
        timespan: Timespan,
        series_type: SeriesType,
        adjusted: bool,
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
            adjusted: Whether the bars are adjusted for splits; True is Massive's default.
            start: The lower bound of `TimestampStart`, or None.
            end: The upper bound of `TimestampStart`, or None.
            order: The timestamp order, or None.
            limit: The cap on each symbol's rows, or None.
            filters: Further query parameters, or None.

        Returns:
            The dataset's `Query`.

        Raises:
            TypeError: If an argument has the wrong type, such as a `series_type` that is not
                a `dm.SeriesType`, or a timespan passed as a window (`_check_timespan`,
                `_check_window`, `require_positive_int`, `_require_enum`, `list_params`,
                `build_query`).
            ValueError: If the endpoint does not accept the timespan, a window is not
                positive, or another argument has a value the dataset refuses
                (`_check_timespan`, `require_positive_int`, `build_query`).
        """
        span = _check_timespan(self._core, spec, timespan)
        params: dict[str, ParamValue | None] = {
            name: _check_window(name, value) for name, value in windows.items()
        }
        params["series_type"] = _require_enum(series_type, SeriesType, "series_type")
        params.update(list_params(sort=None, order=order))
        query = build_query(
            self._core,
            spec,
            symbols=symbols,
            all_alternative="pass a list of symbols",
            timespan=span,
            adjusted=adjusted,
            start=start,
            end=end,
            params=params,
            filters=filters,
            limit=limit,
        )
        return cast("Query", query)

    @raises_through(_query)
    def sma(
        self,
        symbols: Symbols,
        window: int = 50,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool = True,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The simple moving average of a ticker's bars, one row per bar.

        Each row holds the average for one bar, with the bar's start and end. `Date` is the bar's
        day, in UTC for crypto and forex and in New York time for the rest. Pass `start`, or a
        `limit` with the default newest-first order; without either, the call reads the whole
        series.

        A read through today includes the current bar's value, which changes until the bar ends.
        `t.where("TimestampEnd <= TimestampInsert")` keeps only finished values. A `poll()` in
        `APPEND` mode keeps the first value it reads of each bar, so a bar read before it ended
        never gets its finished value. On a delayed plan, reading through today raises
        `dm.EntitlementError` unless the subscription is in `MassiveConfig.delayed_subscriptions`.

        Args:
            symbols: Tickers of any asset class. `dm.ALL` is refused.
            window: The number of bars averaged.
            timespan: The bar size, `MINUTE` or longer.
            series_type: The bar price the indicator is computed from.
            adjusted: Split-adjusted bars. Crypto has no adjusted bars, so `False` fails the read of
                a crypto symbol.
            start: The first bar's start.
            end: The end of the range, of the same kind as `start`.
            order: The direction of the bars' times. The endpoints have no `sort`.
            limit: The most rows for each symbol.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.sma.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - the timespan is `SECOND` or `SESSION`;
                - a window isn't positive;
                - a bound is a time without a time zone, or the range ends before it starts;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                sma = m.indicators.sma(["AAPL", "X:BTCUSD"], start="2026-01-02").fetch()
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

    @raises_through(_query)
    def ema(
        self,
        symbols: Symbols,
        window: int = 50,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool = True,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The exponential moving average of a ticker's bars, one row per bar.

        Each row holds the average for one bar, with the bar's start and end. `Date` is the bar's
        day, in UTC for crypto and forex and in New York time for the rest. Pass `start`, or a
        `limit` with the default newest-first order; without either, the call reads the whole
        series.

        A read through today includes the current bar's value, which changes until the bar ends.
        `t.where("TimestampEnd <= TimestampInsert")` keeps only finished values. A `poll()` in
        `APPEND` mode keeps the first value it reads of each bar, so a bar read before it ended
        never gets its finished value. On a delayed plan, reading through today raises
        `dm.EntitlementError` unless the subscription is in `MassiveConfig.delayed_subscriptions`.

        Args:
            symbols: Tickers of any asset class. `dm.ALL` is refused.
            window: The number of bars the average spans.
            timespan: The bar size, `MINUTE` or longer.
            series_type: The bar price the indicator is computed from.
            adjusted: Split-adjusted bars. Crypto has no adjusted bars, so `False` fails the read of
                a crypto symbol.
            start: The first bar's start.
            end: The end of the range, of the same kind as `start`.
            order: The direction of the bars' times. The endpoints have no `sort`.
            limit: The most rows for each symbol.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.ema.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - the timespan is `SECOND` or `SESSION`;
                - a window isn't positive;
                - a bound is a time without a time zone, or the range ends before it starts;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                ema = m.indicators.ema("AAPL", 20, start="2026-01-02", end="2026-06-30").fetch()
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

    @raises_through(_query)
    def rsi(
        self,
        symbols: Symbols,
        window: int = 14,
        *,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool = True,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The relative strength index of a ticker's bars, from 0 to 100, one row per bar.

        Each row holds the index for one bar, with the bar's start and end. `Date` is the bar's day,
        in UTC for crypto and forex and in New York time for the rest. Pass `start`, or a `limit`
        with the default newest-first order; without either, the call reads the whole series.

        A read through today includes the current bar's value, which changes until the bar ends.
        `t.where("TimestampEnd <= TimestampInsert")` keeps only finished values. A `poll()` in
        `APPEND` mode keeps the first value it reads of each bar, so a bar read before it ended
        never gets its finished value. On a delayed plan, reading through today raises
        `dm.EntitlementError` unless the subscription is in `MassiveConfig.delayed_subscriptions`.

        Args:
            symbols: Tickers of any asset class. `dm.ALL` is refused.
            window: The number of bars looked back over.
            timespan: The bar size, `MINUTE` or longer.
            series_type: The bar price the indicator is computed from.
            adjusted: Split-adjusted bars. Crypto has no adjusted bars, so `False` fails the read of
                a crypto symbol.
            start: The first bar's start.
            end: The end of the range, of the same kind as `start`.
            order: The direction of the bars' times. The endpoints have no `sort`.
            limit: The most rows for each symbol.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.rsi.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - the timespan is `SECOND` or `SESSION`;
                - a window isn't positive;
                - a bound is a time without a time zone, or the range ends before it starts;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                rsi = m.indicators.rsi("AAPL", timespan=dm.Timespan.HOUR, limit=100).fetch()
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

    @raises_through(_query)
    def macd(
        self,
        symbols: Symbols,
        *,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: Timespan = Timespan.DAY,
        series_type: SeriesType = SeriesType.CLOSE,
        adjusted: bool = True,
        start: DateLike | None = None,
        end: DateLike | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The moving average convergence/divergence of a ticker's bars, one row per bar.

        Each row holds one bar's MACD line in `Value`, its signal line in `Signal`, and their
        difference in `Histogram`, with the bar's start and end. `Date` is the bar's day, in UTC for
        crypto and forex and in New York time for the rest. Pass `start`, or a `limit` with the
        default newest-first order; without either, the call reads the whole series.

        A read through today includes the current bar's value, which changes until the bar ends.
        `t.where("TimestampEnd <= TimestampInsert")` keeps only finished values. A `poll()` in
        `APPEND` mode keeps the first value it reads of each bar, so a bar read before it ended
        never gets its finished value. On a delayed plan, reading through today raises
        `dm.EntitlementError` unless the subscription is in `MassiveConfig.delayed_subscriptions`.

        Args:
            symbols: Tickers of any asset class. `dm.ALL` is refused.
            short_window: The bars of the fast exponential average.
            long_window: The bars of the slow exponential average.
            signal_window: The bars of the signal line's average.
            timespan: The bar size, `MINUTE` or longer.
            series_type: The bar price the indicator is computed from.
            adjusted: Split-adjusted bars. Crypto has no adjusted bars, so `False` fails the read of
                a crypto symbol.
            start: The first bar's start.
            end: The end of the range, of the same kind as `start`.
            order: The direction of the bars' times. The endpoints have no `sort`.
            limit: The most rows for each symbol.
            filters: Further endpoint parameters.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indicators.macd.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or `dm.ALL`;
                - a ticker isn't Massive's exact ticker;
                - the timespan is `SECOND` or `SESSION`;
                - a window isn't positive;
                - a bound is a time without a time zone, or the range ends before it starts;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                macd = m.indicators.macd("AAPL", start="2026-01-02", end="2026-06-30").fetch()
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
