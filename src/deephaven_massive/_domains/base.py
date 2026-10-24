"""The shared dataset factory behind every namespace method.

Each namespace class (`_Stocks`, `_Options`, ...) is a thin set of typed factory methods; this
module does the work they share when a dataset method is called:

- `Namespace` is the base class of the namespace classes; it holds the session core, which
  each factory method passes to `build_query`.
- `build_query` normalizes symbols (`_symbols.normalize`), rejects `dm.ALL` where the dataset
  cannot serve it (naming the alternative), parses the time-axis bounds, and normalizes the
  typed parameters and the `filters` mapping into a hashable `Request`.
- `bar_query_class` is the run-time half of the `bars()` overloads (DESIGN § 7.1): it picks the
  most specific query class for the actual timespan and multiplier and rejects the timespans
  the asset class's endpoint does not accept.
- `list_params` validates the `sort`, `order`, and `limit` block (`…F`) shared by the REST list
  datasets.

Filter keys and value kinds are checked against the generated SDK parameter table
(`_catalog.check_filters`) synchronously at construction, as DESIGN § 9 requires; the check
is active once that table has been generated. Everything here runs synchronously on the
calling thread, makes no network call, and creates no table.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING

from deephaven_massive._catalog import check_filters
from deephaven_massive._config import require_positive_int
from deephaven_massive._dates import parse_date_like, require_aware
from deephaven_massive._enums import AssetClass, FuturesVenue, Order, Timespan, _require_enum
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    QueryBase,
    Request,
    StreamHistoryQuery,
)
from deephaven_massive._symbols import normalize
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._query import ParamValue
    from deephaven_massive._session import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["Namespace", "bar_query_class", "build_query", "list_params"]

logger = logging.getLogger(__name__)


class Namespace:
    """Base class of the namespace classes reached from a session, such as `m.stocks`.

    A namespace is created once per session by `MassiveSession.__init__` and only holds the
    session core. Its methods are the dataset factories; they validate arguments and return
    queries, and never touch the network. Thread-safe: it has no mutable state.
    """

    __slots__ = ("_core",)

    def __init__(self, core: SessionCore) -> None:
        """Bind the namespace to its session.

        Args:
            core: The session's internal core.
        """
        self._core = core


def bar_query_class(spec: DatasetSpec, timespan: object, multiplier: object) -> type[QueryBase]:
    """Pick the query class of a `bars()` call from its actual timespan and multiplier.

    With `multiplier=1`, a timespan that has a WebSocket channel (`SECOND`, `MINUTE`) gives a
    `StreamHistoryQuery`, and `DAY` on a dataset with a previous-session endpoint gives a
    `HistoryLatestQuery`. Every other combination is history only (`HistoryQuery`). A
    `timespan` that is not a `dm.Timespan` raises `TypeError` (`_require_enum`), and a
    `multiplier` that is not a positive `int` raises `TypeError` or `ValueError`
    (`require_positive_int`).

    Args:
        spec: The bars dataset.
        timespan: The `timespan` argument as passed.
        multiplier: The `multiplier` argument as passed.

    Returns:
        The query class to instantiate.

    Raises:
        ValueError: If `Timespan.DAY` is used for futures (the futures endpoint has no day
            unit; use `Timespan.SESSION`), or `Timespan.SESSION` is used outside futures.
    """
    span = _require_enum(timespan, Timespan, "timespan")
    count = require_positive_int("multiplier", multiplier)
    futures = spec.asset_class is AssetClass.FUTURES
    if futures and span is Timespan.DAY:
        raise ValueError("futures bars have no DAY timespan; use dm.Timespan.SESSION")
    if not futures and span is Timespan.SESSION:
        raise ValueError("Timespan.SESSION exists only for futures bars")
    if count == 1 and span in spec.channels:
        return StreamHistoryQuery
    if count == 1 and span is Timespan.DAY and spec.fetch_method is not None:
        return HistoryLatestQuery
    return HistoryQuery


def list_params(
    *, sort: str | None, order: Order | None, limit: int | None
) -> dict[str, ParamValue]:
    """Validate the `sort`, `order`, and `limit` block of a REST list dataset.

    `limit` is a cap on the total number of rows across pages; the page size is internal.
    An `order` that is not a `dm.Order` raises `TypeError`, and a `limit` that is not a
    positive `int` raises `TypeError` or `ValueError` (`require_positive_int`).

    Args:
        sort: The SDK's sort field name, or None for the endpoint's default.
        order: The sort direction, or None for the endpoint's default.
        limit: The maximum number of rows, or None for every row.

    Returns:
        The parameters that are set, by SDK name.

    Raises:
        TypeError: If `sort` is not a `str`.
    """
    params: dict[str, ParamValue] = {}
    if sort is not None:
        if not isinstance(sort, str):
            raise TypeError(f"sort must be a str, not {type(sort).__name__}")
        params["sort"] = sort
    if order is not None:
        params["order"] = _require_enum(order, Order, "order")
    if limit is not None:
        params["limit"] = require_positive_int("limit", limit)
    return params


def _value(name: str, value: object) -> ParamValue:
    """Normalize one parameter or filter value to its hashable stored form.

    Args:
        name: The parameter's name, for messages.
        value: The value as passed.

    Returns:
        The value; a list or tuple of strings becomes a tuple.

    Raises:
        TypeError: If the value is not a `str`, `int`, `float`, `bool`, `date`, `datetime`,
            enum member, or a sequence of `str`; for a list or tuple, the message names the
            first element that is not a `str`.

    A naive `datetime` raises the `ValueError` of `_dates.require_aware`, the same message as
    a naive time bound (DESIGN § 11).
    """
    if isinstance(value, datetime):
        require_aware(name, value, value)
    if isinstance(value, str | int | float | date | datetime):
        return value
    if isinstance(value, list | tuple):
        bad = next(((i, item) for i, item in enumerate(value) if not isinstance(item, str)), None)
        if bad is None:
            return tuple(str(item) for item in value)
        index, item = bad
        raise TypeError(
            f"{name}: element {index} of the sequence is {type(item).__name__}, not str"
        )
    raise TypeError(
        f"{name} must be a str, int, float, bool, date, datetime, or sequence of str; "
        f"got {type(value).__name__}"
    )


def _filters(spec: DatasetSpec, filters: object) -> tuple[tuple[str, ParamValue], ...]:
    """Normalize the `filters` mapping into a sorted tuple.

    Args:
        spec: The dataset, for its time-axis parameter pair.
        filters: The mapping as passed.

    Returns:
        `(name, value)` pairs sorted by name.

    Raises:
        TypeError: If `filters` is not a mapping, a key is not a `str`, or a value has an
            unsupported type.
        ValueError: If a key is one of the dataset's `start`/`end` parameters.
    """
    if not isinstance(filters, Mapping):
        raise TypeError(f"filters must be a mapping, not {type(filters).__name__}")
    pairs: list[tuple[str, ParamValue]] = []
    for key, value in filters.items():
        if not isinstance(key, str):
            raise TypeError(f"filter names must be str, not {type(key).__name__}")
        if spec.time_axis is not None and key in spec.time_axis:
            raise ValueError(f"{key} is set through start= and end=, not filters")
        pairs.append((key, _value(key, value)))
    return tuple(sorted(pairs))


def build_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None = None,
    all_alternative: str | None = None,
    timespan: Timespan | None = None,
    multiplier: int = 1,
    venue: FuturesVenue | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    filters: Mapping[str, FilterValue] | None = None,
) -> QueryBase:
    """Validate one dataset call and return its query.

    Args:
        core: The session core the query will run on.
        spec: The dataset.
        symbols: The instruments, or None for datasets without them.
        all_alternative: None when the dataset accepts `dm.ALL`; otherwise the alternative to
            name in the `ValueError`, such as `"m.stocks.snapshots(dm.ALL)"`.
        timespan: The bar timespan; for a bars dataset (`DatasetSpec.is_bars`) it also selects
            the query class through `bar_query_class`.
        multiplier: The bar multiplier.
        venue: The futures venue, or None for every venue.
        start: The lower bound of the dataset's time axis, or None.
        end: The upper bound of the dataset's time axis, or None.
        params: The dataset's other typed parameters by SDK name; None values are dropped.
        filters: The `filters` mapping by SDK parameter name.

    Returns:
        An instance of the spec's query class (or of the class `bar_query_class` picks).

    Raises:
        ValueError: If `dm.ALL` is passed where `all_alternative` is set.

    Invalid symbols, enum arguments, time bounds, parameters, or filters raise the `TypeError`
    or `ValueError` of the helper that checks them (`_symbols.normalize`, `_require_enum`,
    `_dates.parse_date_like`, `bar_query_class`).
    """
    asset_class = spec.asset_class or AssetClass.STOCKS
    normalized = None if symbols is None else normalize(asset_class, symbols)
    if isinstance(normalized, _All) and all_alternative is not None:
        raise ValueError(f"{spec.name} does not accept dm.ALL; use {all_alternative}")
    if venue is not None:
        _require_enum(venue, FuturesVenue, "venue")
    query_class = spec.query_class
    if timespan is not None:
        _require_enum(timespan, Timespan, "timespan")
    if timespan is not None and spec.is_bars:
        query_class = bar_query_class(spec, timespan, multiplier)
    request = Request(
        symbols=normalized,
        timespan=timespan,
        multiplier=multiplier,
        venue=venue,
        start=None if start is None else parse_date_like(start, "start"),
        end=None if end is None else parse_date_like(end, "end"),
        params=tuple(sorted((k, _value(k, v)) for k, v in (params or {}).items() if v is not None)),
        filters=() if filters is None else _filters(spec, filters),
    )
    check_filters(spec, dict(request.filters))
    return query_class(core, spec, request)
