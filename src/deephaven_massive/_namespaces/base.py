"""The shared dataset factory behind every namespace method.

Each namespace class (`_Stocks`, `_Options`, ...) is a thin set of typed factory methods; this
module does the work they share when a dataset method is called:

- `Namespace` is the base class of the namespace classes; it holds the session core, which
  each factory method passes to `build_query`.
- `build_query` normalizes symbols (`normalize` in `_datasets/symbols.py`; a reactive
  `Table` is checked for its `Symbol` column and boxed in a `SymbolTable` here), rejects
  `dm.ALL` where the dataset cannot serve it (naming the alternative), parses the time-axis
  bounds, and normalizes the typed parameters and the `filters` mapping into a hashable
  `Request`.
- `query_class` derives a dataset's query class from the transports its spec declares, so no
  spec names a class and the dataset layer never refers to the query classes.
- `bar_query_class` is the run-time half of the `bars()` overloads: it picks the most specific query
  class for the actual timespan and multiplier and rejects the timespans the asset class's endpoint
  does not accept.
- `list_params` validates the `sort` and `order` parameters of the `…F` block shared by the
  REST list datasets; its `limit` goes to `build_query(limit=...)` (`Request.limit`), because
  it is a total-row cap, not the endpoint's page-size parameter.

Filter keys and value kinds are checked against the generated parameters of the dataset's REST
paths (`Catalog.check_filters` in `_datasets/catalog.py`) synchronously at construction, so a
bad filter fails before any verb runs, and the `Request` stores each value as the check parsed
it (ISO-8601 text of a date parameter becomes a `date`), never the caller's text. Everything
here runs synchronously on the calling thread, makes no network call, and creates no table.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING, ClassVar

from deephaven.table import Table

from deephaven_massive._config import require_positive_int
from deephaven_massive._datasets.catalog import filter_key, param_family
from deephaven_massive._datasets.dates import check_order, parse_date_like, require_aware
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.symbols import SymbolTable, normalize
from deephaven_massive._enums import AssetClass, FuturesVenue, Order, Timespan, _require_enum
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._types import _All, override

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = ["Namespace", "bar_query_class", "build_query", "list_params", "query_class"]

logger = logging.getLogger(__name__)


class Namespace:
    """Base class of the namespace classes reached from a session, such as `m.stocks`.

    A namespace is created once per session by `MassiveSession.__init__` and only holds the
    session core. Its methods are the dataset factories; they validate arguments and return
    queries, and never touch the network. Thread-safe: it has no mutable state.
    """

    __slots__ = ("_core",)

    _path: ClassVar[str] = ""
    """The namespace's path from the session, such as `"stocks"` or `"partners.benzinga"`."""

    def __init__(self, core: SessionCore) -> None:
        """Bind the namespace to its session.

        Args:
            core: The session's internal core.
        """
        self._core = core

    @override
    def __repr__(self) -> str:
        """Name the session and the namespace's path.

        Returns:
            For example `<MassiveSession #1 stocks>` or `<MassiveSession #1 partners.tmx>`.
        """
        return f"<{self._core.label} {self._path}>"


def query_class(spec: DatasetSpec) -> type[QueryBase]:
    """Return the query class of a dataset, derived from the transports its spec declares.

    A dataset's verbs are exactly what its transports can serve, so the class follows from the
    spec: channels give the stream verbs, a history source (a REST history or grouped-daily
    path, or flat-file prefixes) gives `history()`, and a fetch path gives `fetch()`. Each
    namespace factory's return annotation is this class, which the namespace tests check.

    | Channels | History | Fetch | Class                      |
    |----------|---------|-------|----------------------------|
    | no       | no      | yes   | `Query`                    |
    | yes      | no      | no    | `StreamQuery`              |
    | no       | yes     | no    | `HistoryQuery`             |
    | yes      | yes     | no    | `StreamHistoryQuery`       |
    | yes      | yes     | yes   | `StreamHistoryLatestQuery` |

    `DatasetSpec` rejects every other combination when it is created. A `bars` dataset gives
    `HistoryQuery`, the class of its most general overload: `bar_query_class` picks the class
    of each call from its timespan and multiplier.

    Args:
        spec: The dataset.

    Returns:
        The query class its factory returns.
    """
    if spec.is_bars:
        return HistoryQuery
    if not spec.has_channels:
        return Query if spec.has_fetch else HistoryQuery
    if not spec.has_history:
        return StreamQuery
    return StreamHistoryLatestQuery if spec.has_fetch else StreamHistoryQuery


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
    if count == 1 and span is Timespan.DAY and spec.has_fetch:
        return HistoryLatestQuery
    return HistoryQuery


def list_params(*, sort: str | None, order: Order | None) -> dict[str, ParamValue]:
    """Validate the `sort` and `order` parameters of a REST list dataset.

    The third member of the `…F` block, `limit`, is not a wire parameter: it caps the total
    number of rows across pages and goes to `build_query(limit=...)`, which stores it in
    `Request.limit`. An `order` that is not a `dm.Order` raises `TypeError`.

    Args:
        sort: The field to sort by, or None for the endpoint's default.
        order: The sort direction, or None for the endpoint's default.

    Returns:
        The parameters that are set, by wire name.

    Whether `order` may be given without `sort` depends on the dataset's paths, so
    `build_query` decides it: a path with its own `order` parameter takes it as is, and only
    a path without one needs `sort` to carry the direction as its `.asc` or `.desc` suffix.

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

    A naive `datetime` raises the `ValueError` of `require_aware` in `_datasets/dates.py`, the
    same message as a naive time bound.
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


def _filters(
    spec: DatasetSpec, filters: object, *, typed: Iterable[str]
) -> tuple[tuple[str, ParamValue], ...]:
    """Normalize the `filters` mapping into a sorted tuple.

    Args:
        spec: The dataset, for its time-axis parameter pair.
        filters: The mapping as passed.
        typed: The wire names of the factory's typed parameters, set or not; a filter may not
            name one of them, nor any of its suffixed forms.

    Returns:
        `(name, value)` pairs sorted by name.

    Raises:
        TypeError: If `filters` is not a mapping, a key is not a `str`, or a value has an
            unsupported type.
        ValueError: If a key is a parameter of the dataset's time axis (`DatasetSpec.axis_params`,
            the `start`/`end` pair's base name and every suffixed form), a typed parameter of
            the factory or one of its suffixed forms (`param_family`), or `adjusted`, which
            has its own typed argument: a filter would reach REST but not the flat-file
            source choice, which reads only `Request.adjusted`.
    """
    if not isinstance(filters, Mapping):
        raise TypeError(f"filters must be a mapping, not {type(filters).__name__}")
    reserved = {filter_key(form) for name in typed for form in param_family(name)}
    pairs: list[tuple[str, ParamValue]] = []
    for key, value in filters.items():
        if not isinstance(key, str):
            raise TypeError(f"filter names must be str, not {type(key).__name__}")
        if key in {filter_key(name) for name in spec.axis_params}:
            raise ValueError(f"{key} is set through start= and end=, not filters")
        if key == "adjusted":
            raise ValueError("adjusted is set through build_query(adjusted=...), not filters")
        if key in reserved:
            raise ValueError(f"{key} is set through its own argument, not filters")
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
    adjusted: bool | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    filters: Mapping[str, FilterValue] | None = None,
    limit: int | None = None,
) -> QueryBase:
    """Validate one dataset call and return its query.

    Args:
        core: The session core the query will run on.
        spec: The dataset.
        symbols: The instruments, or None for datasets without them. A Deephaven `Table`
            must have a `Symbol` column; it is boxed by identity in a `SymbolTable` and read
            when a verb runs.
        all_alternative: None when the dataset accepts `dm.ALL`; otherwise the alternative to
            name in the `ValueError`, such as `"m.stocks.snapshots(dm.ALL)"`.
        timespan: The bar timespan; for a bars dataset (`DatasetSpec.is_bars`) it also selects
            the query class through `bar_query_class`.
        multiplier: The bar multiplier.
        venue: The futures venue, or None for every venue.
        adjusted: Whether bars are adjusted for splits, for a dataset with an `adjusted`
            parameter; None for a dataset without one (`Request.adjusted`).
        start: The lower bound of the dataset's time axis, or None.
        end: The upper bound of the dataset's time axis, or None.
        params: The dataset's other typed parameters by wire name; None values are dropped,
            but every key, set or not, is reserved from `filters` with its suffixed forms.
        filters: The `filters` mapping, by key: a wire parameter name with its dots written
            as underscores. `Catalog.check_filters` validates it and parses each value into
            the form that is sent (ISO-8601 text of a date or timestamp parameter becomes a
            `date` or an aware `datetime`, a bare `str` for a list parameter a one-item
            tuple); the `Request` holds those parsed values.
        limit: The cap on the total number of rows (`Request.limit`), or None for every row;
            a positive `int`.

    Returns:
        An instance of the spec's query class (`query_class`, or the class `bar_query_class`
        picks for a bars dataset).

    Raises:
        ValueError: If `dm.ALL` is passed where `all_alternative` is set, a `Table` has no
            `Symbol` column, or `params` or `filters` holds `adjusted`, which has its own typed
            argument; or if `params` sets `order` without `sort` and none of the dataset's
            REST paths has an `order` parameter, so the direction could only travel as the
            `.asc` or `.desc` suffix of `sort` (`Catalog.takes_param`). Where some path has
            `order`, the REST transport sends it as is there, folds it into `sort` on a path
            without it, and fails the verb on a chosen path that has neither.
        TypeError: If `adjusted` is neither None nor a `bool`.

    Invalid symbols, enum arguments, time bounds, parameters, or filters raise the `TypeError`
    or `ValueError` of the helper that checks them (`normalize` in `_datasets/symbols.py`,
    `_require_enum`, `parse_date_like` in `_datasets/dates.py`, `bar_query_class`,
    `require_positive_int` for `limit`).
    """
    normalized: NormalizedSymbols | None = None
    if isinstance(symbols, Table):
        if "Symbol" not in symbols.column_names:
            raise ValueError("a symbol table must have a Symbol column")
        normalized = SymbolTable(symbols)
    elif symbols is not None:
        normalized = normalize(spec.asset_class, symbols)
    if isinstance(normalized, _All) and all_alternative is not None:
        raise ValueError(f"{spec.name} does not accept dm.ALL; use {all_alternative}")
    if venue is not None:
        _require_enum(venue, FuturesVenue, "venue")
    if adjusted is not None and not isinstance(adjusted, bool):
        raise TypeError(f"adjusted must be a bool, not {type(adjusted).__name__}")
    if params is not None and "adjusted" in params:
        raise ValueError("adjusted is set through build_query(adjusted=...), not params")
    typed = {k: v for k, v in (params or {}).items() if v is not None}
    if "order" in typed and "sort" not in typed and not core.catalog.takes_param(spec, "order"):
        raise ValueError(
            f"{spec.name}: order needs sort, since its paths take the direction as a suffix "
            "of the sort field"
        )
    cls = query_class(spec)
    if timespan is not None:
        _require_enum(timespan, Timespan, "timespan")
    if timespan is not None and spec.is_bars:
        cls = bar_query_class(spec, timespan, multiplier)
    first = None if start is None else parse_date_like(start, "start")
    last = None if end is None else parse_date_like(end, "end")
    if first is not None and last is not None:
        check_order(first, last)
    given = () if filters is None else _filters(spec, filters, typed=params or {})
    request = Request(
        symbols=normalized,
        timespan=timespan,
        multiplier=multiplier,
        venue=venue,
        adjusted=adjusted,
        start=first,
        end=last,
        params=tuple(sorted((k, _value(k, v)) for k, v in (params or {}).items() if v is not None)),
        filters=core.catalog.check_filters(spec, dict(given)),
        limit=None if limit is None else require_positive_int("limit", limit),
    )
    return cls(core, spec, request)
