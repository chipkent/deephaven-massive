"""Query building: the shared dataset factory behind every namespace method.

Each namespace class (`_Stocks`, `_Options`, ...) is a thin set of typed factory methods; this
module does the work they share when a dataset method is called:

- `Namespace` is the base class of the namespace classes; it holds the session core, which
  each factory method passes to `build_query`, and wraps each factory in `shows_call`.
- `shows_call` records on each query the call that built it (`QueryCall`), which is the
  query's `repr`: the method's path and the arguments that differ from their defaults.
- `build_query` checks symbols (`check_symbols` in `_datasets/symbols.py`, which takes only
  Massive's exact tickers; a reactive
  `Table` is checked for its `Symbol` column and boxed in a `SymbolTable` here), rejects
  `dm.ALL` where the dataset cannot serve it (naming the alternative), parses the time-axis
  bounds, and normalizes the typed parameters and the `filters` mapping into a hashable
  `Request`.
- The query class `build_query` returns is the one `_query.py` chooses for the dataset
  (`query_class`, and `bar_query_class` for a `bars()` call).
- `list_params` validates the `sort` and `order` parameters of the `…F` block shared by the
  REST list datasets; its `limit` goes to `build_query(limit=...)` (`Request.limit`), because
  it is a total-row cap, not the endpoint's page-size parameter. `list_query` builds the query
  of a REST list dataset with them.
- `require_bool` and `optional_text` check the plain `bool` and optional `str` arguments of
  dataset methods, and `value_table` boxes a table passed where a dataset takes one value (an
  underlying, a futures product code) in a `ValueTable`, which `build_query` stores in the
  `Request` for the verbs to read.
- `ticker_key` declares the keys of a source whose rows name their instrument in `Ticker`.

Filter keys and value kinds are checked against the generated parameters of the dataset's REST
paths (`check_filters` in `_datasets/filters.py`) synchronously at construction, so a
bad filter fails before any verb runs, and the `Request` stores each value as the check parsed
it (ISO-8601 text of a date parameter becomes a `date`), never the caller's text. Everything
here runs synchronously on the calling thread, makes no network call, and creates no table.
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import replace
from datetime import date, datetime
from typing import TYPE_CHECKING, ClassVar, Final, TypeVar, cast, get_overloads, overload

from deephaven import dtypes
from deephaven.table import Table

from deephaven_massive._config import require_positive_int
from deephaven_massive._datasets.catalog import KeySpec, SourceKeys
from deephaven_massive._datasets.dates import (
    calendar_day,
    check_order,
    parse_date_like,
)
from deephaven_massive._datasets.filters import (
    check_filters,
    filter_key,
    param_base,
    param_family,
    symbol_family,
    takes_instants,
)
from deephaven_massive._datasets.request import Request, ValueTable
from deephaven_massive._datasets.symbols import SymbolTable, check_symbols
from deephaven_massive._enums import FuturesVenue, Order, Timespan, _require_enum
from deephaven_massive._massive import ParamKind, Transport
from deephaven_massive._query import Query, QueryBase, QueryCall, bar_query_class, query_class
from deephaven_massive._tables.time_text import deephaven_time_text
from deephaven_massive._types import _All, override

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import RestPath
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = [
    "ADJUSTED_PRICE",
    "ADJUSTED_VOLUME",
    "EVERY_TICKER",
    "SNAPSHOT_SYMBOLS_PER_REQUEST",
    "Namespace",
    "accepted_values",
    "adjustment_notes",
    "build_query",
    "day_argument",
    "list_params",
    "list_query",
    "one_or_any_of",
    "optional_text",
    "require_bool",
    "shows_call",
    "ticker_key",
    "value_table",
]

logger = logging.getLogger(__name__)

ADJUSTED_PRICE: Final = (
    "Split-adjusted when the query's adjusted is True, the default, served only by REST: a "
    "price from before a split is restated in the split's terms (divided by its ratio), so it "
    "is not the price that traded. With adjusted=False, from the flat files, and on the "
    "WebSocket it is as traded. One history() never mixes the two, and Massive never adjusts "
    "for cash dividends."
)
"""The note on every price column of a dataset with a split-adjustment choice."""

ADJUSTED_VOLUME: Final = (
    "Split-adjusted like the prices when the query's adjusted is True, the default, served only "
    "by REST: a volume from before a split is restated in the split's terms (multiplied by its "
    "ratio). With adjusted=False, from the flat files, and on the WebSocket it is as traded."
)
"""The note on every volume column of a dataset with a split-adjustment choice."""


_F = TypeVar("_F", bound=Callable[..., QueryBase])

SNAPSHOT_SYMBOLS_PER_REQUEST: Final = 250
"""The most tickers one snapshot request names in its comma-separated ticker list.

Massive's unified snapshot documents 250 as the limit of its `ticker.any_of`; the per-class
snapshots take the same size, so every long watchlist is split into requests whose URLs stay
well under the lengths servers refuse (`DatasetSpec.symbols_per_request`)."""


EVERY_TICKER: Final = "leave symbols out, which returns every ticker"
"""The alternative to `dm.ALL` that the REST list datasets name in their `ValueError`."""


def adjustment_notes(prices: Iterable[str], volumes: Iterable[str]) -> Mapping[str, str]:
    """Return the column notes of a dataset whose prices may be split-adjusted.

    Massive adjusts stock and option prices and volumes for splits when a request says
    `adjusted=true`, but no vendor description says so, and the same column holds either basis
    depending on the query; each such column's description therefore ends with a note
    (`DatasetSpec.column_notes`), which the column tooltips, `columns()`, and the column
    reference show. Only columns REST fills take a note: a column only the WebSocket sends is
    always as traded.

    Args:
        prices: The dataset's price columns REST fills, such as `"Close"`.
        volumes: Its volume columns REST fills, such as `"Volume"`.

    Returns:
        `ADJUSTED_PRICE` for each price column and `ADJUSTED_VOLUME` for each volume column.
    """
    return {
        **dict.fromkeys(prices, ADJUSTED_PRICE),
        **dict.fromkeys(volumes, ADJUSTED_VOLUME),
    }


def _is_default(value: object, default: object) -> bool:
    """Tell whether an argument has its parameter's default, so a query's repr leaves it out.

    Args:
        value: The argument as passed.
        default: The parameter's default, or `inspect.Parameter.empty` for none.

    Returns:
        True for the default object itself, or a value equal to a default other than None
        (`{dm.FuturesVenue.CME, ...}` equals the default `frozenset`). None is compared by
        identity, so a symbol table is never compared with `==`. An array-like value, whose
        `==` answers element by element (a numpy array, a pandas series), is never the
        default: only a plain `bool` answer counts, since the truth of an array raises.
    """
    if value is default:
        return True
    if default is None or default is inspect.Parameter.empty:
        return False
    equal: object = value == default  # an array's `==` is an array
    return isinstance(equal, bool) and equal


def _frozen(value: object) -> object:
    """Return an argument as a query's call keeps it, safe from later changes by the caller.

    Args:
        value: The argument as passed.

    Returns:
        A list as a tuple, a set as a `frozenset`, a mapping as the tuple of its items, and
        anything else unchanged.
    """
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, AbstractSet):
        return frozenset(value)
    if isinstance(value, Mapping):
        return tuple(value.items())
    return value


def shows_call(path: str) -> Callable[[_F], _F]:
    """Make a dataset method record its call on the query it returns, as the query's `repr`.

    The wrapper binds the arguments to the method's signature after the method has returned
    (so a refused call raises the method's own error) and stores a `QueryCall` on the query:
    the method's path from the session, and the arguments that differ from their defaults
    (`_is_default`), in signature order (`_frozen`), and the parameters it requires, which the
    repr writes by position; the repr writes each argument from the query's normalized request
    where that holds it (`QueryCall.text`). Equality of queries is
    unchanged: the call is not part of it. `Namespace` applies it to every public method of
    each namespace class, and `MassiveSession` to its own dataset method, `snapshots`. An
    exception the method raises, a refused argument's `TypeError` or `ValueError`, is logged
    at DEBUG with the method's path and re-raised.

    Args:
        path: The method's owner as reached from a session, such as `"m.options"`.

    Returns:
        A decorator; its wrapper has the method's name, docstring, signature, and
        `raises_through` helpers (`functools.wraps`).
    """

    def decorate(method: _F) -> _F:
        signature = inspect.signature(method)
        called = f"{path}.{method.__name__}"
        required = frozenset(
            name
            for name, param in list(signature.parameters.items())[1:]
            if param.default is inspect.Parameter.empty
            and param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        )

        @functools.wraps(method)
        def factory(*args: object, **kwargs: object) -> QueryBase:
            try:
                query = method(*args, **kwargs)
            except Exception as error:
                # The caller receives the refusal; the log keeps which method refused, and why.
                logger.debug("%s refused: %s: %s", called, type(error).__name__, error)
                raise
            given = signature.bind(*args, **kwargs).arguments
            arguments = tuple(
                (name, _frozen(given[name]))
                for name, param in list(signature.parameters.items())[1:]
                if name in given and not _is_default(given[name], param.default)
            )
            return replace(query, _call=QueryCall(called, arguments, required))

        return cast("_F", factory)

    return decorate


class Namespace:
    """Base class of the namespace classes reached from a session, such as `m.stocks`.

    A namespace is created once per session by `MassiveSession.__init__` and only holds the
    session core. Its methods are the dataset factories; they validate arguments and return
    queries, and never touch the network. Each subclass's public methods are wrapped in
    `shows_call` when the class is defined, so every query a namespace returns shows the call
    that built it. Thread-safe: it has no mutable state.
    """

    __slots__ = ("_core",)

    _path: ClassVar[str] = ""
    """The namespace's path from the session, such as `"stocks"` or `"partners.benzinga"`."""

    @override
    def __init_subclass__(cls, **kwargs: object) -> None:
        """Name each public method by its path, such as `stocks.bars`, and wrap it in `shows_call`.

        Python names a function by its `__qualname__` in the `TypeError` of a wrong call
        (`stocks.bars() takes from 2 to 3 positional arguments but 5 were given`). The class
        name, such as `_Stocks`, is private and means nothing to a user who reached the method
        as `m.stocks.bars`, so each public function the class defines itself is renamed to its
        path from the session when the class is created, at import. `typing` files a
        function's overloads under its `__qualname__`, so each overload is renamed too and
        registered again under the new name, which keeps `typing.get_overloads` finding them.

        Every public method of a namespace class is a dataset factory, so each is then wrapped
        in `shows_call`, whose wrapper keeps the new name; a sub-namespace is a property, which
        is left alone.

        Args:
            **kwargs: Passed on to `object.__init_subclass__`.
        """
        super().__init_subclass__(**kwargs)
        record = shows_call(f"m.{cls._path}")
        for name, member in list(vars(cls).items()):
            if not name.startswith("_") and inspect.isfunction(member):
                path = f"{cls._path}.{name}"
                for variant in get_overloads(member):
                    variant.__qualname__ = path
                    overload(variant)
                member.__qualname__ = path
                setattr(cls, name, record(member))

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


def day_argument(spec: DatasetSpec, value: DateLike, name: str) -> date:
    """Read a dataset's one-day argument, such as a daily summary's `date` or `as_of`.

    The argument takes every `DateLike` form and Deephaven's own date text
    (`deephaven_time_text`), and must name a calendar day (`dates.calendar_day`).

    Args:
        spec: The dataset, for messages.
        value: The argument as the caller passed it.
        name: The parameter's name, such as `"date"`.

    Returns:
        The calendar date to send.

    The `ValueError` or `TypeError` of `deephaven_time_text` and `calendar_day` propagates: a
    time instead of a day, text neither form reads, or a value of another type.
    """
    return calendar_day(spec, deephaven_time_text(value, name), name, given=value)


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


def accepted_values(core: SessionCore, spec: DatasetSpec, name: str) -> tuple[str, ...]:
    """Return the values the enum parameter of a dataset's fetch path accepts, as generated.

    Reading them from the generated path, not a copy, makes a dataset method accept exactly
    what Massive's specification lists, through every refresh.

    Args:
        core: The session core, whose catalog holds the generated paths.
        spec: The dataset; its fetch path has an enum parameter `name`.
        name: The parameter's wire name, such as `"timespan"`.

    Returns:
        The accepted wire values, in the specification's order.
    """
    path = cast("RestPath", core.catalog.source(Transport.REST, cast("str", spec.fetch_path)))
    return next(param.values for param in path.params if param.name == name)


def require_bool(name: str, value: object) -> bool:
    """Check a dataset method's `bool` argument, such as a flag that includes extra rows.

    Args:
        name: The parameter's name, for the message.
        value: The argument as passed.

    Returns:
        The value unchanged.

    Raises:
        TypeError: If `value` is not a `bool` (`0`, `1`, and `"true"` are refused, so a typo
            never becomes a filter Massive reads differently).
    """
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, not {type(value).__name__}")
    return value


def optional_text(name: str, value: object) -> str | None:
    """Check a dataset method's optional text argument, such as a search term.

    Args:
        name: The parameter's name, for the message.
        value: The argument as passed, or None when the caller left it out.

    Returns:
        The value unchanged.

    Raises:
        TypeError: If `value` is neither None nor a `str`.
    """
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} must be a str, not {type(value).__name__}")
    return value


def _require_text_column(table: Table, column: str, *, what: str) -> None:
    """Refuse a table whose column of tickers or codes does not hold text.

    A ticker or product code is text, and the library reads the column as text: a column of
    numbers or dates would otherwise fail deep inside a verb, with no word of what to fix.

    Args:
        table: The table as passed, which has the column.
        column: The column, `Symbol` or `ProductCode`.
        what: The table as the message names it, such as `"a symbol table"`.

    Raises:
        ValueError: If the column is not a `String` column; the message names its type.
    """
    data_type = next(c.data_type for c in table.columns if c.name == column)
    if data_type != dtypes.string:
        # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
        logger.debug("%s refused: column %s is %s, not String", what, column, data_type)
        raise ValueError(
            f"the {column} column of {what} must be a String column; this one holds "
            f"{data_type}: each row holds one value as text, such as 'AAPL' or 'ES'"
        )


def value_table(table: Table, *, argument: str, parameter: str, underlying: bool) -> ValueTable:
    """Box a table passed where a dataset takes one value, after checking its column.

    `underlying=` reads its values from a `Symbol` column, as a symbol table does, and
    `product_code=` from a `ProductCode` column, as `m.futures.products()` rows carry them.
    The table is read when a verb runs: a static one once, a refreshing one followed.

    Args:
        table: The table as passed.
        argument: The method's argument, `"underlying"` or `"product_code"`.
        parameter: The wire parameter each value is sent as, such as `"underlyingAsset"`.
        underlying: True for a table of underlyings, read from `Symbol`; False for product
            codes, read from `ProductCode`.

    Returns:
        The boxed table (`ValueTable`).

    Raises:
        ValueError: If the table lacks the column; the message names the columns it has. If
            the column is not a `String` column (`_require_text_column`).
    """
    column = "Symbol" if underlying else "ProductCode"
    if column not in table.column_names:
        columns = ", ".join(table.column_names)
        # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
        logger.debug("%s refused: a table without %s, columns %s", argument, column, columns)
        raise ValueError(
            f"a table passed as {argument} must have a {column} column; this one has: "
            f'{columns}. Name the column {column}, as in t.view("{column} = Value")'
        )
    _require_text_column(table, column, what=f"a table passed as {argument}")
    return ValueTable(argument, parameter, column, underlying, table)


def one_or_any_of(name: str, value: object, *, every: str) -> dict[str, object]:
    """Return the wire parameter that carries an argument taking one value or several.

    Many list endpoints take a parameter both as `name` (one value) and as `name.any_of` (a
    comma-separated list), such as a futures `product_code` or a filing's `issuer_cik`. The
    factory argument accepts either: one `str` is sent as `name`, a sequence as
    `name.any_of`. Both keys are always returned, the unset one as None, so `build_query`
    reserves every form of the parameter from `filters` even when the argument is not given.

    Args:
        name: The wire name of the single-value form, which is also the argument's name.
        value: The argument as passed: one `str`, a sequence of `str`, or None.
        every: What None selects, for the message that refuses an empty sequence, such as
            `"every product"`.

    Returns:
        `{name: value, f"{name}.any_of": None}` for one value or None, and
        `{name: None, f"{name}.any_of": values}` for a sequence, with the values as a tuple.

    Raises:
        TypeError: If the argument is neither None, a `str`, nor a sequence.
        ValueError: If the sequence is empty, which would select nothing.

    An element that is not a `str` raises the `TypeError` of `build_query`.
    """
    plural = f"{name}.any_of"
    if value is None or isinstance(value, str):
        return {name: value, plural: None}
    if not isinstance(value, Sequence):
        raise TypeError(
            f"{name} must be a str, a sequence of str, or None, not {type(value).__name__}"
        )
    if not value:
        raise ValueError(f"{name} is empty; pass None for {every}")
    return {name: None, plural: tuple(value)}


def _time_value(name: str, value: object) -> object:
    """Read a date or time object as the `date` or aware `datetime` it names.

    `filters` take the same date and time objects as `start`, `end`, `as_of`, and `date`: a
    `date`, an aware `datetime`, a Java `LocalDate`, `Instant`, or `ZonedDateTime`, a
    `numpy.datetime64`, or a `pandas.Timestamp`, each read by `parse_date_like`. An instant
    finer than a microsecond is rounded up for an upper bound (a name ending in `lt` or `lte`)
    and down for any other, as a filter's ISO-8601 text is, so no row is lost.

    Args:
        name: The parameter or filter key, for messages and the rounding direction.
        value: The value as passed.

    Returns:
        The `date` or aware `datetime`; a value that is no date or time object, text among
        them, unchanged. Whether a filter's text is a time depends on its parameter, which
        `check_filters` reads.

    A naive `datetime` or `pandas.Timestamp`, a `NaT`, or a `numpy.datetime64` of a week,
    month, or year raises the `ValueError` of `parse_date_like`.
    """
    if isinstance(value, str | int | float):
        return value
    upper = name.replace(".", "_").rpartition("_")[2] in ("lt", "lte")
    try:
        return parse_date_like(cast("DateLike", value), name, upper=upper)
    except TypeError:
        return value


def _value(name: str, value: object) -> ParamValue:
    """Normalize one parameter or filter value to its hashable stored form.

    Args:
        name: The parameter's name, for messages.
        value: The value as passed.

    Returns:
        The value; a date or time object as the `date` or aware `datetime` it names
        (`_time_value`); a list or tuple as a tuple of `str` and `date` (for a list of
        dates, such as `date_any_of`).

    Raises:
        TypeError: If the value is not a `str`, `int`, `float`, `bool`, enum member, date or
            time object, or a sequence of `str` and dates; for a list or tuple, the message
            names the first element that is neither (an instant is not a day).

    A naive `datetime`, or another time `parse_date_like` refuses, raises its `ValueError`,
    the same message as a refused time bound.
    """
    if isinstance(value, list | tuple):
        items = [_time_value(name, item) for item in value]
        for index, item in enumerate(items):
            if not isinstance(item, str | date) or isinstance(item, datetime):
                raise TypeError(
                    f"{name}: element {index} of the sequence is {type(value[index]).__name__}, "
                    "not str or date"
                )
        return tuple(item if isinstance(item, date) else str(item) for item in items)
    found = _time_value(name, value)
    if isinstance(found, str | int | float | date):
        return found
    raise TypeError(
        f"{name} must be a str, int, float, bool, date, datetime, Java LocalDate, Instant, or "
        "ZonedDateTime, numpy.datetime64, pandas.Timestamp, or sequence of str or date; "
        f"got {type(value).__name__}"
    )


def _fixed_params(spec: DatasetSpec, params: Mapping[str, object]) -> dict[str, object]:
    """Return the parameters the library sets itself, after checking no typed one names them.

    Args:
        spec: The dataset, whose `fixed_params` are returned.
        params: The factory's typed parameters by wire name.

    Returns:
        A copy of `DatasetSpec.fixed_params`.

    Raises:
        ValueError: If a typed parameter has the name of a fixed one.
    """
    clash = sorted(spec.fixed_params.keys() & params.keys())
    if clash:
        raise ValueError(f"{spec.name}: the library sets {clash} itself; they are not params")
    return dict(spec.fixed_params)


def _filters(
    spec: DatasetSpec,
    filters: object,
    *,
    typed: Iterable[str],
    arguments: Mapping[str, str],
) -> tuple[tuple[str, ParamValue], ...]:
    """Normalize the `filters` mapping into a sorted tuple.

    Args:
        spec: The dataset, for its time-axis parameter pair, its symbol parameter, and the
            parameters it fixes (`DatasetSpec.fixed_params`).
        filters: The mapping as passed.
        typed: The wire names of the factory's typed parameters, set or not; a filter may not
            name one of them, nor any of its suffixed forms.
        arguments: The factory's argument name of each typed wire parameter whose argument
            is named differently (`{"type": "ticker_type"}`), for the message that names
            the argument to use; any other parameter's argument has its wire name.

    Returns:
        `(name, value)` pairs sorted by name.

    Raises:
        TypeError: If `filters` is not a mapping, a key is not a `str`, or a value has an
            unsupported type.
        ValueError: If a key is a parameter of the dataset's time axis (`DatasetSpec.axis_params`,
            the `start`/`end` pair's base name and every suffixed form), an equality form of
            its symbol parameter (`symbol_family`, such as `ticker_any_of`: symbols go through
            `symbols=`), a parameter the library sets itself or a typed parameter of the
            factory, or one of their suffixed forms (`param_family`), or `adjusted`, which
            has its own typed argument: a filter would reach REST but not the flat-file
            source choice, which reads only `Request.adjusted`.
    """
    if not isinstance(filters, Mapping):
        raise TypeError(f"filters must be a mapping, not {type(filters).__name__}")
    reserved = {
        filter_key(form): arguments.get(name, name) for name in typed for form in param_family(name)
    }
    library = {filter_key(form) for name in spec.fixed_params for form in param_family(name)}
    symbol_keys = (
        set()
        if spec.symbol_param is None
        else set(map(filter_key, symbol_family(spec.symbol_param)))
    )
    pairs: list[tuple[str, ParamValue]] = []
    for key, value in filters.items():
        if not isinstance(key, str):
            raise TypeError(f"filter names must be str, not {type(key).__name__}")
        if key in {filter_key(name) for name in spec.axis_params}:
            raise ValueError(f"{key} is set through start= and end=, not filters")
        if key == "adjusted":
            raise ValueError("adjusted is set through its own argument, adjusted=, not filters")
        if key in symbol_keys:
            raise ValueError(f"{key} is set through symbols=, not filters")
        if key in library:
            raise ValueError(f"the library sets {key} itself; it is not a filter")
        if key in reserved:
            raise ValueError(
                f"{key} is set through its own argument, {reserved[key]}=, not filters"
            )
        pairs.append((key, _value(key, value)))
    return tuple(sorted(pairs))


def _calendar_axis(core: SessionCore, spec: DatasetSpec) -> str | None:
    """Return the field of a dataset's time axis when it is a calendar date, or None.

    An axis is a calendar date when one of the dataset's REST paths takes its lower parameter
    as a date of a calendar-date row field (`ex_dividend_date`, `filing_date`, the futures
    `session_end_date`), not an instant (`filters.takes_instants`).

    Args:
        core: The session core, whose catalog holds the generated paths.
        spec: The dataset.

    Returns:
        The axis field, such as `"ex_dividend_date"`; None for an instant axis or none.
    """
    if spec.time_axis is None:
        return None
    name = spec.time_axis[0]
    for template in spec.rest_paths:
        rest_path = cast("RestPath", core.catalog.source(Transport.REST, template))
        param = next((p for p in rest_path.params if p.name == name), None)
        if (
            param is not None
            and param.kind is ParamKind.DATE
            and not takes_instants(rest_path, name, param.kind)
        ):
            return param_base(name)
    return None


def _check_axis(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    start: date | datetime | None,
    end: date | datetime | None,
) -> None:
    """Check the `start` and `end` of a `…T` dataset's time axis.

    The rules are those of `history()`: both bounds of one kind (dates are whole days,
    datetimes exact instants), `start` not after `end`, and equal datetimes an empty range. A
    calendar-date axis holds whole days, so it takes dates only. A bound left None is open.

    Args:
        core: The session core.
        spec: The dataset.
        start: The parsed lower bound, or None.
        end: The parsed upper bound, or None.

    Raises:
        ValueError: If a datetime is given for a calendar-date axis (`"<axis> is a calendar
            date; pass dates"`). Mixed or reversed bounds raise the `ValueError` of
            `dates.check_order`.
    """
    given = [bound for bound in (start, end) if bound is not None]
    if any(isinstance(bound, datetime) for bound in given):
        axis = _calendar_axis(core, spec)
        if axis is not None:
            raise ValueError(f"{spec.name}: {axis} is a calendar date; pass dates")
    if start is not None and end is not None:
        check_order(start, end)


def futures_venues(venues: object) -> tuple[FuturesVenue, ...]:
    """Check the `venues` argument of a futures dataset and order it for the request.

    Args:
        venues: The argument as passed: a set of `dm.FuturesVenue` members, such as
            `{dm.FuturesVenue.CME}` or the default `frozenset(dm.FuturesVenue)`.

    Returns:
        The venues in `FuturesVenue` order, whatever the set's order, so equal sets give equal
        queries and the flat files are read in one order.

    Raises:
        TypeError: If `venues` is not a set (a single member, a list, or text included), or a
            member is not a `dm.FuturesVenue`; the message names the set to pass.
        ValueError: If the set is empty, which would read no flat file.
    """
    if not isinstance(venues, AbstractSet):
        raise TypeError(
            f"venues must be a set of dm.FuturesVenue, such as {{dm.FuturesVenue.CME}}, not "
            f"{type(venues).__name__}"
        )
    for venue in venues:
        if not isinstance(venue, FuturesVenue):
            raise TypeError(
                f"venues must hold only dm.FuturesVenue members, not {type(venue).__name__}"
            )
    if not venues:
        raise ValueError("venues is empty; leave it out to read every venue")
    return tuple(venue for venue in FuturesVenue if venue in venues)


def build_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None = None,
    symbols_optional: bool = False,
    all_alternative: str | None = None,
    timespan: Timespan | None = None,
    multiplier: int = 1,
    venues: AbstractSet[FuturesVenue] | None = None,
    adjusted: bool | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    arguments: Mapping[str, str] | None = None,
    filters: Mapping[str, FilterValue] | None = None,
    limit: int | None = None,
    values: ValueTable | None = None,
) -> QueryBase:
    """Validate one dataset call and return its query.

    The dataset's `fixed_params` (parameters the library sets itself, such as the indicators'
    `expand_underlying=False`) join the typed parameters in `Request.params` and are sent on
    every request. The catalog's `wire parameters` gate guarantees that every REST path of the
    dataset takes each of them, so a fixed parameter can never be blamed on the caller as a
    parameter its path lacks. Each one's whole family is reserved from `filters` ("the library
    sets expand_underlying itself; it is not a filter") and left out of the valid keys a
    refused filter lists.

    Args:
        core: The session core the query will run on.
        spec: The dataset.
        symbols: The instruments, or None for datasets without them. A Deephaven `Table`
            must have a `Symbol` column; it is boxed by identity in a `SymbolTable` and read
            when a verb runs. None is refused for an instrument dataset
            (`Catalog.takes_symbols`) unless `symbols_optional` is set.
        symbols_optional: True for a dataset whose `symbols` only filters the request, so
            None asks for every ticker; False (the default) for one that must be told its
            instruments.
        all_alternative: None when the dataset accepts `dm.ALL`; otherwise what the
            `ValueError` tells the caller to do instead, an instruction such as
            `"use m.stocks.snapshots(dm.ALL)"` or `"leave symbols out, which returns every
            ticker"`.
        timespan: The bar timespan; for a bars dataset (`DatasetSpec.is_bars`) it also selects
            the query class through `bar_query_class`.
        multiplier: The bar multiplier.
        venues: The futures venues whose flat files `history()` reads (`futures_venues`), or
            None for a dataset without venues.
        adjusted: Whether bars are adjusted for splits, for a dataset with an `adjusted`
            parameter; None for a dataset without one (`Request.adjusted`).
        start: The lower bound of the dataset's time axis, or None for an open start: a date
            (a whole day) or an aware datetime (an exact instant).
        end: The upper bound of the dataset's time axis, of the same kind as `start`, or None
            for an open end, which sends no upper bound, so future-dated rows (an announced
            ex-dividend date) are returned; a date is included whole, a datetime is the first
            instant excluded. A calendar-date axis takes dates only (`_check_axis`).
        params: The dataset's other typed parameters by wire name; None values are dropped,
            but every key, set or not, is reserved from `filters` with its suffixed forms.
        arguments: The factory's argument name of each key of `params` that the factory
            names differently from the wire (`{"type": "ticker_type"}`), so a refused filter
            names the argument to use; None when every argument has its wire name.
        filters: The `filters` mapping, by key: a wire parameter name with its dots written
            as underscores. `filters.check_filters` validates it and parses each value into
            the form that is sent (ISO-8601 text of a date or timestamp parameter becomes a
            `date` or an aware `datetime`, and a list parameter's value the tuple of its
            comma-separated items), and a Java, numpy, or pandas date or time object the
            `date` or aware `datetime` it names (`_time_value`); the `Request` holds those
            parsed values.
        limit: The cap on the total number of rows (`Request.limit`), or None for every row;
            a positive `int`.
        values: A table passed where the dataset takes one value (`value_table`), or None.
            Its parameter must be None in `params`, which still reserves it from `filters`;
            the verbs send each of the table's values as that parameter.

    Returns:
        An instance of the spec's query class (`query_class`, or the class `bar_query_class`
        picks for a bars dataset), whose call names only the dataset; the dataset method's
        `shows_call` wrapper replaces it with the call as made.

    Raises:
        ValueError: If `symbols` is None for an instrument dataset that needs symbols (the
            message names the forms it takes, `dm.ALL` among them where the dataset takes
            it), `dm.ALL` is passed where `all_alternative` is set, a `Table` has no
            `Symbol` column or one that is not a `String` column, `params` or `filters` holds
            `adjusted`, which has its own typed argument, or `params` names one of the dataset's
            `fixed_params`; or if `params` sets `order` without `sort` and none of the dataset's
            REST paths has an `order` parameter, so the direction could only travel as the `.asc` or
            `.desc` suffix of `sort` (`Catalog.takes_param`). Where some path has `order`, the REST
            transport sends it as is there, folds it into `sort` on a path without it, and fails the
            verb on a chosen path that has neither.
        TypeError: If `adjusted` is neither None nor a `bool`.

    Invalid symbols, enum arguments, venues, time bounds, a `sort`, parameters, or filters
    raise the `TypeError` or `ValueError` of the helper that checks them (`check_symbols` in
    `_datasets/symbols.py`, `_require_enum`, `futures_venues`, `Catalog.check_sort`,
    `parse_date_like` in `_datasets/dates.py`, `_check_axis` for mixed, reversed, or
    calendar-date bounds, `bar_query_class`, `require_positive_int` for `limit`).
    """
    normalized: NormalizedSymbols | None = None
    if symbols is None and not symbols_optional and core.catalog.takes_symbols(spec):
        forms = (
            "a ticker, a list of tickers, a table with a Symbol column, or dm.ALL for every ticker"
            if all_alternative is None
            else "a ticker, a list of tickers, or a table with a Symbol column (it does not "
            f"take dm.ALL: {all_alternative})"
        )
        # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
        logger.debug("%s refused: symbols=None on a dataset that needs symbols", spec.name)
        raise ValueError(f"{spec.name} needs symbols, got None: pass {forms}")
    if isinstance(symbols, Table):
        if "Symbol" not in symbols.column_names:
            columns = ", ".join(symbols.column_names)
            # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
            logger.debug(
                "%s refused: a symbol table without Symbol, columns %s", spec.name, columns
            )
            raise ValueError(
                f"a symbol table must have a Symbol column; this one has: {columns}. Name the "
                'column of tickers Symbol, as in t.view("Symbol = Ticker"); for tickers spelled '
                "another way (lower case, another convention), convert them first with "
                'dm.canonicalize_symbols(t, asset_class, column="Ticker")'
            )
        _require_text_column(symbols, "Symbol", what="a symbol table")
        normalized = SymbolTable(symbols)
    elif symbols is not None:
        normalized = check_symbols(spec.asset_class, symbols, as_written=spec.symbols_as_written)
    if isinstance(normalized, _All) and all_alternative is not None:
        raise ValueError(f"{spec.name} does not accept dm.ALL; {all_alternative}")
    if adjusted is not None and not isinstance(adjusted, bool):
        raise TypeError(f"adjusted must be a bool, not {type(adjusted).__name__}")
    if params is not None and "adjusted" in params:
        raise ValueError("adjusted is set through build_query(adjusted=...), not params")
    library = _fixed_params(spec, params or {})
    typed = {k: v for k, v in (params or {}).items() if v is not None}
    if "order" in typed and "sort" not in typed and not core.catalog.takes_param(spec, "order"):
        raise ValueError(
            f"{spec.name}: order needs sort, since its paths take the direction as a suffix "
            "of the sort field"
        )
    sort = typed.get("sort")
    if isinstance(sort, str):  # `list_params` refused any other type
        core.catalog.check_sort(spec, sort, ordered="order" in typed)
    cls = query_class(spec)
    if timespan is not None:
        _require_enum(timespan, Timespan, "timespan")
    if timespan is not None and spec.is_bars:
        cls = bar_query_class(spec, timespan, multiplier)
    first = None if start is None else parse_date_like(deephaven_time_text(start, "start"), "start")
    last = (
        None if end is None else parse_date_like(deephaven_time_text(end, "end"), "end", upper=True)
    )
    _check_axis(core, spec, start=first, end=last)
    given = (
        ()
        if filters is None
        else _filters(spec, filters, typed=params or {}, arguments=arguments or {})
    )
    request = Request(
        symbols=normalized,
        timespan=timespan,
        multiplier=multiplier,
        venues=None if venues is None else futures_venues(venues),
        adjusted=adjusted,
        start=first,
        end=last,
        params=tuple(
            sorted(
                (k, _value(k, v)) for k, v in {**(params or {}), **library}.items() if v is not None
            )
        ),
        filters=check_filters(core.catalog, spec, dict(given), typed=params or {}),
        limit=None if limit is None else require_positive_int("limit", limit),
        value_table=values,
    )
    return cls(core, spec, request, QueryCall(f"m.{spec.name}"))


def list_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    all_alternative: str,
    symbols: Symbols | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    arguments: Mapping[str, str] | None = None,
    sort: str | None = None,
    order: Order | None = None,
    limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None,
) -> Query:
    """Build the query of a REST list dataset from its factory's arguments.

    A list dataset takes the `sort`, `order`, and `limit` block (`list_params`) beside its own
    typed parameters, and has only a fetch path, so `build_query` builds a `Query`. A ticker
    filter (`DatasetSpec.symbol_param`) is optional; a ticker in the path, such as Benzinga's
    consensus ratings, is not.

    Args:
        core: The session core.
        spec: The dataset.
        all_alternative: What the `ValueError` for `dm.ALL` tells the caller to pass instead,
            such as "symbols=None, which returns every ticker".
        symbols: The tickers that filter the request, or None for none.
        start: The lower bound of the dataset's time axis, or None.
        end: The upper bound of the dataset's time axis, or None.
        params: The dataset's other typed parameters by wire name; None values are dropped.
        arguments: The factory's argument name of each key of `params` it names differently,
            such as `{"issuer_cik.any_of": "issuer_cik"}`, for the message that refuses that
            key in `filters`; None when every argument has its wire name.
        sort: The field to sort by, or None for the endpoint's default.
        order: The sort direction, or None for the endpoint's default.
        limit: The cap on the total number of rows, or None for every row.
        filters: The `filters` mapping, or None.

    Returns:
        The query.

    Raises:
        TypeError: If an argument has the wrong type (`list_params`, `build_query`).
        ValueError: If an argument has a value the dataset refuses (`build_query`).
    """
    query = build_query(
        core,
        spec,
        symbols=symbols,
        symbols_optional=spec.symbol_param is not None,
        all_alternative=all_alternative,
        start=start,
        end=end,
        params={**list_params(sort=sort, order=order), **(params or {})},
        arguments=arguments,
        filters=filters,
        limit=limit,
    )
    # A list dataset has only a fetch path, so `build_query` builds its `Query`.
    return cast("Query", query)


def ticker_key(time: str | None = None) -> SourceKeys:
    """Return the keys of a source whose rows name their instrument in `Ticker`.

    Most market-data sources carry the symbol in a `Ticker` column and their primary time in a
    column of their own, so a namespace module declares them with this one call.

    Args:
        time: The source's column that holds the row's primary time, such as
            `"SipTimestamp"`, or None for a source without one.

    Returns:
        The source's keys: its `symbol` key `Ticker`, and its `time` key when given.
    """
    return SourceKeys(symbol=KeySpec("Ticker"), time=None if time is None else KeySpec(time))
