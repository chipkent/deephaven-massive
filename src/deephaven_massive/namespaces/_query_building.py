"""The dataset factory every namespace method calls to turn its arguments into a query.

A dataset method, such as `m.stocks.bars("AAPL", dm.Timespan.DAY)`, does three things: it
checks its arguments, it builds an immutable request from them, and it returns the query of
the class its dataset serves. The namespace classes only declare each method's typed signature
and documentation; this module does that shared work, synchronously on the caller's thread,
with no network call and no table created, so a wrong argument fails at the call, before any
method of the query runs.

- `build_query` checks the symbols (`check_symbols` in `_datasets/symbols.py`; a `Table` must
  have a `Symbol` column and is boxed in a `SymbolTable`), refuses `dm.ALL` where the dataset
  can't serve it and names the alternative, parses the time bounds, and normalizes the typed
  parameters and the `filters` mapping into a hashable `Request`. It builds the class
  `queries/_choice.py` derives from the dataset (`query_class`), or for a `bars()` call from its
  timespan and multiplier too (`bar_query_class`), and fails, as a library defect, when that
  class isn't the one the method declares or a subclass of it. Filter keys and values are checked
  against the dataset's generated REST parameters (`check_filters` in `_datasets/filters.py`),
  and the `Request` stores each value as the check parsed it, never the caller's text.
- `list_query` builds the query of a REST list dataset, whose `sort` and `order` arguments
  `list_params` checks; its `limit` is a cap on the total rows (`Request.limit`), not the
  endpoint's page size.
- The checks of plain arguments (`require_bool`, `optional_text`, `day_argument`,
  `one_or_any_of`, `futures_venues`) and `value_table`, which boxes a table passed where a
  dataset takes one value, are the first step of a dataset method, before `build_query`.
- `ticker_key`, `adjustment_notes`, and the constants beside them are what the namespace
  modules declare about their datasets' sources and columns when they build their specs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from datetime import date, datetime
from typing import TYPE_CHECKING, Final, TypeVar

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
from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable, check_symbols
from deephaven_massive._enums import FuturesVenue, OnError, Order, Timespan, _require_enum
from deephaven_massive._errors import ErrorSource, MassiveError, library_bug, required
from deephaven_massive._massive import ParamKind
from deephaven_massive._tables.time_text import deephaven_time_text
from deephaven_massive.queries import FetchQuery, Query
from deephaven_massive.queries._call import QueryCall
from deephaven_massive.queries._choice import bar_query_class, query_class
from deephaven_massive.queries._query import made_query

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._datasets.symbols import NormalizedSymbols, SymbolsLike
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "ADJUSTED_PRICE",
    "ADJUSTED_VOLUME",
    "EVERY_TICKER",
    "SNAPSHOT_SYMBOLS_PER_REQUEST",
    "accepted_values",
    "adjustment_notes",
    "build_query",
    "day_argument",
    "list_params",
    "list_query",
    "one_or_any_of",
    "optional_text",
    "require_bool",
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


_Q = TypeVar("_Q", bound=Query)

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
    path = core.catalog.rest_path(required(spec.fetch_path, f"{spec.name}'s fetch path"))
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
        return parse_date_like(value, name, upper=upper)
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
        rest_path = core.catalog.rest_path(template)
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


def _venue_choice(
    core: SessionCore, spec: DatasetSpec, *, venues: object, refused_venues: object
) -> tuple[tuple[FuturesVenue, ...] | None, OnError | None]:
    """Return the venues a futures `history()` reads and what it does with a refused one.

    A dataset's flat files are per venue when its prefix holds the `{venue}` placeholder. When
    the call names its venues, a refused one follows the call's `refused_venues`, or
    `MassiveConfig.refused_venues` when the call left that out. When the call left `venues`
    out, it reads every venue the plan allows: each one S3 refuses is skipped and logged, as
    `dm.OnError.LOG` does, whatever `refused_venues` says.

    Args:
        core: The session core, for `MassiveConfig.refused_venues`.
        spec: The dataset.
        venues: The `venues` argument as passed; None when it was left out.
        refused_venues: The `refused_venues` argument as passed; None when it was left out.

    Returns:
        The venues in `FuturesVenue` order (`futures_venues`) and the `Request.refused_venues`
        member; (None, None) for a dataset without venues.

    Raises:
        TypeError: The errors of `futures_venues` for a named `venues`, and the error of
            `_require_enum` for a `refused_venues` that is neither None nor a `dm.OnError`.
        ValueError: The error of `futures_venues` for an empty `venues`.
    """
    if not any("{venue}" in prefix for prefix in spec.flat_file_prefixes.values()):
        return None, None
    policy = (
        core.config.refused_venues
        if refused_venues is None
        else _require_enum(refused_venues, OnError, "refused_venues")
    )
    if venues is None:
        return tuple(FuturesVenue), OnError.LOG
    return futures_venues(venues), policy


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


def _normalized_symbols(
    core: SessionCore,
    spec: DatasetSpec,
    symbols: SymbolsLike | None,
    *,
    symbols_optional: bool,
    all_alternative: str | None,
) -> NormalizedSymbols | None:
    """Check a dataset call's `symbols` and return them normalized (`build_query`).

    Args:
        core: The session core, whose catalog says whether the dataset takes symbols.
        spec: The dataset.
        symbols: The caller's instruments, or None.
        symbols_optional: True for a dataset whose `symbols` only filters the request.
        all_alternative: None when the dataset accepts `dm.ALL`; otherwise what the
            `ValueError` tells the caller to do instead.

    Returns:
        A `SymbolTable` boxing a table, the checked tickers or `ALL` (`check_symbols`), or
        None when `symbols` is None.

    Raises:
        ValueError: If `symbols` is None for a dataset that needs symbols, a table lacks a
            `Symbol` column, or `dm.ALL` is given to a dataset that refuses it.

    A `Symbol` column that is not text raises the `TypeError` of `_require_text_column`, and
    a ticker `check_symbols` refuses its error.
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
    if isinstance(normalized, AllSymbols) and all_alternative is not None:
        raise ValueError(f"{spec.name} does not accept dm.ALL; {all_alternative}")
    return normalized


def _typed_params(
    core: SessionCore, spec: DatasetSpec, params: Mapping[str, object]
) -> dict[str, object]:
    """Check a dataset call's typed parameters and add the ones the library sets itself.

    Args:
        core: The session core, whose catalog checks `order` and `sort`.
        spec: The dataset.
        params: The typed parameters by wire name, None where not set.

    Returns:
        The typed parameters with the dataset's `fixed_params` (`_fixed_params`), None values
        kept.

    Raises:
        ValueError: If `params` names `adjusted` or a fixed parameter, or sets `order`
            without `sort` on a dataset whose paths take the direction as a suffix of `sort`.

    A `sort` with a suffixed part beside `order` raises the `ValueError` of `Catalog.check_sort`.
    """
    if "adjusted" in params:
        raise ValueError("adjusted is set through build_query(adjusted=...), not params")
    library = _fixed_params(spec, params)
    typed = {k: v for k, v in params.items() if v is not None}
    if "order" in typed and "sort" not in typed and not core.catalog.takes_param(spec, "order"):
        raise ValueError(
            f"{spec.name}: order needs sort, since its paths take the direction as a suffix "
            "of the sort field"
        )
    sort = typed.get("sort")
    if isinstance(sort, str):  # `list_params` refused any other type
        core.catalog.check_sort(spec, sort, ordered="order" in typed)
    return {**params, **library}


def _check_bar_period(timespan: object, multiplier: object) -> None:
    """Check the types of a `bars()` call's timespan and multiplier, before any request.

    Only the types are checked. Massive judges the values: it answers HTTP 400 to a
    multiplier below 1 and to `session` bars outside futures, so both are sent as given.

    Args:
        timespan: The `timespan` argument as passed.
        multiplier: The `multiplier` argument as passed.

    Raises:
        TypeError: If `multiplier` is not an `int` (a `bool` is not accepted).

    A `timespan` that is not a `dm.Timespan` raises the `TypeError` of `_require_enum`.
    """
    _require_enum(timespan, Timespan, "timespan")
    if isinstance(multiplier, bool) or not isinstance(multiplier, int):
        raise TypeError(f"multiplier must be an int, not {type(multiplier).__name__}")


def _query_class(
    spec: DatasetSpec, query_type: type[_Q], *, timespan: Timespan | None, multiplier: int
) -> type[_Q]:
    """Derive the class of a dataset's query, and check it against the declared class.

    The class follows from the dataset's spec (`query_class`), so no dataset method names it by
    hand; a `bars()` call's class also follows from its timespan and multiplier
    (`bar_query_class`), since Massive streams only some bars. The method declares the class it
    returns, and a `bars()` method declares `HistoryQuery`, the class every bars query is; a
    derived class that is neither that class nor a subclass of it means the method's signature
    and its dataset disagree, a defect of the library that no caller can cause.

    Args:
        spec: The dataset.
        query_type: The class the dataset method declares it returns.
        timespan: The call's timespan; a bars call's has passed `_check_bar_period`.
        multiplier: The call's multiplier.

    Returns:
        The class to build.

    Raises:
        MassiveError: A library defect, if the derived class is not `query_type` or a
            subclass of it.
    """
    cls: type[Query]
    if spec.is_bars:
        assert timespan is not None  # `_check_bar_period` refused anything but a Timespan
        cls = bar_query_class(spec, timespan, multiplier)
    else:
        cls = query_class(spec)
    if not issubclass(cls, query_type):
        message = f"m.{spec.name} gives a {cls.__name__}, not the {query_type.__name__} it declares"
        raise MassiveError(library_bug(message), source=ErrorSource.LIBRARY)
    return cls


def build_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    query_type: type[_Q],
    symbols: SymbolsLike | None = None,
    symbols_optional: bool = False,
    all_alternative: str | None = None,
    timespan: Timespan | None = None,
    multiplier: int = 1,
    venues: AbstractSet[FuturesVenue] | None = None,
    refused_venues: OnError | None = None,
    adjusted: bool | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    arguments: Mapping[str, str] | None = None,
    filters: Mapping[str, FilterValue] | None = None,
    limit: int | None = None,
    values: ValueTable | None = None,
) -> _Q:
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
        query_type: The query class the dataset method declares it returns. The class of the
            query is derived from the spec (`query_class`; for bars, `bar_query_class`), and must
            be this class or a subclass of it.
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
        timespan: The bar timespan; for a bars dataset (`DatasetSpec.is_bars`) it is checked
            with the multiplier (`_check_bar_period`).
        multiplier: The bar multiplier.
        venues: For a dataset with venues, the futures venues whose flat files `history()`
            reads, as the call named them; None when the call left them out, for every venue
            the plan allows (`_venue_choice`). Ignored for a dataset without venues.
        refused_venues: For a dataset with venues, what `history()` does with a named venue
            whose flat files S3 refuses; None when the call left it out, for
            `MassiveConfig.refused_venues` (`_venue_choice`).
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
        An instance of the spec's query class (`query_class`, or `bar_query_class` for a
        `bars()` call), whose call names only the dataset; the dataset method's
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
    `_datasets/symbols.py`, `_require_enum`, `_venue_choice`, `Catalog.check_sort`,
    `parse_date_like` in `_datasets/dates.py`, `_check_axis` for mixed, reversed, or
    calendar-date bounds, `_check_bar_period`, `require_positive_int` for `limit`). A `query_type`
    the dataset can't give is a library defect, the `MassiveError` of `_query_class`.
    """
    normalized = _normalized_symbols(
        core,
        spec,
        symbols,
        symbols_optional=symbols_optional,
        all_alternative=all_alternative,
    )
    if adjusted is not None and not isinstance(adjusted, bool):
        raise TypeError(f"adjusted must be a bool, not {type(adjusted).__name__}")
    sent = _typed_params(core, spec, params or {})
    if spec.is_bars:
        _check_bar_period(timespan, multiplier)
    elif timespan is not None:
        _require_enum(timespan, Timespan, "timespan")
    cls = _query_class(spec, query_type, timespan=timespan, multiplier=multiplier)
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
    read_venues, refused = _venue_choice(core, spec, venues=venues, refused_venues=refused_venues)
    request = Request(
        symbols=normalized,
        timespan=timespan,
        multiplier=multiplier,
        venues=read_venues,
        refused_venues=refused,
        adjusted=adjusted,
        start=first,
        end=last,
        params=tuple(sorted((k, _value(k, v)) for k, v in sent.items() if v is not None)),
        filters=check_filters(core.catalog, spec, dict(given), typed=params or {}),
        # Refused below 1: Massive answers `limit=0` with HTTP 200 and no rows, which reads as
        # an empty result rather than an error (the dividends, sent `limit=0`, did).
        limit=None if limit is None else require_positive_int("limit", limit),
        value_table=values,
    )
    return made_query(cls, core=core, spec=spec, request=request, call=QueryCall(f"m.{spec.name}"))


def list_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    all_alternative: str,
    symbols: SymbolsLike | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    arguments: Mapping[str, str] | None = None,
    sort: str | None = None,
    order: Order | None = None,
    limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None,
) -> FetchQuery:
    """Build the query of a REST list dataset from its factory's arguments.

    A list dataset takes the `sort`, `order`, and `limit` block (`list_params`) beside its own
    typed parameters, and has only a fetch path, so `build_query` builds a `FetchQuery`. A ticker
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
    return build_query(
        core,
        spec,
        query_type=FetchQuery,
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
