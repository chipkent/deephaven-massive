"""The `filters=` rules: which wire parameters a dataset's `filters` may set, and their values.

A dataset factory takes `filters`, a mapping from a filter key to a value, for any query
parameter of the dataset's REST paths that the library does not set itself. This module owns
every rule about them:

- `filter_key`: a wire parameter's key in `filters`, its dots written as underscores
  (`strike_price_gte` for `strike_price.gte`);
- `param_family` and `symbol_family`: the forms of a parameter the library reserves, the whole
  family of the time axis and of each typed parameter, and the equality forms of the symbol
  parameter;
- `takes_instants`: whether a date parameter compares instants or calendar days, read from the
  generated field it is named after;
- `check_filters`: the validation a query runs when it is built, which puts each value in the
  form that is sent and checks it with the wire package's own encoder.

The rules read the catalog's generated REST paths (`Catalog.rest_path`) and the dataset's
declaration; they make no I/O, start no thread, and keep no state, so every function is safe on
any thread. The REST transport sends the checked values (`_ingest/rest.py`), and the dataset
factories reserve the typed parameters (`_namespaces/query_building.py`).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.dates import instant_bound, parse_date_like, require_aware
from deephaven_massive._massive import ParamKind, WireType, encode_value

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.request import ParamValue
    from deephaven_massive._massive import RestPath, WireUnit

__all__ = [
    "check_filters",
    "filter_key",
    "param_family",
    "symbol_family",
    "takes_instants",
]


_LIBRARY_PARAMS: Final = frozenset({"limit", "sort", "order", "adjusted"})
"""Query parameters the library sets from typed arguments, never through `filters`."""

_TIME_KINDS: Final = frozenset({ParamKind.DATE, ParamKind.TIMESTAMP})
"""The parameter kinds whose text value must be an ISO-8601 date or an instant with an offset."""

_FILTER_SUFFIXES: Final = ("gt", "gte", "lt", "lte", "any_of", "all_of")
"""The suffixes the specification adds to a parameter's base name (`timestamp.gte`)."""

_EQUALITY_SUFFIXES: Final = ("any_of", "all_of")
"""The suffixes that, like the base name, select rows by equal values (`tickers.any_of`)."""

_INSTANT_TYPES: Final = frozenset({WireType.ISO_INSTANT, WireType.EPOCH})
"""Wire types of a row field that holds an instant rather than a calendar date."""


def param_family(name: str) -> frozenset[str]:
    """Return a wire parameter, its base name, and every suffixed form of the base.

    The time axis reserves this whole family: no filter may bound or select the axis beside
    `start` and `end`, although `start` and `end` cannot express an `.any_of` list of dates.

    Args:
        name: A wire parameter name, such as `"timestamp.gte"` or `"ticker"`.

    Returns:
        The base (`"timestamp"`) and its `.gt`, `.gte`, `.lt`, `.lte`, `.any_of`, and
        `.all_of` forms.
    """
    base = name.split(".", 1)[0]
    return frozenset({base, *(f"{base}.{suffix}" for suffix in _FILTER_SUFFIXES)})


def symbol_family(name: str) -> frozenset[str]:
    """Return the equality forms of a symbol parameter, which only `symbols` may set.

    The range forms stay filters: `ticker_gte` and `ticker_lt` select an alphabetical range
    of tickers, which `symbols` cannot express.

    Args:
        name: The symbol parameter, such as `"ticker.any_of"` or `"tickers"`.

    Returns:
        The base (`"ticker"`) and its `.any_of` and `.all_of` forms.
    """
    base = name.split(".", 1)[0]
    return frozenset({base, *(f"{base}.{suffix}" for suffix in _EQUALITY_SUFFIXES)})


def filter_key(name: str) -> str:
    """Return the `filters` key of a wire parameter: its dots written as underscores.

    Args:
        name: The wire name, such as `"strike_price.gte"`.

    Returns:
        The key, such as `"strike_price_gte"`.
    """
    return name.replace(".", "_")


def takes_instants(rest_path: RestPath, name: str, kind: ParamKind) -> bool:
    """Tell whether a REST parameter compares instants rather than calendar days.

    A timestamp parameter does. Massive also types some instant filters as dates
    (`published_utc.gte` on the news paths, `last_updated.gte` on the benzinga paths); the
    generated row field the parameter is named after says which: its wire key is the
    parameter's base name, and an ISO-8601 or epoch instant field makes the parameter compare
    instants. A date parameter over a calendar-date field (`ex_dividend_date`), or with no
    top-level field of its name, compares days.

    Args:
        rest_path: The path, whose generated fields are read.
        name: The wire name of one of the path's parameters, such as `"published_utc.gte"`.
        kind: That parameter's kind.

    Returns:
        True when a date given to the parameter must become an instant before it is sent.
    """
    if kind is ParamKind.TIMESTAMP:
        return True
    if kind is not ParamKind.DATE:
        return False
    base = name.partition(".")[0]
    return any(f.wire_key == base and f.wire_type in _INSTANT_TYPES for f in rest_path.fields)


def _list_items(spec: DatasetSpec, key: str, value: ParamValue, *, dates: bool) -> ParamValue:
    """Return a list filter's values one text item each, so every spelling gives one key.

    The wire joins a list with commas, so a comma can never be part of one value: `"a,b"`,
    `("a,b",)`, and `("a", "b")` all send `a,b`. Splitting every item on commas, and
    stripping the spaces around each part, makes them one value, so equal filters give equal
    queries. On a list of dates (`date.any_of`), a `date` item is its ISO-8601 day.

    Args:
        spec: The dataset, for messages.
        key: The filter's key, for messages.
        value: A `str`, or a tuple of `str` and, when `dates`, `date` items; anything else is
            returned unchanged for the encoder to refuse.
        dates: True when the list's base parameter is a date parameter.

    Returns:
        The tuple of text items.

    Raises:
        ValueError: If an item is empty (`"a,,b"`, `""`, or `" "`), which Massive would read
            as a value of its own, or a `date` item is given to a list that does not take
            dates.
    """
    if isinstance(value, str):
        value = (value,)
    if not isinstance(value, tuple):
        return value
    texts: list[str] = []
    for item in value:
        if isinstance(item, date) and not isinstance(item, datetime):
            if not dates:
                raise ValueError(f"{spec.name} filter {key!r} takes text items, not {item!r}")
            texts.append(item.isoformat())
        elif isinstance(item, str):
            texts.extend(part.strip() for part in item.split(","))
        else:
            return value  # the encoder names the item it cannot send
    if "" in texts:
        raise ValueError(f"{spec.name} filter {key!r} has an empty value in {value!r}")
    return tuple(texts)


def _check_day_filter(
    spec: DatasetSpec, value: date, *, key: str, name: str, unit: WireUnit | None
) -> None:
    """Check that a date given to a range filter of an instant parameter can be sent exactly.

    The REST transport sends such a date as the instant that keeps the filter's calendar
    meaning, in the day zone of each request's symbols (`dates.instant_bound`): `_gte` a day
    is from its start, `_lt` a day before its start, `_gt` a day after its end, and `_lte` a
    day up to its end. The date itself is stored, since the zone depends on the symbols. The
    end of a day is the next day's start less one unit of the parameter, which needs a unit
    a `datetime` can step by exactly; the same function decides that here, at construction.

    Args:
        spec: The dataset, for messages.
        value: The date.
        key: The filter's key, for messages.
        name: The wire name of the parameter, which compares instants (`takes_instants`).
        unit: The parameter's timestamp unit, or None for a date parameter, which takes the
            instant as ISO-8601 text.

    Raises:
        ValueError: If the filter is a `_gt` or `_lte` bound and the parameter counts
            nanoseconds or takes ISO-8601 instants of no documented precision, so the end of
            the day cannot be written exactly; the message names the forms that work.
    """
    base, _, suffix = name.partition(".")
    if suffix not in ("gt", "gte", "lt", "lte"):
        return
    try:
        instant_bound(spec.asset_class, name, value, unit=unit, upper=suffix in ("gt", "lte"))
    except ValueError:
        raise ValueError(
            f"{spec.name} filter {key!r}: {name} has no exact unit to end a day with; "
            f"pass an aware datetime, or a date to {base}_gte or {base}_lt"
        ) from None


def _sent_value(
    spec: DatasetSpec,
    rest_path: RestPath,
    value: ParamValue,
    *,
    key: str,
    name: str,
    kind: ParamKind,
    unit: WireUnit | None,
) -> ParamValue:
    """Return a normalized `filters` value in the form that is sent.

    The request must carry the value, never the caller's spelling, so it is turned into its
    wire form here, once, and that form is both checked and stored: ISO-8601 text of a date
    or timestamp parameter is parsed, and a list is split into its items (`_list_items`). A
    date on a range filter of a parameter that compares instants stays a date, because its
    day zone follows each request's symbols; the REST transport turns it into its day's
    bound, and `_check_day_filter` refuses here the forms it could not send exactly.

    Args:
        spec: The dataset, for messages.
        rest_path: The path the parameter belongs to, whose fields say if it compares instants.
        value: The value; sequences are already tuples.
        key: The filter's key, for messages.
        name: The parameter's wire name.
        kind: The parameter's kind.
        unit: The parameter's timestamp unit, or None.

    Returns:
        The value to store; any other value unchanged.

    Text that is not ISO-8601, or has a time without an offset, raises the `ValueError` of
    `dates.parse_date_like`; a bad list or day bound the `ValueError` of `_list_items` or
    `_check_day_filter`.
    """
    if kind is ParamKind.LIST:
        base = name.rpartition(".")[0]
        dates = any(p.name == base and p.kind is ParamKind.DATE for p in rest_path.params)
        return _list_items(spec, key, value, dates=dates)
    if isinstance(value, str) and kind in _TIME_KINDS:
        value = parse_date_like(value, key)
    if (
        isinstance(value, date)
        and not isinstance(value, datetime)
        and takes_instants(rest_path, name, kind)
    ):
        _check_day_filter(spec, value, key=key, name=name, unit=unit)
    return value


def check_filters(
    catalog: Catalog,
    spec: DatasetSpec,
    filters: Mapping[str, ParamValue],
    *,
    typed: Iterable[str] = (),
) -> tuple[tuple[str, ParamValue], ...]:
    """Validate a `filters` mapping and return the values the request carries and sends.

    Runs synchronously when the query is built. A key is a wire parameter's name with its
    dots written as underscores (`strike_price_gte` for `strike_price.gte`), for any query
    parameter of any of the dataset's REST paths, except the ones the library sets itself:
    the equality forms of the symbol parameter (`symbol_family`: its base, `.any_of`, and
    `.all_of`), the whole family of the time axis (`param_family`: its base and every
    suffixed form), the whole family of each of the dataset's `fixed_params`, `limit`,
    `sort`, `order`, and `adjusted`. None of these is accepted or named among the valid
    keys.

    Every path that declares the key checks it, and all of them must read it alike, since
    any of them may send it. Each value is first put in the form that is sent
    (`_sent_value`): ISO-8601 text of a
    date or timestamp parameter becomes a `date` or an aware `datetime`; a date on a range
    filter of a parameter that compares instants (`takes_instants`, such as
    `last_updated_gte`) stays a date, which the REST transport sends as its day's bound
    in the day zone of each request's symbols, after this check has refused a `_gt` or
    `_lte` form whose unit cannot end a day exactly; and a list becomes its
    comma-separated items, so `"a,b"` and `("a", "b")` are one value. The parsed value is
    then checked by the wire package's own encoder (`encode_value`), the same function
    that writes it into the request, so a filter that passes here cannot fail to encode
    later.

    Args:
        catalog: The catalog, whose generated REST paths declare the filters.
        spec: The dataset.
        filters: The normalized filters by key; sequences are already tuples.
        typed: The wire names of the factory's typed parameters, set or not; each one's
            whole family (`param_family`) is reserved like the time axis, so neither
            accepted nor named among the valid keys.

    Returns:
        The `(key, value)` pairs in the order given, each value in its parsed form; empty
        when `filters` is.

    Raises:
        ValueError: If a key is not a filter of the dataset (the message lists the valid
            keys), or a value cannot be sent as the parameter's kind: the wrong type, a
            value outside an enum parameter's values, a non-finite number, an empty list,
            or an instant finer than a timestamp parameter's unit (the message quotes the
            encoder's). A naive `datetime`, or text of a date or timestamp parameter that
            is not ISO-8601 or has a time without an offset, raises the `ValueError` of
            `dates.require_aware` or `dates.parse_date_like`. An empty list item, or a
            date on a `_gt` or `_lte` filter whose parameter has no exact unit to end a
            day with, raises the `ValueError` of `_list_items` or `_check_day_filter`. A
            value two of the dataset's paths read differently (a date parameter on one, a
            text parameter on another) raises `ValueError`, and a path that cannot encode
            it names itself in the message.

    An unknown REST path raises the `KeyError` of `Catalog.source`.
    """
    if not filters:
        return ()
    symbols = () if spec.symbol_param is None else symbol_family(spec.symbol_param)
    fixed = (form for name in spec.fixed_params for form in param_family(name))
    families = (form for name in typed for form in param_family(name))
    reserved = {*_LIBRARY_PARAMS, *spec.axis_params, *symbols, *families, *fixed}
    # Every path that declares a filter, with the parameter's position in it.
    params: dict[str, list[tuple[RestPath, int]]] = {}
    for path in spec.rest_paths:
        rest_path = catalog.rest_path(path)
        for index, param in enumerate(rest_path.params):
            if not param.in_path and param.name not in reserved:
                params.setdefault(filter_key(param.name), []).append((rest_path, index))
    checked: list[tuple[str, ParamValue]] = []
    for key, value in filters.items():
        if isinstance(value, datetime):
            require_aware(key, value, value)
        found = params.get(key)
        if found is None:
            raise ValueError(f"{spec.name} has no filter {key!r}; valid filters: {sorted(params)}")
        label = f"{spec.name} filter {key!r}"
        forms = []
        for rest_path, index in found:
            param = rest_path.params[index]
            parsed = _sent_value(
                spec,
                rest_path,
                value,
                key=key,
                name=param.name,
                kind=param.kind,
                unit=param.unit,
            )
            try:
                encode_value(param, parsed, label=label)
            except (TypeError, ValueError) as error:
                where = f" (on {rest_path.template})" if len(found) > 1 else ""
                raise ValueError(f"{error}{where}") from None
            forms.append(parsed)
        first = forms[0]
        if any(type(form) is not type(first) or form != first for form in forms):
            raise ValueError(
                f"{label}: the dataset's paths read {value!r} as different values "
                f"({[f'{type(form).__name__} {form!r}' for form in forms]}), so it cannot "
                "be sent alike"
            )
        checked.append((key, first))
    return tuple(checked)
