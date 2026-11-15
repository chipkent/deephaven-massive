"""Turn the rows of a REST page into one Arrow batch of the path's generated fields.

`to_batch` is the only code that walks a REST row: the decoded JSON goes straight to Arrow,
with no row object or model class between them. Each column is one `Field` of the path's
`RestPath`, in the generated order, named by its long-form column. A nested object is reached
through the field's dotted wire key (`"day.c"` is the `c` of the row's `day` object).

Each value is checked and converted by `spec.convert`, the converter every JSON decoder
shares (a field with a `conversion` through that named conversion, from the vendor's documented
form to its `wire_type`), and stored as:

| `WireType` | Arrow type |
|---|---|
| `long`, `epoch` (a raw count in its unit; `_datasets/timestamps.py` converts it) | `int64` |
| `double` | `float64` |
| `bool` | `bool` |
| `string`, `json-text` (compact JSON) | `string` |
| `date` | `date32` |
| `iso-instant` (exact integer nanoseconds; never offset-less) | `timestamp[ns, UTC]` |
| `long-array` | `list<int64>` |
| `string-array` | `list<string>` |

A value that does not fit raises `WireError`: values are never coerced.

**Two readings, one result.** A page is read column by column: each field's values are gathered
into one list, `spec.convert.column_converter` checks the whole list at once, and the list goes to
Arrow in one call, so a column of plain numbers or text costs no Python call per value. The
WebSocket decoder reads each frame the same way. A page that column reading cannot vouch for
(an odd row, or a value its converter refuses) is read again row by row, the reference reading,
which judges each value on its own. Either way the batch and the drift are the same; a page read
row by row logs why at DEBUG.

**Drift** is reported, not raised, in the returned `DriftReport`:

- `extra`: keys a row had that the path does not declare (dropped). A nested object the path
  does not reach into is reported by its own key, not by each of its leaves. A key the path
  lists as ignored (`Source.ignored_keys`) is dropped without being reported.
- `missing`: the fields the vendor marks required (`Field.required`) that some row lacked or
  held as null. Every other absent field is a null, silently: JSON omits absent values and the
  vendor marks almost nothing required.
- `unconvertible`: the fields that held, in some row, a value that does not fit their type or
  named conversion, read as null. A nested object the fields reach into that holds neither an
  object nor null is reported by its own key, and its fields read as absent.

  Each extra or unconvertible key carries a sample (`DriftReport.samples`): its value in the
  first row that had it, so the library's WARNING can quote it.

A key the caller names in `required` (the dataset's key columns for this path) that is absent,
null, or does not fit in a row means the row cannot be placed: the row is dropped and counted
(`DriftReport.dropped_rows`, with the first row's reason in `dropped_reason`). Whether a page
whose every row was dropped is an error is the library's decision. A dropped row, and a row
holding a value that does not fit, is logged whole at DEBUG (up to 2,000 characters of its
JSON) on this module's logger, so the log shows every field it held.

The function is safe from any thread and changes nothing but its own result and that DEBUG
line. This module imports only the standard library, `pyarrow`, `net`, and `spec`.
"""

from __future__ import annotations

import dataclasses
import itertools
import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from deephaven_massive._massive.spec.convert import (
    UnconvertibleError,
    column_converter,
    convert_field,
    excerpt,
    missing_key_message,
    row_excerpt,
)
from deephaven_massive._massive.spec.types import DriftReport, DriftSample, WireType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Mapping, Sequence

    from deephaven_massive._massive.spec.types import Field, RestPath

_log = logging.getLogger(__name__)

__all__ = ["to_batch"]

# The Arrow type of each wire type's column.
_ARROW: dict[WireType, pa.DataType] = {
    WireType.LONG: pa.int64(),
    WireType.EPOCH: pa.int64(),
    WireType.DOUBLE: pa.float64(),
    WireType.BOOL: pa.bool_(),
    WireType.STRING: pa.string(),
    WireType.DATE: pa.date32(),
    WireType.ISO_INSTANT: pa.timestamp("ns", tz="UTC"),
    WireType.LONG_ARRAY: pa.list_(pa.int64()),
    WireType.STRING_ARRAY: pa.list_(pa.string()),
    WireType.JSON_TEXT: pa.string(),
}

_NO_OBJECT: Final[Mapping[str, object]] = MappingProxyType({})
"""Stands in for an absent or null nested object, whose fields are all absent."""


def _prefixes(fields: Sequence[Field]) -> frozenset[str]:
    """Return every dotted prefix of the fields' wire keys: the objects a row is walked into.

    Args:
        fields: The path's fields.

    Returns:
        For `"day.c"` and `"day.o"`, `{"day"}`; for `"a.b.c"`, `{"a", "a.b"}`.
    """
    return frozenset(
        key.rsplit(".", depth)[0]
        for key in (f.wire_key for f in fields)
        for depth in range(1, key.count(".") + 1)
    )


def _leaves(
    row: Mapping[str, object], prefix: str, *, objects: frozenset[str]
) -> Iterator[tuple[str, object]]:
    """Walk a row's keys, descending into the nested objects the fields reach into.

    Args:
        row: A decoded JSON object, the row or an object nested in it.
        prefix: The dotted path of `row` within the top-level row, with a trailing dot, or
            `""` at the top.
        objects: The dotted prefixes of the fields' wire keys, from `_prefixes`.

    Yields:
        `(dotted key, value)` for each key reached that is not an object holding fields: a
        field's key, a key the path does not declare, or an object prefix whose value is
        neither an object nor null (which the caller rejects). A null object yields nothing,
        so its fields are absent.
    """
    for key, value in row.items():
        path = prefix + key
        if path in objects and isinstance(value, dict):
            yield from _leaves(value, f"{path}.", objects=objects)
        elif path not in objects or value is not None:
            yield path, value


def _row(
    row: Mapping[str, object],
    *,
    keys: frozenset[str],
    objects: frozenset[str],
    extra: dict[str, object],
    odd: dict[str, object],
) -> dict[str, object]:
    """Return the declared values of one row, by wire key, and collect its undeclared keys.

    Args:
        row: The row.
        keys: The wire keys of the path's fields.
        objects: The dotted prefixes of the fields' wire keys.
        extra: Updated in place with each undeclared key of the row not already in it, and
            the key's value, so it keeps the first value seen.
        odd: Updated in place, the same way, with each object the fields reach into that holds
            neither an object nor null; its fields are absent from the result.

    Returns:
        Each field's raw value by wire key; an absent field is absent here too.
    """
    found: dict[str, object] = {}
    for key, value in _leaves(row, "", objects=objects):
        if key in objects:
            odd.setdefault(key, value)
        elif key in keys:
            found[key] = value
        else:
            extra.setdefault(key, value)
    return found


def to_batch(
    rows: Sequence[Mapping[str, object]], rest_path: RestPath, *, required: Collection[str]
) -> tuple[pa.RecordBatch, DriftReport]:
    """Convert a page's rows into one Arrow batch of the path's fields, and report drift.

    Call it on each `Page.rows` the REST client returns; the library then renames, merges,
    and converts the batch into its dataset's table. It allocates the batch and nothing else,
    and never raises for a row: an odd value is drift, and a row that cannot be placed is
    dropped and counted.

    Args:
        rows: The page's rows, decoded JSON objects in server order.
        rest_path: The path the rows came from; its `fields` give the columns, in order.
        required: The long-form columns every row must carry with a value: the dataset's key
            columns for this path that it does not fill from the request (its `Symbol` and
            `Timestamp` sources, for instance). Empty when the dataset declares no key.

    Returns:
        The batch, one column per field named by `Field.column` and typed by the module's
        table, one row per input row that was not dropped, in server order; and the
        `DriftReport` of the rows (`extra`, `missing`, `unconvertible`, and the dropped rows;
        `statuses` is always empty here). A row is dropped when a `required` column is absent,
        null, or holds a value that does not fit; `dropped_reason` is then the path's template,
        the first such row's index, and the clause `spec.convert` shares with every decoder
        (the field, the expected type, and the first 200 characters of the value's JSON text).
        Any other value that does not fit its field's type or named conversion is null, its
        wire key in `unconvertible`.

    Raises:
        ValueError: If `required` names a column the path does not have.
    """
    fields = rest_path.fields
    unknown = set(required) - {f.column for f in fields}
    if unknown:
        raise ValueError(f"{rest_path.template} has no columns {sorted(unknown)}")
    objects = _prefixes(fields)
    reading = _by_column(rows, fields, objects=objects, required=required)
    if isinstance(reading, str):
        _log.debug("%s: reading %d rows one by one: %s", rest_path.template, len(rows), reading)
        reading = _by_row(rows, rest_path, objects=objects, required=required)
    schema = pa.schema([pa.field(f.column, _ARROW[f.wire_type]) for f in fields])
    batch = pa.RecordBatch.from_arrays(reading.arrays, schema=schema)
    ignored = sorted(rest_path.ignored_keys & reading.extra.keys())
    if ignored:
        _log.debug("%s: dropped the keys it ignores, %s", rest_path.template, ignored)
        for key in ignored:
            del reading.extra[key]
    odd = {**reading.extra, **reading.unconvertible}
    return batch, DriftReport(
        extra=frozenset(reading.extra),
        missing=frozenset(reading.missing),
        unconvertible=frozenset(reading.unconvertible),
        dropped_rows=reading.dropped,
        dropped_reason=reading.reason,
        samples={key: DriftSample(None, excerpt(value)) for key, value in odd.items()},
    )


@dataclasses.dataclass(slots=True)
class _Reading:
    """A page read into columns, and what the reading found.

    Attributes:
        arrays: The columns, one per field, holding the rows that were not dropped.
        extra: The undeclared keys, with the first value of each.
        missing: The wire keys of the vendor-required fields some row lacked or held as null.
        unconvertible: The wire keys whose value did not fit in some row, with the first such
            value; read as null.
        dropped: How many rows were dropped for a required column.
        reason: Why the first dropped row was dropped; `""` when none was.
    """

    arrays: list[pa.Array]
    extra: dict[str, object]
    missing: set[str]
    unconvertible: dict[str, object] = dataclasses.field(default_factory=dict)
    dropped: int = 0
    reason: str = ""


def _objects_under(
    holders: Sequence[Mapping[str, object]], key: str
) -> list[Mapping[str, object]] | None:
    """Return the object each holder keeps under a key, an empty one where it holds none.

    Args:
        holders: The rows, or the objects one level into them.
        key: The key to read in each.

    Returns:
        One object per holder; or None when some holder keeps a value that is not an object.
    """
    found: list[Mapping[str, object]] = []
    for holder in holders:
        value = holder.get(key)
        if value is None:
            found.append(_NO_OBJECT)
        elif type(value) is dict:
            found.append(value)
        else:
            return None
    return found


def _by_column(
    rows: Sequence[Mapping[str, object]],
    fields: Sequence[Field],
    *,
    objects: frozenset[str],
    required: Collection[str],
) -> _Reading | str:
    """Convert a page column by column, or say why the page must be read row by row.

    The fast path, taken by nearly every page: each field's values are gathered into one list
    (through the nested objects the fields reach into), the types of a whole list are checked at
    once (`spec.convert.column_converter`), and the list goes to Arrow in one call. A column of
    plain values of its type needs no call per value; any other column calls the field's
    converter once per value. Undeclared keys are found by one subset test
    per object, and only an object that has one is walked.

    It never judges an odd row: anything it is not sure of (a nested object that is neither an
    object nor null, a required key that is absent or null, a value the converter refuses, an
    integer out of its column's range) makes it return the reason instead, and `to_batch` reads
    the page row by row (`_by_row`), which judges each value on its own.

    Args:
        rows: The page's rows.
        fields: The path's fields.
        objects: The dotted prefixes of the fields' wire keys (`_prefixes`).
        required: The long-form columns every row must carry with a value.

    Returns:
        The reading, with no value found unconvertible and no row dropped; or, when the page
        must be read row by row, why.
    """
    # Each object the fields reach into, one per row, by its dotted prefix; "" is the rows.
    levels: dict[str, Sequence[Mapping[str, object]]] = {"": rows}
    for prefix in sorted(objects, key=lambda name: name.count(".")):
        parent, _, key = prefix.rpartition(".")
        found = _objects_under(levels[parent], key)
        if found is None:
            return f"the wire key {prefix!r} holds a value that is not an object"
        levels[prefix] = found
    extra: dict[str, object] = {}
    for prefix, level in levels.items():
        _undeclared(level, prefix, fields, objects=objects, into=extra)
    arrays: list[pa.Array] = []
    missing: set[str] = set()
    for f in fields:
        parent, _, key = f.wire_key.rpartition(".")
        values = [level.get(key) for level in levels[parent]]
        if None in values:
            if f.column in required:
                return f"the required column {f.column} is absent or null in some row"
            if f.required:
                missing.add(f.wire_key)
        array = _array(f, values)
        if array is None:
            return f"the column {f.column} holds a value that needs its own check"
        arrays.append(array)
    return _Reading(arrays, extra, missing)


def _undeclared(
    level: Sequence[Mapping[str, object]],
    prefix: str,
    fields: Sequence[Field],
    *,
    objects: frozenset[str],
    into: dict[str, object],
) -> None:
    """Collect the keys that objects at one level hold and the path does not declare.

    Args:
        level: The objects at one dotted prefix, one per row (the rows themselves for `""`).
        prefix: The dotted prefix, or `""` for the rows.
        fields: The path's fields.
        objects: The dotted prefixes of the fields' wire keys.
        into: Updated with each undeclared key, as a dotted path, and its first value.
    """
    start = f"{prefix}." if prefix else ""
    declared = frozenset(
        name[len(start) :].partition(".")[0]
        for name in itertools.chain((f.wire_key for f in fields), objects)
        if name.startswith(start)
    )
    if all(map(declared.issuperset, level)):
        return
    for item in level:
        for key, value in item.items():
            if key not in declared:
                into.setdefault(start + key, value)


def _array(field: Field, values: list[object]) -> pa.Array | None:
    """Convert one field's values to an Arrow array in one call, if that is certain to be right.

    Args:
        field: The field.
        values: Its value in each row, `None` where absent.

    Arrow checks the numbers in one native pass. When it refuses one (an integer out of the
    64-bit range, or one a double cannot hold exactly), the column is read again with every
    number checked and converted in Python, which rounds such an integer in a `double` column as
    the converter does.

    Returns:
        The array; or None when `spec.convert.column_converter` cannot vouch for a value, so the
        page must be read row by row.
    """
    arrow_type = _ARROW[field.wire_type]
    converted = column_converter(field, arrow_checks_numbers=True)(values)
    if converted is None:
        return None
    try:
        return pa.array(converted, type=arrow_type)
    except (OverflowError, pa.ArrowInvalid):
        checked = column_converter(field)(values)
        return None if checked is None else pa.array(checked, type=arrow_type)


def _by_row(
    rows: Sequence[Mapping[str, object]],
    rest_path: RestPath,
    *,
    objects: frozenset[str],
    required: Collection[str],
) -> _Reading:
    """Convert a page row by row, judging each value on its own.

    The reference reading of a page: `to_batch` takes it when `_by_column` cannot vouch for the
    fast path. Each row is walked key by key, and each value checked and converted on its own.
    A row whose required column is absent, null, or does not fit is dropped; any other value
    that does not fit is null. Each such row is logged whole at DEBUG (`_logged`).

    Args:
        rows: The page's rows.
        rest_path: The path the rows came from.
        objects: The dotted prefixes of the fields' wire keys (`_prefixes`).
        required: The long-form columns every row must carry with a value.

    Returns:
        The reading of the rows kept, with the values that did not fit and the rows dropped.
    """
    fields = rest_path.fields
    keys = frozenset(f.wire_key for f in fields)
    columns: list[list[object]] = [[] for _ in fields]
    reading = _Reading([], {}, set())
    for index, row in enumerate(rows):
        odd: dict[str, object] = {}
        found = _row(row, keys=keys, objects=objects, extra=reading.extra, odd=odd)
        problems = [
            f"wire key {key!r} expects an object, got {excerpt(v)}" for key, v in odd.items()
        ]
        values: list[object] = []
        absent: list[str] = []
        dropped = ""
        for f in fields:
            raw = found.get(f.wire_key)
            value, problem = _value(f, raw)
            if f.column in required and (raw is None or problem is not None):
                dropped = problem or missing_key_message(f)
                break
            if problem is not None:
                odd.setdefault(f.wire_key, raw)
                problems.append(problem)
            elif raw is None and f.required:
                absent.append(f.wire_key)
            values.append(value)
        if dropped:
            message = _logged(rest_path, index=index, row=row, problem=dropped)
            reading.dropped += 1
            reading.reason = reading.reason or message
            continue
        if problems:
            _logged(rest_path, index=index, row=row, problem="; ".join(problems))
        reading.missing.update(absent)
        for key, value in odd.items():
            reading.unconvertible.setdefault(key, value)
        for value, column in zip(values, columns, strict=True):
            column.append(value)
    reading.arrays = [
        pa.array(c, type=_ARROW[f.wire_type]) for c, f in zip(columns, fields, strict=True)
    ]
    return reading


def _value(field: Field, value: object) -> tuple[object, str | None]:
    """Convert one value of a row, or say why it cannot be read.

    Args:
        field: The value's field.
        value: Its raw value, or None when the row lacks it or holds it as null.

    Returns:
        The converted value and None, or `(None, None)` for an absent value; or, when the
        value does not fit, None and the clause `spec.convert` shares with every decoder.
    """
    if value is None:
        return None, None
    try:
        return convert_field(field, value), None
    except UnconvertibleError as error:
        return None, str(error)


def _logged(rest_path: RestPath, *, index: int, row: Mapping[str, object], problem: str) -> str:
    """Log a row the reading dropped or nulled a value of, whole, at DEBUG; return its reason.

    Args:
        rest_path: The path the row came from.
        index: The row's index in the page.
        row: The row, as decoded.
        problem: The clause `spec.convert` shares with every decoder: the field, the expected
            type, and the value, or the missing key.

    Returns:
        `"<template>: row <index>: <problem>"`.
    """
    message = f"{rest_path.template}: row {index}: {problem}"
    if _log.isEnabledFor(logging.DEBUG):  # the row's JSON is built only for the line
        _log.debug("%s; the row: %s", message, row_excerpt(row))
    return message
