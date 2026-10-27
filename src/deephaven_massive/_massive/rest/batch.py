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
| `long`, `epoch` (a raw count in the field's unit; `_timeunits` converts it later) | `int64` |
| `double` | `float64` |
| `bool` | `bool` |
| `string`, `json-text` (compact JSON) | `string` |
| `date` | `date32` |
| `iso-instant` (exact integer nanoseconds; never offset-less) | `timestamp[ns, UTC]` |
| `int-array` | `list<int32>` |

A value that does not fit raises `WireError`: values are never coerced.

**Drift** is reported, not raised, in the returned `DriftReport`:

- `extra`: keys a row had that the path does not declare (dropped). A nested object the path
  does not reach into is reported by its own key, not by each of its leaves.
- `missing`: the fields the vendor marks required (`Field.required`) that some row lacked or
  held as null. Every other absent field is a null, silently: JSON omits absent values and the
  vendor marks almost nothing required.

A key the caller names in `required` (the dataset's key columns for this path) that is absent
or null in any row is not drift: it raises `WireError`, because the row cannot be placed.

The function is pure and safe from any thread. This module imports only the standard library,
`pyarrow`, `net`, and `spec`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.convert import (
    UnconvertibleError,
    convert_field,
    excerpt,
    missing_key_message,
)
from deephaven_massive._massive.spec.types import DriftReport, WireType

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Mapping, Sequence

    from deephaven_massive._massive.spec.types import Field, RestPath

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
    WireType.INT_ARRAY: pa.list_(pa.int32()),
    WireType.JSON_TEXT: pa.string(),
}


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
    rest_path: RestPath,
    *,
    keys: frozenset[str],
    objects: frozenset[str],
    extra: set[str],
) -> dict[str, object]:
    """Return the declared values of one row, by wire key, and collect its undeclared keys.

    Args:
        row: The row.
        rest_path: The path.
        keys: The wire keys of the path's fields.
        objects: The dotted prefixes of the fields' wire keys.
        extra: Updated in place with the row's undeclared keys.

    Returns:
        Each field's raw value by wire key; an absent field is absent here too.

    Raises:
        WireError: If an object the fields reach into is neither an object nor null.
    """
    found: dict[str, object] = {}
    for key, value in _leaves(row, "", objects=objects):
        if key in objects:
            raise WireError(
                f"{rest_path.template}: wire key {key!r} expects an object, got {excerpt(value)}",
                path=rest_path.template,
            )
        if key in keys:
            found[key] = value
        else:
            extra.add(key)
    return found


def to_batch(
    rows: Sequence[Mapping[str, object]], rest_path: RestPath, *, required: Collection[str]
) -> tuple[pa.RecordBatch, DriftReport]:
    """Convert a page's rows into one Arrow batch of the path's fields, and report drift.

    Call it on each `Page.rows` the REST client returns; the library then renames, merges,
    and converts the batch into its dataset's table. It allocates the batch and nothing else.

    Args:
        rows: The page's rows, decoded JSON objects in server order.
        rest_path: The path the rows came from; its `fields` give the columns, in order.
        required: The long-form columns every row must carry with a value: the dataset's key
            columns for this path that it does not fill from the request (its `Symbol` and
            `Timestamp` sources, for instance). Empty when the dataset declares no key.

    Returns:
        The batch, one column per field named by `Field.column` and typed by the module's
        table, one row per input row; and the `DriftReport` of the rows (`extra` and
        `missing`; `statuses` is always empty here).

    Raises:
        ValueError: If `required` names a column the path does not have.
        WireError: If a row lacks a `required` column or holds it as null, or a value does not
            fit its field's type or named conversion: the message is the path's template and
            the row's index, then the clause `spec.convert` shares with every decoder (the
            field, the expected type, and the first 50 characters of the value's JSON text).
            Also if an object the fields reach into is neither an object nor null, naming its
            wire key and quoting the value the same way. Its `path` is the path's template and
            its `status` is `None`.
    """
    fields = rest_path.fields
    unknown = set(required) - {f.column for f in fields}
    if unknown:
        raise ValueError(f"{rest_path.template} has no columns {sorted(unknown)}")
    keys = frozenset(f.wire_key for f in fields)
    objects = _prefixes(fields)
    columns: list[list[object]] = [[] for _ in fields]
    extra: set[str] = set()
    missing: set[str] = set()
    for index, row in enumerate(rows):
        found = _row(row, rest_path, keys=keys, objects=objects, extra=extra)
        for f, column in zip(fields, columns, strict=True):
            value = found.get(f.wire_key)
            if value is None and f.column in required:
                raise WireError(
                    f"{rest_path.template}: row {index}: {missing_key_message(f)}",
                    path=rest_path.template,
                )
            if value is None:
                if f.required:
                    missing.add(f.wire_key)
                column.append(None)
                continue
            try:
                column.append(convert_field(f, value))
            except UnconvertibleError as error:
                message = f"{rest_path.template}: row {index}: {error}"
                raise WireError(message, path=rest_path.template) from None
    types = [_ARROW[f.wire_type] for f in fields]
    schema = pa.schema([pa.field(f.column, t) for f, t in zip(fields, types, strict=True)])
    arrays = [pa.array(c, type=t) for c, t in zip(columns, types, strict=True)]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    return batch, DriftReport(extra=frozenset(extra), missing=frozenset(missing))
