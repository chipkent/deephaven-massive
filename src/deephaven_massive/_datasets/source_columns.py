"""The columns one source contributes to one dataset: the key rename, required keys, and widening.

A dataset reads several generated sources (REST paths, WebSocket channels, flat-file prefixes),
whose fields already carry their long-form PascalCase columns and wire types. Before their rows
join the dataset's table, each source's columns are renamed for the dataset, and this module
owns that step:

- `rename`, the run-time key rename of one source for one dataset: the source's fields, through
  the projection of the dataset's membership of its kind, with the dataset's declared key
  columns renamed to `Symbol`, `Timestamp` (for a bar, `TimestampStart`), `TimestampEnd`,
  `Underlying`, and `Date` (`KEY_COLUMNS`, `key_column`; a fan-out key that is no instrument,
  such as a filer CIK, keeps its column), and a key the source does not carry (one filled from
  the request) added as a constant `String` column. Nothing else changes. Each column is a
  `SourceColumn`, which keeps its epoch unit (`timestamps.time_unit`) for the transports.
- `required_fields`, which resolves the keys a source must carry back to the generated fields
  behind them, for the wire clients' required-key checks.
- `kind_position`, a source's place in its kind's group, which orders the merge.
- `widen`, the cast of one decoded column to its dataset column's Arrow type by the lattice
  joins (`WireType.common_type`), which every transport applies alike.

The merge of a kind's renamed sources into one schema is `schema.dataset_schema`. Everything
here reads the committed generated files only: no I/O, threads, or Deephaven calls, and no
state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.row_times import DATE, END, TIMESTAMP, time_column
from deephaven_massive._datasets.timestamps import TimeUnit, time_unit
from deephaven_massive._massive import WireType

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
    from deephaven_massive._massive import Field, Source

__all__ = [
    "KEY_COLUMNS",
    "SourceColumn",
    "key_column",
    "kind_position",
    "rename",
    "required_fields",
    "widen",
]


KEY_COLUMNS: Final[Mapping[str, str]] = {
    "symbol": "Symbol",
    "time": TIMESTAMP,
    "end": END,
    "fanout": "Underlying",
    "date": DATE,
}
"""The column each renamed key role takes. A bar's `time` key takes `TimestampStart` instead
(`key_column`), and a `fanout` key that is no instrument keeps its own column (`FilerCik`)."""


def key_column(spec: DatasetSpec, role: str, key: KeySpec) -> str:
    """Return the dataset column one declared key takes.

    Args:
        spec: The dataset, whose row times decide where its `time` key goes.
        role: The key's role: `symbol`, `time`, `end`, `fanout`, or `date`.
        key: The key's declaration.

    Returns:
        `Symbol`, `Timestamp` (for a bar or daily summary, `TimestampStart`), `TimestampEnd`,
        `Underlying`, or `Date` (`KEY_COLUMNS`); a `fanout` key that is no instrument keeps
        its own column, such as `FilerCik`.
    """
    if role == "time":
        return time_column(spec.times)
    if role in KEY_COLUMNS and key.instrument:
        return KEY_COLUMNS[role]
    return key.column


@dataclass(frozen=True, slots=True)
class SourceColumn:
    """One column a source contributes to a dataset, after the projection and the key rename.

    `rename` builds these; a transport reads them to rename a decoded batch's columns, pick the
    unit of each epoch, and fill a key from the request.

    Attributes:
        column: The dataset column, such as `"Timestamp"` or `"Price"`.
        wire_type: The type of the values the source sends; `STRING` for a key filled from the
            request.
        field: The generated field behind the column; None for a key filled from the request.
        unit: The unit of an epoch field (`timestamps.time_unit`); None for any other column.
        parameter: For a key filled from the request, the request parameter it comes from
            (`KeySpec.column`, such as `"stocksTicker"`); None otherwise.
    """

    column: str
    wire_type: WireType
    field: Field | None = None
    unit: TimeUnit | None = None
    parameter: str | None = None


def _projection(spec: DatasetSpec, source: Source) -> Mapping[str, str] | None:
    """Return the projection of the source's membership of the dataset's kind and group.

    Args:
        spec: The dataset.
        source: One of its sources.

    Returns:
        The kept wire keys and their columns, or None when the source joins the dataset with
        every field under its own column (no kind, no membership, or no projection).
    """
    for membership in source.memberships:
        if membership.kind == spec.kind and membership.group == spec.group:
            return membership.projection
    return None


def kind_position(spec: DatasetSpec, source: Source) -> int | None:
    """Return the source's position in the dataset's kind and group, if it holds one.

    Args:
        spec: The dataset.
        source: One of its sources.

    Returns:
        The membership's position, or None for a source in no membership of the dataset's
        kind and group (and for every source of a dataset in no kind).
    """
    for membership in source.memberships:
        if membership.kind == spec.kind and membership.group == spec.group:
            return membership.position
    return None


def rename(spec: DatasetSpec, source: Source) -> tuple[SourceColumn, ...]:
    """Return the columns one source contributes to one dataset.

    The source's fields, kept and named by the projection of the dataset's membership of its
    kind (`Source.memberships`), in the source's field order; then the dataset's key columns
    for this source (`DatasetSpec.keys`): the `symbol`, `time`, `end`, instrument `fanout`, and
    `date` keys are renamed to `Symbol`, `Timestamp` (for a bar, `TimestampStart`),
    `TimestampEnd`, `Underlying`, and `Date` (`key_column`), a `fanout` key that is no
    instrument keeps its column, and a key filled from the request comes first, as a constant
    `String` column. Nothing else changes.

    Args:
        spec: The dataset.
        source: One of the dataset's generated sources.

    Returns:
        The columns, keys filled from the request first, then the fields in source order.

    Raises:
        ValueError: If a declared key is not a column of the source after the projection,
            a time or end key is not an `epoch` or `iso-instant` field, a date key is not a
            `date` field, a symbol key from the rows is not a `string` field, or two keys name
            one column (`"declared keys: ..."`); or
            if two columns share a name after the rename (`"unique columns: ..."`). The
            message names the dataset and the source.
    """
    projection = _projection(spec, source)
    fields = [f for f in source.fields if projection is None or f.wire_key in projection]
    named = {f.wire_key: f.column if projection is None else projection[f.wire_key] for f in fields}
    by_column = {named[f.wire_key]: f for f in fields}
    where = f"{spec.name}: {source.transport.value} {source.key}"
    renames: dict[str, str] = {}
    roles: dict[str, str] = {}  # each source column a key claims, and the key's role
    columns: list[SourceColumn] = []
    for role, key in spec.keys_for(source.transport, source.key).declared():
        target = key_column(spec, role, key)
        if key.from_request:
            # DatasetSpec allows only an instrument symbol or fan-out key from the request.
            columns.append(SourceColumn(target, WireType.STRING, parameter=key.column))
        elif key.column not in by_column:
            raise ValueError(
                f"declared keys: {where}: the {role} key {key.column!r} is not a column of the "
                "source"
            )
        elif role in ("time", "end") and by_column[key.column].wire_type not in _INSTANTS:
            raise ValueError(
                f"declared keys: {where}: the {role} key {key.column!r} is "
                f"{by_column[key.column].wire_type}, not an instant (epoch or iso-instant)"
            )
        elif role == "date" and by_column[key.column].wire_type is not WireType.DATE:
            raise ValueError(
                f"declared keys: {where}: the date key {key.column!r} is "
                f"{by_column[key.column].wire_type}, not a date"
            )
        elif role == "symbol" and by_column[key.column].wire_type is not WireType.STRING:
            # A list of tickers describes no single instrument (a filing's `tickers`).
            raise ValueError(
                f"declared keys: {where}: the symbol key {key.column!r} is "
                f"{by_column[key.column].wire_type}, not a string"
            )
        elif key.column in roles:
            # One column cannot take two names: the second rename would replace the first.
            raise ValueError(
                f"declared keys: {where}: the {role} key {key.column!r} is already the "
                f"{roles[key.column]} key"
            )
        else:
            renames[key.column] = target
            roles[key.column] = role
    for field in fields:
        column = renames.get(named[field.wire_key], named[field.wire_key])
        unit = None if field.unit is None else time_unit(source, field)
        columns.append(SourceColumn(column, field.wire_type, field, unit))
    names = [c.column for c in columns]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(f"unique columns: {where}: columns {repeated} repeat after the rename")
    return tuple(columns)


def required_fields(spec: DatasetSpec, source: Source) -> tuple[Field, ...]:
    """Return the generated fields behind a source's required keys for one dataset.

    The required keys are the keys the dataset declares for the source, does not fill from
    the request, and does not mark optional (`KeySpec.required`); a row without one is a loud
    failure, never a null key. A key named through a
    membership projection (the index snapshot's `Timestamp`) resolves to the field behind it
    (`last_updated`, whose own column is `LastUpdated`), so a transport hands each wire client
    the names it decodes by: `Field.column` for `to_batch(..., required=)` and
    `WsClient.set_required`, `Field.wire_key` for a flat file's `ReadPlan.required`.

    Args:
        spec: The dataset.
        source: One of the dataset's generated sources.

    Returns:
        The fields, in the order `symbol`, `time`, `end`, `fanout`, `date`.

    A declared key the source lacks raises the `ValueError` of `rename`.
    """
    columns = {c.column: c.field for c in rename(spec, source) if c.field is not None}
    return tuple(
        columns[key_column(spec, role, key)]
        for role, key in spec.keys_for(source.transport, source.key).declared()
        if not key.from_request and key.required
    )


_INSTANTS: Final = frozenset({WireType.EPOCH, WireType.ISO_INSTANT})
"""The wire types a `time` or `end` key may have, so that their columns are always `Instant`s."""


def widen(values: pa.Array, target: pa.DataType) -> pa.Array:
    """Widen one source column to its dataset column's Arrow type, by the lattice joins.

    A source joins its kind with its own wire type, which the merged column may widen
    (`WireType.common_type`): a `long` in a `double` column is cast, and a `long` in an
    `int[]` column becomes a one-element list (a null stays null). A column already of its
    type is cast to itself, at no cost. Every transport converts its decoded columns with it,
    so they all widen alike.
    A `long` beyond 2**53 in a `double` column takes the nearest `double`, as the lattice join
    means it to, rather than failing the whole column.

    Args:
        values: The column as decoded, in its wire type's Arrow type.
        target: The dataset column's Arrow type (`ColumnType.arrow_type`).

    Returns:
        The column in `target`.
    """
    if pa.types.is_list(target) and not pa.types.is_list(values.type):
        offsets = pa.array(range(len(values) + 1), pa.int32())
        items = pc.cast(values, target.value_type)
        return pa.ListArray.from_arrays(offsets, items, mask=values.is_null())
    to_float = pa.types.is_integer(values.type) and pa.types.is_floating(target)
    return pc.cast(values, target, safe=not to_float)
