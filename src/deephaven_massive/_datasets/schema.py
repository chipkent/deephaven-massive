"""Dataset schemas: column types, the run-time key rename, and the merge of a kind's sources.

No column list is written by hand. Each source a dataset reads (a REST path, a WebSocket
channel, a flat-file prefix) is a generated `Source` of the wire package, whose fields already
carry their long-form PascalCase columns and wire types. This module turns those sources into
the dataset's table shape:

- `ColumnType`, the closed set of Deephaven column types (`Instant`, `LocalDate`, `double`,
  `long`, `int`, `String`, `boolean`, `int[]`), each with its Arrow type, and `COLUMN_TYPES`,
  the fixed map from a field's `WireType` to its column type.
- `Schema`, an ordered set of typed columns that turns row dictionaries into an Arrow table
  (used by the diagnostics tables, whose rows are built in Python).
- `rename`, the run-time key rename of one source for one dataset: the source's fields,
  through the projection of the dataset's membership of its kind, with the dataset's declared
  key columns renamed to `Symbol`, `Timestamp`, and `Underlying` (a fan-out key that is no
  instrument, such as a filer CIK, keeps its column), and a key the source does
  not carry (one filled from the request) added as a constant `String` column. Nothing else
  changes. `required_fields` resolves the keys a source must carry back to the generated
  fields behind them, for the wire clients' required-key checks.
- `dataset_schema`, the merge: the ordered union by column name of the renamed columns of
  every source of every member of the dataset's kind (`Catalog.members`), `Symbol` and
  `Timestamp` first where the dataset declares them, each column's wire type
  the lattice join (`join`) of every contribution. Every member of a kind therefore has the
  same columns: crypto trades carry the stocks-only `Tape`, always null. `Catalog` caches the
  result per dataset; this module holds no state.

The merge runs from the committed generated files only, never from data. The Deephaven types of
these columns, and the column definitions `table_publisher` and `input_table` take, come from
`_tables/columns.py`: this module makes no Deephaven call. Epoch timestamps keep their wire
unit here (`SourceColumn.unit`); the transports convert them with `timestamps.py`.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from deephaven_massive._datasets.timestamps import TimeUnit, time_unit
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
    from deephaven_massive._massive import Field, Source, Transport

__all__ = [
    "COLUMN_TYPES",
    "KEY_COLUMNS",
    "Column",
    "ColumnType",
    "Schema",
    "SourceColumn",
    "dataset_schema",
    "join",
    "key_column",
    "pascal_case",
    "rename",
    "required_fields",
]

logger = logging.getLogger(__name__)

_ISO_FRACTION: Final = re.compile(r"\.(\d+)")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


class ColumnType(StrEnum):
    """The type of a column, named by its Deephaven type.

    Each member knows its Arrow type (`arrow_type`); its Deephaven type is `dh_type` in
    `_tables/columns.py`. A dataset column's type follows from its wire type (`COLUMN_TYPES`):
    timestamps are `Instant`, calendar dates `LocalDate`, and condition codes `int[]`. `INT` is
    used only by the library's own diagnostics tables.
    """

    INSTANT = "Instant"
    LOCAL_DATE = "LocalDate"
    DOUBLE = "double"
    LONG = "long"
    INT = "int"
    STRING = "String"
    BOOLEAN = "boolean"
    INT_ARRAY = "int[]"

    @property
    def arrow_type(self) -> pa.DataType:
        """The Arrow type that `deephaven.arrow.to_table` converts to this column type."""
        return _ARROW_TYPES[self]


_ARROW_TYPES: Final[Mapping[ColumnType, pa.DataType]] = {
    ColumnType.INSTANT: pa.timestamp("ns", tz="UTC"),
    ColumnType.LOCAL_DATE: pa.date32(),
    ColumnType.DOUBLE: pa.float64(),
    ColumnType.LONG: pa.int64(),
    ColumnType.INT: pa.int32(),
    ColumnType.STRING: pa.string(),
    ColumnType.BOOLEAN: pa.bool_(),
    ColumnType.INT_ARRAY: pa.list_(pa.int32()),
}

COLUMN_TYPES: Final[Mapping[WireType, ColumnType]] = {
    WireType.LONG: ColumnType.LONG,
    WireType.DOUBLE: ColumnType.DOUBLE,
    WireType.BOOL: ColumnType.BOOLEAN,
    WireType.STRING: ColumnType.STRING,
    WireType.JSON_TEXT: ColumnType.STRING,
    WireType.DATE: ColumnType.LOCAL_DATE,
    WireType.EPOCH: ColumnType.INSTANT,
    WireType.ISO_INSTANT: ColumnType.INSTANT,
    WireType.INT_ARRAY: ColumnType.INT_ARRAY,
}
"""The column type of each wire type: `string` and `json-text` are both text, and an `epoch`
and an `iso-instant` are both an `Instant`."""

KEY_COLUMNS: Final[Mapping[str, str]] = {
    "symbol": "Symbol",
    "time": "Timestamp",
    "fanout": "Underlying",
}
"""The column each renamed key role takes. The `session` role keeps its generated column (the
futures `SessionEndDate`), and so does a `fanout` key that is no instrument (`FilerCik`)."""


def key_column(role: str, key: KeySpec) -> str:
    """Return the dataset column one declared key takes.

    Args:
        role: The key's role: `symbol`, `time`, `fanout`, or `session`.
        key: The key's declaration.

    Returns:
        `Symbol`, `Timestamp`, or `Underlying` for a key the rename moves (`KEY_COLUMNS`; a
        `fanout` key only when it is an instrument); otherwise the key's own column, such as
        `SessionEndDate` or `FilerCik`.
    """
    if role in KEY_COLUMNS and key.instrument:
        return KEY_COLUMNS[role]
    return key.column


@dataclass(frozen=True, slots=True)
class Column:
    """One typed column of a schema.

    Attributes:
        name: The PascalCase column name.
        type: The column's type.
    """

    name: str
    type: ColumnType


_INTEGER_TYPES: Final = frozenset({ColumnType.LONG, ColumnType.INT})


def _coerce(value: object, column_type: ColumnType) -> object:
    """Prepare one Python value for `pa.array` of the column's Arrow type.

    Five conversions Arrow does not do by itself: an ISO `YYYY-MM-DD` string becomes a `date`
    for a `LocalDate` column; an ISO-8601 date-time string (`2024-06-24T18:33:53Z`) becomes
    epoch nanoseconds for an `Instant` column, read as UTC when it carries no offset, keeping
    every fraction digit down to the nanosecond; a dict or list becomes its JSON text for a
    `String` column; a single integer becomes a one-element list for an `int[]` column; and an
    integral `float` becomes an `int` for a `long` or `int` column, and for each element of an
    `int[]` list, while any other `float` there is rejected, because Arrow would silently
    truncate `7140.08` to `7140`. Everything else passes through for Arrow to check; in
    particular a naive `datetime` object in an `Instant` column is read as UTC, as Arrow reads
    it.

    Args:
        value: A non-null value from a row.
        column_type: The type of the column it fills.

    Returns:
        The value in a form `pa.array` accepts for that type.

    Raises:
        ValueError: If a `float` with a fractional part, or a NaN or infinity, fills a `long`
            or `int` column or an element of an `int[]` list (`Schema.to_arrow` reports it as
            a schema mismatch).
    """
    if column_type is ColumnType.LOCAL_DATE and isinstance(value, str):
        return date.fromisoformat(value)
    if column_type is ColumnType.INSTANT and isinstance(value, str):
        return _iso_to_epoch_ns(value)
    if column_type is ColumnType.STRING and isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True)
    if column_type is ColumnType.INT_ARRAY and isinstance(value, int | list):
        items: list[object] = value if isinstance(value, list) else [value]
        return [None if v is None else _coerce(v, ColumnType.INT) for v in items]
    if column_type in _INTEGER_TYPES and isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{value!r} is not an integer")
        return int(value)
    return value


def _iso_to_epoch_ns(text: str) -> int:
    """Convert ISO-8601 date-time text to exact epoch nanoseconds.

    `datetime` resolves only microseconds, so the fraction is taken apart first: the text
    without it is parsed by `datetime.fromisoformat`, and up to nine fraction digits are added
    as nanoseconds (digits beyond the nanosecond are dropped). Text without an offset is UTC.

    Args:
        text: For example `2024-06-24T18:33:53.123456789Z`.

    Returns:
        Nanoseconds since the Unix epoch.
    """
    match = _ISO_FRACTION.search(text)
    fraction = match.group(1) if match else ""
    whole = text[: match.start()] + text[match.end() :] if match else text
    moment = datetime.fromisoformat(whole)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    delta = moment - _EPOCH
    seconds = delta.days * 86_400 + delta.seconds
    return seconds * 10**9 + int(fraction[:9].ljust(9, "0"))


@dataclass(frozen=True, slots=True)
class Schema:
    """A named, ordered set of typed columns: the shape of one dataset's tables.

    Instances are immutable and shared freely across threads.

    Example:
        ::

            from deephaven_massive._datasets.schema import Column, ColumnType, Schema

            schema = Schema(
                "example",
                (Column("Symbol", ColumnType.STRING), Column("Price", ColumnType.DOUBLE)),
            )
            schema.to_arrow([{"Symbol": "AAPL", "Price": 1.5}])  # one-row pyarrow.Table

    Attributes:
        name: A label used in error messages, usually the dataset name.
        columns: The columns, in table order.
    """

    name: str
    columns: tuple[Column, ...]

    def __post_init__(self) -> None:
        """Reject a schema whose column names repeat.

        Raises:
            ValueError: If two columns share a name.
        """
        names = [column.name for column in self.columns]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"schema {self.name} repeats columns {duplicates}")

    @property
    def column_names(self) -> tuple[str, ...]:
        """The column names, in table order."""
        return tuple(column.name for column in self.columns)

    def arrow_schema(self) -> pa.Schema:
        """Return the Arrow schema whose tables `deephaven.arrow.to_table` converts to this one.

        Returns:
            A `pyarrow.Schema` with one field per column, in table order.
        """
        return pa.schema([pa.field(c.name, c.type.arrow_type) for c in self.columns])

    def to_arrow(self, rows: Sequence[Mapping[str, object]]) -> pa.Table:
        """Build an Arrow table in this schema from row dictionaries keyed by column name.

        A column missing from a row, or `None`, is null. Keys that are not columns are ignored,
        so a caller may pass rows that carry extra fields. Instant values must already be epoch
        nanoseconds or `datetime`s (a naive one is read as UTC). A `float` with a fractional
        part in a `long` or `int` column is a schema mismatch, never truncated. This runs on the
        calling thread and makes no Deephaven call.

        Args:
            rows: The rows, each a mapping from column name to value.

        Returns:
            A `pyarrow.Table` with exactly this schema and `len(rows)` rows.

        Raises:
            MassiveError: If a value does not fit its column's type ("schema mismatch"), for
                example a string in a `double` column.
        """
        arrays = []
        for column in self.columns:
            values = [row.get(column.name) for row in rows]
            try:
                prepared = [None if v is None else _coerce(v, column.type) for v in values]
                arrays.append(pa.array(prepared, type=column.type.arrow_type))
            except (
                pa.ArrowInvalid,
                pa.ArrowTypeError,
                OverflowError,  # an int beyond the column's range, such as 2**63 in a long
                TypeError,
                ValueError,
            ) as e:
                raise MassiveError(
                    f"schema mismatch in {self.name}.{column.name} "
                    f"(expected {column.type.value}): {e}"
                ) from e
        return pa.Table.from_arrays(arrays, schema=self.arrow_schema())


def pascal_case(path: str) -> str:
    """Turn a snake-case name or a dotted path of them into a PascalCase column name.

    Every underscore- or dot-separated word is capitalized and joined: `sip_timestamp` becomes
    `SipTimestamp` and `last_trade.sip_timestamp` becomes `LastTradeSipTimestamp`. Words that
    are already capitalized keep their other letters. The diagnostics tables name their columns
    with it.

    Args:
        path: A name or a dot-separated path of names.

    Returns:
        The PascalCase column name.
    """
    words = re.split(r"[._]+", path)
    return "".join(word[:1].upper() + word[1:] for word in words if word)


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


def _position(spec: DatasetSpec, source: Source) -> int | None:
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
    for this source (`DatasetSpec.keys`): the `symbol`, `time`, and instrument `fanout` keys
    are renamed to `Symbol`, `Timestamp`, and `Underlying` (`key_column`), the `session` key
    and a `fanout` key that is no instrument keep their columns, and a key filled from the
    request comes first, as a constant `String` column. Nothing else changes.

    Args:
        spec: The dataset.
        source: One of the dataset's generated sources.

    Returns:
        The columns, keys filled from the request first, then the fields in source order.

    Raises:
        ValueError: If a declared key is not a column of the source after the projection,
            a time key is not an `epoch` or `iso-instant` field, a symbol key from the rows
            is not a `string` field, or two keys name one column (`"declared keys: ..."`); or
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
        target = key_column(role, key)
        if key.from_request:
            # DatasetSpec allows only an instrument symbol or fan-out key from the request.
            columns.append(SourceColumn(target, WireType.STRING, parameter=key.column))
        elif key.column not in by_column:
            raise ValueError(
                f"declared keys: {where}: the {role} key {key.column!r} is not a column of the "
                "source"
            )
        elif role == "time" and by_column[key.column].wire_type not in _INSTANTS:
            raise ValueError(
                f"declared keys: {where}: the time key {key.column!r} is "
                f"{by_column[key.column].wire_type}, not an instant (epoch or iso-instant)"
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

    The required keys are the keys the dataset declares for the source and does not fill from
    the request; a row without one is a loud failure, never a null key. A key named through a
    membership projection (the index snapshot's `Timestamp`) resolves to the field behind it
    (`last_updated`, whose own column is `LastUpdated`), so a transport hands each wire client
    the names it decodes by: `Field.column` for `to_batch(..., required=)` and
    `WsClient.set_required`, `Field.wire_key` for a flat file's `ReadPlan.required`.

    Args:
        spec: The dataset.
        source: One of the dataset's generated sources.

    Returns:
        The fields, in the order `symbol`, `time`, `fanout`, `session`.

    A declared key the source lacks raises the `ValueError` of `rename`.
    """
    columns = {c.column: c.field for c in rename(spec, source) if c.field is not None}
    return tuple(
        columns[key_column(role, key)]
        for role, key in spec.keys_for(source.transport, source.key).declared()
        if not key.from_request
    )


_LEADING: Final = ("Symbol", "Timestamp")
"""The key columns a merged schema starts with, in this order, where the dataset declares them;
every other column, `Underlying` included, takes its place by first appearance."""

_INSTANTS: Final = frozenset({WireType.EPOCH, WireType.ISO_INSTANT})
"""The wire types a `time` key may have, so that `Timestamp` is always an `Instant`."""


def _by_position(
    member: DatasetSpec, source: Callable[[Transport, str], Source]
) -> list[tuple[Transport, str, Source]]:
    """Return a member's sources in the order of their positions in its kind's group.

    The merge walks them in this order, so a schema's column order follows the generated
    memberships, never the order of the spec's role paths or mappings. Every source of a
    kind dataset holds a position (the kind-membership gate); a dataset in no kind has none,
    so its sources keep their declared order.

    Args:
        member: The dataset.
        source: Returns the generated source of a transport and key.

    Returns:
        `(transport, key, source)` for every source of the member.
    """
    found = [(transport, key, source(transport, key)) for transport, key in member.sources]

    def rank(item: tuple[int, tuple[Transport, str, Source]]) -> tuple[int, int]:
        index, (_, _, generated) = item
        return (_position(member, generated) or 0, index)

    return [entry for _, entry in sorted(enumerate(found), key=rank)]


_JOINS: Final[Mapping[frozenset[WireType], WireType]] = {
    frozenset({WireType.LONG, WireType.DOUBLE}): WireType.DOUBLE,
    frozenset({WireType.LONG, WireType.INT_ARRAY}): WireType.INT_ARRAY,
}
"""The widening joins of two different wire types; every other pair is a type conflict."""


def join(first: WireType, second: WireType) -> WireType | None:
    """Return the lattice join of two wire types of one column, or None for a conflict.

    Equal types join to themselves (two `epoch`s each keep their own unit, so they join too);
    `long` with `double` gives `double`, and `long` with `int-array` gives `int-array` (a
    scalar becomes a one-element list). Any other pair is a type conflict.

    Args:
        first: One contribution's type.
        second: Another contribution's type.

    Returns:
        The joined type, or None when the two conflict.
    """
    if first is second:
        return first
    return _JOINS.get(frozenset({first, second}))


def dataset_schema(
    spec: DatasetSpec,
    members: Sequence[DatasetSpec],
    source: Callable[[Transport, str], Source],
) -> Schema:
    """Merge the renamed columns of every member's sources into one dataset's schema.

    The ordered union by column name of `rename(member, s)` for every source `s` of every
    member, in member order, then the order of the sources' positions in the kind's group
    (declared order for a source without one), then field order; `Symbol` and then
    `Timestamp` come first where the dataset declares them, and every other column,
    `Underlying` included, takes its place by first appearance. Each column's type is the
    `join` of every contribution, mapped through `COLUMN_TYPES`. `Catalog.dataset_schema`
    calls it once per dataset and caches the result.

    Args:
        spec: The dataset whose schema is built; it names the schema and decides the leading
            key columns.
        members: The datasets whose sources merge, in member order: the datasets of the
            dataset's kind (`Catalog.members`), or the dataset alone when it is in no kind.
        source: Returns the generated source of a transport and key.

    Returns:
        The schema, named after the dataset.

    Raises:
        ValueError: If two contributions to one column conflict (`"member types: ..."`, naming
            the column, both types, and the member and source of the second). A failed rename
            raises the `ValueError` of `rename`, and an unknown source whatever `source` raises.
    """
    types: dict[str, WireType] = {}
    for member in members:
        for transport, key, found in _by_position(member, source):
            for contribution in rename(member, found):
                prior = types.get(contribution.column)
                joined = (
                    contribution.wire_type if prior is None else join(prior, contribution.wire_type)
                )
                if joined is None:
                    raise ValueError(
                        f"member types: {spec.name}: column {contribution.column!r} is "
                        f"{prior} elsewhere but {contribution.wire_type} in {member.name} "
                        f"{transport.value} {key}"
                    )
                types[contribution.column] = joined
    declared = {key_column(r, k) for keys in spec.keys.values() for r, k in keys.declared()}
    leading = [name for name in _LEADING if name in declared]
    order = [*leading, *(name for name in types if name not in leading)]
    return Schema(spec.name, tuple(Column(name, COLUMN_TYPES[types[name]]) for name in order))
