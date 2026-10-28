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
  same columns: crypto trades carry the stocks-only `Tape`, always null. Each column also gets
  one description chosen for the dataset, the transports that fill it, and the wire keys behind
  it (`Column`); every result's column descriptions and the queries' `columns()` tables read
  them from here. `Catalog` caches the result per dataset; this module holds no state.

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
from dataclasses import field as dataclass_field
from datetime import UTC, date, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.timestamps import TimeUnit, time_unit
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
    from deephaven_massive._massive import Field, Source

__all__ = [
    "COLUMN_TYPES",
    "KEY_COLUMNS",
    "TRANSPORT_LABELS",
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
    "widen",
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
    """One typed column of a schema, with what it means and where it comes from.

    A dataset schema (`dataset_schema`) fills every attribute; the library's own diagnostics
    tables give only a name and a type. The description and the transports are what every
    result's column descriptions and the queries' `columns()` tables show, so both read them
    from here and never disagree; the wire keys name the vendor fields behind the column.
    Immutable and shared freely across threads.

    Attributes:
        name: The PascalCase column name.
        type: The column's type.
        description: What the column holds, chosen per dataset from its fields' descriptions
            (the vendor's, or the refresh's correction where the vendor's is wrong) or written
            by the library for the key columns and for a column only other datasets of the
            kind supply (see `dataset_schema`); empty when nothing describes it.
        supplied_by: The transports whose sources of this dataset fill the column (a key
            filled from the request counts), in the order `REST`, `FLAT_FILES`, `WS`; empty
            for a column only other datasets of the kind supply, which this dataset leaves
            null.
        wire_keys: For each transport of `supplied_by`, the distinct wire keys behind the
            column, in the order of the dataset's sources, before any projection renames
            them; None stands for a key filled from the request. Excluded from hashing.
    """

    name: str
    type: ColumnType
    description: str = ""
    supplied_by: tuple[Transport, ...] = ()
    wire_keys: Mapping[Transport, tuple[str | None, ...]] = dataclass_field(
        default_factory=lambda: MappingProxyType({}), hash=False
    )


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
        The fields, in the order `symbol`, `time`, `fanout`, `session`.

    A declared key the source lacks raises the `ValueError` of `rename`.
    """
    columns = {c.column: c.field for c in rename(spec, source) if c.field is not None}
    return tuple(
        columns[key_column(role, key)]
        for role, key in spec.keys_for(source.transport, source.key).declared()
        if not key.from_request and key.required
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


_TRANSPORT_ORDER: Final = (Transport.REST, Transport.FLAT_FILES, Transport.WS)
"""The order in which a column's transports are listed and its descriptions are tried: REST
first, because its descriptions are the most complete."""

TRANSPORT_LABELS: Final[Mapping[Transport, str]] = MappingProxyType(
    {Transport.REST: "REST", Transport.FLAT_FILES: "flat files", Transport.WS: "WebSocket"}
)
"""How every text a user reads spells each transport: the key-column descriptions, the
`SuppliedBy` column of `columns()`, and the column reference all use it, so a filter copied
from one works on another. The `Transport` values are internal names and appear only in the
diagnostics tables. Read-only."""

_KEY_SENTENCES: Final[Mapping[str, str]] = {
    "Symbol": "The instrument, in the library's canonical symbol form.",
    "Timestamp": "The row's primary time, as an Instant.",
    "Underlying": "The underlying instrument, in the library's canonical symbol form.",
}
"""The library's own sentence for each renamed key column; its description goes on to name
the wire key or request parameter behind it on each transport."""

_Contribution = tuple[Transport, SourceColumn]
"""One column one source of a dataset contributes, with the source's transport."""


def _own_contributions(
    spec: DatasetSpec, source: Callable[[Transport, str], Source]
) -> dict[str, list[_Contribution]]:
    """Return what the dataset's own sources contribute to each column, in description order.

    Args:
        spec: The dataset.
        source: Returns the generated source of a transport and key.

    Returns:
        For each column the dataset's own sources fill, every contribution: the transports in
        the order `REST`, `FLAT_FILES`, `WS` and, within one transport, the sources in their
        declared order (`DatasetSpec.sources`: role paths, then channels, then prefixes).
    """
    own: dict[str, list[_Contribution]] = {}
    for transport in _TRANSPORT_ORDER:
        for declared, key in spec.sources:
            if declared is transport:
                for column in rename(spec, source(transport, key)):
                    own.setdefault(column.column, []).append((transport, column))
    return own


def _key_part(transport: Transport, column: SourceColumn, asset_class: str | None) -> str:
    """Say where one source's value of a renamed key column comes from.

    Args:
        transport: The source's transport.
        column: The source's contribution to the key column.
        asset_class: The dataset's asset class (an `AssetClass` value), or `None`; it picks
            the field's description (`Field.description_for`).

    Returns:
        ``REST `sip_timestamp`: "<field description>"`` for a wire key with a description,
        ``REST `sip_timestamp`.`` for one without, or ``REST: the request's `stocksTicker`
        parameter.`` for a key filled from the request, with the transport spelled as
        `TRANSPORT_LABELS` spells it (``WebSocket `t`: …``).
    """
    label = TRANSPORT_LABELS[transport]
    if column.field is None:
        return f"{label}: the request's `{column.parameter}` parameter."
    described = column.field.description_for(asset_class)
    if described:
        return f'{label} `{column.field.wire_key}`: "{described}"'
    return f"{label} `{column.field.wire_key}`."


def _description(
    spec: DatasetSpec,
    name: str,
    *,
    own: Sequence[_Contribution],
    suppliers: Sequence[str],
    is_key: bool,
) -> str:
    """Pick one column's description for one dataset (the rules of `dataset_schema`).

    Args:
        spec: The dataset.
        name: The column.
        own: The dataset's own contributions to the column, in description order; empty when
            only other members of the kind supply it.
        suppliers: The members of the kind that supply the column, by name, in member order.
        is_key: True when the column is one of the dataset's declared key columns. Only then
            does a `Symbol`, `Timestamp`, or `Underlying` column get the library's sentence;
            a vendor field that merely carries one of those names is described as any other.

    Returns:
        The description; empty when no source describes the column.
    """
    asset_class = spec.asset_class.value if spec.asset_class is not None else None
    if not own:
        named = ", ".join(f"`{member}`" for member in suppliers)
        return f"Always null for `{spec.name}`; supplied by {named}."
    if is_key and name in _KEY_SENTENCES:
        parts = [_KEY_SENTENCES[name]]
        for transport, column in own:
            part = _key_part(transport, column, asset_class)
            if part not in parts:
                parts.append(part)
        return " ".join(parts)
    for _, column in own:
        described = column.field.description_for(asset_class) if column.field else ""
        if described:
            return described
    return ""


def _column(
    spec: DatasetSpec,
    name: str,
    *,
    column_type: ColumnType,
    own: Sequence[_Contribution],
    suppliers: Sequence[str],
    is_key: bool,
) -> Column:
    """Build one column of a dataset schema with its description, transports, and wire keys.

    Args:
        spec: The dataset.
        name: The column.
        column_type: Its merged type.
        own: The dataset's own contributions to the column, in description order.
        suppliers: The members of the kind that supply the column, by name, in member order.
        is_key: True when the column is one of the dataset's declared key columns.

    Returns:
        The column.
    """
    wire_keys: dict[Transport, list[str | None]] = {}
    for transport, column in own:
        keys = wire_keys.setdefault(transport, [])
        key = None if column.field is None else column.field.wire_key
        if key not in keys:
            keys.append(key)
    return Column(
        name,
        column_type,
        _description(spec, name, own=own, suppliers=suppliers, is_key=is_key),
        tuple(wire_keys),
        MappingProxyType({transport: tuple(keys) for transport, keys in wire_keys.items()}),
    )


def widen(values: pa.Array, target: pa.DataType) -> pa.Array:
    """Widen one source column to its dataset column's Arrow type, by the lattice joins.

    A source joins its kind with its own wire type, which the merged column may widen
    (`join`): a `long` in a `double` column is cast, and a `long` in an `int[]` column becomes
    a one-element list (a null stays null). A column already of its type is cast to itself, at
    no cost. Every transport converts its decoded columns with it, so they all widen alike.
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

    Each column also gets its transports and wire keys from the dataset's own sources
    (`Column.supplied_by`, `Column.wire_keys`), and one description chosen for this dataset,
    never one per kind: a kind spans asset classes, and the stock text for a price ("dollar
    value per whole share") is wrong for crypto. A field's description is the one for the
    dataset's asset class (`Field.description_for`): the per-class description a source that
    serves several classes gives, else its own, which is the vendor's or the refresh's
    correction of it. Three rules choose it:

    1. A column the dataset's own sources supply takes the first non-empty field description
       among them, trying the transports in the order REST, flat files, WebSocket and, within
       one transport, the sources in their declared order (role paths, then channels, then
       prefixes). REST comes first because its text is the most complete.
    2. A column that exists only through the kind's union, which no source of the dataset
       supplies, says so and names the members that do, in member order:
       ``Always null for `crypto.trades`; supplied by `stocks.trades`.``
    3. `Symbol`, `Timestamp`, and `Underlying`, when they are declared key columns of the
       dataset, get the library's own sentence ("The row's primary time, as an Instant."),
       followed, per source in the order of rule 1, by the wire key the source renames and
       its quoted field description
       (``REST `sip_timestamp`: "The nanosecond accuracy SIP Unix Timestamp..."``), or, for a
       key filled from the request, the request parameter it came from (``REST: the
       request's `stocksTicker` parameter.``); a part two sources share appears once.

    A column whose sources carry no description gets `""`, and nothing fails.

    Args:
        spec: The dataset whose schema is built; it names the schema, decides the leading
            key columns, and is the dataset the descriptions are chosen for.
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
    suppliers: dict[str, list[str]] = {}
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
                named = suppliers.setdefault(contribution.column, [])
                if member.name not in named:
                    named.append(member.name)
    declared = {key_column(r, k) for keys in spec.keys.values() for r, k in keys.declared()}
    leading = [name for name in _LEADING if name in declared]
    order = [*leading, *(name for name in types if name not in leading)]
    own = _own_contributions(spec, source)
    return Schema(
        spec.name,
        tuple(
            _column(
                spec,
                name,
                column_type=COLUMN_TYPES[types[name]],
                own=own.get(name, ()),
                suppliers=suppliers[name],
                is_key=name in declared,
            )
            for name in order
        ),
    )
