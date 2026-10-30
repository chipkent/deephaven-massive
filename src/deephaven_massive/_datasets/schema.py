"""Dataset schemas: column types, the run-time key rename, and the merge of a kind's sources.

No column list is written by hand. Each source a dataset reads (a REST path, a WebSocket
channel, a flat-file prefix) is a generated `Source` of the wire package, whose fields already
carry their long-form PascalCase columns and wire types. This module turns those sources into
the dataset's table shape:

- `ColumnType`, the closed set of Deephaven column types (`Instant`, `double`, `long`, `int`,
  `String`, `boolean`, `long[]`, `String[]`), each with its Arrow type, and `COLUMN_TYPES`, the
  fixed map from a field's `WireType` to its column type. Every calendar date is ISO `yyyy-MM-dd`
  text, a `String`, so a date joins and compares as text; `Column.calendar_date` marks those
  columns, and the Deephaven side stores one shared `String` per distinct date (`_tables/columns.py`
  `dataset_table`), so a date costs a reference per row. A date is a stored column, never a
  `LocalDate` and never a view computed on read; instants stay `Instant`. The library's time columns
  (`row_times.py`) own their names: no vendor field keeps `Timestamp` or any `Timestamp<Qualifier>`
  outside a declared key, and on a tick, bar, or daily-summary dataset none keeps `Date` either (a
  vendor `date` stays `Date` only on a dataset without the library's `Date`, such as the economy
  data); no refresh `NAMES` entry may coin one.
- `Schema`, an ordered set of typed columns that turns row dictionaries into an Arrow table
  (used by the diagnostics tables, whose rows are built in Python).
- `rename`, the run-time key rename of one source for one dataset: the source's fields, through the
  projection of the dataset's membership of its kind, with the dataset's declared key columns
  renamed to `Symbol`, `Timestamp` (for a bar, `TimestampStart`), `TimestampEnd`, `Underlying`, and
  `Date` (a fan-out key that is no instrument, such as a filer CIK, keeps its column), and a key the
  source does not carry (one filled from the request) added as a constant `String` column. Nothing
  else changes. `required_fields` resolves the keys a source must carry back to the generated fields
  behind them, for the wire clients' required-key checks.
- `dataset_schema`, the merge: the columns of the dataset's kind (`Catalog.members`) that some
  source of the dataset itself fills, in the kind's order, with the library's own time columns
  (`row_times`): `Symbol`, `Date`, and `Timestamp` or `TimestampStart` and `TimestampEnd`
  first, `TimestampInsert` last, each column's wire type the common type
  (`WireType.common_type`) of every contribution across the kind. A column only another member
  of the kind fills is left out, so crypto trades have no stocks-only `Tape`, while the columns
  two members share keep one name, type, and place, so their tables merge on them. Each column
  also gets
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
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.dates import DAY_ZONE_NAMES, DAY_ZONES, NEW_YORK
from deephaven_massive._datasets.row_times import (
    DATE,
    END,
    INSERT,
    START,
    TIMESTAMP,
    RowTimes,
    time_column,
)
from deephaven_massive._datasets.timestamps import TimeUnit, time_unit
from deephaven_massive._enums import AssetClass, ClassFromPrefix
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from datetime import tzinfo

    from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
    from deephaven_massive._massive import Field, Source

__all__ = [
    "COLUMN_TYPES",
    "JSON_TEXT_NOTE",
    "KEY_COLUMNS",
    "OPTION_ROOT",
    "RAW_SYMBOL",
    "TRANSPORT_LABELS",
    "Column",
    "ColumnType",
    "Schema",
    "SourceColumn",
    "dataset_schema",
    "filled_columns",
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
    timestamps are `Instant`, calendar dates ISO `yyyy-MM-dd` text (`String`), and condition
    codes `long[]`, lists of text `String[]`. `INT` is used only by the library's own diagnostics
    tables.
    """

    INSTANT = "Instant"
    DOUBLE = "double"
    LONG = "long"
    INT = "int"
    STRING = "String"
    BOOLEAN = "boolean"
    LONG_ARRAY = "long[]"
    STRING_ARRAY = "String[]"

    @property
    def arrow_type(self) -> pa.DataType:
        """The Arrow type that `deephaven.arrow.to_table` converts to this column type."""
        return _ARROW_TYPES[self]


_ARROW_TYPES: Final[Mapping[ColumnType, pa.DataType]] = {
    ColumnType.INSTANT: pa.timestamp("ns", tz="UTC"),
    ColumnType.DOUBLE: pa.float64(),
    ColumnType.LONG: pa.int64(),
    ColumnType.INT: pa.int32(),
    ColumnType.STRING: pa.string(),
    ColumnType.BOOLEAN: pa.bool_(),
    ColumnType.LONG_ARRAY: pa.list_(pa.int64()),
    ColumnType.STRING_ARRAY: pa.list_(pa.string()),
}

COLUMN_TYPES: Final[Mapping[WireType, ColumnType]] = {
    WireType.LONG: ColumnType.LONG,
    WireType.DOUBLE: ColumnType.DOUBLE,
    WireType.BOOL: ColumnType.BOOLEAN,
    WireType.STRING: ColumnType.STRING,
    WireType.JSON_TEXT: ColumnType.STRING,
    WireType.DATE: ColumnType.STRING,
    WireType.EPOCH: ColumnType.INSTANT,
    WireType.ISO_INSTANT: ColumnType.INSTANT,
    WireType.LONG_ARRAY: ColumnType.LONG_ARRAY,
    WireType.STRING_ARRAY: ColumnType.STRING_ARRAY,
}
"""The column type of each wire type: `string` and `json-text` are both text, a `date` is ISO
`yyyy-MM-dd` text too (every calendar date the library produces is), and an `epoch` and an
`iso-instant` are both an `Instant`."""

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
            by the library for the key columns and the columns it computes (see
            `dataset_schema`); empty when nothing describes it.
        supplied_by: The transports whose sources of this dataset fill the column (a key
            filled from the request counts), in the order `REST`, `FLAT_FILES`, `WS`; never
            empty for a dataset column, since a dataset keeps only the columns it can fill.
        wire_keys: For each transport of `supplied_by`, the distinct wire keys behind the
            column, in the order of the dataset's sources, before any projection renames
            them; None stands for a key filled from the request. Excluded from hashing.
        calendar_date: True for a calendar date, ISO `yyyy-MM-dd` text (`Date`, a vendor
            `date` field): the Deephaven side stores one shared string per distinct date in
            such a column (`_tables/columns.py`, `dataset_table`).
    """

    name: str
    type: ColumnType
    description: str = ""
    supplied_by: tuple[Transport, ...] = ()
    wire_keys: Mapping[Transport, tuple[str | None, ...]] = dataclass_field(
        default_factory=lambda: MappingProxyType({}), hash=False
    )
    calendar_date: bool = False


_INTEGER_TYPES: Final = frozenset({ColumnType.LONG, ColumnType.INT})


def _coerce(value: object, column_type: ColumnType) -> object:
    """Prepare one Python value for `pa.array` of the column's Arrow type.

    Four conversions Arrow does not do by itself: an ISO-8601 date-time string
    (`2024-06-24T18:33:53Z`) becomes epoch nanoseconds for an `Instant` column, read as UTC when it
    carries no offset, keeping every fraction digit down to the nanosecond; a dict or list becomes
    its JSON text for a `String` column; a single integer becomes a one-element list for a `long[]`
    column; and an integral `float` becomes an `int` for a `long` or `int` column, and for each
    element of a `long[]` list, while any other `float` there is rejected, because Arrow would
    silently truncate `7140.08` to `7140`. Everything else passes through for Arrow to check; in
    particular a naive `datetime` object in an `Instant` column is read as UTC, as Arrow reads it.

    Args:
        value: A non-null value from a row.
        column_type: The type of the column it fills.

    Returns:
        The value in a form `pa.array` accepts for that type.

    Raises:
        ValueError: If a `float` with a fractional part, or a NaN or infinity, fills a `long`
            or `int` column or an element of a `long[]` list (`Schema.to_arrow` reports it as
            a schema mismatch).
    """
    if column_type is ColumnType.INSTANT and isinstance(value, str):
        return _iso_to_epoch_ns(value)
    if column_type is ColumnType.STRING and isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True)
    if column_type is ColumnType.LONG_ARRAY and isinstance(value, int | list):
        items: list[object] = value if isinstance(value, list) else [value]
        return [None if v is None else _coerce(v, ColumnType.LONG) for v in items]
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

    @property
    def date_columns(self) -> tuple[str, ...]:
        """The calendar-date columns (`Column.calendar_date`), in table order."""
        return tuple(column.name for column in self.columns if column.calendar_date)

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


_LEADING: Final = ("Symbol", DATE, TIMESTAMP, START, END)
"""The columns a merged schema starts with, in this order, where the dataset declares them as
keys or the library fills them; every other column, `Underlying` included, takes its place by
first appearance, and `TimestampInsert` comes last."""

_INSTANTS: Final = frozenset({WireType.EPOCH, WireType.ISO_INSTANT})
"""The wire types a `time` or `end` key may have, so that their columns are always `Instant`s."""

_LIBRARY_TYPES: Final[Mapping[str, WireType]] = {
    DATE: WireType.DATE,
    START: WireType.EPOCH,
    END: WireType.EPOCH,
    INSERT: WireType.EPOCH,
}
"""The wire type each library time column joins the merge with: `Date` is a date (ISO text), the
others instants."""


_Read: TypeAlias = "tuple[DatasetSpec, Transport, str, Source]"
"""One source the merge walks: the member that reads it, its transport and key, and itself."""


def _by_position(
    members: Sequence[DatasetSpec], source: Callable[[Transport, str], Source]
) -> list[_Read]:
    """Return every member's sources, each group's in the order of their positions in it.

    The merge walks them in this order, so a schema's column order follows the generated
    memberships, never the order of the spec's role paths or mappings, nor how a group is
    split over datasets: the trades of stocks give their columns in the same order whether
    `stocks.trades` reads the last-trade path or `stocks.last_trade` does. The
    members come grouped (`Catalog.members` orders them by group), and within a group the
    sources of all its members are ordered together. Every source of a kind dataset holds a
    position (the kind-membership gate); a dataset in no kind has none, so its sources keep
    their declared order.

    Args:
        members: The datasets whose sources merge, grouped, in member order.
        source: Returns the generated source of a transport and key.

    Returns:
        `(member, transport, key, source)` for every source of every member.
    """
    found = [
        (member, transport, key, source(transport, key))
        for member in members
        for transport, key in member.sources
    ]
    groups = {member.group: rank for rank, member in reversed(list(enumerate(members)))}

    def rank(item: tuple[int, _Read]) -> tuple[int, int, int]:
        index, (member, _, _, generated) = item
        return (groups[member.group], _position(member, generated) or 0, index)

    return [entry for _, entry in sorted(enumerate(found), key=rank)]


_TRANSPORT_ORDER: Final = (Transport.REST, Transport.FLAT_FILES, Transport.WS)
"""The order in which a column's transports are listed and its descriptions are tried: REST
first, because its descriptions are the most complete."""

TRANSPORT_LABELS: Final[Mapping[Transport, str]] = MappingProxyType(
    {Transport.REST: "REST", Transport.FLAT_FILES: "flat files", Transport.WS: "WebSocket"}
)
"""How every text a user reads spells each transport: the key-column descriptions, the
`SuppliedBy` column of `columns()`, the column reference, and the `Transport` column of the
diagnostics tables all use it, so a filter copied from one works on another. The `Transport`
values stay internal names, which only logs and code show. Read-only."""

_KEY_SENTENCES: Final[Mapping[str, str]] = {
    "Symbol": "The instrument, in the library's canonical symbol form.",
    TIMESTAMP: "The row's primary time, as an Instant.",
    "Underlying": (
        "The instrument Massive links the row to, exactly as Massive sends it; for an option, "
        "the linked instrument, not what the contract delivers."
    ),
}
"""The library's own sentence for each renamed key column of every dataset; its description
goes on to name the wire key or request parameter behind it on each transport."""

OPTION_ROOT: Final = "OptionRoot"
"""The derived column of every options dataset with a `Symbol`: the OCC root of the symbol."""

RAW_SYMBOL: Final = "RawSymbol"
"""The derived column of a dataset that reads its symbol from rows, in a session whose
`unreadable_symbols` is `LOG` (the default) or `IGNORE`: the text of a symbol the library could
not read."""

_DERIVED_SENTENCES: Final[Mapping[str, str]] = {
    OPTION_ROOT: (
        "The OCC option root, read from `Symbol`: the symbol without its `O:` prefix and its "
        "last 15 characters (expiry, type, and strike), such as `SPXW` for "
        "`O:SPXW251219C06000000`. It is never mapped to the underlying: an adjusted contract "
        "keeps its own root, such as `AAPL1`."
    ),
    RAW_SYMBOL: (
        "The symbol exactly as Massive sent it, on a row whose spelling the library cannot "
        "read as the dataset's class (its `Symbol` is then null); null on every other row. "
        "Present unless the session's `unreadable_symbols` is `dm.OnError.RAISE`, which fails "
        "on such a row instead."
    ),
}
"""The library's description of each derived column."""

_OTHER_ROWS: Final = "Null on a row of any other class."
"""The sentence that ends `OptionRoot`'s description on a dataset of options and other rows."""

JSON_TEXT_NOTE: Final = "The column holds Massive's value as JSON text."
"""The sentence that ends the description of every column of nested values kept as JSON text:
an object, a list of objects, or a value of several shapes (`WireType.JSON_TEXT`)."""

_QUOTED_ENDS: Final = ('."', '?"', '!"')
"""The endings of a quoted description that already closes a sentence, so the key column's
"From …" sentence takes no period after the closing quote."""

_ZONE_WORDS: Final[Mapping[tzinfo, str]] = {NEW_YORK: "New York", UTC: "UTC"}
"""How a day zone of `dates.DAY_ZONES` names a date in a description: "the New York date"."""

_ZONE_CLOCKS: Final[Mapping[str, str]] = {
    _ZONE_WORDS[zone]: name for zone, name in DAY_ZONE_NAMES.items()
}
"""How a day zone names a time of day in a description, by its date word: "00:00 New York
time" (`dates.DAY_ZONE_NAMES`)."""

_WINDOW_END: Final = (
    "The exclusive end of the bar's window, as an Instant: the first instant at which the bar "
    'is complete, so an as-of join on it, `aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`, '
    "never attaches a bar that is still open. It is when the window closes, not when Massive "
    "published the bar, and Massive may correct a bar later."
)
"""The first sentences of every bars dataset's `TimestampEnd`."""

_INDEX_HOUR: Final = (
    "For indices that end is an hour later, 01:00 New York time, since whether an index day bar "
    "covers the New York or the Chicago day is not verified."
)
"""Why an index bar of a day or longer ends at 01:00 New York time."""

_NULL_DATE: Final = (
    "Joining an intraday row to the same day's daily bar on `Date` attaches values not known "
    "until that day ended, and a null `Date` matches another null, so join longer bars on "
    "`TimestampEnd`."
)
"""The join caveat every bars dataset's `Date` ends with."""

_INSERT_SENTENCE: Final = (
    "When the library received the row, as an Instant: the local clock when the REST page, "
    "flat-file block, or WebSocket frame that carried it arrived, the same for every row of one. "
    "It is not monotonic and is not Massive's own receive time; on history() rows it is the "
    "download time, and in a poll() table the latest poll's."
)
"""The description of every dataset's `TimestampInsert`."""

_FROM_PARTS: Final = frozenset({"Symbol", "Underlying", TIMESTAMP, START, DATE})
"""The library's columns whose description goes on to quote the wire key behind each source;
`TimestampEnd` does not, since the library recomputes it rather than keep Massive's."""


def _zone_word(spec: DatasetSpec) -> str | None:
    """Return how a dataset's day zone is named in its descriptions.

    Args:
        spec: The dataset.

    Returns:
        "New York" or "UTC" for a class with a day zone; None for futures, whose days are
        sessions, and for a dataset without a class, whose zone follows its symbols.
    """
    if not isinstance(spec.asset_class, AssetClass) or spec.asset_class not in DAY_ZONES:
        return None
    return _ZONE_WORDS[DAY_ZONES[spec.asset_class]]


def _date_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a tick or bar dataset's `Date`.

    Args:
        spec: A dataset whose rows carry `Date` (`RowTimes` other than `NONE`).

    Returns:
        The description.
    """
    zone = _zone_word(spec)
    times = spec.times
    if times is RowTimes.DAY:
        return f"The day the summary covers, as ISO yyyy-MM-dd text: a {zone} calendar day."
    if spec.asset_class is AssetClass.FUTURES:
        if times is RowTimes.TICK:
            return (
                "The futures session the row belongs to (its trading date), as ISO yyyy-MM-dd "
                "text: Massive's `session_end_date` from REST and the flat files. The WebSocket "
                "sends none, so a streamed row gets the session that ends at the next 17:00 "
                "Chicago time (a Saturday's or Sunday's moves to Monday), corrected to the "
                "session Massive's futures schedules place the row in; a row the schedules do "
                "not cover keeps that date, and the stream's m.streams() row shows the error."
            )
        return (
            "The futures session of the bar (its trading date), as ISO yyyy-MM-dd text: "
            "Massive's `session_end_date` from REST and the flat files, and on the WebSocket the "
            "session as for futures trades; null for a bar of several sessions, a week or "
            f"longer, or an intraday bar that crosses 17:00 Chicago time. {_NULL_DATE}"
        )
    if times is RowTimes.TICK:
        return (
            f"The trading day of the row, as ISO yyyy-MM-dd text: the {zone} date of `Timestamp`."
        )
    where = (
        f"the {zone} date of `TimestampStart`"
        if zone is not None
        else "the date of `TimestampStart` in the day zone of the symbol's class (UTC for forex "
        "and crypto, New York otherwise)"
    )
    return (
        f"The trading day of the bar, as ISO yyyy-MM-dd text: {where} for an intraday or 1-day "
        "bar, and null for a bar that spans more than one date (several days, a week or longer, "
        f"or an intraday bar that crosses midnight). {_NULL_DATE}"
    )


def _start_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a bars dataset's `TimestampStart`.

    Args:
        spec: A dataset of bars (`RowTimes.BAR`) or daily summaries (`RowTimes.DAY`).

    Returns:
        The description, before the wire keys it quotes.
    """
    zone = _zone_word(spec)
    if spec.times is RowTimes.DAY:
        return (
            "The start of the day the summary covers, as an Instant: 00:00 "
            f"{_ZONE_CLOCKS[str(zone)]} of `Date`."
        )
    if spec.asset_class is AssetClass.FUTURES:
        return "The start of the bar's window, as an Instant: Massive's own window start."
    if zone is None:
        return (
            "The start of the bar the value is computed at, as an Instant. A bar of a day or "
            "longer starts at 00:00 of its window's first day in the day zone of the symbol's "
            "class (UTC for forex and crypto, New York otherwise)."
        )
    first = (
        "a week's Monday"
        if spec.asset_class in (AssetClass.FOREX, AssetClass.CRYPTO)
        else "a week's Monday, a month's, quarter's, or year's first weekday"
    )
    return (
        "The start of the bar's window, as an Instant. A bar of a day or longer starts at "
        f"00:00 {_ZONE_CLOCKS[zone]} of its window's first day ({first}), whichever source "
        "serves it; each "
        "source's own stamp, quoted next, is moved there."
    )


def _end_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a bars dataset's `TimestampEnd`.

    Args:
        spec: A dataset of bars (`RowTimes.BAR`) or daily summaries (`RowTimes.DAY`).

    Returns:
        The description.
    """
    zone = _zone_word(spec)
    indices = spec.asset_class is AssetClass.INDICES
    if spec.times is RowTimes.DAY:
        later = f" {_INDEX_HOUR}" if indices else ""
        return (
            "The exclusive end of the day the summary covers, as an Instant: 00:00 "
            f"{_ZONE_CLOCKS[str(zone)]} of the next day.{later}"
        )
    if spec.asset_class is AssetClass.FUTURES:
        return (
            f"{_WINDOW_END} An intraday bar ends its length after its start (a one-second "
            "WebSocket bar at the later of that and Massive's `e`); a session bar at 17:00 "
            "Chicago time on its session date; a bar of several sessions, or of a week or "
            "longer, at the latest end its label allows, since whether Massive labels it with "
            "its first or its last session is not verified."
        )
    where = (
        f"00:00 {_ZONE_CLOCKS[zone]}"
        if zone is not None
        else "00:00, in the day zone of the symbol's class,"
    )
    later = f" {_INDEX_HOUR}" if indices else ""
    return (
        f"{_WINDOW_END} An intraday bar ends its length after its start; a bar of a day or longer "
        f"at {where} after its window's last day.{later}"
    )


def _library_sentence(spec: DatasetSpec, name: str) -> str:
    """Return the library's own sentence for one of a dataset's library columns.

    Args:
        spec: The dataset.
        name: A key column (`Symbol`, `Timestamp`, `Underlying`) or a time column the
            library fills (`Date`, `TimestampStart`, `TimestampEnd`, `TimestampInsert`).

    Returns:
        The sentence, before any wire keys it quotes.
    """
    if name == DATE:
        return _date_sentence(spec)
    if name == START:
        return _start_sentence(spec)
    if name == END:
        return _end_sentence(spec)
    if name == INSERT:
        return _INSERT_SENTENCE
    return _KEY_SENTENCES[name]


def _library_names(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the time columns the library fills for every row of a dataset.

    Args:
        spec: The dataset.

    Returns:
        `Date` for ticks; `Date`, `TimestampStart`, and `TimestampEnd` for bars and daily
        summaries; and `TimestampInsert` for every dataset, last.
    """
    times = spec.times
    if times is RowTimes.TICK:
        return (DATE, INSERT)
    if times is RowTimes.NONE:
        return (INSERT,)
    return (DATE, START, END, INSERT)


def filled_columns(
    spec: DatasetSpec,
    keys: Iterable[tuple[Transport, str]],
    source: Callable[[Transport, str], Source],
) -> frozenset[str]:
    """Return the columns of a dataset that some of its sources fill, by the generated schemas.

    The columns the sources contribute after the rename (`rename`), plus the time columns the
    library fills for every row whatever the source (`Date`, `TimestampStart`,
    `TimestampEnd`, `TimestampInsert`, as the dataset has them), so two calls for different
    sources of one dataset differ only by what their sources carry. The derived columns
    (`OptionRoot`, `RawSymbol`), which every transport computes alike, are left out.

    Args:
        spec: The dataset.
        keys: The sources, as `(transport, source key)`, such as the REST history path and
            every flat-file prefix of the dataset.
        source: Returns the generated source of a transport and key.

    Returns:
        The column names.

    A bad source raises what `rename` or `source` raises.
    """
    found = {c.column for transport, key in keys for c in rename(spec, source(transport, key))}
    return frozenset(found.union(_library_names(spec)))


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
        One item of the list `_description` joins with "; ", with no closing punctuation:
        ``REST `sip_timestamp`: "<field description>"`` for a wire key with a description,
        ``REST `sip_timestamp``` for one without, or ``REST: the request's `stocksTicker`
        parameter`` for a key filled from the request, with the transport spelled as
        `TRANSPORT_LABELS` spells it (``WebSocket `t`: …``).
    """
    label = TRANSPORT_LABELS[transport]
    if column.field is None:
        return f"{label}: the request's `{column.parameter}` parameter"
    described = column.field.description_for(asset_class)
    if described:
        return f'{label} `{column.field.wire_key}`: "{described}"'
    return f"{label} `{column.field.wire_key}`"


def _description(
    spec: DatasetSpec,
    name: str,
    *,
    own: Sequence[_Contribution],
    library: bool,
    json_text: bool,
) -> str:
    """Pick one column's description for one dataset (the rules of `dataset_schema`).

    Args:
        spec: The dataset.
        name: The column.
        own: The dataset's own contributions to the column, in description order.
        library: True when the library describes the column: one of the dataset's declared key
            columns, or a time column the library fills (`Date`, `TimestampStart`,
            `TimestampEnd`, `TimestampInsert`). A vendor field that merely carries one of those
            names in a dataset that has no such column of its own is described as any other.
        json_text: True for a column whose values are nested objects or lists kept as JSON
            text (`WireType.JSON_TEXT`), so the description says how to read the text.

    Returns:
        The description, with the dataset's note on the column (`DatasetSpec.column_notes`)
        after it, then `JSON_TEXT_NOTE` on a JSON-text column; empty when nothing describes
        the column.
    """
    text = _chosen(spec, name, own=own, library=library)
    for sentence in (spec.column_notes.get(name, ""), JSON_TEXT_NOTE if json_text else ""):
        text = _sentences(text, sentence)
    return text


def _sentences(text: str, sentence: str) -> str:
    """Return a description with one more sentence after it.

    Args:
        text: The description so far; may be empty, and the vendor's may lack its closing
            period.
        sentence: The sentence to add; empty adds nothing.

    Returns:
        `text`, closed with a period where it has no end of its own, then `sentence`.
    """
    if not text or not sentence:
        return text or sentence
    stop = "" if text.endswith((".", "!", "?", *_QUOTED_ENDS)) else "."
    return f"{text}{stop} {sentence}"


def _chosen(
    spec: DatasetSpec,
    name: str,
    *,
    own: Sequence[_Contribution],
    library: bool,
) -> str:
    """Pick one column's description, before the dataset's note (`_description`).

    Args:
        spec: The dataset.
        name: The column.
        own: The dataset's own contributions to the column, in description order.
        library: True when the library describes the column.

    Returns:
        The description; empty when no source describes the column.
    """
    # The wire spec keys its per-class descriptions by class value, and has none for a
    # dataset of every class.
    asset_class = None if spec.asset_class is ClassFromPrefix.ANY else spec.asset_class.value
    if library:
        parts: list[str] = []
        if name in _FROM_PARTS:
            for transport, column in own:
                part = _key_part(transport, column, asset_class)
                if part not in parts:
                    parts.append(part)
        sentence = _library_sentence(spec, name)
        if not parts:
            return sentence
        # One sentence after the library's own, so a lowercase label never starts a sentence.
        # A quote that already ends the sentence inside it gets no second period after it.
        listed = "; ".join(parts)
        end = "" if listed.endswith(_QUOTED_ENDS) else "."
        return f"{sentence} From {listed}{end}"
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
    library: bool,
    filled: tuple[Transport, ...],
    json_text: bool,
) -> Column:
    """Build one column of a dataset schema with its description, transports, and wire keys.

    Args:
        spec: The dataset.
        name: The column.
        column_type: Its merged type.
        own: The dataset's own contributions to the column, in description order.
        library: True when the library describes the column (`_description`).
        filled: The transports on which the library fills the column itself (every transport
            of the dataset for a library time column, none otherwise); they are listed in
            `Column.supplied_by` with the transports that contribute it.
        json_text: True for a column of nested values kept as JSON text (`_description`).

    Returns:
        The column.
    """
    wire_keys: dict[Transport, list[str | None]] = {}
    for transport, column in own:
        keys = wire_keys.setdefault(transport, [])
        key = None if column.field is None else column.field.wire_key
        if key not in keys:
            keys.append(key)
    supplied = tuple(t for t in _TRANSPORT_ORDER if t in wire_keys or t in filled)
    return Column(
        name,
        column_type,
        _description(spec, name, own=own, library=library, json_text=json_text),
        supplied,
        MappingProxyType({transport: tuple(keys) for transport, keys in wire_keys.items()}),
    )


def widen(values: pa.Array, target: pa.DataType) -> pa.Array:
    """Widen one source column to its dataset column's Arrow type, by the lattice joins.

    A source joins its kind with its own wire type, which the merged column may widen
    (`WireType.common_type`): a `long` in a `double` column is cast, and a `long` in a
    `long[]` column becomes a one-element list (a null stays null). A column already of its
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


def _reads_symbol(spec: DatasetSpec) -> bool:
    """Tell whether some source of a dataset carries its symbol in its rows.

    Args:
        spec: The dataset.

    Returns:
        True when a declared `symbol` key is read from rows, not filled from the request: only
        then can Massive send a spelling the library cannot read.
    """
    return any(
        keys.symbol is not None and not keys.symbol.from_request for keys in spec.keys.values()
    )


def _derived(spec: DatasetSpec, symbol: Column | None, *, raw_symbol: bool) -> list[Column]:
    """Return a dataset's derived columns, which no source carries and the transports compute.

    Args:
        spec: The dataset.
        symbol: Its `Symbol` column, or None for a dataset without one.
        raw_symbol: Whether the session keeps unreadable symbols in `RawSymbol`.

    Returns:
        `OptionRoot` for an options dataset, or one whose rows include options contracts
        (`DatasetSpec.option_rows`), with a `Symbol`, then `RawSymbol` when `raw_symbol` is
        set and the dataset reads its symbol from rows. Each is a `String` column with the
        transports and wire keys of `Symbol`, from which it is computed.
    """
    if symbol is None:
        return []
    options = spec.asset_class is AssetClass.OPTIONS or spec.option_rows
    names = [OPTION_ROOT] if options else []
    if raw_symbol and _reads_symbol(spec):
        names.append(RAW_SYMBOL)
    mixed = _OTHER_ROWS if spec.option_rows else ""
    return [
        Column(
            name,
            ColumnType.STRING,
            _sentences(_DERIVED_SENTENCES[name], mixed if name == OPTION_ROOT else ""),
            symbol.supplied_by,
            symbol.wire_keys,
        )
        for name in names
    ]


def dataset_schema(
    spec: DatasetSpec,
    members: Sequence[DatasetSpec],
    source: Callable[[Transport, str], Source],
    *,
    raw_symbol: bool = False,
) -> Schema:
    """Merge the renamed columns of every member's sources into one dataset's schema.

    The kind's order is the ordered union by column name of `rename(member, s)` for every
    source `s` of every member, group by group in member order, each group's sources in their
    positions in it, whichever member reads them (declared order for a source without one;
    `_by_position`), then field order. The dataset keeps, in that order, only the columns some
    source of its own fills, on any transport, with the library's time columns for the
    dataset's row times (`DatasetSpec.times`): `Date` for ticks, `Date`, `TimestampStart`, and
    `TimestampEnd` for bars and daily summaries, and `TimestampInsert` for every dataset.
    `Symbol`, `Date`, `Timestamp`, `TimestampStart`, and `TimestampEnd` come first, in that
    order, where the dataset declares them as keys or the library fills them; `TimestampInsert`
    comes last; every other column, `Underlying` included, takes its place by first appearance.
    A column only other members of the kind fill is left out, so a dataset's columns depend on
    its declared sources alone, never on a call's arguments, and a history table and a live
    table of one dataset have the same columns. Each column's type is the
    `WireType.common_type` of every contribution across the kind, so the columns two members
    share have one type and their tables merge on them. It is mapped through `COLUMN_TYPES` (a
    library `Date` is a date, so ISO text, and the other library columns instants).
    `Catalog.dataset_schema` calls it once per dataset and caches the result.

    Each column also gets its transports and wire keys from the dataset's own sources
    (`Column.supplied_by`, `Column.wire_keys`; a library time column lists every transport of
    the dataset, since the library fills it on each), and one description chosen for this
    dataset, never one per kind: a kind spans asset classes, and the stock text for a price
    ("dollar value per whole share") is wrong for crypto. A field's description is the one for
    the dataset's asset class (`Field.description_for`): the per-class description a source
    that serves several classes gives, else its own, which is the vendor's or the refresh's
    correction of it. Two rules choose it:

    1. A column the dataset's own sources supply takes the first non-empty field description
       among them, trying the transports in the order REST, flat files, WebSocket and, within
       one transport, the sources in their declared order (role paths, then channels, then
       prefixes). REST comes first because its text is the most complete.
    2. The library describes its own columns: `Symbol`, `Timestamp`, and `Underlying` when
       they are declared key columns, and the time columns it fills (`Date`,
       `TimestampStart`, `TimestampEnd`, `TimestampInsert`), each with a sentence of its own
       that says what the column means for this dataset ("The row's primary time, as an
       Instant."; for a bar's `TimestampStart`, where a bar of a day or longer starts). The
       key columns and `Date` and `TimestampStart` go on, per source in the order of rule 1,
       to the wire key the source renames and its quoted field description
       (``REST `sip_timestamp`: "The nanosecond accuracy SIP Unix Timestamp..."``), or, for a
       key filled from the request, the request parameter it came from (``REST: the
       request's `stocksTicker` parameter.``); a part two sources share appears once.

    A dataset's note on a column (`DatasetSpec.column_notes`) follows its description. A column
    whose sources carry no description gets `""`, and nothing fails.

    Derived columns, which no source carries and every transport computes from `Symbol`, follow
    the leading key columns, with the library's own description: `OptionRoot` on every options
    dataset with a `Symbol` (`symbols.option_roots`), and `RawSymbol` when `raw_symbol` is set
    and the dataset reads its symbol from rows.

    Args:
        spec: The dataset whose schema is built; it names the schema, decides the leading
            key columns and the library's time columns, and is the dataset the descriptions
            are chosen for.
        members: The datasets whose sources merge, in member order: the datasets of the
            dataset's kind (`Catalog.members`), or the dataset alone when it is in no kind.
        source: Returns the generated source of a transport and key.
        raw_symbol: True for a session whose `unreadable_symbols` is `LOG` or `IGNORE`, which
            keeps the text of an unreadable symbol in `RawSymbol`.

    Returns:
        The schema, named after the dataset.

    Raises:
        ValueError: If two contributions to one column conflict (`"member types: ..."`, naming
            the column, both types, and the member and source of the second, or the library
            for one of its time columns). A failed rename raises the `ValueError` of `rename`,
            and an unknown source whatever `source` raises.
    """
    types: dict[str, WireType] = {}
    for member, transport, key, found in _by_position(members, source):
        for contribution in rename(member, found):
            prior = types.get(contribution.column)
            joined = (
                contribution.wire_type
                if prior is None
                else prior.common_type(contribution.wire_type)
            )
            if joined is None:
                raise ValueError(
                    f"member types: {spec.name}: column {contribution.column!r} is "
                    f"{prior} elsewhere but {contribution.wire_type} in {member.name} "
                    f"{transport.value} {key}"
                )
            types[contribution.column] = joined
    library = _library_names(spec)
    for name in library:
        prior = types.get(name)
        joined = _LIBRARY_TYPES[name] if prior is None else prior.common_type(_LIBRARY_TYPES[name])
        if joined is None:
            raise ValueError(
                f"member types: {spec.name}: column {name!r} is {prior} in a source but "
                f"{_LIBRARY_TYPES[name]} as the library fills it"
            )
        types[name] = joined
    declared = {key_column(spec, r, k) for keys in spec.keys.values() for r, k in keys.declared()}
    described = declared | set(library)
    own = _own_contributions(spec, source)
    leading = [name for name in _LEADING if name in described]
    # A column only other members of the kind fill is left out: no transport of this dataset
    # could ever fill it, whatever the call's arguments.
    rest = [
        name
        for name in types
        if name not in leading and name != INSERT and (name in own or name in described)
    ]
    order = [*leading, *rest, *((INSERT,) if INSERT in types else ())]
    transports = tuple(t for t in _TRANSPORT_ORDER if any(d is t for d, _ in spec.sources))
    columns = [
        replace(
            _column(
                spec,
                name,
                column_type=COLUMN_TYPES[types[name]],
                own=own.get(name, ()),
                library=name in described and (name in _KEY_SENTENCES or name in library),
                filled=transports if name in library else (),
                json_text=types[name] is WireType.JSON_TEXT,
            ),
            calendar_date=types[name] is WireType.DATE,
        )
        for name in order
    ]
    derived = _derived(
        spec,
        next((c for c in columns if c.name == "Symbol" and "Symbol" in declared), None),
        raw_symbol=raw_symbol,
    )
    return Schema(spec.name, (*columns[: len(leading)], *derived, *columns[len(leading) :]))
