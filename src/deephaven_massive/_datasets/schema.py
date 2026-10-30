"""Dataset schemas: column types, `Schema`, and the merge of a kind's sources.

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
- `dataset_schema`, the merge: the renamed columns (`source_columns.rename`) of the dataset's
  kind (`Catalog.members`) that some source of the dataset itself fills, in the kind's order, with
  the library's own time columns (`row_times`): `Symbol`, `Date`, and `Timestamp` or
  `TimestampStart` and `TimestampEnd` first, `TimestampInsert` last, each column's wire type the
  common type (`WireType.common_type`) of every contribution across the kind. A column only another
  member of the kind fills is left out, so crypto trades have no stocks-only `Tape`, while the
  columns two members share keep one name, type, and place, so their tables merge on them. Each
  column also gets one description chosen for the dataset
  (`column_descriptions.column_description`), the transports that fill it, and the wire keys behind
  it (`Column`); every result's column descriptions and the queries' `columns()` tables read them
  from here. `Catalog` caches the result per dataset; this module holds no state.

The merge runs from the committed generated files only, never from data. The Deephaven types of
these columns, and the column definitions `table_publisher` and `input_table` take, come from
`_tables/columns.py`: this module makes no Deephaven call. Epoch timestamps keep their wire
unit here (`source_columns.SourceColumn.unit`); the transports convert them with
`timestamps.py`.
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

from deephaven_massive._datasets.column_descriptions import (
    DERIVED_SENTENCES,
    KEY_SENTENCES,
    OPTION_ROOT,
    OTHER_ROWS,
    RAW_SYMBOL,
    TRANSPORT_ORDER,
    Contribution,
    column_description,
    with_sentence,
)
from deephaven_massive._datasets.row_times import DATE, END, INSERT, START, TIMESTAMP, RowTimes
from deephaven_massive._datasets.source_columns import key_column, kind_position, rename
from deephaven_massive._enums import AssetClass
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._massive import Source

__all__ = [
    "COLUMN_TYPES",
    "Column",
    "ColumnType",
    "Schema",
    "dataset_schema",
    "filled_columns",
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


_LEADING: Final = ("Symbol", DATE, TIMESTAMP, START, END)
"""The columns a merged schema starts with, in this order, where the dataset declares them as
keys or the library fills them; every other column, `Underlying` included, takes its place by
first appearance, and `TimestampInsert` comes last."""

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
        return (groups[member.group], kind_position(member, generated) or 0, index)

    return [entry for _, entry in sorted(enumerate(found), key=rank)]


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


def _own_contributions(
    spec: DatasetSpec, source: Callable[[Transport, str], Source]
) -> dict[str, list[Contribution]]:
    """Return what the dataset's own sources contribute to each column, in description order.

    Args:
        spec: The dataset.
        source: Returns the generated source of a transport and key.

    Returns:
        For each column the dataset's own sources fill, every contribution: the transports in
        the order `REST`, `FLAT_FILES`, `WS` and, within one transport, the sources in their
        declared order (`DatasetSpec.sources`: role paths, then channels, then prefixes).
    """
    own: dict[str, list[Contribution]] = {}
    for transport in TRANSPORT_ORDER:
        for declared, key in spec.sources:
            if declared is transport:
                for column in rename(spec, source(transport, key)):
                    own.setdefault(column.column, []).append((transport, column))
    return own


def _column(
    spec: DatasetSpec,
    name: str,
    *,
    column_type: ColumnType,
    own: Sequence[Contribution],
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
        library: True when the library describes the column (`column_description`).
        filled: The transports on which the library fills the column itself (every transport
            of the dataset for a library time column, none otherwise); they are listed in
            `Column.supplied_by` with the transports that contribute it.
        json_text: True for a column of nested values kept as JSON text (`column_description`).

    Returns:
        The column.
    """
    wire_keys: dict[Transport, list[str | None]] = {}
    for transport, column in own:
        keys = wire_keys.setdefault(transport, [])
        key = None if column.field is None else column.field.wire_key
        if key not in keys:
            keys.append(key)
    supplied = tuple(t for t in TRANSPORT_ORDER if t in wire_keys or t in filled)
    return Column(
        name,
        column_type,
        column_description(spec, name, own=own, library=library, json_text=json_text),
        supplied,
        MappingProxyType({transport: tuple(keys) for transport, keys in wire_keys.items()}),
    )


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
    mixed = OTHER_ROWS if spec.option_rows else ""
    return [
        Column(
            name,
            ColumnType.STRING,
            with_sentence(DERIVED_SENTENCES[name], mixed if name == OPTION_ROOT else ""),
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
        raw_symbol: True for a session whose `unknown_symbols` is `WARN`, which keeps the text
            of an unreadable symbol in `RawSymbol`.

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
    transports = tuple(t for t in TRANSPORT_ORDER if any(d is t for d, _ in spec.sources))
    columns = [
        replace(
            _column(
                spec,
                name,
                column_type=COLUMN_TYPES[types[name]],
                own=own.get(name, ()),
                library=name in described and (name in KEY_SENTENCES or name in library),
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
