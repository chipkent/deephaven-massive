"""The schema registry: column types, SDK-model flattening, and the four tick schemas.

Every dataset kind has exactly one schema, the union of its fields across asset classes and
transports (WebSocket, REST, and flat files), with PascalCase column names (DESIGN § 2.1). A
column a class or transport does not supply is null: crypto trades have no `Tape`, and stocks
have no `SessionEndDate`. This module holds the machinery that builds and fills those schemas:

- `ColumnType`, the closed set of column types (`Instant`, `LocalDate`, `double`, `long`,
  `int`, `String`, `boolean`, `int[]`), each with its Deephaven and Arrow type.
- `Schema`, an ordered set of typed columns that produces Deephaven column definitions for
  `table_publisher` and `input_table`, and turns row dictionaries into an Arrow table.
- `model_schema`, which derives a REST dataset's schema from an SDK `@modelclass`: nested models
  are flattened (`last_trade.price` becomes `LastTradePrice`), Python hints map to column
  types, and an explicit `ColumnOverride` table corrects wrong SDK hints and narrows types.
  There is no column inference from data and no fallback chain of alternative keys.
- The four hand-written tick schemas (`TRADES`, `QUOTES`, `BARS`, `INDEX_VALUES`), shared by the
  WebSocket, REST, and flat-file forms of the tick datasets, each with explicit per-transport
  key maps (wire key, SDK field, or CSV header to column) for the equity form: the stocks and
  options WebSocket frames, the SDK REST models, and the documented flat-file headers. Other
  asset classes declare their own key maps, and the units of their timestamp fields (crypto
  `r` is milliseconds), in their `DatasetSpec` (P4 and P6).

Timestamp *units* are not decided here: a field becomes an `Instant` column because its dataset
declares a unit for it (`_timeunits.py`), and the transports convert values to nanoseconds
before they reach `Schema.to_arrow`.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import types
import typing
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final

import pyarrow as pa
from deephaven import dtypes

from deephaven_massive._enums import Transport
from deephaven_massive._errors import MassiveError

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping, Sequence

    from _typeshed import DataclassInstance

__all__ = [
    "BARS",
    "INDEX_VALUES",
    "QUOTES",
    "TRADES",
    "Column",
    "ColumnOverride",
    "ColumnType",
    "KeyMap",
    "ModelSchema",
    "Schema",
    "TickSchema",
    "flatten",
    "model_schema",
    "pascal_case",
]

logger = logging.getLogger(__name__)

KeyMap = typing.Mapping[str, str]
"""Maps a wire key, SDK field path, or flat-file header to the column it fills."""

_DATE_FIELD: Final = re.compile(r"^(date|.*_date)$")
_UTC_FIELD: Final = re.compile(r"^.+_utc$")
_ISO_FRACTION: Final = re.compile(r"\.(\d+)")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


class ColumnType(StrEnum):
    """The type of a column, named by its Deephaven type.

    Each member knows its Deephaven `DType` (`dh_type`) and its Arrow type (`arrow_type`).
    DESIGN § 11 fixes the conventions: timestamps are `Instant`, calendar dates `LocalDate`,
    sequence numbers `long`, sizes `double` (fractional sizes exist), exchange ids `int`, and
    condition codes `int[]`.
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
    def dh_type(self) -> dtypes.DType:
        """The Deephaven column type, for `table_publisher` and `input_table` definitions."""
        return typing.cast("dtypes.DType", _DH_TYPES[self])

    @property
    def arrow_type(self) -> pa.DataType:
        """The Arrow type that `deephaven.arrow.to_table` converts to this column type."""
        return _ARROW_TYPES[self]


# `dtypes.Instant` and `dtypes.LocalDate` are annotated as `type` by deephaven-core although
# they are `DType` instances, so the table is typed by value and narrowed on lookup.
_DH_TYPES: Final[Mapping[ColumnType, object]] = {
    ColumnType.INSTANT: dtypes.Instant,
    ColumnType.LOCAL_DATE: dtypes.LocalDate,
    ColumnType.DOUBLE: dtypes.double,
    ColumnType.LONG: dtypes.long,
    ColumnType.INT: dtypes.int32,
    ColumnType.STRING: dtypes.string,
    ColumnType.BOOLEAN: dtypes.bool_,
    ColumnType.INT_ARRAY: dtypes.int32_array,
}

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
    epoch nanoseconds for an `Instant` column, read as UTC when it carries no offset (Massive's
    ISO timestamps are UTC), keeping every fraction digit down to the nanosecond; a dict or list
    becomes its JSON text for a `String` column, nested SDK models included (used by
    `ColumnOverride(type=ColumnType.STRING)` on composite SDK fields); a single integer becomes
    a one-element list for an `int[]` column (the WebSocket quote `c` is one condition where
    REST sends a list); and an integral `float` becomes an `int` for a `long` or `int` column,
    and for each element of an `int[]` list, while any other `float` there is rejected,
    because Arrow would silently truncate `7140.08` to `7140`. Everything else passes through
    for Arrow to check; in particular a naive `datetime` object in an `Instant` column is read
    as UTC, as Arrow reads it.

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
        return json.dumps(value, sort_keys=True, default=_jsonable)
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


def _jsonable(value: object) -> object:
    """Make a nested SDK model JSON-serializable, for `json.dumps(default=...)`.

    Args:
        value: An object `json` cannot encode by itself.

    Returns:
        The dictionary of a dataclass instance (nested models included).

    Raises:
        TypeError: If the value is not a dataclass instance.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


@dataclass(frozen=True, slots=True)
class Schema:
    """A named, ordered set of typed columns: the shape of one dataset's tables.

    Instances are immutable and shared freely across threads.

    Example:
        ::

            from deephaven_massive._schema import Column, ColumnType, Schema

            schema = Schema(
                "example",
                (Column("Symbol", ColumnType.STRING), Column("Price", ColumnType.DOUBLE)),
            )
            schema.col_defs()  # {"Symbol": dtypes.string, "Price": dtypes.double}
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

    def col_defs(self) -> dict[str, dtypes.DType]:
        """Return the Deephaven column definitions of this schema.

        Returns:
            A `{name: DType}` mapping in table order, as `table_publisher(col_defs=...)` and
            `input_table(col_defs=...)` take it.
        """
        return {column.name: column.type.dh_type for column in self.columns}

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
        nanoseconds or `datetime`s (a naive one is read as UTC); the transports convert units
        with `_timeunits.py` first. A `float` with a fractional part in a `long` or `int`
        column is a schema mismatch, never truncated. This runs on the calling thread and makes
        no Deephaven call.

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


@dataclass(frozen=True, slots=True)
class ColumnOverride:
    """An explicit correction to the column an SDK model field produces.

    Overrides exist because some SDK type hints are wrong (`IndexValue.timestamp: str`), some
    types need narrowing (exchange ids are `int`, not `long`), and some fields have no
    automatic mapping (unions, lists of strings, dictionaries). Each override is tested by the
    dataset that declares it.

    Without an override, `model_schema` maps a `str` field named `date` or `*_date` to
    `LocalDate`, a `str` field named `*_utc` (an ISO-8601 timestamp such as `published_utc`)
    to `Instant`, and an `int` field the dataset declares a time unit for to `Instant`. An
    override with `type=ColumnType.INSTANT` on any other `str` field parses ISO-8601 text.

    Attributes:
        type: The column type to use instead of the one the hint implies.
        name: The column name to use instead of the PascalCase of the field path.
        drop: True to leave the field out of the schema entirely.
    """

    type: ColumnType | None = None
    name: str | None = None
    drop: bool = False


def pascal_case(path: str) -> str:
    """Turn an SDK field path into a PascalCase column name.

    Every underscore- or dot-separated word is capitalized and joined: `sip_timestamp` becomes
    `SipTimestamp` and the nested path `last_trade.sip_timestamp` becomes
    `LastTradeSipTimestamp`. Words that are already capitalized keep their other letters.

    Args:
        path: A field name or a dot-separated path of nested field names.

    Returns:
        The PascalCase column name.
    """
    words = re.split(r"[._]+", path)
    return "".join(word[:1].upper() + word[1:] for word in words if word)


def _unwrap_optional(hint: object) -> object:
    """Strip `None` from an `Optional[...]`/`X | None` hint.

    Args:
        hint: A resolved type hint.

    Returns:
        The single remaining type, or the hint unchanged when it is not a two-way optional.
    """
    if typing.get_origin(hint) in {typing.Union, types.UnionType}:
        members = [arg for arg in typing.get_args(hint) if arg is not type(None)]
        if len(members) == 1:
            return members[0]
    return hint


_SCALAR_TYPES: Final[Mapping[object, ColumnType]] = {
    bool: ColumnType.BOOLEAN,
    int: ColumnType.LONG,
    float: ColumnType.DOUBLE,
    str: ColumnType.STRING,
}


def _column_type(path: str, hint: object, *, instant: bool) -> ColumnType:
    """Map one resolved SDK hint to a column type, without overrides.

    Args:
        path: The field's dotted path, used for the date-name rule and in errors.
        hint: The field's resolved type hint, `Optional` already removed.
        instant: True when the dataset declares a time unit for this field.

    Returns:
        `INSTANT` for a declared timestamp or a `str` field named `*_utc`, `LOCAL_DATE` for a
        `str` field named `date` or `*_date`, `INT_ARRAY` for `List[int]`, and the scalar
        mapping otherwise (`bool`, `int` to `long`, `float` to `double`, `str`).

    Raises:
        TypeError: If the hint has no automatic mapping (a union, a list of anything but
            `int`, a dictionary); the dataset must declare a `ColumnOverride` for it.
    """
    if instant:
        return ColumnType.INSTANT
    name = path.rsplit(".", 1)[-1]
    if hint is str and _UTC_FIELD.match(name):
        return ColumnType.INSTANT
    if hint is str and _DATE_FIELD.match(name):
        return ColumnType.LOCAL_DATE
    if typing.get_origin(hint) is list and typing.get_args(hint) == (int,):
        return ColumnType.INT_ARRAY
    try:
        return _SCALAR_TYPES[hint]
    except (KeyError, TypeError):
        raise TypeError(
            f"SDK field {path!r} has type {hint!r}, which has no column mapping; "
            "declare a ColumnOverride for it"
        ) from None


@dataclass(frozen=True, slots=True)
class ModelSchema:
    """A REST dataset's schema together with the field path behind each column.

    Built by `model_schema`. `row()` turns one SDK model instance into a row dictionary for
    `Schema.to_arrow`.

    Attributes:
        schema: The dataset's columns.
        columns_by_path: The column each flattened SDK field path fills; dropped fields are
            absent.
    """

    schema: Schema
    columns_by_path: Mapping[str, str] = field(default_factory=dict)

    def row(self, instance: object) -> dict[str, object]:
        """Flatten one SDK model instance into a row keyed by column name.

        Timestamp fields keep their raw epoch integers; the REST transport converts their
        units before building the Arrow batch.

        Args:
            instance: An instance of the model the schema was built from.

        Returns:
            A `{column: value}` dictionary with one entry per non-dropped field.
        """
        flat = flatten(instance)
        return {column: flat.get(path) for path, column in self.columns_by_path.items()}


def _model_fields(model: type, prefix: str = "") -> Iterable[tuple[str, object]]:
    """Yield `(dotted path, hint)` for every leaf field of a dataclass model, depth first.

    Args:
        model: An SDK `@modelclass` (a dataclass).
        prefix: The dotted path of `model` inside its parent; empty for the root.

    Yields:
        One `(path, hint)` per leaf field, with `Optional` removed from the hint.
    """
    hints = typing.get_type_hints(model)
    for model_field in dataclasses.fields(model):
        path = f"{prefix}{model_field.name}"
        hint = _unwrap_optional(hints[model_field.name])
        if isinstance(hint, type) and dataclasses.is_dataclass(hint):
            yield from _model_fields(hint, f"{path}.")
        else:
            yield path, hint


def model_schema(
    name: str,
    model: type,
    *,
    instant_fields: Collection[str] = (),
    overrides: Mapping[str, ColumnOverride] | None = None,
) -> ModelSchema:
    """Derive a dataset's schema from an SDK model class.

    Every leaf field of the model becomes one column, in declaration order, with nested models
    flattened depth first (`last_trade.price` becomes `LastTradePrice`). Types follow the hint
    (see `ColumnType`), a field listed in `instant_fields` becomes an `Instant`, and an
    override replaces the type, renames the column, or drops the field. A field with no
    automatic mapping (a union, a list of anything but `int`, a dictionary) and no override
    raises `TypeError` naming the field, so every such field is decided explicitly.

    Args:
        name: The schema's name, usually the dataset name.
        model: The SDK `@modelclass`, for example `massive.rest.models.Trade`.
        instant_fields: Dotted paths of the fields the dataset declares time units for.
        overrides: Corrections keyed by dotted field path.

    Returns:
        The schema and the path behind each column.

    Raises:
        ValueError: If `model` is not a dataclass, or an override or instant field names a
            path the model does not have.
    """
    if not dataclasses.is_dataclass(model):
        raise ValueError(f"{model!r} is not an SDK model class")
    overrides = overrides or {}
    leaves = dict(_model_fields(model))
    unknown = sorted((set(overrides) | set(instant_fields)) - set(leaves))
    if unknown:
        raise ValueError(f"{model.__name__} has no fields {unknown}")
    columns: list[Column] = []
    columns_by_path: dict[str, str] = {}
    for path, hint in leaves.items():
        override = overrides.get(path, ColumnOverride())
        if override.drop:
            continue
        column_type = override.type or _column_type(path, hint, instant=path in instant_fields)
        column_name = override.name or pascal_case(path)
        columns.append(Column(column_name, column_type))
        columns_by_path[path] = column_name
    return ModelSchema(Schema(name, tuple(columns)), columns_by_path)


def flatten(instance: object) -> dict[str, object]:
    """Flatten an SDK model instance into `{dotted path: value}` for its leaf fields.

    A nested model is recursed into; a nested model that is `None` contributes no keys (its
    columns are null). Values are not converted.

    Args:
        instance: A dataclass instance, such as an SDK `TickerSnapshot`.

    Returns:
        The leaf values keyed by dotted field path, for example
        `{"ticker": "AAPL", "last_trade.price": 189.5}`.

    Raises:
        ValueError: If `instance` is not a dataclass instance.
    """
    if not dataclasses.is_dataclass(instance) or isinstance(instance, type):
        raise ValueError(f"{instance!r} is not an SDK model instance")
    flat: dict[str, object] = {}
    _flatten_into(flat, instance, "")
    return flat


def _flatten_into(flat: dict[str, object], instance: DataclassInstance, prefix: str) -> None:
    """Add the leaf values of one dataclass instance to `flat`.

    Args:
        flat: The dictionary being filled.
        instance: A dataclass instance.
        prefix: The dotted path of `instance` inside the root; empty for the root.
    """
    for model_field in dataclasses.fields(instance):
        value = getattr(instance, model_field.name, None)
        path = f"{prefix}{model_field.name}"
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            _flatten_into(flat, value, f"{path}.")
        else:
            flat[path] = value


@dataclass(frozen=True, slots=True)
class TickSchema:
    """A hand-written tick schema shared by a tick dataset's transports, with its key maps.

    The four tick kinds (trades, quotes, bars, index values) arrive in three forms: WebSocket
    frames with single-letter keys, SDK REST models with snake-case fields, and flat-file CSV
    headers. A tick schema fixes the union of their columns once and maps each form's keys to
    those columns explicitly, one key per column, with no alternatives.

    Attributes:
        schema: The union columns.
        key_maps: For each transport, the key (WebSocket key, SDK field, or CSV header) that
            fills each column, for the equity form of the data.
    """

    schema: Schema
    key_maps: Mapping[Transport, KeyMap]

    def __post_init__(self) -> None:
        """Check that every key map targets columns of the schema.

        Raises:
            ValueError: If a key map names a column the schema lacks.
        """
        names = set(self.schema.column_names)
        for transport, key_map in self.key_maps.items():
            unknown = sorted(set(key_map.values()) - names)
            if unknown:
                raise ValueError(
                    f"{self.schema.name} {transport.value} key map targets unknown {unknown}"
                )


def _schema(name: str, columns: Mapping[str, ColumnType]) -> Schema:
    """Build a schema from an ordered `{name: type}` mapping.

    Args:
        name: The schema's name.
        columns: The columns in table order.

    Returns:
        The schema.
    """
    return Schema(name, tuple(Column(n, t) for n, t in columns.items()))


_S, _D, _L, _I, _T, _A, _LD = (
    ColumnType.STRING,
    ColumnType.DOUBLE,
    ColumnType.LONG,
    ColumnType.INT,
    ColumnType.INSTANT,
    ColumnType.INT_ARRAY,
    ColumnType.LOCAL_DATE,
)

TRADES: Final = TickSchema(
    _schema(
        "trades",
        {
            "Symbol": _S,
            "Timestamp": _T,
            "Price": _D,
            "Size": _D,
            "DecimalSize": _S,
            "Exchange": _I,
            "Id": _S,
            "Conditions": _A,
            "Tape": _I,
            "SequenceNumber": _L,
            "Correction": _I,
            "ParticipantTimestamp": _T,
            "TrfId": _I,
            "TrfTimestamp": _T,
            # Other asset classes (null for stocks and options): crypto `r`, and the futures
            # session date, feed channel, and report sequence.
            "ReceivedTimestamp": _T,
            "SessionEndDate": _LD,
            "Channel": _I,
            "ReportSequence": _L,
        },
    ),
    {
        # massive.websocket.models.EquityTrade.from_dict
        Transport.WS: {
            "sym": "Symbol",
            "t": "Timestamp",
            "p": "Price",
            "s": "Size",
            "ds": "DecimalSize",
            "x": "Exchange",
            "i": "Id",
            "c": "Conditions",
            "z": "Tape",
            "q": "SequenceNumber",
            "trfi": "TrfId",
            "trft": "TrfTimestamp",
        },
        # massive.rest.models.Trade fields
        Transport.REST: {
            "sip_timestamp": "Timestamp",
            "price": "Price",
            "size": "Size",
            "decimal_size": "DecimalSize",
            "exchange": "Exchange",
            "id": "Id",
            "conditions": "Conditions",
            "tape": "Tape",
            "sequence_number": "SequenceNumber",
            "correction": "Correction",
            "participant_timestamp": "ParticipantTimestamp",
            "trf_id": "TrfId",
            "trf_timestamp": "TrfTimestamp",
        },
        # us_stocks_sip/trades_v1 headers
        Transport.FLAT_FILES: {
            "ticker": "Symbol",
            "sip_timestamp": "Timestamp",
            "price": "Price",
            "size": "Size",
            "exchange": "Exchange",
            "id": "Id",
            "conditions": "Conditions",
            "tape": "Tape",
            "sequence_number": "SequenceNumber",
            "correction": "Correction",
            "participant_timestamp": "ParticipantTimestamp",
            "trf_id": "TrfId",
            "trf_timestamp": "TrfTimestamp",
        },
    },
)
"""Trades: one row per trade. `Timestamp` is the SIP timestamp."""

QUOTES: Final = TickSchema(
    _schema(
        "quotes",
        {
            "Symbol": _S,
            "Timestamp": _T,
            "BidPrice": _D,
            "BidSize": _D,
            "BidExchange": _I,
            "AskPrice": _D,
            "AskSize": _D,
            "AskExchange": _I,
            "Conditions": _A,
            "Indicators": _A,
            "Tape": _I,
            "SequenceNumber": _L,
            "ParticipantTimestamp": _T,
            "TrfId": _I,
            "TrfTimestamp": _T,
            # Other asset classes (null for stocks and options): the crypto quote's single
            # exchange `x` and `r`, and the futures session date, feed channel, report
            # sequence, and per-side timestamps.
            "Exchange": _I,
            "ReceivedTimestamp": _T,
            "SessionEndDate": _LD,
            "Channel": _I,
            "ReportSequence": _L,
            "BidTimestamp": _T,
            "AskTimestamp": _T,
        },
    ),
    {
        # massive.websocket.models.EquityQuote.from_dict; `c` is one condition code.
        Transport.WS: {
            "sym": "Symbol",
            "t": "Timestamp",
            "bp": "BidPrice",
            "bs": "BidSize",
            "bx": "BidExchange",
            "ap": "AskPrice",
            "as": "AskSize",
            "ax": "AskExchange",
            "c": "Conditions",
            "i": "Indicators",
            "z": "Tape",
            "q": "SequenceNumber",
            "trfi": "TrfId",
            "trft": "TrfTimestamp",
        },
        # massive.rest.models.Quote fields
        Transport.REST: {
            "sip_timestamp": "Timestamp",
            "bid_price": "BidPrice",
            "bid_size": "BidSize",
            "bid_exchange": "BidExchange",
            "ask_price": "AskPrice",
            "ask_size": "AskSize",
            "ask_exchange": "AskExchange",
            "conditions": "Conditions",
            "indicators": "Indicators",
            "tape": "Tape",
            "sequence_number": "SequenceNumber",
            "participant_timestamp": "ParticipantTimestamp",
            "trf_timestamp": "TrfTimestamp",
        },
        # us_stocks_sip/quotes_v1 headers
        Transport.FLAT_FILES: {
            "ticker": "Symbol",
            "sip_timestamp": "Timestamp",
            "bid_price": "BidPrice",
            "bid_size": "BidSize",
            "bid_exchange": "BidExchange",
            "ask_price": "AskPrice",
            "ask_size": "AskSize",
            "ask_exchange": "AskExchange",
            "conditions": "Conditions",
            "indicators": "Indicators",
            "tape": "Tape",
            "sequence_number": "SequenceNumber",
            "participant_timestamp": "ParticipantTimestamp",
            "trf_timestamp": "TrfTimestamp",
        },
    },
)
"""Quotes: one row per NBBO (or BBO) update. `Timestamp` is the SIP timestamp."""

BARS: Final = TickSchema(
    _schema(
        "bars",
        {
            "Symbol": _S,
            "Timestamp": _T,
            "EndTimestamp": _T,
            "Open": _D,
            "High": _D,
            "Low": _D,
            "Close": _D,
            "Volume": _D,
            "Vwap": _D,
            "Transactions": _L,
            "AccumulatedVolume": _D,
            "OfficialOpenPrice": _D,
            "AggregateVwap": _D,
            "AverageSize": _D,
            "Otc": ColumnType.BOOLEAN,
            # Futures (null for other classes): REST `dollar_volume` and WebSocket `dv`
            # (`FuturesAgg.total_value`), the session date, and the settlement price.
            "DollarVolume": _D,
            "SessionEndDate": _LD,
            "SettlementPrice": _D,
        },
    ),
    {
        # massive.websocket.models.EquityAgg.from_dict
        Transport.WS: {
            "sym": "Symbol",
            "s": "Timestamp",
            "e": "EndTimestamp",
            "o": "Open",
            "h": "High",
            "l": "Low",
            "c": "Close",
            "v": "Volume",
            "vw": "Vwap",
            "av": "AccumulatedVolume",
            "op": "OfficialOpenPrice",
            "a": "AggregateVwap",
            "z": "AverageSize",
            "otc": "Otc",
        },
        # massive.rest.models.Agg fields
        Transport.REST: {
            "timestamp": "Timestamp",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "vwap": "Vwap",
            "transactions": "Transactions",
            "otc": "Otc",
        },
        # us_stocks_sip/{minute,day}_aggs_v1 headers
        Transport.FLAT_FILES: {
            "ticker": "Symbol",
            "window_start": "Timestamp",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "transactions": "Transactions",
        },
    },
)
"""Bars: one row per aggregate window. `Timestamp` is the window start."""

INDEX_VALUES: Final = TickSchema(
    _schema("index_values", {"Symbol": _S, "Timestamp": _T, "Value": _D}),
    {
        # massive.websocket.models.IndexValue.from_dict
        Transport.WS: {"T": "Symbol", "t": "Timestamp", "val": "Value"},
        # us_indices/values_v1 headers
        Transport.FLAT_FILES: {"ticker": "Symbol", "timestamp": "Timestamp", "value": "Value"},
    },
)
"""Index values: one row per published index value."""
