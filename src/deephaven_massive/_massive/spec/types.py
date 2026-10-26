"""The value types that describe Massive's wire format.

A **source** is one thing Massive serves rows from: a REST path (`/v3/trades/{stockTicker}`),
a WebSocket channel of one cluster (`stocks/T`), or a flat-file prefix
(`us_stocks_sip/trades_v1`). Each is described by
its ordered **fields**: the key on the wire, the long-form column the library names it, the
field's `WireType`, its timestamp `WireUnit` when it is an epoch, and whether the vendor marks
it required. A REST source is a `RestPath`, which adds its result key and its parameters.

A source may belong to dataset **kinds** (trades, quotes, bars, ...): the sources whose rows
merge into one table schema. Each `Membership` names the kind, the asset-class group within
it, the source's position in that group, and optionally a projection that keeps only some of
its fields, under other column names, for that membership only.

Decoders compare each payload with its source and return a `DriftReport` of the keys that
were extra or missing.

The refresh generates every value of these types into `spec/*.json`, and the library reads
them from there; tests build them by hand. Every type is a frozen dataclass or a `StrEnum`,
so every value is immutable and safe to share between threads. Constructors check the
invariants a decoder relies on (an epoch has a unit, a source has no two fields with one
column, a REST path's path parameters are exactly its placeholders, ...) and raise
`ValueError` when one fails.

This module imports only the standard library.
"""

from __future__ import annotations

import dataclasses
import string
from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "DriftReport",
    "Field",
    "Membership",
    "Param",
    "ParamKind",
    "RestPath",
    "Source",
    "Transport",
    "WireType",
    "WireUnit",
]


class Transport(StrEnum):
    """The transport a source is served on.

    It is meant to replace the library's own `_enums.Transport`, whose members and values it
    copies exactly (a test pins the two together until then). Its values are the text the
    diagnostics tables show in their `Transport` column.
    """

    WS = "WS"
    REST = "REST"
    FLAT_FILES = "FLAT_FILES"


class WireType(StrEnum):
    """The type of a field's values on the wire, after any declared conversion.

    The refresh assigns each field one of these. Two sources that share a column within a
    dataset kind may differ only by a widening (`LONG` with `DOUBLE` gives `DOUBLE`, `LONG`
    with `INT_ARRAY` gives `INT_ARRAY`); any other pair is a conflict the refresh rejects.
    """

    LONG = "long"
    """A JSON integer, or a flat-file `integer` column, within the signed 64-bit range."""
    DOUBLE = "double"
    """A JSON number, or a flat-file `number` column."""
    BOOL = "bool"
    """A JSON `true` or `false`; never a number."""
    STRING = "string"
    """Text."""
    DATE = "date"
    """A calendar date as ISO-8601 text, `YYYY-MM-DD`."""
    EPOCH = "epoch"
    """An integer count of `WireUnit`s since the Unix epoch; the field's `unit` says which."""
    ISO_INSTANT = "iso-instant"
    """An instant as ISO-8601 date-time text; text without an offset is UTC."""
    INT_ARRAY = "int-array"
    """A list of integers, each within the signed 32-bit range (condition codes)."""
    JSON_TEXT = "json-text"
    """A nested object, a list of strings or objects, or a choice of shapes, kept as JSON text."""


class WireUnit(StrEnum):
    """The unit of an `EPOCH` field: how many of these have elapsed since the Unix epoch."""

    S = "s"
    """Seconds."""
    MS = "ms"
    """Milliseconds."""
    US = "us"
    """Microseconds."""
    NS = "ns"
    """Nanoseconds."""


class ParamKind(StrEnum):
    """The kind of value a REST parameter takes, which decides how a value is checked and sent.

    Every value is sent as text. An enum member is sent by its value, a `bool` as `true` or
    `false`, and a `date` as ISO-8601 `YYYY-MM-DD`. `DATE` and `TIMESTAMP` both accept a date
    or an aware instant; they differ only in how an instant is sent.
    """

    STRING = "string"
    """Free text, sent unchanged."""
    INTEGER = "integer"
    """An `int`."""
    NUMBER = "number"
    """An `int` or a `float`."""
    BOOLEAN = "boolean"
    """A `bool`, sent as `true` or `false`."""
    ENUM = "enum"
    """One of the parameter's `values`."""
    DATE = "date"
    """Date or instant; an instant is sent as ISO-8601 date-time text, not date-only."""
    TIMESTAMP = "timestamp"
    """Date or instant; an instant is sent as exact integer nanoseconds since the Unix epoch."""
    LIST = "list"
    """A sequence of strings, sent joined with commas (the `.any_of` parameters)."""


@dataclass(frozen=True, kw_only=True)
class Field:
    """One field of a source: a key on the wire and the column the library names it.

    Attributes:
        wire_key: The key as Massive sends it. Nested REST objects are flattened to a dotted
            path below the result key, such as `"day.c"`; a flat-file field is its CSV header.
        column: The long-form, PascalCase column name, such as `"Close"` or `"PrevDayVwap"`.
        wire_type: The type of the field's values.
        unit: The timestamp unit; set exactly when `wire_type` is `WireType.EPOCH`.
        required: Whether the vendor marks the field required, so its absence is reported as
            drift. False by default, as for almost every field Massive documents.

    Example:
        ::

            from deephaven_massive._massive.spec.types import Field, WireType, WireUnit

            sip_time = Field(
                wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.MS
            )
    """

    wire_key: str
    column: str
    wire_type: WireType
    unit: WireUnit | None = None
    required: bool = False

    def __post_init__(self) -> None:
        """Check that the unit is set exactly for an epoch.

        Raises:
            ValueError: If `wire_type` is `EPOCH` and `unit` is `None`, or the reverse.
        """
        if (self.wire_type is WireType.EPOCH) != (self.unit is not None):
            raise ValueError(
                f"field {self.wire_key!r}: an epoch needs a unit and any other type has none "
                f"(type {self.wire_type}, unit {self.unit})"
            )


@dataclass(frozen=True, kw_only=True)
class Membership:
    """A source's place in one dataset kind.

    Every dataset of a kind gets the same merged column set, built from the sources holding a
    membership of that kind, in group order and then `position` order.

    Attributes:
        kind: The dataset kind, such as `"trades"`.
        group: The asset-class group within the kind, such as `"stocks"`.
        position: The source's zero-based order within the group.
        projection: For this membership only, the wire keys to keep, each mapped to the
            PascalCase column it takes instead of its own (for example
            `{"last_updated": "Timestamp"}`). `None` keeps every field under its own column.
            The mapping is copied into a read-only view, so later changes to the caller's
            mapping have no effect. That view cannot be pickled or deep-copied, and neither
            can a membership with a projection; the values are built once from the spec and
            shared, never copied.

    Example:
        ::

            from deephaven_massive._massive.spec.types import Membership

            values = Membership(
                kind="values",
                group="indices",
                position=0,
                projection={"ticker": "Ticker", "value": "Value", "last_updated": "Timestamp"},
            )
    """

    kind: str
    group: str
    position: int
    projection: Mapping[str, str] | None = dataclasses.field(default=None, hash=False)

    def __post_init__(self) -> None:
        """Check the position and freeze a copy of the projection.

        Raises:
            ValueError: If `position` is negative or `projection` is empty.
        """
        if self.position < 0:
            raise ValueError(f"membership {self.kind}/{self.group}: negative position")
        if self.projection is not None:
            if not self.projection:
                raise ValueError(f"membership {self.kind}/{self.group}: empty projection")
            object.__setattr__(self, "projection", MappingProxyType(dict(self.projection)))


@dataclass(frozen=True, kw_only=True)
class Param:
    """One parameter of a REST path, reached as an element of `RestPath.params`.

    Attributes:
        name: The wire name, dots included, such as `"timestamp.gte"` or `"stockTicker"`.
        kind: The kind of value the parameter takes.
        in_path: True for a placeholder of the path template, False for a query parameter.
        values: The accepted values of an `ENUM` parameter, in spec order; empty otherwise.
    """

    name: str
    kind: ParamKind
    in_path: bool = False
    values: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Check that values are given exactly for an enum.

        Raises:
            ValueError: If `kind` is `ENUM` and `values` is empty, or the reverse.
        """
        if (self.kind is ParamKind.ENUM) != bool(self.values):
            raise ValueError(
                f"parameter {self.name!r}: an enum needs values and any other kind has none"
            )


def _duplicates(names: Iterable[str]) -> list[str]:
    """Return the names that occur more than once.

    Args:
        names: The names to check.

    Returns:
        Each repeated name once, sorted; empty when all are distinct.
    """
    return sorted(name for name, count in Counter(names).items() if count > 1)


@dataclass(frozen=True, kw_only=True)
class Source:
    """A REST path, WebSocket channel, or flat-file prefix, and the fields its rows carry.

    Attributes:
        key: The source's identity within its transport: the path template for REST (as
            `RestPath.template` also gives it), `"<cluster>/<channel>"` for WebSocket (such as
            `"stocks/T"`, since stocks, options, and futures share channel names), and the S3
            prefix for flat files.
        transport: The transport the source is served on.
        fields: The fields, in the vendor's order. No two share a wire key or a column.
        memberships: The dataset kinds the source belongs to; empty for a source in no kind.
            At most one per (kind, group). A projection names only wire keys of `fields` and
            maps no two of them to one column.

    Example:
        ::

            from deephaven_massive._massive.spec.types import Field, Source, Transport, WireType

            source = Source(
                key="us_stocks_sip/trades_v1",
                transport=Transport.FLAT_FILES,
                fields=(Field(wire_key="price", column="Price", wire_type=WireType.DOUBLE),),
            )
            assert source.field("Price").wire_key == "price"
    """

    key: str
    transport: Transport
    fields: tuple[Field, ...]
    memberships: tuple[Membership, ...] = ()

    def __post_init__(self) -> None:
        """Check the invariants of the source and, for a `RestPath`, of its parameters.

        `RestPath` adds no `__post_init__` of its own: overriding one would need
        `typing.override`, which Python 3.11 lacks and this standard-library-only module
        cannot import from elsewhere. Its checks run here instead.

        Raises:
            ValueError: If two fields share a wire key or a column; two memberships share a
                kind and group; a projection names a wire key the source lacks or maps two
                keys to one column; or, for a `RestPath`, two parameters share a name or the
                path parameters differ from the template's placeholders. The message names
                the source and the offending names.
        """
        wire_keys = [f.wire_key for f in self.fields]
        checks = [
            ("repeated wire keys", _duplicates(wire_keys)),
            ("repeated columns", _duplicates(f.column for f in self.fields)),
            ("repeated memberships", _duplicates(f"{m.kind}/{m.group}" for m in self.memberships)),
        ]
        for m in self.memberships:
            if m.projection is not None:
                where = f"projection of {m.kind}/{m.group}"
                checks.append(
                    (f"{where} names unknown wire keys", sorted(set(m.projection) - set(wire_keys)))
                )
                checks.append((f"{where} repeats columns", _duplicates(m.projection.values())))
        if isinstance(self, RestPath):
            in_path = {p.name for p in self.params if p.in_path}
            placeholders = {name for _, name, _, _ in string.Formatter().parse(self.key) if name}
            checks += [
                ("repeated parameters", _duplicates(p.name for p in self.params)),
                ("placeholders without a path parameter", sorted(placeholders - in_path)),
                ("path parameters without a placeholder", sorted(in_path - placeholders)),
            ]
        for what, names in checks:
            if names:
                raise ValueError(f"source {self.key!r}: {what} {names}")

    def field(self, column: str) -> Field:
        """Return the field named `column`, for example to read its unit.

        Args:
            column: The long-form column name, such as `"SipTimestamp"`.

        Returns:
            The field whose `column` equals `column`.

        Raises:
            KeyError: If the source has no such column; the message names the source.
        """
        for candidate in self.fields:
            if candidate.column == column:
                return candidate
        raise KeyError(f"source {self.key!r} has no column {column!r}")


@dataclass(frozen=True, kw_only=True)
class RestPath(Source):
    """A REST source: a path template with its result key and parameters.

    The REST client reads everything it needs to request and decode a path from this value.
    `transport` is always `Transport.REST` and cannot be passed.

    Attributes:
        result_key: The envelope key that holds the rows; `"results"` unless the path keeps
            them elsewhere (for example `"last"`).
        params: The path's parameters, path placeholders and query parameters alike, in
            spec order.

    Example:
        ::

            from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath

            trades = RestPath(
                key="/v3/trades/{stockTicker}",
                fields=(),
                params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
            )
            assert trades.template == "/v3/trades/{stockTicker}"
    """

    transport: Transport = dataclasses.field(default=Transport.REST, init=False)
    result_key: str = "results"
    params: tuple[Param, ...] = ()

    @property
    def template(self) -> str:
        """The path template, with `{name}` placeholders; the same text as `key`."""
        return self.key


@dataclass(frozen=True, kw_only=True)
class DriftReport:
    """How one payload differed from its source's fields.

    A decoder returns one per REST page, WebSocket frame, or flat-file block. It never raises
    for drift; the library decides what to log and show.

    Attributes:
        extra: Wire keys present in the payload that the source does not declare (dropped).
        missing: Wire keys the source declares that the payload lacked (decoded as null).
    """

    extra: frozenset[str] = frozenset()
    missing: frozenset[str] = frozenset()
