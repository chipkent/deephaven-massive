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

`Provenance` records where the generated values came from: the day the vendor sources were
fetched and the vendor URL of each source.

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
from typing import TYPE_CHECKING, TypeVar

_F = TypeVar("_F")

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable, Mapping, Sequence

    from typing_extensions import override
else:

    def override(method: _F) -> _F:
        """Return `method` unchanged: `typing.override` exists only from Python 3.12.

        Type checkers read `typing_extensions.override` instead, from its bundled stub, so
        overrides stay checked without a run-time dependency.
        """
        return method


__all__ = [
    "ENVELOPE_KEYS",
    "Conversion",
    "DriftReport",
    "Field",
    "Membership",
    "Param",
    "ParamKind",
    "Provenance",
    "RestPath",
    "Source",
    "Transport",
    "WireType",
    "WireUnit",
]


ENVELOPE_KEYS: frozenset[str] = frozenset({"status", "request_id", "next_url", "count"})
"""The keys of Massive's REST envelope that describe a response, never a row.

A path whose body is a single JSON object that is both the envelope and the one row
(`RestPath.result_key` is `None`: `/v1/open-close/{stocksTicker}/{date}`, `/v1/conversion/...`,
`/v1/marketstatus/now`) carries some of these beside the row's own keys. The REST decoder
removes them from such a row before it compares the row with the path's fields, and the refresh
generates no field for them on such a path. A list body is never an envelope: its rows keep
every key, `status` included (`/v1/marketstatus/upcoming`).
"""


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
    """The type of a field's values on the wire, after any named conversion.

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
    """An instant as ISO-8601 date-time text with seconds and an offset (`Z` or `±HH:MM`).

    Decoded to exact integer nanoseconds since the Unix epoch (UTC), keeping up to nine
    fractional digits. Text without an offset is an unconvertible value, never assumed UTC.
    """
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
    """Date or instant; an instant is sent as an exact integer count of the parameter's `unit`."""
    LIST = "list"
    """A sequence of strings, sent joined with commas (the `.any_of` parameters)."""


class Conversion(StrEnum):
    """A named, deliberate conversion of a field whose values the vendor's type does not describe.

    The vendor's specification gives each field a type. Where the documented samples show the
    wire differs, the refresh records one of these on the field (`Field.conversion`) instead of
    loosening the plain converter of any type. Each conversion has one meaning, fixed here: the
    types the specification may declare for such a field (`declarations`), the type the field
    gets (`wire_type`), whether it accepts a form the declared type refuses (`widens`) or only
    retypes values the declared type already accepts, and the forms it accepts (the converters
    live in `spec.convert`). No conversion is a no-op: each one widens or retypes.
    """

    ISO_TEXT_INSTANT = "iso-text-instant"
    """Text the specification leaves untyped that is an ISO-8601 instant (`string` to
    `iso-instant`); it retypes text."""
    ISO_DATE_TEXT = "iso-date-text"
    """Text declared a date-time that is a date, `YYYY-MM-DD` (`iso-instant` to `date`)."""
    QUOTED_INTEGER = "quoted-integer"
    """An integer the specification declares and the vendor's sample sends as decimal digits
    in text, such as `"44"` (`long` to `long`): either form is accepted, and text with
    anything but digits (`"4.8"`) is refused."""
    TEXT_OR_INTEGER_ID = "text-or-integer-id"
    """An identifier sent both as text and as an integer, declared either way; either form is
    kept as text (`long` or `string` to `string`)."""
    NUMBER_LABEL = "number-label"
    """A label declared a number but sent as text, such as the version `"1.0"`; text is kept,
    and a finite number becomes its decimal text (`double` to `string`)."""
    CODE_LIST_TEXT = "code-list-text"
    """Codes documented as an integer but written as one integer or as integers separated by
    commas in text, such as `"12,37"` (`long` to `int-array`)."""
    MIDNIGHT_UTC_DATE = "midnight-utc-date"
    """A date also sent as an ISO-8601 instant at exactly midnight with offset `Z` or `+00:00`
    (`date` to `date`)."""
    DATE_AS_EPOCH = "date-as-epoch"
    """A date sent as an integer epoch count in the field's unit (`date` to `epoch`)."""
    INTEGER_AS_NUMBER = "integer-as-number"
    """A quantity declared an integer whose values carry a fraction, such as a crypto volume
    `951.6112` (`long` to `double`); any number is accepted."""

    @property
    def declarations(self) -> frozenset[WireType]:
        """The types the vendor's specification may give a field that takes this conversion."""
        return _CONVERSION_TYPES[self][0]

    @property
    def wire_type(self) -> WireType:
        """The type a field that takes this conversion has."""
        return _CONVERSION_TYPES[self][1]

    @property
    def widens(self) -> bool:
        """Whether it accepts a form the declared type refuses, rather than only retyping.

        A widening conversion is needed only while a documented sample is in that form; a
        retyping one is needed for as long as the specification declares the type.
        """
        return _CONVERSION_TYPES[self][2]


# Each conversion's declarations, resulting wire type, and whether it widens.
_CONVERSION_TYPES: Mapping[Conversion, tuple[frozenset[WireType], WireType, bool]] = (
    MappingProxyType(
        {
            Conversion.ISO_TEXT_INSTANT: (
                frozenset({WireType.STRING}),
                WireType.ISO_INSTANT,
                False,
            ),
            Conversion.ISO_DATE_TEXT: (frozenset({WireType.ISO_INSTANT}), WireType.DATE, True),
            Conversion.QUOTED_INTEGER: (frozenset({WireType.LONG}), WireType.LONG, True),
            Conversion.TEXT_OR_INTEGER_ID: (
                frozenset({WireType.LONG, WireType.STRING}),
                WireType.STRING,
                True,
            ),
            Conversion.NUMBER_LABEL: (frozenset({WireType.DOUBLE}), WireType.STRING, True),
            Conversion.CODE_LIST_TEXT: (frozenset({WireType.LONG}), WireType.INT_ARRAY, True),
            Conversion.MIDNIGHT_UTC_DATE: (frozenset({WireType.DATE}), WireType.DATE, True),
            Conversion.DATE_AS_EPOCH: (frozenset({WireType.DATE}), WireType.EPOCH, True),
            Conversion.INTEGER_AS_NUMBER: (frozenset({WireType.LONG}), WireType.DOUBLE, True),
        }
    )
)


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
        conversion: The named conversion the decoder applies to each value, set only where the
            vendor's documented samples show values its declared type does not describe (a
            timestamp left as plain text whose samples are ISO-8601 date-times, an identifier
            sent both as text and as an integer). Its `wire_type` is the field's. `None`, the
            default, means each value is converted by `wire_type` alone; a value that does not
            convert fails like any other bad value either way.
        description: What the vendor says the field means, as plain text: its documented
            description with every link written `text (url)` and every run of whitespace
            collapsed to one space. `""`, the default, when the vendor gives none. It is
            documentation only: no name, type, or check is derived from it.

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
    conversion: Conversion | None = None
    description: str = ""

    def __post_init__(self) -> None:
        """Check that the unit is set exactly for an epoch, and that a conversion fits.

        Raises:
            ValueError: If `wire_type` is `EPOCH` and `unit` is `None`, or the reverse; or if
                `conversion` produces another type than `wire_type`.
        """
        if (self.wire_type is WireType.EPOCH) != (self.unit is not None):
            raise ValueError(
                f"field {self.wire_key!r}: an epoch needs a unit and any other type has none "
                f"(type {self.wire_type}, unit {self.unit})"
            )
        if self.conversion is not None and self.conversion.wire_type is not self.wire_type:
            raise ValueError(
                f"field {self.wire_key!r}: the conversion {self.conversion} gives "
                f"{self.conversion.wire_type}, not {self.wire_type}"
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
        unit: The unit an instant is sent in, set exactly when `kind` is `TIMESTAMP`: `NS` for
            the trade and quote filters, `MS` for the indicator filters. An instant is sent as
            an exact integer count of it.
    """

    name: str
    kind: ParamKind
    in_path: bool = False
    values: tuple[str, ...] = ()
    unit: WireUnit | None = None

    def __post_init__(self) -> None:
        """Check that values are given exactly for an enum, and a unit exactly for a timestamp.

        Raises:
            ValueError: If `kind` is `ENUM` and `values` is empty, or the reverse; or if `kind`
                is `TIMESTAMP` and `unit` is `None`, or the reverse.
        """
        if (self.kind is ParamKind.ENUM) != bool(self.values):
            raise ValueError(
                f"parameter {self.name!r}: an enum needs values and any other kind has none"
            )
        if (self.kind is ParamKind.TIMESTAMP) != (self.unit is not None):
            raise ValueError(
                f"parameter {self.name!r}: a timestamp needs a unit and any other kind has none"
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
        """Check the invariants of the fields and the memberships.

        Raises:
            ValueError: If two fields share a wire key or a column, two memberships share a
                kind and group, or a projection names a wire key the source lacks or maps two
                keys to one column. The message names the source and the offending names.
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
        result_key: The envelope key that holds the rows: `"results"` unless the path keeps
            them elsewhere (for example `"last"`). A dotted key walks into nested objects
            (`"results.values"` for the indicators). `None` when the body itself is the rows:
            a JSON object is one row (`/v1/marketstatus/now`), from which the
            `ENVELOPE_KEYS` are removed, and a JSON list is the rows
            (`/v1/marketstatus/upcoming`).
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
    result_key: str | None = "results"
    params: tuple[Param, ...] = ()

    @override
    def __post_init__(self) -> None:
        """Check the source's invariants, then the parameters against the template.

        Raises:
            ValueError: If the source's invariants fail (see `Source`), two parameters share a
                name, or the path parameters differ from the template's placeholders; the
                message names the path and the offending names.
        """
        super().__post_init__()
        in_path = {p.name for p in self.params if p.in_path}
        placeholders = {name for _, name, _, _ in string.Formatter().parse(self.key) if name}
        for what, names in (
            ("repeated parameters", _duplicates(p.name for p in self.params)),
            ("placeholders without a path parameter", sorted(placeholders - in_path)),
            ("path parameters without a placeholder", sorted(in_path - placeholders)),
        ):
            if names:
                raise ValueError(f"source {self.key!r}: {what} {names}")

    @property
    def template(self) -> str:
        """The path template, with `{name}` placeholders; the same text as `key`."""
        return self.key


@dataclass(frozen=True, kw_only=True)
class Provenance:
    """Where the generated sources came from: the vendor snapshot's date and each source's URLs.

    `spec.provenance()` returns the one value read from the generated files; tests build their
    own. A generated document cites it, so that a reader can check a column against the
    vendor's own documentation and see how old that documentation was.

    Attributes:
        fetched: The day the vendor sources the schemas were generated from were downloaded.
        urls: Each generated source, by its transport and key, mapped to the vendor URLs that
            document it, sorted and as fetched. For REST these are the documentation pages
            that name the path: one for most paths, one per asset class for a path several
            share (`/v3/reference/tickers` has a stocks page, an options page, and so on). The
            mapping is copied into a read-only view of tuples, so later changes to the caller's
            mapping have no effect; like a membership's projection, that view cannot be
            pickled or deep-copied.

    Example:
        ::

            import datetime as dt

            from deephaven_massive._massive.spec.types import Provenance, Transport

            trades = (Transport.REST, "/v3/trades/{stockTicker}")
            provenance = Provenance(
                fetched=dt.date(2026, 10, 27),
                urls={trades: ["https://massive.com/docs/rest/stocks/trades-quotes/trades.md"]},
            )
            assert provenance.urls[trades][0].endswith("/trades.md")
    """

    fetched: dt.date
    urls: Mapping[tuple[Transport, str], Sequence[str]] = dataclasses.field(hash=False)

    def __post_init__(self) -> None:
        """Freeze a copy of the URLs, each source's as a tuple.

        Raises:
            TypeError: If a source's URLs are one string rather than a sequence of strings,
                which would otherwise be taken apart character by character.
        """
        strings = sorted(
            f"{t}:{k}" for (t, k), found in self.urls.items() if isinstance(found, str)
        )
        if strings:
            raise TypeError(f"the URLs of {strings} are a string, not a sequence of strings")
        frozen = {key: tuple(found) for key, found in self.urls.items()}
        object.__setattr__(self, "urls", MappingProxyType(frozen))


@dataclass(frozen=True, kw_only=True)
class DriftReport:
    """How one payload differed from its source's fields.

    A decoder returns one per REST page, WebSocket frame, or flat-file block. It never raises
    for drift; the library decides what to log and show.

    Attributes:
        extra: Wire keys present in the payload that the source does not declare (dropped).
        missing: Wire keys of the vendor-`required` fields (`Field.required`) that some row
            lacked or held as null (decoded as null). Absent optional fields are not drift.
        statuses: REST envelope `status` values outside the documented ones (`OK`,
            `DELAYED`, `success`, and the error statuses); the page's rows were still decoded.
            Always empty for WebSocket and flat files.
    """

    extra: frozenset[str] = frozenset()
    missing: frozenset[str] = frozenset()
    statuses: frozenset[str] = frozenset()
