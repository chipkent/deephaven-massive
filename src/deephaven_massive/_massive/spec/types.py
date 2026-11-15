"""The value types that describe Massive's wire format.

A **source** is one thing Massive serves rows from: a REST path (`/v3/trades/{stockTicker}`),
a WebSocket channel of one cluster (`stocks/T`), or a flat-file prefix
(`us_stocks_sip/trades_v1`). Each is described by
its ordered **fields**: the key on the wire, the long-form column the library names it, the
field's `WireType`, its timestamp `WireUnit` when it is an epoch, and whether the vendor marks
it required. A REST source is a `RestPath`, which adds its result key and its parameters.

A source may belong to dataset **kinds** (trades, quotes, bars, ...): the sources whose rows
merge into one table schema. Each `Membership` names the kind, the asset-class group within
it, the source's position in that group, and optionally a projection: the fields the source
contributes to that membership, each under its column there, which may keep only some fields or
keep them all and rename some.

Decoders compare each payload with its source and return a `DriftReport` of the keys that
were extra or missing.

`Provenance` records where the generated values came from: the day the vendor sources were
fetched, the vendor URL of each source, and what each vendor page says about Massive's plans:
which plans include the data, how recent it is on each, and how far back its history goes
(`PlanAccess`, one `PlanTerms` per plan).

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

from typing_extensions import override

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable, Mapping, Sequence


__all__ = [
    "ENVELOPE_KEYS",
    "NANOS_PER_SECOND",
    "SECONDS_PER_DAY",
    "Conversion",
    "DriftReport",
    "DriftSample",
    "Field",
    "Membership",
    "Param",
    "ParamKind",
    "PlanAccess",
    "PlanTerms",
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

    Defined once, here, and used on both sides of the wire boundary: the library names its
    dataset sources and its drift reports by it. Its values are internal names; the library
    spells a transport for its users with labels of its own (`WebSocket`, `REST`,
    `flat files`).
    """

    WS = "WS"
    REST = "REST"
    FLAT_FILES = "FLAT_FILES"


class WireType(StrEnum):
    """The type of a field's values on the wire, after any named conversion.

    The refresh assigns each field one of these. Two sources that share a column within a
    dataset kind may differ only by a widening (`LONG` with `DOUBLE` gives `DOUBLE`, `LONG`
    with `LONG_ARRAY` gives `LONG_ARRAY`, by `common_type`); any other pair is a conflict the
    refresh rejects.
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
    LONG_ARRAY = "long-array"
    """A list of integers, each within the signed 64-bit range (condition codes, indicators).

    Every numeric id or code Massive sends is a `long`, alone or in a list, so a list of codes
    joins the reference table of its ids with no cast.
    """
    STRING_ARRAY = "string-array"
    """A list of text values, such as the tickers an article names."""
    JSON_TEXT = "json-text"
    """A nested object, a list of anything but integers or text, or a choice of shapes, kept as
    JSON text."""

    def common_type(self, other: WireType) -> WireType | None:
        """Return the narrowest wire type that holds values of both types, or None if none does.

        Two sources that share a column within a dataset kind must agree on its type, and this
        is the one rule for that agreement: the refresh's `type-agreement` gate and the
        run-time schema merge both call it, so a kind the refresh accepts always merges.
        Equal types give themselves (two `EPOCH`s each keep their own unit, so they agree
        too); `LONG` with `DOUBLE` gives `DOUBLE`, and `LONG` with `LONG_ARRAY` gives
        `LONG_ARRAY` (a scalar becomes a one-element list). Any other pair is a type conflict.

        Args:
            other: The other contribution's type.

        Returns:
            The common type, or None when the two conflict.
        """
        if self is other:
            return self
        return _COMMON_TYPES.get(frozenset({self, other}))


_COMMON_TYPES: Mapping[frozenset[WireType], WireType] = {
    frozenset({WireType.LONG, WireType.DOUBLE}): WireType.DOUBLE,
    frozenset({WireType.LONG, WireType.LONG_ARRAY}): WireType.LONG_ARRAY,
}
"""The common type of each pair of different wire types that has one; every other pair is a
type conflict."""


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
    MS_OR_NS = "ms|ns"
    """Milliseconds or nanoseconds, told apart value by value: a count of at least 10**17 is
    nanoseconds (any instant after 1973), a smaller one milliseconds (any instant before the
    year 3,170,000). For a field whose documentation and samples disagree on the two; never a
    parameter's unit."""


SECONDS_PER_DAY = 86_400
"""The seconds of a day in Unix time, which counts no leap second."""
NANOS_PER_SECOND = 1_000_000_000
"""The nanoseconds of a second."""


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
    commas in text, such as `"12,37"` (`long` to `long-array`)."""
    MIDNIGHT_UTC_DATE = "midnight-utc-date"
    """A date also sent as an ISO-8601 instant at exactly midnight with offset `Z` or `+00:00`
    (`date` to `date`)."""
    CHICAGO_MIDNIGHT_DATE = "chicago-midnight-date"
    """A date also sent as an integer count of nanoseconds since the epoch at exactly midnight
    in Chicago, giving that Chicago date (`date` to `date`)."""
    INTEGER_AS_NUMBER = "integer-as-number"
    """A quantity declared an integer whose values carry a fraction, such as a crypto volume
    `951.6112` (`long` to `double`); any number is accepted."""
    DECIMAL_TEXT_NUMBER = "decimal-text-number"
    """A number sent as a JSON number or as a finite decimal written as text, such as a futures
    price `"7676.5"` (`double` or `long` to `double`); text with anything but an optional minus
    sign, digits, and one decimal point between digits is refused."""

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
            Conversion.CODE_LIST_TEXT: (frozenset({WireType.LONG}), WireType.LONG_ARRAY, True),
            Conversion.MIDNIGHT_UTC_DATE: (frozenset({WireType.DATE}), WireType.DATE, True),
            Conversion.CHICAGO_MIDNIGHT_DATE: (frozenset({WireType.DATE}), WireType.DATE, True),
            Conversion.INTEGER_AS_NUMBER: (frozenset({WireType.LONG}), WireType.DOUBLE, True),
            Conversion.DECIMAL_TEXT_NUMBER: (
                frozenset({WireType.DOUBLE, WireType.LONG}),
                WireType.DOUBLE,
                True,
            ),
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
            collapsed to one space, or the refresh's correction where the vendor's text is
            clearly wrong for the source. `""`, the default, when the vendor gives none. It is
            documentation only: no name, type, or check is derived from it.
        class_descriptions: For a source that serves several asset classes (the unified
            snapshot), the description a dataset of one asset class shows in place of
            `description`, as `(asset class, description)` pairs sorted by asset class, each
            asset class an `AssetClass` value such as `"options"`. Empty, the default, for
            nearly every field. Read it through `description_for`.

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
    class_descriptions: tuple[tuple[str, str], ...] = ()

    def description_for(self, asset_class: str | None) -> str:
        """Return what the field means for a dataset of one asset class.

        Args:
            asset_class: The dataset's asset class, as an `AssetClass` value such as
                `"options"`, or `None` for a dataset that spans classes.

        Returns:
            The class's own description when `class_descriptions` gives one, else
            `description`.
        """
        return dict(self.class_descriptions).get(asset_class or "", self.description)

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
        default: The value Massive's specification documents as the parameter's default, as
            the JSON scalar it gives (`50` for an indicator's `window`, `"day"` for its
            `timespan`, `True` for `adjusted`); `None` when it documents none, or documents
            one that is not a scalar.
        maximum: The largest value Massive's specification documents for an integer
            parameter (its schema's `maximum`), such as `50000` for most paths' page-size
            `limit`; `None` when it documents none, as for the aggregates' `limit`, whose
            maximum only its prose states.
    """

    name: str
    kind: ParamKind
    in_path: bool = False
    values: tuple[str, ...] = ()
    unit: WireUnit | None = None
    default: str | int | float | bool | None = None
    maximum: int | None = None

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
        ignored_keys: Keys Massive sends at the top of a row or message that the source does
            not declare and the library deliberately does not read; a decoder drops them
            without reporting drift. None of them is a field's wire key.

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
    ignored_keys: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        """Check the invariants of the fields, the memberships, and the ignored keys.

        Raises:
            ValueError: If two fields share a wire key or a column, two memberships share a
                kind and group, a projection names a wire key the source lacks or maps two
                keys to one column, or an ignored key is a field's wire key. The message names
                the source and the offending names.
        """
        wire_keys = [f.wire_key for f in self.fields]
        checks = [
            ("ignored keys that are fields", sorted(self.ignored_keys.intersection(wire_keys))),
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

    def membership(self, kind: str | None, group: str | None) -> Membership | None:
        """Return the source's membership of one dataset kind and group, if it holds one.

        The one lookup of a source's place in a kind: a dataset's merge reads its projection
        and position, and the dataset catalog orders and checks a kind's members by it.

        Args:
            kind: The dataset kind, such as `"trades"`, or None for a dataset in no kind.
            group: The asset-class group, such as `"stocks"`, or None for no group.

        Returns:
            The membership whose kind and group these are; None when the source holds none,
            and always for a kind of None. A source holds at most one per kind and group:
            constructing it with two raises `ValueError`.
        """
        return next(
            (m for m in self.memberships if (m.kind, m.group) == (kind, group)),
            None,
        )


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
class PlanTerms:
    """What one Massive plan gets of the data one vendor page documents.

    Massive's documentation pages end in three tables, "Plan Access", "Plan Recency", and
    "Plan History", each with one row per plan; this is one plan's row of all three.

    Attributes:
        plan: The plan's name as Massive writes it, such as `"Stocks Starter"` or
            `"Stocks Business"`.
        access: Whether the plan includes the data, as Massive writes it: `"Included"` or
            `"Not included"`, usually.
        recency: How recent the data is on the plan, such as `"15-minute delayed"`,
            `"Real-time"`, `"End-of-day"`, or `"Not included"`; None when the page's recency
            table has no row for the plan.
        history: How far back the plan reaches, such as `"5 years"` or `"All history"`; None
            when the page's history table has no row for the plan (a WebSocket page has no
            history table at all).
    """

    plan: str
    access: str
    recency: str | None = None
    history: str | None = None


@dataclass(frozen=True, kw_only=True)
class PlanAccess:
    """What one vendor documentation page says about Massive's plans.

    The refresh reads it from the page's "Plan Access", "Plan Recency", and "Plan History"
    sections (`refresh.plan_access`) and stores it in `descriptions.json`, keyed by the page's
    URL; `Provenance.plans` carries it at run time, and the generated dataset reference renders
    it, so the reference follows Massive's own plan tables after every refresh.

    Attributes:
        access: The access summary line, such as `"Included in all Stocks plans"`.
        recency: The recency summary line, such as `"End-of-day, 15-minute delayed, or
            real-time"`.
        history: The history summary line, such as `"Records date back to September 10,
            2003"` or `"Not applicable to websockets"`.
        plans: One entry per plan, in the order the access table lists them (individual
            plans, then business plans).

    Example:
        ::

            from deephaven_massive._massive.spec.types import PlanAccess, PlanTerms

            bars = PlanAccess(
                access="Included in all Stocks plans",
                recency="End-of-day, 15-minute delayed, or real-time",
                history="Records date back to September 10, 2003",
                plans=(PlanTerms(plan="Stocks Basic", access="Included", history="2 years"),),
            )
    """

    access: str
    recency: str
    history: str
    plans: tuple[PlanTerms, ...]


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
        plans: What each vendor page says about Massive's plans, by the page's URL; a page
            without plan tables has no entry. Empty by default. Copied into a read-only view,
            like `urls`.
        titles: Each vendor page's title, its one `###` heading such as `"Custom Bars
            (OHLC)"`, by the page's URL: every page in `urls` has one. Empty by default. Copied
            into a read-only view, like `urls`.

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
    plans: Mapping[str, PlanAccess] = dataclasses.field(default_factory=dict, hash=False)
    titles: Mapping[str, str] = dataclasses.field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        """Freeze a copy of the URLs, each source's as a tuple, of the plans, and of the titles.

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
        object.__setattr__(self, "plans", MappingProxyType(dict(self.plans)))
        object.__setattr__(self, "titles", MappingProxyType(dict(self.titles)))


@dataclass(frozen=True)
class DriftSample:
    """The first row of a payload in which a drifted field was seen, for the drift's log line.

    Attributes:
        symbol: The row's symbol as the wire spells it, or None when the decoder does not know
            which key holds it (a REST page, whose symbol the request names, or a flat file).
        value: The field's value as `spec.convert.excerpt` quotes it (its JSON text, up to 200
            characters; a flat file's text as it was written) for an extra field or a value
            that does not fit; None for a missing one.
    """

    symbol: str | None
    value: str | None


@dataclass(frozen=True, kw_only=True)
class DriftReport:
    """How one payload differed from its source's fields, and the rows it could not place.

    A decoder returns one per REST page, WebSocket frame, or flat-file block. It never raises
    for drift; the library decides what to log and show. A value that does not fit its field
    is drift too: outside the key columns it is decoded as null, so one odd value never costs
    the rest of its row. A row whose key (its symbol, its time, or another key the dataset
    declares) is absent, null, or does not fit cannot be placed, so it is dropped and counted.

    Attributes:
        extra: Wire keys present in the payload that the source does not declare (dropped).
        missing: Wire keys of the vendor-`required` fields (`Field.required`) that some row
            lacked or held as null (decoded as null). Absent optional fields are not drift.
        unconvertible: Wire keys (for a flat file, headers) outside the key columns that held,
            in some row, a value that does not fit the field's type or its named conversion
            (decoded as null).
        rounded: Wire keys of `long` fields widened into a `double` column (by the lattice
            join, `WireType.common_type`) that held, in some row, a whole number beyond 2**53,
            which a `double` holds only rounded. The value is served rounded, never nulled.
            The decoders never set it: the widening happens after decoding, in the library.
        statuses: REST envelope `status` values outside the documented ones (`OK`,
            `DELAYED`, `success`, and the error statuses); the page's rows were still decoded.
            Always empty for WebSocket and flat files.
        dropped_rows: How many rows were dropped because a key was absent, null, or did not
            fit.
        dropped_reason: Why the first dropped row was dropped, naming the key and quoting its
            value, or `""` when no row was. Not part of equality, the hash, or the `repr`.
        samples: For each wire key in `extra`, `missing`, `unconvertible`, or `rounded`, the
            first row that showed it (`DriftSample`), so a drift WARNING can quote a symbol and
            a value; a key may have none (a flat-file header the file lacks). Read-only. Not
            part of equality, the hash, or the `repr`: two reports of the same drift are equal
            whichever rows showed it.

    Example:
        ::

            from deephaven_massive._massive import DriftReport

            page = DriftReport(unconvertible=frozenset({"x"}), dropped_rows=1)
            both = page.merge(DriftReport(dropped_rows=2))
            assert both.dropped_rows == 3 and both.fields_drifted
    """

    extra: frozenset[str] = frozenset()
    missing: frozenset[str] = frozenset()
    unconvertible: frozenset[str] = frozenset()
    rounded: frozenset[str] = frozenset()
    statuses: frozenset[str] = frozenset()
    dropped_rows: int = 0
    dropped_reason: str = dataclasses.field(default="", compare=False, hash=False, repr=False)
    samples: Mapping[str, DriftSample] = dataclasses.field(
        default_factory=dict, compare=False, hash=False, repr=False
    )

    def __post_init__(self) -> None:
        """Freeze a copy of the samples, so the report stays immutable."""
        object.__setattr__(self, "samples", MappingProxyType(dict(self.samples)))

    @property
    def fields_drifted(self) -> bool:
        """True when some field is extra, missing, held a value that does not fit, or rounded."""
        return bool(self.extra or self.missing or self.unconvertible or self.rounded)

    def merge(self, other: DriftReport) -> DriftReport:
        """Return the drift of two payloads read as one, such as the two parts of a REST page.

        Args:
            other: The later payload's report.

        Returns:
            The union of each set and the sum of the dropped rows. A key's sample and the
            dropped rows' reason are this report's where it has one, else the other's: the
            first row that showed a drift is the one a fix needs.
        """
        return DriftReport(
            extra=self.extra | other.extra,
            missing=self.missing | other.missing,
            unconvertible=self.unconvertible | other.unconvertible,
            rounded=self.rounded | other.rounded,
            statuses=self.statuses | other.statuses,
            dropped_rows=self.dropped_rows + other.dropped_rows,
            dropped_reason=self.dropped_reason or other.dropped_reason,
            samples={**other.samples, **self.samples},
        )
