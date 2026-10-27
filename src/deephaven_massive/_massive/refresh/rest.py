"""Generate the REST sources from the snapshot: every OpenAPI path to a `RestPath`, or a problem.

`generate_rest` reads the snapshot's specification and REST pages and returns one `RestPath`
per generated path, with an account of every path (`Coverage`), or raises `RefreshError` with
every problem found. Nothing here touches the network or a file.

**Which paths are generated.** Every path of the specification is exactly one of:

- *deprecated* (`x-polygon-deprecation` on its operation, or `deprecated: true`) or *draft*
  (`x-polygon-draft`): accounted for by the vendor and never generated;
- *excluded*: named by an `exclusions.EXCLUSIONS` entry, with a reason;
- *generated*: everything else. `x-polygon-ignore` is the vendor's hint to its own SDK
  generator not to emit a second client method for a URL; it says nothing about the
  documentation, so it neither excludes nor generates a path.

Every generated path must be named by a documentation page, and every documented endpoint must
be in the specification (the `docs` gate).

**Rows and fields.** A path's rows sit under its result key: `results` unless
`overrides.RESULT_KEYS` names another key, a dotted path of keys (`results.values`), or `None`
for a body that is itself the rows. A JSON-object body without a result key is one row, and
the envelope keys (`spec.types.ENVELOPE_KEYS`) are left out of its fields. Within a row,
`$ref` is resolved, `allOf` is merged, a `oneOf` or `anyOf` with one branch is that branch
(`openapi`), and nested objects are flattened into dotted wire keys (`day.c`), in the order
the served document lists them (which is the vendor's documented order; Massive serves every
object's keys alphabetically, and the snapshot never re-sorts them).

**Representation rules** decide each field's `WireType`: an integer is `long`, or an `epoch`
when `overrides.UNITS` gives it a unit; a number is `double`; a boolean is `bool`; an array of
integers is `int-array`; an array of anything else, a free-form object, and a choice of two or
more shapes are `json-text`; a string with `format: date-time`, or no format and a name ending
in `_utc`, is an `iso-instant`; a string with `format: date`, or named `date` or `*_date`, is a
`date`; any other string is `string`. A `TYPE_FIXES` entry then replaces the type and records
the vendor's declared type on the field, so the decoder converts it deliberately.

**Names.** Each segment of a wire key is translated to Massive's long form: the vendor's own
long key as it is; a short key through the lexicon of the object it sits in, whose entries
cite the endpoint that spells the same field out; else an identity entry. The last segment is
replaced by its `NAMES` entry when Massive names the key nowhere (a name derived from the
field's description, which the entry quotes). `naming.column_name` joins the long names in
PascalCase: `prevDay.vw` becomes `PrevDayVwap`.

**Parameters** keep the specification's order and wire names. Their `ParamKind` follows the
same kind of mechanical rules: a name ending in `.any_of`, or an array, is `LIST` (sent
comma-joined, even when its values come from an `enum`); any other `enum` is `ENUM`;
booleans, integers, and numbers are themselves; a timestamp-like string parameter takes its
`PARAM_UNITS` entry, `TIMESTAMP` in its unit or `DATE` when it is "not an epoch"; a string
with `format: date` or `format: date-time`, a choice of string formats, or a base name
`date`, `*_date`, `*_utc`, or `as_of` is `DATE`; any other string is `STRING`.

The checks on one field or parameter run as the generator meets it; the checks that span
paths run afterwards, in `gates`. It imports the standard library and other `_massive`
modules only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import docs, gates, naming
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem, RefreshError
from deephaven_massive._massive.refresh.hand import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required, text
from deephaven_massive._massive.spec.types import (
    ENVELOPE_KEYS,
    Field,
    Param,
    ParamKind,
    RestPath,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json, JsonObject
    from deephaven_massive._massive.refresh.snapshot import Snapshot

__all__ = ["Coverage", "generate_rest"]

# The `x-polygon-go-type` names that tag an integer with a unit.
_TAG_UNITS: Final = {
    "INanoseconds": WireUnit.NS,
    "IMicroseconds": WireUnit.US,
    "IMilliseconds": WireUnit.MS,
}
# The vendor extensions a path item and its GET operation may carry. Any other `x-polygon-*`
# key is a change at Massive the refresh stops on rather than ignores.
_PATH_EXTENSIONS: Final = frozenset({"x-polygon-draft", "x-polygon-ignore"})
_OPERATION_EXTENSIONS: Final = frozenset(
    {
        "x-polygon-deprecation",
        "x-polygon-entitlement-allowed-limited-tickers",
        "x-polygon-entitlement-allowed-timeframes",
        "x-polygon-entitlement-data-type",
        "x-polygon-entitlement-market-type",
        "x-polygon-experimental",
        "x-polygon-paginate",
        "x-polygon-replaces",
    }
)
# The documentation pages that describe an area rather than one endpoint.
_OVERVIEW_PAGES: Final = frozenset({"overview", "quickstart"})
# The range and set suffixes of a filter parameter's wire name (`timestamp.gte`).
_FILTER_SUFFIXES: Final = frozenset({"gt", "gte", "lt", "lte", "any_of"})
# The types of the scalar schema types, for fields and for parameters.
_SCALAR_TYPES: Final = {
    "integer": WireType.LONG,
    "number": WireType.DOUBLE,
    "boolean": WireType.BOOL,
}
_SCALAR_KINDS: Final = {
    "integer": ParamKind.INTEGER,
    "number": ParamKind.NUMBER,
    "boolean": ParamKind.BOOLEAN,
}
# The schema type of each envelope key in Massive's envelope; its keys are `ENVELOPE_KEYS`.
_ENVELOPE_TYPES: Final = {
    "status": "string",
    "request_id": "string",
    "next_url": "string",
    "count": "integer",
}


@dataclass(frozen=True, kw_only=True)
class Coverage:
    """How every path of the specification is accounted for.

    Attributes:
        specified: Every path of the specification.
        generated: The paths with a generated `RestPath`.
        excluded: The paths named by an exclusion.
        deprecated: The paths the specification marks deprecated.
        draft: The paths the specification marks draft (and not deprecated).
        pages: Each path a REST documentation page names, mapped to that page's name.
    """

    specified: frozenset[str]
    generated: frozenset[str]
    excluded: frozenset[str]
    deprecated: frozenset[str]
    draft: frozenset[str]
    pages: Mapping[str, str]


@dataclass(frozen=True)
class _Raw:
    """A leaf of a flattened row before it is named and typed.

    Attributes:
        wire_key: The dotted key below the result key.
        schema: The leaf's resolved schema.
        required: Whether it and every object above it are required.
    """

    wire_key: str
    schema: JsonObject
    required: bool


class _PathGenerator:
    """Generate one path's `RestPath`, collecting its problems instead of stopping."""

    def __init__(self, spec: OpenApi, hand: HandInput, path: str) -> None:
        """Prepare to generate `path`.

        Args:
            spec: The specification.
            hand: The hand input.
            path: The path template.
        """
        self.spec = spec
        self.hand = hand
        self.path = path
        self.problems: list[Problem] = []
        self.wire_keys: set[str] = set()
        self.objects: set[str] = {""}
        self.timestamp_bases: set[str] = set()
        self.unit_bases: set[str] = set()
        self.identities: set[str] = set()
        self.concepts: dict[str, str] = {}

    def use(self) -> PathUse:
        """Return what the path consumed so far, for the checks that span paths.

        Returns:
            The wire keys, objects, `PARAM_UNITS` bases, identity keys, and concepts met.
        """
        return PathUse(
            wire_keys=frozenset(self.wire_keys),
            objects=frozenset(self.objects),
            unit_bases=frozenset(self.unit_bases),
            identities=frozenset(self.identities),
            concepts=dict(self.concepts),
        )

    def problem(self, gate: Gate, key: str, message: str, *, description: str = "") -> None:
        """Record a problem of this path.

        Args:
            gate: The gate.
            key: The wire key or parameter.
            message: What is wrong.
            description: The vendor's description, if any.
        """
        self.problems.append(
            Problem(gate=gate, source=self.path, key=key, message=message, description=description)
        )

    def generate(self) -> RestPath | None:
        """Build the path's `RestPath`.

        A `RefreshError` from reading the specification on the way (a reference that cannot be
        followed, say) propagates; the problems recorded before it stay in `problems`.

        Returns:
            The value, or `None` when a problem prevents building it.
        """
        operation = self.spec.deref(
            self.spec.deref(self.spec.document["paths"], "paths").get(self.path), self.path
        ).get("get")
        if not isinstance(operation, dict):
            self.problem(Gate.SHAPE, "", "the path has no GET operation")
            return None
        result_key, rows = self.rows(operation)
        if rows is None:
            return None
        fields = [f for raw in rows if (f := self.field(raw)) is not None]
        self.check_columns(fields)
        params = self.parameters(operation)
        if len(self.problems) > 0:
            return None
        try:
            return RestPath(
                key=self.path, fields=tuple(fields), result_key=result_key, params=tuple(params)
            )
        except ValueError as error:
            self.problem(Gate.SHAPE, "", str(error))
            return None

    def body(self, operation: JsonObject) -> JsonObject | None:
        """Return the schema of the operation's HTTP 200 JSON response.

        Args:
            operation: The GET operation.

        Returns:
            The resolved schema, or `None` (with a problem) when there is none.
        """
        try:
            responses = self.spec.deref(operation.get("responses"), self.path)
            ok = self.spec.deref(responses.get("200"), self.path)
            content = self.spec.deref(ok.get("content"), self.path)
            media = self.spec.deref(content.get("application/json"), self.path)
            return self.spec.schema(media.get("schema"), self.path)
        except RefreshError:
            self.problem(Gate.SHAPE, "", "no JSON schema for the HTTP 200 response")
            return None

    def rows(self, operation: JsonObject) -> tuple[str | None, list[_Raw] | None]:
        """Find the rows of the response and flatten them.

        Args:
            operation: The GET operation.

        Returns:
            The result key and the flattened fields of one row, or the result key and `None`
            (with a problem) when the response has no rows where the result key says.
        """
        entry = self.hand.result_keys.get(self.path)
        result_key = entry.key if entry is not None else "results"
        body = self.body(operation)
        if body is None:
            return result_key, None
        node = body
        for step in result_key.split(".") if result_key is not None else ():
            found = properties(node)
            if found is None or step not in found:
                keys = sorted(found or {})
                gate = Gate.STALE_ENTRY if entry is not None else Gate.SHAPE
                self.problem(
                    gate,
                    "",
                    f"no {step!r} in the response object along result key {result_key!r} "
                    f"(keys {keys}); add or correct its RESULT_KEYS entry",
                )
                return result_key, None
            node = self.spec.schema(found[step], self.path)
        single_object = node.get("type") != "array"
        if not single_object:
            node = self.spec.schema(node.get("items"), self.path)
        if properties(node) is None:
            self.problem(Gate.SHAPE, "", f"the rows under {result_key!r} are not objects")
            return result_key, None
        out: list[_Raw] = []
        skip = ENVELOPE_KEYS if result_key is None and single_object else frozenset()
        self.flatten(node, "", is_required=True, out=out, skip=skip)
        return result_key, out

    def flatten(
        self,
        node: JsonObject,
        prefix: str,
        *,
        is_required: bool,
        out: list[_Raw],
        skip: frozenset[str],
    ) -> None:
        """Append the leaves of an object schema to `out`, nested objects flattened.

        Args:
            node: The resolved object schema.
            prefix: The dotted wire key of the object, with a trailing dot (`""` for the row).
            is_required: Whether the object itself is required.
            out: Where the leaves go.
            skip: Keys of this object to leave out (the envelope keys of a single-object
                body); each must have its envelope type.
        """
        required_here = required(node)
        for name, child in (properties(node) or {}).items():
            wire_key = prefix + name
            schema = self.spec.schema(child, f"{self.path} {wire_key}")
            if name in skip:
                self.check_envelope(name, schema)
                continue
            child_required = is_required and name in required_here
            if properties(schema) is not None:
                self.objects.add(wire_key)
                self.flatten(
                    schema, wire_key + ".", is_required=child_required, out=out, skip=frozenset()
                )
            else:
                self.wire_keys.add(wire_key)
                out.append(_Raw(wire_key, schema, child_required))

    def check_envelope(self, name: str, schema: JsonObject) -> None:
        """Check that an envelope key of a single-object body has the envelope's type.

        Args:
            name: The envelope key.
            schema: Its resolved schema.
        """
        expected = _ENVELOPE_TYPES[name]
        if schema.get("type") != expected:
            self.problem(
                Gate.ENVELOPE,
                name,
                f"an envelope key typed {schema.get('type')!r}, not {expected!r}: it may be "
                "row data that dropping it would lose",
                description=text(schema, "description"),
            )

    def long_names(self, raw: _Raw) -> list[str]:
        """Translate each segment of a wire key to its long name (lexicon, identity, NAMES).

        Args:
            raw: The flattened field.

        Returns:
            The long name of each segment; a short key that nothing translates is kept as is,
            with a `short-key` problem.
        """
        segments = raw.wire_key.split(".")
        names: list[str] = []
        for index, segment in enumerate(segments):
            prefix = ".".join(segments[:index])
            lexicon_name = self.hand.shapes.get((self.path, prefix))
            entries = self.hand.lexicons[lexicon_name].entries if lexicon_name else {}
            override = self.hand.names.get((self.path, raw.wire_key))
            if index == len(segments) - 1 and override is not None:
                quoted = " ".join(override.description.split())
                if quoted != " ".join(text(raw.schema, "description").split()):
                    self.problem(
                        Gate.STALE_ENTRY,
                        raw.wire_key,
                        "NAMES quotes a description the specification no longer gives",
                        description=text(raw.schema, "description"),
                    )
                coined = naming.column_name([override.long_name])
                if coined in naming.KEY_COLUMNS:
                    self.problem(
                        Gate.COLUMN_COLLISION,
                        raw.wire_key,
                        f"NAMES coins the dataset key column {coined!r}; only the dataset key "
                        "rename produces it",
                    )
                names.append(override.long_name)
            elif segment in entries:
                names.append(entries[segment].long_name)
            else:
                if segment in self.hand.identity:
                    self.identities.add(segment)
                elif naming.is_short(segment):
                    self.problem(
                        Gate.SHORT_KEY,
                        raw.wire_key,
                        f"short key {segment!r} has no lexicon, identity, or NAMES entry",
                        description=text(raw.schema, "description"),
                    )
                names.append(segment)
        return names

    def wire_type(self, raw: _Raw) -> WireType | None:
        """Apply the representation rules to a field, before units and type fixes.

        Args:
            raw: The flattened field.

        Returns:
            The field's type, or `None` (with a problem) for a schema with no usable type.
        """
        if raw.schema.get("type") == "array":
            items = self.spec.schema(raw.schema.get("items", {}), f"{self.path} {raw.wire_key}")
            return WireType.INT_ARRAY if items.get("type") == "integer" else WireType.JSON_TEXT
        found = _field_type(raw.schema, raw.wire_key.rsplit(".", 1)[-1])
        if found is None:
            self.problem(
                Gate.SHAPE, raw.wire_key, f"unsupported field type {raw.schema.get('type')!r}"
            )
        return found

    def field(self, raw: _Raw) -> Field | None:
        """Name and type one flattened field, checking its name, unit, and type fix.

        Args:
            raw: The flattened field.

        Returns:
            The generated field, or `None` when its type cannot be decided.
        """
        long_names = self.long_names(raw)
        wire_type = self.wire_type(raw)
        if wire_type is None:
            return None
        description = text(raw.schema, "description")
        fix = self.hand.type_fixes.get((self.path, raw.wire_key))
        unit = self.unit(
            raw, wire_type, long_name=long_names[-1], description=description, fixed=fix is not None
        )
        if unit is not None:
            wire_type = WireType.EPOCH
        declared = None
        if fix is not None and fix.declared is not wire_type:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"TYPE_FIXES expects the vendor type {fix.declared}, but the "
                f"specification now gives {wire_type}",
                description=description,
            )
        elif fix is not None:
            declared, wire_type = wire_type, fix.wire_type
        column = naming.column_name(long_names)
        self.concepts[column] = ".".join(naming.snake(name) for name in long_names)
        return Field(
            wire_key=raw.wire_key,
            column=column,
            wire_type=wire_type,
            unit=unit,
            required=raw.required,
            declared_type=declared,
        )

    def unit(
        self,
        raw: _Raw,
        wire_type: WireType,
        *,
        long_name: str,
        description: str,
        fixed: bool,
    ) -> WireUnit | None:
        """Decide a field's unit from `UNITS`, checking the timestamp unit and the unit tag.

        A field is timestamp-like when the specification tags it with a unit, or its long name
        is (`naming.is_timestamp_like`). A timestamp-like `long` or `double` needs a `UNITS`
        entry, and so does timestamp-like text with no `format`, unless a `TYPE_FIXES` entry
        types it: nothing timestamp-like is left to a default.

        Args:
            raw: The flattened field.
            wire_type: Its type by the representation rules.
            long_name: The long name of its last segment.
            description: The vendor's description.
            fixed: Whether a `TYPE_FIXES` entry names the field.

        Returns:
            The unit of an epoch, or `None` for any other field.
        """
        key = (self.path, raw.wire_key)
        entry = self.hand.units.get(key)
        tag = raw.schema.get("x-polygon-go-type")
        tag_name = tag.get("name") if isinstance(tag, dict) else None
        tag_unit = _TAG_UNITS.get(tag_name) if isinstance(tag_name, str) else None
        timestamp_like = tag_unit is not None or naming.is_timestamp_like(long_name)
        untyped_text = wire_type is WireType.STRING and "format" not in raw.schema
        gated = wire_type in {WireType.LONG, WireType.DOUBLE} or (untyped_text and not fixed)
        if entry is None:
            if timestamp_like and gated:
                self.problem(
                    Gate.TIMESTAMP_UNIT,
                    raw.wire_key,
                    f"timestamp-like {wire_type} (tag {tag_name}) has no UNITS entry"
                    + (" or TYPE_FIXES entry" if untyped_text else ""),
                    description=description,
                )
            return None
        if entry.unit is not None and wire_type is not WireType.LONG:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"UNITS gives a unit to a {wire_type} field; only an integer is an epoch",
                description=description,
            )
            return None
        if wire_type not in {WireType.LONG, WireType.DOUBLE, WireType.STRING}:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"UNITS names a {wire_type} field",
                description=description,
            )
            return None
        conflict = entry.unit is not None and tag_unit is not None and entry.unit is not tag_unit
        if conflict and key not in self.hand.tag_conflicts:
            self.problem(
                Gate.UNITS,
                raw.wire_key,
                f"UNITS says {entry.unit} but the specification tags {tag_name}; add a "
                "TAG_CONFLICTS entry with the reason the tag is wrong",
                description=description,
            )
        if not conflict and key in self.hand.tag_conflicts:
            self.problem(
                Gate.STALE_ENTRY, raw.wire_key, "TAG_CONFLICTS entry, but UNITS agrees with the tag"
            )
        return entry.unit

    def check_columns(self, fields: Sequence[Field]) -> None:
        """Report two fields with one column.

        Args:
            fields: The path's generated fields.
        """
        seen: dict[str, str] = {}
        for item in fields:
            if item.column in seen:
                self.problem(
                    Gate.COLUMN_COLLISION,
                    item.wire_key,
                    f"column {item.column!r} is also the column of {seen[item.column]!r}",
                )
            else:
                seen[item.column] = item.wire_key

    def parameters(self, operation: JsonObject) -> list[Param]:
        """Generate the path's parameters, checking the unit of timestamp-like strings.

        Args:
            operation: The GET operation.

        Returns:
            The parameters, path-item level first, then operation level, in spec order.
        """
        item = self.spec.deref(self.spec.document["paths"], "paths").get(self.path)
        nodes: list[Json] = []
        for owner in (self.spec.deref(item, self.path), operation):
            listed = owner.get("parameters", [])
            nodes += listed if isinstance(listed, list) else []
        params: list[Param] = []
        for node in nodes:
            param = self.parameter(self.spec.deref(node, self.path))
            if param is not None:
                params.append(param)
        return params

    def parameter(self, node: JsonObject) -> Param | None:
        """Generate one parameter by the parameter rules.

        Args:
            node: The resolved parameter object.

        Returns:
            The parameter, or `None` (with a problem) for one outside the path and query.
        """
        name = text(node, "name")
        location = node.get("in")
        description = text(node, "description")
        if location not in {"path", "query"}:
            self.problem(Gate.SHAPE, name, f"unsupported parameter location {location!r}")
            return None
        stem, _, suffix = name.rpartition(".")
        base = stem if suffix in _FILTER_SUFFIXES else name
        schema = self.spec.schema(node.get("schema", {}), f"{self.path} {name}")
        kind, values, unit = self.parameter_kind(name, base, schema, description=description)
        return Param(name=name, kind=kind, in_path=location == "path", values=values, unit=unit)

    def parameter_kind(
        self, name: str, base: str, schema: JsonObject, *, description: str
    ) -> tuple[ParamKind, tuple[str, ...], WireUnit | None]:
        """Decide a parameter's kind, enum values, and timestamp unit.

        The mechanical rules decide every parameter but a string one with a `PARAM_UNITS`
        entry, or a timestamp-like one (`naming.is_timestamp_like` of its base name). An entry
        with a unit makes it `TIMESTAMP` in that unit, "not an epoch" makes it `DATE`, and a
        timestamp-like one with no entry fails the `timestamp-unit` gate. A choice of schemas
        (`oneOf`, `anyOf`) must be a choice of date and date-time strings, which is `DATE`.

        Args:
            name: The wire name.
            base: The name without a filter suffix (`timestamp` for `timestamp.gte`).
            schema: The resolved schema.
            description: The vendor's description.

        Returns:
            The kind, the enum values (empty unless `ENUM`), and the unit (`None` unless
            `TIMESTAMP`).
        """
        for choice in ("oneOf", "anyOf"):
            branches = schema.get(choice)
            if isinstance(branches, list) and not all(
                self.spec.schema(branch, f"{self.path} {name}").get("format")
                in {"date", "date-time"}
                for branch in branches
            ):
                self.problem(
                    Gate.SHAPE, name, f"a {choice} parameter that is not a choice of date formats"
                )
        kind, values = _parameter_rule(name, base, schema)
        entry = self.hand.param_units.get((self.path, base))
        if schema.get("type") != "string" or kind is ParamKind.LIST:
            return kind, values, None
        if entry is None:
            if naming.is_timestamp_like(base) and base not in self.timestamp_bases:
                self.problem(
                    Gate.TIMESTAMP_UNIT,
                    base,
                    "timestamp-like parameter has no PARAM_UNITS entry",
                    description=description,
                )
            self.timestamp_bases.add(base)
            return kind, values, None
        self.unit_bases.add(base)
        if entry.unit is None:
            return ParamKind.DATE, (), None
        return ParamKind.TIMESTAMP, (), entry.unit


def _field_type(schema: JsonObject, last: str) -> WireType | None:
    """Apply the representation rules to a field that is not an array.

    Args:
        schema: The field's resolved schema.
        last: The last segment of its wire key, as the vendor spells it.

    Returns:
        The field's type, or `None` for a schema type the rules do not cover.
    """
    kind = schema.get("type")
    form = schema.get("format")
    if "oneOf" in schema or "anyOf" in schema or kind == "object":
        return WireType.JSON_TEXT
    if kind == "string":
        if form == "date-time" or (form is None and last.endswith("_utc")):
            return WireType.ISO_INSTANT
        dated = form == "date" or last == "date" or last.endswith("_date")
        return WireType.DATE if dated else WireType.STRING
    if kind == "number" and form == "integer":
        return WireType.LONG
    return _SCALAR_TYPES.get(kind) if isinstance(kind, str) else None


def _parameter_rule(name: str, base: str, schema: JsonObject) -> tuple[ParamKind, tuple[str, ...]]:
    """Apply the mechanical parameter rules.

    Args:
        name: The wire name.
        base: The name without a filter suffix.
        schema: The resolved schema.

    Returns:
        The kind and the enum values (empty unless `ENUM`). A timestamp-like string comes out
        as `STRING`; the caller decides it from `PARAM_UNITS`.
    """
    kind = schema.get("type")
    enum = schema.get("enum")
    if name.rpartition(".")[2] == "any_of" or kind == "array":
        return ParamKind.LIST, ()
    if isinstance(enum, list) and enum:
        return ParamKind.ENUM, tuple(_enum_text(value) for value in enum)
    if isinstance(kind, str) and kind in _SCALAR_KINDS:
        return _SCALAR_KINDS[kind], ()
    dated = base in {"date", "as_of"} or base.endswith(("_date", "_utc"))
    choice = "oneOf" in schema or "anyOf" in schema
    if schema.get("format") in {"date", "date-time"} or choice or dated:
        return ParamKind.DATE, ()
    return ParamKind.STRING, ()


def _enum_text(value: Json) -> str:
    """Return an enum value as the text it is sent as.

    Args:
        value: A value of a parameter's `enum` list.

    Returns:
        A string unchanged, a boolean as `true` or `false`, anything else as JSON.
    """
    return value if isinstance(value, str) else json.dumps(value)


def _status(item: JsonObject) -> str:
    """Classify one path item of the specification.

    Args:
        item: The resolved path item.

    Returns:
        `"deprecated"`, `"draft"`, or `""` for a path the vendor publishes as current.
    """
    operation = item.get("get")
    if isinstance(operation, dict) and (
        "x-polygon-deprecation" in operation or operation.get("deprecated") is True
    ):
        return "deprecated"
    return "draft" if item.get("x-polygon-draft") is True else ""


def _documented_pages(snapshot: Snapshot, problems: list[Problem]) -> dict[str, str]:
    """Map each path a REST documentation page names to the page.

    Args:
        snapshot: The snapshot.
        problems: Where a page that names an unreadable endpoint, or no endpoint while not
            being an overview, is reported.

    Returns:
        Path template to page name.
    """
    pages: dict[str, str] = {}
    for name, page in sorted(snapshot.pages.items()):
        try:
            path = docs.endpoint(page)
        except docs.DocsError as error:
            problems.append(Problem(gate=Gate.DOCS, source="", key=name, message=str(error)))
            continue
        if path is not None:
            pages.setdefault(path, name)
        elif PurePosixPath(name).stem not in _OVERVIEW_PAGES:
            problems.append(
                Problem(
                    gate=Gate.DOCS,
                    source="",
                    key=name,
                    message="a page that is not an overview names no endpoint",
                )
            )
    return pages


def _check_extensions(spec: OpenApi, paths: JsonObject) -> Iterator[Problem]:
    """Report the vendor extensions on a path item or operation that the refresh does not know.

    Args:
        spec: The specification.
        paths: Its `paths` object.

    Yields:
        One `shape` problem per unknown `x-polygon-*` key: a change at Massive whose meaning a
        person must decide before the refresh can account for the path.
    """
    for path, node in paths.items():
        item = spec.deref(node, path)
        operation = item.get("get")
        found = {key for key in item if key.startswith("x-polygon-")} - _PATH_EXTENSIONS
        if isinstance(operation, dict):
            found |= {key for key in operation if key.startswith("x-polygon-")} - (
                _OPERATION_EXTENSIONS
            )
        for key in sorted(found):
            yield Problem(gate=Gate.SHAPE, source=path, key=key, message="unknown vendor extension")


def generate_rest(
    snapshot: Snapshot, hand: HandInput | None = None
) -> tuple[tuple[RestPath, ...], Coverage]:
    """Generate every REST source of the snapshot and check every gate.

    Args:
        snapshot: The vendored sources.
        hand: The hand input; the real tables when omitted.

    Returns:
        The generated paths in specification order, and how every path is accounted for.

    Raises:
        RefreshError: With every problem found, if there is any.
    """
    hand = hand or HandInput()
    spec = OpenApi(snapshot.openapi)
    problems: list[Problem] = []
    pages = _documented_pages(snapshot, problems)
    paths = spec.deref(snapshot.openapi.get("paths"), "paths")
    statuses = {path: _status(spec.deref(item, path)) for path, item in paths.items()}
    problems += _check_extensions(spec, paths)
    problems += [
        Problem(
            gate=Gate.DOCS,
            source=path,
            key="",
            message=f"{page} documents a path the specification lacks",
        )
        for path, page in pages.items()
        if path not in statuses
    ]
    problems += gates.check_exclusions(hand, statuses, pages)
    excluded = {e.item for e in hand.exclusions if e.kind is Transport.REST} & set(statuses)
    uses: dict[str, PathUse] = {}
    generated: list[RestPath] = []
    for path, status in statuses.items():
        if status or path in excluded:
            continue
        generator = _PathGenerator(spec, hand, path)
        try:
            value = generator.generate()
        except RefreshError as error:
            problems += error.problems
            continue
        finally:
            # Kept even when the path stopped, so its hand entries are not also reported stale.
            uses[path] = generator.use()
            problems += generator.problems
        if value is not None:
            generated.append(value)
        if path not in pages:
            problems.append(
                Problem(
                    gate=Gate.DOCS,
                    source=path,
                    key="",
                    message="generated, but no documentation page names it; exclude it as "
                    "undocumented",
                )
            )
    problems += gates.stale_entries(hand, uses)
    problems += gates.concept_collisions(hand, uses)
    if problems:
        raise RefreshError(problems)
    coverage = Coverage(
        specified=frozenset(statuses),
        generated=frozenset(p.key for p in generated),
        excluded=frozenset(excluded),
        deprecated=frozenset(p for p, s in statuses.items() if s == "deprecated"),
        draft=frozenset(p for p, s in statuses.items() if s == "draft"),
        pages=pages,
    )
    return tuple(generated), coverage
