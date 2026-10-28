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

**Fields** are named, typed, given their units and conversions, and checked against the
response's example and each field's own `example` by the rules every transport shares
(`fields`). A path in a dataset kind is reordered afterwards, by `kinds`, to its kind's order.

**Parameters** keep the specification's order and wire names. Their `ParamKind` follows the
same kind of mechanical rules: a name ending in `.any_of`, or an array, is `LIST` (sent
comma-joined, even when its values come from an `enum`); any other `enum` is `ENUM`;
booleans, integers, and numbers are themselves; a timestamp-like string parameter takes its
`PARAM_UNITS` entry, `TIMESTAMP` in its unit or `DATE` when it is "not an epoch"; a string
with `format: date` or `format: date-time`, a choice of string formats, a base name `date`,
`*_date`, `*_utc`, or `as_of`, or a description that gives the date format `yyyy-mm-dd` (in
any letter case) is `DATE`; any other string is `STRING`. The description is the vendor's
evidence where the schema is a bare string: Massive types its date filters `{"type":
"string"}` and states the format only in prose ("Value must be formatted 'yyyy-mm-dd'"), on
every date-valued filter whatever the parameter's name.

The checks on one field or parameter run as the generator meets it; the checks that span
sources run afterwards, over every transport, in `generate`. It imports the standard library
and other `_massive` modules only.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import docs, gates, naming
from deephaven_massive._massive.refresh.fields import RawField, SourceFields, openapi_field
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem, RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required, text
from deephaven_massive._massive.spec.types import (
    ENVELOPE_KEYS,
    Field,
    Param,
    ParamKind,
    RestPath,
    Transport,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json, JsonObject
    from deephaven_massive._massive.refresh.snapshot import Snapshot

__all__ = ["Coverage", "generate_rest", "path_status"]

# The vendor extensions a path item and its GET operation may carry, each with what the
# generator does about it. Any other `x-polygon-*` key is a change at Massive the refresh stops
# on rather than ignores.
_PATH_EXTENSIONS: Final = MappingProxyType(
    {
        "x-polygon-draft": "The vendor has not published the path: never generated.",
        "x-polygon-ignore": "A hint to the vendor's own SDK generator not to emit a second "
        "client method for one URL; it says nothing about the data, so it changes nothing.",
    }
)
_OPERATION_EXTENSIONS: Final = MappingProxyType(
    {
        "x-polygon-deprecation": "The vendor is retiring the path: never generated.",
        "x-polygon-entitlement-allowed-limited-tickers": "Which plans see which tickers; the "
        "library learns entitlements from the responses, not from the specification.",
        "x-polygon-entitlement-allowed-timeframes": "Which plans see real-time or delayed "
        "data; learned from the responses, like every entitlement.",
        "x-polygon-entitlement-data-type": "The plan family the path belongs to; learned from "
        "the responses, like every entitlement.",
        "x-polygon-entitlement-market-type": "The market the plan covers; learned from the "
        "responses, like every entitlement.",
        "x-polygon-experimental": "The vendor marks the endpoint experimental but documents "
        "it; documented endpoints are generated, so it is generated like any other.",
        "x-polygon-paginate": "How the vendor's SDK pages; the REST client follows `next_url` "
        "on every path instead.",
        "x-polygon-replaces": "Names the older path a new one replaces; the older path's own "
        "deprecation flag accounts for it.",
    }
)
# The documentation pages that describe an area rather than one endpoint: they name none.
_OVERVIEW_PAGES: Final = frozenset({"overview", "quickstart"})
# The range and set suffixes of a filter parameter's wire name (`timestamp.gte`).
_FILTER_SUFFIXES: Final = frozenset({"gt", "gte", "lt", "lte", "any_of"})
# The date format a parameter's description gives when its values are dates. Massive states it
# in prose ("Value must be formatted 'yyyy-mm-dd'") where the schema is a bare string.
_DATE_FORMAT: Final = re.compile(r"\byyyy-mm-dd\b", re.IGNORECASE)
# The kinds of the scalar schema types of a parameter.
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
        pages: Each path a REST documentation page names, mapped to the names of every
            page that names it, sorted (a path shared by several asset classes, such as
            `/v3/reference/tickers`, has one page per asset class).
        uses: What generating each attempted path consumed, for the checks that span sources.
    """

    specified: frozenset[str]
    generated: frozenset[str]
    excluded: frozenset[str]
    deprecated: frozenset[str]
    draft: frozenset[str]
    pages: Mapping[str, tuple[str, ...]]
    uses: Mapping[str, PathUse]


class _PathGenerator(SourceFields):
    """Generate one path's `RestPath`, collecting its problems instead of stopping.

    The field rules are `SourceFields`'s; this class adds what only a REST path has: the
    response body and its result key, the envelope, and the parameters.
    """

    def __init__(self, spec: OpenApi, hand: HandInput, path: str, *, origin: str) -> None:
        """Prepare to generate `path`.

        Args:
            spec: The specification.
            hand: The hand input.
            path: The path template.
            origin: The documentation origin a root-relative link in a description is
                resolved against (`plain_text`).
        """
        super().__init__(hand, path, origin=origin)
        self.spec = spec
        self.path = path
        self.timestamp_bases: set[str] = set()
        # The specification's example of the HTTP 200 response, once `body` has read it.
        self.example: Json = None

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
        self.check_example(result_key, fields)
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
            self.example = media.get("example")
            return self.spec.schema(media.get("schema"), self.path)
        except RefreshError:
            self.problem(Gate.SHAPE, "", "no JSON schema for the HTTP 200 response")
            return None

    def check_example(self, result_key: str | None, fields: Sequence[Field]) -> None:
        """Convert the response example's rows and each field's own example by the fields.

        The rows sit in the example where the result key says, as in a response
        (`SourceFields.check_examples` does the rest).

        Args:
            result_key: Where the example's rows sit, as for the response itself.
            fields: The path's generated fields.
        """
        node = self.example
        for step in result_key.split(".") if result_key is not None else ():
            node = node.get(step) if isinstance(node, dict) else None
        rows = node if isinstance(node, list) else [node]
        self.check_examples(
            fields,
            rows,
            documented=self.example is not None,
            where=f"under result key {result_key!r}",
        )

    def rows(self, operation: JsonObject) -> tuple[str | None, list[RawField] | None]:
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
        out: list[RawField] = []
        skip = ENVELOPE_KEYS if result_key is None and single_object else frozenset()
        self.flatten(node, "", is_required=True, out=out, skip=skip)
        return result_key, out

    def flatten(
        self,
        node: JsonObject,
        prefix: str,
        *,
        is_required: bool,
        out: list[RawField],
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
                out.append(
                    openapi_field(
                        self.spec,
                        wire_key,
                        schema,
                        required=child_required,
                        where=f"{self.path} {wire_key}",
                    )
                )

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
        kind, values = _parameter_rule(name, base, schema, description=description)
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


def _parameter_rule(
    name: str, base: str, schema: JsonObject, *, description: str
) -> tuple[ParamKind, tuple[str, ...]]:
    """Apply the mechanical parameter rules.

    Args:
        name: The wire name.
        base: The name without a filter suffix.
        schema: The resolved schema.
        description: The vendor's description; a string whose description gives the date
            format `yyyy-mm-dd` is a date.

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
    dated = (
        base in {"date", "as_of"}
        or base.endswith(("_date", "_utc"))
        or _DATE_FORMAT.search(description) is not None
    )
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


def path_status(item: JsonObject) -> str:
    """Classify one path item of the specification.

    The generator decides with it which paths it generates, and the drift check which paths
    the vendor has newly deprecated. The vendor marks WebSocket channels the same way, so the
    drift check classifies them with it too.

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


def _documented_pages(snapshot: Snapshot, problems: list[Problem]) -> dict[str, tuple[str, ...]]:
    """Map each path a REST documentation page names to every page that names it.

    Args:
        snapshot: The snapshot.
        problems: Where a page that names an unreadable endpoint, or no endpoint while not
            being an overview, is reported.

    Returns:
        Path template to page names, sorted.
    """
    pages: dict[str, tuple[str, ...]] = {}
    for name, page in sorted(snapshot.pages.items()):
        try:
            path = docs.endpoint(page)
        except docs.DocsError as error:
            problems.append(Problem(gate=Gate.DOCS, source="", key=name, message=str(error)))
            continue
        if path is not None:
            pages[path] = (*pages.get(path, ()), name)
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
        found = {key for key in item if key.startswith("x-polygon-")} - _PATH_EXTENSIONS.keys()
        if isinstance(operation, dict):
            found |= {key for key in operation if key.startswith("x-polygon-")} - (
                _OPERATION_EXTENSIONS.keys()
            )
        for key in sorted(found):
            yield Problem(gate=Gate.SHAPE, source=path, key=key, message="unknown vendor extension")


def generate_rest(
    snapshot: Snapshot, hand: HandInput | None = None
) -> tuple[tuple[RestPath, ...], Coverage]:
    """Generate every REST source of the snapshot and check the gates met per path.

    The checks that span sources (stale hand entries, the dataset kinds) run afterwards, over
    every transport, in `generate`; `Coverage.uses` carries what they need.

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
    statuses = {path: path_status(spec.deref(item, path)) for path, item in paths.items()}
    problems += _check_extensions(spec, paths)
    problems += [
        Problem(
            gate=Gate.DOCS,
            source=path,
            key="",
            message=f"{', '.join(names)} documents a path the specification lacks",
        )
        for path, names in pages.items()
        if path not in statuses
    ]
    problems += gates.check_exclusions(hand, statuses, pages)
    excluded = {e.item for e in hand.exclusions if e.kind is Transport.REST} & set(statuses)
    uses: dict[str, PathUse] = {}
    generated: list[RestPath] = []
    for path, status in statuses.items():
        if status or path in excluded:
            continue
        generator = _PathGenerator(spec, hand, path, origin=snapshot.origin)
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
    if problems:
        raise RefreshError(problems)
    coverage = Coverage(
        specified=frozenset(statuses),
        generated=frozenset(p.key for p in generated),
        excluded=frozenset(excluded),
        deprecated=frozenset(p for p, s in statuses.items() if s == "deprecated"),
        draft=frozenset(p for p, s in statuses.items() if s == "draft"),
        pages=pages,
        uses=uses,
    )
    return tuple(generated), coverage
