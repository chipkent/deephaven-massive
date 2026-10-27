"""Generate `spec/rest.json` from the vendored snapshot, stopping at anything it cannot name.

This is the REST half of the refresh. `refresh()` reads the snapshot (`snapshot/openapi.json`,
`snapshot/llms.txt`, and the REST pages under `snapshot/rest/`), verifies every file against
`snapshot/manifest.json`, turns every generated OpenAPI
path into a `RestPath`, checks the result against the refresh gates, and writes
`spec/rest.json` in its canonical form. Nothing here touches the network.

**Which paths are generated.** Every path of the specification is exactly one of:

- *deprecated* (`x-polygon-deprecation` on its operation, or `deprecated: true`) or *draft*
  (`x-polygon-draft`): accounted for by the vendor and never generated;
- *excluded*: named by an `exclusions.EXCLUSIONS` entry, with a reason;
- *generated*: everything else. `x-polygon-ignore` is the vendor's hint to its own SDK
  generator not to emit a second client method for a URL; it says nothing about the
  documentation, so it neither excludes nor generates a path.

**Rows and fields.** A path's rows sit under its result key: `results` unless
`overrides.RESULT_KEYS` names another key, a dotted path of keys (`results.values`), or `None`
for a body that is itself the rows. A JSON-object body without a result key is one row, and
the envelope keys (`spec.types.ENVELOPE_KEYS`) are left out of its fields. Within a row,
`$ref` is resolved, `allOf` is merged, a `oneOf` or `anyOf` with one branch is that branch,
and nested objects are flattened into dotted wire keys (`day.c`), in the order the served
document lists them (which is the vendor's documented order; Massive serves every object's
keys alphabetically, and the snapshot never re-sorts them).

**Representation rules** decide each field's `WireType`: an integer is `long`, or an `epoch`
when `overrides.UNITS` gives it a unit; a number is `double`; a boolean is `bool`; an array of
integers is `int-array`; an array of anything else, a free-form object, and a choice of two or
more shapes are `json-text`; a string with `format: date-time`, or no format and a name ending
in `_utc`, is an `iso-instant`; a string with `format: date`, or named `date` or `*_date`, is a
`date`; any other string is `string`. A `TYPE_FIXES` entry then replaces the type and records
the vendor's declared type on the field, so the decoder converts it deliberately.

**The naming policy.** A field's column is its wire key split into segments, each segment
translated to Massive's long form (the vendor's own long key as it is; a short key through the
lexicon of the object it sits in, whose entries cite the endpoint that spells the same field
out; else an identity entry), the last segment replaced by its `NAMES` entry when Massive
names the key nowhere (a name derived from the field's description, which the entry quotes),
and the segments joined in PascalCase: `prevDay.vw` becomes `PrevDayVwap`. Nothing else
renames a field: a vendor `symbol` is generated as `Symbol`, and the library's dataset key
rename, not the generator, makes the key columns.

**Parameters** keep the specification's order and wire names. Their `ParamKind` follows the
same kind of mechanical rules: a name ending in `.any_of`, or an array, is `LIST` (sent
comma-joined, even when its values come from an `enum`); any other `enum` is `ENUM`;
booleans, integers, and numbers are themselves; a timestamp-like string parameter takes its
`PARAM_UNITS` entry, `TIMESTAMP` in its unit or `DATE` when it is "not an epoch"; a string
with `format: date` or `format: date-time`, a choice of string formats, or a base name
`date`, `*_date`, `*_utc`, or `as_of` is `DATE`; any other string is `STRING`.

**Gates.** Every problem is collected, then the refresh stops with all of them, each naming
the gate, the path, the key, and the vendor's description for the reviewer (the description
is shown to help a person choose a name; it never chooses one):

- **G1:** a short key (three characters or fewer, no underscore) with no lexicon, identity,
  or `NAMES` entry;
- **G2:** two fields of one path with one column, or a `NAMES` entry that coins a dataset
  key column (`Symbol`, `Timestamp`, `Underlying`);
- **G3:** a timestamp-like field (a unit tag, or a long name whose snake case is `timestamp`,
  `*_timestamp`, `updated`, `*_updated`, `window_start`, `window_end`, or `*_time`) left to a
  default: a `long` or `double` with no `UNITS` entry, or text with no `format` and neither a
  `UNITS` nor a `TYPE_FIXES` entry; or a timestamp-like string parameter with no `PARAM_UNITS`
  entry;
- **G4:** a stale hand entry: a lexicon shape, identity, `NAMES`, `UNITS`, tag-conflict,
  `TYPE_FIXES`, `RESULT_KEYS`, `PARAM_UNITS`, or `EXCLUSIONS` entry naming a path, field, or
  parameter the snapshot no longer generates, or claiming what the snapshot contradicts;
- **G8:** within one of `overrides.KINDS`, one column holding two concepts (a concept is a
  field's full long-name path, in snake case);
- **snapshot:** a vendor file whose SHA-256 differs from the manifest's, or a file set that
  differs from the manifest's;
- **units:** a `UNITS` unit that differs from the specification's `x-polygon-go-type` tag
  without a tag-conflict entry;
- **envelope:** an envelope key of a single-object body whose type is not the envelope's;
- **docs:** a documented endpoint the specification lacks, a generated path no page names,
  a page that is not an overview yet names no endpoint, or an exclusion whose `undocumented`
  claim the documentation contradicts;
- **shape:** a response the generator cannot read (no rows under the result key, an untyped
  field, a parameter outside the path or query, a parameter choice that is not of date
  formats), or an unknown `x-polygon-*` extension on a path or its operation.

The generator runs only from a source checkout (`refresh/` is not shipped). It imports the
standard library and other `_massive` modules only.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._massive.refresh import docs, exclusions, lexicon, overrides
from deephaven_massive._massive.refresh.fetch import LLMS, MANIFEST, OPENAPI, REST_PAGES
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
    from collections.abc import Iterable, Iterator, Mapping, Sequence

__all__ = [
    "SNAPSHOT_DIRECTORY",
    "SPEC_FILE",
    "Coverage",
    "HandInput",
    "Problem",
    "RefreshError",
    "Snapshot",
    "column_name",
    "generate_rest",
    "refresh",
    "render_rest",
]

SNAPSHOT_DIRECTORY: Final = Path(__file__).parent / "snapshot"
"""The vendored snapshot the refresh reads."""

SPEC_FILE: Final = Path(__file__).parent.parent / "spec" / "rest.json"
"""The generated REST schema the refresh writes."""

# A JSON value as the standard library decodes it.
Json: TypeAlias = "Mapping[str, Json] | list[Json] | str | int | float | bool | None"
JsonObject: TypeAlias = "Mapping[str, Json]"

# The `x-polygon-go-type` names that tag an integer with a unit.
_TAG_UNITS: Final = {
    "INanoseconds": WireUnit.NS,
    "IMicroseconds": WireUnit.US,
    "IMilliseconds": WireUnit.MS,
}
# A long name, in snake case, that makes a field or a string parameter timestamp-like (G3).
_TIMESTAMP_LIKE: Final = re.compile(
    r"timestamp|.+_timestamp|updated|.+_updated|window_start|window_end|.+_time"
)
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
# The dataset key columns the library's key rename produces; `NAMES` never coins them.
_KEY_COLUMNS: Final = frozenset({"Symbol", "Timestamp", "Underlying"})
# How deep `$ref` chains may go before the specification is taken to be cyclic.
_MAX_REF_DEPTH: Final = 64
# The longest key gate G1 treats as short.
_SHORT_KEY_LENGTH: Final = 3


@dataclass(frozen=True, kw_only=True)
class Problem:
    """One reason the refresh stops.

    Attributes:
        gate: The gate that found it (`G1` to `G4`, `G8`, `units`, `envelope`, `snapshot`,
            `docs`, `shape`).
        source: The REST path template, or `""` when no path is involved.
        key: The wire key, parameter, or hand-input item concerned; `""` when none.
        message: What is wrong and what would fix it.
        description: The vendor's description of the field or parameter, when it has one.
    """

    gate: str
    source: str
    key: str
    message: str
    description: str = ""

    def text(self) -> str:
        """Return the problem as the refresh prints it.

        Returns:
            `"<gate> <source> <key>: <message>"` (an empty source or key left out), followed
            by an indented line with the vendor's description when there is one.
        """
        text = " ".join(part for part in (self.gate, self.source, self.key) if part)
        text += f": {self.message}"
        if self.description:
            text += f"\n    description: {' '.join(self.description.split())}"
        return text


class RefreshError(Exception):
    """The refresh found problems and wrote nothing.

    Attributes:
        problems: Every problem found, sorted by gate, path, and key.
    """

    def __init__(self, problems: Iterable[Problem]) -> None:
        """Collect the problems into one error.

        Args:
            problems: The problems found; at least one.
        """
        self.problems: tuple[Problem, ...] = tuple(
            sorted(problems, key=lambda p: (p.gate, p.source, p.key, p.message))
        )
        lines = [f"the refresh stopped on {len(self.problems)} problem(s):"]
        lines += [problem.text() for problem in self.problems]
        super().__init__("\n".join(lines))


@dataclass(frozen=True, kw_only=True)
class HandInput:
    """Every hand-written input of the REST generator, so tests can substitute their own.

    `HandInput()` holds the real tables of `lexicon`, `overrides`, and `exclusions`.

    Attributes:
        lexicons: Lexicon name to lexicon.
        shapes: `(path, object prefix)` to the lexicon name of that object's keys.
        identity: Short keys that are already Massive's long form, each with its reason.
        names: `(path, wire key)` to the long name of the last segment of a key Massive names
            nowhere, with the description it is derived from.
        units: `(path, wire key)` to the unit of an integer field, or to "not a timestamp" for
            a timestamp-like field that is not an epoch.
        tag_conflicts: `(path, wire key)` of a `units` entry that differs from the vendor's
            unit tag, with the reason the tag is wrong.
        type_fixes: `(path, wire key)` to a declared conversion of a mistyped field.
        result_keys: Path to where its rows sit, when that is not `results`.
        param_units: `(path, base parameter)` to the unit of a timestamp parameter, or to
            "not an epoch".
        exclusions: The excluded vendor items.
        kinds: Kind to asset-class group to the sources of that group, the dataset kinds
            within which gate G8 requires one concept per column.
    """

    lexicons: Mapping[str, lexicon.Lexicon] = field(default_factory=lambda: lexicon.LEXICONS)
    shapes: Mapping[tuple[str, str], str] = field(default_factory=lambda: lexicon.SHAPES)
    identity: Mapping[str, str] = field(default_factory=lambda: lexicon.IDENTITY)
    names: Mapping[tuple[str, str], overrides.Name] = field(default_factory=lambda: overrides.NAMES)
    units: Mapping[tuple[str, str], overrides.Unit] = field(default_factory=lambda: overrides.UNITS)
    tag_conflicts: Mapping[tuple[str, str], str] = field(
        default_factory=lambda: overrides.TAG_CONFLICTS
    )
    type_fixes: Mapping[tuple[str, str], overrides.TypeFix] = field(
        default_factory=lambda: overrides.TYPE_FIXES
    )
    result_keys: Mapping[str, overrides.ResultKey] = field(
        default_factory=lambda: overrides.RESULT_KEYS
    )
    param_units: Mapping[tuple[str, str], overrides.Unit] = field(
        default_factory=lambda: overrides.PARAM_UNITS
    )
    exclusions: Sequence[exclusions.Exclusion] = field(
        default_factory=lambda: exclusions.EXCLUSIONS
    )
    kinds: Mapping[str, Mapping[str, Sequence[str]]] = field(
        default_factory=lambda: overrides.KINDS
    )


@dataclass(frozen=True, kw_only=True)
class Snapshot:
    """The vendor sources the REST generator reads, already parsed.

    Attributes:
        openapi: The OpenAPI specification.
        llms: The text of the documentation index.
        pages: Each REST page the index links, by its name below the snapshot's `rest/`
            directory (`"stocks/trades-quotes/trades.md"`), as text.
        openapi_sha256: The SHA-256 of the stored specification, verified against the
            manifest and written into `rest.json`, so every generated file names the snapshot
            it came from.
    """

    openapi: JsonObject
    llms: str
    pages: Mapping[str, str]
    openapi_sha256: str

    @classmethod
    def load(cls, directory: Path = SNAPSHOT_DIRECTORY) -> Snapshot:
        """Read a snapshot directory written by `fetch`, verifying it against its manifest.

        Every file is hashed again and compared with the SHA-256 the manifest recorded when it
        was stored, and the files on disk must be exactly the files the manifest lists. So a
        hand edit of a vendor file (to get a gate past, say) stops the refresh instead of
        reaching `rest.json`.

        Args:
            directory: The snapshot directory.

        Returns:
            The parsed snapshot, with `openapi_sha256` computed from the file itself.

        Raises:
            RefreshError: If the manifest or the specification is not a JSON object, a file's
                hash differs from the manifest's, a file is on disk but not in the manifest or
                the reverse, or a REST page the index links is missing.
        """
        manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
        openapi = json.loads((directory / OPENAPI).read_text(encoding="utf-8"))
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if not isinstance(files, dict) or not isinstance(openapi, dict):
            raise RefreshError(
                [Problem(gate="snapshot", source="", key="", message="not a JSON object")]
            )
        problems = list(_verify(directory, files))
        llms = (directory / LLMS).read_text(encoding="utf-8")
        pages: dict[str, str] = {}
        for url in docs.rest_page_urls(llms):
            name = str(docs.page_file(url))
            path = directory / REST_PAGES / name
            if path.is_file():
                pages[name] = path.read_text(encoding="utf-8")
            else:
                problems.append(
                    Problem(
                        gate="docs",
                        source="",
                        key=name,
                        message=f"the index links {url} but the snapshot lacks the page",
                    )
                )
        if problems:
            raise RefreshError(problems)
        digest = hashlib.sha256((directory / OPENAPI).read_bytes()).hexdigest()
        return cls(openapi=openapi, llms=llms, pages=pages, openapi_sha256=digest)


def _verify(directory: Path, files: Mapping[str, Json]) -> Iterator[Problem]:
    """Compare a snapshot's files with its manifest.

    Args:
        directory: The snapshot directory.
        files: The manifest's `files` object: name to `{"stored_sha256", ...}`.

    Yields:
        A `snapshot` problem for each file whose hash differs from the manifest's, each file
        on disk the manifest does not list, and each listed file that is missing.
    """
    on_disk = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*")
        if path.is_file() and path.name != MANIFEST
    }
    for name in sorted(on_disk - set(files)):
        yield Problem(gate="snapshot", source="", key=name, message="not in the manifest")
    for name, entry in sorted(files.items()):
        recorded = entry.get("stored_sha256") if isinstance(entry, dict) else None
        if name not in on_disk:
            yield Problem(gate="snapshot", source="", key=name, message="listed but missing")
        elif hashlib.sha256((directory / name).read_bytes()).hexdigest() != recorded:
            yield Problem(
                gate="snapshot",
                source="",
                key=name,
                message="its SHA-256 differs from the manifest's: a vendor file was edited; "
                "refetch instead",
            )


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


class _Spec:
    """Read-only access to the OpenAPI document: `$ref`, `allOf`, and one-branch choices.

    Every accessor checks the shape it expects and reports a `shape` problem instead of
    guessing, since the document is untrusted vendor data.
    """

    def __init__(self, document: JsonObject) -> None:
        """Wrap a parsed specification.

        Args:
            document: The OpenAPI document.
        """
        self.document = document

    def deref(self, node: Json, where: str) -> JsonObject:
        """Follow `$ref`s from `node` to the object they name.

        Args:
            node: A schema, parameter, or reference object.
            where: The path and key, for messages.

        Returns:
            The referenced object.

        Raises:
            RefreshError: If the node is not an object, a reference is not local
                (`#/...`), names nothing, or the chain is longer than 64 (a cycle).
        """
        for _ in range(_MAX_REF_DEPTH):
            if not isinstance(node, dict):
                raise RefreshError([_shape_problem(where, "expected a JSON object")])
            ref = node.get("$ref")
            if ref is None:
                return node
            if not isinstance(ref, str) or not ref.startswith("#/"):
                raise RefreshError([_shape_problem(where, f"unsupported reference {ref!r}")])
            target: Json = self.document
            for part in ref[2:].split("/"):
                target = target.get(part) if isinstance(target, dict) else None
            if target is None:
                raise RefreshError([_shape_problem(where, f"reference {ref!r} names nothing")])
            node = target
        raise RefreshError(
            [_shape_problem(where, "reference chain too long; the specification may be cyclic")]
        )

    def schema(self, node: Json, where: str, depth: int = 0) -> JsonObject:
        """Resolve a schema: follow `$ref`, merge `allOf`, and unwrap a one-branch choice.

        `allOf` parts are merged in order: their properties (a later part's property replaces
        an earlier one of the same name), the union of their `required` lists, and their other
        keys, first part first. A `oneOf` or `anyOf` with exactly one branch is that branch;
        one with two or more branches is returned as is (the caller makes it `json-text`).

        Args:
            node: The schema.
            where: The path and key, for messages.
            depth: How many `allOf` parts and one-branch choices enclose this one.

        Returns:
            The resolved schema.

        Raises:
            RefreshError: If parts and choices nest more than 64 deep (a cycle), or a
                reference cannot be followed.
        """
        if depth > _MAX_REF_DEPTH:
            raise RefreshError([_shape_problem(where, "schema nesting too deep; it may be cyclic")])
        resolved = self.deref(node, where)
        for choice in ("oneOf", "anyOf"):
            branches = resolved.get(choice)
            if isinstance(branches, list) and len(branches) == 1:
                return self.schema(branches[0], where, depth + 1)
        parts = resolved.get("allOf")
        if not isinstance(parts, list):
            return resolved
        merged: dict[str, Json] = {"type": "object"}
        properties: dict[str, Json] = {}
        required: list[Json] = []
        for part in parts:
            piece = self.schema(part, where, depth + 1)
            for key, value in piece.items():
                if key == "properties" and isinstance(value, dict):
                    properties.update(value)
                elif key == "required" and isinstance(value, list):
                    required += [name for name in value if name not in required]
                else:
                    merged.setdefault(key, value)
        merged["properties"] = properties
        merged["required"] = required
        return merged


def _shape_problem(where: str, message: str) -> Problem:
    """Return the problem for a specification the generator cannot read.

    Args:
        where: The path and key concerned.
        message: What is wrong.

    Returns:
        A `shape` problem.
    """
    return Problem(gate="shape", source=where, key="", message=message)


def _properties(schema: JsonObject) -> JsonObject | None:
    """Return an object schema's properties, or `None` when it has none to flatten.

    Args:
        schema: A resolved schema.

    Returns:
        The non-empty `properties` mapping of an object schema (typed `object`, or untyped
        with properties), else `None`.
    """
    properties = schema.get("properties")
    if schema.get("type", "object") != "object" or not isinstance(properties, dict):
        return None
    return properties or None


def _required(schema: JsonObject) -> frozenset[str]:
    """Return the names an object schema lists as required.

    Args:
        schema: A resolved object schema.

    Returns:
        The string entries of its `required` list.
    """
    names = schema.get("required")
    return (
        frozenset(n for n in names if isinstance(n, str))
        if isinstance(names, list)
        else (frozenset())
    )


def _text(schema: JsonObject, key: str) -> str:
    """Return a string attribute of a schema, or `""`.

    Args:
        schema: A schema or parameter object.
        key: The attribute, such as `"description"`.

    Returns:
        The attribute when it is a string, else `""`.
    """
    value = schema.get(key)
    return value if isinstance(value, str) else ""


def _short(key: str) -> bool:
    """Return whether a key is short in the sense of gate G1.

    Args:
        key: One segment of a wire key.

    Returns:
        True for three characters or fewer and no underscore.
    """
    return len(key) <= _SHORT_KEY_LENGTH and "_" not in key


_WORD_BREAK: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def column_name(long_names: Iterable[str]) -> str:
    """Join the long names of a wire key's segments into a PascalCase column name.

    Each long name is split into words at underscores and at camel-case boundaries, and each
    word is written with its first character upper-case and the rest lower-case, so
    `["prevDay", "vwap"]` gives `"PrevDayVwap"`, `["sip_mapping", "CTA"]` gives
    `"SipMappingCta"`, and `["aff_10b5_one"]` gives `"Aff10b5One"`.

    Args:
        long_names: The long name of each segment, outermost first.

    Returns:
        The column name.
    """
    words = [word for name in long_names for word in _WORD_BREAK.sub("_", name).split("_") if word]
    return "".join(word[0].upper() + word[1:].lower() for word in words)


class _PathGenerator:
    """Generate one path's `RestPath`, collecting its problems instead of stopping."""

    def __init__(self, spec: _Spec, hand: HandInput, path: str) -> None:
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
        self.params: set[str] = set()
        self.timestamp_bases: set[str] = set()
        self.unit_bases: set[str] = set()
        self.identities: set[str] = set()
        self.concepts: dict[str, str] = {}

    def problem(self, gate: str, key: str, message: str, *, description: str = "") -> None:
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

        Returns:
            The value, or `None` when a problem prevents building it.
        """
        operation = self.spec.deref(
            self.spec.deref(self.spec.document["paths"], "paths").get(self.path), self.path
        ).get("get")
        if not isinstance(operation, dict):
            self.problem("shape", "", "the path has no GET operation")
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
            self.problem("shape", "", str(error))
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
            self.problem("shape", "", "no JSON schema for the HTTP 200 response")
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
            properties = _properties(node)
            if properties is None or step not in properties:
                keys = sorted(properties or {})
                gate = "G4" if entry is not None else "shape"
                self.problem(
                    gate,
                    "",
                    f"no {step!r} in the response object along result key {result_key!r} "
                    f"(keys {keys}); add or correct its RESULT_KEYS entry",
                )
                return result_key, None
            node = self.spec.schema(properties[step], self.path)
        single_object = node.get("type") != "array"
        if not single_object:
            node = self.spec.schema(node.get("items"), self.path)
        properties = _properties(node)
        if properties is None:
            self.problem("shape", "", f"the rows under {result_key!r} are not objects")
            return result_key, None
        out: list[_Raw] = []
        skip = ENVELOPE_KEYS if result_key is None and single_object else frozenset()
        self.flatten(node, "", required=True, out=out, skip=skip)
        return result_key, out

    def flatten(
        self,
        node: JsonObject,
        prefix: str,
        *,
        required: bool,
        out: list[_Raw],
        skip: frozenset[str],
    ) -> None:
        """Append the leaves of an object schema to `out`, nested objects flattened.

        Args:
            node: The resolved object schema.
            prefix: The dotted wire key of the object, with a trailing dot (`""` for the row).
            required: Whether the object itself is required.
            out: Where the leaves go.
            skip: Keys of this object to leave out (the envelope keys of a single-object
                body); each must have its envelope type.
        """
        properties = _properties(node) or {}
        required_here = _required(node)
        for name, child in properties.items():
            wire_key = prefix + name
            schema = self.spec.schema(child, f"{self.path} {wire_key}")
            if name in skip:
                self.check_envelope(name, schema)
                continue
            is_required = required and name in required_here
            if _properties(schema) is not None:
                self.objects.add(wire_key)
                self.flatten(
                    schema, wire_key + ".", required=is_required, out=out, skip=frozenset()
                )
            else:
                self.wire_keys.add(wire_key)
                out.append(_Raw(wire_key, schema, is_required))

    def check_envelope(self, name: str, schema: JsonObject) -> None:
        """Check that an envelope key of a single-object body has the envelope's type.

        Args:
            name: The envelope key.
            schema: Its resolved schema.
        """
        expected = _ENVELOPE_TYPES[name]
        if schema.get("type") != expected:
            self.problem(
                "envelope",
                name,
                f"an envelope key typed {schema.get('type')!r}, not {expected!r}: it may be "
                "row data that dropping it would lose",
                description=_text(schema, "description"),
            )

    def long_names(self, raw: _Raw) -> list[str]:
        """Translate each segment of a wire key to its long name (lexicon, identity, NAMES).

        Args:
            raw: The flattened field.

        Returns:
            The long name of each segment; a short key that nothing translates is kept as is,
            with a G1 problem.
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
                if quoted != " ".join(_text(raw.schema, "description").split()):
                    self.problem(
                        "G4",
                        raw.wire_key,
                        "NAMES quotes a description the specification no longer gives",
                        description=_text(raw.schema, "description"),
                    )
                coined = column_name([override.long_name])
                if coined in _KEY_COLUMNS:
                    self.problem(
                        "G2",
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
                elif _short(segment):
                    self.problem(
                        "G1",
                        raw.wire_key,
                        f"short key {segment!r} has no lexicon, identity, or NAMES entry",
                        description=_text(raw.schema, "description"),
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
                "shape", raw.wire_key, f"unsupported field type {raw.schema.get('type')!r}"
            )
        return found

    def field(self, raw: _Raw) -> Field | None:
        """Name and type one flattened field, checking G1, G3, units, and type fixes.

        Args:
            raw: The flattened field.

        Returns:
            The generated field, or `None` when its type cannot be decided.
        """
        long_names = self.long_names(raw)
        wire_type = self.wire_type(raw)
        if wire_type is None:
            return None
        description = _text(raw.schema, "description")
        fix = self.hand.type_fixes.get((self.path, raw.wire_key))
        unit = self.unit(
            raw, wire_type, long_name=long_names[-1], description=description, fixed=fix is not None
        )
        if unit is not None:
            wire_type = WireType.EPOCH
        declared = None
        if fix is not None and fix.declared is not wire_type:
            self.problem(
                "G4",
                raw.wire_key,
                f"TYPE_FIXES expects the vendor type {fix.declared}, but the "
                f"specification now gives {wire_type}",
                description=description,
            )
        elif fix is not None:
            declared, wire_type = wire_type, fix.wire_type
        column = column_name(long_names)
        self.concepts[column] = ".".join(_snake(name) for name in long_names)
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
        """Decide a field's unit from `UNITS`, checking G3 and the unit tag.

        A field is timestamp-like when the specification tags it with a unit, or the snake
        case of its long name is `timestamp`, `*_timestamp`, `updated`, `*_updated`,
        `window_start`, `window_end`, or `*_time` (so `serverTime` counts). A timestamp-like
        `long` or `double` needs a `UNITS` entry, and so does timestamp-like text with no
        `format`, unless a `TYPE_FIXES` entry types it: nothing timestamp-like is left to a
        default.

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
        timestamp_like = tag_unit is not None or bool(_TIMESTAMP_LIKE.fullmatch(_snake(long_name)))
        untyped_text = wire_type is WireType.STRING and "format" not in raw.schema
        gated = wire_type in {WireType.LONG, WireType.DOUBLE} or (untyped_text and not fixed)
        if entry is None:
            if timestamp_like and gated:
                self.problem(
                    "G3",
                    raw.wire_key,
                    f"timestamp-like {wire_type} (tag {tag_name}) has no UNITS entry"
                    + (" or TYPE_FIXES entry" if untyped_text else ""),
                    description=description,
                )
            return None
        if entry.unit is not None and wire_type is not WireType.LONG:
            self.problem(
                "G4",
                raw.wire_key,
                f"UNITS gives a unit to a {wire_type} field; only an integer is an epoch",
                description=description,
            )
            return None
        if wire_type not in {WireType.LONG, WireType.DOUBLE, WireType.STRING}:
            self.problem(
                "G4", raw.wire_key, f"UNITS names a {wire_type} field", description=description
            )
            return None
        conflict = entry.unit is not None and tag_unit is not None and entry.unit is not tag_unit
        if conflict and key not in self.hand.tag_conflicts:
            self.problem(
                "units",
                raw.wire_key,
                f"UNITS says {entry.unit} but the specification tags {tag_name}; add a "
                "TAG_CONFLICTS entry with the reason the tag is wrong",
                description=description,
            )
        if not conflict and key in self.hand.tag_conflicts:
            self.problem("G4", raw.wire_key, "TAG_CONFLICTS entry, but UNITS agrees with the tag")
        return entry.unit

    def check_columns(self, fields: Sequence[Field]) -> None:
        """Report two fields with one column (gate G2).

        Args:
            fields: The path's generated fields.
        """
        seen: dict[str, str] = {}
        for item in fields:
            if item.column in seen:
                self.problem(
                    "G2",
                    item.wire_key,
                    f"column {item.column!r} is also the column of {seen[item.column]!r}",
                )
            else:
                seen[item.column] = item.wire_key

    def parameters(self, operation: JsonObject) -> list[Param]:
        """Generate the path's parameters, checking G3 for timestamp-like strings.

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
        name = _text(node, "name")
        location = node.get("in")
        description = _text(node, "description")
        if location not in {"path", "query"}:
            self.problem("shape", name, f"unsupported parameter location {location!r}")
            return None
        self.params.add(name)
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
        entry, or a timestamp-like one (the snake case of its base name is `timestamp`,
        `*_timestamp`, `updated`, `*_updated`, `window_start`, `window_end`, or `*_time`). An
        entry with a unit makes it `TIMESTAMP` in that unit, "not an epoch" makes it `DATE`,
        and a timestamp-like one with no entry fails gate G3. A choice of schemas (`oneOf`,
        `anyOf`) must be a choice of date and date-time strings, which is `DATE`.

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
                    "shape", name, f"a {choice} parameter that is not a choice of date formats"
                )
        kind, values = _parameter_rule(name, base, schema)
        entry = self.hand.param_units.get((self.path, base))
        timestamp_like = bool(_TIMESTAMP_LIKE.fullmatch(_snake(base)))
        if schema.get("type") != "string" or kind is ParamKind.LIST:
            return kind, values, None
        if entry is None:
            if timestamp_like and base not in self.timestamp_bases:
                self.problem(
                    "G3",
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


def _snake(name: str) -> str:
    """Return a long name in snake case, so gate G3 matches camel-case names too.

    Args:
        name: A long name, such as `"serverTime"` or `"last_updated"`.

    Returns:
        The name with camel-case boundaries turned into underscores, lower-cased
        (`"server_time"`).
    """
    return _WORD_BREAK.sub("_", name).lower()


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
        problems: Where a page that names an unreadable endpoint is reported.

    Returns:
        Path template to page name.
    """
    pages: dict[str, str] = {}
    for name, text in sorted(snapshot.pages.items()):
        try:
            path = docs.endpoint(text)
        except docs.DocsError as error:
            problems.append(Problem(gate="docs", source="", key=name, message=str(error)))
            continue
        if path is not None:
            pages.setdefault(path, name)
        elif PurePosixPath(name).stem not in _OVERVIEW_PAGES:
            problems.append(
                Problem(
                    gate="docs",
                    source="",
                    key=name,
                    message="a page that is not an overview names no endpoint",
                )
            )
    return pages


def _check_extensions(spec: _Spec, paths: JsonObject) -> Iterator[Problem]:
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
            yield Problem(gate="shape", source=path, key=key, message="unknown vendor extension")


def _check_kinds(hand: HandInput, generators: Mapping[str, _PathGenerator]) -> Iterator[Problem]:
    """Require one concept per column within each kind (G8).

    A field's concept is its full long-name path, each segment in snake case (`day.close`,
    `last_trade.sip_timestamp`), so a snapshot's `DayClose`, `LastMinuteClose`, and
    `PrevDayClose` are three concepts, and the v2 snapshot's `lastTrade.p` and the unified
    snapshot's `last_trade.price` are one. Within the sources of one dataset kind, which merge
    into one table, a column must always hold the same concept, or two quantities would land
    in one column: two paths can read alike once PascalCased (`last.trade_price` and
    `last_trade.price` both give `LastTradePrice`). The reverse, one concept in two columns,
    cannot happen while a column is the PascalCase of its concept; the projections that rename
    a kind member's fields extend the gate to it.

    Args:
        hand: The hand input, whose `kinds` lists each kind's sources.
        generators: The generator of each generated path, after it ran.

    Yields:
        One G8 problem per column that holds two concepts within a kind.
    """
    for kind, groups in sorted(hand.kinds.items()):
        concepts: dict[str, set[str]] = {}
        for source in (s for group in groups.values() for s in group):
            generator = generators.get(source)
            for column, concept in (generator.concepts if generator else {}).items():
                concepts.setdefault(column, set()).add(concept)
        for column, names in sorted(concepts.items()):
            if len(names) > 1:
                yield Problem(
                    gate="G8",
                    source="",
                    key=column,
                    message=f"kind {kind!r}: one column holds the concepts {sorted(names)}",
                )


def _stale_entries(hand: HandInput, generators: Mapping[str, _PathGenerator]) -> Iterator[Problem]:
    """Report the hand entries that name a path, field, or parameter not generated (G4).

    Args:
        hand: The hand input.
        generators: The generator of each generated path, after it ran.

    Yields:
        One G4 problem per stale entry.
    """
    field_tables: list[tuple[str, Iterable[tuple[str, str]]]] = [
        ("NAMES", hand.names),
        ("UNITS", hand.units),
        ("TAG_CONFLICTS", hand.tag_conflicts),
        ("TYPE_FIXES", hand.type_fixes),
    ]
    for table, keys in field_tables:
        for path, wire_key in keys:
            generator = generators.get(path)
            if generator is None or wire_key not in generator.wire_keys:
                yield Problem(
                    gate="G4",
                    source=path,
                    key=wire_key,
                    message=f"{table} entry for a field the snapshot does not generate",
                )
    for path, prefix in hand.shapes:
        generator = generators.get(path)
        if generator is None or prefix not in generator.objects:
            yield Problem(
                gate="G4",
                source=path,
                key=prefix,
                message="lexicon shape for an object the snapshot does not generate",
            )
    for path, base in hand.param_units:
        generator = generators.get(path)
        if generator is None or base not in generator.unit_bases:
            yield Problem(
                gate="G4",
                source=path,
                key=base,
                message="PARAM_UNITS entry for no string parameter the snapshot generates",
            )
    used = set().union(*(generator.identities for generator in generators.values()))
    for key in sorted(set(hand.identity) - used):
        yield Problem(
            gate="G4",
            source="",
            key=key,
            message="IDENTITY entry for a key no generated object has",
        )
    for path in hand.result_keys:
        if path not in generators:
            yield Problem(
                gate="G4",
                source=path,
                key="",
                message="RESULT_KEYS entry for a path the snapshot does not generate",
            )


def _check_exclusions(
    hand: HandInput,
    statuses: Mapping[str, str],
    pages: Mapping[str, str],
) -> Iterator[Problem]:
    """Check each REST exclusion against the specification and the documentation.

    Args:
        hand: The hand input.
        statuses: Each specification path's status (see `_status`).
        pages: Each documented path's page.

    Yields:
        A G4 problem for an exclusion naming a path the specification lacks or already marks
        deprecated or draft, and a docs problem for an `undocumented` claim the documentation
        contradicts, in either direction.
    """
    for entry in hand.exclusions:
        if entry.kind is not Transport.REST:
            continue
        if entry.item not in statuses:
            yield Problem(
                gate="G4",
                source=entry.item,
                key="",
                message="EXCLUSIONS entry for a path the specification lacks",
            )
        elif statuses[entry.item]:
            yield Problem(
                gate="G4",
                source=entry.item,
                key="",
                message=f"EXCLUSIONS entry for a path the specification marks "
                f"{statuses[entry.item]}, which already accounts for it",
            )
        elif entry.undocumented and entry.item in pages:
            yield Problem(
                gate="docs",
                source=entry.item,
                key="",
                message=f"excluded as undocumented, but {pages[entry.item]} documents it",
            )
        elif not entry.undocumented and entry.item not in pages:
            yield Problem(
                gate="docs",
                source=entry.item,
                key="",
                message="no documentation page names it; mark the exclusion undocumented",
            )


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
    spec = _Spec(snapshot.openapi)
    problems: list[Problem] = []
    pages = _documented_pages(snapshot, problems)
    paths = spec.deref(snapshot.openapi.get("paths"), "paths")
    statuses = {path: _status(spec.deref(item, path)) for path, item in paths.items()}
    problems += _check_extensions(spec, paths)
    problems += [
        Problem(
            gate="docs",
            source=path,
            key="",
            message=f"{page} documents a path the specification lacks",
        )
        for path, page in pages.items()
        if path not in statuses
    ]
    problems += _check_exclusions(hand, statuses, pages)
    excluded = {e.item for e in hand.exclusions if e.kind is Transport.REST} & set(statuses)
    generators: dict[str, _PathGenerator] = {}
    generated: list[RestPath] = []
    for path, status in statuses.items():
        if status or path in excluded:
            continue
        generator = _PathGenerator(spec, hand, path)
        generators[path] = generator
        try:
            value = generator.generate()
        except RefreshError as error:
            problems += generator.problems + list(error.problems)
            continue
        problems += generator.problems
        if value is not None:
            generated.append(value)
        if path not in pages:
            problems.append(
                Problem(
                    gate="docs",
                    source=path,
                    key="",
                    message="generated, but no documentation page names it; exclude it as "
                    "undocumented",
                )
            )
    problems += _stale_entries(hand, generators)
    problems += _check_kinds(hand, generators)
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


def _line(value: Mapping[str, Json]) -> str:
    """Return one JSON object on one line, keys sorted.

    Args:
        value: The object.

    Returns:
        Its compact JSON text.
    """
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _field_json(item: Field) -> dict[str, Json]:
    """Return a field as it is written to `rest.json`.

    Args:
        item: The field.

    Returns:
        Every attribute, enums by value.
    """
    return {
        "wire_key": item.wire_key,
        "column": item.column,
        "type": item.wire_type.value,
        "unit": item.unit.value if item.unit else None,
        "required": item.required,
        "declared_type": item.declared_type.value if item.declared_type else None,
    }


def _param_json(item: Param) -> dict[str, Json]:
    """Return a parameter as it is written to `rest.json`.

    Args:
        item: The parameter.

    Returns:
        Every attribute, enums by value.
    """
    return {
        "name": item.name,
        "kind": item.kind.value,
        "in_path": item.in_path,
        "values": list(item.values),
        "unit": item.unit.value if item.unit else None,
    }


def render_rest(paths: Sequence[RestPath], *, openapi_sha256: str) -> str:
    """Return the canonical text of `rest.json`.

    The file is JSON with one line per field and per parameter, so a refresh diff shows each
    changed field or parameter as one changed line. Paths keep their order; within a field or
    parameter, keys are sorted.

    Args:
        paths: The generated paths.
        openapi_sha256: The stored hash of the specification they came from.

    Returns:
        The file's text, ending in a newline.
    """
    out = [
        "{",
        f' "generated_by": {json.dumps("python -m deephaven_massive._massive.refresh")},',
        f' "openapi_sha256": {json.dumps(openapi_sha256)},',
        ' "sources": [',
    ]
    for index, path in enumerate(paths):
        out += [
            "  {",
            f'   "key": {json.dumps(path.key)},',
            f'   "result_key": {json.dumps(path.result_key)},',
            '   "params": [',
            *_rows([_line(_param_json(p)) for p in path.params], "    "),
            "   ],",
            '   "fields": [',
            *_rows([_line(_field_json(f)) for f in path.fields], "    "),
            "   ],",
            '   "memberships": []',
            "  }" + ("," if index < len(paths) - 1 else ""),
        ]
    out += [" ]", "}"]
    return "\n".join(out) + "\n"


def _rows(lines: Sequence[str], indent: str) -> list[str]:
    """Indent JSON lines and separate them with commas.

    Args:
        lines: One JSON value per line.
        indent: The indentation to prefix.

    Returns:
        The lines, each indented, every one but the last followed by a comma.
    """
    return [f"{indent}{line}{',' if i < len(lines) - 1 else ''}" for i, line in enumerate(lines)]


def refresh(
    snapshot_directory: Path | None = None,
    spec_file: Path | None = None,
    hand: HandInput | None = None,
) -> bool:
    """Regenerate `spec/rest.json` from the snapshot.

    The file is written only when every gate passes and its text changes.

    Args:
        snapshot_directory: The snapshot to read; `SNAPSHOT_DIRECTORY` when omitted.
        spec_file: The file to write; `SPEC_FILE` when omitted. Both defaults are read when
            the function runs, so a test can point them elsewhere.
        hand: The hand input; the real tables when omitted.

    Returns:
        True when the file was written (its text changed), False when it was already current.
    """
    snapshot = Snapshot.load(snapshot_directory or SNAPSHOT_DIRECTORY)
    spec_file = spec_file or SPEC_FILE
    paths, _ = generate_rest(snapshot, hand)
    text = render_rest(paths, openapi_sha256=snapshot.openapi_sha256)
    if spec_file.is_file() and spec_file.read_text(encoding="utf-8") == text:
        return False
    spec_file.write_text(text, encoding="utf-8")
    return True
