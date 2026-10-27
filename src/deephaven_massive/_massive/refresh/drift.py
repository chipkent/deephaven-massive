"""Compare Massive's live published API with the vendored snapshot: the drift check.

`python -m deephaven_massive._massive.refresh --check` calls `check`, which downloads the live
vendor sources exactly as `--fetch` does (into a scratch directory, never over the vendored
snapshot), compares them with the snapshot, and returns a `Report`. Nothing in the source
checkout changes. The report lists every difference as one `Drift` item in one of three
sections, each with the action a person takes:

- **Unaccounted items** (`Section.UNACCOUNTED`): a REST endpoint, WebSocket feed, or flat-file
  dataset the live documentation names that the committed `spec/*.json` neither generates nor
  `EXCLUSIONS` excludes (nor, for an endpoint or channel, the vendor marks deprecated or
  draft); a flat-file dataset page no overview row names; and, when S3 credentials are set, a
  top-level prefix of the flat-file bucket (`FlatFileClient.prefixes`) that no overview row,
  generated source, or exclusion names.
- **Field, type, and column changes** (`Section.CHANGES`): REST paths and WebSocket channels
  new in the specifications, removed while not generated, unreadable, or whose status changed
  otherwise than to deprecated (draft to current, say); response and message fields added,
  removed, or retyped, or whose unit tag (`x-polygon-go-type`) or requiredness changed; REST
  parameters added, removed, or changed; documentation pages added to or removed from
  `llms.txt`; a REST page that documents another endpoint, or a WebSocket page another feed;
  a flat-file page's "Columns" table, its "Example Data" header, or an overview's dataset
  rows; and every problem a refresh gate would stop on if the live sources were fetched and
  regenerated (`gate_stops`).
- **Deprecations** (`Section.DEPRECATIONS`): a path or channel the vendor newly deprecates; a
  deprecated or removed one the committed `rest.json` or `ws.json` still holds; and a
  committed flat-file prefix no live overview names any more.

The comparison is semantic: descriptions, examples, key order, and formatting are ignored,
so a reworded page or a reordered document reports nothing. Fields are compared on the vendor
document's own terms (dotted keys below the response body, such as `results[].p`, with their
schema type, format, unit tag, and requiredness), independently of the refresh's naming rules,
so a change is reported even where a refresh gate would stop on it.

**Untrusted text.** Every vendor value in a report (a path, key, type, header, or gate
message) is quoted as Markdown code with its backticks replaced and its whitespace collapsed,
so the report can be posted as a GitHub issue body without a vendor string becoming a
mention, a link, or markup. The report holds no credential: the S3 keys are read from the
environment, handed to the flat-file client, and never written anywhere.

It imports the standard library, the rest of `refresh`, the flat-file client, and `spec`.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Final, NamedTuple

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.refresh import docs, fetch, generate, rest, ws
from deephaven_massive._massive.refresh.flatfiles import venue_template
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required
from deephaven_massive._massive.refresh.snapshot import Snapshot
from deephaven_massive._massive.spec import sources
from deephaven_massive._massive.spec.types import Transport

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json, JsonObject

__all__ = [
    "S3_BUCKET",
    "S3_CREDENTIAL_VARIABLES",
    "S3_ENDPOINT",
    "Committed",
    "Drift",
    "Report",
    "Section",
    "check",
    "compare",
    "gate_stops",
    "unnamed_prefixes",
]

S3_CREDENTIAL_VARIABLES: Final = ("MASSIVE_S3_ACCESS_KEY_ID", "MASSIVE_S3_SECRET_ACCESS_KEY")
"""The environment variables of the S3 access key id and secret access key, the library's own."""

S3_ENDPOINT: Final = "https://files.massive.com"
"""Massive's flat-file S3 endpoint."""

S3_BUCKET: Final = "flatfiles"
"""Massive's flat-file bucket."""

# The documentation pages that describe an area rather than one endpoint, feed, or dataset.
_OVERVIEW_PAGES: Final = frozenset({"overview", "quickstart"})
# How deep fields nest before a nested object is compared as one opaque value. Massive's
# deepest response nests five levels; the bound only stops a schema that refers to itself.
_MAX_NESTING: Final = 16
# The vendor statuses that account for a path or channel without generating it.
_NOT_GENERATED: Final = frozenset({"deprecated", "draft"})
# The command a person runs to see the report locally.
_COMMAND: Final = "uv run python -m deephaven_massive._massive.refresh"


class Section(Enum):
    """The three kinds of drift, each with the action that resolves it, in report order.

    Attributes:
        heading: The section's heading in the report.
        action: What a person does about each item of the section.
    """

    UNACCOUNTED = (
        "Unaccounted items",
        (
            f"run `{_COMMAND} --fetch` and the plain refresh, then add a dataset, or add an "
            "`EXCLUSIONS` entry with its reason."
        ),
    )
    CHANGES = (
        "Field, type, and column changes",
        (
            f"run `{_COMMAND} --fetch` and the plain refresh, adding a lexicon, `NAMES`, or "
            "`UNITS` line where a gate asks."
        ),
    )
    DEPRECATIONS = (
        "Deprecations",
        "replace the dataset's path, or add it to `EXCLUSIONS` with its reason.",
    )

    def __init__(self, heading: str, action: str) -> None:
        """Name the section's heading and action.

        Args:
            heading: The section's heading in the report.
            action: What a person does about each item.
        """
        self.heading = heading
        self.action = action


@dataclass(frozen=True, kw_only=True)
class Drift:
    """One difference between the live vendor sources and what the repository accounts for.

    Created by `compare`, `gate_stops`, and `unnamed_prefixes`. Immutable.

    Attributes:
        section: Which kind of drift it is, and so what to do about it.
        area: Where it is: `"REST"`, `"WebSocket"`, `"flat files"`, `"documentation"`,
            `"bucket"`, or `"refresh"` (a gate stop).
        item: The vendor item concerned (a path template, specification path, page URL,
            prefix, or source key), as the vendor spells it.
        detail: What changed, as Markdown with every vendor value already quoted as code.
    """

    section: Section
    area: str
    item: str
    detail: str

    def line(self) -> str:
        """Return the item as one Markdown list item.

        Returns:
            `"- <area> <item as code>: <detail>"`.
        """
        return f"- {self.area} {_code(self.item)}: {self.detail}"


@dataclass(frozen=True, kw_only=True)
class Report:
    """What `check` found: every drift item, and what the check could and could not see.

    Created by `check`. Immutable.

    Attributes:
        items: Every drift item, in report order (section, then area, item, and detail).
        fetched: The fetch date of the vendored snapshot compared against.
        bucket_listed: Whether the bucket's prefixes were listed, which needs both S3
            credentials in the environment.
    """

    items: tuple[Drift, ...]
    fetched: dt.date
    bucket_listed: bool

    def text(self) -> str:
        """Return the report as Markdown, ready to print or to post as an issue body.

        Returns:
            With drift: a title, one section per kind of drift found, each with its action
            and its items, and a closing note on how to reproduce the report. Without drift:
            one line saying so. Both end with a note when the bucket was not listed, and with
            a newline.
        """
        lines: list[str] = []
        if not self.items:
            lines.append(
                f"No drift: the live sources match the vendored snapshot of {self.fetched}."
            )
        else:
            lines += [
                "# Massive API drift",
                "",
                (
                    f"The live vendor sources differ from the vendored snapshot of "
                    f"{self.fetched} (`vendor/massive/`) in {len(self.items)} item(s)."
                ),
            ]
            for section in Section:
                found = [item.line() for item in self.items if item.section is section]
                if found:
                    lines += ["", f"## {section.heading} ({len(found)})", ""]
                    lines += [f"Action: {section.action}", "", *found]
            lines += [
                "",
                f"Reproduce this report with `{_COMMAND} --check` in a source checkout.",
            ]
        if not self.bucket_listed:
            lines += [
                "",
                (
                    f"The flat-file bucket was not listed: set `{S3_CREDENTIAL_VARIABLES[0]}` "
                    f"and `{S3_CREDENTIAL_VARIABLES[1]}` to include it."
                ),
            ]
        return "\n".join(lines) + "\n"


@dataclass(frozen=True, kw_only=True)
class Committed:
    """The keys of the sources the committed `spec/*.json` generate, per transport.

    `load()` reads them from the generated files; a test builds its own. Immutable.

    Attributes:
        rest: The REST path templates (`"/v3/trades/{stockTicker}"`).
        ws: The WebSocket keys (`"stocks/T"`).
        flat_files: The flat-file prefixes and templates (`"us_futures_{venue}/trades_v1"`).
    """

    rest: frozenset[str]
    ws: frozenset[str]
    flat_files: frozenset[str]

    @classmethod
    def load(cls) -> Committed:
        """Read the keys from the generated files the package holds.

        Returns:
            The keys of `spec/rest.json`, `spec/ws.json`, and `spec/flatfiles.json`.
        """
        return cls(
            rest=frozenset(sources(Transport.REST)),
            ws=frozenset(sources(Transport.WS)),
            flat_files=frozenset(sources(Transport.FLAT_FILES)),
        )


class _Leaf(NamedTuple):
    """One field of a response or message, as the drift check compares it.

    Attributes:
        type: Its schema type as text (`_type_text`).
        tag: Its unit tag, or `None`.
        required: Whether it is required, as the generators decide it: listed as required by
            its object and by every enclosing object up to the row.
    """

    type: str
    tag: str | None
    required: bool


@dataclass(frozen=True)
class _Operation:
    """What the drift check compares of one path or channel of a specification.

    Attributes:
        status: `"deprecated"`, `"draft"`, or `"current"`.
        parameters: Each parameter's name mapped to its location, type, and constraints.
        fields: Each dotted field key below the body mapped to its type, unit tag, and
            requiredness.
    """

    status: str
    parameters: Mapping[str, str]
    fields: Mapping[str, _Leaf]


def _code(value: str) -> str:
    """Quote a vendor value as Markdown code that cannot break out or become markup.

    Args:
        value: Any text, such as a field key or a gate's message.

    Returns:
        The text with its backticks replaced by `'` and its whitespace collapsed to single
        spaces, in backticks; a value with nothing left is `(none)` in backticks.
    """
    text = " ".join(value.replace("`", "'").split())
    return f"`{text or '(none)'}`"


def _walk(spec: OpenApi, node: Json, keys: Sequence[str], *, where: str) -> Json:
    """Follow a chain of keys through objects that may be references.

    Args:
        spec: The specification.
        node: Where to start.
        keys: The keys to follow, in order.
        where: The path, for messages.

    A `RefreshError` from `OpenApi.deref` (a value on the way that is neither an object nor
    `None`, or a reference that cannot be followed) propagates.

    Returns:
        The value at the end of the chain, or `None` when a key is missing on the way.
    """
    for key in keys:
        if node is None:
            return None
        node = spec.deref(node, where).get(key)
    return node


def _type_text(spec: OpenApi, schema: JsonObject, where: str) -> str:
    """Describe a resolved schema's type as the report shows it.

    Args:
        spec: The specification.
        schema: A resolved schema.
        where: The path, for messages.

    Returns:
        The type and format (`"string (date-time)"`), `"array of <item type>"`, `"oneOf of 2
        shapes"` for a choice, or `"untyped"`.
    """
    for choice in ("oneOf", "anyOf"):
        branches = schema.get(choice)
        if isinstance(branches, list):
            return f"{choice} of {len(branches)} shapes"
    kind = schema.get("type")
    text = kind if isinstance(kind, str) else "untyped"
    form = schema.get("format")
    if isinstance(form, str):
        text += f" ({form})"
    if kind == "array":
        text += f" of {_type_text(spec, spec.schema(schema.get('items', {}), where), where)}"
    return text


def _tag(schema: JsonObject) -> str | None:
    """Return a schema's unit tag: the name of its `x-polygon-go-type`, if it has one.

    Args:
        schema: A resolved schema.

    Returns:
        The tag's name, such as `"IMilliseconds"`, or `None`.
    """
    tag = schema.get("x-polygon-go-type")
    name = tag.get("name") if isinstance(tag, dict) else None
    return name if isinstance(name, str) else None


def _leaves(
    spec: OpenApi,
    node: Json,
    *,
    prefix: str,
    where: str,
    depth: int = 0,
    is_required: bool = True,
) -> Iterator[tuple[str, _Leaf]]:
    """Flatten a body or message schema into its dotted field keys.

    Object properties are joined with `.`; an array of objects adds `[]` to its key and goes
    on into the objects (`results[].day.c`). Anything else is a leaf. A property is required
    when its object lists it in `required` and the object itself is required; the body and
    each object of an array (a row) count as required, as the generators count them.

    Args:
        spec: The specification.
        node: The schema, possibly a reference.
        prefix: The key of `node`; `""` for the body itself.
        where: The path, for messages.
        depth: How many objects and arrays enclose `node`.
        is_required: Whether `node` itself is required.

    A `RefreshError` from resolving a schema (a reference that cannot be followed)
    propagates.

    Yields:
        Each leaf's key (`"(body)"` for a body that is itself a leaf) and its type and tag.
    """
    schema = spec.schema(node, where)
    found = properties(schema)
    if depth < _MAX_NESTING and found is not None:
        needed = required(schema)
        for name, child in found.items():
            yield from _leaves(
                spec,
                child,
                prefix=f"{prefix}.{name}" if prefix else name,
                where=where,
                depth=depth + 1,
                is_required=is_required and name in needed,
            )
        return
    if depth < _MAX_NESTING and schema.get("type") == "array":
        items = spec.schema(schema.get("items", {}), where)
        if properties(items) is not None:
            yield from _leaves(spec, items, prefix=f"{prefix}[]", where=where, depth=depth + 1)
            return
    yield prefix or "(body)", _Leaf(_type_text(spec, schema, where), _tag(schema), is_required)


def _parameter(spec: OpenApi, node: Json, where: str) -> tuple[str, str]:
    """Describe one parameter as the report compares it.

    Args:
        spec: The specification.
        node: The parameter object, possibly a reference.
        where: The path, for messages.

    A `RefreshError` from resolving it (a reference that cannot be followed) propagates.

    Returns:
        Its name, and its location, type, whether it is required, and its allowed values.
    """
    parameter = spec.deref(node, where)
    schema = spec.schema(parameter.get("schema", {}), where)
    text = f"{parameter.get('in')} {_type_text(spec, schema, where)}"
    if parameter.get("required") is True:
        text += ", required"
    enum = schema.get("enum")
    if isinstance(enum, list):
        text += f", one of {', '.join(sorted(str(value) for value in enum))}"
    return str(parameter.get("name")), text


def _operation(spec: OpenApi, path: str, node: Json) -> _Operation | str:
    """Read what the drift check compares of one path or channel.

    Args:
        spec: The specification.
        path: The path, as the specification spells it.
        node: Its path item, possibly a reference.

    Returns:
        The operation, or the reason it cannot be read (a reference that names nothing, say).
    """
    try:
        item = spec.deref(node, path)
        operation = spec.deref(item.get("get", {}), path)
        parameters = [
            _parameter(spec, parameter, path)
            for holder in (item, operation)
            if isinstance(listed := holder.get("parameters"), list)
            for parameter in listed
        ]
        body = _walk(
            spec,
            operation,
            ("responses", "200", "content", "application/json", "schema"),
            where=path,
        )
        fields = {} if body is None else dict(_leaves(spec, body, prefix="", where=path))
    except RefreshError as error:
        return "; ".join(problem.message for problem in error.problems)
    return _Operation(
        status=rest.path_status(item) or "current", parameters=dict(parameters), fields=fields
    )


def _operations(document: JsonObject) -> dict[str, _Operation | str]:
    """Read every path or channel of a specification.

    Args:
        document: An OpenAPI specification: the REST one, or the WebSocket one, whose paths
            are its channels.

    Returns:
        Each path mapped to its operation or the reason it cannot be read.
    """
    spec = OpenApi(document)
    paths = document.get("paths")
    found = paths if isinstance(paths, dict) else {}
    return {path: _operation(spec, path, node) for path, node in found.items()}


def _changed(old: Mapping[str, str], new: Mapping[str, str]) -> Iterator[str]:
    """Describe how one mapping of labelled values changed.

    Args:
        old: The snapshot's values, by a label that is already Markdown with every vendor
            name in it quoted (`"field `price`"`).
        new: The live values, by the same labels.

    Yields:
        One detail per label added, removed, or whose value changed, in label order.
    """
    for label in sorted(old.keys() | new.keys()):
        before, after = old.get(label), new.get(label)
        if before is None:
            yield f"{label} added: {_code(str(after))}"
        elif after is None:
            yield f"{label} removed (was {_code(before)})"
        elif before != after:
            yield f"{label} changed from {_code(before)} to {_code(after)}"


def _labelled(what: str, values: Mapping[str, str]) -> dict[str, str]:
    """Label each vendor name of a mapping for `_changed`.

    Args:
        what: What a name is, such as `"field"`.
        values: Values by vendor name.

    Returns:
        The values by `"<what> <name as code>"`.
    """
    return {f"{what} {_code(name)}": value for name, value in values.items()}


def _field_changes(old: Mapping[str, _Leaf], new: Mapping[str, _Leaf]) -> Iterator[str]:
    """Describe how a response's or message's fields changed.

    Args:
        old: The snapshot's fields.
        new: The live fields.

    Yields:
        One detail per field added or removed, and per field whose type, requiredness, or
        unit tag changed.
    """
    yield from _changed(
        _labelled("field", {key: leaf.type for key, leaf in old.items() if key not in new}),
        _labelled("field", {key: leaf.type for key, leaf in new.items() if key not in old}),
    )
    for key in sorted(old.keys() & new.keys()):
        before, after = old[key], new[key]
        if before.type != after.type:
            yield f"field {_code(key)} retyped from {_code(before.type)} to {_code(after.type)}"
        if before.required != after.required:
            now = "now required" if after.required else "no longer required"
            yield f"field {_code(key)} {now}"
        if before.tag != after.tag:
            yield (
                f"field {_code(key)} unit tag changed from {_code(before.tag or 'none')} to "
                f"{_code(after.tag or 'none')}"
            )


def _specification_changes(
    area: str,
    snapshot: JsonObject,
    live: JsonObject,
    *,
    generated: Callable[[str], bool],
    file: str,
) -> Iterator[Drift]:
    """Compare one specification's paths or channels between the snapshot and the live copy.

    Args:
        area: `"REST"` or `"WebSocket"`.
        snapshot: The vendored specification.
        live: The live specification.
        generated: Whether the committed generated file holds a path.
        file: That file's name, for the report.

    Yields:
        The drift items: paths new, removed, newly deprecated, deprecated or removed while
        still generated, with another status, or unreadable, and the parameter and field
        changes of every path in both.
    """
    old, new = _operations(snapshot), _operations(live)
    for path in sorted(old.keys() | new.keys()):
        before, after = old.get(path), new.get(path)
        if after is None:
            section = Section.DEPRECATIONS if generated(path) else Section.CHANGES
            kept = f", but `{file}` still has it" if generated(path) else ""
            yield Drift(section=section, area=area, item=path, detail=f"removed{kept}")
            continue
        if isinstance(after, str):
            if after != before:
                yield Drift(
                    section=Section.CHANGES,
                    area=area,
                    item=path,
                    detail=f"can no longer be read: {_code(after)}",
                )
            continue
        yield from _status_changes(
            area, path, before=before, after=after, generated=generated(path), file=file
        )
        if isinstance(before, _Operation):
            details = [
                *_changed(
                    _labelled("parameter", before.parameters),
                    _labelled("parameter", after.parameters),
                ),
                *_field_changes(before.fields, after.fields),
            ]
            yield from (
                Drift(section=Section.CHANGES, area=area, item=path, detail=detail)
                for detail in details
            )


def _status_changes(
    area: str,
    path: str,
    *,
    before: _Operation | str | None,
    after: _Operation,
    generated: bool,
    file: str,
) -> Iterator[Drift]:
    """Report a path's status: new, newly deprecated, otherwise changed, or still generated.

    Args:
        area: `"REST"` or `"WebSocket"`.
        path: The path.
        before: The snapshot's operation, the reason it could not be read, or `None` when the
            snapshot lacks the path.
        after: The live operation.
        generated: Whether the committed generated file holds the path.
        file: That file's name, for the report.

    Yields:
        At most one deprecation and one change.
    """
    was = before.status if isinstance(before, _Operation) else None
    if after.status == "deprecated" and generated:
        yield Drift(
            section=Section.DEPRECATIONS,
            area=area,
            item=path,
            detail=f"deprecated, but `{file}` still has it",
        )
    elif after.status == "deprecated" and was != "deprecated":
        yield Drift(section=Section.DEPRECATIONS, area=area, item=path, detail="newly deprecated")
    if before is None:
        detail = f"new ({after.status})"
    elif isinstance(before, str):
        detail = f"can be read again ({after.status})"
    elif after.status not in {was, "deprecated"}:
        detail = f"now {after.status} (was {was})"
    else:
        return
    yield Drift(section=Section.CHANGES, area=area, item=path, detail=detail)


def _page_facts(area: docs.Area, page: str) -> dict[str, str]:
    """Read what a documentation page says that the refresh depends on.

    Args:
        area: The page's area.
        page: The page's text.

    Returns:
        Each fact's label (as `_changed` takes it) mapped to its value: for a REST page its
        endpoint; for a WebSocket page its feed; for a flat-file page each "Columns" row's
        type, its "Example Data" header, and each overview row's dataset page. A page that
        cannot be read has one fact, `"the page"`, whose value is why.
    """
    try:
        if area is docs.REST:
            return {"the endpoint": docs.endpoint(page) or "none"}
        if area is docs.WEBSOCKET:
            return {"the feed": "/".join(docs.feed(page) or ("none",))}
        facts = _labelled("column", {column.name: column.type for column in docs.columns(page)})
        header = docs.example_header(page)
        if header:
            facts["the Example Data header"] = ", ".join(header)
        facts |= _labelled("overview row", {prefix: url for url, prefix in docs.datasets(page)})
    except docs.DocsError as error:
        return {"the page": f"unreadable: {error}"}
    return facts


def _area_pages(snapshot: Snapshot) -> dict[docs.Area, tuple[Mapping[str, str], Mapping[str, str]]]:
    """Return each documentation area's pages and their URLs.

    Args:
        snapshot: A snapshot.

    Returns:
        Area to (page name to text, page name to URL).
    """
    return {
        docs.REST: (snapshot.pages, snapshot.page_urls),
        docs.WEBSOCKET: (snapshot.websocket_pages, snapshot.websocket_page_urls),
        docs.FLAT_FILES: (snapshot.flat_file_pages, snapshot.flat_file_page_urls),
    }


def _page_changes(before: Snapshot, after: Snapshot) -> Iterator[Drift]:
    """Compare the documentation pages of every area.

    Args:
        before: The vendored snapshot.
        after: The live sources.

    Yields:
        A change per page the index added or dropped, and per fact a page states differently.
    """
    old_areas, new_areas = _area_pages(before), _area_pages(after)
    for area, (new_pages, new_urls) in new_areas.items():
        old_pages, old_urls = old_areas[area]
        for name in sorted(old_pages.keys() | new_pages.keys()):
            url = new_urls.get(name) or old_urls[name]
            if name not in old_pages:
                details = ["new in `llms.txt`"]
            elif name not in new_pages:
                details = ["removed from `llms.txt`"]
            else:
                details = list(
                    _changed(_page_facts(area, old_pages[name]), _page_facts(area, new_pages[name]))
                )
            yield from (
                Drift(section=Section.CHANGES, area="documentation", item=url, detail=detail)
                for detail in details
            )


def _documented_endpoints(snapshot: Snapshot) -> set[str]:
    """Return every endpoint a REST page of the snapshot documents.

    Args:
        snapshot: A snapshot.

    Returns:
        The path templates; a page that cannot be read is left out (a gate reports it).
    """
    found: set[str] = set()
    for page in snapshot.pages.values():
        try:
            path = docs.endpoint(page)
        except docs.DocsError:
            continue
        if path is not None:
            found.add(path)
    return found


def _documented_feeds(snapshot: Snapshot) -> set[str]:
    """Return every feed a WebSocket page of the snapshot documents.

    Args:
        snapshot: A snapshot.

    Returns:
        The keys (`"stocks/T"`); a page that cannot be read is left out (a gate reports it).
    """
    found: set[str] = set()
    for page in snapshot.websocket_pages.values():
        try:
            feed = docs.feed(page)
        except docs.DocsError:
            continue
        if feed is not None:
            found.add("/".join(feed))
    return found


def _overview_rows(snapshot: Snapshot) -> list[tuple[str, str]]:
    """Return every dataset row of the snapshot's flat-file overview pages.

    Args:
        snapshot: A snapshot.

    Returns:
        `(page url, prefix)` per row, pages in name order.
    """
    return [
        row for _, page in sorted(snapshot.flat_file_pages.items()) for row in docs.datasets(page)
    ]


def _unaccounted(
    after: Snapshot, *, committed: Committed, hand: HandInput, live: Mapping[Transport, set[str]]
) -> Iterator[Drift]:
    """Find what the live documentation names that nothing in the repository accounts for.

    Args:
        after: The live sources.
        committed: The keys the committed generated files hold.
        hand: The hand input, for its exclusions and venues.
        live: Per transport, the paths or channel keys the live specifications mark
            deprecated or draft.

    Yields:
        An unaccounted item per documented endpoint, feed, or dataset prefix neither generated
        nor excluded nor accounted for by the vendor, and per dataset page no overview row
        names; and a deprecation per committed flat-file prefix no overview names.
    """
    excluded = {kind: {e.item for e in hand.exclusions if e.kind is kind} for kind in Transport}
    excluded_feeds = {ws.channel_key(path) for path in excluded[Transport.WS]}
    for area, documented, accounted in (
        (
            "REST",
            _documented_endpoints(after),
            committed.rest | excluded[Transport.REST] | live[Transport.REST],
        ),
        ("WebSocket", _documented_feeds(after), committed.ws | excluded_feeds | live[Transport.WS]),
    ):
        for item in sorted(documented - accounted):
            yield Drift(
                section=Section.UNACCOUNTED,
                area=area,
                item=item,
                detail="documented, but neither generated nor excluded",
            )
    rows = _overview_rows(after)
    templates = {venue_template(prefix, list(hand.venues))[0] for _, prefix in rows}
    for template in sorted(templates - committed.flat_files - excluded[Transport.FLAT_FILES]):
        yield Drift(
            section=Section.UNACCOUNTED,
            area="flat files",
            item=template,
            detail="an overview names it, but it is neither generated nor excluded",
        )
    for template in sorted(committed.flat_files - templates):
        yield Drift(
            section=Section.DEPRECATIONS,
            area="flat files",
            item=template,
            detail="no overview names it any more, but `flatfiles.json` still has it",
        )
    named = {url for url, _ in rows}
    for name in sorted(after.flat_file_pages):
        url = after.flat_file_page_urls[name]
        if PurePosixPath(name).stem not in _OVERVIEW_PAGES and url not in named:
            yield Drift(
                section=Section.UNACCOUNTED,
                area="flat files",
                item=url,
                detail="a dataset page no overview row names",
            )


def _not_generated(
    operations: Mapping[str, _Operation | str], key: Callable[[str], str | None]
) -> set[str]:
    """Return the keys of the paths a specification marks deprecated or draft.

    Args:
        operations: A specification's operations by path.
        key: Maps a path to its key (`ws.channel_key` for channels).

    Returns:
        The keys.
    """
    return {
        found
        for path, operation in operations.items()
        if isinstance(operation, _Operation)
        and operation.status in _NOT_GENERATED
        and (found := key(path)) is not None
    }


def compare(
    before: Snapshot, after: Snapshot, *, committed: Committed, hand: HandInput
) -> list[Drift]:
    """Compare the live vendor sources with the vendored snapshot and the committed schemas.

    Pure: it reads only its arguments. The refresh gates are `gate_stops`'s and the bucket
    `unnamed_prefixes`'s; `check` adds both.

    Args:
        before: The vendored snapshot.
        after: The live sources, loaded as a snapshot.
        committed: The keys the committed `spec/*.json` hold.
        hand: The hand input, for its exclusions and venues.

    Returns:
        Every drift item, in report order; empty when the live sources match.
    """
    found = [
        *_specification_changes(
            "REST",
            before.openapi,
            after.openapi,
            generated=committed.rest.__contains__,
            file="rest.json",
        ),
        *_specification_changes(
            "WebSocket",
            before.websocket,
            after.websocket,
            generated=lambda path: ws.channel_key(path) in committed.ws,
            file="ws.json",
        ),
        *_page_changes(before, after),
    ]
    live = {
        Transport.REST: _not_generated(_operations(after.openapi), lambda path: path),
        Transport.WS: _not_generated(_operations(after.websocket), ws.channel_key),
    }
    found += _unaccounted(after, committed=committed, hand=hand, live=live)
    return _ordered(found)


def gate_stops(after: Snapshot, hand: HandInput) -> list[Drift]:
    """Run every refresh gate on the live sources, as a refresh after `--fetch` would.

    Args:
        after: The live sources, loaded as a snapshot.
        hand: The hand input.

    Returns:
        One change per problem a gate would stop on, in report order; empty when the live
        sources would regenerate cleanly.
    """
    try:
        generate.generate(after, hand)
    except RefreshError as error:
        return _ordered(
            Drift(
                section=Section.CHANGES,
                area="refresh",
                item=problem.source,
                detail=f"the {problem.gate} gate would stop"
                + (f" on {_code(problem.key)}" if problem.key else "")
                + f": {_code(problem.message)}",
            )
            for problem in error.problems
        )
    return []


def unnamed_prefixes(
    prefixes: Sequence[str], after: Snapshot, *, committed: Committed, hand: HandInput
) -> list[Drift]:
    """Find the bucket's top-level prefixes that nothing names.

    A prefix is named when it is the first segment of a prefix a live overview row names, of
    a committed flat-file source, or of a flat-file exclusion, with each `{venue}` template
    filled with every venue.

    Args:
        prefixes: The bucket's top-level prefixes (`FlatFileClient.prefixes`).
        after: The live sources.
        committed: The keys the committed `spec/*.json` hold.
        hand: The hand input, for its exclusions and venues.

    Returns:
        An unaccounted item per prefix nothing names, in report order.
    """
    known = [prefix for _, prefix in _overview_rows(after)]
    known += committed.flat_files
    known += [e.item for e in hand.exclusions if e.kind is Transport.FLAT_FILES]
    named = {
        prefix.partition("/")[0].replace("{venue}", venue)
        for prefix in known
        for venue in (*hand.venues, "")
    }
    return _ordered(
        Drift(
            section=Section.UNACCOUNTED,
            area="bucket",
            item=prefix,
            detail="a bucket prefix no overview row, generated source, or exclusion names",
        )
        for prefix in prefixes
        if prefix not in named
    )


def _ordered(items: Iterable[Drift]) -> list[Drift]:
    """Put drift items in report order.

    Args:
        items: The items.

    Returns:
        The items sorted by section (in `Section` order), area, item, and detail.
    """
    order = list(Section)
    return sorted(items, key=lambda d: (order.index(d.section), d.area, d.item, d.detail))


def check(
    *,
    snapshot_directory: Path | None = None,
    get: Callable[[str], bytes] | None = None,
    environ: Mapping[str, str] | None = None,
    committed: Committed | None = None,
    hand: HandInput | None = None,
) -> Report:
    """Download the live vendor sources and report how they differ from the repository.

    Network: every download `--fetch` makes (through `get`) and, when both S3 credentials are
    set, a listing of the flat-file bucket's top-level prefixes. Blocks for all of it. The
    downloads go to a scratch directory that is removed afterwards; the vendored snapshot and
    the generated files are only read.

    A `fetch.FetchError` (a download failed), a `RefreshError` (the vendored snapshot is
    missing or does not match its manifest, or the live sources cannot be loaded), and a
    `WireError` from listing the bucket propagate: the check could not be made.

    Args:
        snapshot_directory: The vendored snapshot; `snapshot.DIRECTORY` when omitted.
        get: Returns the body of a URL; an `fetch.HttpGetter` when omitted.
        environ: Where the S3 credentials are read (`S3_CREDENTIAL_VARIABLES`);
            `os.environ` when omitted. Without both, the bucket is not listed.
        committed: The committed keys; `Committed.load()` when omitted.
        hand: The hand input; the real tables when omitted.

    Returns:
        The report.
    """
    environ = os.environ if environ is None else environ
    committed = committed or Committed.load()
    hand = hand or HandInput()
    before = Snapshot.load(snapshot_directory)
    with tempfile.TemporaryDirectory(prefix="massive-drift-") as scratch:
        directory = Path(scratch) / "massive"
        fetch.fetch(directory, get=get)
        after = Snapshot.load(directory)
    items = compare(before, after, committed=committed, hand=hand) + gate_stops(after, hand)
    access_key, secret_key = (environ.get(name, "").strip() for name in S3_CREDENTIAL_VARIABLES)
    listed = bool(access_key and secret_key)
    if listed:
        client = FlatFileClient(
            access_key_id=access_key,
            secret_access_key=secret_key,
            endpoint_url=S3_ENDPOINT,
            bucket=S3_BUCKET,
            max_pool=1,
        )
        try:
            prefixes = client.prefixes()
        finally:
            client.close()
        items += unnamed_prefixes(prefixes, after, committed=committed, hand=hand)
    return Report(items=tuple(_ordered(items)), fetched=before.fetched, bucket_listed=listed)
