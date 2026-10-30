"""Flatten the paths of an OpenAPI specification and describe how two copies differ.

The drift check (`drift`) compares the vendored REST and WebSocket specifications with their
live copies. This module owns the part of that comparison that reads one specification: it
turns every path (a REST endpoint, or a WebSocket channel, since the WebSocket specification
is an OpenAPI document whose paths are its channels) into an `Operation`, the facts a change
to which matters, and it describes the difference between two operations in words. `drift`
decides which differences are drift of which kind, and words the report around them.

**What is compared.** An operation's status (`rest_sources.path_status`), its parameters (location,
type, whether required), and its fields: the dotted keys below its 200 JSON response
(`results[].day.c`), each with a semantic type signature (`type_text`: type, format, allowed
values, array items, object properties, choice branches), its unit tag
(`x-polygon-go-type`), and whether it is required as the generators decide it. A WebSocket
message's `ev` values are kept apart, so `drift` can check them against the channel.
Descriptions, examples, property order, and branch order are not compared, so a reworded or
reordered document is no change. Every traversal is bounded (`MAX_NESTING`), so a schema that
refers to itself is described to that depth instead of recursing forever.

**Quoting.** Every vendor value in a description is quoted as Markdown code by `quote`, which
replaces backticks and collapses whitespace, so a description is safe in a GitHub issue body:
no vendor string becomes a mention, a reference, a link, or markup.

It imports the standard library and the rest of `refresh` only, and reads no file or network.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, NamedTuple

from deephaven_massive._massive.refresh import rest_sources
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json, JsonObject

__all__ = [
    "MAX_NESTING",
    "Leaf",
    "Operation",
    "changed",
    "labelled",
    "operation_changes",
    "operations",
    "quote",
    "type_text",
]

MAX_NESTING: Final = 16
"""How deep schemas nest before the rest is described as `"(nested too deep)"`.

Massive's deepest response nests five levels; the bound only stops a schema that refers to
itself."""

# Where an operation's response schema sits below it.
_BODY: Final = ("responses", "200", "content", "application/json", "schema")
# The key of a WebSocket message that names its channel.
_EVENT_KEY: Final = "ev"


class Leaf(NamedTuple):
    """One field of a response or message, as the drift check compares it.

    Attributes:
        type: Its type signature (`type_text`).
        tag: Its unit tag, or `None`.
        required: Whether it is required, as the generators decide it: listed as required by
            its object and by every enclosing object up to the row.
    """

    type: str
    tag: str | None
    required: bool


@dataclass(frozen=True)
class Operation:
    """What the drift check compares of one path or channel of a specification.

    Created by `operations`. Immutable.

    Attributes:
        status: `"deprecated"`, `"draft"`, or `"current"`.
        parameters: Each parameter's name mapped to its location, type signature, and whether
            it is required.
        fields: Each dotted field key below the body mapped to its leaf.
        events: The values the body's `ev` key may take, in the specification's order, or
            `None` when the body has no `ev` key or it lists no values.
    """

    status: str
    parameters: Mapping[str, str]
    fields: Mapping[str, Leaf]
    events: tuple[str, ...] | None = None


def quote(value: str) -> str:
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


def type_text(spec: OpenApi, schema: JsonObject, *, where: str, depth: int = 0) -> str:
    """Describe a resolved schema's type as the report shows and compares it.

    The text is a semantic signature: two schemas get the same text exactly when their types,
    formats, allowed values, array items, object properties, and choice branches agree,
    whatever their descriptions, examples, property order, or branch order.

    A `RefreshError` from resolving a nested schema (a reference that cannot be followed)
    propagates.

    Args:
        spec: The specification.
        schema: A resolved schema.
        where: The path, for messages.
        depth: How many schemas enclose this one, counting from the body.

    Returns:
        The type and format (`"string (date-time)"`), then the allowed values in text order
        (`"string enum [A, B]"`); `"array of <item type>"`; an object's properties in name
        order (`"object {c: number, o: number}"`); a choice's branches in text order
        (`"oneOf of [number | string]"`); `"untyped"` for a schema without a type; or
        `"(nested too deep)"` once the nesting reaches `MAX_NESTING`.
    """
    if depth >= MAX_NESTING:
        return "(nested too deep)"

    def inner(node: Json) -> str:
        return type_text(spec, spec.schema(node, where), where=where, depth=depth + 1)

    for choice in ("oneOf", "anyOf"):
        branches = schema.get(choice)
        if isinstance(branches, list):
            return f"{choice} of [{' | '.join(sorted(inner(branch) for branch in branches))}]"
    kind = schema.get("type")
    text = kind if isinstance(kind, str) else "untyped"
    form = schema.get("format")
    if isinstance(form, str):
        text += f" ({form})"
    enum = schema.get("enum")
    if isinstance(enum, list):
        text += f" enum [{', '.join(sorted(str(value) for value in enum))}]"
    if kind == "array":
        text += f" of {inner(schema.get('items', {}))}"
    found = properties(schema)
    if found is not None:
        text += f" {{{', '.join(f'{name}: {inner(found[name])}' for name in sorted(found))}}}"
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
) -> Iterator[tuple[str, Leaf]]:
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
        Each leaf's key (`"(body)"` for a body that is itself a leaf) and its leaf.
    """
    schema = spec.schema(node, where)
    found = properties(schema)
    if depth < MAX_NESTING and found is not None:
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
    if depth < MAX_NESTING and schema.get("type") == "array":
        items = spec.schema(schema.get("items", {}), where)
        if properties(items) is not None:
            yield from _leaves(spec, items, prefix=f"{prefix}[]", where=where, depth=depth + 1)
            return
    leaf = Leaf(type_text(spec, schema, where=where, depth=depth), _tag(schema), is_required)
    yield prefix or "(body)", leaf


def _parameter(spec: OpenApi, node: Json, where: str) -> tuple[str, str]:
    """Describe one parameter as the report compares it.

    Args:
        spec: The specification.
        node: The parameter object, possibly a reference.
        where: The path, for messages.

    A `RefreshError` from resolving it (a reference that cannot be followed) propagates.

    Returns:
        Its name, and its location, type signature (allowed values included), and whether it
        is required.
    """
    parameter = spec.deref(node, where)
    schema = spec.schema(parameter.get("schema", {}), where)
    text = f"{parameter.get('in')} {type_text(spec, schema, where=where)}"
    if parameter.get("required") is True:
        text += ", required"
    return str(parameter.get("name")), text


def _events(spec: OpenApi, body: Json, where: str) -> tuple[str, ...] | None:
    """Return the values a message's `ev` key may take.

    Args:
        spec: The specification.
        body: The body schema, possibly a reference.
        where: The path, for messages.

    A `RefreshError` from resolving a schema propagates.

    Returns:
        The `enum` of the body's `ev` property, as text, or `None` when there is none.
    """
    found = properties(spec.schema(body, where)) or {}
    if _EVENT_KEY not in found:
        return None
    enum = spec.schema(found[_EVENT_KEY], where).get("enum")
    return tuple(str(value) for value in enum) if isinstance(enum, list) else None


def _operation(spec: OpenApi, path: str, node: Json) -> Operation | str:
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
        body = _walk(spec, operation, _BODY, where=path)
        fields = {} if body is None else dict(_leaves(spec, body, prefix="", where=path))
        events = None if body is None else _events(spec, body, path)
    except RefreshError as error:
        return "; ".join(problem.message for problem in error.problems)
    return Operation(
        status=rest_sources.path_status(item) or "current",
        parameters=dict(parameters),
        fields=fields,
        events=events,
    )


def operations(document: JsonObject) -> dict[str, Operation | str]:
    """Read every path or channel of a specification.

    Args:
        document: An OpenAPI specification: the REST one, or the WebSocket one, whose paths
            are its channels.

    Returns:
        Each path mapped to its operation or the reason it cannot be read; empty when the
        document has no `paths` object.
    """
    spec = OpenApi(document)
    paths = document.get("paths")
    found = paths if isinstance(paths, dict) else {}
    return {path: _operation(spec, path, node) for path, node in found.items()}


def changed(old: Mapping[str, str], new: Mapping[str, str]) -> Iterator[str]:
    """Describe how one mapping of labelled values changed.

    Args:
        old: The snapshot's values, by a label that is already Markdown with every vendor
            name in it quoted (`labelled`).
        new: The live values, by the same labels.

    Yields:
        One description per label added, removed, or whose value changed, in label order.
    """
    for label in sorted(old.keys() | new.keys()):
        before, after = old.get(label), new.get(label)
        if before is None:
            yield f"{label} added: {quote(str(after))}"
        elif after is None:
            yield f"{label} removed (was {quote(before)})"
        elif before != after:
            yield f"{label} changed from {quote(before)} to {quote(after)}"


def labelled(what: str, values: Mapping[str, str]) -> dict[str, str]:
    """Label each vendor name of a mapping for `changed`.

    Args:
        what: What a name is, such as `"field"`.
        values: Values by vendor name.

    Returns:
        The values by `"<what> <name as code>"`.
    """
    return {f"{what} {quote(name)}": value for name, value in values.items()}


def _field_changes(old: Mapping[str, Leaf], new: Mapping[str, Leaf]) -> Iterator[str]:
    """Describe how a response's or message's fields changed.

    Args:
        old: The snapshot's fields.
        new: The live fields.

    Yields:
        One description per field added or removed, and per field whose type, requiredness,
        or unit tag changed.
    """
    yield from changed(
        labelled("field", {key: leaf.type for key, leaf in old.items() if key not in new}),
        labelled("field", {key: leaf.type for key, leaf in new.items() if key not in old}),
    )
    for key in sorted(old.keys() & new.keys()):
        before, after = old[key], new[key]
        if before.type != after.type:
            yield f"field {quote(key)} retyped from {quote(before.type)} to {quote(after.type)}"
        if before.required != after.required:
            now = "now required" if after.required else "no longer required"
            yield f"field {quote(key)} {now}"
        if before.tag != after.tag:
            yield (
                f"field {quote(key)} unit tag changed from {quote(before.tag or 'none')} to "
                f"{quote(after.tag or 'none')}"
            )


def operation_changes(before: Operation, after: Operation) -> list[str]:
    """Describe how one path's or channel's parameters and fields changed.

    The status is not compared here: what a status change means depends on what the
    repository generates, which is `drift`'s to judge.

    Args:
        before: The snapshot's operation.
        after: The live operation.

    Returns:
        One description per parameter added, removed, or changed, then one per field change
        (`_field_changes`); empty when they agree.
    """
    return [
        *changed(labelled("parameter", before.parameters), labelled("parameter", after.parameters)),
        *_field_changes(before.fields, after.fields),
    ]
