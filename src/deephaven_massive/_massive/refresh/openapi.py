"""Read-only access to an OpenAPI document: `$ref`, `allOf`, one-branch choices, and accessors.

The vendor's specification is untrusted data. `OpenApi` resolves the indirections a generator
meets on the way to a schema (local `$ref`s, `allOf` merges, a `oneOf` or `anyOf` with one
branch), checking the shape it expects at every step and reporting a `shape` problem instead of
guessing; a cyclic document stops after a fixed depth. The small functions below read one
attribute of a resolved schema without trusting its type.

The REST generator (`rest`) is the first reader; the WebSocket specification uses the same
schema language, so its generator reads it through this module too. It imports the standard
library and `gates` only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._massive.refresh.gates import Gate, Problem, RefreshError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["Json", "JsonObject", "OpenApi", "properties", "required", "shape_problem", "text"]

# A JSON value as the standard library decodes it.
Json: TypeAlias = "Mapping[str, Json] | list[Json] | str | int | float | bool | None"
JsonObject: TypeAlias = "Mapping[str, Json]"

# How deep `$ref` chains and nested `allOf` parts may go before the document is taken to be
# cyclic.
_MAX_DEPTH: Final = 64


def shape_problem(where: str, message: str) -> Problem:
    """Return the problem for a specification the generator cannot read.

    Args:
        where: The path and key concerned.
        message: What is wrong.

    Returns:
        A `shape` problem.
    """
    return Problem(gate=Gate.SHAPE, source=where, key="", message=message)


class OpenApi:
    """A parsed OpenAPI document, with the resolution every generator needs.

    One instance wraps one decoded document for the length of a refresh; it holds no other
    state, and the refresh uses it from one thread.

    Example:
        ::

            from deephaven_massive._massive.refresh.openapi import OpenApi

            spec = OpenApi({"components": {"schemas": {"Bar": {"type": "object"}}}})
            spec.deref({"$ref": "#/components/schemas/Bar"}, "/v1/bars")  # {"type": "object"}
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
        for _ in range(_MAX_DEPTH):
            if not isinstance(node, dict):
                raise RefreshError([shape_problem(where, "expected a JSON object")])
            ref = node.get("$ref")
            if ref is None:
                return node
            if not isinstance(ref, str) or not ref.startswith("#/"):
                raise RefreshError([shape_problem(where, f"unsupported reference {ref!r}")])
            target: Json = self.document
            for part in ref[2:].split("/"):
                target = target.get(part) if isinstance(target, dict) else None
            if target is None:
                raise RefreshError([shape_problem(where, f"reference {ref!r} names nothing")])
            node = target
        raise RefreshError(
            [shape_problem(where, "reference chain too long; the specification may be cyclic")]
        )

    def schema(self, node: Json, where: str, depth: int = 0) -> JsonObject:
        """Resolve a schema: follow `$ref`, merge `allOf`, and unwrap a one-branch choice.

        `allOf` parts are merged in order: their properties (a later part's property replaces
        an earlier one of the same name), the union of their `required` lists, and their other
        keys, first part first. A `oneOf` or `anyOf` with exactly one branch is that branch;
        one with two or more branches is returned as is (the caller makes it `json-text`).

        A `description` written beside a `$ref`, an `allOf`, or a one-branch choice describes
        this use of the schema, so it replaces the one the resolved schema carries: the
        nearest wins (`{"$ref": "#/.../Price", "description": "The ask price."}` is described
        as the ask price, not by `Price`'s generic text). No other key of the wrapper is kept.

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
        if depth > _MAX_DEPTH:
            raise RefreshError([shape_problem(where, "schema nesting too deep; it may be cyclic")])
        resolved = self.deref(node, where)
        found = self._expand(resolved, where, depth)
        # The node's own description, else the one beside the `allOf` or choice it names. `deref`
        # has already refused a node that is not an object.
        for holder in (node, resolved):
            description = holder.get("description") if isinstance(holder, dict) else None
            if isinstance(description, str):
                if found.get("description") == description:
                    return found
                return {**found, "description": description}
        return found

    def _expand(self, resolved: JsonObject, where: str, depth: int) -> JsonObject:
        """Unwrap a one-branch choice and merge `allOf` parts, as `schema` describes.

        Args:
            resolved: A schema whose `$ref`s are already followed.
            where: The path and key, for messages.
            depth: How many `allOf` parts and one-branch choices enclose this one.

        Returns:
            The resolved schema; `resolved` itself when it is neither.
        """
        for choice in ("oneOf", "anyOf"):
            branches = resolved.get(choice)
            if isinstance(branches, list) and len(branches) == 1:
                return self.schema(branches[0], where, depth + 1)
        parts = resolved.get("allOf")
        if not isinstance(parts, list):
            return resolved
        merged: dict[str, Json] = {"type": "object"}
        merged_properties: dict[str, Json] = {}
        merged_required: list[Json] = []
        for part in parts:
            piece = self.schema(part, where, depth + 1)
            for key, value in piece.items():
                if key == "properties" and isinstance(value, dict):
                    merged_properties.update(value)
                elif key == "required" and isinstance(value, list):
                    merged_required += [name for name in value if name not in merged_required]
                else:
                    merged.setdefault(key, value)
        merged["properties"] = merged_properties
        merged["required"] = merged_required
        return merged


def properties(schema: JsonObject) -> JsonObject | None:
    """Return an object schema's properties, or `None` when it has none to flatten.

    Args:
        schema: A resolved schema.

    Returns:
        The non-empty `properties` mapping of an object schema (typed `object`, or untyped
        with properties), else `None`.
    """
    found = schema.get("properties")
    if schema.get("type", "object") != "object" or not isinstance(found, dict):
        return None
    return found or None


def required(schema: JsonObject) -> frozenset[str]:
    """Return the names an object schema lists as required.

    Args:
        schema: A resolved object schema.

    Returns:
        The string entries of its `required` list; empty when it has none.
    """
    names = schema.get("required")
    if not isinstance(names, list):
        return frozenset()
    return frozenset(name for name in names if isinstance(name, str))


def text(schema: JsonObject, key: str) -> str:
    """Return a string attribute of a schema, or `""`.

    Args:
        schema: A schema or parameter object.
        key: The attribute, such as `"description"`.

    Returns:
        The attribute when it is a string, else `""`.
    """
    value = schema.get(key)
    return value if isinstance(value, str) else ""
