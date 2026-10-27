"""Tests for OpenAPI resolution: references, `allOf` merges, one-branch choices, and accessors.

Each resolution failure is a `shape` problem, never a guess; `test_rest.py` shows the
generator reporting them per path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required, text

if TYPE_CHECKING:
    from deephaven_massive._massive.refresh.openapi import Json, JsonObject

_SCHEMAS: dict[str, JsonObject] = {
    "Bar": {"type": "object", "properties": {"close": {"type": "number"}}},
    "Alias": {"$ref": "#/components/schemas/Bar"},
    "Base": {
        "type": "object",
        "properties": {"ticker": {"type": "string"}},
        "required": ["ticker"],
    },
    "Loop": {"$ref": "#/components/schemas/Loop"},
    "Deep": {"allOf": [{"$ref": "#/components/schemas/Deep"}]},
}
_SPEC = OpenApi({"components": {"schemas": _SCHEMAS}})


def test_deref_follows_a_chain_of_local_references() -> None:
    assert _SPEC.deref({"$ref": "#/components/schemas/Alias"}, "w") == _SCHEMAS["Bar"]
    assert _SPEC.deref({"type": "string"}, "w") == {"type": "string"}


@pytest.mark.parametrize(
    ("node", "message"),
    [
        ("not an object", "expected a JSON object"),
        ({"$ref": "https://example.com/x"}, "unsupported reference"),
        ({"$ref": "#/components/schemas/Missing"}, "names nothing"),
        ({"$ref": "#/components/schemas/Loop"}, "reference chain too long"),
    ],
)
def test_deref_reports_what_it_cannot_follow(node: Json, message: str) -> None:
    with pytest.raises(RefreshError) as caught:
        _SPEC.deref(node, "/v1/x price")
    (problem,) = caught.value.problems
    assert (problem.gate, problem.source) == ("shape", "/v1/x price")
    assert message in problem.message


def test_schema_merges_all_of_parts_in_order() -> None:
    node: Json = {
        "allOf": [
            {"$ref": "#/components/schemas/Base"},
            {
                "type": "object",
                "description": "second",
                "properties": {"ticker": {"type": "integer"}, "day": {"type": "number"}},
                "required": ["day", "ticker"],
            },
        ]
    }
    assert _SPEC.schema(node, "w") == {
        "type": "object",
        "description": "second",
        "properties": {"ticker": {"type": "integer"}, "day": {"type": "number"}},
        "required": ["ticker", "day"],
    }


def test_schema_unwraps_a_one_branch_choice_only() -> None:
    one: Json = {"oneOf": [{"$ref": "#/components/schemas/Bar"}]}
    assert _SPEC.schema(one, "w") == _SCHEMAS["Bar"]
    two: Json = {"anyOf": [{"type": "string"}, {"type": "number"}]}
    assert _SPEC.schema(two, "w") == two


_DESCRIBED: dict[str, JsonObject] = {
    "Price": {"type": "number", "description": "A price."},
    "Priced": {"allOf": [{"$ref": "#/components/schemas/Price"}], "description": "The bid."},
}


@pytest.mark.parametrize(
    ("node", "description"),
    [
        pytest.param(
            {"$ref": "#/components/schemas/Price", "description": "The ask."},
            "The ask.",
            id="beside a reference",
        ),
        pytest.param(
            {"allOf": [{"$ref": "#/components/schemas/Price"}], "description": "The close."},
            "The close.",
            id="beside an allOf",
        ),
        pytest.param(
            {"anyOf": [{"$ref": "#/components/schemas/Price"}], "description": "The open."},
            "The open.",
            id="beside a one-branch choice",
        ),
        pytest.param(
            {"$ref": "#/components/schemas/Priced"},
            "The bid.",
            id="beside the allOf a reference names",
        ),
        pytest.param(
            {"$ref": "#/components/schemas/Priced", "description": "The last."},
            "The last.",
            id="the nearest of several",
        ),
        pytest.param({"$ref": "#/components/schemas/Price"}, "A price.", id="none beside"),
    ],
)
def test_schema_the_nearest_description_wins(node: Json, description: str) -> None:
    spec = OpenApi({"components": {"schemas": _DESCRIBED}})
    assert spec.schema(node, "w")["description"] == description
    assert _DESCRIBED["Price"]["description"] == "A price."  # nothing is changed in place


def test_schema_keeps_the_type_of_a_scalar_all_of() -> None:
    node: Json = {"allOf": [{"type": "integer", "format": "int64"}, {"minimum": 0}]}
    assert _SPEC.schema(node, "w") == {"type": "integer", "format": "int64", "minimum": 0}


def test_schema_keeps_the_type_of_a_referenced_scalar_with_a_description() -> None:
    spec = OpenApi({"components": {"schemas": _DESCRIBED}})
    beside: Json = {"allOf": [{"$ref": "#/components/schemas/Price"}], "description": "The high."}
    assert spec.schema(beside, "w") == {"type": "number", "description": "The high."}
    # A description in a later part does not replace the earlier part's; only the wrapper's does.
    part: Json = {"allOf": [{"$ref": "#/components/schemas/Price"}, {"description": "The low."}]}
    assert spec.schema(part, "w") == {"type": "number", "description": "A price."}


def test_schema_reports_all_of_scalar_parts_of_conflicting_types() -> None:
    node: Json = {"allOf": [{"type": "integer"}, {"minimum": 0}, {"type": "string"}]}
    with pytest.raises(RefreshError) as caught:
        _SPEC.schema(node, "/v1/x price")
    (problem,) = caught.value.problems
    assert (problem.gate, problem.source) == ("shape", "/v1/x price")
    assert problem.message == "allOf parts disagree on the type: 'integer', 'string'"


@pytest.mark.parametrize(
    ("parts", "names"),
    [
        pytest.param(
            [{"type": "string", "description": "Status."}, {"$ref": "#/components/schemas/Base"}],
            ["ticker"],
            id="a scalar part beside an object part",
        ),
        pytest.param(
            [{"description": "Status."}, {"properties": {"ticker": {"type": "string"}}}],
            [],
            id="an untyped part with properties",
        ),
    ],
)
def test_schema_an_all_of_with_an_object_part_is_an_object(
    parts: list[Json], names: list[Json]
) -> None:
    assert _SPEC.schema({"allOf": parts}, "w") == {
        "type": "object",
        "description": "Status.",
        "properties": {"ticker": {"type": "string"}},
        "required": names,
    }


def test_schema_an_all_of_without_a_type_is_an_object() -> None:
    node: Json = {"allOf": [{"description": "Empty."}, {"minimum": 0}]}
    assert _SPEC.schema(node, "w") == {
        "type": "object",
        "description": "Empty.",
        "minimum": 0,
        "properties": {},
        "required": [],
    }


def test_schema_stops_on_a_cyclic_all_of() -> None:
    with pytest.raises(RefreshError, match="nesting too deep"):
        _SPEC.schema({"$ref": "#/components/schemas/Deep"}, "w")


def test_properties_of_object_schemas_only() -> None:
    assert properties(_SCHEMAS["Bar"]) == {"close": {"type": "number"}}
    assert properties({"properties": {"a": {}}}) == {"a": {}}  # untyped, with properties
    assert properties({"type": "object", "properties": {}}) is None
    assert properties({"type": "array", "properties": {"a": {}}}) is None
    assert properties({"type": "object"}) is None


def test_required_keeps_only_names() -> None:
    assert required({"required": ["a", 1, "b"]}) == {"a", "b"}
    assert required({"required": "a"}) == frozenset()
    assert required({}) == frozenset()


def test_text_reads_only_strings() -> None:
    assert text({"description": "The price."}, "description") == "The price."
    assert text({"description": 3}, "description") == ""
    assert text({}, "description") == ""
