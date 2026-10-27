"""Tests for reading a specification's paths as the drift check compares them.

The drift report built from these is tested in `test_drift.py`; here each piece is shown on
its own: the quoting, the type signature, the flattened fields, the parameters, the `ev`
values, and the descriptions of a change.
"""

from __future__ import annotations

from typing import Any

import pytest

from deephaven_massive._massive.refresh import spec_diff
from deephaven_massive._massive.refresh.openapi import OpenApi
from deephaven_massive._massive.refresh.spec_diff import Leaf, Operation


def document(fields: dict[str, Any], parameters: tuple[dict[str, Any], ...] = ()) -> dict[str, Any]:
    # A specification of one path whose 200 response is an object of `fields`, `a` required.
    schema = {"type": "object", "required": ["a"], "properties": fields}
    operation = {"responses": {"200": {"content": {"application/json": {"schema": schema}}}}}
    return {"paths": {"/v1/x": {"get": {**operation, "parameters": list(parameters)}}}}


@pytest.mark.parametrize(
    ("value", "quoted"),
    [("`@team` #1\n[x](y)", "`'@team' #1 [x](y)`"), ("", "`(none)`"), (" \t", "`(none)`")],
)
def test_quote_keeps_vendor_text_inside_one_code_span(value: str, quoted: str) -> None:
    assert spec_diff.quote(value) == quoted


@pytest.mark.parametrize(
    ("schema", "text"),
    [
        ({"type": "string", "format": "date"}, "string (date)"),
        ({"type": "string", "enum": ["b", "a"]}, "string enum [a, b]"),
        ({"type": "integer", "enum": [2, 10]}, "integer enum [10, 2]"),
        ({"type": "array", "items": {"type": "number"}}, "array of number"),
        ({"type": "array"}, "array of untyped"),
        ({"properties": {"b": {"type": "string"}, "a": {}}}, "untyped {a: untyped, b: string}"),
        ({"anyOf": [{"type": "string"}, {"type": "boolean"}]}, "anyOf of [boolean | string]"),
        ({"type": ["string", "null"]}, "untyped"),
    ],
)
def test_type_text_is_a_semantic_signature(schema: dict[str, Any], text: str) -> None:
    assert spec_diff.type_text(OpenApi({}), schema, where="/v1/x") == text


def test_type_text_ignores_descriptions_examples_and_order() -> None:
    first: dict[str, Any] = {
        "oneOf": [{"type": "string", "description": "a"}, {"type": "number", "example": 1}]
    }
    second: dict[str, Any] = {"oneOf": [{"type": "number"}, {"type": "string"}]}
    spec = OpenApi({})
    assert spec_diff.type_text(spec, first, where="") == spec_diff.type_text(spec, second, where="")


def test_type_text_stops_at_the_nesting_bound() -> None:
    spec = OpenApi({"components": {"L": {"type": "array", "items": {"$ref": "#/components/L"}}}})
    text = spec_diff.type_text(spec, spec.schema({"$ref": "#/components/L"}, ""), where="")
    assert text == "array of " * spec_diff.MAX_NESTING + "(nested too deep)"


def test_operations_read_fields_parameters_requiredness_and_events() -> None:
    fields = {
        "a": {"type": "integer", "x-polygon-go-type": {"name": "IMilliseconds"}},
        "ev": {"type": "string", "enum": ["T"]},
        "obj": {"type": "object", "properties": {"c": {"type": "number"}}},
    }
    parameter = {"name": "limit", "in": "query", "required": True, "schema": {"type": "integer"}}
    (operation,) = spec_diff.operations(document(fields, (parameter,))).values()
    assert operation == Operation(
        status="current",
        parameters={"limit": "query integer, required"},
        fields={
            "a": Leaf("integer", "IMilliseconds", required=True),
            "ev": Leaf("string enum [T]", None, required=False),
            "obj.c": Leaf("number", None, required=False),
        },
        events=("T",),
    )


def test_an_operation_without_a_body_or_events() -> None:
    spec: dict[str, Any] = {"paths": {"/v1/x": {"get": {}}, "/v1/y": {"get": {"deprecated": True}}}}
    assert spec_diff.operations(spec) == {
        "/v1/x": Operation(status="current", parameters={}, fields={}),
        "/v1/y": Operation(status="deprecated", parameters={}, fields={}),
    }
    (operation,) = spec_diff.operations(document({"a": {"type": "string"}})).values()
    assert isinstance(operation, Operation)
    assert operation.events is None


def test_an_unreadable_operation_is_its_reason_and_no_paths_is_none() -> None:
    assert spec_diff.operations({"paths": {"/v1/x": {"$ref": "#/nowhere"}}}) == {
        "/v1/x": "reference '#/nowhere' names nothing"
    }
    assert spec_diff.operations({"paths": []}) == {}


def test_operation_changes_describe_parameters_then_fields() -> None:
    before = Operation(
        status="current",
        parameters={"p": "query string"},
        fields={
            "a": Leaf("integer", None, required=True),
            "b": Leaf("string", None, required=False),
        },
    )
    after = Operation(
        status="deprecated",
        parameters={"p": "query integer"},
        fields={
            "a": Leaf("number", "INanoseconds", required=False),
            "c": Leaf("string", None, required=True),
        },
    )
    assert spec_diff.operation_changes(before, after) == [
        "parameter `p` changed from `query string` to `query integer`",
        "field `b` removed (was `string`)",
        "field `c` added: `string`",
        "field `a` retyped from `integer` to `number`",
        "field `a` no longer required",
        "field `a` unit tag changed from `none` to `INanoseconds`",
    ]
    assert spec_diff.operation_changes(before, before) == []


def test_changed_and_labelled() -> None:
    old = spec_diff.labelled("column", {"x": "integer", "y": "string"})
    new = spec_diff.labelled("column", {"x": "number", "z": "string"})
    assert list(spec_diff.changed(old, new)) == [
        "column `x` changed from `integer` to `number`",
        "column `y` removed (was `string`)",
        "column `z` added: `string`",
    ]
