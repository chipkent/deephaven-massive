"""Tests for the canonical text of `spec/rest.json`: one line per field and per parameter."""

from __future__ import annotations

import json

from deephaven_massive._massive.refresh import render
from deephaven_massive._massive.spec.types import (
    Field,
    Param,
    ParamKind,
    RestPath,
    WireType,
    WireUnit,
)

_PATH = RestPath(
    key="/v1/things/{id}",
    result_key="results",
    params=(
        Param(name="id", kind=ParamKind.STRING, in_path=True),
        Param(name="timestamp.gte", kind=ParamKind.TIMESTAMP, unit=WireUnit.MS),
    ),
    fields=(
        Field(wire_key="price", column="Price", wire_type=WireType.DOUBLE),
        Field(
            wire_key="t",
            column="Timestamp",
            wire_type=WireType.EPOCH,
            unit=WireUnit.NS,
            required=True,
        ),
    ),
)


def test_render_writes_one_line_per_field_and_parameter() -> None:
    text = render.render_rest([_PATH, _PATH], openapi_sha256="f" * 64)
    document = json.loads(text)
    assert document["openapi_sha256"] == "f" * 64
    assert document["generated_by"] == "python -m deephaven_massive._massive.refresh"
    assert document["sources"][1]["fields"][0] == {
        "column": "Price",
        "conversion": None,
        "required": False,
        "type": "double",
        "unit": None,
        "wire_key": "price",
    }
    assert document["sources"][0]["params"][1] == {
        "in_path": False,
        "kind": "timestamp",
        "name": "timestamp.gte",
        "unit": "ms",
        "values": [],
    }
    lines = text.splitlines()
    assert '    {"column": "Price", "conversion": null, "required": false, ' in text
    assert sum('"wire_key"' in line for line in lines) == 4
    assert sum('"in_path"' in line for line in lines) == 4
    assert text.endswith("}\n")


def test_render_of_no_paths_is_an_empty_source_list() -> None:
    assert json.loads(render.render_rest([], openapi_sha256="0" * 64))["sources"] == []


def test_the_rest_file_is_the_spec_package_data() -> None:
    assert render.REST_FILE.parent.name == "spec"
    assert render.REST_FILE.name == "rest.json"
