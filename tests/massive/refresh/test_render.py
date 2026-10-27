"""Tests for the canonical text of `spec/rest.json` and `spec/descriptions.json`."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json

from deephaven_massive._massive.refresh import render
from deephaven_massive._massive.spec.types import (
    Field,
    Param,
    ParamKind,
    RestPath,
    Source,
    Transport,
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
        Field(
            wire_key="price",
            column="Price",
            wire_type=WireType.DOUBLE,
            description="The price.",
        ),
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
    text = render.render_rest([_PATH, _PATH])
    document = json.loads(text)
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


def test_rest_json_holds_neither_descriptions_nor_the_snapshot() -> None:
    # A reworded description, or any vendor change that moves no schema, must leave the
    # schema file unchanged.
    described = render.render_rest([_PATH])
    fields = tuple(dataclasses.replace(f, description="Reworded.") for f in _PATH.fields)
    reworded = dataclasses.replace(_PATH, fields=fields)
    assert render.render_rest([reworded]) == described
    assert "The price." not in described
    assert set(json.loads(described)) == {"generated_by", "sources"}


def test_render_of_no_paths_is_an_empty_source_list() -> None:
    assert json.loads(render.render_rest([]))["sources"] == []


def test_the_files_are_the_spec_package_data() -> None:
    assert render.SPEC_DIRECTORY.name == "spec"
    assert render.REST_FILE == render.SPEC_DIRECTORY / "rest.json"
    assert render.DESCRIPTIONS_FILE == render.SPEC_DIRECTORY / "descriptions.json"


# The descriptions file.

_URL = "https://massive.com/docs/rest/things.md"
_OTHER = "https://massive.com/docs/rest/crypto/things.md"


def test_render_descriptions_writes_the_documented_format() -> None:
    channel = Source(
        key="stocks/T",
        transport=Transport.WS,
        fields=(Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE, description="P."),),
    )
    text = render.render_descriptions(
        {Transport.REST: [_PATH], Transport.WS: [channel]},
        fetched=dt.date(2026, 10, 27),
        urls={Transport.REST: {_PATH.key: (_URL, _OTHER)}},
    )
    assert json.loads(text) == {
        "generated_by": "python -m deephaven_massive._massive.refresh",
        "fetched": "2026-10-27",
        # A source's URLs are sorted.
        "urls": {"REST": {_PATH.key: [_OTHER, _URL]}},
        # `t` has no description, so it has no entry.
        "descriptions": {
            "REST": {_PATH.key: {"price": "The price."}},
            "WS": {"stocks/T": {"p": "P."}},
        },
    }
    # One line per entry, so a reworded description is one changed line of the diff.
    assert '    "price": "The price."' in text.splitlines()
    assert f'    "{_URL}"' in text.splitlines()
    assert text.endswith("}\n")
    assert not text.endswith("\n\n")


def test_a_source_without_descriptions_has_no_entry_but_keeps_its_url() -> None:
    bare = RestPath(
        key="/v1/bare",
        fields=(Field(wire_key="x", column="X", wire_type=WireType.LONG),),
    )
    document = json.loads(
        render.render_descriptions(
            {Transport.REST: [bare]},
            fetched=dt.date(2026, 1, 2),
            urls={Transport.REST: {"/v1/bare": [_URL]}},
        )
    )
    assert document["descriptions"] == {"REST": {}}
    assert document["urls"] == {"REST": {"/v1/bare": [_URL]}}


def test_render_descriptions_keeps_text_that_is_not_ascii() -> None:
    accented = RestPath(
        key="/v1/x",
        fields=(Field(wire_key="x", column="X", wire_type=WireType.LONG, description="Café ≥ 1"),),
    )
    text = render.render_descriptions(
        {Transport.REST: [accented]}, fetched=dt.date(2026, 1, 2), urls={}
    )
    assert '"x": "Café ≥ 1"' in text
