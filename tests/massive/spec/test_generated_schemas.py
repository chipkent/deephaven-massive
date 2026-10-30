"""Tests for the strict parsing of the generated schema files and of `descriptions.json`."""

from __future__ import annotations

import datetime as dt
import json
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.spec import generated_schemas
from deephaven_massive._massive.spec.types import (
    Conversion,
    Membership,
    Param,
    ParamKind,
    RestPath,
    Source,
    Transport,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def _file(*entries: Mapping[str, object]) -> str:
    return json.dumps({"sources": list(entries)})


_FIELD = {
    "wire_key": "x",
    "column": "X",
    "type": "string",
    "unit": None,
    "required": False,
    "conversion": "text-or-integer-id",
}


def test_parse_reads_memberships_and_other_transports() -> None:
    entry = {
        "key": "stocks/T",
        "fields": [_FIELD],
        "memberships": [
            {"kind": "trades", "group": "stocks", "position": 2, "projection": None},
            {"kind": "values", "group": "indices", "position": 0, "projection": {"x": "Value"}},
        ],
    }
    (value,) = generated_schemas.parse(Transport.WS, _file(entry)).values()
    assert type(value) is Source
    assert value.transport is Transport.WS
    assert value.fields[0].conversion is Conversion.TEXT_OR_INTEGER_ID
    assert value.memberships == (
        Membership(kind="trades", group="stocks", position=2),
        Membership(kind="values", group="indices", position=0, projection={"x": "Value"}),
    )


def test_parse_reads_parameter_values() -> None:
    entry = {
        "key": "/v1/x",
        "result_key": "results",
        "fields": [],
        "memberships": [],
        "params": [
            {
                "name": "order",
                "kind": "enum",
                "in_path": False,
                "values": ["asc"],
                "unit": None,
                "default": None,
            },
            {
                "name": "window",
                "kind": "integer",
                "in_path": False,
                "values": [],
                "unit": None,
                "default": 50,
            },
        ],
    }
    (value,) = generated_schemas.parse(Transport.REST, _file(entry)).values()
    assert isinstance(value, RestPath)
    assert value.params == (
        Param(name="order", kind=ParamKind.ENUM, values=("asc",)),
        Param(name="window", kind=ParamKind.INTEGER, default=50),
    )


def test_parse_rejects_two_sources_with_one_key() -> None:
    entry = {"key": "stocks/T", "fields": [], "memberships": []}
    with pytest.raises(ValueError, match="share the key 'stocks/T'"):
        generated_schemas.parse(Transport.WS, _file(entry, entry))


def test_parse_rejects_a_list_that_is_not_of_objects() -> None:
    with pytest.raises(ValueError, match="expected an array of objects"):
        generated_schemas.parse(
            Transport.WS, _file({"key": "stocks/T", "fields": ["x"], "memberships": []})
        )


def test_parse_rejects_a_position_that_is_not_an_integer() -> None:
    membership = {"kind": "k", "group": "g", "position": True, "projection": None}
    entry = {"key": "stocks/T", "fields": [], "memberships": [membership]}
    with pytest.raises(TypeError, match="position True is not an integer"):
        generated_schemas.parse(Transport.WS, _file(entry))


def test_parse_rejects_values_that_break_the_invariants() -> None:
    field = {**_FIELD, "type": "epoch", "conversion": None}  # an epoch without a unit
    with pytest.raises(ValueError, match="an epoch needs a unit"):
        generated_schemas.parse(
            Transport.WS, _file({"key": "s", "fields": [field], "memberships": []})
        )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"column": None}, "'column' is None, not a string"),
        ({"required": 1}, "'required' is 1, not a boolean"),
        ({"unit": 5}, "'unit' is 5, not a string"),
    ],
)
def test_parse_coerces_nothing_in_a_field(change: dict[str, object], message: str) -> None:
    entry = {"key": "s", "fields": [{**_FIELD, **change}], "memberships": []}
    with pytest.raises(TypeError, match=message):
        generated_schemas.parse(Transport.WS, _file(entry))


def test_parse_rejects_parameter_values_that_are_not_strings() -> None:
    param = {
        "name": "precision",
        "kind": "enum",
        "in_path": False,
        "values": [0],
        "unit": None,
        "default": None,
    }
    entry = {"key": "/v1/x", "result_key": None, "fields": [], "memberships": [], "params": [param]}
    with pytest.raises(TypeError, match="not an array of strings"):
        generated_schemas.parse(Transport.REST, _file(entry))


def test_parse_rejects_a_parameter_default_that_is_not_a_scalar() -> None:
    param = {
        "name": "window",
        "kind": "integer",
        "in_path": False,
        "values": [],
        "unit": None,
        "default": [50],
    }
    entry = {"key": "/v1/x", "result_key": None, "fields": [], "memberships": [], "params": [param]}
    with pytest.raises(TypeError, match=r"'default' is \[50\], not a JSON scalar"):
        generated_schemas.parse(Transport.REST, _file(entry))


def test_parse_rejects_a_projection_that_is_not_of_strings() -> None:
    membership = {"kind": "k", "group": "g", "position": 0, "projection": {"x": 1}}
    entry = {"key": "stocks/T", "fields": [], "memberships": [membership]}
    with pytest.raises(TypeError, match="not an object of strings"):
        generated_schemas.parse(Transport.WS, _file(entry))


@pytest.mark.parametrize(
    ("conversion", "message"),
    [
        ("bool-as-text", "'bool-as-text' is not a valid Conversion"),
        ("quoted-integer", "the conversion quoted-integer gives long, not string"),
    ],
)
def test_parse_rejects_a_conversion_the_decoder_lacks_or_that_does_not_fit(
    conversion: str, message: str
) -> None:
    field = {**_FIELD, "type": "string", "conversion": conversion}
    with pytest.raises(ValueError, match=message):
        generated_schemas.parse(
            Transport.WS, _file({"key": "s", "fields": [field], "memberships": []})
        )


_PRICE = {**_FIELD, "wire_key": "p", "column": "P", "conversion": None}
_DESCRIBED_ENTRY = {"key": "stocks/T", "fields": [_PRICE], "memberships": []}


def test_parse_joins_descriptions_and_leaves_the_rest_empty() -> None:
    entry = {**_DESCRIBED_ENTRY, "fields": [_PRICE, _FIELD]}
    (value,) = generated_schemas.parse(
        Transport.WS, _file(entry), {"stocks/T": {"p": "The price."}}
    ).values()
    assert [(f.wire_key, f.description) for f in value.fields] == [("p", "The price."), ("x", "")]
    (bare,) = generated_schemas.parse(Transport.WS, _file(entry)).values()
    assert [f.description for f in bare.fields] == ["", ""]


def test_parse_joins_class_descriptions_sorted_by_asset_class() -> None:
    by_class = {"stocks/T": {"p": {"options": "Per share.", "crypto": "Per coin."}}}
    (value,) = generated_schemas.parse(
        Transport.WS, _file(_DESCRIBED_ENTRY), {}, class_descriptions=by_class
    ).values()
    (price,) = value.fields
    assert price.class_descriptions == (("crypto", "Per coin."), ("options", "Per share."))


@pytest.mark.parametrize(
    ("descriptions", "message"),
    [
        (
            {"stocks/Q": {"p": "The price."}},
            "descriptions name the WS source 'stocks/Q', which is absent",
        ),
        (
            {"stocks/T": {"p": "The price.", "z": "Gone."}},
            r"descriptions of WS 'stocks/T' name no field: \['z'\]",
        ),
    ],
    ids=["unknown source", "unknown wire key"],
)
def test_parse_rejects_a_description_of_something_the_schema_lacks(
    descriptions: Mapping[str, Mapping[str, str]], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        generated_schemas.parse(Transport.WS, _file(_DESCRIBED_ENTRY), descriptions)
    by_class = {
        key: {wire_key: {"options": text}}
        for key, texts in descriptions.items()
        for wire_key, text in texts.items()
    }
    with pytest.raises(ValueError, match=f"class {message}"):
        generated_schemas.parse(Transport.WS, _file(_DESCRIBED_ENTRY), class_descriptions=by_class)


_URL = "https://massive.com/docs/rest/x.md"


def _descriptions(**change: object) -> str:
    document = {
        "generated_by": "python -m deephaven_massive._massive.refresh",
        "fetched": "2026-10-27",
        "urls": {"REST": {"/v1/x": [_URL]}},
        "descriptions": {"REST": {"/v1/x": {"price": "The price."}}},
        "class_descriptions": {"REST": {"/v1/x": {"price": {"options": "Per share."}}}},
    }
    return json.dumps({**document, **change})


def test_parse_descriptions_reads_the_generated_format() -> None:
    found = generated_schemas.parse_descriptions(_descriptions())
    assert found.fetched == dt.date(2026, 10, 27)
    assert found.urls == {Transport.REST: {"/v1/x": (_URL,)}}
    assert found.texts == {Transport.REST: {"/v1/x": {"price": "The price."}}}
    assert found.class_texts == {Transport.REST: {"/v1/x": {"price": {"options": "Per share."}}}}
    assert isinstance(found.class_texts[Transport.REST]["/v1/x"]["price"], MappingProxyType)
    assert isinstance(found.urls, MappingProxyType)
    assert isinstance(found.urls[Transport.REST], MappingProxyType)
    assert isinstance(found.texts[Transport.REST], MappingProxyType)
    assert isinstance(found.texts[Transport.REST]["/v1/x"], MappingProxyType)


@pytest.mark.parametrize(
    ("text", "error", "message"),
    [
        ("[]", TypeError, "the descriptions file is \\[\\], not an object"),
        (_descriptions(fetched="27 Oct 2026"), ValueError, "Invalid isoformat"),
        (_descriptions(fetched=None), TypeError, "'fetched' is None, not a string"),
        (_descriptions(urls={"SMS": {}}), ValueError, "'SMS' is not a valid Transport"),
        (_descriptions(urls=[]), TypeError, "urls is \\[\\], not an object"),
        (_descriptions(urls={"REST": []}), TypeError, "URLs of REST is \\[\\], not an object"),
        (
            _descriptions(urls={"REST": {"/v1/x": _URL}}),
            TypeError,
            "URLs of '/v1/x' is .* not an array of strings",
        ),
        (
            _descriptions(urls={"REST": {"/v1/x": [1]}}),
            TypeError,
            "not an array of strings",
        ),
        (
            _descriptions(descriptions={"REST": {"/v1/x": {"price": None}}}),
            TypeError,
            "descriptions of '/v1/x' is .* not an object of strings",
        ),
        (
            _descriptions(descriptions={"REST": ["/v1/x"]}),
            TypeError,
            "descriptions of REST is .* not an object",
        ),
        (json.dumps({"fetched": "2026-10-27"}), ValueError, "missing member 'urls'"),
        (
            json.dumps({"fetched": "2026-10-27", "urls": {}}),
            ValueError,
            "missing member 'descriptions'",
        ),
        (
            _descriptions(class_descriptions={"REST": {"/v1/x": {"price": {"options": 1}}}}),
            TypeError,
            "class descriptions of '/v1/x' is .* not an object of strings",
        ),
        (
            _descriptions(class_descriptions={"REST": {"/v1/x": ["price"]}}),
            TypeError,
            "class descriptions of '/v1/x' is .* not an object",
        ),
        (
            json.dumps({"fetched": "2026-10-27", "urls": {}, "descriptions": {}}),
            ValueError,
            "missing member 'class_descriptions'",
        ),
    ],
    ids=[
        "not an object",
        "a date that is not ISO",
        "no date",
        "unknown transport",
        "urls not an object",
        "a transport's urls not an object",
        "a source's urls not a list",
        "a url not text",
        "a description not text",
        "a transport's descriptions not an object",
        "no urls",
        "no descriptions",
        "a class description not text",
        "a field's class descriptions not an object",
        "no class descriptions",
    ],
)
def test_parse_descriptions_coerces_nothing(
    text: str, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        generated_schemas.parse_descriptions(text)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[]", "the schema file is \\[\\], not an object"),
        ("{}", "missing member 'sources'"),
        (_file({"key": "s", "memberships": []}), "missing member 'fields'"),
        (_file({"fields": [], "memberships": []}), "missing member 'key'"),
    ],
    ids=["not an object", "no sources", "a source without fields", "a source without a key"],
)
def test_parse_names_a_missing_member(text: str, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        generated_schemas.parse(Transport.WS, text)
