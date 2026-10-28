"""Tests for the field rules every generator shares.

`test_rest.py` shows each rule through the REST generator (names, representation, units,
conversions, examples, and their gates); the tests here show the rules directly, as the
WebSocket and flat-file generators use them, on fields built by hand.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.refresh.fields import (
    RawField,
    SourceFields,
    openapi_field,
    representation,
)
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi
from deephaven_massive._massive.spec.types import WireType, WireUnit

if TYPE_CHECKING:
    from deephaven_massive._massive.refresh.openapi import Json, JsonObject

# Hand input with every table empty; each test replaces the tables it needs.
_EMPTY = HandInput(
    lexicons={},
    shapes={},
    identity={},
    names={},
    units={},
    tag_conflicts={},
    type_fixes={},
    key_fixes={},
    result_keys={},
    param_units={},
    exclusions=(),
    kinds={},
    sample_errors=(),
    projections={},
    transport_only={},
    venues={},
)


def _rules(hand: HandInput = _EMPTY) -> SourceFields:
    return SourceFields(hand, "us_x/trades_v1", origin="https://massive.com")


@pytest.mark.parametrize(
    ("schema", "last", "expected"),
    [
        ({"type": "integer"}, "size", WireType.LONG),
        ({"type": "number", "format": "integer"}, "volume", WireType.LONG),
        ({"type": "number"}, "price", WireType.DOUBLE),
        ({"type": "boolean"}, "otc", WireType.BOOL),
        ({"type": "string"}, "ticker", WireType.STRING),
        ({"type": "string"}, "session_end_date", WireType.DATE),
        ({"type": "string", "format": "date"}, "day", WireType.DATE),
        ({"type": "string"}, "published_utc", WireType.ISO_INSTANT),
        ({"type": "string", "format": "date-time"}, "at", WireType.ISO_INSTANT),
        ({"type": "object"}, "extra", WireType.JSON_TEXT),
        ({"oneOf": [{}, {}]}, "either", WireType.JSON_TEXT),
        ({"type": "null"}, "nothing", None),
        ({}, "untyped", None),
    ],
)
def test_representation(schema: JsonObject, last: str, expected: WireType | None) -> None:
    assert representation(schema, last) is expected


def test_a_field_takes_its_name_type_and_plain_description() -> None:
    rules = _rules()
    described = {"type": "number", "description": "See [Price](/docs/price)."}
    item = rules.field(RawField("ask_price", described, required=True))
    assert item is not None
    assert (item.column, item.wire_type, item.required) == ("AskPrice", WireType.DOUBLE, True)
    assert item.description == "See Price (https://massive.com/docs/price)."
    use = rules.use()
    assert use.wire_keys == {"ask_price"}
    assert use.columns == {"ask_price": "AskPrice"}
    assert use.concepts == {"AskPrice": "ask_price"}


def test_a_field_documented_as_a_timestamp_needs_its_unit_whatever_its_name() -> None:
    raw = RawField("start", {"type": "integer"}, required=False, timestamp=True)
    rules = _rules()
    assert rules.field(raw) is not None
    assert [(p.gate, p.source, p.key) for p in rules.problems] == [
        ("timestamp-unit", "us_x/trades_v1", "start")
    ]
    ns = overrides.Unit(unit=WireUnit.NS, reason="test")
    rules = _rules(dataclasses.replace(_EMPTY, units={("us_x/trades_v1", "start"): ns}))
    item = rules.field(raw)
    assert item is not None
    assert (item.wire_type, item.unit) == (WireType.EPOCH, WireUnit.NS)
    assert rules.problems == []
    # Without the documented type, the same name is an ordinary integer.
    plain = _rules()
    assert plain.field(RawField("start", {"type": "integer"}, required=False)) is not None
    assert plain.problems == []


def test_a_field_of_no_usable_type_is_reported() -> None:
    rules = _rules()
    assert rules.field(RawField("x_flag", {"type": "null"}, required=False)) is None
    assert [(p.gate, p.key) for p in rules.problems] == [("shape", "x_flag")]


def test_an_array_s_items_decide_its_type() -> None:
    # An OpenAPI field's items are resolved where it is read; the rules only look at them.
    spec = OpenApi({"components": {"schemas": {"Code": {"type": "integer"}}}})
    rules = SourceFields(_EMPTY, "stocks/T", origin="https://massive.com")
    codes: JsonObject = {"type": "array", "items": {"$ref": "#/components/schemas/Code"}}
    raw = openapi_field(spec, "conditions", codes, required=False)
    assert raw.items == {"type": "integer"}
    item = rules.field(raw)
    assert item is not None
    assert item.wire_type is WireType.INT_ARRAY
    texts: JsonObject = {"type": "array", "items": {"type": "string"}}
    names = rules.field(openapi_field(spec, "names", texts, required=False))
    assert names is not None
    assert names.wire_type is WireType.JSON_TEXT
    assert openapi_field(spec, "price", {"type": "number"}, required=True).items is None


def test_examples_are_checked_by_the_generated_field() -> None:
    rules = _rules()
    price = rules.field(RawField("price", {"type": "number"}, required=False))
    assert price is not None
    rules.check_examples([price], [{"price": 1.5}], documented=True, where="in the message")
    assert rules.problems == []
    rules.check_examples([price], [{"price": "1.5"}], documented=True, where="in the message")
    assert [(p.gate, p.key) for p in rules.problems] == [("example-type", "price")]


def test_a_key_fix_reads_a_field_under_the_key_the_samples_send() -> None:
    fix = overrides.KeyFix(wire_key="type", reason="test")
    rules = _rules(dataclasses.replace(_EMPTY, key_fixes={("us_x/trades_v1", "event_type"): fix}))
    described = {"type": "string", "description": "The event's type."}
    item = rules.field(RawField("event_type", described, required=True))
    assert item is not None
    assert (item.wire_key, item.column, item.description) == ("type", "Type", "The event's type.")
    # The declared key keeps the entry from going stale; the wire key serves the other entries.
    assert rules.use().wire_keys == {"event_type", "type"}
    rules.check_examples([item], [{"type": "ticker_change"}], documented=True, where="here")
    assert rules.problems == []


@pytest.mark.parametrize(
    "rows",
    [
        [{"event_type": "ticker_change"}],
        [{"type": "ticker_change"}, {"event_type": "ticker_change"}],
        [None],
    ],
)
def test_a_key_fix_is_stale_once_the_example_stops_sending_its_key(rows: list[Json]) -> None:
    fix = overrides.KeyFix(wire_key="type", reason="test")
    rules = _rules(dataclasses.replace(_EMPTY, key_fixes={("us_x/trades_v1", "event_type"): fix}))
    item = rules.field(RawField("event_type", {"type": "string"}, required=True))
    assert item is not None
    rules.check_examples([item], rows, documented=True, where="here")
    assert ("stale-entry", "event_type") in [(p.gate, p.key) for p in rules.problems]
