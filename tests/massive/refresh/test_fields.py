"""Tests for the field rules every generator shares.

`test_rest_sources.py` shows each rule through the REST generator (names, representation, units,
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
from deephaven_massive._massive.spec.types import Field, WireType, WireUnit

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
    description_fixes={},
    result_keys={},
    param_units={},
    exclusions=(),
    kinds={},
    sample_errors=(),
    projections={},
    spellings={},
    shared_descriptions={},
    transport_only={},
    venues={},
    undeclared_fields={},
    optional_fields={},
    ignored_keys=(),
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


def _spelled(**long_names: str) -> SourceFields:
    """Return rules whose `SPELLINGS` give each dotted key (`__` for `.`) its long name."""
    spellings = {
        ("us_x/trades_v1", key.replace("__", ".")): overrides.Spelling(
            long_name=name, source="/v1/flat", reason="test"
        )
        for key, name in long_names.items()
    }
    return _rules(dataclasses.replace(_EMPTY, spellings=spellings))


def test_a_spelling_drops_a_wrapper_object_s_name_and_renames_a_field() -> None:
    rules = _spelled(details="", last_quote__bid="bid_price")
    number = {"type": "number", "description": "The strike price."}
    strike = rules.field(RawField("details.strike_price", number, required=True))
    bid = rules.field(RawField("last_quote.bid", number, required=True))
    assert strike is not None
    assert bid is not None
    assert (strike.column, bid.column) == ("StrikePrice", "LastQuoteBidPrice")
    assert rules.problems == []
    use = rules.use()
    assert use.concepts == {
        "StrikePrice": "strike_price",
        "LastQuoteBidPrice": "last_quote.bid_price",
    }
    assert use.long_names == {"details.strike_price": "strike_price", "last_quote.bid": "bid_price"}
    assert use.descriptions == {
        "details.strike_price": "The strike price.",
        "last_quote.bid": "The strike price.",
    }


def test_a_spelling_that_drops_a_field_s_own_name_keeps_its_key_and_is_reported() -> None:
    rules = _spelled(details__ticker="")
    item = rules.field(RawField("details.ticker", {"type": "string"}, required=True))
    assert item is not None
    assert item.column == "DetailsTicker"
    assert [(p.gate, p.key) for p in rules.problems] == [("field-spelling", "details.ticker")]


def test_a_spelling_that_coins_a_library_column_is_reported() -> None:
    rules = _spelled(last_updated="timestamp")
    instant = {"type": "string", "format": "date-time"}
    item = rules.field(RawField("last_updated", instant, required=True))
    assert item is not None
    assert item.column == "Timestamp"
    assert [(p.gate, p.key) for p in rules.problems] == [("field-spelling", "last_updated")]


def test_a_field_records_its_corrected_own_description_without_notes() -> None:
    fix = overrides.DescriptionFix(vendor="Per share.", text="Per coin.", reason="test")
    rules = _rules(dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "q.p"): fix}))
    raw = RawField(
        "q.p",
        {"type": "number", "description": "Per share."},
        required=True,
        notes=(("q", "A quote."),),
    )
    item = rules.field(raw)
    assert item is not None
    assert item.description == "Per coin. From the `q` object: A quote."
    assert rules.use().descriptions == {"q.p": "Per coin."}


def _fixed(vendor: str) -> HandInput:
    fix = overrides.DescriptionFix(vendor=vendor, text="The price of one coin.", reason="test")
    return dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "price"): fix})


def test_a_description_fix_replaces_the_vendor_text_it_quotes_as_plain_text() -> None:
    # The quote is the plain text, the link already rewritten, with any line breaks.
    rules = _rules(_fixed("Per whole\n  share. See Price (https://massive.com/docs/price)."))
    described = {"type": "number", "description": "Per whole share. See [Price](/docs/price)."}
    item = rules.field(RawField("price", described, required=True))
    assert item is not None
    assert item.description == "The price of one coin."
    assert rules.problems == []


def test_a_description_fix_is_stale_once_the_vendor_rewrites_the_text() -> None:
    rules = _rules(_fixed("Per whole share."))
    described = {"type": "number", "description": "The price of the trade."}
    item = rules.field(RawField("price", described, required=True))
    assert item is not None
    assert item.description == "The price of the trade."
    assert [(p.gate, p.source, p.key, p.description) for p in rules.problems] == [
        ("stale-entry", "us_x/trades_v1", "price", "The price of the trade.")
    ]


def test_a_description_fix_names_the_field_by_its_corrected_key() -> None:
    hand = dataclasses.replace(
        _fixed("Per whole share."),
        key_fixes={("us_x/trades_v1", "px"): overrides.KeyFix(wire_key="price", reason="test")},
    )
    rules = _rules(hand)
    described = {"type": "number", "description": "Per whole share."}
    item = rules.field(RawField("px", described, required=True))
    assert item is not None
    assert (item.wire_key, item.description) == ("price", "The price of one coin.")
    assert rules.use().described_keys == {"price"}


def test_a_description_fix_under_the_declared_key_of_a_key_fix_is_never_looked_up() -> None:
    fix = overrides.DescriptionFix(vendor="Per whole share.", text="Per coin.", reason="test")
    hand = dataclasses.replace(
        _EMPTY,
        key_fixes={("us_x/trades_v1", "px"): overrides.KeyFix(wire_key="price", reason="test")},
        description_fixes={("us_x/trades_v1", "px"): fix},
    )
    rules = _rules(hand)
    described = {"type": "number", "description": "Per whole share."}
    item = rules.field(RawField("px", described, required=True))
    assert item is not None
    assert item.description == "Per whole share."
    use = rules.use()
    assert "px" in use.wire_keys
    assert use.described_keys == frozenset()


def test_a_stale_description_fix_by_asset_class_gives_no_class_texts() -> None:
    fix = overrides.DescriptionFix(
        vendor="Per whole share.", asset_classes={"options": "Per share."}, reason="test"
    )
    hand = dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "price"): fix})
    rules = _rules(hand)
    described = {"type": "number", "description": "Reworded."}
    item = rules.field(RawField("price", described, required=True))
    assert item is not None
    assert (item.description, item.class_descriptions) == ("Reworded.", ())
    assert [(p.gate, p.key) for p in rules.problems] == [("stale-entry", "price")]


def test_a_description_fix_by_asset_class_keeps_the_vendor_text_for_the_rest() -> None:
    fix = overrides.DescriptionFix(
        vendor="Per whole share.",
        asset_classes={"options": "Per share of the underlying.", "crypto": "Per coin."},
        reason="test",
    )
    hand = dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "price"): fix})
    described = {"type": "number", "description": "Per whole share."}
    item = _rules(hand).field(RawField("price", described, required=True))
    assert item is not None
    assert item.description == "Per whole share."
    assert item.class_descriptions == (
        ("crypto", "Per coin."),
        ("options", "Per share of the underlying."),
    )
    assert item.description_for("options") == "Per share of the underlying."
    assert item.description_for("stocks") == "Per whole share."
    assert item.description_for(None) == "Per whole share."


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
    assert item.wire_type is WireType.LONG_ARRAY
    texts: JsonObject = {"type": "array", "items": {"type": "string"}}
    names = rules.field(openapi_field(spec, "names", texts, required=False))
    assert names is not None
    assert names.wire_type is WireType.STRING_ARRAY
    # An array whose items have no type of their own is kept as JSON text.
    bare: JsonObject = {"type": "array", "items": {}}
    untyped = rules.field(openapi_field(spec, "any", bare, required=False))
    assert untyped is not None
    assert untyped.wire_type is WireType.JSON_TEXT
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


# The notes of enclosing objects.

_SESSION_NOTE = "Prices are omitted until the [regular open](/docs/hours)."
_SESSION_TEXT = "Prices are omitted until the regular open (https://massive.com/docs/hours)."


def test_a_field_carries_the_notes_of_its_enclosing_objects_outermost_first() -> None:
    raw = RawField(
        "session.bars.close",
        {"type": "number", "description": "The close price."},
        required=False,
        notes=(("session", _SESSION_NOTE), ("session.bars", "One bar per\n  minute.")),
    )
    item = _rules().field(raw)
    assert item is not None
    assert item.description == (
        f"The close price. From the `session` object: {_SESSION_TEXT} "
        "From the `session.bars` object: One bar per minute."
    )


@pytest.mark.parametrize(
    ("own", "expected"),
    [
        ("The close price", "The close price. From the `day` object: The daily bar."),
        ("Is it open?", "Is it open? From the `day` object: The daily bar."),
        ("", "From the `day` object: The daily bar."),
    ],
)
def test_a_note_follows_a_whole_sentence_of_the_fields_own_text(own: str, expected: str) -> None:
    raw = RawField(
        "day.close",
        {"type": "number", "description": own},
        required=False,
        notes=(("day", "The daily bar."),),
    )
    item = _rules().field(raw)
    assert item is not None
    assert item.description == expected


def test_a_field_without_notes_keeps_its_own_text_unchanged() -> None:
    raw = RawField("close", {"type": "number", "description": "Close"}, required=True)
    item = _rules().field(raw)
    assert item is not None
    assert item.description == "Close"


def test_a_description_fix_replaces_only_the_fields_own_text_and_keeps_the_notes() -> None:
    # The fix quotes the field's own text, never the notes, and every text it gives,
    # per asset class included, gets the notes.
    fix = overrides.DescriptionFix(
        vendor="Per whole share.", asset_classes={"options": "Per share."}, reason="test"
    )
    hand = dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "last.price"): fix})
    raw = RawField(
        "last.price",
        {"type": "number", "description": "Per whole share."},
        required=False,
        notes=(("last", "Only on some plans."),),
    )
    rules = _rules(hand)
    item = rules.field(raw)
    assert item is not None
    assert rules.problems == []
    note = "From the `last` object: Only on some plans."
    assert item.description == f"Per whole share. {note}"
    assert item.class_descriptions == (("options", f"Per share. {note}"),)


def _note_fixed(fix: overrides.DescriptionFix) -> HandInput:
    return dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "last"): fix})


def _in_last(rules: SourceFields, name: str, own: str = "") -> Field:
    raw = RawField(
        f"last.{name}",
        {"type": "number", "description": own},
        required=False,
        notes=(("last", "The most recent quote."),),
    )
    item = rules.field(raw)
    assert item is not None
    return item


def test_a_description_fix_under_an_objects_key_corrects_its_note() -> None:
    fix = overrides.DescriptionFix(
        vendor="The most recent quote.", text="The most recent trade.", reason="test"
    )
    rules = _rules(_note_fixed(fix))
    item = _in_last(rules, "price", "The price.")
    assert item.description == "The price. From the `last` object: The most recent trade."
    assert rules.problems == []
    assert rules.use().described_keys == {"last"}


def test_a_stale_description_fix_on_an_object_is_reported_once_and_keeps_the_note() -> None:
    fix = overrides.DescriptionFix(vendor="Reworded.", text="The trade.", reason="test")
    rules = _rules(_note_fixed(fix))
    first = _in_last(rules, "price")
    _in_last(rules, "size")
    assert first.description == "From the `last` object: The most recent quote."
    assert [(p.gate, p.key) for p in rules.problems] == [("stale-entry", "last")]


def test_per_asset_class_texts_combine_a_fields_own_and_its_notes() -> None:
    # A class listed by the field's entry or by a note's entry gets its own text; each part
    # it does not list keeps the shared text.
    note_fix = overrides.DescriptionFix(
        vendor="The most recent quote.",
        asset_classes={"crypto": "The latest crypto trade."},
        reason="test",
    )
    field_fix = overrides.DescriptionFix(
        vendor="Per share.", asset_classes={"options": "Per contract."}, reason="test"
    )
    hand = dataclasses.replace(
        _EMPTY,
        description_fixes={
            ("us_x/trades_v1", "last"): note_fix,
            ("us_x/trades_v1", "last.price"): field_fix,
        },
    )
    item = _in_last(_rules(hand), "price", "Per share.")
    note = "From the `last` object:"
    assert item.description == f"Per share. {note} The most recent quote."
    assert item.class_descriptions == (
        ("crypto", f"Per share. {note} The latest crypto trade."),
        ("options", f"Per contract. {note} The most recent quote."),
    )


def test_every_part_closes_its_last_sentence_before_the_next() -> None:
    raw = RawField(
        "event.change.ticker",
        {"type": "string", "description": "The new ticker"},
        required=False,
        notes=(("event", "Details about an event"), ("event.change", "A ticker change")),
    )
    item = _rules().field(raw)
    assert item is not None
    assert item.description == (
        "The new ticker. From the `event` object: Details about an event. "
        "From the `event.change` object: A ticker change."
    )


@pytest.mark.parametrize(
    ("fixed_key", "expected"),
    [
        # Moved out of `details` to the top of the row: no object encloses it any more.
        ("ticker", "The ticker."),
        # Moved within `details`: `details` still encloses it, `details.codes` does not.
        ("details.ticker", "The ticker. From the `details` object: The contract details."),
        # A key that merely starts with an object's name is not inside it.
        ("details_ticker", "The ticker."),
    ],
)
def test_a_key_fix_keeps_only_the_notes_of_the_objects_that_still_enclose_the_field(
    fixed_key: str, expected: str
) -> None:
    fix = overrides.KeyFix(wire_key=fixed_key, reason="test")
    hand = dataclasses.replace(_EMPTY, key_fixes={("us_x/trades_v1", "details.codes.t"): fix})
    raw = RawField(
        "details.codes.t",
        {"type": "string", "description": "The ticker."},
        required=False,
        notes=(("details", "The contract details."), ("details.codes", "The codes.")),
    )
    item = _rules(hand).field(raw)
    assert item is not None
    assert (item.wire_key, item.description) == (fixed_key, expected)


def test_an_undescribed_objects_note_given_for_some_classes_adds_nothing_to_the_rest() -> None:
    fix = overrides.DescriptionFix(
        vendor="", asset_classes={"options": "The contract's last minute."}, reason="test"
    )
    hand = dataclasses.replace(_EMPTY, description_fixes={("us_x/trades_v1", "last"): fix})
    raw = RawField(
        "last.price",
        {"type": "number", "description": "The price"},
        required=False,
        notes=(("last", ""),),
    )
    item = _rules(hand).field(raw)
    assert item is not None
    assert item.description == "The price"
    assert item.class_descriptions == (
        ("options", "The price. From the `last` object: The contract's last minute."),
    )
