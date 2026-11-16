"""Tests for the WebSocket generator: channels, keys, fields, documentation, and its gates.

The first group runs against the vendored snapshot, and skips where there is none (an sdist
leaves it out). The rest build small synthetic specifications and pages, so each rule and
gate is shown in isolation; every gate has a test that fails when the gate is removed. The
field rules every transport shares are tested with `fields` and `rest_sources`.
"""

from __future__ import annotations

import dataclasses
import json
import os
from datetime import date
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import exclusions, lexicon, overrides
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.snapshot import DIRECTORY, Snapshot
from deephaven_massive._massive.refresh.ws_sources import WsCoverage, channel_key, generate_ws
from deephaven_massive._massive.spec.convert import convert_field
from deephaven_massive._massive.spec.types import (
    Conversion,
    Feed,
    FeedHost,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# --- The vendored snapshot -------------------------------------------------------------------

_requires_snapshot = pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)


@pytest.fixture(scope="module")
def real() -> tuple[tuple[Source, ...], WsCoverage]:
    """Generate the WebSocket sources of the vendored snapshot once for the module.

    Returns:
        The generated channels and the coverage.
    """
    return generate_ws(Snapshot.load())


@_requires_snapshot
def test_the_channels_are_keyed_by_cluster_and_channel(
    real: tuple[tuple[Source, ...], WsCoverage],
) -> None:
    channels, coverage = real
    assert [c.key for c in channels][:7] == [
        "stocks/T",
        "stocks/Q",
        "stocks/A",
        "stocks/AM",
        "stocks/LULD",
        "stocks/NOI",
        "stocks/FMV",
    ]
    assert all(c.transport is Transport.WS for c in channels)
    # Every generated channel cites the page of its feed.
    assert coverage.pages["stocks/FMV"] == ("stocks/fair-market-value.md",)
    assert set(coverage.pages) == coverage.generated


@_requires_snapshot
def test_the_messages_take_the_lexicons_and_names(
    real: tuple[tuple[Source, ...], WsCoverage],
) -> None:
    channels = {c.key: c for c in real[0]}
    trade = {f.wire_key: f for f in channels["stocks/T"].fields}
    assert "ev" not in trade  # the event type names the channel, not a field
    assert (trade["sym"].column, trade["sym"].required) == ("Ticker", True)
    assert (trade["t"].column, trade["t"].unit) == ("SipTimestamp", WireUnit.MS)
    assert (trade["pt"].column, trade["pt"].required) == ("ParticipantTimestamp", False)
    assert (trade["trfi"].column, trade["trft"].column) == ("TrfId", "TrfTimestamp")
    bars = {f.wire_key: f.column for f in channels["stocks/AM"].fields}
    assert {k: bars[k] for k in ("dv", "av", "dav", "op", "a", "z", "s", "e")} == {
        "dv": "DecimalVolume",
        "av": "AccumulatedVolume",
        "dav": "AccumulatedDecimalVolume",
        "op": "OfficialOpen",
        "a": "DayVwap",
        "z": "AverageTradeSize",
        "s": "WindowStart",
        "e": "EndTimestamp",
    }
    assert channels["futures/AM"].field("DollarVolume").wire_key == "dv"
    forex = {f.wire_key: f.column for f in channels["forex/C"].fields}
    assert (forex["a"], forex["b"]) == ("AskPrice", "BidPrice")
    assert channels["crypto/XT"].field("ParticipantTimestamp").wire_key == "t"
    assert channels["indices/V"].field("Timestamp").wire_key == "t"


@_requires_snapshot
def test_each_kind_lists_its_channels_in_the_specification_s_order() -> None:
    # `KINDS` positions follow the vendor's own order of the channels (`x-polygon-order`).
    websocket = json.loads((DIRECTORY / "websocket.json").read_text(encoding="utf-8"))
    vendor = [
        channel_key(path)
        for market in websocket["x-polygon-order"].values()
        for entry in market["market"]
        for path in entry["paths"]
    ]
    for groups in overrides.KINDS.values():
        for sources in groups.values():
            channels = [s for s in sources if s in vendor]
            assert channels == sorted(channels, key=vendor.index), channels


@_requires_snapshot
def test_the_websocket_type_fixes_follow_their_samples(
    real: tuple[tuple[Source, ...], WsCoverage],
) -> None:
    # Each WebSocket TYPE_FIXES entry cites the specification's example value, or the live run
    # whose values showed the form, and the example value decodes through the generated field.
    channels = {c.key: c for c in real[0]}
    websocket = json.loads((DIRECTORY / "websocket.json").read_text(encoding="utf-8"))
    examples = {
        channel_key(path): item["get"]["responses"]["200"]["content"]["application/json"]["example"]
        for path, item in websocket["paths"].items()
    }
    fixed = [(k, fix) for k, fix in overrides.TYPE_FIXES.items() if k[0] in channels]
    assert len(fixed) == 17
    for (key, wire_key), fix in fixed:
        typed = next(f for f in channels[key].fields if f.wire_key == wire_key)
        assert typed.conversion is fix.conversion
        sample = examples[key][wire_key]
        cited = "live run of 2026-11-02" if fix.observed_live else json.dumps(sample)
        assert cited in fix.reason, (key, wire_key)
        assert convert_field(typed, sample) is not None


# --- Synthetic specifications ---------------------------------------------------------------


def _channel(
    properties: Mapping[str, object],
    *,
    required: Sequence[str] = (),
    example: object = None,
    operation_extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    media: dict[str, object] = {
        "schema": {"type": "object", "properties": properties, "required": list(required)}
    }
    if example is not None:
        media["example"] = example
    return {
        "get": {
            "responses": {"200": {"content": {"application/json": media}}},
            **(operation_extra or {}),
        }
    }


def _feed(cluster: str, channel: str) -> str:
    return (
        "## Connection\n\n| Feed | URL |\n| --- | --- |\n"
        f"| Real-Time | `wss://socket.massive.com/{cluster}` |\n\n"
        f"The channel prefix for this feed is `{channel}`.\n"
    )


def _snapshot(
    paths: Mapping[str, object],
    *,
    pages: Mapping[str, str] | None = None,
    schemas: Mapping[str, object] | None = None,
) -> Snapshot:
    """Return a snapshot of a WebSocket specification, each channel documented by a page.

    Pages given in `pages` come first; every channel of a well-formed path gets a page.
    """
    document = {"openapi": "3.0.3", "paths": paths, "components": {"schemas": schemas or {}}}
    named = dict(pages or {})
    for path in paths:
        key = channel_key(path)
        if key is not None and pages is None:
            named[f"{key.lower()}.md"] = _feed(*key.split("/"))
    return Snapshot(
        openapi={"paths": {}},
        llms="",
        pages={},
        fetched=date(2026, 1, 2),
        page_urls={},
        index_url="https://massive.com/docs/llms.txt",
        websocket=json.loads(json.dumps(document)),
        websocket_pages=named,
        websocket_page_urls={n: f"https://massive.com/docs/websocket/{n}" for n in named},
    )


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


def _problems(snapshot: Snapshot, hand: HandInput | None = None) -> list[tuple[str, str, str]]:
    with pytest.raises(RefreshError) as caught:
        generate_ws(snapshot, hand or _EMPTY)
    return sorted((p.gate, p.source, p.key) for p in caught.value.problems)


_PRICE = {"type": "number", "description": "The price."}
_TICKER = {"type": "string", "description": "The ticker."}
_EVENT = {"enum": ["T"], "description": "The event type."}


@pytest.mark.parametrize(
    ("path", "key"),
    [
        ("/stocks/T", "stocks/T"),
        ("/business/stocks/FMV", "stocks/FMV"),
        ("/launchpad/stocks/AM", None),
        ("/stocks", None),
        ("/stocks/T/x", None),
        ("/stocks/t", None),
        ("stocks/T", None),
    ],
)
def test_channel_key_is_the_cluster_and_the_channel(path: str, key: str | None) -> None:
    assert channel_key(path) == key


def test_a_message_s_properties_in_order_without_the_event_type() -> None:
    message = {"ev": _EVENT, "ticker": _TICKER, "price": _PRICE}
    (channel,), coverage = generate_ws(
        _snapshot({"/stocks/T": _channel(message, required=["ev", "ticker"])}), _EMPTY
    )
    assert channel.key == "stocks/T"
    assert [(f.wire_key, f.column, f.required) for f in channel.fields] == [
        ("ticker", "Ticker", True),
        ("price", "Price", False),
    ]
    assert channel.field("Price").description == "The price."
    assert coverage.generated == {"stocks/T"}
    assert coverage.pages == {"stocks/T": ("stocks/t.md",)}


def test_a_message_built_from_all_of_parts_is_merged() -> None:
    schemas = {"Base": {"type": "object", "properties": {"ticker": _TICKER}}}
    message = {
        "allOf": [
            {"$ref": "#/components/schemas/Base"},
            {"type": "object", "properties": {"price": _PRICE}},
        ]
    }
    item = {"get": {"responses": {"200": {"content": {"application/json": {"schema": message}}}}}}
    (channel,), _ = generate_ws(_snapshot({"/stocks/A": item}, schemas=schemas), _EMPTY)
    assert [f.wire_key for f in channel.fields] == ["ticker", "price"]


def test_short_keys_take_the_shape_s_lexicon() -> None:
    lexicons = {
        "trade": lexicon.Lexicon(
            name="trade",
            reason="test",
            entries={"p": lexicon.LongName(long_name="price", source="test")},
        )
    }
    hand = dataclasses.replace(_EMPTY, lexicons=lexicons, shapes={("stocks/T", ""): "trade"})
    (channel,), _ = generate_ws(_snapshot({"/stocks/T": _channel({"p": _PRICE})}), hand)
    assert channel.field("Price").wire_key == "p"
    assert _problems(_snapshot({"/stocks/T": _channel({"p": _PRICE})})) == [
        ("short-key", "stocks/T", "p")
    ]


def test_deprecated_and_excluded_channels_are_not_generated() -> None:
    message = _channel({"price": _PRICE})
    deprecated = _channel({"price": _PRICE}, operation_extra={"x-polygon-deprecation": {}})
    snapshot = _snapshot(
        {"/stocks/T": message, "/crypto/XL2": deprecated, "/launchpad/stocks/LV": message}
    )
    entry = exclusions.Exclusion(kind=Transport.WS, item="/launchpad/stocks/LV", reason="t")
    channels, coverage = generate_ws(snapshot, dataclasses.replace(_EMPTY, exclusions=(entry,)))
    assert [c.key for c in channels] == ["stocks/T"]
    assert coverage.specified == {"/stocks/T", "/crypto/XL2", "/launchpad/stocks/LV"}
    assert coverage.excluded == {"/launchpad/stocks/LV"}
    assert coverage.deprecated == {"/crypto/XL2"}


def test_exclusions_of_other_transports_are_left_to_their_generators() -> None:
    other = exclusions.Exclusion(kind=Transport.REST, item="/v1/x", reason="test")
    channels, _ = generate_ws(
        _snapshot({"/stocks/T": _channel({"price": _PRICE})}),
        dataclasses.replace(_EMPTY, exclusions=(other,)),
    )
    assert [c.key for c in channels] == ["stocks/T"]


@pytest.mark.parametrize("item", ["/stocks/GONE", "/crypto/XL2"], ids=["absent", "deprecated"])
def test_gate_stale_entry_an_exclusion_the_specification_does_not_need(item: str) -> None:
    deprecated = _channel({"price": _PRICE}, operation_extra={"deprecated": True})
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE}), "/crypto/XL2": deprecated})
    entry = exclusions.Exclusion(kind=Transport.WS, item=item, reason="t")
    assert _problems(snapshot, dataclasses.replace(_EMPTY, exclusions=(entry,))) == [
        ("stale-entry", item, "")
    ]


def test_gate_shape_a_path_that_is_not_a_channel() -> None:
    snapshot = _snapshot({"/launchpad/stocks/AM": _channel({"price": _PRICE})})
    assert _problems(snapshot) == [("shape", "/launchpad/stocks/AM", "")]


def test_gate_shape_two_paths_for_one_channel() -> None:
    message = _channel({"price": _PRICE})
    snapshot = _snapshot({"/stocks/FMV": message, "/business/stocks/FMV": message})
    assert _problems(snapshot) == [("shape", "/business/stocks/FMV", "")]


def test_gate_shape_an_unknown_vendor_extension() -> None:
    snapshot = _snapshot(
        {"/stocks/T": _channel({"price": _PRICE}, operation_extra={"x-polygon-new": True})}
    )
    assert _problems(snapshot) == [("shape", "/stocks/T", "x-polygon-new")]
    item = {**_channel({"price": _PRICE}), "x-polygon-draft": True, "summary": "Trades"}
    assert _problems(_snapshot({"/stocks/T": item})) == [("shape", "/stocks/T", "x-polygon-draft")]


@pytest.mark.parametrize(
    "item",
    [
        {},
        {"get": {"responses": {}}},
        {"get": {"responses": {"200": {"content": {"text/plain": {}}}}}},
    ],
    ids=["no operation", "no response", "no JSON"],
)
def test_gate_shape_a_channel_without_a_message_schema(item: dict[str, object]) -> None:
    assert _problems(_snapshot({"/stocks/T": item})) == [("shape", "stocks/T", "")]


def test_gate_shape_a_message_that_is_not_an_object() -> None:
    item = {
        "get": {
            "responses": {"200": {"content": {"application/json": {"schema": {"type": "string"}}}}}
        }
    }
    assert _problems(_snapshot({"/stocks/T": item})) == [("shape", "stocks/T", "")]


def test_gate_shape_a_property_that_cannot_be_resolved() -> None:
    message = {"price": {"$ref": "#/components/schemas/Missing"}}
    assert _problems(_snapshot({"/stocks/T": _channel(message)})) == [
        ("shape", "stocks/T price", "")
    ]


def test_gate_example_type_a_sample_the_declared_type_refuses() -> None:
    message = {"x": {"type": "integer", "description": "The exchange ID."}, "price": _PRICE}
    snapshot = _snapshot({"/forex/C": _channel(message, example={"x": "44", "price": 1.5})})
    assert _problems(snapshot, dataclasses.replace(_EMPTY, identity={"x": "test"})) == [
        ("example-type", "forex/C", "x")
    ]
    fix = overrides.TypeFix(conversion=Conversion.QUOTED_INTEGER, reason="test")
    hand = dataclasses.replace(_EMPTY, identity={"x": "test"}, type_fixes={("forex/C", "x"): fix})
    (channel,), coverage = generate_ws(snapshot, hand)
    assert (channel.field("X").wire_type, channel.field("X").conversion) == (
        WireType.LONG,
        Conversion.QUOTED_INTEGER,
    )
    assert coverage.uses["forex/C"].needed_conversions == {"x"}


def test_gate_example_type_an_example_without_the_message_s_keys() -> None:
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE}, example={"other": 1})})
    assert _problems(snapshot) == [
        ("example-type", "stocks/T", ""),
        ("example-type", "stocks/T", "other"),
    ]


def test_gate_example_type_a_key_the_message_does_not_declare() -> None:
    example = {"ev": "T", "price": 1.5, "z": 3}
    snapshot = _snapshot({"/futures/T": _channel({"price": _PRICE}, example=example)})
    assert _problems(snapshot) == [("example-type", "futures/T", "z")]
    excused = exclusions.SampleError(path="futures/T", wire_key="z", sample=3, reason="t")
    hand = dataclasses.replace(_EMPTY, sample_errors=(excused,))
    _, coverage = generate_ws(snapshot, hand)
    assert coverage.uses["futures/T"].example_keys == {"z"}
    # Another sample of the key is not the one the entry excuses.
    other = exclusions.SampleError(path="futures/T", wire_key="z", sample=4, reason="t")
    hand = dataclasses.replace(_EMPTY, sample_errors=(other,))
    assert _problems(snapshot, hand) == [("example-type", "futures/T", "z")]


def test_gate_docs_a_generated_channel_no_page_names() -> None:
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages={})
    assert _problems(snapshot) == [("docs", "stocks/T", "")]


def test_gate_docs_a_page_naming_a_channel_the_specification_lacks() -> None:
    pages = {"stocks/t.md": _feed("stocks", "T"), "stocks/x.md": _feed("stocks", "X")}
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages=pages)
    assert _problems(snapshot) == [("docs", "stocks/X", "")]


def test_gate_docs_a_page_that_names_no_feed_or_two() -> None:
    pages = {
        "stocks/t.md": _feed("stocks", "T"),
        "stocks/overview.md": "# Overview\n",
        "quickstart.md": "# Quickstart\n",
        "stocks/notes.md": "# Notes\n",
        "stocks/two.md": _feed("stocks", "T") + _feed("stocks", "Q"),
    }
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages=pages)
    assert _problems(snapshot) == [("docs", "", "stocks/notes.md"), ("docs", "", "stocks/two.md")]


_CONNECTION = "## Connection\n\n| Feed | URL |\n| --- | --- |\n"


def _feed_on(*rows: str, channel: str = "T") -> str:
    return _CONNECTION + "".join(rows) + f"\nThe channel prefix for this feed is `{channel}`.\n"


def test_a_channel_carries_the_hosts_its_page_lists_sorted_by_feed() -> None:
    page = _feed_on(
        "| Real-Time | `wss://socket.massive.com/stocks` |\n",
        "| Delayed | `wss://delayed.massive.com/stocks` |\n",
    )
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages={"stocks/t.md": page})
    (channel,), coverage = generate_ws(snapshot, _EMPTY)
    expected = (
        FeedHost(feed=Feed.DELAYED, host="delayed.massive.com"),
        FeedHost(feed=Feed.REAL_TIME, host="socket.massive.com"),
    )
    assert channel.hosts == expected
    assert coverage.hosts == {"stocks/T": expected}


@pytest.mark.parametrize(
    "rows",
    [
        ("| Night | `wss://night.massive.com/stocks` |\n",),
        (
            "| Delayed | `wss://delayed.massive.com/stocks` |\n",
            "| Delayed | `wss://other.massive.com/stocks` |\n",
        ),
    ],
    ids=["unknown feed", "a feed twice"],
)
def test_gate_docs_a_page_whose_feeds_cannot_be_read(rows: tuple[str, ...]) -> None:
    pages = {"stocks/t.md": _feed_on(*rows)}
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages=pages)
    assert _problems(snapshot) == [("docs", "", "stocks/t.md")]


def test_gate_docs_two_pages_of_one_channel_that_list_other_hosts() -> None:
    pages = {
        "stocks/t.md": _feed_on("| Real-Time | `wss://socket.massive.com/stocks` |\n"),
        "stocks/u.md": _feed_on("| Business | `wss://business.massive.com/stocks` |\n"),
    }
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages=pages)
    assert _problems(snapshot) == [("docs", "stocks/T", "stocks/u.md")]


def test_two_pages_of_one_channel_that_agree_pass() -> None:
    page = _feed_on("| Real-Time | `wss://socket.massive.com/stocks` |\n")
    pages = {"stocks/t.md": page, "stocks/u.md": page}
    snapshot = _snapshot({"/stocks/T": _channel({"price": _PRICE})}, pages=pages)
    (channel,), coverage = generate_ws(snapshot, _EMPTY)
    assert channel.only_host == "socket.massive.com"
    assert coverage.pages["stocks/T"] == ("stocks/t.md", "stocks/u.md")


def test_a_snapshot_without_a_websocket_specification_generates_nothing() -> None:
    channels, coverage = generate_ws(_snapshot({}), _EMPTY)
    assert channels == ()
    assert coverage.specified == frozenset()
