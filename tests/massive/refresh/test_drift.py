"""Tests for the drift check, and for the workflow script that files its report as an issue.

Each kind of drift is shown on a synthetic pair of snapshots: a small vendored one and a live
one that differs from it in exactly that way. The real vendored snapshot, compared with
itself, must report nothing. `check` runs end to end on a synthetic snapshot stored on disk,
with the live sources served from a dictionary and the bucket from the S3 fake. The issue
script runs under bash against the fake `gh` of `tests/_fakes/gh.py`.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import pytest

from deephaven_massive._massive.net.errors import WireEntitlementError
from deephaven_massive._massive.refresh import drift, fetch, snapshot, spec_diff
from deephaven_massive._massive.refresh.drift import Committed, Drift, Report, Section
from deephaven_massive._massive.refresh.exclusions import Exclusion
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.snapshot import Snapshot
from deephaven_massive._massive.spec.types import Transport
from tests._fakes.gh import FakeGh

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._massive.refresh.openapi import JsonObject

FETCHED = dt.date(2026, 10, 27)
ROOT = Path(__file__).resolve().parents[3]
REST = "https://massive.com/docs/rest/"
WS = "https://massive.com/docs/websocket/"
FLAT = "https://massive.com/docs/flat-files/"
COMMAND = "uv run python -m deephaven_massive._massive.refresh"
NO_HAND = HandInput(
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
    transport_only={},
    venues={},
)
"""Hand input with every table empty, so a synthetic snapshot needs none."""

needs_snapshot = pytest.mark.skipif(
    not snapshot.DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)


# Synthetic vendor documents.


def body(fields: Mapping[str, object]) -> dict[str, Any]:
    # A GET operation whose 200 response is `{"results": [<object of fields>]}`.
    rows = {"type": "array", "items": {"type": "object", "properties": dict(fields)}}
    schema = {"type": "object", "properties": {"results": rows}}
    return {"responses": {"200": {"content": {"application/json": {"schema": schema}}}}}


PRICE = {"price": {"type": "number", "description": "The price."}}


def item(
    fields: Mapping[str, object] = PRICE,
    *,
    parameters: tuple[dict[str, Any], ...] = (),
    status: str = "",
    extensions: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    # A path item with one GET operation, deprecated or draft when `status` says so, and with
    # the given extra keys on the operation.
    operation: dict[str, Any] = {**body(fields), "parameters": list(parameters)}
    operation |= extensions or {}
    if status == "deprecated":
        operation["deprecated"] = True
    found: dict[str, Any] = {"get": operation}
    if status == "draft":
        found["x-polygon-draft"] = True
    return found


def rest_page(path: str) -> str:
    return f"# A page\n\n**Endpoint:** `GET {path}`\n\nWhat it returns.\n"


def ws_page(cluster: str, channel: str) -> str:
    return (
        f"# A feed\n\n| Real-Time | `wss://socket.massive.com/{cluster}` |\n\n"
        f"The channel prefix for this feed is `{channel}`.\n"
    )


def overview(*rows: tuple[str, str]) -> str:
    lines = ["# Overview", "", "| Dataset | Description | Location |", "|---|---|---|"]
    lines += [f"| [D]({FLAT}{page}) | A dataset. | `S3 {prefix}` |" for page, prefix in rows]
    return "\n".join(lines) + "\n"


def dataset_page(columns: Mapping[str, str], header: tuple[str, ...] | None = None) -> str:
    header = tuple(columns) if header is None else header
    lines = [
        "# A dataset",
        "",
        "## Columns",
        "",
        "| Column | Type | Description |",
        "|---|---|---|",
    ]
    lines += [f"| `{name}` | {kind} | The {name}. |" for name, kind in columns.items()]
    lines += ["", "## Example Data", "", f"| {' | '.join(header)} |"]
    lines += [f"| {' | '.join('---' for _ in header)} |", f"| {' | '.join('1' for _ in header)} |"]
    return "\n".join(lines) + "\n"


WIDGETS = {"stocks/widgets.md": rest_page("/v1/widgets")}


def make(
    *,
    rest: Mapping[str, dict[str, Any]] | None = None,
    ws: Mapping[str, dict[str, Any]] | None = None,
    rest_pages: Mapping[str, str] = WIDGETS,
    ws_pages: Mapping[str, str] | None = None,
    flat_pages: Mapping[str, str] | None = None,
    components: Mapping[str, Any] | None = None,
) -> Snapshot:
    # A snapshot of one REST path, one channel, and one flat-file dataset, or the given parts.
    rest = {"/v1/widgets": item()} if rest is None else rest
    ws = {"/stocks/W": {"get": body(PRICE)}} if ws is None else ws
    ws_pages = {"stocks/widgets.md": ws_page("stocks", "W")} if ws_pages is None else ws_pages
    if flat_pages is None:
        flat_pages = {
            "stocks/overview.md": overview(("stocks/widgets.md", "us_stocks_sip/widgets_v1")),
            "stocks/widgets.md": dataset_page({"ticker": "string", "price": "number"}),
        }
    return Snapshot(
        openapi={"openapi": "3.0.3", "paths": dict(rest), "components": components or {}},
        llms="",
        pages=dict(rest_pages),
        fetched=FETCHED,
        page_urls={name: REST + name for name in rest_pages},
        index_url="https://massive.com/docs/llms.txt",
        websocket={"openapi": "3.0.3", "paths": dict(ws)},
        websocket_pages=dict(ws_pages),
        websocket_page_urls={name: WS + name for name in ws_pages},
        flat_file_pages=dict(flat_pages),
        flat_file_page_urls={name: FLAT + name for name in flat_pages},
    )


COMMITTED = Committed(
    rest=frozenset({"/v1/widgets"}),
    ws=frozenset({"stocks/W"}),
    flat_files=frozenset({"us_stocks_sip/widgets_v1"}),
)


def compare(
    after: Snapshot,
    before: Snapshot | None = None,
    *,
    committed: Committed = COMMITTED,
    hand: HandInput = NO_HAND,
) -> list[str]:
    # Compare `after` with `before` (`make()` when omitted), returning the report lines.
    found = drift.compare(before or make(), after, committed=committed, hand=hand)
    return [f"{d.section.name} {d.line()}" for d in found]


# No drift.


def test_a_snapshot_compared_with_itself_has_no_drift() -> None:
    assert compare(make()) == []


@needs_snapshot
def test_the_vendored_snapshot_compared_with_itself_has_no_drift() -> None:
    vendored = Snapshot.load()
    hand = HandInput()
    committed = Committed.load()
    assert drift.compare(vendored, vendored, committed=committed, hand=hand) == []
    assert drift.gate_stops(vendored, hand) == []


def test_committed_keys_load_from_the_generated_files() -> None:
    committed = Committed.load()
    assert "/v3/trades/{stockTicker}" in committed.rest
    assert "stocks/T" in committed.ws
    assert "us_futures_{venue}/trades_v1" in committed.flat_files


def test_descriptions_and_examples_are_not_drift() -> None:
    reworded = {"price": {"type": "number", "description": "Reworded.", "example": 1.5}}
    assert compare(make(rest={"/v1/widgets": item(reworded)})) == []


# REST and WebSocket paths.


def test_a_new_documented_path_is_new_and_unaccounted() -> None:
    after = make(
        rest={"/v1/widgets": item(), "/v1/gadgets": item()},
        rest_pages={**WIDGETS, "stocks/gadgets.md": rest_page("/v1/gadgets")},
    )
    assert compare(after) == [
        "UNACCOUNTED - REST `/v1/gadgets`: documented, but neither generated nor excluded",
        "CHANGES - REST `/v1/gadgets`: new (current)",
        f"CHANGES - documentation `{REST}stocks/gadgets.md`: new in `llms.txt`",
    ]


def test_an_excluded_deprecated_or_draft_documented_path_is_accounted_for() -> None:
    pages = {f"stocks/{n}.md": rest_page(f"/v1/{n}") for n in ("a", "b", "c")}
    snap = make(
        rest={
            "/v1/widgets": item(),
            "/v1/a": item(),
            "/v1/b": item(status="deprecated"),
            "/v1/c": item(status="draft"),
        },
        rest_pages={**WIDGETS, **pages},
    )
    excluded = (Exclusion(kind=Transport.REST, item="/v1/a", reason="r"),)
    assert compare(snap, snap, hand=dataclasses.replace(NO_HAND, exclusions=excluded)) == []


def test_a_removed_path_is_a_deprecation_while_generated_and_a_change_otherwise() -> None:
    before = make(rest={"/v1/widgets": item(), "/v1/old": item(status="deprecated")})
    assert compare(make(rest={}), before) == [
        "CHANGES - REST `/v1/old`: removed",
        "DEPRECATIONS - REST `/v1/widgets`: removed, but `rest.json` still has it",
    ]


def test_a_committed_source_neither_specification_has_is_a_deprecation() -> None:
    # The snapshot was refetched after the vendor removed them, but the schemas still hold them.
    gone = make(rest={}, ws={})
    assert compare(gone, gone) == [
        "DEPRECATIONS - REST `/v1/widgets`: in neither specification, but `rest.json` still has it",
        "DEPRECATIONS - WebSocket `stocks/W`: in neither specification, but `ws.json` still has it",
    ]


def test_a_newly_deprecated_path_is_a_deprecation() -> None:
    before = make(rest={"/v1/widgets": item(), "/v1/other": item()})
    after = make(rest={"/v1/widgets": item(), "/v1/other": item(status="deprecated")})
    assert compare(after, before) == ["DEPRECATIONS - REST `/v1/other`: newly deprecated"]


def test_a_deprecated_path_still_generated_is_a_deprecation_even_when_not_new() -> None:
    deprecated = make(rest={"/v1/widgets": item(status="deprecated")})
    assert compare(deprecated, deprecated) == [
        "DEPRECATIONS - REST `/v1/widgets`: deprecated, but `rest.json` still has it"
    ]
    # A path deprecated before and still not generated is no news.
    nothing = dataclasses.replace(COMMITTED, rest=frozenset())
    assert compare(deprecated, deprecated, committed=nothing) == []


def test_a_status_change_other_than_deprecation_is_a_change() -> None:
    before = make(rest={"/v1/widgets": item(status="draft")})
    assert compare(make(), before) == ["CHANGES - REST `/v1/widgets`: now current (was draft)"]


def test_parameters_added_removed_and_changed() -> None:
    enum = {"type": "string", "enum": ["a", "b"]}
    kind: dict[str, Any] = {"name": "kind", "in": "query", "schema": enum}
    limit: dict[str, Any] = {"name": "limit", "in": "query", "schema": {"type": "integer"}}
    ticker = {"name": "ticker", "in": "path", "required": True, "schema": {"type": "string"}}
    before = make(rest={"/v1/widgets": {**item(parameters=(kind, limit)), "parameters": [ticker]}})
    kind_more = {**kind, "schema": {"type": "string", "enum": ["b", "a", "c"]}}
    sort = {"name": "sort", "in": "query", "schema": {"$ref": "#/components/schemas/Sort"}}
    sorts = {"schemas": {"Sort": {"type": "string"}}}
    changed = item(parameters=(kind_more, sort))
    after = make(rest={"/v1/widgets": {**changed, "parameters": [ticker]}}, components=sorts)
    assert compare(after, before) == [
        (
            "CHANGES - REST `/v1/widgets`: parameter `kind` changed from "
            "`query string enum [a, b]` to `query string enum [a, b, c]`"
        ),
        "CHANGES - REST `/v1/widgets`: parameter `limit` removed (was `query integer`)",
        "CHANGES - REST `/v1/widgets`: parameter `sort` added: `query string`",
    ]
    # The path-level parameter is read too, with whether it is required.
    without = make(rest={"/v1/widgets": changed}, components=sorts)
    assert compare(without, before)[-1] == (
        "CHANGES - REST `/v1/widgets`: parameter `ticker` removed (was `path string, required`)"
    )


def test_fields_added_removed_retyped_and_retagged() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "IMilliseconds"}}
    day = {"type": "object", "properties": {"c": {"type": "number"}}}
    before = make(
        rest={
            "/v1/widgets": item(
                {"price": {"type": "number"}, "t": tagged, "size": {"type": "integer"}, "day": day}
            )
        }
    )
    day_more = {"type": "object", "properties": {"c": {"type": "number"}, "o": {"type": "number"}}}
    after = make(
        rest={
            "/v1/widgets": item(
                {
                    "price": {"type": "string", "format": "decimal"},
                    "t": {**tagged, "x-polygon-go-type": {"name": "INanoseconds"}},
                    "day": day_more,
                    "tags": {"type": "array", "items": {"type": "string"}},
                }
            )
        }
    )
    path = "CHANGES - REST `/v1/widgets`:"
    assert compare(after, before) == [
        f"{path} field `results[].day.o` added: `number`",
        f"{path} field `results[].price` retyped from `number` to `string (decimal)`",
        f"{path} field `results[].size` removed (was `integer`)",
        f"{path} field `results[].t` unit tag changed from `IMilliseconds` to `INanoseconds`",
        f"{path} field `results[].tags` added: `array of string`",
    ]


def test_requiredness_changes_including_an_enclosing_objects() -> None:
    def widgets(*, required: list[str], day_required: list[str]) -> Snapshot:
        day = {"type": "object", "required": ["c"], "properties": {"c": {"type": "number"}}}
        fields = {"price": {"type": "number"}, "day": day}
        found = item(fields)
        rows = found["get"]["responses"]["200"]["content"]["application/json"]["schema"]
        rows["properties"]["results"]["items"]["required"] = required
        day["required"] = day_required
        return make(rest={"/v1/widgets": found})

    before = widgets(required=["price", "day"], day_required=["c"])
    path = "CHANGES - REST `/v1/widgets`:"
    # `day.c` stays required in `day`, but `day` itself no longer is.
    assert compare(widgets(required=["price"], day_required=["c"]), before) == [
        f"{path} field `results[].day.c` no longer required"
    ]
    assert compare(before, widgets(required=["day"], day_required=[])) == [
        f"{path} field `results[].day.c` now required",
        f"{path} field `results[].price` now required",
    ]


def test_a_tag_added_is_reported_against_none() -> None:
    before = make(rest={"/v1/widgets": item({"t": {"type": "integer"}})})
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "INanoseconds"}}
    after = make(rest={"/v1/widgets": item({"t": tagged})})
    assert compare(after, before) == [
        (
            "CHANGES - REST `/v1/widgets`: field `results[].t` unit tag changed from `none` to "
            "`INanoseconds`"
        )
    ]


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ({"oneOf": [{"type": "string"}, {"type": "number"}]}, "oneOf of [number | string]"),
        (
            {"anyOf": [{"type": "object", "properties": {"b": {}, "a": {"type": "integer"}}}, {}]},
            "anyOf of [object {a: integer, b: untyped} | untyped]",
        ),
        ({"description": "no type"}, "untyped"),
        ({"type": ["string", "null"]}, "untyped"),
        ({"type": "array", "items": {"type": "array", "items": {}}}, "array of array of untyped"),
        ({"type": "object"}, "object"),
        ({"type": "integer", "x-polygon-go-type": "not an object"}, "integer"),
    ],
)
def test_how_a_field_type_is_described(schema: JsonObject, expected: str) -> None:
    after = make(rest={"/v1/widgets": item({**PRICE, "x": schema})})
    assert compare(after) == [
        f"CHANGES - REST `/v1/widgets`: field `results[].x` added: `{expected}`"
    ]


def test_a_body_that_is_itself_a_value_or_absent() -> None:
    text = {"application/json": {"schema": {"type": "string"}}}
    scalar = {"get": {"responses": {"200": {"content": text}}}}
    before = make(rest={"/v1/widgets": item(), "/v1/text": scalar, "/v1/empty": {"get": {}}})
    after = make(
        rest={"/v1/widgets": item(), "/v1/text": {"get": body({})}, "/v1/empty": {"get": {}}}
    )
    assert compare(after, before) == [
        "CHANGES - REST `/v1/text`: field `(body)` removed (was `string`)",
        "CHANGES - REST `/v1/text`: field `results` added: `array of object`",
    ]


def test_a_schema_that_refers_to_itself_is_compared_to_a_bounded_depth() -> None:
    node = {"type": "object", "properties": {"child": {"$ref": "#/components/schemas/Node"}}}
    after = make(
        rest={"/v1/widgets": item({**PRICE, "tree": {"$ref": "#/components/schemas/Node"}})},
        components={"schemas": {"Node": node}},
    )
    (line,) = compare(after)
    # The body, `results`, its items, and `tree` take four of the levels.
    key = ".".join(["results[]", "tree", *["child"] * (spec_diff.MAX_NESTING - 3)])
    assert line == f"CHANGES - REST `/v1/widgets`: field `{key}` added: `(nested too deep)`"


def test_an_array_that_contains_itself_is_described_to_a_bounded_depth() -> None:
    nested = {"type": "array", "items": {"$ref": "#/components/schemas/Nested"}}
    after = make(
        rest={"/v1/widgets": item({**PRICE, "n": {"$ref": "#/components/schemas/Nested"}})},
        components={"schemas": {"Nested": nested}},
    )
    (line,) = compare(after)
    # The body, `results`, its items, and `n` take four of the levels.
    described = "array of " * (spec_diff.MAX_NESTING - 3) + "(nested too deep)"
    assert line == f"CHANGES - REST `/v1/widgets`: field `results[].n` added: `{described}`"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ([{"type": "string"}, {"type": "number"}], [{"type": "string"}, {"type": "boolean"}]),
        (
            [{"type": "object", "properties": {"a": {"type": "integer"}}}, {"type": "string"}],
            [{"type": "object", "properties": {"a": {"type": "number"}}}, {"type": "string"}],
        ),
    ],
    ids=["branch-type", "inside-an-object-branch"],
)
def test_a_choice_with_the_same_number_of_branches_can_still_change(
    old: list[dict[str, Any]], new: list[dict[str, Any]]
) -> None:
    before = make(rest={"/v1/widgets": item({"x": {"oneOf": old}})})
    after = make(rest={"/v1/widgets": item({"x": {"oneOf": new}})})
    (line,) = compare(after, before)
    assert line.startswith("CHANGES - REST `/v1/widgets`: field `results[].x` retyped from")
    # Branch order, descriptions, and examples are not drift.
    reordered = [{**branch, "description": "d", "example": 1} for branch in reversed(old)]
    assert compare(make(rest={"/v1/widgets": item({"x": {"oneOf": reordered}})}), before) == []


def test_a_path_that_can_no_longer_or_can_again_be_read() -> None:
    unreadable = make(rest={"/v1/widgets": {"get": {"$ref": "#/components/missing"}}})
    assert compare(unreadable) == [
        (
            "CHANGES - REST `/v1/widgets`: can no longer be read: "
            "`reference '#/components/missing' names nothing`"
        )
    ]
    assert compare(unreadable, unreadable) == []
    assert compare(make(), unreadable) == [
        "CHANGES - REST `/v1/widgets`: can be read again (current)"
    ]


def test_a_specification_without_paths_has_none() -> None:
    before = make(rest={}, rest_pages={})
    after = dataclasses.replace(before, openapi={"openapi": "3.0.3"})
    assert compare(after, before, committed=dataclasses.replace(COMMITTED, rest=frozenset())) == []


def test_a_channel_whose_ev_values_leave_out_its_channel() -> None:
    def message(events: list[str] | None) -> dict[str, Any]:
        event: dict[str, Any] = {"type": "string"}
        if events is not None:
            event["enum"] = events
        schema = {"type": "object", "properties": {**PRICE, "ev": event}}
        return {
            "get": {"responses": {"200": {"content": {"application/json": {"schema": schema}}}}}
        }

    before = make(ws={"/stocks/W": message(["W"])})
    after = make(ws={"/stocks/W": message(["X", "Y"])})
    assert compare(after, before) == [
        (
            "CHANGES - WebSocket `/stocks/W`: field `ev` retyped from `string enum [W]` to "
            "`string enum [X, Y]`"
        ),
        "CHANGES - WebSocket `/stocks/W`: the `ev` values `X, Y` leave out the channel `W`",
    ]
    assert compare(before, before) == []
    # An `ev` without listed values, or a channel no source key has, is not checked.
    unlisted = make(ws={"/stocks/W": message(None), "/launchpad/stocks/Z": message(["Q"])})
    assert compare(unlisted, unlisted) == []


def test_websocket_channels_new_removed_deprecated_and_changed() -> None:
    before = make(ws={"/stocks/W": {"get": body(PRICE)}, "/stocks/Q": {"get": body(PRICE)}})
    deprecated = {**body({"price": {"type": "string"}}), "x-polygon-deprecation": {}}
    after = make(ws={"/stocks/W": {"get": deprecated}, "/stocks/N": {"get": body(PRICE)}})
    assert compare(after, before) == [
        "CHANGES - WebSocket `/stocks/N`: new (current)",
        "CHANGES - WebSocket `/stocks/Q`: removed",
        (
            "CHANGES - WebSocket `/stocks/W`: field `results[].price` retyped from `number` to "
            "`string`"
        ),
        "DEPRECATIONS - WebSocket `/stocks/W`: deprecated, but `ws.json` still has it",
    ]
    assert compare(make(ws={})) == [
        "DEPRECATIONS - WebSocket `/stocks/W`: removed, but `ws.json` still has it"
    ]


# Documentation pages.


def test_pages_added_to_or_removed_from_the_index() -> None:
    pages = {"stocks/widgets.md": ws_page("stocks", "W"), "overview.md": "# Overview\n"}
    nothing = dataclasses.replace(COMMITTED, rest=frozenset())
    after = make(rest={}, rest_pages={}, ws_pages=pages)
    before = make(rest={})
    assert compare(after, before, committed=nothing) == [
        f"CHANGES - documentation `{REST}stocks/widgets.md`: removed from `llms.txt`",
        f"CHANGES - documentation `{WS}overview.md`: new in `llms.txt`",
    ]


def test_a_page_that_documents_another_endpoint_or_feed() -> None:
    after = make(
        rest={"/v1/widgets": item(), "/v2/widgets": item()},
        rest_pages={"stocks/widgets.md": rest_page("/v2/widgets")},
        ws={"/stocks/W": {"get": body(PRICE)}, "/stocks/X": {"get": body(PRICE)}},
        ws_pages={"stocks/widgets.md": ws_page("stocks", "X")},
    )
    assert compare(after) == [
        "UNACCOUNTED - REST `/v2/widgets`: documented, but neither generated nor excluded",
        "UNACCOUNTED - WebSocket `stocks/X`: documented, but neither generated nor excluded",
        "CHANGES - REST `/v2/widgets`: new (current)",
        "CHANGES - WebSocket `/stocks/X`: new (current)",
        (
            f"CHANGES - documentation `{REST}stocks/widgets.md`: the endpoint changed from "
            "`/v1/widgets` to `/v2/widgets`"
        ),
        (
            f"CHANGES - documentation `{WS}stocks/widgets.md`: the feed changed from `stocks/W` "
            "to `stocks/X`"
        ),
    ]


def test_an_overview_page_names_no_endpoint_or_feed() -> None:
    before = make(rest_pages={**WIDGETS, "overview.md": "# Overview\n"})
    after = make(rest_pages={**WIDGETS, "overview.md": rest_page("/v1/widgets")})
    ws_before = make(ws_pages={"stocks/widgets.md": ws_page("stocks", "W"), "o.md": "# O\n"})
    ws_after = make(
        ws_pages={"stocks/widgets.md": ws_page("stocks", "W"), "o.md": ws_page("x", "Y")}
    )
    assert compare(after, before) == [
        (
            f"CHANGES - documentation `{REST}overview.md`: the endpoint changed from `none` to "
            "`/v1/widgets`"
        )
    ]
    assert compare(ws_after, ws_before)[-1] == (
        f"CHANGES - documentation `{WS}o.md`: the feed changed from `none` to `x/Y`"
    )


def test_flat_file_columns_header_and_overview_rows() -> None:
    after = make(
        flat_pages={
            "stocks/overview.md": overview(
                ("stocks/widgets.md", "us_stocks_sip/widgets_v1"),
                ("stocks/gadgets.md", "us_stocks_sip/gadgets_v1"),
            ),
            "stocks/widgets.md": dataset_page(
                {"ticker": "string", "price": "string", "size": "integer"},
                ("price", "ticker", "size"),
            ),
            "stocks/gadgets.md": dataset_page({"ticker": "string"}),
        }
    )
    widgets = f"CHANGES - documentation `{FLAT}stocks/widgets.md`:"
    assert compare(after) == [
        (
            "UNACCOUNTED - flat files `us_stocks_sip/gadgets_v1`: an overview names it, but it "
            "is neither generated nor excluded"
        ),
        f"CHANGES - documentation `{FLAT}stocks/gadgets.md`: new in `llms.txt`",
        (
            f"CHANGES - documentation `{FLAT}stocks/overview.md`: overview row "
            f"`us_stocks_sip/gadgets_v1` added: `{FLAT}stocks/gadgets.md`"
        ),
        f"{widgets} column `price` changed from `number` to `string`",
        f"{widgets} column `size` added: `integer`",
        f"{widgets} the Example Data header changed from `ticker, price` to `price, ticker, size`",
    ]


def test_an_unreadable_page_is_a_change() -> None:
    bad = "# A dataset\n\n## Columns\n\n| Column | Type |\n| not a separator |\n"
    pages = {
        "stocks/overview.md": overview(("stocks/widgets.md", "us_stocks_sip/widgets_v1")),
        "stocks/widgets.md": bad,
    }
    widgets = f"CHANGES - documentation `{FLAT}stocks/widgets.md`:"
    assert compare(make(flat_pages=pages)) == [
        f"{widgets} column `price` removed (was `number`)",
        f"{widgets} column `ticker` removed (was `string`)",
        (
            f"CHANGES - documentation `{FLAT}stocks/widgets.md`: the Example Data header removed "
            "(was `ticker, price`)"
        ),
        (
            f"CHANGES - documentation `{FLAT}stocks/widgets.md`: the page added: `unreadable: "
            "unreadable Columns header: '| Column | Type |'`"
        ),
    ]


def test_unreadable_rest_and_websocket_pages_document_nothing() -> None:
    twice = rest_page("/v1/widgets") + rest_page("/v1/other")
    feeds = ws_page("stocks", "W") + ws_page("options", "W")
    after = make(rest_pages={"stocks/widgets.md": twice}, ws_pages={"stocks/widgets.md": feeds})
    assert [line.split(": ")[0] for line in compare(after)] == [
        f"CHANGES - documentation `{REST}stocks/widgets.md`",
        f"CHANGES - documentation `{REST}stocks/widgets.md`",
        f"CHANGES - documentation `{WS}stocks/widgets.md`",
        f"CHANGES - documentation `{WS}stocks/widgets.md`",
    ]


# What nothing accounts for.


def test_a_dataset_page_no_overview_names_is_unaccounted() -> None:
    pages = {
        **make().flat_file_pages,
        "stocks/stray.md": dataset_page({"ticker": "string"}),
        "quickstart.md": "# Quickstart\n",
    }
    assert compare(make(flat_pages=pages))[0] == (
        f"UNACCOUNTED - flat files `{FLAT}stocks/stray.md`: a dataset page no overview row names"
    )


def test_a_committed_prefix_no_overview_names_is_a_deprecation() -> None:
    pages = {"stocks/overview.md": overview(), "stocks/widgets.md": dataset_page({"a": "string"})}
    lines = compare(make(flat_pages=pages))
    assert [line for line in lines if not line.startswith("CHANGES")] == [
        f"UNACCOUNTED - flat files `{FLAT}stocks/widgets.md`: a dataset page no overview row names",
        (
            "DEPRECATIONS - flat files `us_stocks_sip/widgets_v1`: no overview names it any more, "
            "but `flatfiles.json` still has it"
        ),
    ]


def test_venue_prefixes_fold_into_their_template_and_exclusions_count() -> None:
    rows = [(f"futures/{v}.md", f"us_futures_{v}/trades_v1") for v in ("cme", "cbot")]
    flat = {
        "futures/overview.md": overview(*rows, ("futures/x.md", "us_futures_cme/x_v1")),
        "futures/cme.md": dataset_page({"ticker": "string"}),
        "futures/cbot.md": dataset_page({"ticker": "string"}),
        "futures/x.md": dataset_page({"ticker": "string"}),
    }
    snap = make(flat_pages=flat)
    hand = dataclasses.replace(
        NO_HAND,
        venues={"cme": "r", "cbot": "r"},
        exclusions=(
            Exclusion(kind=Transport.FLAT_FILES, item="us_futures_{venue}/x_v1", reason="r"),
        ),
    )
    committed = dataclasses.replace(
        COMMITTED, flat_files=frozenset({"us_futures_{venue}/trades_v1"})
    )
    assert compare(snap, snap, hand=hand, committed=committed) == []


def test_a_websocket_exclusion_or_deprecation_accounts_for_a_feed() -> None:
    deprecated = {"get": {**body(PRICE), "deprecated": True}}
    snap = make(
        ws={
            "/stocks/W": {"get": body(PRICE)},
            "/stocks/X": {"get": body(PRICE)},
            "/stocks/D": deprecated,
            # A deprecated path of a shape no source key has.
            "/launchpad/stocks/D": deprecated,
        },
        ws_pages={f"stocks/{c.lower()}.md": ws_page("stocks", c) for c in ("W", "X", "D")},
    )
    excluded = (Exclusion(kind=Transport.WS, item="/stocks/X", reason="r"),)
    assert compare(snap, snap, hand=dataclasses.replace(NO_HAND, exclusions=excluded)) == []


# Gates and the bucket.


def test_gate_stops_are_changes_naming_the_gate_source_and_key() -> None:
    assert drift.gate_stops(make(), NO_HAND) == []
    unknown = item(extensions={"x-polygon-new": True})
    after = make(rest={"/v1/widgets": unknown, "/v1/hidden": item()})
    found = drift.gate_stops(after, NO_HAND)
    assert [d.line() for d in found] == [
        (
            "- refresh `/v1/hidden`: the docs gate would stop: `generated, but no documentation "
            "page names it; exclude it as undocumented`"
        ),
        (
            "- refresh `/v1/widgets`: the shape gate would stop on `x-polygon-new`: `unknown "
            "vendor extension`"
        ),
    ]
    assert {d.section for d in found} == {Section.CHANGES}


def test_unnamed_bucket_prefixes_are_unaccounted() -> None:
    pages = {
        "overview.md": overview(("stocks/w.md", "us_stocks_sip/w_v1")),
        "stocks/w.md": dataset_page({"a": "string"}),
    }
    hand = dataclasses.replace(
        NO_HAND,
        venues={"cme": "r"},
        exclusions=(Exclusion(kind=Transport.FLAT_FILES, item="global_excluded/x_v1", reason="r"),),
    )
    committed = dataclasses.replace(
        COMMITTED, flat_files=frozenset({"us_futures_{venue}/trades_v1"})
    )
    found = drift.unnamed_prefixes(
        ["us_stocks_sip", "us_futures_cme", "global_excluded", "mystery", "another"],
        make(flat_pages=pages),
        committed=committed,
        hand=hand,
    )
    detail = "a bucket prefix no overview row, generated source, or exclusion names"
    assert [d.line() for d in found] == [
        f"- bucket `another`: {detail}",
        f"- bucket `mystery`: {detail}",
    ]
    assert {d.section for d in found} == {Section.UNACCOUNTED}


# The report.


def test_the_report_without_drift_is_one_line() -> None:
    assert Report(items=(), fetched=FETCHED, bucket_listed=True).text() == (
        "No drift: the live sources match the vendored snapshot of 2026-10-27.\n"
    )
    assert Report(items=(), fetched=FETCHED, bucket_listed=False).text() == (
        "No drift: the live sources match the vendored snapshot of 2026-10-27.\n\n"
        "The flat-file bucket was not listed: set `MASSIVE_S3_ACCESS_KEY_ID` and "
        "`MASSIVE_S3_SECRET_ACCESS_KEY` to include it.\n"
    )


def test_the_report_lists_each_section_found_with_its_action() -> None:
    items = (
        Drift(section=Section.UNACCOUNTED, area="REST", item="/v1/new", detail="documented"),
        Drift(section=Section.DEPRECATIONS, area="REST", item="/v1/old", detail="deprecated"),
    )
    assert Report(items=items, fetched=FETCHED, bucket_listed=True).text() == (
        "# Massive API drift\n\n"
        "The live vendor sources differ from the vendored snapshot of 2026-10-27 "
        "(`vendor/massive/`) in 2 item(s).\n\n"
        "## Unaccounted items (1)\n\n"
        f"Action: run `{COMMAND} --fetch` and the plain refresh, then add a dataset, add an "
        "`EXCLUSIONS` entry with its reason, or defer the item with a `DEFERRED` entry "
        "that names its issue.\n\n"
        "- REST `/v1/new`: documented\n\n"
        "## Deprecations (1)\n\n"
        "Action: replace the dataset's path, or add it to `EXCLUSIONS` with its reason.\n\n"
        "- REST `/v1/old`: deprecated\n\n"
        f"Reproduce this report with `{COMMAND} --check` in a source checkout.\n"
    )
    changes = (Drift(section=Section.CHANGES, area="REST", item="/v1/x", detail="new"),)
    text = Report(items=changes, fetched=FETCHED, bucket_listed=False).text()
    assert "## Field, type, and column changes (1)\n\n" in text
    assert f"Action: run `{COMMAND} --fetch` and the plain refresh, adding a lexicon" in text
    assert text.endswith("`MASSIVE_S3_SECRET_ACCESS_KEY` to include it.\n")


@pytest.mark.parametrize(
    ("value", "quoted"),
    [
        ("`@team` #1\n[x](y)", "`'@team' #1 [x](y)`"),
        ("", "`(none)`"),
        ("  ", "`(none)`"),
    ],
)
def test_vendor_text_cannot_break_out_of_its_code_span(value: str, quoted: str) -> None:
    line = Drift(section=Section.CHANGES, area="REST", item=value, detail="x").line()
    assert line == f"- REST {quoted}: x"


# The whole check: the vendored snapshot on disk, the live sources from a dictionary.


def vendor_files(snap: Snapshot) -> dict[str, tuple[str, bytes]]:
    # The files of a stored copy of `snap` by name, each with its URL and its bytes.
    llms = "# Massive\n"
    for title, urls in (
        ("REST API", snap.page_urls),
        ("WebSocket", snap.websocket_page_urls),
        ("Flat Files", snap.flat_file_page_urls),
    ):
        llms += f"\n## {title}\n\n" + "".join(
            f"- [Page]({url}): A page.\n" for url in urls.values()
        )
    files = {
        snapshot.OPENAPI: (fetch.OPENAPI_URL, json.dumps(snap.openapi, indent=1).encode() + b"\n"),
        snapshot.WEBSOCKET: (
            fetch.WEBSOCKET_URL,
            json.dumps(snap.websocket, indent=1).encode() + b"\n",
        ),
        snapshot.LLMS: (fetch.LLMS_URL, llms.encode()),
    }
    for directory, pages, urls in (
        ("rest", snap.pages, snap.page_urls),
        ("websocket", snap.websocket_pages, snap.websocket_page_urls),
        ("flat-files", snap.flat_file_pages, snap.flat_file_page_urls),
    ):
        files |= {f"{directory}/{name}": (urls[name], pages[name].encode()) for name in pages}
    return files


def store(snap: Snapshot, directory: Path) -> Path:
    snapshot.store(
        directory,
        [
            snapshot.VendorFile(name=name, url=url, raw=data, stored=data)
            for name, (url, data) in vendor_files(snap).items()
        ],
        fetched=FETCHED,
    )
    return directory


def served(snap: Snapshot) -> dict[str, bytes]:
    return dict(vendor_files(snap).values())


class FakeS3(Protocol):
    listings: list[str | None]

    def put(self, key: str, data: bytes) -> None: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...


CREDENTIALS = {
    "MASSIVE_S3_ACCESS_KEY_ID": "AKIDDRIFT",
    "MASSIVE_S3_SECRET_ACCESS_KEY": "drift-secret",
}


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {"MASSIVE_S3_ACCESS_KEY_ID": "AKIDONLY"},
        {"MASSIVE_S3_ACCESS_KEY_ID": "AKIDBLANK", "MASSIVE_S3_SECRET_ACCESS_KEY": "  \t"},
    ],
    ids=["none", "one", "blank-secret"],
)
def test_check_without_drift_or_credentials(tmp_path: Path, environ: dict[str, str]) -> None:
    before = store(make(), tmp_path / "vendor")
    stored = sorted(p.read_bytes() for p in before.rglob("*") if p.is_file())
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    # No S3 fake is installed: listing the bucket here would fail the test.
    report = drift.check(
        snapshot_directory=before,
        get=served(make()).__getitem__,
        environ=environ,
        committed=COMMITTED,
        hand=NO_HAND,
        scratch=scratch,
    )
    assert report == Report(items=(), fetched=FETCHED, bucket_listed=False)
    # The vendored snapshot is only read, and the downloads' scratch directory is removed.
    assert sorted(p.read_bytes() for p in before.rglob("*") if p.is_file()) == stored
    assert list(scratch.iterdir()) == []


@pytest.mark.parametrize("which", ["openapi", "websocket"])
@pytest.mark.parametrize("document", [{"status": "ERROR"}, {"paths": {}}], ids=["error", "empty"])
def test_a_live_specification_without_paths_fails_the_check(
    tmp_path: Path, which: str, document: dict[str, Any]
) -> None:
    before = store(make(), tmp_path / "vendor")
    live = make()
    live = (
        dataclasses.replace(live, openapi=document)
        if which == "openapi"
        else dataclasses.replace(live, websocket=document)
    )
    with pytest.raises(RefreshError) as info:
        drift.check(
            snapshot_directory=before,
            get=served(live).__getitem__,
            environ={},
            committed=COMMITTED,
            hand=NO_HAND,
        )
    (problem,) = info.value.problems
    assert problem.key == f"{which}.json"
    assert "no paths" in problem.message


def test_check_reports_drift_gate_stops_and_unnamed_bucket_prefixes(
    tmp_path: Path, fake_s3: FakeS3
) -> None:
    before = store(make(), tmp_path / "vendor")
    live = make(rest={"/v1/widgets": item({"price": {"type": "string"}})})
    fake_s3.put("us_stocks_sip/widgets_v1/2025/03/2025-03-03.csv.gz", b"x")
    fake_s3.put("mystery/files_v1/2025/03/2025-03-03.csv.gz", b"x")
    report = drift.check(
        snapshot_directory=before,
        get=served(live).__getitem__,
        environ=CREDENTIALS,
        committed=COMMITTED,
        hand=NO_HAND,
    )
    assert report.bucket_listed
    assert [d.line() for d in report.items] == [
        "- bucket `mystery`: a bucket prefix no overview row, generated source, or exclusion names",
        "- REST `/v1/widgets`: field `results[].price` retyped from `number` to `string`",
    ]
    text = report.text()
    assert "AKIDDRIFT" not in text
    assert "drift-secret" not in text
    assert fake_s3.listings == [None]


def test_check_adds_the_gate_stops_of_the_live_sources(tmp_path: Path) -> None:
    before = store(make(), tmp_path / "vendor")
    live = make(rest={"/v1/widgets": item(extensions={"x-polygon-new": True})})
    report = drift.check(
        snapshot_directory=before,
        get=served(live).__getitem__,
        environ={},
        committed=COMMITTED,
        hand=NO_HAND,
    )
    assert [d.line() for d in report.items] == [
        (
            "- refresh `/v1/widgets`: the shape gate would stop on `x-polygon-new`: `unknown "
            "vendor extension`"
        )
    ]


def test_check_lets_a_bucket_failure_through(tmp_path: Path, fake_s3: FakeS3) -> None:
    before = store(make(), tmp_path / "vendor")
    fake_s3.refuse("", status=403, code="AccessDenied", message="No")
    with pytest.raises(WireEntitlementError, match=r"^HTTP 403 for s3://flatfiles/ at ") as info:
        drift.check(
            snapshot_directory=before,
            get=served(make()).__getitem__,
            environ=CREDENTIALS,
            committed=COMMITTED,
            hand=NO_HAND,
        )
    assert "drift-secret" not in str(info.value)


def test_check_lets_a_failed_download_through(tmp_path: Path) -> None:
    before = store(make(), tmp_path / "vendor")

    def offline(url: str) -> bytes:
        raise fetch.FetchError(f"GET {url} failed")

    with pytest.raises(fetch.FetchError, match=r"^GET https://api\.massive\.com/openapi failed$"):
        drift.check(
            snapshot_directory=before, get=offline, environ={}, committed=COMMITTED, hand=NO_HAND
        )


# The workflow's issue script, against the fake gh.

needs_jq = pytest.mark.skipif(
    shutil.which("jq") is None and "CI" not in os.environ,
    reason="the issue script needs jq; CI never skips",
)
REPORT = "# Massive API drift\n\n- REST `/v1/new`: documented\n"
TITLE = "Massive API drift"


def run_script(fake: FakeGh, report: str) -> subprocess.CompletedProcess[str]:
    env = fake.install()
    # The command is literal; the report travels on standard input.
    result = subprocess.run(
        ["/bin/bash", "scripts/spec-drift-issue.sh"],
        cwd=ROOT,
        input=report,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    fake.load()
    return result


@needs_jq
def test_the_script_creates_the_label_and_the_issue(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    fake.issue("Massive API drift, old", labels=["spec-drift"], state="CLOSED")
    fake.issue(TITLE, labels=["bug"])  # the title without the label is not the standing issue
    result = run_script(fake, REPORT)
    assert result.returncode == 0, result.stderr
    assert fake.labels == ["spec-drift"]
    assert fake.issues[-1] == {
        "number": 3,
        "title": TITLE,
        "state": "OPEN",
        "labels": ["spec-drift"],
        "body": REPORT.rstrip("\n"),
        "comments": [],  # opening the issue notifies; no comment is needed
    }
    assert [call[:2] for call in fake.calls] == [
        ["label", "list"],
        ["label", "create"],
        ["issue", "list"],
        ["issue", "create"],
    ]


@needs_jq
def test_the_script_keeps_a_label_that_differs_only_in_case(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["Spec-Drift"]
    result = run_script(fake, REPORT)
    assert result.returncode == 0, result.stderr
    assert fake.labels == ["Spec-Drift"]
    assert ["label", "create"] not in [call[:2] for call in fake.calls]


@needs_jq
def test_a_changed_report_updates_the_open_issue_and_comments_once(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["spec-drift"]
    old = "# Massive API drift\n\n- REST `/v1/a`: documented\n- REST `/v1/b`: documented\n"
    number = fake.issue(TITLE, labels=["spec-drift"], body=old)
    result = run_script(fake, REPORT)
    assert result.returncode == 0, result.stderr
    assert len(fake.issues) == 1
    issue = fake.issues[0]
    assert (issue["state"], issue["body"]) == ("OPEN", REPORT.rstrip("\n"))
    # GitHub notifies no one of an edited body; the comment does, and holds no vendor text.
    assert issue["comments"] == ["The drift report changed: 1 item(s), was 2."]
    listing = ["--label", "spec-drift", "--state", "all", "--limit", "1000"]
    assert fake.calls[1:] == [
        ["issue", "list", *listing, "--json", "number,title,state"],
        ["issue", "view", str(number), "--json", "body"],
        ["issue", "edit", str(number), "--body-file", fake.calls[3][-1]],
        ["issue", "comment", str(number), "--body", issue["comments"][0]],
    ]


@needs_jq
def test_an_unchanged_report_touches_nothing(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["spec-drift"]
    fake.issue(TITLE, labels=["spec-drift"], body=REPORT)
    result = run_script(fake, REPORT)
    assert result.returncode == 0, result.stderr
    assert fake.issues[0]["comments"] == []
    assert [call[:2] for call in fake.calls] == [
        ["label", "list"],
        ["issue", "list"],
        ["issue", "view"],
    ]


@needs_jq
@pytest.mark.parametrize("body", ["", REPORT], ids=["changed", "unchanged"])
def test_the_script_reopens_the_oldest_standing_issue(tmp_path: Path, body: str) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["spec-drift"]
    fake.issue(TITLE, labels=["spec-drift"], state="CLOSED", body=body)
    fake.issue(TITLE, labels=["spec-drift"])
    result = run_script(fake, REPORT)
    assert result.returncode == 0, result.stderr
    assert [(i["state"], i["body"], i["comments"]) for i in fake.issues] == [
        ("OPEN", REPORT.rstrip("\n"), []),  # reopening notifies; no comment is needed
        ("OPEN", "", []),
    ]
    assert fake.calls[-1] == ["issue", "reopen", "1"]


@needs_jq
def test_the_script_cuts_a_report_too_long_for_an_issue(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["spec-drift"]
    line = "- REST `/v1/path{}`: documented, but neither generated nor excluded\n"
    report = "".join(line.format(n) for n in range(2000))
    result = run_script(fake, report)
    assert result.returncode == 0, result.stderr
    body = fake.issues[0]["body"]
    assert len(body) <= 65536
    *kept, blank, note = body.splitlines()
    assert kept == report.splitlines()[: len(kept)]
    assert len(kept) > 800
    assert (blank, note) == (
        "",
        (
            "_The report is longer than an issue allows; run the check in a source checkout for "
            "the rest._"
        ),
    )


@needs_jq
def test_the_script_refuses_an_empty_report(tmp_path: Path) -> None:
    fake = FakeGh(tmp_path)
    result = run_script(fake, "")
    assert result.returncode == 1
    assert "empty" in result.stderr
    assert fake.calls == []


@needs_jq
@pytest.mark.parametrize(
    "command", [("label", "list"), ("issue", "list"), ("issue", "view"), ("issue", "edit")]
)
def test_the_script_stops_when_gh_fails(tmp_path: Path, command: tuple[str, ...]) -> None:
    fake = FakeGh(tmp_path)
    fake.labels = ["spec-drift"]
    fake.issue(TITLE, labels=["spec-drift"], state="CLOSED")
    fake.fail(*command)
    result = run_script(fake, REPORT)
    assert result.returncode != 0
    assert fake.calls[-1][:2] == list(command)
    assert fake.issues[0]["state"] == "CLOSED"
