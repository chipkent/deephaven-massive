"""Tests for the REST generator: the parity of `spec/rest.json`, coverage, rules, and gates.

The first group runs against the vendored snapshot. The rest build small synthetic
specifications, so each representation rule and each gate is shown in isolation; every gate
has a test that fails when the gate is removed.
"""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import exclusions, generate, lexicon, overrides
from deephaven_massive._massive.refresh import exclusions as exclusions_module
from deephaven_massive._massive.refresh.generate import (
    HandInput,
    Problem,
    RefreshError,
    Snapshot,
    column_name,
    generate_rest,
)
from deephaven_massive._massive.spec.types import (
    ENVELOPE_KEYS,
    ParamKind,
    RestPath,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

# --- The vendored snapshot -------------------------------------------------------------------


@pytest.fixture(scope="module")
def real() -> tuple[tuple[RestPath, ...], generate.Coverage]:
    """Generate from the vendored snapshot once for the module.

    Returns:
        The generated paths and the coverage.
    """
    return generate_rest(Snapshot.load())


def test_rest_json_matches_the_snapshot_byte_for_byte(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    paths, _ = real
    expected = generate.render_rest(paths, openapi_sha256=Snapshot.load().openapi_sha256)
    assert generate.SPEC_FILE.read_text(encoding="utf-8") == expected


def test_every_specified_path_is_accounted_for_exactly_once(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    _, coverage = real
    parts = [coverage.generated, coverage.excluded, coverage.deprecated | coverage.draft]
    assert sum(len(part) for part in parts) == len(coverage.documented)
    assert frozenset().union(*parts) == coverage.documented


def test_every_documented_endpoint_is_specified(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    _, coverage = real
    assert set(coverage.pages) <= coverage.documented
    assert coverage.pages  # the pages were read at all


def test_undocumented_exclusions_are_absent_from_the_documentation(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    _, coverage = real
    rest = [e for e in exclusions.EXCLUSIONS if e.kind is Transport.REST]
    assert {e.item for e in rest if e.undocumented}.isdisjoint(coverage.pages)
    assert {e.item for e in rest if not e.undocumented} <= set(coverage.pages)


def test_the_vendor_ignore_hint_does_not_exclude_a_path(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    # `x-polygon-ignore` marks these; each has its own documentation page and dataset.
    _, coverage = real
    assert {
        "/v3/trades/{optionsTicker}",
        "/v3/trades/{cryptoTicker}",
        "/v3/quotes/{optionsTicker}",
        "/v3/quotes/{fxTicker}",
        "/v2/last/trade/{optionsTicker}",
    } <= coverage.generated


def test_excluded_indicator_templates_repeat_the_stocks_template() -> None:
    # Generate the excluded indicator templates with the stocks template's hand input, then
    # compare: an exclusion that claims a repeat must still be one.
    repeats = {e.item for e in exclusions.EXCLUSIONS if e.item.startswith("/v1/indicators/")}
    assert len(repeats) == 12

    def stocks(path: str) -> str:
        return path.rsplit("/", 1)[0] + "/{stockTicker}"

    hand = HandInput()
    hand = dataclasses.replace(
        hand,
        exclusions=tuple(e for e in hand.exclusions if e.item not in repeats),
        result_keys={**hand.result_keys, **{p: hand.result_keys[stocks(p)] for p in repeats}},
        units={
            **hand.units,
            **{(p, "timestamp"): hand.units[stocks(p), "timestamp"] for p in repeats},
        },
        tag_conflicts={
            **hand.tag_conflicts,
            **{(p, "timestamp"): hand.tag_conflicts[stocks(p), "timestamp"] for p in repeats},
        },
        param_units={
            **hand.param_units,
            **{(p, "timestamp"): hand.param_units[stocks(p), "timestamp"] for p in repeats},
        },
    )
    paths = {p.key: p for p in generate_rest(Snapshot.load(), hand)[0]}

    def anonymous(path: RestPath) -> list[object]:
        return [dataclasses.replace(p, name="ticker") if p.in_path else p for p in path.params]

    for repeat in sorted(repeats):
        assert paths[repeat].fields == paths[stocks(repeat)].fields, repeat
        assert anonymous(paths[repeat]) == anonymous(paths[stocks(repeat)]), repeat
        assert paths[repeat].result_key == paths[stocks(repeat)].result_key, repeat


def test_the_generated_names_follow_the_lexicons(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    paths = {p.key: p for p in real[0]}
    snapshot = paths["/v2/snapshot/locale/us/markets/stocks/tickers"]
    columns = {f.wire_key: f.column for f in snapshot.fields}
    assert columns["prevDay.vw"] == "PrevDayVwap"
    assert columns["min.c"] == "MinuteClose"
    assert columns["lastTrade.t"] == "LastTradeSipTimestamp"
    assert columns["lastQuote.P"] == "LastQuoteAskPrice"
    assert columns["min.dav"] == "MinuteAccumulatedDecimalVolume"
    nbbo = {f.wire_key: f for f in paths["/v2/last/nbbo/{stocksTicker}"].fields}
    assert (nbbo["t"].column, nbbo["t"].unit) == ("SipTimestamp", WireUnit.NS)
    bars = paths["/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"]
    assert bars.field("WindowStart").unit is WireUnit.MS
    for path in real[0]:
        assert {"Symbol", "Underlying"}.isdisjoint(f.column for f in path.fields), path.key


def test_the_indicator_timestamps_are_milliseconds_against_their_tags(
    real: tuple[tuple[RestPath, ...], generate.Coverage],
) -> None:
    paths = {p.key: p for p in real[0]}
    sma = paths["/v1/indicators/sma/{stockTicker}"]
    assert sma.result_key == "results.values"
    assert sma.field("Timestamp").unit is WireUnit.MS
    timestamp = next(p for p in sma.params if p.name == "timestamp.gte")
    assert (timestamp.kind, timestamp.unit) == (ParamKind.TIMESTAMP, WireUnit.MS)


def test_refresh_writes_only_a_changed_file(tmp_path: Path) -> None:
    target = tmp_path / "rest.json"
    assert generate.refresh(spec_file=target) is True
    assert target.read_text(encoding="utf-8") == generate.SPEC_FILE.read_text(encoding="utf-8")
    assert generate.refresh(spec_file=target) is False


# --- Synthetic specifications ---------------------------------------------------------------

_PATH = "/v1/things/{id}"


def _row(**properties: object) -> Mapping[str, object]:
    return {"type": "object", "properties": properties}


def _item(
    row: Mapping[str, object],
    *,
    params: list[dict[str, object]] | None = None,
    result: str | None = "results",
    array: bool = True,
    extra: Mapping[str, object] | None = None,
    operation_extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    rows: Mapping[str, object] = {"type": "array", "items": row} if array else row
    body = (
        rows
        if result is None
        else {
            "type": "object",
            "properties": {"status": {"type": "string"}, result: rows},
        }
    )
    in_path = [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}]
    operation: dict[str, object] = {
        "parameters": in_path + (params or []),
        "responses": {"200": {"content": {"application/json": {"schema": body}}}},
        **(operation_extra or {}),
    }
    return {"get": operation, **(extra or {})}


def _snapshot(
    paths: Mapping[str, object],
    *,
    pages: dict[str, str] | None = None,
    schemas: Mapping[str, object] | None = None,
) -> Snapshot:
    document = {"openapi": "3.0.3", "paths": paths, "components": {"schemas": schemas or {}}}
    # A JSON round trip gives the document the type a decoded specification has.
    openapi = json.loads(json.dumps(document))
    return Snapshot(openapi=openapi, llms="", pages=pages or {}, openapi_sha256="0" * 64)


def _hand(
    *,
    lexicons: Mapping[str, lexicon.Lexicon] | None = None,
    shapes: Mapping[tuple[str, str], str] | None = None,
    identity: Mapping[str, str] | None = None,
    names: Mapping[tuple[str, str], overrides.Name] | None = None,
    units: Mapping[tuple[str, str], overrides.Unit] | None = None,
    tag_conflicts: Mapping[tuple[str, str], str] | None = None,
    type_fixes: Mapping[tuple[str, str], overrides.TypeFix] | None = None,
    result_keys: Mapping[str, overrides.ResultKey] | None = None,
    param_units: Mapping[tuple[str, str], overrides.Unit] | None = None,
    exclusions: Sequence[exclusions_module.Exclusion] = (),
) -> HandInput:
    """Return hand input with every table empty except those given."""
    return HandInput(
        lexicons=lexicons or {},
        shapes=shapes or {},
        identity=identity or {},
        names=names or {},
        units=units or {},
        tag_conflicts=tag_conflicts or {},
        type_fixes=type_fixes or {},
        result_keys=result_keys or {},
        param_units=param_units or {},
        exclusions=exclusions,
    )


def _one(
    row: Mapping[str, object],
    hand: HandInput | None = None,
    *,
    params: list[dict[str, object]] | None = None,
    result: str | None = "results",
    array: bool = True,
) -> RestPath:
    item = _item(row, params=params, result=result, array=array)
    paths, _ = generate_rest(_snapshot({_PATH: item}), hand or _hand())
    (path,) = paths
    return path


def _problems(snapshot: Snapshot, hand: HandInput | None = None) -> list[tuple[str, str, str]]:
    with pytest.raises(RefreshError) as caught:
        generate_rest(snapshot, hand or _hand())
    return [(p.gate, p.source, p.key) for p in caught.value.problems]


def _types(path: RestPath) -> dict[str, tuple[WireType, WireUnit | None]]:
    return {f.wire_key: (f.wire_type, f.unit) for f in path.fields}


_MS = overrides.Unit(unit=WireUnit.MS, reason="test")


# The representation rules.


def test_representation_rules() -> None:
    path = _one(
        _row(
            count={"type": "integer"},
            whole={"type": "number", "format": "integer"},
            price={"type": "number"},
            flag={"type": "boolean"},
            name={"type": "string"},
            created={"type": "string", "format": "date-time"},
            listed_utc={"type": "string"},
            listed={"type": "string", "format": "date"},
            date={"type": "string"},
            ex_date={"type": "string"},
            codes={"type": "array", "items": {"type": "integer"}},
            tags={"type": "array", "items": {"type": "string"}},
            extra={"type": "object"},
            either={"oneOf": [{"type": "string"}, {"type": "integer"}]},
            anything={"anyOf": [{"type": "string"}, {"type": "number"}]},
        )
    )
    assert _types(path) == {
        "count": (WireType.LONG, None),
        "whole": (WireType.LONG, None),
        "price": (WireType.DOUBLE, None),
        "flag": (WireType.BOOL, None),
        "name": (WireType.STRING, None),
        "created": (WireType.ISO_INSTANT, None),
        "listed_utc": (WireType.ISO_INSTANT, None),
        "listed": (WireType.DATE, None),
        "date": (WireType.DATE, None),
        "ex_date": (WireType.DATE, None),
        "codes": (WireType.INT_ARRAY, None),
        "tags": (WireType.JSON_TEXT, None),
        "extra": (WireType.JSON_TEXT, None),
        "either": (WireType.JSON_TEXT, None),
        "anything": (WireType.JSON_TEXT, None),
    }


def test_a_utc_name_with_another_format_follows_the_format() -> None:
    path = _one(_row(day_utc={"type": "string", "format": "date"}))
    assert _types(path) == {"day_utc": (WireType.DATE, None)}


def test_a_units_entry_makes_an_integer_an_epoch() -> None:
    path = _one(_row(t={"type": "integer"}), _hand(units={(_PATH, "t"): _MS}, identity={"t": ""}))
    assert _types(path) == {"t": (WireType.EPOCH, WireUnit.MS)}


def test_a_not_a_timestamp_units_entry_keeps_a_long() -> None:
    unit = overrides.Unit(unit=None, reason="a count")
    path = _one(_row(updated={"type": "integer"}), _hand(units={(_PATH, "updated"): unit}))
    assert _types(path) == {"updated": (WireType.LONG, None)}


def test_nested_objects_flatten_in_order_with_refs_and_all_of() -> None:
    schemas = {
        "Bar": {"type": "object", "properties": {"close": {"type": "number"}}},
        "Base": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    }
    row = {
        "allOf": [
            {"$ref": "#/components/schemas/Base"},
            {
                "type": "object",
                "properties": {
                    "day": {"$ref": "#/components/schemas/Bar"},
                    "close": {"type": "number"},
                },
                "required": ["day"],
            },
        ]
    }
    paths, _ = generate_rest(
        _snapshot({_PATH: _item(row)}, schemas=schemas), _hand(identity={"day": ""})
    )
    assert [(f.wire_key, f.column, f.required) for f in paths[0].fields] == [
        ("ticker", "Ticker", True),
        ("day.close", "DayClose", False),
        ("close", "Close", False),
    ]


def test_a_one_branch_choice_is_that_branch() -> None:
    row = {
        "oneOf": [
            _row(event_type={"type": "string"}, ticker_change=_row(ticker={"type": "string"}))
        ]
    }
    path = _one(row)
    assert [f.column for f in path.fields] == ["EventType", "TickerChangeTicker"]


def test_nested_required_needs_every_enclosing_object_required() -> None:
    inner = {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]}
    row = {
        "type": "object",
        "properties": {"kept": inner, "loose": inner},
        "required": ["kept"],
    }
    path = _one(row, _hand(identity={"a": ""}))
    assert [(f.wire_key, f.required) for f in path.fields] == [
        ("kept.a", True),
        ("loose.a", False),
    ]


# Result keys.


def test_a_named_result_key() -> None:
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key="last", reason="test")})
    path = _one(_row(price={"type": "number"}), hand, result="last", array=False)
    assert (path.result_key, [f.column for f in path.fields]) == ("last", ["Price"])


def test_a_dotted_result_key() -> None:
    values = {"type": "array", "items": _row(value={"type": "number"})}
    row = _row(underlying=_row(url={"type": "string"}), values=values)
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key="results.values", reason="test")})
    path = _one(row, hand, array=False)
    assert (path.result_key, [f.column for f in path.fields]) == ("results.values", ["Value"])


def test_an_object_body_drops_the_envelope_keys() -> None:
    row = _row(
        status={"type": "string"},
        request_id={"type": "string"},
        next_url={"type": "string"},
        count={"type": "integer"},
        close={"type": "number"},
    )
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key=None, reason="test")})
    path = _one(row, hand, result=None, array=False)
    assert path.result_key is None
    assert [f.wire_key for f in path.fields] == ["close"]
    assert {"status", "request_id", "next_url", "count"} == ENVELOPE_KEYS


def test_a_list_body_keeps_its_row_keys() -> None:
    row = _row(status={"type": "string"}, date={"type": "string"})
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key=None, reason="test")})
    path = _one(row, hand, result=None, array=True)
    assert [f.wire_key for f in path.fields] == ["status", "date"]


def test_gate_envelope_a_mistyped_envelope_key_is_reported() -> None:
    row = _row(status={"type": "integer"}, close={"type": "number"})
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key=None, reason="test")})
    snapshot = _snapshot({_PATH: _item(row, result=None, array=False)})
    assert _problems(snapshot, hand) == [("envelope", _PATH, "status")]


def test_a_missing_default_result_key_is_reported() -> None:
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), result="last")})
    assert _problems(snapshot) == [("shape", _PATH, "")]


def test_gate_g4_a_result_key_the_response_lacks() -> None:
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key="results.values", reason="test")})
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, hand) == [("G4", _PATH, "")]


def test_rows_that_are_not_objects_are_reported() -> None:
    snapshot = _snapshot({_PATH: _item({"type": "string"})})
    assert _problems(snapshot) == [("shape", _PATH, "")]


# Naming.


@pytest.mark.parametrize(
    ("long_names", "column"),
    [
        (["prevDay", "vwap"], "PrevDayVwap"),
        (["sip_mapping", "CTA"], "SipMappingCta"),
        (["aff_10b5_one"], "Aff10b5One"),
        (["isUTC"], "IsUtc"),
        (["todaysChangePerc"], "TodaysChangePerc"),
        (["obfr_25th_percentile"], "Obfr25thPercentile"),
        (["s_and_p"], "SAndP"),
    ],
)
def test_column_names(long_names: list[str], column: str) -> None:
    assert column_name(long_names) == column


def test_lexicons_identity_and_names_translate_short_keys() -> None:
    lexicons = {
        "bar": lexicon.Lexicon(name="bar", reason="test", entries={"c": "close", "min": "minute"}),
        "trade": lexicon.Lexicon(name="trade", reason="test", entries={"p": "price"}),
    }
    row = _row(
        c={"type": "number"},
        min=_row(c={"type": "number"}),
        last=_row(p={"type": "number"}, av={"type": "integer"}),
        id={"type": "string"},
    )
    hand = _hand(
        lexicons=lexicons,
        shapes={(_PATH, ""): "bar", (_PATH, "min"): "bar", (_PATH, "last"): "trade"},
        identity={"id": "a word"},
        names={(_PATH, "last.av"): overrides.Name(long_name="accumulated_volume", reason="test")},
    )
    path = _one(row, hand)
    assert [(f.wire_key, f.column) for f in path.fields] == [
        ("c", "Close"),
        ("min.c", "MinuteClose"),
        ("last.p", "LastPrice"),
        ("last.av", "LastAccumulatedVolume"),
        ("id", "Id"),
    ]


def test_gate_g1_a_short_key_without_a_name() -> None:
    snapshot = _snapshot({_PATH: _item(_row(p={"type": "number"}, price={"type": "number"}))})
    assert _problems(snapshot) == [("G1", _PATH, "p")]


def test_gate_g1_a_short_object_key_without_a_name() -> None:
    snapshot = _snapshot({_PATH: _item(_row(min=_row(close={"type": "number"})))})
    assert _problems(snapshot) == [("G1", _PATH, "min.close")]


def test_gate_g1_prints_the_vendor_description() -> None:
    snapshot = _snapshot({_PATH: _item(_row(p={"type": "number", "description": "The  price."}))})
    with pytest.raises(RefreshError) as caught:
        generate_rest(snapshot, _hand())
    assert str(caught.value).splitlines() == [
        "the refresh stopped on 1 problem(s):",
        f"G1 {_PATH} p: short key 'p' has no lexicon, identity, or NAMES entry",
        "    description: The price.",
    ]


def test_gate_g2_two_keys_with_one_column() -> None:
    snapshot = _snapshot(
        {_PATH: _item(_row(lastPrice={"type": "number"}, last_price={"type": "number"}))}
    )
    assert _problems(snapshot) == [("G2", _PATH, "last_price")]


@pytest.mark.parametrize("key", ["symbol", "underlying"])
def test_gate_g2_a_reserved_column(key: str) -> None:
    snapshot = _snapshot({_PATH: _item(_row(**{key: {"type": "string"}}))})
    assert _problems(snapshot) == [("G2", _PATH, key)]


def test_a_names_entry_renames_a_reserved_column() -> None:
    hand = _hand(names={(_PATH, "symbol"): overrides.Name(long_name="ticker", reason="test")})
    assert [f.column for f in _one(_row(symbol={"type": "string"}), hand).fields] == ["Ticker"]


# Gate G3 and units.


@pytest.mark.parametrize(
    "key",
    [
        "timestamp",
        "sip_timestamp",
        "updated",
        "last_updated",
        "window_start",
        "window_end",
        "open_time",
    ],
)
def test_gate_g3_a_timestamp_like_integer_without_units(key: str) -> None:
    snapshot = _snapshot({_PATH: _item(_row(**{key: {"type": "integer"}}))})
    assert _problems(snapshot) == [("G3", _PATH, key)]


def test_gate_g3_a_unit_tag_makes_any_integer_timestamp_like() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "INanoseconds"}}
    snapshot = _snapshot({_PATH: _item(_row(when=tagged))})
    assert _problems(snapshot) == [("G3", _PATH, "when")]


def test_gate_g3_runs_on_the_long_name() -> None:
    lexicons = {
        "trade": lexicon.Lexicon(name="trade", reason="test", entries={"t": "sip_timestamp"})
    }
    snapshot = _snapshot({_PATH: _item(_row(t={"type": "integer"}))})
    assert _problems(snapshot, _hand(lexicons=lexicons, shapes={(_PATH, ""): "trade"})) == [
        ("G3", _PATH, "t")
    ]


def test_an_ordinary_integer_needs_no_units() -> None:
    assert _types(_one(_row(volume={"type": "integer"}))) == {"volume": (WireType.LONG, None)}


def test_gate_units_a_unit_that_contradicts_its_tag() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "IMicroseconds"}}
    snapshot = _snapshot({_PATH: _item(_row(timestamp=tagged))})
    hand = _hand(units={(_PATH, "timestamp"): _MS})
    assert _problems(snapshot, hand) == [("units", _PATH, "timestamp")]
    resolved = dataclasses.replace(hand, tag_conflicts={(_PATH, "timestamp"): "test"})
    paths, _ = generate_rest(snapshot, resolved)
    assert _types(paths[0]) == {"timestamp": (WireType.EPOCH, WireUnit.MS)}


def test_gate_g4_a_tag_conflict_that_is_no_conflict() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "IMilliseconds"}}
    snapshot = _snapshot({_PATH: _item(_row(timestamp=tagged))})
    hand = _hand(units={(_PATH, "timestamp"): _MS}, tag_conflicts={(_PATH, "timestamp"): "test"})
    assert _problems(snapshot, hand) == [("G4", _PATH, "timestamp")]


def test_gate_g4_units_on_a_field_that_is_not_an_integer() -> None:
    snapshot = _snapshot({_PATH: _item(_row(timestamp={"type": "string"}))})
    assert _problems(snapshot, _hand(units={(_PATH, "timestamp"): _MS})) == [
        ("G4", _PATH, "timestamp")
    ]


# Declared conversions.


def test_a_type_fix_records_the_declared_type() -> None:
    fix = overrides.TypeFix(declared=WireType.STRING, wire_type=WireType.LONG, reason="test")
    path = _one(_row(size={"type": "string"}), _hand(type_fixes={(_PATH, "size"): fix}))
    (field,) = path.fields
    assert (field.wire_type, field.declared_type) == (WireType.LONG, WireType.STRING)


def test_gate_g4_a_type_fix_the_specification_no_longer_needs() -> None:
    fix = overrides.TypeFix(declared=WireType.STRING, wire_type=WireType.LONG, reason="test")
    snapshot = _snapshot({_PATH: _item(_row(size={"type": "integer"}))})
    assert _problems(snapshot, _hand(type_fixes={(_PATH, "size"): fix})) == [("G4", _PATH, "size")]


# Parameters.


def _param(name: str, schema: dict[str, object], *, where: str = "query") -> dict[str, object]:
    return {"name": name, "in": where, "schema": schema}


def test_parameter_rules() -> None:
    params = [
        _param("order", {"type": "string", "enum": ["asc", "desc"]}),
        _param("precision", {"type": "integer", "enum": [0, 1]}),
        _param("ticker.any_of", {"type": "string"}),
        _param("type.any_of", {"type": "string", "enum": ["a", "b"]}),
        _param("tickers", {"type": "array", "items": {"type": "string"}}),
        _param("adjusted", {"type": "boolean"}),
        _param("limit", {"type": "integer"}),
        _param("amount", {"type": "number"}),
        _param("on", {"type": "string", "format": "date"}),
        _param("at", {"type": "string", "format": "date-time"}),
        _param("published_utc.gte", {"oneOf": [{"format": "date-time"}, {"format": "date"}]}),
        _param("date", {"type": "string"}),
        _param("ex_dividend_date.lt", {"type": "string"}),
        _param("as_of", {"type": "string"}),
        _param("cusip", {"type": "string"}),
    ]
    path = _one(_row(price={"type": "number"}), params=params)
    assert [(p.name, p.kind, p.values, p.in_path) for p in path.params] == [
        ("id", ParamKind.STRING, (), True),
        ("order", ParamKind.ENUM, ("asc", "desc"), False),
        ("precision", ParamKind.ENUM, ("0", "1"), False),
        ("ticker.any_of", ParamKind.LIST, (), False),
        ("type.any_of", ParamKind.LIST, (), False),
        ("tickers", ParamKind.LIST, (), False),
        ("adjusted", ParamKind.BOOLEAN, (), False),
        ("limit", ParamKind.INTEGER, (), False),
        ("amount", ParamKind.NUMBER, (), False),
        ("on", ParamKind.DATE, (), False),
        ("at", ParamKind.DATE, (), False),
        ("published_utc.gte", ParamKind.DATE, (), False),
        ("date", ParamKind.DATE, (), False),
        ("ex_dividend_date.lt", ParamKind.DATE, (), False),
        ("as_of", ParamKind.DATE, (), False),
        ("cusip", ParamKind.STRING, (), False),
    ]


def test_path_item_parameters_come_first() -> None:
    item = _item(_row(price={"type": "number"}))
    item["parameters"] = [_param("shared", {"type": "string"})]
    paths, _ = generate_rest(_snapshot({_PATH: item}), _hand())
    assert [p.name for p in paths[0].params] == ["shared", "id"]


def test_param_units_make_a_timestamp_or_a_date() -> None:
    params = [
        _param("timestamp", {"type": "string"}),
        _param("timestamp.gte", {"type": "string"}),
        _param("last_updated", {"type": "string"}),
    ]
    hand = _hand(
        param_units={
            (_PATH, "timestamp"): overrides.Unit(unit=WireUnit.NS, reason="test"),
            (_PATH, "last_updated"): overrides.Unit(unit=None, reason="test"),
        }
    )
    path = _one(_row(price={"type": "number"}), hand, params=params)
    assert [(p.name, p.kind, p.unit) for p in path.params[1:]] == [
        ("timestamp", ParamKind.TIMESTAMP, WireUnit.NS),
        ("timestamp.gte", ParamKind.TIMESTAMP, WireUnit.NS),
        ("last_updated", ParamKind.DATE, None),
    ]


def test_gate_g3_a_timestamp_like_parameter_without_units_is_reported_once() -> None:
    params = [
        _param("window_start", {"type": "string"}),
        _param("window_start.lt", {"type": "string"}),
    ]
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot) == [("G3", _PATH, "window_start")]


def test_a_timestamp_like_integer_parameter_needs_no_units() -> None:
    params = [_param("timestamp", {"type": "integer"})]
    path = _one(_row(price={"type": "number"}), params=params)
    assert path.params[1].kind is ParamKind.INTEGER


def test_gate_g4_param_units_for_a_parameter_that_is_not_a_timestamp_string() -> None:
    params = [_param("timestamp", {"type": "integer"})]
    hand = _hand(param_units={(_PATH, "timestamp"): overrides.Unit(unit=WireUnit.NS, reason="t")})
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot, hand) == [("G4", _PATH, "timestamp")]


def test_a_parameter_outside_the_path_and_query_is_reported() -> None:
    params = [_param("X-Key", {"type": "string"}, where="header")]
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot) == [("shape", _PATH, "X-Key")]


def test_a_path_whose_parameters_break_its_template_is_reported() -> None:
    item = _item(_row(price={"type": "number"}))
    snapshot = _snapshot({"/v1/things/{other}": item})
    assert _problems(snapshot) == [("shape", "/v1/things/{other}", "")]


# Gate G4 for every other table.


def test_gate_g4_hand_entries_naming_nothing() -> None:
    lexicons = {"bar": lexicon.Lexicon(name="bar", reason="test", entries={})}
    hand = _hand(
        lexicons=lexicons,
        shapes={(_PATH, "gone"): "bar", ("/gone", ""): "bar"},
        names={(_PATH, "gone"): overrides.Name(long_name="x", reason="test")},
        units={("/gone", "t"): _MS},
        tag_conflicts={(_PATH, "gone"): "test"},
        type_fixes={
            (_PATH, "gone"): overrides.TypeFix(
                declared=WireType.LONG, wire_type=WireType.DOUBLE, reason="test"
            )
        },
        result_keys={"/gone": overrides.ResultKey(key=None, reason="test")},
        param_units={(_PATH, "gone"): _MS},
    )
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert sorted(_problems(snapshot, hand)) == sorted(
        [
            ("G4", _PATH, "gone"),  # shape
            ("G4", "/gone", ""),  # shape of a path not generated
            ("G4", _PATH, "gone"),  # names
            ("G4", "/gone", "t"),  # units
            ("G4", _PATH, "gone"),  # tag conflicts
            ("G4", _PATH, "gone"),  # type fixes
            ("G4", "/gone", ""),  # result keys
            ("G4", _PATH, "gone"),  # param units
        ]
    )


# Deprecated, draft, and excluded paths; the documentation.


def test_paths_are_classified() -> None:
    row = _row(price={"type": "number"})
    snapshot = _snapshot(
        {
            "/a/{id}": _item(row),
            "/b/{id}": _item(row, extra={"x-polygon-draft": True}),
            "/c/{id}": _item(row),
            "/d/{id}": _item(row, extra={"x-polygon-ignore": True}),
            "/e/{id}": _item(row, operation_extra={"x-polygon-deprecation": {"date": 0}}),
            "/f/{id}": _item(row, operation_extra={"deprecated": True}),
        },
        pages={"c.md": "**Endpoint:** `GET /c/{id}`\n"},
    )
    hand = _hand(
        exclusions=(exclusions.Exclusion(kind=Transport.REST, item="/c/{id}", reason="t"),)
    )
    paths, coverage = generate_rest(snapshot, hand)
    assert [p.key for p in paths] == ["/a/{id}", "/d/{id}"]
    assert coverage.generated == {"/a/{id}", "/d/{id}"}
    assert coverage.excluded == {"/c/{id}"}
    assert coverage.draft == {"/b/{id}"}
    assert coverage.deprecated == {"/e/{id}", "/f/{id}"}
    assert coverage.documented == {"/a/{id}", "/b/{id}", "/c/{id}", "/d/{id}", "/e/{id}", "/f/{id}"}
    assert coverage.pages == {"/c/{id}": "c.md"}


def test_exclusions_of_other_transports_are_left_to_their_generators() -> None:
    other = exclusions.Exclusion(kind=Transport.WS, item="stocks/LV", reason="test")
    path = _one(_row(price={"type": "number"}), _hand(exclusions=(other,)))
    assert path.key == _PATH


@pytest.mark.parametrize(
    ("extra", "operation_extra"),
    [(None, {"x-polygon-deprecation": {}}), ({"x-polygon-draft": True}, None)],
    ids=["deprecated", "draft"],
)
def test_gate_g4_an_exclusion_the_specification_already_accounts_for(
    extra: dict[str, object] | None, operation_extra: dict[str, object] | None
) -> None:
    item = _item(_row(price={"type": "number"}), extra=extra, operation_extra=operation_extra)
    hand = _hand(
        exclusions=(
            exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t", undocumented=True),
        )
    )
    assert _problems(_snapshot({_PATH: item}), hand) == [("G4", _PATH, "")]


def test_gate_g4_an_exclusion_the_specification_lacks() -> None:
    hand = _hand(exclusions=(exclusions.Exclusion(kind=Transport.REST, item="/gone", reason="t"),))
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, hand) == [("G4", "/gone", "")]


def test_gate_docs_an_undocumented_exclusion_that_is_documented() -> None:
    entry = exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t", undocumented=True)
    snapshot = _snapshot(
        {_PATH: _item(_row(price={"type": "number"}))},
        pages={"thing.md": f"**Endpoint:** `GET {_PATH}`\n"},
    )
    assert _problems(snapshot, _hand(exclusions=(entry,))) == [("docs", _PATH, "")]


def test_gate_docs_a_documented_exclusion_that_no_page_names() -> None:
    entry = exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t")
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, _hand(exclusions=(entry,))) == [("docs", _PATH, "")]


def test_gate_docs_a_documented_path_the_specification_lacks() -> None:
    snapshot = _snapshot(
        {_PATH: _item(_row(price={"type": "number"}))},
        pages={"new.md": "**Endpoint:** `GET /v9/new`\n"},
    )
    assert _problems(snapshot) == [("docs", "/v9/new", "")]


def test_gate_docs_a_page_with_two_endpoints() -> None:
    page = "**Endpoint:** `GET /a`\n**Endpoint:** `GET /b`\n"
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))}, pages={"two.md": page})
    assert _problems(snapshot) == [("docs", "", "two.md")]


# Specifications the generator cannot read.


@pytest.mark.parametrize(
    ("item", "schemas"),
    [
        ({"post": {}}, None),  # no GET
        ({"get": {"responses": {"404": {}}}}, None),  # no 200
        ({"get": {"responses": {"200": {"content": {"text/csv": {}}}}}}, None),  # no JSON
        (_item({"$ref": "https://example.com/x"}), None),  # a reference elsewhere
        (_item({"$ref": "#/components/schemas/Missing"}), None),  # a reference to nothing
        (
            _item({"$ref": "#/components/schemas/Loop"}),
            {"Loop": {"$ref": "#/components/schemas/Loop"}},
        ),
        (
            _item({"allOf": [{"$ref": "#/components/schemas/Deep"}]}),
            {"Deep": {"allOf": [{"$ref": "#/components/schemas/Deep"}]}},
        ),
        (_item(_row(oddity={"type": "null"})), None),  # an unsupported field type
        (_item(_row(oddity="not a schema")), None),  # a schema that is not an object
    ],
)
def test_unreadable_specifications_are_reported(
    item: dict[str, object], schemas: dict[str, object] | None
) -> None:
    problems = _problems(_snapshot({"/v1/things": item}, schemas=schemas))
    assert {gate for gate, _, _ in problems} == {"shape"}


def test_problem_text_without_a_description() -> None:
    problem = Problem(gate="G4", source="", key="", message="stale")
    assert problem.text() == "G4: stale"


# Loading a snapshot.


def _write_snapshot(directory: Path, *, llms: str, pages: dict[str, str]) -> None:
    directory.mkdir()
    manifest = {"files": {"openapi.json": {"stored_sha256": "ab" * 32}}}
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (directory / "openapi.json").write_text(json.dumps({"paths": {}}), encoding="utf-8")
    (directory / "llms.txt").write_text(llms, encoding="utf-8")
    for name, text in pages.items():
        (directory / "rest" / name).parent.mkdir(parents=True, exist_ok=True)
        (directory / "rest" / name).write_text(text, encoding="utf-8")


_INDEX = "## REST API\n- [Trades](https://massive.com/docs/rest/stocks/trades.md): Trades\n"


def test_a_snapshot_loads(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    snapshot = Snapshot.load(tmp_path / "s")
    assert snapshot.pages == {"stocks/trades.md": "page"}
    assert snapshot.openapi == {"paths": {}}
    assert snapshot.openapi_sha256 == "ab" * 32


def test_gate_docs_a_linked_page_missing_from_the_snapshot(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key) for p in caught.value.problems] == [("docs", "stocks/trades.md")]


def test_a_snapshot_that_is_not_json_objects_is_reported(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / "openapi.json").write_text("[]", encoding="utf-8")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [p.gate for p in caught.value.problems] == ["shape"]


def test_the_default_hand_input_is_the_real_tables() -> None:
    hand = HandInput()
    assert hand.lexicons is lexicon.LEXICONS
    assert hand.shapes is lexicon.SHAPES
    assert hand.identity is lexicon.IDENTITY
    assert hand.names is overrides.NAMES
    assert hand.units is overrides.UNITS
    assert hand.tag_conflicts is overrides.TAG_CONFLICTS
    assert hand.type_fixes is overrides.TYPE_FIXES
    assert hand.result_keys is overrides.RESULT_KEYS
    assert hand.param_units is overrides.PARAM_UNITS
    assert hand.exclusions is exclusions.EXCLUSIONS


def test_render_writes_one_line_per_field_and_parameter() -> None:
    path = _one(_row(price={"type": "number"}))
    text = generate.render_rest([path, path], openapi_sha256="f" * 64)
    assert json.loads(text)["sources"][1]["fields"] == [
        {
            "column": "Price",
            "declared_type": None,
            "required": False,
            "type": "double",
            "unit": None,
            "wire_key": "price",
        }
    ]
    assert '    {"column": "Price", "declared_type": null, ' in text
    assert text.endswith("}\n")
