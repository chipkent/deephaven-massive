"""Tests for the REST generator: coverage, representation, naming, parameters, and its gates.

The first group runs against the vendored snapshot, and skips where there is none (an sdist
leaves it out). The rest build small synthetic specifications, so each rule and each gate the
generator applies is shown in isolation; every gate has a test that fails when the gate is
removed. A problem is looked for through the whole refresh (`generate.generate`), since the
checks that span sources (stale hand entries among them) run there, after every generator.
The field rules every transport shares are those of `fields`, shown here on REST paths. The
parity of the committed `spec/rest.json` is tested with `generate`.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
from datetime import date
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import (
    exclusions,
    generate,
    lexicon,
    overrides,
    rest_sources,
)
from deephaven_massive._massive.refresh import exclusions as exclusions_module
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.rest_sources import Coverage, generate_rest
from deephaven_massive._massive.refresh.snapshot import DIRECTORY, Snapshot
from deephaven_massive._massive.spec.convert import convert_field
from deephaven_massive._massive.spec.types import (
    ENVELOPE_KEYS,
    Conversion,
    ParamKind,
    RestPath,
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
def real() -> tuple[tuple[RestPath, ...], Coverage]:
    """Generate from the vendored snapshot once for the module.

    Returns:
        The generated paths and the coverage.
    """
    return generate_rest(Snapshot.load())


@_requires_snapshot
def test_every_specified_path_is_accounted_for_exactly_once(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    _, coverage = real
    parts = [coverage.generated, coverage.excluded, coverage.deprecated | coverage.draft]
    assert sum(len(part) for part in parts) == len(coverage.specified)
    assert frozenset().union(*parts) == coverage.specified


@_requires_snapshot
def test_every_documented_endpoint_is_specified(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    _, coverage = real
    assert set(coverage.pages) <= coverage.specified
    assert coverage.pages  # the pages were read at all


@_requires_snapshot
def test_undocumented_exclusions_are_absent_from_the_documentation(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    _, coverage = real
    rest = [e for e in exclusions.EXCLUSIONS if e.kind is Transport.REST]
    assert {e.item for e in rest if e.undocumented}.isdisjoint(coverage.pages)
    assert {e.item for e in rest if not e.undocumented} <= set(coverage.pages)


@_requires_snapshot
def test_the_vendor_ignore_hint_does_not_exclude_a_path(
    real: tuple[tuple[RestPath, ...], Coverage],
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


@_requires_snapshot
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


@_requires_snapshot
def test_the_generated_names_follow_the_lexicons(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    paths = {p.key: p for p in real[0]}
    snapshot = paths["/v2/snapshot/locale/us/markets/stocks/tickers"]
    columns = {f.wire_key: f.column for f in snapshot.fields}
    assert columns["prevDay.vw"] == "PrevDayVwap"
    assert columns["min.c"] == "LastMinuteClose"
    assert columns["lastTrade.t"] == "LastTradeSipTimestamp"
    assert columns["lastQuote.P"] == "LastQuoteAskPrice"
    assert columns["min.av"] == "LastMinuteAccumulatedVolume"
    assert columns["min.dav"] == "LastMinuteAccumulatedDecimalVolume"
    forex = {
        f.wire_key: f.column
        for f in paths["/v2/snapshot/locale/global/markets/forex/tickers"].fields
    }
    assert forex["lastQuote.a"] == "LastQuoteAskPrice"
    assert forex["lastQuote.t"] == "LastQuoteParticipantTimestamp"
    crypto = {
        f.wire_key: f.column
        for f in paths["/v2/snapshot/locale/global/markets/crypto/tickers"].fields
    }
    assert crypto["lastTrade.t"] == "LastTradeParticipantTimestamp"
    nbbo = {f.wire_key: f for f in paths["/v2/last/nbbo/{stocksTicker}"].fields}
    assert (nbbo["t"].column, nbbo["t"].unit) == ("SipTimestamp", WireUnit.NS)
    bars = paths["/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"]
    assert bars.field("WindowStart").unit is WireUnit.MS
    # A vendor field literally named `symbol` keeps its name; the dataset key rename uses it.
    open_close = paths["/v1/open-close/{stocksTicker}/{date}"]
    assert open_close.field("Symbol").wire_key == "symbol"


@_requires_snapshot
def test_the_indicator_timestamps_are_milliseconds_against_their_tags(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    paths = {p.key: p for p in real[0]}
    sma = paths["/v1/indicators/sma/{stockTicker}"]
    assert sma.result_key == "results.values"
    assert sma.field("Timestamp").unit is WireUnit.MS
    timestamp = next(p for p in sma.params if p.name == "timestamp.gte")
    assert (timestamp.kind, timestamp.unit) == (ParamKind.TIMESTAMP, WireUnit.MS)


@_requires_snapshot
def test_the_type_fixes_follow_their_samples(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    # Each REST TYPE_FIXES entry, checked against the documentation sample it cites: the
    # reason quotes a value the path's page shows for the field, and it decodes. (A REST
    # source key is a path, which begins with a slash.)
    paths = {p.key: p for p in real[0]}
    pages = Snapshot.load().pages
    for (path, wire_key), fix in overrides.TYPE_FIXES.items():
        if not path.startswith("/"):
            continue
        typed = next(f for f in paths[path].fields if f.wire_key == wire_key)
        assert (typed.wire_type, typed.conversion) == (fix.conversion.wire_type, fix.conversion)
        shown_on = [pages[name] for name in coverage_pages(real[1], path)]
        key = wire_key.rsplit(".", 1)[-1]
        cited = re.findall(r'"[^"]+"|\b\d{6,}\b', fix.reason)
        shown = [s for s in cited if any(f'"{key}": {s}' in page for page in shown_on)]
        assert shown, (path, wire_key)
        # The sample must decode through the conversion the decoder applies.
        assert convert_field(typed, json.loads(shown[0])) is not None


@_requires_snapshot
@pytest.mark.parametrize(
    "case",
    [
        (
            "/futures/v1/snapshot",
            "details.settlement_date",
            (WireType.DATE, None),
            1_753_851_600_000_000_000,
            date(2025, 7, 30),  # midnight in Chicago
        ),
        (
            "/v1/open-close/{indicesTicker}/{date}",
            "from",
            (WireType.DATE, None),
            "2023-03-10T00:00:00.000Z",
            date(2023, 3, 10),
        ),
        (
            "/v1/open-close/crypto/{from}/{to}/{date}",
            "day",
            (WireType.DATE, None),
            "2020-10-09T00:00:00.000Z",
            date(2020, 10, 9),
        ),
        (
            "/v2/snapshot/locale/global/markets/crypto/tickers",
            "lastTrade.i",
            (WireType.STRING, None),
            413131,
            "413131",
        ),
        ("/tmx/v1/corporate-events", "tmx_company_id", (WireType.STRING, None), "2208", "2208"),
        ("/stocks/taxonomies/vX/risk-factors", "taxonomy", (WireType.STRING, None), "1.0", "1.0"),
    ],
    ids=lambda case: f"{case[0]} {case[1]}",
)
def test_the_documented_samples_decode(
    real: tuple[tuple[RestPath, ...], Coverage],
    case: tuple[str, str, tuple[WireType, WireUnit | None], object, object],
) -> None:
    # Each sample the vendor's types contradicted decodes through its generated field.
    path, wire_key, (wire_type, unit), sample, expected = case
    typed = next(f for f in {p.key: p for p in real[0]}[path].fields if f.wire_key == wire_key)
    assert (typed.wire_type, typed.unit) == (wire_type, unit)
    assert convert_field(typed, sample) == expected


@_requires_snapshot
def test_gate_example_type_finds_the_samples_the_vendor_types_contradict() -> None:
    # Without their hand input, the vendor's own examples stop the refresh.
    fixed = {
        ("/futures/v1/snapshot", "details.settlement_date"),
        ("/v1/open-close/{indicesTicker}/{date}", "from"),
        ("/v1/open-close/crypto/{from}/{to}/{date}", "day"),
        ("/v2/snapshot/locale/global/markets/crypto/tickers", "lastTrade.i"),
        ("/tmx/v1/corporate-events", "tmx_company_id"),
        ("/stocks/taxonomies/vX/risk-factors", "taxonomy"),
    }
    # The movers snapshot shares the trade-ID decision but documents only text.
    movers = ("/v2/snapshot/locale/global/markets/crypto/{direction}", "lastTrade.i")
    real_hand = HandInput()
    hand = dataclasses.replace(
        real_hand,
        type_fixes={k: v for k, v in real_hand.type_fixes.items() if k not in {*fixed, movers}},
        units={k: v for k, v in real_hand.units.items() if k not in fixed},
        sample_errors=tuple(e for e in real_hand.sample_errors if not e.path.startswith("/")),
    )
    assert sorted(_problems(Snapshot.load(), hand)) == sorted(
        [
            *(("example-type", path, wire_key) for path, wire_key in fixed),
            ("example-type", "/vX/reference/ipos", "us_code"),  # a wrong sample
            ("example-type", "/futures/v1/products", "last_updated"),  # a wrong sample
        ]
    )


@_requires_snapshot
def test_gate_stale_entry_a_shared_type_fix_is_needed_while_any_member_needs_it() -> None:
    # Without the full-market snapshot's share of it (whose integer sample a SAMPLE_ERRORS entry
    # now excuses instead), the trade-ID fix the movers snapshot shares is needed by no sample,
    # so it is stale.
    tickers = ("/v2/snapshot/locale/global/markets/crypto/tickers", "lastTrade.i")
    movers = ("/v2/snapshot/locale/global/markets/crypto/{direction}", "lastTrade.i")
    real_hand = HandInput()
    excused = exclusions.SampleError(
        path=tickers[0], wire_key=tickers[1], sample=413131, reason="t"
    )
    hand = dataclasses.replace(
        real_hand,
        type_fixes={k: v for k, v in real_hand.type_fixes.items() if k != tickers},
        sample_errors=(*real_hand.sample_errors, excused),
    )
    # The WebSocket trade, whose integer sample its declared type accepts, shares the
    # decision too.
    assert sorted(_problems(Snapshot.load(), hand)) == [
        ("stale-entry", *movers),
        ("stale-entry", "crypto/XT", "i"),
    ]
    # Without either, the full-market sample stops its generator first.
    hand = dataclasses.replace(hand, sample_errors=real_hand.sample_errors)
    assert sorted(_problems(Snapshot.load(), hand)) == [("example-type", *tickers)]


def coverage_pages(coverage: Coverage, path: str) -> tuple[str, ...]:
    """Return the documentation pages that name a path."""
    return coverage.pages[path]


@_requires_snapshot
def test_a_path_shared_by_asset_classes_keeps_every_page(
    real: tuple[tuple[RestPath, ...], Coverage],
) -> None:
    assert coverage_pages(real[1], "/v3/reference/tickers") == (
        "crypto/tickers/all-tickers.md",
        "forex/tickers/all-tickers.md",
        "indices/tickers/all-tickers.md",
        "stocks/tickers/all-tickers.md",
    )
    assert coverage_pages(real[1], "/v3/trades/{stockTicker}") == (
        "stocks/trades-quotes/trades.md",
    )


# --- Synthetic specifications ---------------------------------------------------------------

_PATH = "/v1/things/{id}"


def _names(**long_names: str) -> dict[str, lexicon.LongName]:
    """Return lexicon entries, all citing one test source."""
    return {k: lexicon.LongName(long_name=v, source="test") for k, v in long_names.items()}


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
    example: object = None,
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
        "responses": {
            "200": {"content": {"application/json": {"schema": body, "example": example}}}
        },
        **(operation_extra or {}),
    }
    return {"get": operation, **(extra or {})}


def _snapshot(
    paths: Mapping[str, object],
    *,
    pages: dict[str, str] | None = None,
    schemas: Mapping[str, object] | None = None,
    undocumented: Sequence[str] = (),
) -> Snapshot:
    """Return a snapshot of `paths`, each documented by a page unless named in `undocumented`.

    Pages given in `pages` come first; every other path gets a generated page.
    """
    document = {"openapi": "3.0.3", "paths": paths, "components": {"schemas": schemas or {}}}
    # A JSON round trip gives the document the type a decoded specification has.
    openapi = json.loads(json.dumps(document))
    named = dict(pages or {})
    covered = set(named.values())
    for index, path in enumerate(paths):
        endpoint = f"**Endpoint:** `GET {path}`\n"
        if path not in undocumented and endpoint not in covered:
            named[f"auto/page-{index}.md"] = endpoint
    return Snapshot(
        openapi=openapi,
        llms="",
        pages=named,
        fetched=date(2026, 1, 2),
        page_urls={name: f"https://massive.com/docs/rest/{name}" for name in named},
        index_url="https://massive.com/docs/llms.txt",
    )


def _hand(
    *,
    lexicons: Mapping[str, lexicon.Lexicon] | None = None,
    shapes: Mapping[tuple[str, str], str] | None = None,
    identity: Mapping[str, str] | None = None,
    names: Mapping[tuple[str, str], overrides.Name] | None = None,
    units: Mapping[tuple[str, str], overrides.Unit] | None = None,
    tag_conflicts: Mapping[tuple[str, str], str] | None = None,
    type_fixes: Mapping[tuple[str, str], overrides.TypeFix] | None = None,
    key_fixes: Mapping[tuple[str, str], overrides.KeyFix] | None = None,
    result_keys: Mapping[str, overrides.ResultKey] | None = None,
    param_units: Mapping[tuple[str, str], overrides.Unit] | None = None,
    exclusions: Sequence[exclusions_module.Exclusion] = (),
    kinds: Mapping[str, Mapping[str, Sequence[str]]] | None = None,
    sample_errors: Sequence[exclusions_module.SampleError] = (),
    undeclared_fields: Mapping[tuple[str, str], overrides.UndeclaredField] | None = None,
    optional_fields: Mapping[tuple[str, str], str] | None = None,
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
        key_fixes=key_fixes or {},
        description_fixes={},
        result_keys=result_keys or {},
        param_units=param_units or {},
        exclusions=exclusions,
        kinds=kinds or {},
        sample_errors=sample_errors,
        projections={},
        spellings={},
        shared_descriptions={},
        transport_only={},
        venues={},
        undeclared_fields=undeclared_fields or {},
        optional_fields=optional_fields or {},
        ignored_keys=(),
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


def _one_of(snapshot: Snapshot, hand: HandInput) -> RestPath:
    paths, _ = generate_rest(snapshot, hand)
    (path,) = paths
    return path


def _problems(snapshot: Snapshot, hand: HandInput | None = None) -> list[tuple[str, str, str]]:
    with pytest.raises(RefreshError) as caught:
        generate.generate(snapshot, hand or _hand())
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
            points={"type": "array", "items": {"type": "number"}},
            rows={"type": "array", "items": {"type": "object"}},
            exchange={"type": "number", "format": "int32"},
            venue_ids={"type": "array", "items": {"type": "number", "format": "int64"}},
            stamps={"type": "array", "items": {"type": "string", "format": "date-time"}},
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
        "codes": (WireType.LONG_ARRAY, None),
        "tags": (WireType.STRING_ARRAY, None),
        "points": (WireType.JSON_TEXT, None),
        "rows": (WireType.JSON_TEXT, None),
        "exchange": (WireType.LONG, None),
        "venue_ids": (WireType.LONG_ARRAY, None),
        "stamps": (WireType.JSON_TEXT, None),
        "extra": (WireType.JSON_TEXT, None),
        "either": (WireType.JSON_TEXT, None),
        "anything": (WireType.JSON_TEXT, None),
    }


# Fields Massive sends that the specification does not declare, or marks required but omits.

_VWAP = overrides.UndeclaredField(
    json_type="number", description="The volume weighted average price.", reason="test"
)
_DAY = {
    "type": "object",
    "description": "The most recent daily bar.",
    "properties": {"c": {"type": "number"}, "o": {"type": "number"}},
}


def test_an_undeclared_field_joins_its_object_after_its_last_field() -> None:
    hand = _hand(
        identity={"day": "", "min": "", "c": "", "o": "", "vw": ""},
        undeclared_fields={(_PATH, "day.vw"): _VWAP, (_PATH, "label"): _VWAP},
    )
    path = _one(_row(day=_DAY, name={"type": "string"}), hand)
    assert [f.wire_key for f in path.fields] == ["day.c", "day.o", "day.vw", "name", "label"]
    vwap = path.fields[2]
    assert (vwap.column, vwap.wire_type, vwap.required) == ("DayVw", WireType.DOUBLE, False)
    # It carries its object's note, as a declared field there does.
    assert vwap.description == (
        "The volume weighted average price. From the `day` object: The most recent daily bar."
    )


def test_an_undeclared_field_the_specification_declares_or_cannot_hold_is_stale() -> None:
    hand = _hand(
        identity={"day": "", "min": "", "c": "", "o": "", "vw": ""},
        undeclared_fields={(_PATH, "day.c"): _VWAP, (_PATH, "min.vw"): _VWAP},
    )
    assert _problems(_snapshot({_PATH: _item(_row(day=_DAY))}), hand) == [
        ("stale-entry", _PATH, "day.c"),
        ("stale-entry", _PATH, "min.vw"),
    ]


def test_an_undeclared_field_of_a_path_the_snapshot_lacks_is_stale() -> None:
    hand = _hand(undeclared_fields={("/v1/gone", "x"): _VWAP})
    assert _problems(_snapshot({_PATH: _item(_row(name={"type": "string"}))}), hand) == [
        ("stale-entry", "/v1/gone", "x")
    ]


def test_an_optional_field_entry_drops_the_vendor_requirement() -> None:
    row = {**_row(price={"type": "number"}, size={"type": "number"}), "required": ["price"]}
    hand = _hand(optional_fields={(_PATH, "price"): "test"})
    path = _one(row, hand)
    assert [f.required for f in path.fields] == [False, False]
    # An entry for a field the specification does not mark required is stale.
    stale = _hand(optional_fields={(_PATH, "size"): "test"})
    assert _problems(_snapshot({_PATH: _item(row)}), stale) == [("stale-entry", _PATH, "size")]


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
    # Then the lead order moves the top-level `close` to the top level's first place.
    assert [(f.wire_key, f.column, f.required) for f in paths[0].fields] == [
        ("close", "Close", False),
        ("day.close", "DayClose", False),
        ("ticker", "Ticker", True),
    ]


def test_each_field_records_its_resolved_description_as_plain_text() -> None:
    # The description is read after `$ref` and `allOf` are resolved, and normalized.
    schemas = {
        "Bar": {
            "type": "object",
            "properties": {"close": {"type": "number", "description": "The close\n price."}},
        },
        "Base": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "See [tickers](/docs/tickers)."}
            },
        },
    }
    row = {
        "allOf": [
            {"$ref": "#/components/schemas/Base"},
            {"type": "object", "properties": {"day": {"$ref": "#/components/schemas/Bar"}}},
        ]
    }
    paths, _ = generate_rest(
        _snapshot({_PATH: _item(row)}, schemas=schemas), _hand(identity={"day": ""})
    )
    assert [(f.wire_key, f.description) for f in paths[0].fields] == [
        # A root-relative link resolves against the documentation index's origin.
        ("ticker", "See tickers (https://massive.com/docs/tickers)."),
        ("day.close", "The close price."),
    ]


def test_a_flattened_field_carries_the_descriptions_of_its_enclosing_objects() -> None:
    # Each described object on the way down adds its note, outermost first, under its wire
    # key; the description beside a `$ref` wins over the referenced schema's own, and an
    # object without a description, or with only whitespace, adds none.
    schemas = {
        "Bar": {
            "type": "object",
            "description": "A generic bar.",
            "properties": {"close": {"type": "number", "description": "The close."}},
        },
    }
    row = _row(
        session={
            "type": "object",
            "description": "Omitted before the open.",
            "properties": {
                "day": {"$ref": "#/components/schemas/Bar", "description": "The daily bar."},
                "plain": {
                    "type": "object",
                    "description": " ",
                    "properties": {"lowest": {"type": "number", "description": "The low."}},
                },
                "price": {"type": "number", "description": "The price."},
            },
        },
        extra={"type": "object", "properties": {"high": {"type": "number"}}},
        volume={"type": "number", "description": "The volume."},
    )
    paths, _ = generate_rest(
        _snapshot({_PATH: _item(row)}, schemas=schemas),
        _hand(identity={"session": "", "day": "", "plain": "", "extra": ""}),
    )
    session = "From the `session` object: Omitted before the open."
    assert [(f.wire_key, f.description) for f in paths[0].fields] == [
        (
            "session.day.close",
            f"The close. {session} From the `session.day` object: The daily bar.",
        ),
        ("session.plain.lowest", f"The low. {session}"),
        ("session.price", f"The price. {session}"),
        ("extra.high", ""),
        ("volume", "The volume."),
    ]


def test_a_description_fix_gives_an_undescribed_object_its_note() -> None:
    # An object the vendor leaves undescribed adds a note only when an entry names it; the
    # entry quotes the empty text, so it goes stale once the vendor describes the object.
    fix = overrides.DescriptionFix(vendor="", text="The latest bar.", reason="test")
    hand = dataclasses.replace(
        _hand(identity={"bar": "", "other": ""}),
        description_fixes={(_PATH, "bar"): fix},
    )
    row = _row(
        bar={"type": "object", "properties": {"close": {"type": "number", "description": "C."}}},
        other={"type": "object", "properties": {"open": {"type": "number", "description": "O."}}},
    )
    assert [(f.wire_key, f.description) for f in _one(row, hand).fields] == [
        ("bar.close", "C. From the `bar` object: The latest bar."),
        ("other.open", "O."),
    ]


def test_a_field_without_a_description_generates_and_stops_nothing() -> None:
    # No gate looks at descriptions: a missing, empty, or non-text one is simply "".
    path = _one(
        _row(
            price={"type": "number"},
            size={"type": "number", "description": "  "},
            remark={"type": "number", "description": 5},
        )
    )
    assert [(f.wire_key, f.description) for f in path.fields] == [
        ("price", ""),
        ("size", ""),
        ("remark", ""),
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


def test_gate_stale_entry_a_result_key_the_response_lacks() -> None:
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key="results.values", reason="test")})
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, hand) == [("stale-entry", _PATH, "")]


def test_rows_that_are_not_objects_are_reported() -> None:
    snapshot = _snapshot({_PATH: _item({"type": "string"})})
    assert _problems(snapshot) == [("shape", _PATH, "")]


# Naming.


def test_lexicons_identity_and_names_translate_short_keys() -> None:
    lexicons = {
        "bar": lexicon.Lexicon(name="bar", reason="test", entries=_names(c="close", min="minute")),
        "trade": lexicon.Lexicon(name="trade", reason="test", entries=_names(p="price")),
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
        names={(_PATH, "last.av"): overrides.Name(long_name="accumulated_volume", description="")},
    )
    path = _one(row, hand)
    assert [(f.wire_key, f.column) for f in path.fields] == [
        ("c", "Close"),
        ("min.c", "MinuteClose"),
        ("last.p", "LastPrice"),
        ("last.av", "LastAccumulatedVolume"),
        ("id", "Id"),
    ]


def test_gate_short_key_a_short_key_without_a_name() -> None:
    snapshot = _snapshot({_PATH: _item(_row(p={"type": "number"}, price={"type": "number"}))})
    assert _problems(snapshot) == [("short-key", _PATH, "p")]


def test_gate_short_key_a_short_object_key_without_a_name() -> None:
    snapshot = _snapshot({_PATH: _item(_row(min=_row(close={"type": "number"})))})
    assert _problems(snapshot) == [("short-key", _PATH, "min.close")]


def test_gate_short_key_prints_the_vendor_description() -> None:
    snapshot = _snapshot({_PATH: _item(_row(p={"type": "number", "description": "The  price."}))})
    with pytest.raises(RefreshError) as caught:
        generate_rest(snapshot, _hand())
    assert str(caught.value).splitlines() == [
        "the refresh stopped on 1 problem(s):",
        f"short-key {_PATH} p: short key 'p' has no lexicon, identity, or NAMES entry",
        "    description: The price.",
    ]


def test_gate_column_collision_two_keys_with_one_column() -> None:
    snapshot = _snapshot(
        {_PATH: _item(_row(lastPrice={"type": "number"}, last_price={"type": "number"}))}
    )
    assert _problems(snapshot) == [("column-collision", _PATH, "last_price")]


def test_a_vendor_symbol_keeps_its_name() -> None:
    # The dataset key rename, not the generator, turns a key field into `Symbol`; a field the
    # vendor itself calls `symbol` is simply generated as such.
    assert [f.column for f in _one(_row(symbol={"type": "string"})).fields] == ["Symbol"]


@pytest.mark.parametrize(
    "long_name", ["symbol", "underlying", "timestamp", "date", "timestamp_start", "timestampFoo"]
)
def test_gate_column_collision_names_never_coins_a_key_column(long_name: str) -> None:
    hand = _hand(names={(_PATH, "sym"): overrides.Name(long_name=long_name, description="")})
    snapshot = _snapshot({_PATH: _item(_row(sym={"type": "boolean"}))})
    assert _problems(snapshot, hand) == [("column-collision", _PATH, "sym")]


def test_gate_column_collision_names_the_library_column() -> None:
    hand = _hand(names={(_PATH, "sym"): overrides.Name(long_name="timestamp_end", description="")})
    snapshot = _snapshot({_PATH: _item(_row(sym={"type": "boolean"}))})
    with pytest.raises(RefreshError) as caught:
        generate.generate(snapshot, hand)
    (problem,) = caught.value.problems
    assert problem.message == (
        "NAMES coins the library column 'TimestampEnd'; only the dataset key rename and the "
        "library's time columns produce it"
    )


def test_gate_stale_entry_a_names_quote_the_specification_no_longer_gives() -> None:
    hand = _hand(names={(_PATH, "av"): overrides.Name(long_name="volume", description="Old.")})
    snapshot = _snapshot({_PATH: _item(_row(av={"type": "integer", "description": "New."}))})
    assert _problems(snapshot, hand) == [("stale-entry", _PATH, "av")]
    quoted = _hand(names={(_PATH, "av"): overrides.Name(long_name="volume", description=" New. ")})
    assert [f.column for f in generate_rest(snapshot, quoted)[0][0].fields] == ["Volume"]


# The timestamp-unit gate and units.


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
def test_gate_timestamp_unit_a_timestamp_like_integer_without_units(key: str) -> None:
    snapshot = _snapshot({_PATH: _item(_row(**{key: {"type": "integer"}}))})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, key)]


def test_gate_timestamp_unit_a_unit_tag_makes_any_integer_timestamp_like() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "INanoseconds"}}
    snapshot = _snapshot({_PATH: _item(_row(when=tagged))})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, "when")]


def test_gate_timestamp_unit_runs_on_the_long_name() -> None:
    lexicons = {
        "trade": lexicon.Lexicon(name="trade", reason="test", entries=_names(t="sip_timestamp"))
    }
    snapshot = _snapshot({_PATH: _item(_row(t={"type": "integer"}))})
    assert _problems(snapshot, _hand(lexicons=lexicons, shapes={(_PATH, ""): "trade"})) == [
        ("timestamp-unit", _PATH, "t")
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


def test_gate_stale_entry_a_tag_conflict_that_is_no_conflict() -> None:
    tagged = {"type": "integer", "x-polygon-go-type": {"name": "IMilliseconds"}}
    snapshot = _snapshot({_PATH: _item(_row(timestamp=tagged))})
    hand = _hand(units={(_PATH, "timestamp"): _MS}, tag_conflicts={(_PATH, "timestamp"): "test"})
    assert _problems(snapshot, hand) == [("stale-entry", _PATH, "timestamp")]


def test_gate_stale_entry_units_on_a_field_that_is_not_an_integer() -> None:
    snapshot = _snapshot({_PATH: _item(_row(timestamp={"type": "string"}))})
    assert _problems(snapshot, _hand(units={(_PATH, "timestamp"): _MS})) == [
        ("stale-entry", _PATH, "timestamp")
    ]


# Declared conversions.


def test_a_type_fix_records_its_conversion() -> None:
    fix = overrides.TypeFix(conversion=Conversion.QUOTED_INTEGER, reason="test")
    path = _one(_row(size={"type": "integer"}), _hand(type_fixes={(_PATH, "size"): fix}))
    (field,) = path.fields
    assert (field.wire_type, field.conversion) == (WireType.LONG, Conversion.QUOTED_INTEGER)


def test_gate_stale_entry_a_type_fix_the_specification_no_longer_needs() -> None:
    fix = overrides.TypeFix(conversion=Conversion.NUMBER_LABEL, reason="test")
    snapshot = _snapshot({_PATH: _item(_row(size={"type": "integer"}))})
    assert _problems(snapshot, _hand(type_fixes={(_PATH, "size"): fix})) == [
        ("stale-entry", _PATH, "size")
    ]


def test_a_type_fix_to_a_chicago_midnight_keeps_a_date() -> None:
    fix = overrides.TypeFix(conversion=Conversion.CHICAGO_MIDNIGHT_DATE, reason="test")
    # The field's own example is the integer that makes the conversion necessary.
    row = _row(settles_on={"type": "string", "format": "date", "example": 1_753_851_600 * 10**9})
    (field,) = _one(row, _hand(type_fixes={(_PATH, "settles_on"): fix})).fields
    assert (field.wire_type, field.unit, field.conversion) == (
        WireType.DATE,
        None,
        Conversion.CHICAGO_MIDNIGHT_DATE,
    )


def test_gate_stale_entry_a_unit_on_a_field_a_type_fix_keeps_from_an_epoch() -> None:
    fix = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="test")
    hand = _hand(
        type_fixes={(_PATH, "ref"): fix}, units={(_PATH, "ref"): _MS}, identity={"ref": ""}
    )
    snapshot = _snapshot({_PATH: _item(_row(ref={"type": "integer"}))})
    assert _problems(snapshot, hand) == [("stale-entry", _PATH, "ref")]


# The example-type gate.


def test_gate_example_type_a_documented_value_the_type_refuses() -> None:
    row = _row(code={"type": "integer"}, bar=_row(close={"type": "number"}))
    example = {
        "status": "OK",
        "results": [
            {"code": "2208", "bar": {"close": 1.5}},
            {"code": "2209", "bar": {"close": "high"}},
        ],
    }
    snapshot = _snapshot({_PATH: _item(row, example=example)})
    hand = _hand(identity={"bar": ""})
    # Each field is reported once, however many rows disagree.
    assert _problems(snapshot, hand) == [
        ("example-type", _PATH, "bar.close"),
        ("example-type", _PATH, "code"),
    ]
    fix = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="test")
    fixed = _snapshot({_PATH: _item(_row(code={"type": "integer"}), example=example)})
    (field,) = _one_of(fixed, _hand(type_fixes={(_PATH, "code"): fix})).fields
    assert field.wire_type is WireType.STRING


def test_gate_example_type_reads_rows_where_the_result_key_says() -> None:
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key="results.values", reason="test")})
    row = _row(values={"type": "array", "items": _row(value={"type": "number"})})
    bad = {"results": {"values": [{"value": "1.0"}]}}
    snapshot = _snapshot({_PATH: _item(row, array=False, example=bad)})
    assert _problems(snapshot, hand) == [("example-type", _PATH, "value")]
    body = _hand(result_keys={_PATH: overrides.ResultKey(key=None, reason="test")})
    listed = _snapshot(
        {_PATH: _item(_row(label={"type": "string"}), result=None, example=[{"label": 3}])}
    )
    assert _problems(listed, body) == [("example-type", _PATH, "label")]


def test_gate_example_type_skips_what_the_example_leaves_out() -> None:
    row = _row(price={"type": "number"}, size={"type": "integer"}, last=_row(p={"type": "number"}))
    example = {"results": [{"price": None, "last": None}, "not a row", {"size": 3}]}
    hand = _hand(identity={"p": ""})
    assert [
        f.wire_key for f in _one_of(_snapshot({_PATH: _item(row, example=example)}), hand).fields
    ] == [
        "price",
        "size",
        "last.p",
    ]


def test_gate_example_type_checks_a_field_s_own_example() -> None:
    row = _row(
        cusip={"type": "string", "example": 37833100}, size={"type": "integer", "example": 3}
    )
    snapshot = _snapshot({_PATH: _item(row)})
    assert _problems(snapshot) == [("example-type", _PATH, "cusip")]


def test_a_sample_error_matches_its_sample_as_json_text() -> None:
    # `true`, `1`, and `1.0` are three samples: an entry for `1` does not excuse `true`.
    row = _row(label={"type": "string", "example": True})
    error = exclusions.SampleError(path=_PATH, wire_key="label", sample=1, reason="t")
    snapshot = _snapshot({_PATH: _item(row)})
    assert sorted(_problems(snapshot, _hand(sample_errors=(error,)))) == [
        ("example-type", _PATH, "label"),
        ("stale-entry", _PATH, "label"),
    ]


def test_gate_stale_entry_a_sample_error_for_a_field_that_is_gone() -> None:
    error = exclusions.SampleError(path=_PATH, wire_key="us_code", sample=1, reason="t")
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, _hand(sample_errors=(error,))) == [("stale-entry", _PATH, "us_code")]


def test_a_sample_error_skips_exactly_its_wrong_sample() -> None:
    row = _row(cusip={"type": "string", "example": 37833100})
    error = exclusions.SampleError(path=_PATH, wire_key="cusip", sample=37833100, reason="t")
    assert [f.column for f in _one(row, _hand(sample_errors=(error,))).fields] == ["Cusip"]
    # Another wrong sample of the same field is still reported.
    example = {"results": [{"cusip": 12}]}
    snapshot = _snapshot({_PATH: _item(row, example=example)})
    assert _problems(snapshot, _hand(sample_errors=(error,))) == [("example-type", _PATH, "cusip")]


@pytest.mark.parametrize("sample", ["037833100", 99])
def test_gate_stale_entry_a_sample_error_the_specification_no_longer_shows(
    sample: str | int,
) -> None:
    # The sample is now accepted, or another value is documented: the entry is stale.
    row = _row(cusip={"type": "string", "example": "037833100"})
    error = exclusions.SampleError(path=_PATH, wire_key="cusip", sample=sample, reason="t")
    snapshot = _snapshot({_PATH: _item(row)})
    assert _problems(snapshot, _hand(sample_errors=(error,))) == [("stale-entry", _PATH, "cusip")]


def test_gate_example_type_an_example_with_no_rows_where_the_result_key_points() -> None:
    row = _row(price={"type": "number"})
    snapshot = _snapshot({_PATH: _item(row, example={"status": "OK", "data": [{"price": 1}]})})
    assert _problems(snapshot) == [("example-type", _PATH, "")]


def test_a_type_fix_from_untyped_text_is_needed_while_the_text_is_untyped() -> None:
    # Plain text accepts an ISO instant, so no sample "needs" the fix; it stays until the
    # specification types the field.
    fix = overrides.TypeFix(conversion=Conversion.ISO_TEXT_INSTANT, reason="t")
    row = _row(serverTime={"type": "string", "example": "2020-11-10T17:37:37-05:00"})
    (field,) = _one(row, _hand(type_fixes={(_PATH, "serverTime"): fix})).fields
    assert field.conversion is Conversion.ISO_TEXT_INSTANT


def test_gate_stale_entry_a_type_fix_no_sample_needs() -> None:
    # A corrected sample makes the identifier fix unnecessary.
    fix = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="t")
    row = _row(company={"type": "integer", "example": 2208})
    snapshot = _snapshot({_PATH: _item(row)})
    assert _problems(snapshot, _hand(type_fixes={(_PATH, "company"): fix})) == [
        ("stale-entry", _PATH, "company")
    ]


# Parameters.


def _param(name: str, schema: Mapping[str, object], *, where: str = "query") -> dict[str, object]:
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


def test_a_string_whose_description_gives_the_date_format_is_a_date() -> None:
    # The vendor's financial statements type `period_end` and its range filters as bare
    # strings, naming the date format only in the description.
    stated = "Value must be formatted 'yyyy-mm-dd'."
    params = [
        {**_param("period_end", {"type": "string"}), "description": f"The last date. {stated}"},
        {**_param("period_end.gte", {"type": "string"}), "description": f"Filter. {stated}"},
        {**_param("period_end.any_of", {"type": "string"}), "description": stated},
        {**_param("active_from", {"type": "string"}), "description": "A date (YYYY-MM-DD)."},
        {**_param("fiscal_year", {"type": "integer"}), "description": stated},
        {**_param("timeframe", {"type": "string"}), "description": "Annual or quarterly."},
    ]
    path = _one(_row(price={"type": "number"}), params=params)
    assert [(p.name, p.kind) for p in path.params[1:]] == [
        ("period_end", ParamKind.DATE),
        ("period_end.gte", ParamKind.DATE),
        ("period_end.any_of", ParamKind.LIST),
        ("active_from", ParamKind.DATE),
        ("fiscal_year", ParamKind.INTEGER),
        ("timeframe", ParamKind.STRING),
    ]


def test_a_stated_date_format_does_not_replace_a_timestamp_unit() -> None:
    # A timestamp filter that also accepts a date keeps the unit its entry gives.
    stated = "Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd'."
    params = [{**_param("timestamp.gte", {"type": "string"}), "description": stated}]
    hand = _hand(param_units={(_PATH, "timestamp"): overrides.Unit(unit=WireUnit.NS, reason="t")})
    path = _one(_row(price={"type": "number"}), hand, params=params)
    assert (path.params[1].kind, path.params[1].unit) == (ParamKind.TIMESTAMP, WireUnit.NS)


def test_a_parameter_keeps_the_scalar_default_its_schema_documents() -> None:
    params = [
        _param("window", {"type": "integer", "default": 50}),
        _param("timespan", {"type": "string", "enum": ["day", "week"], "default": "day"}),
        _param("adjusted", {"type": "boolean", "default": True}),
        _param("ratio", {"type": "number", "default": 0.5}),
        _param("tickers", {"type": "array", "items": {"type": "string"}, "default": ["A"]}),
        _param("search", {"type": "string"}),
    ]
    path = _one(_row(price={"type": "number"}), params=params)
    assert [(p.name, p.default) for p in path.params[1:]] == [
        ("window", 50),
        ("timespan", "day"),
        ("adjusted", True),
        ("ratio", 0.5),
        ("tickers", None),  # not a scalar
        ("search", None),
    ]


def test_a_parameter_keeps_the_integer_maximum_its_schema_documents() -> None:
    params = [
        _param("limit", {"type": "integer", "default": 10, "maximum": 50000}),
        _param("ratio", {"type": "number", "maximum": 1.5}),  # not an integer
        _param("flag", {"type": "integer", "maximum": True}),  # a boolean, not an integer
        _param("count", {"type": "integer"}),  # the maximum is stated only in prose, if at all
    ]
    path = _one(_row(price={"type": "number"}), params=params)
    assert [(p.name, p.maximum) for p in path.params[1:]] == [
        ("limit", 50000),
        ("ratio", None),
        ("flag", None),
        ("count", None),
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


def test_gate_timestamp_unit_a_timestamp_like_parameter_without_units_is_reported_once() -> None:
    params = [
        _param("window_start", {"type": "string"}),
        _param("window_start.lt", {"type": "string"}),
    ]
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, "window_start")]


def test_a_timestamp_like_integer_parameter_needs_no_units() -> None:
    params = [_param("timestamp", {"type": "integer"})]
    path = _one(_row(price={"type": "number"}), params=params)
    assert path.params[1].kind is ParamKind.INTEGER


def test_gate_stale_entry_param_units_for_a_parameter_that_is_not_a_timestamp_string() -> None:
    params = [_param("timestamp", {"type": "integer"})]
    hand = _hand(param_units={(_PATH, "timestamp"): overrides.Unit(unit=WireUnit.NS, reason="t")})
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot, hand) == [("stale-entry", _PATH, "timestamp")]


def test_a_parameter_outside_the_path_and_query_is_reported() -> None:
    params = [_param("X-Key", {"type": "string"}, where="header")]
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}), params=params)})
    assert _problems(snapshot) == [("shape", _PATH, "X-Key")]


def test_a_path_whose_parameters_break_its_template_is_reported() -> None:
    item = _item(_row(price={"type": "number"}))
    snapshot = _snapshot({"/v1/things/{other}": item})
    assert _problems(snapshot) == [("shape", "/v1/things/{other}", "")]


# The stale-entry gate for every other table.


def test_gate_stale_entry_hand_entries_naming_nothing() -> None:
    lexicons = {"bar": lexicon.Lexicon(name="bar", reason="test", entries={})}
    hand = _hand(
        lexicons=lexicons,
        shapes={(_PATH, "gone"): "bar", ("/gone", ""): "bar"},
        names={(_PATH, "gone"): overrides.Name(long_name="x", description="")},
        identity={"zz": "unused"},
        units={("/gone", "t"): _MS},
        tag_conflicts={(_PATH, "gone"): "test"},
        type_fixes={
            (_PATH, "gone"): overrides.TypeFix(
                conversion=Conversion.TEXT_OR_INTEGER_ID, reason="test"
            )
        },
        result_keys={"/gone": overrides.ResultKey(key=None, reason="test")},
        param_units={(_PATH, "gone"): _MS},
    )
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert sorted(_problems(snapshot, hand)) == sorted(
        [
            ("stale-entry", _PATH, "gone"),  # shape
            ("stale-entry", "/gone", ""),  # shape of a path not generated
            ("stale-entry", _PATH, "gone"),  # names
            ("stale-entry", "/gone", "t"),  # units
            ("stale-entry", _PATH, "gone"),  # tag conflicts
            ("stale-entry", _PATH, "gone"),  # type fixes
            ("stale-entry", "/gone", ""),  # result keys
            ("stale-entry", _PATH, "gone"),  # param units
            ("stale-entry", "", "zz"),  # identity
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
    assert coverage.specified == {"/a/{id}", "/b/{id}", "/c/{id}", "/d/{id}", "/e/{id}", "/f/{id}"}
    assert coverage.pages["/c/{id}"] == ("c.md",)


def test_exclusions_of_other_transports_are_left_to_their_generators() -> None:
    other = exclusions.Exclusion(kind=Transport.WS, item="stocks/LV", reason="test")
    path = _one(_row(price={"type": "number"}), _hand(exclusions=(other,)))
    assert path.key == _PATH


@pytest.mark.parametrize(
    ("extra", "operation_extra"),
    [(None, {"x-polygon-deprecation": {}}), ({"x-polygon-draft": True}, None)],
    ids=["deprecated", "draft"],
)
def test_gate_stale_entry_an_exclusion_the_specification_already_accounts_for(
    extra: dict[str, object] | None, operation_extra: dict[str, object] | None
) -> None:
    item = _item(_row(price={"type": "number"}), extra=extra, operation_extra=operation_extra)
    hand = _hand(
        exclusions=(
            exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t", undocumented=True),
        )
    )
    assert _problems(_snapshot({_PATH: item}), hand) == [("stale-entry", _PATH, "")]


def test_gate_stale_entry_an_exclusion_the_specification_lacks() -> None:
    hand = _hand(exclusions=(exclusions.Exclusion(kind=Transport.REST, item="/gone", reason="t"),))
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))})
    assert _problems(snapshot, hand) == [("stale-entry", "/gone", "")]


def test_gate_docs_an_undocumented_exclusion_that_is_documented() -> None:
    entry = exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t", undocumented=True)
    snapshot = _snapshot(
        {_PATH: _item(_row(price={"type": "number"}))},
        pages={"thing.md": f"**Endpoint:** `GET {_PATH}`\n"},
    )
    assert _problems(snapshot, _hand(exclusions=(entry,))) == [("docs", _PATH, "")]


def test_gate_docs_a_documented_exclusion_that_no_page_names() -> None:
    entry = exclusions.Exclusion(kind=Transport.REST, item=_PATH, reason="t")
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))}, undocumented=[_PATH])
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


# The timestamp-unit gate beyond plain integers.


@pytest.mark.parametrize("key", ["serverTime", "lastUpdated", "sipTimestamp"])
def test_gate_timestamp_unit_matches_camel_case_names(key: str) -> None:
    snapshot = _snapshot({_PATH: _item(_row(**{key: {"type": "integer"}}))})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, key)]


def test_gate_timestamp_unit_covers_numbers() -> None:
    snapshot = _snapshot({_PATH: _item(_row(timestamp={"type": "number"}))})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, "timestamp")]


def test_gate_timestamp_unit_covers_text_with_no_format() -> None:
    snapshot = _snapshot({_PATH: _item(_row(serverTime={"type": "string"}))})
    assert _problems(snapshot) == [("timestamp-unit", _PATH, "serverTime")]


def test_timestamp_like_text_is_decided_by_a_type_fix_or_a_units_entry() -> None:
    row = _row(serverTime={"type": "string"})
    fix = overrides.TypeFix(conversion=Conversion.ISO_TEXT_INSTANT, reason="t")
    fixed = _one(row, _hand(type_fixes={(_PATH, "serverTime"): fix}))
    assert _types(fixed) == {"serverTime": (WireType.ISO_INSTANT, None)}
    text = overrides.Unit(unit=None, reason="free text, not a time")
    kept = _one(row, _hand(units={(_PATH, "serverTime"): text}))
    assert _types(kept) == {"serverTime": (WireType.STRING, None)}


def test_formatted_timestamp_text_needs_no_entry() -> None:
    row = _row(updated={"type": "string", "format": "date-time"})
    assert _types(_one(row)) == {"updated": (WireType.ISO_INSTANT, None)}


@pytest.mark.parametrize("kind", ["number", "string"])
def test_gate_stale_entry_a_unit_on_a_number_or_text(kind: str) -> None:
    snapshot = _snapshot({_PATH: _item(_row(timestamp={"type": kind}))})
    assert _problems(snapshot, _hand(units={(_PATH, "timestamp"): _MS})) == [
        ("stale-entry", _PATH, "timestamp")
    ]


def test_gate_stale_entry_units_on_a_field_that_cannot_be_a_timestamp() -> None:
    entry = overrides.Unit(unit=None, reason="t")
    snapshot = _snapshot({_PATH: _item(_row(flags={"type": "boolean"}))})
    assert _problems(snapshot, _hand(units={(_PATH, "flags"): entry})) == [
        ("stale-entry", _PATH, "flags")
    ]


# Parameters beyond the timestamp-like names.


def test_param_units_make_any_named_string_parameter_a_timestamp() -> None:
    params = [_param("from", {"type": "string"})]
    hand = _hand(param_units={(_PATH, "from"): _MS})
    path = _one(_row(price={"type": "number"}), hand, params=params)
    assert (path.params[1].kind, path.params[1].unit) == (ParamKind.TIMESTAMP, WireUnit.MS)


def test_gate_shape_a_parameter_choice_that_is_not_of_date_formats() -> None:
    choice = {"oneOf": [{"type": "integer"}, {"type": "string", "format": "date"}]}
    snapshot = _snapshot(
        {_PATH: _item(_row(price={"type": "number"}), params=[_param("since", choice)])}
    )
    assert _problems(snapshot) == [("shape", _PATH, "since")]


# Vendor extensions and the documentation.


def test_gate_shape_an_unknown_vendor_extension() -> None:
    row = _row(price={"type": "number"})
    snapshot = _snapshot(
        {
            "/a/{id}": _item(row, extra={"x-polygon-new-flag": True}),
            "/b/{id}": _item(row, operation_extra={"x-polygon-beta": True}),
            "/c/{id}": _item(row, operation_extra={"x-polygon-experimental": {}}),
        }
    )
    assert _problems(snapshot) == [
        ("shape", "/a/{id}", "x-polygon-new-flag"),
        ("shape", "/b/{id}", "x-polygon-beta"),
    ]


def test_gate_docs_a_generated_path_no_page_names() -> None:
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))}, undocumented=[_PATH])
    assert _problems(snapshot) == [("docs", _PATH, "")]


def test_gate_docs_a_page_that_is_not_an_overview_names_no_endpoint() -> None:
    pages = {"stocks/overview.md": "# Stocks\n", "stocks/trades.md": "# Trades, no endpoint\n"}
    snapshot = _snapshot({_PATH: _item(_row(price={"type": "number"}))}, pages=pages)
    assert _problems(snapshot) == [("docs", "", "stocks/trades.md")]


# The concept-collision gate, through the whole refresh: one concept, one column, in a kind.


def test_gate_concept_collision_one_column_for_two_concepts_within_a_kind() -> None:
    # Two nested paths that PascalCase alike are two concepts. (A kind of REST paths alone
    # has no flat-file or WebSocket order either, which `column-order` reports too.)
    lexicons = {"trade": lexicon.Lexicon(name="trade", reason="t", entries=_names(p="price"))}
    snapshot = _snapshot(
        {
            "/a/{id}": _item(_row(lastTrade=_row(p={"type": "number"}))),
            "/b/{id}": _item(_row(last_trade=_row(price={"type": "number"}))),
            "/c/{id}": _item(_row(last=_row(trade_price={"type": "number"}))),
        }
    )
    hand = _hand(lexicons=lexicons, shapes={("/a/{id}", "lastTrade"): "trade"})
    same = dataclasses.replace(hand, kinds={"trades": {"stocks": ["/a/{id}", "/b/{id}"]}})
    assert {gate for gate, _, _ in _problems(snapshot, same)} == {"column-order"}
    clash = dataclasses.replace(hand, kinds={"trades": {"stocks": ["/a/{id}", "/c/{id}"]}})
    assert ("concept-collision", "", "LastTradePrice") in _problems(snapshot, clash)


# Problems are never lost.


def test_problems_found_before_an_unreadable_schema_are_kept() -> None:
    # The mistyped envelope key is reported before the bad reference stops the path.
    row = _row(status={"type": "integer"}, nested={"$ref": "#/components/schemas/Missing"})
    hand = _hand(result_keys={_PATH: overrides.ResultKey(key=None, reason="test")})
    snapshot = _snapshot({_PATH: _item(row, result=None, array=False)})
    assert sorted(gate for gate, _, _ in _problems(snapshot, hand)) == ["envelope", "shape"]


def test_the_envelope_types_cover_exactly_the_envelope_keys() -> None:
    assert set(rest_sources._ENVELOPE_TYPES) == ENVELOPE_KEYS
