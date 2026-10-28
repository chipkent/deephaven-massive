"""Tests for the refresh: the parity of `spec/*.json` with the snapshot, and when it writes.

Every test reads the vendored snapshot, so the module skips where there is none (an sdist
leaves it out). The gates that span transports are shown on the real snapshot here, by taking
one piece of hand input away or changing it: each such test fails if its gate is deleted.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import exclusions, generate, lexicon, overrides, render
from deephaven_massive._massive.refresh import snapshot as snapshot_module
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.snapshot import (
    DIRECTORY,
    MANIFEST,
    OPENAPI,
    Snapshot,
    VendorFile,
)
from deephaven_massive._massive.spec.types import Transport

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)

_COMMITTED = (
    render.REST_FILE,
    render.WS_FILE,
    render.FLAT_FILES_FILE,
    render.DESCRIPTIONS_FILE,
)


@pytest.fixture(scope="module")
def real() -> generate.Generated:
    """Generate from the vendored snapshot once for the module.

    Returns:
        Every generated source and coverage.
    """
    return generate.generate(Snapshot.load())


def test_the_generated_files_match_the_snapshot_byte_for_byte(tmp_path: Path) -> None:
    # A fresh rendering must be each committed file exactly: they change only through the
    # refresh.
    written = generate.refresh(spec_directory=tmp_path)
    assert written == [tmp_path / path.name for path in _COMMITTED]
    for committed in _COMMITTED:
        assert (tmp_path / committed.name).read_bytes() == committed.read_bytes(), committed.name


def test_no_link_syntax_survives_in_the_descriptions() -> None:
    # Descriptions have no gate, so a link form the plain-text rules do not know would land
    # silently; this fails on the refresh that brings one, so the rules can be extended.
    document = json.loads(render.DESCRIPTIONS_FILE.read_text(encoding="utf-8"))
    texts = [
        text
        for by_key in document["descriptions"].values()
        for described in by_key.values()
        for text in described.values()
    ]
    assert texts
    leftovers = [t for t in texts if any(form in t.lower() for form in ("](", "<a", "href"))]
    assert leftovers == []


def test_every_transport_s_descriptions_and_urls_are_recorded() -> None:
    document = json.loads(render.DESCRIPTIONS_FILE.read_text(encoding="utf-8"))
    assert set(document["urls"]) == {"REST", "WS", "FLAT_FILES"}
    assert set(document["descriptions"]) == {"REST", "WS", "FLAT_FILES"}
    # A futures dataset's URLs are its four venue pages.
    assert len(document["urls"]["FLAT_FILES"]["us_futures_{venue}/trades_v1"]) == 4
    assert document["urls"]["WS"]["stocks/T"] == [
        "https://massive.com/docs/websocket/stocks/trades.md"
    ]


def test_refresh_writes_only_a_changed_file(tmp_path: Path) -> None:
    committed = [path.read_bytes() for path in _COMMITTED]
    for path in _COMMITTED:
        (tmp_path / path.name).write_bytes(path.read_bytes())
    (tmp_path / "descriptions.json").write_text("stale\n", encoding="utf-8")
    assert generate.refresh(spec_directory=tmp_path) == [tmp_path / "descriptions.json"]
    assert (tmp_path / "descriptions.json").read_bytes() == committed[-1]
    assert generate.refresh(spec_directory=tmp_path) == []
    assert [path.read_bytes() for path in _COMMITTED] == committed


def test_refresh_reads_the_snapshot_it_is_given(tmp_path: Path) -> None:
    # The default snapshot is read only when no directory is passed.
    assert len(generate.refresh(DIRECTORY, tmp_path)) == len(_COMMITTED)
    assert (tmp_path / "rest.json").read_bytes() == render.REST_FILE.read_bytes()


def _copy_snapshot(target: Path, openapi: bytes) -> None:
    """Store a copy of the vendored snapshot with another specification, re-hashed.

    Args:
        target: Where to store it.
        openapi: The stored bytes of the replacement specification.
    """
    manifest = json.loads((DIRECTORY / MANIFEST).read_text(encoding="utf-8"))
    files = []
    for name, entry in manifest["files"].items():
        stored = openapi if name == OPENAPI else (DIRECTORY / name).read_bytes()
        files.append(VendorFile(name=name, url=entry["url"], raw=stored, stored=stored))
    snapshot_module.store(target, files, fetched=dt.date.fromisoformat(manifest["fetched"]))


def test_a_reworded_description_changes_only_the_descriptions_file(tmp_path: Path) -> None:
    # The only change is one field's description: the schema files must stay byte for byte as
    # they are, so a reviewer sees that no schema moved.
    original = (DIRECTORY / OPENAPI).read_bytes()
    reworded = original.replace(b"The exchange symbol that this item is traded under.", b"Moved.")
    assert reworded != original
    _copy_snapshot(tmp_path / "snapshot", reworded)
    (tmp_path / "spec").mkdir()
    for committed in _COMMITTED:
        (tmp_path / "spec" / committed.name).write_bytes(committed.read_bytes())
    written = generate.refresh(tmp_path / "snapshot", tmp_path / "spec")
    assert written == [tmp_path / "spec" / "descriptions.json"]
    for committed in _COMMITTED[:-1]:
        assert (tmp_path / "spec" / committed.name).read_bytes() == committed.read_bytes()
    texts = json.loads((tmp_path / "spec" / "descriptions.json").read_text(encoding="utf-8"))
    assert "Moved." in {
        text for by_key in texts["descriptions"]["REST"].values() for text in by_key.values()
    }


# --- Coverage ------------------------------------------------------------------------------


def test_every_channel_is_accounted_for_exactly_once(real: generate.Generated) -> None:
    # The specification's 37 channels: generated, excluded (Launchpad), or deprecated (`XL2`).
    coverage = real.ws_coverage
    assert len(coverage.specified) == 37
    generated = {f"/{key}" for key in coverage.generated} | {
        f"/business/{key}" for key in coverage.generated if key.endswith("/FMV")
    }
    parts = [generated & coverage.specified, coverage.excluded, coverage.deprecated]
    assert sum(len(part) for part in parts) == len(coverage.specified)
    assert frozenset().union(*parts) == coverage.specified
    assert coverage.deprecated == {"/crypto/XL2"}
    assert len(coverage.generated) == 28


def test_every_documented_prefix_is_accounted_for_exactly_once(
    real: generate.Generated,
) -> None:
    coverage = real.flat_file_coverage
    assert coverage.generated | coverage.excluded == coverage.specified
    assert coverage.generated.isdisjoint(coverage.excluded)
    assert len(coverage.specified) == 21
    # The four venues' prefixes of each futures dataset fold into one template.
    assert {key for key in coverage.specified if "{venue}" in key} == {
        "us_futures_{venue}/trades_v1",
        "us_futures_{venue}/quotes_v1",
        "us_futures_{venue}/minute_aggs_v1",
        "us_futures_{venue}/session_aggs_v1",
    }


def test_every_generated_channel_and_prefix_belongs_to_a_kind(real: generate.Generated) -> None:
    # Every streamed or filed dataset of the namespace tree has a kind, so a channel or prefix
    # in no kind would be documented yet neither used nor excluded.
    for source in (*real.ws, *real.flat_files):
        assert source.memberships, source.key


# --- The dataset kinds ---------------------------------------------------------------------


def test_the_stocks_aggregate_order_is_the_flat_files_own(real: generate.Generated) -> None:
    # The vendor's file order, from the page's example header (its "Columns" table is
    # alphabetical), orders every source of the kind's stocks group.
    order = ["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]
    files = {s.key: s for s in real.flat_files}
    for prefix in ("us_stocks_sip/day_aggs_v1", "us_stocks_sip/minute_aggs_v1"):
        assert [f.wire_key for f in files[prefix].fields] == order
    columns = [f.column for f in files["us_stocks_sip/day_aggs_v1"].fields]
    rest = {p.key: p for p in real.rest}
    range_path = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
    ws = {c.key: c for c in real.ws}
    for source in (rest[range_path], ws["stocks/AM"]):
        shared = [f.column for f in source.fields if f.column in columns]
        assert shared == [c for c in columns if c in shared], source.key
    # A REST source in no kind keeps the specification's alphabetical order.
    tickers = [f.wire_key for f in rest["/v3/reference/tickers"].fields]
    assert tickers == sorted(tickers)
    # The committed files record the order.
    committed = json.loads(render.FLAT_FILES_FILE.read_text(encoding="utf-8"))
    day = next(s for s in committed["sources"] if s["key"] == "us_stocks_sip/day_aggs_v1")
    assert [f["wire_key"] for f in day["fields"]] == order


def test_the_quote_prices_are_generated_names(real: generate.Generated) -> None:
    # The examples join trades to quotes on `BidPrice` and `AskPrice`: every quotes source of
    # every transport generates both.
    members = [
        (source, membership)
        for source in (*real.rest, *real.ws, *real.flat_files)
        for membership in source.memberships
        if membership.kind == "quotes"
    ]
    assert {s.transport for s, _ in members} == set(Transport)
    for source, membership in members:
        if membership.projection is None:
            columns = {f.column for f in source.fields}
        else:
            # The forex last quote and the crypto stream spell them `ask` and `bid`, as
            # Massive's last-quote objects do; their projections give the quotes spelling.
            columns = set(membership.projection.values())
        assert {"BidPrice", "AskPrice"} <= columns, source.key


def test_memberships_follow_kinds_and_projections(real: generate.Generated) -> None:
    rest = {p.key: p for p in real.rest}
    (values,) = rest["/v3/snapshot/indices"].memberships
    assert (values.kind, values.group, values.position) == ("values", "indices", 0)
    assert values.projection == {"ticker": "Ticker", "value": "Value", "last_updated": "Timestamp"}
    ws = {c.key: c for c in real.ws}
    (trades,) = ws["stocks/T"].memberships
    assert (trades.kind, trades.group, trades.position, trades.projection) == (
        "trades",
        "stocks",
        2,
        None,
    )
    # A REST source in no kind has no memberships.
    assert rest["/v3/reference/tickers"].memberships == ()


def _problems(hand: HandInput) -> list[tuple[str, str, str]]:
    with pytest.raises(RefreshError) as caught:
        generate.generate(Snapshot.load(), hand)
    return sorted((p.gate, p.source, p.key) for p in caught.value.problems)


def _relexicon(name: str, **long_names: str) -> HandInput:
    """Return the real hand input with some entries of one lexicon renamed."""
    old = lexicon.LEXICONS[name]
    entries = {
        **old.entries,
        **{k: lexicon.LongName(long_name=v, source="test") for k, v in long_names.items()},
    }
    lexicons = {**lexicon.LEXICONS, name: dataclasses.replace(old, entries=entries)}
    return dataclasses.replace(HandInput(), lexicons=lexicons)


def test_gate_type_agreement_stocks_dv_against_futures_dv() -> None:
    # Naming the futures dollar volume like the stocks fractional volume puts a double and a
    # string into one column of the bars kind: the naming error the gate exists to catch.
    # The renamed column is also one the futures stream alone supplies.
    assert _problems(_relexicon("futures_bar", dv="decimal_volume")) == [
        ("transport-agreement", "bars", "DecimalVolume"),
        ("type-agreement", "bars", "DecimalVolume"),
    ]


def test_gate_type_agreement_needs_its_type_fix() -> None:
    # Without its conversion the crypto WebSocket trade ID is an integer, while REST and the
    # flat file keep it as text.
    real_hand = HandInput()
    hand = dataclasses.replace(
        real_hand,
        type_fixes={k: v for k, v in real_hand.type_fixes.items() if k != ("crypto/XT", "i")},
    )
    assert _problems(hand) == [("type-agreement", "trades", "Id")]


def test_gate_transport_agreement_a_one_sided_lexicon_slip() -> None:
    # A futures WebSocket trade's `s` named `shares`, while REST and the files say `size`.
    assert _problems(_relexicon("futures_trade", s="shares")) == [
        ("transport-agreement", "trades", "Shares"),
    ]


def test_gate_transport_agreement_needs_each_transport_only_entry() -> None:
    real_hand = HandInput()
    hand = dataclasses.replace(
        real_hand,
        transport_only={
            k: v
            for k, v in real_hand.transport_only.items()
            if k != ("bars", "options", "AccumulatedVolume")
        },
    )
    assert _problems(hand) == [("transport-agreement", "bars", "AccumulatedVolume")]


def test_gate_stale_entry_a_kind_member_the_snapshot_does_not_generate() -> None:
    real_hand = HandInput()
    kinds = {**real_hand.kinds, "luld": {"stocks": ["stocks/LULD", "stocks/LV"]}}
    assert _problems(dataclasses.replace(real_hand, kinds=kinds)) == [
        ("stale-entry", "stocks/LV", "luld"),
    ]


def test_gate_column_order_a_kind_with_no_stream_or_file() -> None:
    real_hand = HandInput()
    kinds = {**real_hand.kinds, "tickers": {"all": ["/v3/reference/tickers"]}}
    assert _problems(dataclasses.replace(real_hand, kinds=kinds)) == [
        ("column-order", "/v3/reference/tickers", "tickers"),
    ]


def test_gate_stale_entry_channel_and_prefix_entries_naming_nothing() -> None:
    real_hand = HandInput()
    gone = overrides.Unit(unit=None, reason="test")
    hand = dataclasses.replace(
        real_hand,
        units={**real_hand.units, ("stocks/T", "gone"): gone, ("us_gone/trades_v1", "t"): gone},
        exclusions=(
            *real_hand.exclusions,
            exclusions.Exclusion(kind=Transport.WS, item="/stocks/GONE", reason="test"),
        ),
    )
    assert _problems(hand) == [
        ("stale-entry", "/stocks/GONE", ""),
    ]
    hand = dataclasses.replace(hand, exclusions=real_hand.exclusions)
    assert _problems(hand) == [
        ("stale-entry", "stocks/T", "gone"),
        ("stale-entry", "us_gone/trades_v1", "t"),
    ]


def test_gate_shape_two_transports_with_one_source_key() -> None:
    # A channel and a prefix spelled alike would make every hand entry naming them ambiguous.
    message = {"type": "object", "properties": {"price": {"type": "number"}}}
    media = {"application/json": {"schema": message}}
    overview = (
        "| Datasets | Description | Path |\n| --- | --- | --- |\n"
        "| [D](https://massive.com/docs/flat-files/x/d.md) | D. | `S3 stocks/1` |\n"
    )
    dataset = (
        "## Columns\n\n| Column | Type | Description |\n|---|---|---|\n"
        "| `price` | number | The price. |\n\n## Example Data\n\n| price |\n| --- |\n"
    )
    feed = "`wss://socket.massive.com/stocks`\nThe channel prefix for this feed is `1`.\n"
    snapshot = Snapshot(
        openapi={"paths": {}},
        llms="",
        pages={},
        fetched=dt.date(2026, 1, 2),
        page_urls={},
        index_url="https://massive.com/docs/llms.txt",
        # A JSON round trip gives the document the type a decoded specification has.
        websocket=json.loads(
            json.dumps(
                {"paths": {"/stocks/1": {"get": {"responses": {"200": {"content": media}}}}}}
            )
        ),
        websocket_pages={"stocks/one.md": feed},
        flat_file_pages={"x/overview.md": overview, "x/d.md": dataset},
    )
    empty = HandInput(
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
    with pytest.raises(RefreshError) as caught:
        generate.generate(snapshot, empty)
    assert [(p.gate, p.source) for p in caught.value.problems] == [("shape", "stocks/1")]
