"""Tests for the refresh gates: the reported problems and the checks that span sources.

The cross-source checks read plain values (`PathUse`, path statuses, page names), so each is
shown here on hand-built input; `test_rest_sources.py`, `test_ws_sources.py`, and
`test_flat_file_sources.py` show the generators feeding them, and one test runs them on the vendored
snapshot.
"""

from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.refresh.exclusions import Exclusion, IgnoredKey, SampleError
from deephaven_massive._massive.refresh.gates import (
    Gate,
    PathUse,
    Problem,
    RefreshError,
    check_exclusions,
    concept_collisions,
    field_spellings,
    stale_entries,
    unneeded_type_fixes,
    unused_description_fixes,
)
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.rest_sources import generate_rest
from deephaven_massive._massive.refresh.snapshot import DIRECTORY, Snapshot
from deephaven_massive._massive.spec.types import Conversion, Transport, WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable

_PATH = "/v1/things/{id}"


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


def _found(problems: Iterable[Problem]) -> list[tuple[str, str, str]]:
    return sorted((p.gate, p.source, p.key) for p in problems)


# --- Gates and problems ---------------------------------------------------------------------


def test_the_gate_names_are_descriptive() -> None:
    assert [gate.value for gate in Gate] == [
        "short-key",
        "column-collision",
        "timestamp-unit",
        "stale-entry",
        "type-agreement",
        "transport-agreement",
        "venue-agreement",
        "column-order",
        "concept-collision",
        "field-spelling",
        "example-type",
        "units",
        "envelope",
        "docs",
        "shape",
        "snapshot",
    ]


def test_problem_text_without_a_description() -> None:
    problem = Problem(gate=Gate.STALE_ENTRY, source="", key="", message="stale")
    assert problem.text() == "stale-entry: stale"


def test_problem_text_with_a_description_collapses_its_whitespace() -> None:
    problem = Problem(
        gate=Gate.SHORT_KEY, source="/v1/x", key="p", message="short", description="The\n  price."
    )
    assert problem.text() == "short-key /v1/x p: short\n    description: The price."


def test_a_refresh_error_sorts_its_problems_and_prints_them_all() -> None:
    second = Problem(gate=Gate.SHAPE, source="/b", key="", message="unreadable")
    first = Problem(gate=Gate.DOCS, source="/a", key="", message="undocumented")
    error = RefreshError([second, first])
    assert error.problems == (first, second)
    assert str(error).splitlines() == [
        "the refresh stopped on 2 problem(s):",
        "docs /a: undocumented",
        "shape /b: unreadable",
    ]


# --- One concept per column within a kind ---------------------------------------------------


def test_concept_collision_one_column_for_two_concepts_within_a_kind() -> None:
    uses = {
        "/a": PathUse(concepts={"LastTradePrice": "last_trade.price"}),
        "/b": PathUse(concepts={"LastTradePrice": "last.trade_price"}),
        "/c": PathUse(concepts={"LastTradePrice": "last_trade.price"}),
    }
    clash = dataclasses.replace(_EMPTY, kinds={"trades": {"stocks": ["/a"], "crypto": ["/b"]}})
    assert _found(concept_collisions(clash, uses)) == [("concept-collision", "", "LastTradePrice")]
    same = dataclasses.replace(_EMPTY, kinds={"trades": {"stocks": ["/a"], "crypto": ["/c"]}})
    assert list(concept_collisions(same, uses)) == []


def test_concept_collision_skips_a_source_that_was_not_generated() -> None:
    uses = {"/a": PathUse(concepts={"Price": "price"})}
    hand = dataclasses.replace(_EMPTY, kinds={"trades": {"stocks": ["/a", "/excluded"]}})
    assert list(concept_collisions(hand, uses)) == []


def test_concept_collision_compares_columns_only_within_one_kind() -> None:
    uses = {"/a": PathUse(concepts={"Price": "price"}), "/b": PathUse(concepts={"Price": "bid"})}
    hand = dataclasses.replace(
        _EMPTY, kinds={"trades": {"stocks": ["/a"]}, "quotes": {"stocks": ["/b"]}}
    )
    assert list(concept_collisions(hand, uses)) == []


def _projected(
    kind: str, source: str, **fields: str
) -> dict[tuple[str, str], overrides.Projection]:
    """Return a `PROJECTIONS` table of one entry."""
    return {(kind, source): overrides.Projection(fields=fields, reason="test")}


def test_concept_collision_a_projected_column_holds_its_long_name_s_concept() -> None:
    # The snapshot's `last_updated` joins the values kind as `Timestamp`, the flat file's own
    # concept: one concept in one column, and the snapshot's other fields stay out.
    uses = {
        "/snapshot": PathUse(
            concepts={"LastUpdated": "last_updated", "Name": "name"},
            columns={"last_updated": "LastUpdated", "name": "Name"},
        ),
        "file": PathUse(concepts={"Timestamp": "timestamp", "Name": "label"}),
    }
    hand = dataclasses.replace(
        _EMPTY,
        kinds={"values": {"indices": ["/snapshot", "file"]}},
        projections=_projected("values", "/snapshot", last_updated="timestamp", gone="gone"),
    )
    assert list(concept_collisions(hand, uses)) == []


def test_concept_collision_a_projected_concept_also_in_another_column() -> None:
    # The projection moves `last_updated` into `Timestamp`, while another member of the kind
    # keeps the same concept in `LastUpdated`: one concept in two columns.
    uses = {
        "/snapshot": PathUse(
            concepts={"LastUpdated": "last_updated"}, columns={"last_updated": "LastUpdated"}
        ),
        "/other": PathUse(concepts={"LastUpdated": "last_updated"}),
    }
    hand = dataclasses.replace(
        _EMPTY,
        kinds={"values": {"indices": ["/snapshot", "/other"]}},
        projections=_projected("values", "/snapshot", last_updated="timestamp"),
    )
    assert _found(concept_collisions(hand, uses)) == [("concept-collision", "", "last_updated")]


def test_concept_collision_a_renaming_projection_keeps_every_other_concept() -> None:
    # `ask` joins the quotes kind as `AskPrice`; the source's other fields keep their own
    # columns and concepts, so a peer's `Exchange` that is another concept collides.
    uses = {
        "/last": PathUse(
            concepts={"Ask": "ask", "Exchange": "exchange"},
            columns={"ask": "Ask", "exchange": "Exchange"},
        ),
        "stream": PathUse(concepts={"AskPrice": "ask_price", "Exchange": "exchange_id"}),
    }
    projection = overrides.Projection(renames={"ask": "ask_price"}, reason="test")
    hand = dataclasses.replace(
        _EMPTY,
        kinds={"quotes": {"forex": ["/last", "stream"]}},
        projections={("quotes", "/last"): projection},
    )
    assert _found(concept_collisions(hand, uses)) == [("concept-collision", "", "Exchange")]


@pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)
def test_concept_collision_passes_a_real_nested_kind() -> None:
    # The v2 stocks snapshot and the unified snapshot as one kind: `DayClose`,
    # `LastMinuteClose`, and `PrevDayClose` are distinct concepts, and the v2 `lastTrade.p`
    # and the unified `last_trade.price` are one concept in one column.
    v2, unified = "/v2/snapshot/locale/us/markets/stocks/tickers", "/v3/snapshot"
    paths, coverage = generate_rest(Snapshot.load())
    by_key = {p.key: p for p in paths}
    assert by_key[v2].field("LastTradePrice").wire_key == "lastTrade.p"
    assert by_key[unified].field("LastTradePrice").wire_key == "last_trade.price"
    assert {"DayClose", "LastMinuteClose", "PrevDayClose"} <= {f.column for f in by_key[v2].fields}
    hand = dataclasses.replace(_EMPTY, kinds={"snapshots": {"stocks": [v2], "all": [unified]}})
    assert list(concept_collisions(hand, coverage.uses)) == []


# --- Stale hand entries ---------------------------------------------------------------------


def test_stale_entry_hand_entries_naming_nothing() -> None:
    ms = overrides.Unit(unit=WireUnit.MS, reason="test")
    hand = dataclasses.replace(
        _EMPTY,
        shapes={(_PATH, "gone"): "bar", ("/gone", ""): "bar"},
        names={(_PATH, "gone"): overrides.Name(long_name="x", description="")},
        identity={"zz": "unused", "id": "used"},
        units={("/gone", "t"): ms},
        tag_conflicts={(_PATH, "gone"): "test"},
        type_fixes={
            (_PATH, "gone"): overrides.TypeFix(
                conversion=Conversion.TEXT_OR_INTEGER_ID, reason="test"
            )
        },
        key_fixes={(_PATH, "declared"): overrides.KeyFix(wire_key="sent", reason="test")},
        spellings={
            (_PATH, "details"): overrides.Spelling(long_name="", source="/v1/flat", reason="test"),
            ("/gone", "bid"): overrides.Spelling(
                long_name="bid_price", source="/v1/q", reason="test"
            ),
        },
        result_keys={"/gone": overrides.ResultKey(key=None, reason="test")},
        param_units={(_PATH, "gone"): ms},
        sample_errors=(
            SampleError(path=_PATH, wire_key="renamed", sample=1, reason="test"),
            SampleError(path="/gone", wire_key="price", sample=1, reason="test"),
        ),
    )
    uses = {_PATH: PathUse(wire_keys=frozenset({"price"}), identities=frozenset({"id"}))}
    assert _found(stale_entries(hand, uses)) == sorted(
        [
            ("stale-entry", _PATH, "gone"),  # shape
            ("stale-entry", "/gone", ""),  # shape of a path not generated
            ("stale-entry", _PATH, "gone"),  # names
            ("stale-entry", "/gone", "t"),  # units
            ("stale-entry", _PATH, "gone"),  # tag conflicts
            ("stale-entry", _PATH, "gone"),  # type fixes
            ("stale-entry", _PATH, "declared"),  # key fixes
            ("stale-entry", _PATH, "details"),  # spellings
            ("stale-entry", "/gone", "bid"),  # spellings of a path not generated
            ("stale-entry", "/gone", ""),  # result keys
            ("stale-entry", _PATH, "gone"),  # param units
            ("stale-entry", "", "zz"),  # identity
            ("stale-entry", _PATH, "renamed"),  # a sample error of a field that is gone
            ("stale-entry", "/gone", "price"),  # a sample error of a path that is gone
        ]
    )


def test_stale_entry_none_for_entries_the_paths_use() -> None:
    ms = overrides.Unit(unit=WireUnit.MS, reason="test")
    hand = dataclasses.replace(
        _EMPTY,
        shapes={(_PATH, "min"): "bar"},
        spellings={
            (_PATH, "min"): overrides.Spelling(
                long_name="last_minute", source="/v1/q", reason="test"
            ),
            (_PATH, "min.t"): overrides.Spelling(
                long_name="timestamp", source="/v1/q", reason="test"
            ),
        },
        units={(_PATH, "min.t"): ms},
        identity={"id": "used"},
        result_keys={_PATH: overrides.ResultKey(key=None, reason="test")},
        param_units={(_PATH, "timestamp"): ms},
    )
    use = PathUse(
        wire_keys=frozenset({"min.t"}),
        objects=frozenset({"", "min"}),
        unit_bases=frozenset({"timestamp"}),
        identities=frozenset({"id"}),
    )
    assert list(stale_entries(hand, {_PATH: use})) == []


# --- One name per field Massive documents alike ---------------------------------------------


def _described(**fields: tuple[str, str, str]) -> PathUse:
    """Return a source's use: each wire key (`__` for `.`) to its description, long name, column."""
    keys = {key.replace("__", "."): value for key, value in fields.items()}
    return PathUse(
        descriptions={key: text for key, (text, _, _) in keys.items()},
        long_names={key: name for key, (_, name, _) in keys.items()},
        columns={key: column for key, (_, _, column) in keys.items()},
    )


_BID = "The bid price."


def test_field_spelling_two_long_names_for_one_description() -> None:
    uses = {
        "/quotes": PathUse(),
        # An empty description proves nothing, so the two `ask` names stand.
        "/a": _described(bid_price=(_BID, "bid_price", "BidPrice"), ask=("", "ask_price", "A")),
        "/b": _described(last__bid=(_BID, "bid", "LastBid"), ask=("", "ask", "Ask")),
    }
    problems = list(field_spellings(_EMPTY, uses))
    assert _found(problems) == [("field-spelling", "", "bid | bid_price")]
    assert problems[0].description == _BID
    assert "bid (/b last.bid); bid_price (/a bid_price)" in problems[0].message
    assert "SPELLINGS" in problems[0].message
    assert "SHARED_DESCRIPTIONS" in problems[0].message


def test_field_spelling_one_wire_key_and_description_in_two_columns() -> None:
    # The long names agree; only one source drops the wrapper object's name.
    uses = {
        "/chain": _described(details__strike=("Strike.", "strike", "Strike")),
        "/unified": _described(details__strike=("Strike.", "strike", "DetailsStrike")),
        "/contracts": _described(strike=("Strike.", "strike", "Strike")),
    }
    problems = list(field_spellings(_EMPTY, uses))
    assert _found(problems) == [("field-spelling", "", "details.strike")]
    assert "DetailsStrike (/unified); Strike (/chain)" in problems[0].message


def test_field_spelling_a_renaming_projection_gives_the_name_its_table_shows() -> None:
    projection = overrides.Projection(renames={"bid": "bid_price"}, reason="test")
    hand = dataclasses.replace(_EMPTY, projections={("quotes", "/last"): projection})
    uses = {
        "/last": _described(bid=(_BID, "bid", "Bid")),
        "/quotes": _described(bid_price=(_BID, "bid_price", "BidPrice")),
    }
    assert list(field_spellings(hand, uses)) == []


def test_field_spelling_reads_every_name_a_projection_or_the_source_gives() -> None:
    # A source that joins a kind keeping some `fields` still shows its own name in its own
    # dataset; two kinds that rename one field differently give it two names.
    kept = overrides.Projection(fields={"bid": "bid_price"}, reason="test")
    hand = dataclasses.replace(_EMPTY, projections={("quotes", "/last"): kept})
    uses = {"/last": _described(bid=(_BID, "bid", "Bid"))}
    assert _found(field_spellings(hand, uses)) == [
        ("field-spelling", "", "bid"),
        ("field-spelling", "", "bid | bid_price"),
    ]
    one = overrides.Projection(renames={"bid": "bid_price"}, reason="test")
    other = overrides.Projection(renames={"bid": "best_bid"}, reason="test")
    hand = dataclasses.replace(_EMPTY, projections={("a", "/last"): one, ("b", "/last"): other})
    assert _found(field_spellings(hand, uses)) == [
        ("field-spelling", "", "best_bid | bid_price"),
        ("field-spelling", "", "bid"),
    ]


_TIME = "The Timestamp in Unix MS."
_TIMES = overrides.SharedDescription(
    long_names=frozenset({"sip_timestamp", "timestamp"}), reason="test"
)


def test_field_spelling_a_shared_description_exempts_exactly_its_long_names() -> None:
    hand = dataclasses.replace(_EMPTY, shared_descriptions={_TIME: _TIMES})
    listed = {
        "options/T": _described(t=(_TIME, "sip_timestamp", "SipTimestamp")),
        "indices/V": _described(t=(_TIME, "timestamp", "Timestamp")),
    }
    assert list(field_spellings(hand, listed)) == []
    another = {**listed, "stocks/LULD": _described(t=(_TIME, "unix_timestamp", "UnixTimestamp"))}
    problems = list(field_spellings(hand, another))
    assert _found(problems) == [
        ("field-spelling", "", "sip_timestamp | timestamp | unix_timestamp")
    ]
    assert "lists the long names ['sip_timestamp', 'timestamp']" in problems[0].message


def test_field_spelling_a_shared_description_no_field_has_is_stale() -> None:
    hand = dataclasses.replace(_EMPTY, shared_descriptions={_TIME: _TIMES})
    problems = list(field_spellings(hand, {"/a": _described(t=("Moved.", "timestamp", "T"))}))
    assert _found(problems) == [("stale-entry", "", "sip_timestamp | timestamp")]
    assert problems[0].description == _TIME


@pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)
def test_field_spelling_finds_the_second_names_the_real_spellings_remove() -> None:
    # The full refresh passes every gate with the real tables (`test_generate.py`). Without
    # them, the REST sources alone give the snapshots' bid a second name; without the unified
    # snapshot's entry alone, its strike price sits in a second column beside the chain's.
    def found(hand: HandInput) -> set[str]:
        return {p.key for p in field_spellings(hand, generate_rest(Snapshot.load(), hand)[1].uses)}

    real = HandInput()
    assert {"bid | bid_price", "ask | ask_price"} <= found(dataclasses.replace(real, spellings={}))
    partial = {k: v for k, v in real.spellings.items() if k != ("/v3/snapshot", "details")}
    assert "details.strike_price" in found(dataclasses.replace(real, spellings=partial))


# --- Description fixes no field looked up ------------------------------------------------

_FIX = overrides.DescriptionFix(vendor="Per share.", text="Per coin.", reason="test")


def test_a_description_fix_takes_effect_only_under_a_key_its_field_looked_up() -> None:
    # `declared` is a key a key fix replaces and `sent` one only an example sends: the source
    # has both, but no field looks a description up under either.
    keys = ("price", "declared", "sent", "absent")
    hand = dataclasses.replace(
        _EMPTY,
        description_fixes={**{(_PATH, key): _FIX for key in keys}, ("/gone", "price"): _FIX},
    )
    use = PathUse(
        wire_keys=frozenset({"price", "declared"}),
        example_keys=frozenset({"sent"}),
        described_keys=frozenset({"price"}),
    )
    assert _found(unused_description_fixes(hand, {_PATH: use})) == [
        ("stale-entry", "/gone", "price"),
        ("stale-entry", _PATH, "absent"),
        ("stale-entry", _PATH, "declared"),
        ("stale-entry", _PATH, "sent"),
    ]


# --- Type fixes no sample needs ---------------------------------------------------------

_SHARED = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="test")


def test_a_shared_type_fix_is_needed_while_any_member_needs_it() -> None:
    hand = dataclasses.replace(_EMPTY, type_fixes={("/a", "i"): _SHARED, ("/b", "i"): _SHARED})
    needed = PathUse(converted=frozenset({"i"}), needed_conversions=frozenset({"i"}))
    unneeded = PathUse(converted=frozenset({"i"}))
    assert list(unneeded_type_fixes(hand, {"/a": needed, "/b": unneeded})) == []
    assert _found(unneeded_type_fixes(hand, {"/a": unneeded, "/b": unneeded})) == [
        ("stale-entry", "/a", "i"),
        ("stale-entry", "/b", "i"),
    ]


def test_a_type_fix_whose_field_was_not_generated_is_left_to_other_checks() -> None:
    hand = dataclasses.replace(_EMPTY, type_fixes={("/a", "i"): _SHARED})
    assert list(unneeded_type_fixes(hand, {})) == []
    assert list(unneeded_type_fixes(hand, {"/a": PathUse(wire_keys=frozenset({"i"}))})) == []


@pytest.mark.parametrize("conversion", [c for c in Conversion if not c.widens])
def test_a_type_fix_that_only_retypes_is_never_unneeded(conversion: Conversion) -> None:
    # A sample its declared type accepts is exactly what such a fix converts, so no sample
    # ever "needs" it; it lasts while the specification declares the type.
    fix = overrides.TypeFix(conversion=conversion, reason="test")
    hand = dataclasses.replace(_EMPTY, type_fixes={("/a", "x"): fix})
    use = PathUse(converted=frozenset({"x"}))
    assert list(unneeded_type_fixes(hand, {"/a": use})) == []


def test_a_type_fix_seen_only_live_is_never_unneeded() -> None:
    # No documented sample shows the form a live response did, so none can retire the fix.
    fix = overrides.TypeFix(
        conversion=Conversion.DECIMAL_TEXT_NUMBER, reason="test", observed_live=True
    )
    hand = dataclasses.replace(_EMPTY, type_fixes={("futures/T", "p"): fix})
    use = PathUse(converted=frozenset({"p"}))
    assert list(unneeded_type_fixes(hand, {"futures/T": use})) == []


def test_ignored_keys_are_stale_once_declared_or_their_source_is_gone() -> None:
    hand = dataclasses.replace(
        _EMPTY,
        ignored_keys=(
            IgnoredKey(source="forex/C", wire_key="i", reason="test"),
            IgnoredKey(source="forex/C", wire_key="x", reason="test"),
            IgnoredKey(source="forex/GONE", wire_key="i", reason="test"),
        ),
    )
    uses = {"forex/C": PathUse(wire_keys=frozenset({"x"}))}
    problems = list(stale_entries(hand, uses))
    assert _found(problems) == [
        ("stale-entry", "forex/C", "x"),
        ("stale-entry", "forex/GONE", "i"),
    ]
    assert sorted(p.message for p in problems) == [
        "IGNORED_KEYS entry for a declared key",
        "IGNORED_KEYS entry for a source the snapshot does not generate",
    ]


def test_fixes_written_separately_are_separate_decisions_however_alike() -> None:
    # Equal values, separate objects: one path's need does not keep the other's fix alive.
    first = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="same")
    second = overrides.TypeFix(conversion=Conversion.TEXT_OR_INTEGER_ID, reason="same")
    assert first == second
    hand = dataclasses.replace(_EMPTY, type_fixes={("/a", "i"): first, ("/b", "i"): second})
    needed = PathUse(converted=frozenset({"i"}), needed_conversions=frozenset({"i"}))
    unneeded = PathUse(converted=frozenset({"i"}))
    assert _found(unneeded_type_fixes(hand, {"/a": needed, "/b": unneeded})) == [
        ("stale-entry", "/b", "i")
    ]


# --- Exclusions -----------------------------------------------------------------------------


def _excluding(
    item: str, *, undocumented: bool = False, kind: Transport = Transport.REST
) -> HandInput:
    entry = Exclusion(kind=kind, item=item, reason="test", undocumented=undocumented)
    return dataclasses.replace(_EMPTY, exclusions=(entry,))


@pytest.mark.parametrize("status", ["deprecated", "draft"])
def test_stale_entry_an_exclusion_the_specification_already_accounts_for(status: str) -> None:
    problems = check_exclusions(_excluding(_PATH), {_PATH: status}, {_PATH: ("page.md",)})
    assert _found(problems) == [("stale-entry", _PATH, "")]


def test_stale_entry_an_exclusion_the_specification_lacks() -> None:
    problems = check_exclusions(_excluding("/gone"), {_PATH: ""}, {})
    assert _found(problems) == [("stale-entry", "/gone", "")]


def test_docs_an_undocumented_exclusion_that_is_documented() -> None:
    hand = _excluding(_PATH, undocumented=True)
    assert _found(check_exclusions(hand, {_PATH: ""}, {_PATH: ("page.md",)})) == [
        ("docs", _PATH, "")
    ]
    assert list(check_exclusions(hand, {_PATH: ""}, {})) == []


def test_docs_a_documented_exclusion_that_no_page_names() -> None:
    hand = _excluding(_PATH)
    assert _found(check_exclusions(hand, {_PATH: ""}, {})) == [("docs", _PATH, "")]
    assert list(check_exclusions(hand, {_PATH: ""}, {_PATH: ("page.md",)})) == []


def test_exclusions_of_other_transports_are_not_checked_here() -> None:
    hand = _excluding("stocks/LV", kind=Transport.WS)
    assert list(check_exclusions(hand, {_PATH: ""}, {})) == []
