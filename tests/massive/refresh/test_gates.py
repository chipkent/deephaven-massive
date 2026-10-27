"""Tests for the refresh gates: the reported problems and the checks that span sources.

The cross-source checks read plain values (`PathUse`, path statuses, page names), so each is
shown here on hand-built input; `test_rest.py`, `test_ws.py`, and `test_flatfiles.py` show the
generators feeding them, and one test runs them on the vendored snapshot.
"""

from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.refresh.exclusions import Exclusion, SampleError
from deephaven_massive._massive.refresh.gates import (
    Gate,
    PathUse,
    Problem,
    RefreshError,
    check_exclusions,
    concept_collisions,
    stale_entries,
    unneeded_type_fixes,
)
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.rest import generate_rest
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
    result_keys={},
    param_units={},
    exclusions=(),
    kinds={},
    sample_errors=(),
    projections={},
    transport_only={},
    venues={},
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
                conversion=Conversion.INTEGER_ID_AS_TEXT, reason="test"
            )
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
