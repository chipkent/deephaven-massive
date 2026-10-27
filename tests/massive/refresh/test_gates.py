"""Tests for the refresh gates: the reported problems and the checks that span paths.

The cross-path checks read plain values (`PathUse`, path statuses, page names), so each is
shown here on hand-built input; `test_rest.py` shows the generator feeding them.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.refresh.exclusions import Exclusion
from deephaven_massive._massive.refresh.gates import (
    Gate,
    PathUse,
    Problem,
    RefreshError,
    check_exclusions,
    concept_collisions,
    stale_entries,
)
from deephaven_massive._massive.refresh.hand import HandInput
from deephaven_massive._massive.spec.types import Transport, WireType, WireUnit

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
        "concept-collision",
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
                declared=WireType.LONG, wire_type=WireType.STRING, reason="test"
            )
        },
        result_keys={"/gone": overrides.ResultKey(key=None, reason="test")},
        param_units={(_PATH, "gone"): ms},
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


# --- Exclusions -----------------------------------------------------------------------------


def _excluding(
    item: str, *, undocumented: bool = False, kind: Transport = Transport.REST
) -> HandInput:
    entry = Exclusion(kind=kind, item=item, reason="test", undocumented=undocumented)
    return dataclasses.replace(_EMPTY, exclusions=(entry,))


@pytest.mark.parametrize("status", ["deprecated", "draft"])
def test_stale_entry_an_exclusion_the_specification_already_accounts_for(status: str) -> None:
    problems = check_exclusions(_excluding(_PATH), {_PATH: status}, {_PATH: "page.md"})
    assert _found(problems) == [("stale-entry", _PATH, "")]


def test_stale_entry_an_exclusion_the_specification_lacks() -> None:
    problems = check_exclusions(_excluding("/gone"), {_PATH: ""}, {})
    assert _found(problems) == [("stale-entry", "/gone", "")]


def test_docs_an_undocumented_exclusion_that_is_documented() -> None:
    hand = _excluding(_PATH, undocumented=True)
    assert _found(check_exclusions(hand, {_PATH: ""}, {_PATH: "page.md"})) == [("docs", _PATH, "")]
    assert list(check_exclusions(hand, {_PATH: ""}, {})) == []


def test_docs_a_documented_exclusion_that_no_page_names() -> None:
    hand = _excluding(_PATH)
    assert _found(check_exclusions(hand, {_PATH: ""}, {})) == [("docs", _PATH, "")]
    assert list(check_exclusions(hand, {_PATH: ""}, {_PATH: "page.md"})) == []


def test_exclusions_of_other_transports_are_not_checked_here() -> None:
    hand = _excluding("stocks/LV", kind=Transport.WS)
    assert list(check_exclusions(hand, {_PATH: ""}, {})) == []
