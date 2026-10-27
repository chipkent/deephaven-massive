"""Tests for the enums, the runtime enum check, and the enums-only rule."""

import collections.abc
import importlib
import inspect
import typing
from enum import Enum, StrEnum
from pathlib import Path

import pytest
from deephaven.table import Table
from deephaven.table_factory import InputTable
from massive.rest.models import common as sdk

import deephaven_massive as dm
from deephaven_massive import _enums
from deephaven_massive._enums import ChannelFamily, Recency, _require_enum

EXPECTED_VALUES: dict[type[StrEnum], list[str]] = {
    dm.AssetClass: ["stocks", "options", "futures", "indices", "forex", "crypto"],
    dm.Timespan: [
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "session",
    ],
    dm.PollMode: ["replace", "upsert", "append"],
    dm.HistorySource: ["auto", "rest", "flat_files"],
    dm.FuturesVenue: ["cme", "cbot", "nymex", "comex"],
    dm.Direction: ["gainers", "losers"],
    dm.ContractType: ["call", "put", "other"],
    dm.SeriesType: ["open", "high", "low", "close"],
    dm.Timeframe: ["annual", "quarterly"],
    dm.Order: ["asc", "desc"],
    dm.Market: ["stocks", "otc", "indices", "fx", "crypto"],
    dm.Locale: ["us", "global"],
    dm.ConditionDataType: ["trade", "bbo", "nbbo"],
    dm.Sip: ["CTA", "UTP", "OPRA"],
}

# Enums that mirror an SDK enum must have the same value set.
SDK_MIRRORS: dict[type[StrEnum], type[Enum]] = {
    dm.Order: sdk.Order,
    dm.Locale: sdk.Locale,
    dm.Market: sdk.Market,
    dm.Direction: sdk.Direction,
    dm.Timeframe: sdk.Timeframe,
    dm.SeriesType: sdk.SeriesType,
    dm.Sip: sdk.SIP,
    dm.ConditionDataType: sdk.DataType,
}
# reason: dm.AssetClass is not a mirror. Its values are the WebSocket cluster paths (with
# futures and forex), while the SDK's REST AssetClass has "fx" and no futures.


@pytest.mark.parametrize(("enum", "values"), EXPECTED_VALUES.items())
def test_each_exported_enum_is_a_strenum_of_wire_values(
    enum: type[StrEnum], values: list[str]
) -> None:
    assert issubclass(enum, StrEnum)
    assert [member.value for member in enum] == values
    assert all(member == member.value for member in enum)


def test_every_exported_enum_is_tested() -> None:
    exported = {
        getattr(dm, name)
        for name in dm.__all__
        if isinstance(getattr(dm, name), type) and issubclass(getattr(dm, name), Enum)
    }
    assert exported == set(EXPECTED_VALUES)


@pytest.mark.parametrize(("ours", "theirs"), SDK_MIRRORS.items())
def test_mirrored_enums_have_the_sdk_value_set(ours: type[StrEnum], theirs: type[Enum]) -> None:
    assert {m.value for m in ours} == {m.value for m in theirs}


def test_internal_enums() -> None:
    assert [m.value for m in Recency] == ["REALTIME", "DELAYED", "UNKNOWN"]
    assert [m.value for m in ChannelFamily] == ["TICK", "FMV"]
    assert not hasattr(dm, "Recency")
    assert not hasattr(dm, "ChannelFamily")


def test_require_enum_returns_members() -> None:
    assert _require_enum(dm.Timespan.MINUTE, dm.Timespan, "timespan") is dm.Timespan.MINUTE


def test_require_enum_names_the_matching_member() -> None:
    with pytest.raises(TypeError) as info:
        _require_enum("minute", dm.Timespan, "timespan")
    assert str(info.value) == "timespan must be dm.Timespan, e.g. dm.Timespan.MINUTE"


def test_require_enum_matches_names_and_ignores_case() -> None:
    with pytest.raises(TypeError, match=r"e\.g\. dm\.Sip\.OPRA$"):
        _require_enum("opra", dm.Sip, "sip")
    with pytest.raises(TypeError, match=r"e\.g\. dm\.HistorySource\.FLAT_FILES$"):
        _require_enum("FLAT_FILES", dm.HistorySource, "source")


def test_require_enum_falls_back_to_the_first_member() -> None:
    with pytest.raises(
        TypeError, match=r"^mode must be dm\.PollMode, e\.g\. dm\.PollMode\.REPLACE$"
    ):
        _require_enum("sometimes", dm.PollMode, "mode")
    with pytest.raises(TypeError, match=r"dm\.Order\.ASC$"):
        _require_enum(3, dm.Order, "order")


def test_require_enum_rejects_other_enums() -> None:
    with pytest.raises(TypeError, match=r"dm\.Market"):
        _require_enum(dm.AssetClass.STOCKS, dm.Market, "market")


def test_text_converts_with_the_constructor() -> None:
    assert dm.Timespan("minute") is dm.Timespan.MINUTE


def test_module_exports() -> None:
    assert set(_enums.__all__) == {e.__name__ for e in EXPECTED_VALUES} | {
        "Recency",
        "ChannelFamily",
        "Transport",
    }


# --- The enums-only rule: no Literal of strings in any public signature. ---------------------


def _resolution_namespace() -> dict[str, object]:
    # Names that annotations in the package refer to, including TYPE_CHECKING-only ones.
    namespace: dict[str, object] = {}
    namespace.update(vars(typing))
    namespace.update(vars(collections.abc))
    for module_name in (
        "_types",
        "_enums",
        "_errors",
        "_config",
        "_query",
        "_session",
        "_datasets.catalog",
        "_tables.liveness",
        "_datasets.symbols",
        "_datasets.dates",
        "_datasets.schema",
        "_datasets.timestamps",
        "_tables.diagnostics",
        "_namespaces.base",
        "_namespaces.partners",
        "_namespaces.stocks",
    ):
        namespace.update(vars(importlib.import_module(f"deephaven_massive.{module_name}")))
    namespace.update({"Table": Table, "InputTable": InputTable})
    return namespace


def _literal_strings(annotation: object) -> list[str]:
    # Every `str` (not enum member) inside a `Literal` anywhere in an annotation.
    found: list[str] = []
    if typing.get_origin(annotation) is typing.Literal:
        found.extend(a for a in typing.get_args(annotation) if type(a) is str)
    for arg in typing.get_args(annotation):
        found.extend(_literal_strings(arg))
    return found


def _public_callables() -> list[tuple[str, object]]:
    # Every public function and method reachable from the package and its namespaces.
    query = importlib.import_module("deephaven_massive._query")
    classes: list[type] = [dm.MassiveSession, dm.MassiveConfig]
    classes += [
        getattr(query, name)
        for name in (
            "Query",
            "StreamQuery",
            "HistoryQuery",
            "StreamHistoryQuery",
            "StreamHistoryLatestQuery",
            "HistoryLatestQuery",
        )
    ]
    seen: set[type] = set()
    found: list[tuple[str, object]] = []
    while classes:
        cls = classes.pop()
        if cls in seen:
            continue
        seen.add(cls)
        for name, member in inspect.getmembers(cls):
            if name.startswith("_") and name != "__init__":
                continue
            if isinstance(member, property):
                returned = typing.get_type_hints(member.fget, _resolution_namespace()).get("return")
                if isinstance(returned, type) and returned.__module__.startswith(
                    "deephaven_massive"
                ):
                    classes.append(returned)
                continue
            if callable(member):
                found.append((f"{cls.__qualname__}.{name}", member))
    return found


def test_public_signatures_have_no_string_literals() -> None:
    namespace = _resolution_namespace()
    callables = _public_callables()
    assert len(callables) > 10
    offenders = {}
    for name, function in callables:
        hints = typing.get_type_hints(function, namespace)
        literals = [s for hint in hints.values() for s in _literal_strings(hint)]
        if literals:
            offenders[name] = literals
    assert offenders == {}


def test_namespaces_are_reached_by_the_walk() -> None:
    names = {name.split(".")[0] for name, _ in _public_callables()}
    assert {"MassiveSession", "Query", "StreamQuery", "HistoryQuery"} <= names


def test_literal_detection_ignores_enum_members() -> None:
    assert _literal_strings(typing.Literal[dm.Timespan.DAY]) == []
    assert _literal_strings(typing.Literal["a", 1] | None) == ["a"]


# Every public parameter typed with an enum, by `Class.method.parameter`. Each one must reject a
# plain string through `_require_enum`, and each has a test that says so (named alongside). A new
# enum parameter fails `test_enum_parameters_are_the_allowlist` until it is added here together
# with its own rejection test.
ENUM_PARAMETERS = {
    "MassiveConfig.__init__.ws_hosts": "test_config.py::test_ws_hosts_types",
    "MassiveSession.snapshots.order": "test_session.py::test_unified_snapshots_shell",
    "Query.poll.mode": "test_query.py::test_poll_mode_is_an_enum",
    "HistoryQuery.history.source": "test_query.py::test_history_argument_errors",
    "StreamHistoryQuery.history.source": "test_query.py::test_history_argument_errors",
    "StreamHistoryLatestQuery.history.source": "test_query.py::test_history_argument_errors",
    "HistoryLatestQuery.history.source": "test_query.py::test_history_argument_errors",
}


def _enum_types(annotation: object) -> set[type]:
    # The StrEnum classes in an annotation, looking through unions and mapping keys.
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        return {annotation}
    found: set[type] = set()
    for arg in typing.get_args(annotation):
        found |= _enum_types(arg)
    return found


def test_enum_parameters_are_the_allowlist() -> None:
    namespace = _resolution_namespace()
    found = set()
    for name, function in _public_callables():
        hints = typing.get_type_hints(function, namespace)
        for parameter, hint in hints.items():
            if parameter != "return" and _enum_types(hint):
                found.add(f"{name}.{parameter}")
    assert found == set(ENUM_PARAMETERS)


def test_every_enum_parameter_names_an_existing_rejection_test() -> None:
    tests = Path(__file__).parent
    for location in ENUM_PARAMETERS.values():
        file_name, test_name = location.split("::")
        assert f"def {test_name}(" in (tests / file_name).read_text(), location
