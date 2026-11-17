"""Tests for the values `canonicalize_symbols` refuses in a live table, and their followers."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from deephaven import dtypes, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, OnError
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.problem_events import Problem, ProblemCause, ProblemSpans
from deephaven_massive._tables.refused_values import (
    ConversionRule,
    RefusedValues,
    refused_values_of,
    track_refused,
)
from deephaven_massive._tables.watchlist import SkippedValues, SymbolRowRule

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
    from deephaven.table_factory import InputTable

RULE = SymbolRowRule("crypto.trades", AssetClass.CRYPTO, as_written=False, noncanonical=OnError.LOG)


def spans_of(problems: list[Problem]) -> list[dict[str, tuple[str | None, OnError | None]]]:
    # Each snapshot as its spans' symbols and settings, by name.
    return [
        {name: (span.symbol, span.policy) for name, span in p.spans.items()}
        for p in problems
        if isinstance(p, ProblemSpans)
    ]


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def test_a_conversion_rule_accepts_what_converts_and_never_logs() -> None:
    rule = ConversionRule("canonicalize_symbols", "Symbol", {"bad": "no such pair"}.get)
    assert rule.accepts("good")
    assert not rule.accepts("bad")
    assert rule.refusal("bad") == "no such pair"
    assert rule.refusal("good") == ""
    rule.skip("bad", stream_id=None)  # logs nothing: the converter logs each value itself
    assert rule.noncanonical is OnError.LOG


def test_a_follower_gets_the_values_now_and_each_change_beside_its_own() -> None:
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    refused = RefusedValues(OnError.IGNORE, why={"^GSPC": "not a pair"}.get)
    refused.change(("^GSPC",))
    refused.follow(skipped)  # the values refused now, at once
    skipped.update(("btc",))  # the table's own skipped value joins them
    refused.change(())  # fixed or deleted
    assert spans_of(problems) == [
        {"refused ^GSPC": ("^GSPC", OnError.IGNORE)},
        {"refused ^GSPC": ("^GSPC", OnError.IGNORE), "btc": ("btc", None)},
        {"btc": ("btc", None)},
    ]
    first = problems[0]
    assert isinstance(first, ProblemSpans)
    assert first.cause is ProblemCause.NONCANONICAL_SYMBOLS
    assert first.spans["refused ^GSPC"].detail == "canonicalize_symbols refused it: not a pair"
    del skipped  # a follower that is gone gets nothing more
    refused.change(("^GSPC",))
    assert len(problems) == 3


def test_a_live_converted_table_keeps_its_refused_values_current(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Id": dtypes.string, "Symbol": dtypes.string}, key_cols="Id")
    source.add(new_table([string_col("Id", ["1", "2"]), string_col("Symbol", ["BTC-USD", "^X"])]))
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO)
    refused = refused_values_of(result)
    assert refused is not None
    assert refused_values_of(source) is None  # only the returned table is known
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    refused.follow(skipped)
    assert [list(found) for found in spans_of(problems)] == [["refused ^X"]]
    source.add(new_table([string_col("Id", ["3"]), string_col("Symbol", ["^Y"])]))
    wait_for(lambda: len(problems) == 2)
    source.add(new_table([string_col("Id", ["2"]), string_col("Symbol", ["ETH-USD"])]))  # fixed
    wait_for(lambda: len(problems) == 3)
    source.delete(new_table([string_col("Id", ["3"])]))  # deleted
    wait_for(lambda: len(problems) == 4)
    assert [list(found) for found in spans_of(problems)] == [
        ["refused ^X"],
        ["refused ^X", "refused ^Y"],
        ["refused ^Y"],
        [],
    ]
    release_scope(dh_exec_ctx, scope)


def test_a_static_or_failing_conversion_tracks_nothing(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    static = dm.canonicalize_symbols(new_table([string_col("Symbol", ["^X"])]), AssetClass.CRYPTO)
    assert refused_values_of(static) is None
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        strict = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
    assert refused_values_of(strict) is None  # a refused value fails it instead
    release_scope(dh_exec_ctx, scope)


def test_a_tracked_table_is_forgotten_with_its_values(dh_exec_ctx: ExecutionContext) -> None:
    table = new_table([string_col("Symbol", ["a"])])
    refused = RefusedValues(OnError.LOG, why=RULE.refusal)
    track_refused(table, refused)
    assert refused_values_of(table) is refused
