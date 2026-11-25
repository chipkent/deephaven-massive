"""Tests for the values `canonicalize_symbols` refuses in a live table, and their followers."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from deephaven import dtypes, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, OnError
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.problem_events import (
    Problem,
    ProblemAction,
    ProblemCause,
    ProblemSpans,
)
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
        {"refused None '^GSPC'": ("^GSPC", OnError.IGNORE)},
        {"refused None '^GSPC'": ("^GSPC", OnError.IGNORE), "skipped None 'btc'": ("btc", None)},
        {"skipped None 'btc'": ("btc", None)},
    ]
    first = problems[0]
    assert isinstance(first, ProblemSpans)
    assert first.cause is ProblemCause.NONCANONICAL_SYMBOLS
    assert (
        first.spans["refused None '^GSPC'"].detail == "canonicalize_symbols refused it: not a pair"
    )
    del skipped  # a follower that is gone gets nothing more
    refused.change(("^GSPC",))
    assert len(problems) == 3


def test_a_follower_that_is_gone_is_dropped_when_another_follows() -> None:
    # Readers made and released while the values never change leave no entry behind.
    refused = RefusedValues(OnError.LOG, why={"^X": "no"}.get)
    reported: list[Problem] = []
    for _ in range(5):
        refused.follow(SkippedValues(RULE, stream_id=7, report=reported.extend))
    keep = SkippedValues(RULE, stream_id=8, report=reported.extend)
    refused.follow(keep)
    assert len(refused._followers) == 1


def test_a_table_of_values_is_a_source_of_its_own_named_by_its_column() -> None:
    # A poll that reads a converted symbol table and a converted table of values: each table's
    # refused values come and go alone, and the values table's spans name its column.
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    symbols = RefusedValues(OnError.LOG, why={"^X": "not a pair"}.get)
    values = RefusedValues(OnError.IGNORE, why={"no pe!": "not a ticker"}.get)
    symbols.change(("^X",))
    symbols.follow(skipped)
    values.follow(skipped, field="Symbol")  # nothing refused there yet: no new snapshot
    values.change(("no pe!",))  # refused
    values.change(())  # fixed, or its row deleted: the symbol table's span holds on
    symbols.change(())
    assert spans_of(problems) == [
        {"refused None '^X'": ("^X", OnError.LOG)},
        {
            "refused None '^X'": ("^X", OnError.LOG),
            "refused 'Symbol' 'no pe!'": ("no pe!", OnError.IGNORE),
        },
        {"refused None '^X'": ("^X", OnError.LOG)},
        {},
    ]
    spans = problems[1]
    assert isinstance(spans, ProblemSpans)
    assert (
        spans.spans["refused None '^X'"].field,
        spans.spans["refused 'Symbol' 'no pe!'"].field,
    ) == (
        None,
        "Symbol",
    )


def test_one_converted_table_followed_as_both_symbols_and_values_is_two_sources() -> None:
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    both = RefusedValues(OnError.LOG, why={"^X": "not a pair"}.get)
    both.follow(skipped)
    both.follow(skipped, field="Symbol")
    both.change(("^X",))
    both.change(())
    # Each source takes the change in turn, so neither hides the other's span.
    assert [sorted(found) for found in spans_of(problems)] == [
        ["refused None '^X'"],
        ["refused 'Symbol' '^X'", "refused None '^X'"],
        ["refused 'Symbol' '^X'"],
        [],
    ]


def test_two_sources_whose_values_read_alike_keep_a_span_each() -> None:
    # The symbol table refuses "Symbol x" and the table of values in column Symbol refuses
    # "x": each is its own span, whatever text the values hold.
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    RefusedValues(OnError.LOG, why={"Symbol x": "no"}.get).follow(skipped)
    values = RefusedValues(OnError.LOG, why={"x": "no"}.get)
    values.follow(skipped, field="Symbol")
    values.change(("x",))
    symbols = RefusedValues(OnError.LOG, why={"Symbol x": "no"}.get)
    symbols.change(("Symbol x",))
    symbols.follow(skipped)
    last = problems[-1]
    assert isinstance(last, ProblemSpans)
    assert sorted((span.symbol, span.field) for span in last.spans.values()) == [
        ("Symbol x", None),
        ("x", "Symbol"),
    ]


def test_a_live_converted_table_keeps_its_refused_values_current(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Id": dtypes.string, "Symbol": dtypes.string}, key_cols="Id")
    source.add(new_table([string_col("Id", ["1", "2"]), string_col("Symbol", ["BTC-USD", "^X"])]))
    scope = new_scope()
    with scope.open():
        result = dm.canonicalize_symbols(source, AssetClass.CRYPTO)
    refused = refused_values_of(result, "Symbol")
    assert refused is not None
    assert refused_values_of(source, "Symbol") is None  # only the returned table is known
    assert refused_values_of(result, "ProductCode") is None  # nor any column it didn't convert
    problems: list[Problem] = []
    skipped = SkippedValues(RULE, stream_id=7, report=problems.extend)
    refused.follow(skipped)
    assert [list(found) for found in spans_of(problems)] == [["refused None '^X'"]]
    source.add(new_table([string_col("Id", ["3"]), string_col("Symbol", ["^Y"])]))
    wait_for(lambda: len(problems) == 2)
    source.add(new_table([string_col("Id", ["2"]), string_col("Symbol", ["ETH-USD"])]))  # fixed
    wait_for(lambda: len(problems) == 3)
    source.delete(new_table([string_col("Id", ["3"])]))  # deleted
    wait_for(lambda: len(problems) == 4)
    assert [list(found) for found in spans_of(problems)] == [
        ["refused None '^X'"],
        ["refused None '^X'", "refused None '^Y'"],
        ["refused None '^Y'"],
        [],
    ]
    release_scope(dh_exec_ctx, scope)


def test_a_static_conversion_keeps_its_refused_values_once() -> None:
    # A static input never changes: its refused values are the ones found at the conversion,
    # and a one-time read lists each as a problem of its call.
    static = dm.canonicalize_symbols(
        new_table([string_col("Symbol", ["^X", "X:BTCUSD"])]), AssetClass.CRYPTO
    )
    refused = refused_values_of(static, "Symbol")
    assert refused is not None
    problems: list[Problem] = []
    refused.follow(SkippedValues(RULE, stream_id=7, report=problems.extend))
    assert [list(found) for found in spans_of(problems)] == [["refused None '^X'"]]
    [event] = refused.events(dataset="crypto.trades", stream_id=9, where="w", field="Symbol")
    assert (event.cause, event.action, event.symbol, event.field, event.stream_id) == (
        ProblemCause.NONCANONICAL_SYMBOLS,
        ProblemAction.SKIPPED,
        "^X",
        "Symbol",
        9,
    )
    # The converter's reason alone: the row's Cause leads to the fix.
    assert event.detail == "canonicalize_symbols refused it: '^X' is not a crypto pair."
    assert event.policy is OnError.LOG


def test_a_failing_conversion_tracks_nothing(
    dh_exec_ctx: ExecutionContext, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        strict = dm.canonicalize_symbols(source, AssetClass.CRYPTO, on_error=OnError.RAISE)
    assert refused_values_of(strict, "Symbol") is None  # a refused value fails it instead
    release_scope(dh_exec_ctx, scope)


def test_a_tracked_table_is_forgotten_with_its_values(dh_exec_ctx: ExecutionContext) -> None:
    table = new_table([string_col("Symbol", ["a"])])
    refused = RefusedValues(OnError.LOG, why=RULE.refusal)
    track_refused(table, refused, column="Symbol")
    assert refused_values_of(table, "Symbol") is refused
