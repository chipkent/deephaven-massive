"""Tests for the plain-data problems, their causes, span snapshots, and drift events."""

import dataclasses

import deephaven_massive as dm
from deephaven_massive._error_policy import ErrorPolicy
from deephaven_massive._massive import DriftReport, DriftSample, RowFault
from deephaven_massive._tables.problem_events import (
    CAUSES,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    ProblemSource,
    Span,
    SpanTracker,
    drift_events,
)

EVENT = ProblemEvent(
    cause=ProblemCause.UNCONVERTIBLE_VALUES,
    action=ProblemAction.WITHHELD,
    dataset="stocks.trades",
    stream_id=None,
    where="REST GET /v3/trades/{stockTicker}",
    symbol="AAPL",
    field="Price",
    rows=2,
    detail="Price holds 'x', not a double",
    at=1,
    policy=dm.OnError.LOG,
)


def test_an_event_is_frozen_and_slotted() -> None:
    assert hash(EVENT) == hash(dataclasses.replace(EVENT))  # frozen, so hashable by value
    assert not hasattr(EVENT, "__dict__")


def test_every_cause_has_traits_and_names_a_real_policy_field() -> None:
    assert list(CAUSES) == list(ProblemCause)
    fields = {f.name for f in dataclasses.fields(ErrorPolicy)}
    for cause in ProblemCause:
        traits = cause.traits
        assert traits.policy is None or traits.policy == cause.value
        assert traits.policy is None or traits.policy in fields
        assert traits.summary.endswith(".")
    # Every field of the policy is a cause, so `Policy` always names its own cause's setting.
    assert fields <= {cause.value for cause in ProblemCause}


def test_every_row_fault_is_a_withheld_cause_of_massive_s_data() -> None:
    for fault in RowFault:
        cause = ProblemCause(fault.value)
        assert cause.traits.action is ProblemAction.WITHHELD
        assert cause.traits.source is ProblemSource.MASSIVE


def test_the_fix_for_massive_s_unreadable_rows_names_the_setting_not_a_bug_report() -> None:
    # The rows are Massive's data: the fix says so and names the setting, and never asks for a
    # bug report to the library.
    fixes = {
        cause: CAUSES[cause].fix
        for cause in (
            ProblemCause.UNREADABLE_SYMBOLS,
            ProblemCause.UNREADABLE_KEYS,
            ProblemCause.UNCONVERTIBLE_VALUES,
            ProblemCause.MALFORMED_ROWS,
        )
    }
    assert fixes == {
        **{
            cause: "The library withheld the row because Massive sent it in a form the library "
            "can't read. `Detail` gives what Massive sent and why, for a report to Massive. To "
            f"quiet it, set `{cause.value}` to `dm.OnError.IGNORE`; to fail the table, "
            "`dm.OnError.RAISE`."
            for cause in (
                ProblemCause.UNREADABLE_SYMBOLS,
                ProblemCause.UNREADABLE_KEYS,
                ProblemCause.UNCONVERTIBLE_VALUES,
            )
        },
        ProblemCause.MALFORMED_ROWS: "The library skipped the line because Massive sent it in a "
        "form the library can't read. `Detail` names the file and its first such line, for a "
        "report to Massive. To quiet it, set `malformed_rows` to `dm.OnError.IGNORE`; to fail "
        "the read, `dm.OnError.RAISE`.",
    }


def test_a_span_tracker_keeps_each_span_s_start_and_reports_only_changes() -> None:
    tracker = SpanTracker(
        ProblemCause.NONCANONICAL_SYMBOLS, dataset="stocks.trades", stream_id=7, where="here"
    )
    before = tracker.holds
    first = tracker.update({"aapl": ("aapl", None, "bad", 0)}, at=10)
    assert first is not None
    assert (before, tracker.holds) == (False, True)
    assert first.spans == {"aapl": Span(symbol="aapl", detail="bad", since=10)}
    assert (first.cause, first.stream_id, first.at) == (ProblemCause.NONCANONICAL_SYMBOLS, 7, 10)
    assert tracker.update({"aapl": ("aapl", None, "bad", 0)}, at=20) is None  # unchanged
    both = tracker.update(
        {"aapl": ("aapl", None, "bad", 0), "msft!": ("msft!", None, "worse", 0)}, at=30
    )
    assert both is not None
    assert {name: span.since for name, span in both.spans.items()} == {"aapl": 10, "msft!": 30}
    ended = tracker.update({}, at=40)
    assert ended is not None
    assert (ended.spans, ended.at) == ({}, 40)
    assert tracker.update({}, at=50) is None


def test_a_span_tracker_takes_a_start_the_emitter_knows() -> None:
    tracker = SpanTracker(ProblemCause.OUTAGE, dataset="d", stream_id=1, where="w")
    spans = tracker.update({"outage": (None, None, "down", 0)}, at=100, since=60)
    assert spans is not None
    assert spans.spans["outage"].since == 60
    later = tracker.update({"outage": (None, None, "down; waiting", 0)}, at=200, since=150)
    assert later is not None
    assert later.spans["outage"].since == 60  # a span that still holds keeps its start
    assert tracker.update({"outage": (None, None, "down; waiting", 0)}) is None


def test_drift_events_report_only_fields_the_documentation_lacks() -> None:
    drift = DriftReport(
        extra=frozenset({"z", "halted"}),
        missing=frozenset({"m"}),
        unconvertible=frozenset({"x"}),
        samples={"halted": DriftSample("AAPL", "true")},
    )
    found = drift_events(drift, dataset="stocks.trades", stream_id=7, where="GET /v3/trades")
    assert [(e.cause, e.action, e.field, e.symbol, e.stream_id) for e in found] == [
        (ProblemCause.SCHEMA_DRIFT, ProblemAction.PUBLISHED, "halted", None, 7),
        (ProblemCause.SCHEMA_DRIFT, ProblemAction.PUBLISHED, "z", None, 7),
    ]
    assert found[0].detail == (
        "Massive sent the field 'halted', which its documentation lacks; the rows are "
        "published without it; first sent for AAPL; first value true"
    )
    assert found[1].detail.endswith("published without it")
    missing = DriftReport(missing=frozenset({"m"}))
    assert drift_events(missing, dataset="d", stream_id=None, where="w") == ()


def test_the_causes_are_pinned() -> None:
    # Each value is a cell users query: a change here is a public change.
    assert [cause.value for cause in ProblemCause] == [
        "noncanonical_symbols",
        "unreadable_symbols",
        "unavailable_symbols",
        "unreadable_keys",
        "unconvertible_values",
        "malformed_rows",
        "refused_venues",
        "unlisted_contracts",
        "unscheduled_products",
        "undated_rows",
        "stale_symbol_data",
        "schema_drift",
        "outage",
        "quiet_feed",
        "options_cap",
        "unacknowledged_topics",
        "rate_limited",
        "poll_cycle_failed",
        "poll_cycle_late",
        "symbol_read_retrying",
        "symbol_read_failed",
        "stream_failed",
    ]
    assert [action.value for action in ProblemAction] == [
        "WITHHELD",
        "SKIPPED",
        "FAILED",
        "GAP",
        "DELAYED",
        "PUBLISHED",
        "OVERFLOW",
    ]
