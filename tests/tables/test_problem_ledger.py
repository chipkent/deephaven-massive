"""Tests for the rows of `m.problems()`: merging, spans, the cap, queries, and redaction."""

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import Redactor
from deephaven_massive._tables import problem_ledger
from deephaven_massive._tables.problem_events import (
    ProblemAction,
    ProblemCause,
    ProblemEvent,
    ProblemSource,
    ProblemSpans,
    Span,
)
from deephaven_massive._tables.problem_ledger import (
    PROBLEM_KEYS,
    PROBLEMS,
    ProblemLedger,
    ProblemRow,
    problem_values,
)

KEY = "sekrit-key"
"""The session's API key, which no row may show."""


def event(
    *,
    symbol: str | None = "AAPL",
    rows: int = 2,
    at: int = 100,
    detail: str = "Price holds 'x'",
    stream_id: int | None = 7,
) -> ProblemEvent:
    return ProblemEvent(
        cause=ProblemCause.UNCONVERTIBLE_VALUES,
        action=ProblemAction.WITHHELD,
        dataset="stocks.trades",
        stream_id=stream_id,
        where="REST GET /v3/trades",
        symbol=symbol,
        field="Price",
        rows=rows,
        detail=detail,
        at=at,
        policy=dm.OnError.LOG,
    )


def snapshot(at: int, spans: dict[str, Span], *, stream_id: int | None = 7) -> ProblemSpans:
    return ProblemSpans(
        cause=ProblemCause.OUTAGE,
        dataset="stocks.trades",
        stream_id=stream_id,
        where="wss://h/stocks",
        spans=spans,
        at=at,
    )


@pytest.fixture
def ledger() -> ProblemLedger:
    return ProblemLedger(Redactor(KEY))


def test_the_schema_holds_the_key_columns() -> None:
    assert PROBLEMS.column_names[: len(PROBLEM_KEYS)] == PROBLEM_KEYS


def test_events_of_one_key_add_up_in_one_row(ledger: ProblemLedger) -> None:
    [first] = ledger.apply([event()])
    assert (first.rows, first.occurrences, first.first_seen, first.last_seen) == (2, 1, 100, 100)
    [again] = ledger.apply([event(rows=3, at=200, detail="later")])
    assert again.key == first.key
    assert (again.rows, again.occurrences, again.first_seen, again.last_seen) == (5, 2, 100, 200)
    assert again.detail == "Price holds 'x'"  # the first problem's reason
    [other] = ledger.apply([event(symbol="MSFT")])
    assert other.key != first.key


def test_a_batch_returns_one_row_per_key(ledger: ProblemLedger) -> None:
    rows = ledger.apply([event(), event(at=150), event(symbol="MSFT")])
    assert [(row.symbol, row.occurrences) for row in rows] == [("AAPL", 2), ("MSFT", 1)]


def test_values_follow_the_columns(ledger: ProblemLedger) -> None:
    [row] = ledger.apply([event()])
    values = problem_values(row)
    assert list(values) == list(PROBLEMS.column_names)
    assert values["Cause"] == "unconvertible_values"
    assert values["Action"] == "WITHHELD"
    assert values["Policy"] == "log"
    assert values["Source"] == ProblemSource.LIBRARY
    assert values["SpanStart"] is None
    assert values["EndedAt"] is None


def test_a_snapshot_opens_updates_and_ends_spans(ledger: ProblemLedger) -> None:
    [opened] = ledger.apply([snapshot(100, {"outage": Span(detail="down", since=60)})])
    assert (opened.span_start, opened.first_seen, opened.ended_at) == (60, 60, None)
    assert opened.action is ProblemAction.GAP
    [same] = ledger.apply([snapshot(150, {"outage": Span(detail="still down", since=60)})])
    assert (same.key, same.detail, same.last_seen, same.ended_at) == (
        opened.key,
        "still down",
        150,
        None,
    )
    [ended] = ledger.apply([snapshot(200, {})])
    assert (ended.key, ended.ended_at) == (opened.key, 200)
    assert ledger.apply([snapshot(250, {})]) == []
    [second] = ledger.apply([snapshot(300, {"outage": Span(detail="down again", since=290)})])
    assert second.key != opened.key  # a new span, its own row
    [replaced, renewed] = ledger.apply([snapshot(400, {"outage": Span(detail="x", since=390)})])
    assert (replaced.key, replaced.ended_at) == (second.key, 400)
    assert renewed.span_start == 390


def test_a_span_that_ended_can_hold_again_from_the_same_start(ledger: ProblemLedger) -> None:
    ledger.apply([snapshot(100, {"outage": Span(detail="down", since=60)})])
    ledger.apply([snapshot(200, {})])
    held = Span(detail="down; waiting for a slot", since=60, policy=dm.OnError.IGNORE)
    [again] = ledger.apply([snapshot(300, {"outage": held})])
    assert (again.span_start, again.ended_at, again.last_seen) == (60, None, 300)
    assert (again.detail, again.policy) == ("down; waiting for a slot", dm.OnError.IGNORE)


def test_a_table_that_ended_ends_its_open_spans(ledger: ProblemLedger) -> None:
    ledger.apply([snapshot(100, {"outage": Span(detail="down", since=60)})])
    ledger.apply([snapshot(100, {"outage": Span(detail="down", since=60)}, stream_id=8)])
    [ended] = ledger.stream_ended(7, 500)
    assert (ended.stream_id, ended.ended_at) == (7, 500)
    assert ledger.stream_ended(7, 600) == []
    # A snapshot that comes after the end adds its new span already ended, and nothing for
    # the span the table showed before it ended.
    late = {"outage": Span(detail="down", since=60), "other": Span(detail="late", since=650)}
    [added] = ledger.apply([snapshot(700, late)])
    assert (added.detail, added.span_start, added.ended_at) == ("late", 650, 700)
    assert ledger.apply([snapshot(800, late)]) == []


def test_a_query_learned_later_fills_the_earlier_rows(ledger: ProblemLedger) -> None:
    [row] = ledger.apply([event()])
    assert row.query is None
    [filled] = ledger.stream_seen(7, f"m.stocks.trades('{KEY}')")
    assert filled.query == "m.stocks.trades('***')"
    assert ledger.stream_seen(7, "other") == []  # the first query stays
    [later] = ledger.apply([event(symbol="MSFT")])
    assert later.query == "m.stocks.trades('***')"


def test_every_text_is_redacted(ledger: ProblemLedger) -> None:
    [row] = ledger.apply([event(symbol=f"x{KEY}", detail=f"refused key {KEY}")])
    assert row.symbol == "x***"
    assert row.detail == "refused key ***"
    [span] = ledger.apply([snapshot(1, {"a": Span(symbol=KEY, detail=KEY, since=1)})])
    assert (span.symbol, span.detail) == ("***", "***")


def test_past_the_cap_problems_add_up_in_one_overflow_row(
    ledger: ProblemLedger, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(problem_ledger, "KEYS_PER_CAUSE", 2)
    ledger.apply([event(symbol="A"), event(symbol="B")])
    [overflow] = ledger.apply([event(symbol="C", rows=4)])
    assert (overflow.symbol, overflow.field, overflow.action, overflow.rows) == (
        None,
        None,
        ProblemAction.OVERFLOW,
        4,
    )
    [more] = ledger.apply([event(symbol="D", rows=1)])
    assert (more.key, more.rows, more.occurrences) == (overflow.key, 5, 2)
    [known] = ledger.apply([event(symbol="A")])  # a kept key still adds up in its own row
    assert known.symbol == "A"
    [other] = ledger.apply([event(symbol="Z", stream_id=8)])  # the cap is per table
    assert other.symbol == "Z"


def test_past_the_cap_spans_add_up_in_the_overflow_row(
    ledger: ProblemLedger, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(problem_ledger, "KEYS_PER_CAUSE", 1)
    spans = {"a": Span(detail="a", since=1), "b": Span(detail="b", since=2, rows=3)}
    kept, overflow = ledger.apply([snapshot(10, spans)])
    assert kept.span_start == 1
    assert (overflow.action, overflow.span_start, overflow.rows) == (
        ProblemAction.OVERFLOW,
        None,
        3,
    )
    # A counted span is counted once while it holds, and again once it holds anew.
    [still] = ledger.apply([snapshot(20, spans)])
    assert (still.key, still.last_seen) == (kept.key, 20)
    [ended] = ledger.apply([snapshot(30, {})])
    assert (ended.key, ended.ended_at) == (kept.key, 30)
    [again] = ledger.apply([snapshot(40, {"b": Span(detail="b", since=35, rows=3)})])
    assert (again.key, again.occurrences, again.rows) == (overflow.key, 2, 6)
    # A counted span that renews from a new start without ending first counts again too.
    [renewed] = ledger.apply([snapshot(50, {"b": Span(detail="b", since=45, rows=3)})])
    assert (renewed.key, renewed.occurrences, renewed.rows) == (overflow.key, 3, 9)
    assert ledger.apply([snapshot(60, {"b": Span(detail="b", since=45, rows=3)})]) == []


def test_a_row_is_frozen_and_slotted() -> None:
    row = ProblemRow(
        first_seen=1,
        last_seen=1,
        dataset=None,
        stream_id=None,
        symbol=None,
        field=None,
        cause=ProblemCause.RATE_LIMITED,
        action=ProblemAction.DELAYED,
        rows=0,
        policy=None,
        detail="",
    )
    assert not hasattr(row, "__dict__")
    assert row.key == (None, ProblemCause.RATE_LIMITED, None, None, None)


def test_an_earlier_event_that_arrives_later_moves_first_seen(ledger: ProblemLedger) -> None:
    ledger.apply([event(at=200)])
    [row] = ledger.apply([event(at=100)])
    assert (row.first_seen, row.last_seen, row.occurrences) == (100, 200, 2)


def test_a_late_span_past_the_cap_adds_up_in_the_overflow_row(
    ledger: ProblemLedger, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(problem_ledger, "KEYS_PER_CAUSE", 1)
    ledger.apply([snapshot(10, {"a": Span(detail="a", since=1)})])
    ledger.stream_ended(7, 20)
    [overflow] = ledger.apply([snapshot(30, {"b": Span(detail="b", since=25, rows=2)})])
    assert (overflow.action, overflow.span_start, overflow.rows) == (
        ProblemAction.OVERFLOW,
        None,
        2,
    )
