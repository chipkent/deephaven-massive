"""Tests for each symbol of a `poll()` table: which ones a cycle reads, and what a failure does."""

from __future__ import annotations

import threading

import pytest

from deephaven_massive._enums import OnError
from deephaven_massive._errors import EntitlementError, MassiveError
from deephaven_massive._tables.poll_symbols import DroppedNames, PollSymbols, Settled, _is_due
from deephaven_massive._tables.problem_events import ProblemCause
from deephaven_massive._tables.symbol_read_rows import SymbolReadState
from deephaven_massive._tables.symbol_retry import RETRY_DELAYS, SymbolRun

LIMIT = 300.0
"""The default `max_outage`, in seconds."""

AT = 1_000
"""An epoch-nanosecond time for the spans."""


def busy() -> MassiveError:
    return MassiveError("Massive answered GET /x with HTTP 503", status=503)


def refused() -> EntitlementError:
    return EntitlementError("Massive answered GET /x with HTTP 403", status=403)


def names(*, field: str | None = None) -> PollSymbols:
    return PollSymbols(
        dataset="stocks.dividends",
        stream_id=7,
        field=field,
        refused_symbols=OnError.LOG,
        limit=LIMIT,
    )


def spans_by_cause(settled: Settled) -> dict[ProblemCause, set[str]]:
    return {spans.cause: set(spans.spans) for spans in settled.spans}


# --- DroppedNames -------------------------------------------------------------------------------


def test_dropped_names_keep_each_removed_symbol_until_taken() -> None:
    dropped = DroppedNames()
    dropped.on_change(frozenset({"AAPL"}), frozenset())
    assert dropped.take() == frozenset()
    dropped.on_change(frozenset(), frozenset({"MSFT"}))
    dropped.on_change(frozenset({"MSFT"}), frozenset({"IBM"}))  # MSFT added back: still dropped
    assert dropped.take() == {"MSFT", "IBM"}
    assert dropped.take() == frozenset()


def test_dropped_names_take_changes_from_another_thread() -> None:
    dropped = DroppedNames()

    def drop(n: int) -> None:
        for i in range(200):
            dropped.on_change(frozenset(), frozenset({f"S{n}_{i}"}))

    threads = [threading.Thread(target=drop, args=(n,)) for n in range(4)]
    taken: set[str] = set()
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        taken |= dropped.take()
    for thread in threads:
        thread.join()
    taken |= dropped.take()
    assert len(taken) == 800


# --- due ----------------------------------------------------------------------------------------


def test_every_listed_symbol_is_due_at_first() -> None:
    symbols = names()
    assert symbols.due(("AAPL", "MSFT"), now=0.0) == ("AAPL", "MSFT")


def test_a_failed_symbol_is_not_due_and_a_retrying_one_is_due_once_its_retry_is() -> None:
    symbols = names()
    symbols.due(("AAPL", "IBM", "MSFT"), now=0.0)
    symbols.settle(
        ("AAPL", "IBM", "MSFT"), {"AAPL": refused(), "IBM": busy()}, now=0.0, at=AT, since=AT
    )
    assert symbols.due(("AAPL", "IBM", "MSFT"), now=RETRY_DELAYS[0] - 0.1) == ("MSFT",)
    assert symbols.due(("AAPL", "IBM", "MSFT"), now=RETRY_DELAYS[0]) == ("IBM", "MSFT")


def test_a_symbol_that_left_the_list_is_read_afresh_when_it_comes_back() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {"MSFT": refused()}, now=0.0, at=AT, since=AT)
    assert symbols.due(("AAPL",), now=1.0) == ("AAPL",)
    assert symbols.due(("AAPL", "MSFT"), now=2.0) == ("AAPL", "MSFT")


def test_a_symbol_dropped_and_added_back_between_two_cycles_is_read_afresh() -> None:
    dropped = DroppedNames()
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {"MSFT": refused()}, now=0.0, at=AT, since=AT)
    assert symbols.due(("AAPL", "MSFT"), now=1.0) == ("AAPL",)
    # The watch is a Deephaven listener; its `on_change` is what a delete and re-add reports.
    symbols._dropped = dropped
    dropped.on_change(frozenset(), frozenset({"MSFT"}))
    dropped.on_change(frozenset({"MSFT"}), frozenset())
    assert symbols.due(("AAPL", "MSFT"), now=2.0) == ("AAPL", "MSFT")


def test_a_poll_without_a_watch_has_none_to_stop() -> None:
    names().stop_watch()  # no error: nothing to stop


# --- holds_keys ---------------------------------------------------------------------------------


def test_only_a_retrying_listed_symbol_holds_its_keys() -> None:
    symbols = names()
    symbols.due(("AAPL", "IBM", "MSFT"), now=0.0)
    symbols.settle(
        ("AAPL", "IBM", "MSFT"), {"AAPL": refused(), "IBM": busy()}, now=0.0, at=AT, since=AT
    )
    assert [symbols.holds_keys(name) for name in ("AAPL", "IBM", "MSFT", "TSLA")] == [
        False,
        True,
        False,
        False,
    ]


# --- settle -------------------------------------------------------------------------------------


def test_a_cycle_with_one_answer_is_good_and_spans_each_failure() -> None:
    symbols = names(field="Underlying")
    symbols.due(("AAPL", "IBM", "MSFT"), now=0.0)
    settled = symbols.settle(
        ("AAPL", "IBM", "MSFT"), {"AAPL": refused(), "IBM": busy()}, now=0.0, at=AT, since=AT
    )
    assert (settled.answered, settled.failure, settled.fatal) == (True, None, None)
    assert spans_by_cause(settled) == {
        ProblemCause.REFUSED_SYMBOLS: {"AAPL"},
        ProblemCause.SYMBOL_READ_RETRYING: {"IBM"},
    }  # only the causes whose spans changed


def test_a_cycle_where_nothing_answered_fails_on_a_failure_that_may_pass() -> None:
    symbols = names()
    symbols.due(("AAPL", "IBM", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "IBM", "MSFT"), {"AAPL": refused()}, now=0.0, at=AT, since=AT)
    symbols.due(("AAPL", "IBM", "MSFT"), now=1.0)
    error = busy()
    settled = symbols.settle(
        ("IBM", "MSFT"), {"IBM": error, "MSFT": busy()}, now=1.0, at=AT, since=AT
    )
    assert (settled.answered, settled.failure, settled.fatal) == (False, error, None)


def test_a_cycle_whose_failures_are_all_final_is_neutral() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {}, now=0.0, at=AT, since=AT)
    symbols.due(("AAPL", "MSFT", "TSLA"), now=1.0)
    settled = symbols.settle(("TSLA",), {"TSLA": refused()}, now=1.0, at=AT, since=AT)
    assert (settled.answered, settled.failure, settled.fatal) == (False, None, None)


def test_a_cycle_with_no_symbol_is_good_and_one_with_none_due_is_neutral() -> None:
    symbols = names()
    assert symbols.due((), now=0.0) == ()
    assert symbols.settle((), {}, now=0.0, at=AT, since=AT).answered
    symbols.due(("AAPL",), now=1.0)
    symbols.settle(("AAPL",), {"AAPL": refused()}, now=1.0, at=AT, since=AT)
    assert symbols.due(("AAPL",), now=2.0) == ()
    assert not symbols.settle((), {}, now=2.0, at=AT, since=AT).answered


# --- the call-time check ------------------------------------------------------------------------


def test_the_table_fails_once_every_first_cycle_symbol_has_failed_for_good() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    first = symbols.settle(
        ("AAPL", "MSFT"), {"AAPL": refused(), "MSFT": busy()}, now=0.0, at=AT, since=AT
    )
    assert first.fatal is None  # MSFT may still answer
    symbols.due(("AAPL", "MSFT"), now=LIMIT)
    settled = symbols.settle(("MSFT",), {"MSFT": busy()}, now=LIMIT, at=AT, since=AT)
    assert isinstance(settled.fatal, EntitlementError)  # AAPL's, first in name order
    symbols.due(("AAPL", "MSFT"), now=2 * LIMIT)
    assert symbols.settle((), {}, now=2 * LIMIT, at=AT, since=AT).fatal is None  # once only


def test_one_first_cycle_symbol_answering_settles_the_check() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {"AAPL": refused()}, now=0.0, at=AT, since=AT)
    symbols.due(("AAPL", "MSFT"), now=1.0)
    settled = symbols.settle(("MSFT",), {"MSFT": refused()}, now=1.0, at=AT, since=AT)
    assert settled.fatal is None


def test_a_later_symbol_answering_leaves_the_check_open() -> None:
    symbols = names()
    symbols.due(("AAPL",), now=0.0)
    symbols.settle(("AAPL",), {"AAPL": busy()}, now=0.0, at=AT, since=AT)
    symbols.due(("AAPL", "MSFT"), now=LIMIT)
    settled = symbols.settle(("AAPL", "MSFT"), {"AAPL": busy()}, now=LIMIT, at=AT, since=AT)
    assert isinstance(settled.fatal, MassiveError)


def test_a_first_cycle_symbol_leaving_before_it_failed_settles_the_check() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {"AAPL": busy(), "MSFT": busy()}, now=0.0, at=AT, since=AT)
    symbols.due(("MSFT",), now=LIMIT)  # AAPL leaves while it may still answer
    settled = symbols.settle(("MSFT",), {"MSFT": busy()}, now=LIMIT, at=AT, since=AT)
    assert settled.fatal is None


def test_a_failed_first_cycle_symbol_leaving_keeps_the_check() -> None:
    symbols = names()
    symbols.due(("AAPL", "MSFT"), now=0.0)
    symbols.settle(("AAPL", "MSFT"), {"AAPL": refused(), "MSFT": busy()}, now=0.0, at=AT, since=AT)
    symbols.due(("MSFT",), now=LIMIT)  # AAPL, refused, leaves
    settled = symbols.settle(("MSFT",), {"MSFT": busy()}, now=LIMIT, at=AT, since=AT)
    assert settled.fatal is not None
    assert str(settled.fatal).startswith("MSFT has failed for 300 s")


def test_the_last_pending_first_cycle_symbol_leaving_settles_the_check() -> None:
    symbols = names()
    symbols.due(("AAPL",), now=0.0)
    symbols.settle(("AAPL",), {"AAPL": busy()}, now=0.0, at=AT, since=AT)
    symbols.due(("MSFT",), now=1.0)  # AAPL, the only first-cycle symbol, leaves
    settled = symbols.settle(("MSFT",), {"MSFT": refused()}, now=1.0, at=AT, since=AT)
    assert settled.fatal is None


def test_a_first_cycle_with_no_symbol_has_no_check() -> None:
    symbols = names()
    symbols.due((), now=0.0)
    symbols.due(("AAPL",), now=1.0)
    settled = symbols.settle(("AAPL",), {"AAPL": refused()}, now=1.0, at=AT, since=AT)
    assert settled.fatal is None


# --- _is_due ------------------------------------------------------------------------------------


@pytest.mark.parametrize("state", list(SymbolReadState))
def test_every_state_is_due_or_not(state: SymbolReadState) -> None:
    run = SymbolRun(state, due=5.0 if state is SymbolReadState.RETRYING else None)
    expected = state not in {SymbolReadState.FAILED, SymbolReadState.RETRYING}
    assert _is_due(run, 4.0) is expected
    if state is SymbolReadState.RETRYING:
        assert _is_due(run, 5.0)


def test_a_state_no_case_handles_is_refused() -> None:
    with pytest.raises(AssertionError, match="bogus"):
        _is_due(SymbolRun("bogus"), 0.0)  # type: ignore[arg-type]  # the point: refused at run time


def test_a_step_no_case_handles_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    symbols = names()
    symbols.due(("AAPL",), now=0.0)
    monkeypatch.setattr(SymbolRun, "failed", lambda *_, **__: "bogus")
    with pytest.raises(AssertionError, match="bogus"):
        symbols.settle(("AAPL",), {"AAPL": busy()}, now=0.0, at=AT, since=AT)
