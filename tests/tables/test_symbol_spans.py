"""Tests for `_tables/symbol_spans.py`: the spans a read's troubled symbols hold, by cause."""

from __future__ import annotations

import pytest

from deephaven_massive._enums import OnError
from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._tables.problem_events import ProblemCause
from deephaven_massive._tables.symbol_spans import SymbolSpans

REFUSED = ProblemCause.REFUSED_SYMBOLS
FAILED = ProblemCause.SYMBOL_READ_FAILED


def spans() -> SymbolSpans:
    return SymbolSpans(
        dataset="stocks.trades", stream_id=7, where="call 7", refused_symbols=OnError.RAISE
    )


def test_a_symbol_s_span_starts_under_its_cause_with_the_read_s_refusal_setting() -> None:
    (failed, refused) = spans().update(
        {REFUSED: {"XYZ": "refused"}, FAILED: {"ABC": "broken"}}, field="Ticker", at=5, since=3
    )
    assert (failed.cause, failed.dataset, failed.stream_id, failed.where) == (
        FAILED,
        "stocks.trades",
        7,
        "call 7",
    )
    assert (refused.policy, failed.policy) == (OnError.RAISE, None)
    span = refused.spans["XYZ"]
    assert (span.symbol, span.field, span.detail, span.since, span.rows) == (
        "XYZ",
        "Ticker",
        "refused",
        3,
        0,
    )


def test_only_a_changed_cause_is_returned_and_a_cause_left_out_ends_its_spans() -> None:
    tracked = spans()
    tracked.update({REFUSED: {"XYZ": "refused"}, FAILED: {"ABC": "broken"}}, at=1)
    assert tracked.update({REFUSED: {"XYZ": "refused"}, FAILED: {"ABC": "broken"}}, at=2) == ()
    (ended,) = tracked.update({REFUSED: {"XYZ": "refused"}}, at=3)
    assert (ended.cause, dict(ended.spans), ended.at) == (FAILED, {}, 3)


def test_a_cause_that_is_no_span_cause_is_a_defect() -> None:
    with pytest.raises(BrokenGuaranteeError, match="unreadable_keys"):
        spans().update({ProblemCause.UNREADABLE_KEYS: {"XYZ": "bad"}})
