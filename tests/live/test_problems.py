"""Tests of how a live check reads the problems it found (`tests/_live/problems.py`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import deephaven_massive as dm
from deephaven_massive._tables.diagnostics import Diagnostics
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent
from tests._live import problems

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest
    from deephaven.table import Table


def _problem() -> ProblemEvent:
    return ProblemEvent(
        cause=ProblemCause.NONCANONICAL_SYMBOLS,
        action=ProblemAction.SKIPPED,
        dataset="stocks.trades",
        stream_id=1,
        where="GET /v3/trades",
        symbol="msft",
        field=None,
        rows=0,
        detail="'msft' is not a Massive stock ticker",
        at=1_000,
        policy=dm.OnError.IGNORE,
    )


def test_the_lines_wait_for_every_queued_problem_before_reading(
    *,
    massive_session: dm.MassiveSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A problem queued just before the read is in the lines: the read waits for it to show.
    steps: list[str] = []
    wait = Diagnostics.wait_until_shown

    def waited(self: Diagnostics, timeout: float) -> bool:
        steps.append("wait")
        return wait(self, timeout)

    def read(table: Table) -> list[dict[str, Any]]:
        steps.append("read")
        return dh_to_pylist(table)

    monkeypatch.setattr(Diagnostics, "wait_until_shown", waited)
    massive_session._core.diagnostics.report_problems([_problem()])
    lines = problems.problem_lines(massive_session, read)
    assert steps == ["wait", "read"]
    assert lines == ["problem noncanonical_symbols msft : 'msft' is not a Massive stock ticker"]
