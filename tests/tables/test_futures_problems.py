"""Tests for how a futures stream words the rows its calendar discarded.

The module is `_tables/futures_problems.py`.
"""

from __future__ import annotations

import pytest

from deephaven_massive._enums import OnError
from deephaven_massive._error_policy import ErrorPolicy, resolve_policy
from deephaven_massive._tables.futures_dates import Readiness
from deephaven_massive._tables.futures_problems import DISCARDS, futures_events
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause


@pytest.mark.parametrize(
    ("readiness", "cause", "words"),
    [
        (
            Readiness.UNLISTED,
            ProblemCause.UNLISTED_CONTRACTS,
            (
                "futures.trades: neither Massive's futures snapshot nor its contracts list has "
                "held 2 [ESZ6, NQZ6] for 30 minutes, so their rows are left out"
            ),
        ),
        (
            Readiness.UNSCHEDULED,
            ProblemCause.UNSCHEDULED_PRODUCTS,
            (
                "futures.trades: Massive's futures schedules give no session for rows of 2 [ESZ6, "
                "NQZ6], so those rows are left out"
            ),
        ),
    ],
)
def test_each_discard_has_its_cause_setting_and_words(
    readiness: Readiness, cause: ProblemCause, words: str
) -> None:
    rule = DISCARDS[readiness]
    assert rule.cause is cause
    raising = resolve_policy(ErrorPolicy(**{cause.value: OnError.RAISE}), ErrorPolicy())
    assert rule.setting(raising) is OnError.RAISE
    assert rule.message("futures.trades", ["NQZ6", "ESZ6"]) == words


def test_every_discarded_state_has_a_rule() -> None:
    assert set(DISCARDS) == {Readiness.UNLISTED, Readiness.UNSCHEDULED}


def test_a_streams_events_record_each_discard_with_its_setting() -> None:
    policy = resolve_policy(ErrorPolicy(unscheduled_products=OnError.IGNORE), ErrorPolicy())
    events = futures_events(
        "futures.trades",
        7,
        "flush channel T",
        discarded={Readiness.UNSCHEDULED: {"BZTZ6": (3, "no session")}},
        policy=policy,
    )
    assert [
        (e.cause, e.action, e.symbol, e.rows, e.detail, e.stream_id, e.field, e.policy)
        for e in events
    ] == [
        (
            ProblemCause.UNSCHEDULED_PRODUCTS,
            ProblemAction.WITHHELD,
            "BZTZ6",
            3,
            "no session",
            7,
            None,
            OnError.IGNORE,
        )
    ]
