"""Tests of the live tests' pacing (`tests/_live/pacing.py`): declared requests, wait lines."""

from __future__ import annotations

import pytest

from deephaven_massive._massive import BucketState
from tests._live import pacing


def test_declared_requests_reads_the_marker_or_takes_the_default() -> None:
    assert pacing.declared_requests(pytest.mark.requests(3).mark) == 3
    assert pacing.declared_requests(None) == pacing.DEFAULT_REQUESTS


class _Clock:
    """The wall clock of `_Governor`, stopped at one time."""

    def time(self) -> float:
        return 1_000.0


class _Governor:
    """A rate governor whose buckets a test sets, for `WaitProgress`."""

    def __init__(self) -> None:
        self.states: list[BucketState] = [_state("options", limit=5), _state("other")]
        self.clock = _Clock()
        self.estimated: list[tuple[str, int]] = []

    def snapshot(self) -> tuple[BucketState, ...]:
        return tuple(self.states)

    def estimate(self, bucket: str, requests: int) -> float:
        self.estimated.append((bucket, requests))
        return 47.4

    def put(self, state: BucketState) -> None:
        self.states = [state if s.bucket == state.bucket else s for s in self.states]


def _state(
    bucket: str,
    *,
    limit: int | None = None,
    waiting: int = 0,
    attempts: int = 0,
    paused_until: float | None = None,
) -> BucketState:
    return BucketState(
        bucket=bucket,
        limit=limit,
        declared=limit is not None,
        learned=False,
        in_flight=0,
        waiting=waiting,
        recent=0,
        attempts=attempts,
        refusals=0,
        paused_until=paused_until,
        last_refusal=None,
    )


def test_wait_progress_writes_one_line_per_new_wait() -> None:
    governor = _Governor()
    governor.put(_state("options", limit=5, attempts=7))
    lines: list[str] = []
    progress = pacing.WaitProgress(governor, requests=5, write=lines.append)
    progress()  # nothing waits
    governor.put(_state("options", limit=5, attempts=9, waiting=1))
    progress()
    progress()  # the same wait: no new line
    governor.put(_state("options", limit=5, attempts=12, waiting=2))
    progress()
    governor.put(_state("options", limit=5, attempts=12))
    progress()  # the waits ended
    assert lines == [
        (
            "options: waiting 47 s for a request slot (paced at 5 requests a minute); "
            "3 of this test's 5 requests left"
        ),
        (
            "options: waiting 47 s for a request slot (paced at 5 requests a minute); "
            "0 of this test's 5 requests left"
        ),
    ]
    assert governor.estimated == [("options", 1), ("options", 2)]


def test_wait_progress_shows_a_pause_after_a_refusal() -> None:
    governor = _Governor()
    lines: list[str] = []
    progress = pacing.WaitProgress(governor, requests=3, write=lines.append)
    governor.put(_state("other", attempts=1, waiting=1, paused_until=1_030.0))
    progress()
    governor.put(_state("other", attempts=1, waiting=2))
    progress()  # the pause has ended: a wait under 5 s prints nothing
    assert lines == [
        (
            "other: waiting 30 s for a request slot (paused after an HTTP 429); "
            "2 of this test's 3 requests left"
        ),
    ]
