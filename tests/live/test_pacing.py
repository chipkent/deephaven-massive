"""Tests of the Basic plan's pacing of the live tests (`tests/_live/pacing.py`).

Besides the module's functions, one test holds every live test of the repository to the limit:
none that may read a class on Basic declares more than 5 requests.
"""

from __future__ import annotations

import importlib
import types
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from deephaven_massive._massive import BucketState
from tests._live import pacing

if TYPE_CHECKING:
    from deephaven_massive._massive import RequestGovernor

_ROOT = Path(__file__).resolve().parents[2]
"""The repository root, whose `tests/` holds the live test modules."""


def test_basic_reads_names_the_classes_needed_on_basic() -> None:
    needs = ("stocks:basic", "options:starter", "some:basic", "s3", "currencies:basic")
    assert pacing.basic_reads(needs) == ("stocks", "currencies")
    assert pacing.basic_reads(("indices:starter",)) == ()


def test_declared_requests_reads_the_marker_or_takes_the_default() -> None:
    assert pacing.declared_requests(pytest.mark.requests(3).mark) == 3
    assert pacing.declared_requests(None) == pacing.DEFAULT_REQUESTS > pacing.BASIC_LIMIT


def test_classes_on_basic() -> None:
    tiers = {"stocks": "basic", "options": "starter", "futures": "basic"}
    assert pacing.classes_on_basic(tiers) == {"stocks", "futures"}


def test_no_skip_within_the_limit_or_without_a_class_on_basic() -> None:
    assert pacing.skip_reason(["options"], 5) is None
    assert pacing.skip_reason([], 45) is None


def test_the_skip_reason_says_how_long_the_requests_would_wait() -> None:
    assert pacing.skip_reason(["options"], 45) == (
        "needs a paid options plan: 45 requests would take about 8 minutes on Basic"
    )
    assert pacing.skip_reason(["options", "stocks"], 6) == (
        "needs a paid options and stocks plan: 6 requests would take about 1 minute on Basic"
    )


def _module() -> types.ModuleType:
    """Build a live test module with each way a test declares its plans and requests.

    Returns:
        The module.
    """
    module = types.ModuleType("live_example")

    @pytest.mark.needs("stocks:basic")
    @pytest.mark.requests(4)
    def test_within() -> None: ...

    @pytest.mark.needs("options:basic")
    @pytest.mark.requests(45)
    def test_over() -> None: ...

    @pytest.mark.needs("stocks:starter")
    @pytest.mark.requests(45)
    def test_paid() -> None: ...

    @pytest.mark.needs("futures:basic")
    def test_undeclared() -> None: ...

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(1, marks=[pytest.mark.needs("indices:basic"), pytest.mark.requests(9)]),
            pytest.param(2, marks=pytest.mark.requests(2), id="two"),
            3,
        ],
    )
    @pytest.mark.needs("indices:basic")
    def test_cases(case: int) -> None: ...

    module.__dict__.update(
        test_within=test_within,
        test_over=test_over,
        test_paid=test_paid,
        test_undeclared=test_undeclared,
        test_cases=test_cases,
        test_value=3,
        helper=test_within,
        pytestmark=pytest.mark.integration,
    )
    return module


def test_live_cases_read_each_case_with_its_closest_marks() -> None:
    cases = {case.name: case for case in pacing.live_cases(_module())}
    assert set(cases) == {
        "test_within",
        "test_over",
        "test_paid",
        "test_undeclared",
        "test_cases[0]",
        "test_cases[two]",
        "test_cases[2]",
    }
    assert cases["test_within"] == pacing.LiveCase("test_within", ("stocks:basic",), 4)
    assert cases["test_undeclared"].requests == pacing.DEFAULT_REQUESTS
    assert cases["test_cases[0]"] == pacing.LiveCase(
        "test_cases[0]", ("indices:basic", "indices:basic"), 9
    )
    assert cases["test_cases[two]"].requests == 2
    assert cases["test_cases[2]"].requests == pacing.DEFAULT_REQUESTS


def test_live_cases_read_a_list_of_module_marks() -> None:
    module = _module()
    module.__dict__["pytestmark"] = [pytest.mark.integration, pytest.mark.requests(3)]
    cases = {case.name: case for case in pacing.live_cases(module)}
    assert cases["test_undeclared"].requests == 3
    assert cases["test_over"].requests == 45


def test_over_the_limit_names_the_cases_that_may_read_basic() -> None:
    assert pacing.over_the_limit(pacing.live_cases(_module())) == [
        "test_over: requests(45) on options",
        "test_undeclared: requests(10) on futures",
        "test_cases[0]: requests(9) on indices, indices",
        "test_cases[2]: requests(10) on indices",
    ]


def test_no_live_test_sends_more_than_five_requests_to_a_class_on_basic() -> None:
    """Hold every live test of the repository to the Basic limit.

    A live test that may read a class on Basic sends that class at most 5 requests there, the
    most that start within a minute, and says so with its `requests` marker. Reading less on
    Basic (fewer symbols or pages, or leaving an extra part out) and declaring the smaller
    count is how a test meets the limit; one without the marker counts as 10.
    """
    cases = []
    for path in sorted((_ROOT / "tests").rglob("test_*_integration.py")):
        name = ".".join(path.relative_to(_ROOT).with_suffix("").parts)
        cases.extend(pacing.live_cases(importlib.import_module(name)))
    assert len(cases) > 100, "the live test modules were not found"
    assert pacing.over_the_limit(cases) == []


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
    progress = pacing.WaitProgress(
        cast("RequestGovernor", governor), requests=5, write=lines.append
    )
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
    progress = pacing.WaitProgress(
        cast("RequestGovernor", governor), requests=3, write=lines.append
    )
    governor.put(_state("other", attempts=1, waiting=1, paused_until=1_030.0))
    progress()
    governor.put(_state("other", attempts=1, waiting=2))
    progress()
    assert lines == [
        (
            "other: waiting 30 s for a request slot (paused after an HTTP 429); "
            "2 of this test's 3 requests left"
        ),
        (
            "other: waiting 0 s for a request slot (paused after an HTTP 429); "
            "2 of this test's 3 requests left"
        ),
    ]
