"""The Basic plan's pacing of the live tests: the request limit, its skip, and the wait lines.

Massive's free Basic plan allows 5 REST requests a minute to each asset class, and a live run
paces every request to a class on Basic at that rate: the run shares one rate governor of the
key across its tests (`tests/conftest.py`). A sixth request in a minute waits for the first
one's minute to end, so a test that sent 45 requests to one class would sit silent for nine
minutes. No live test that may read a class on Basic sends that class more than 5 requests:
when its class is on Basic it reads fewer underlyings, symbols, or pages, or leaves an extra
part out, and it declares the most requests it then sends to one class with
`@pytest.mark.requests(count)`. On a paid plan nothing is paced, so the same test may send
more. This module holds:

- `basic_reads` and `declared_requests`: the classes a test reads on their Basic plan, and the
  count its `requests` marker declares;
- `skip_reason`: the reason a test that would send a class on Basic more than 5 requests is
  skipped, the run's backstop;
- `live_cases` and `over_the_limit`: every case of a live test module with its plans and
  count, and those over the limit, for the unit test that holds every live test to it;
- `WaitProgress`: the governor listener that prints one terminal line each time a request of
  a live test starts to wait `SHOWN_WAIT` seconds or more, so a paced run never looks hung.

`tests/conftest.py` turns these into its collection hook and fixtures. Everything here but
`WaitProgress` is pure.
"""

from __future__ import annotations

import dataclasses
import math
import threading
from typing import TYPE_CHECKING, Final, Protocol

import pytest

from deephaven_massive._massive import BASIC_REQUESTS_PER_MINUTE
from tests._live.plans import TIERS

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from types import ModuleType

    from deephaven_massive._massive import BucketState

BASIC_LIMIT: Final = BASIC_REQUESTS_PER_MINUTE
"""The most REST requests a live test may send to one class on Basic: a minute's worth, so no
test waits more than about a minute for its pace."""

DEFAULT_REQUESTS: Final = 10
"""The requests a live test is taken to send to one class when it declares none: more than
`BASIC_LIMIT`, so a test that may read a class on Basic must declare its count."""

SHOWN_WAIT: Final = 5.0
"""The shortest wait, in seconds, that `WaitProgress` prints: a shorter one only adds noise to
the terminal, and no run looks hung for it."""


def basic_reads(requirements: Iterable[str]) -> tuple[str, ...]:
    """Return the classes a test reads on their Basic plan, whose requests may be paced.

    A test that runs on a class's Basic plan (`needs("<class>:basic")`) reads that class
    whatever the key's plan, so its requests may wait for a place in the class's 5 a minute.
    `some:basic`, which asks for any class left on Basic, names no class.

    Args:
        requirements: The test's `needs` requirements, such as `"stocks:basic"` or `"s3"`.

    Returns:
        The classes, in requirement order.
    """
    return tuple(
        name
        for name, _, plan in (requirement.partition(":") for requirement in requirements)
        if plan == "basic" and name in TIERS
    )


def declared_requests(marker: pytest.Mark | None) -> int:
    """Return the requests a test declares it sends to one class on Basic.

    Args:
        marker: The test's `requests` marker, or None when it has none.

    Returns:
        The marker's count, or `DEFAULT_REQUESTS` without one.
    """
    return DEFAULT_REQUESTS if marker is None else int(marker.args[0])


def classes_on_basic(tiers: Mapping[str, str]) -> frozenset[str]:
    """Return the classes the key holds on Basic, for a test to read less of them.

    Args:
        tiers: The plan of each class, such as `LivePlans.tiers`.

    Returns:
        The names of the classes whose plan is `"basic"`.
    """
    return frozenset(name for name, tier in tiers.items() if tier == "basic")


def skip_reason(paced: Sequence[str], requests: int) -> str | None:
    """Return why a test that would wait more than a minute for its pace is skipped.

    Args:
        paced: The classes the test reads that the key holds on Basic.
        requests: The requests it declares it sends to one class (`declared_requests`).

    Returns:
        `"needs a paid <class> plan: N requests would take about M minutes on Basic"`, where
        M is the minutes the requests after the first 5 wait; None when no class it reads is
        on Basic or it sends at most `BASIC_LIMIT` requests.
    """
    if not paced or requests <= BASIC_LIMIT:
        return None
    minutes = math.ceil(requests / BASIC_LIMIT) - 1
    unit = "minute" if minutes == 1 else "minutes"
    return (
        f"needs a paid {' and '.join(paced)} plan: {requests} requests would take about "
        f"{minutes} {unit} on Basic"
    )


@dataclasses.dataclass(frozen=True)
class LiveCase:
    """One case of a live test, as its markers declare it.

    Attributes:
        name: The test function's name, with the case's id for a parametrized one.
        requirements: Its `needs` requirements.
        requests: The requests it declares it sends to one class (`declared_requests`).
    """

    name: str
    requirements: tuple[str, ...]
    requests: int


def _marks(value: object) -> tuple[pytest.Mark, ...]:
    """Return the marks of a `pytestmark` value or a parameter's `marks`.

    Args:
        value: A mark or a mark decorator, or a list or tuple of them.

    Returns:
        The marks, in order.
    """
    items = value if isinstance(value, list | tuple) else (value,)
    marks: list[pytest.Mark] = []
    for item in items:
        mark = item.mark if isinstance(item, pytest.MarkDecorator) else item
        assert isinstance(mark, pytest.Mark), mark
        marks.append(mark)
    return tuple(marks)


def live_cases(module: ModuleType) -> list[LiveCase]:
    """Return every case of a live test module, with the plans and requests its markers declare.

    A test function is one case, or one per value of its `parametrize` marker, each with that
    value's own marks. A case's marks are its value's, then its function's, then the module's
    (`pytestmark`), and its `requests` marker is the first of them, as pytest's closest
    marker is.

    Args:
        module: The imported test module.

    Returns:
        The cases, in the module's order.
    """
    shared = _marks(getattr(module, "pytestmark", []))
    cases = []
    for name, function in vars(module).items():
        if not name.startswith("test_") or not callable(function):
            continue
        own = _marks(getattr(function, "pytestmark", []))
        variants: list[tuple[str, tuple[pytest.Mark, ...]]] = [(name, ())]
        for mark in own:
            if mark.name == "parametrize":
                variants = [
                    (
                        f"{name}[{getattr(value, 'id', None) or index}]",
                        _marks(getattr(value, "marks", ())),
                    )
                    for index, value in enumerate(mark.args[1])
                ]
        for case, marks in variants:
            every = (*marks, *own, *shared)
            requirements = tuple(str(a) for m in every if m.name == "needs" for a in m.args)
            marker = next((m for m in every if m.name == "requests"), None)
            cases.append(LiveCase(case, requirements, declared_requests(marker)))
    return cases


def over_the_limit(cases: Iterable[LiveCase]) -> list[str]:
    """Return the cases that may read a class on Basic and declare more than `BASIC_LIMIT` requests.

    Args:
        cases: The live cases (`live_cases`).

    Returns:
        One line per such case: its name, count, and the classes it may read on Basic.
    """
    return [
        f"{case.name}: requests({case.requests}) on {', '.join(basic_reads(case.requirements))}"
        for case in cases
        if basic_reads(case.requirements) and case.requests > BASIC_LIMIT
    ]


class WallClock(Protocol):
    """The governor's time source, as `WaitProgress` reads it."""

    def time(self) -> float:
        """Return the wall-clock time, in seconds since the epoch.

        Returns:
            The time.
        """
        ...


class PacedGovernor(Protocol):
    """The key's rate governor, as `WaitProgress` reads it: a `RequestGovernor`."""

    @property
    def clock(self) -> WallClock:
        """The governor's time source."""
        ...

    def snapshot(self) -> tuple[BucketState, ...]:
        """Return the state of every bucket (`RequestGovernor.snapshot`).

        Returns:
            The states.
        """
        ...

    def estimate(self, bucket: str, requests: int, /) -> float | None:
        """Return how long new requests of a bucket take to start (`RequestGovernor.estimate`).

        Args:
            bucket: The bucket.
            requests: The new requests.

        Returns:
            The seconds, or None when the bucket is unpaced.
        """
        ...


class WaitProgress:
    """Prints one line each time a request of a live test starts to wait for a request slot.

    A paced request waits silently inside the library's rate governor, so a live run on Basic
    would look hung. The live run subscribes one of these to the key's governor
    (`RequestGovernor.subscribe`) for each live test, and unsubscribes it when the test ends.
    Each time a bucket's count of waiting requests grows and the new request waits at least
    `SHOWN_WAIT` seconds, it writes the bucket, the seconds until that request may start, and
    how many of the test's declared requests to the bucket are left, such as `"options:
    waiting 47 s for a request slot (paced at 5 requests a minute); 3 of this test's 5
    requests left"`.

    The governor calls it on whichever thread changed a bucket, without the governor's lock;
    a lock of its own guards the counts it remembers. It reads the governor (`snapshot`,
    `estimate`, which take the governor's lock) and calls `write` after releasing its lock.
    """

    def __init__(
        self, governor: PacedGovernor, *, requests: int, write: Callable[[str], None]
    ) -> None:
        """Remember each bucket's waiting requests and attempts as the test begins.

        Args:
            governor: The key's rate governor, shared by the run.
            requests: The requests the test declares it sends to one class
                (`declared_requests`); the requests left count down from it in each bucket.
            write: Prints one line to the terminal; called on the waiting request's thread.
        """
        self._governor = governor
        self._requests = requests
        self._write = write
        self._lock = threading.Lock()
        states = governor.snapshot()
        self._waiting = {state.bucket: state.waiting for state in states}
        self._attempts = {state.bucket: state.attempts for state in states}

    def __call__(self) -> None:
        """Write a line for each bucket with more requests waiting than at the last change."""
        lines = []
        with self._lock:
            for state in self._governor.snapshot():
                before = self._waiting.get(state.bucket, 0)
                self._waiting[state.bucket] = state.waiting
                if state.waiting <= before:
                    continue
                if state.limit is not None:
                    seconds = self._governor.estimate(state.bucket, state.waiting) or 0.0
                    why = f"paced at {state.limit} requests a minute"
                else:
                    until = state.paused_until or self._governor.clock.time()
                    seconds = max(until - self._governor.clock.time(), 0.0)
                    why = "paused after an HTTP 429"
                if seconds < SHOWN_WAIT:
                    continue
                made = state.attempts - self._attempts.get(state.bucket, 0)
                left = max(self._requests - made, 0)
                lines.append(
                    f"{state.bucket}: waiting {seconds:.0f} s for a request slot ({why}); "
                    f"{left} of this test's {self._requests} requests left"
                )
        for line in lines:
            self._write(line)
