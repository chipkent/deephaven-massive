"""The pacing of the live tests: the requests a test declares, and the wait lines.

Massive limits how many REST requests a minute some subscriptions of a key may send, and the
library paces each limited subscription at its limit: the run shares one rate governor of the
key across its tests (`tests/conftest.py`), declared from the owner's settings file or learned
from Massive's first refusal. A paced request waits silently for a later minute, so a test that
sends many requests to a limited subscription may sit for minutes. Each live test declares the
most REST requests it sends to one subscription with `@pytest.mark.requests(count)`, which
scales its timeout and counts down in the wait lines. This module holds:

- `declared_requests`: the count a test's `requests` marker declares;
- `WaitProgress`: the governor listener that prints one terminal line each time a request of
  a live test starts to wait `SHOWN_WAIT` seconds or more, so a paced run never looks hung.

`tests/conftest.py` turns these into its collection hook and fixtures. `declared_requests` is
pure.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Final, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest

    from deephaven_massive._massive import BucketState

DEFAULT_REQUESTS: Final = 10
"""The requests a live test is taken to send to one subscription when it declares none."""

SHOWN_WAIT: Final = 5.0
"""The shortest wait, in seconds, that `WaitProgress` prints: a shorter one only adds noise to
the terminal, and no run looks hung for it."""


def declared_requests(marker: pytest.Mark | None) -> int:
    """Return the most REST requests a test declares it sends to one subscription.

    Args:
        marker: The test's `requests` marker, or None when it has none.

    Returns:
        The marker's count, or `DEFAULT_REQUESTS` without one.
    """
    return DEFAULT_REQUESTS if marker is None else int(marker.args[0])


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

    A paced request waits silently inside the library's rate governor, so a live run would
    look hung. The live run subscribes one of these to the key's governor
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
            requests: The requests the test declares it sends to one subscription
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
