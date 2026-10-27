"""A fake `Clock` for the wire clients: virtual time, so no test ever sleeps.

`FakeClock` implements the `Clock` protocol of `net.retry`. Its monotonic and wall clocks
start at fixed values and move only when a thread sleeps (automatic mode) or when the test
calls `advance` (manual mode):

- **Automatic** (the default): `wait(event, d)` returns at once and moves time to at least
  the waiter's deadline (its time at the call plus `d`). Waits of several threads overlap, as
  real ones do: two threads waiting 5 s from time 0 end at time 5, not 10.
- **Manual**: `wait(event, d)` blocks until the test advances time past the deadline. A test
  waits for threads to reach their waits with `wait_for_sleepers`, so it can check what they
  did (and did not do) before releasing them.

In both modes a wait whose event is set ends at once and returns True, without moving time,
as `threading.Event.wait` does; a blocked manual wait notices the event within 10 ms of real
time. Every wait is recorded. All state sits behind one condition, so any thread may call any
method.
"""

from __future__ import annotations

import threading

_WAIT = 10.0
"""Seconds a test waits for sleepers before failing; real time, only on failure."""

_POLL = 0.01
"""Seconds of real time between checks of a blocked manual wait's event."""


class FakeClock:
    """Virtual monotonic and wall clocks; see the module docstring."""

    def __init__(self, *, manual: bool = False, wall: float = 1_800_000_000.0) -> None:
        """Start both clocks.

        Args:
            manual: Whether sleeps block until `advance` (True) or return at once (False).
            wall: The wall-clock time at monotonic time 0, in seconds since the Unix epoch.
        """
        self._manual = manual
        self._wall = wall
        self._now = 0.0
        self._sleeps: list[float] = []
        self._deadlines: list[float] = []
        self._changed = threading.Condition()

    def monotonic(self) -> float:
        """Return the virtual monotonic time in seconds."""
        with self._changed:
            return self._now

    def time(self) -> float:
        """Return the virtual wall-clock time: the start's wall time plus the elapsed time."""
        with self._changed:
            return self._wall + self._now

    def wait(self, event: threading.Event, seconds: float) -> bool:
        """Record a wait; end it at once (automatic), when time reaches it (manual), or on `event`.

        Args:
            event: Ends the wait early when set.
            seconds: The requested duration.

        Returns:
            True if `event` was set, False if the time ran out.
        """
        with self._changed:
            deadline = self._now + seconds
            self._sleeps.append(seconds)
            if event.is_set():
                return True
            if not self._manual:
                self._now = max(self._now, deadline)
                return False
            self._deadlines.append(deadline)
            self._changed.notify_all()
            while not self._changed.wait_for(
                lambda: self._now >= deadline or event.is_set(), _POLL
            ):
                pass
            self._deadlines.remove(deadline)
            self._changed.notify_all()
            return event.is_set()

    def advance(self, seconds: float) -> None:
        """Move time forward and wake every sleeper whose deadline has come.

        Args:
            seconds: How far to move.
        """
        with self._changed:
            self._now += seconds
            self._changed.notify_all()

    def wait_for_sleepers(self, count: int) -> list[float]:
        """Wait until exactly `count` threads are blocked in a manual wait.

        Args:
            count: The number of sleepers expected.

        Returns:
            Their deadlines, sorted.

        Raises:
            AssertionError: If that many never sleep within 10 s of real time.
        """
        with self._changed:
            if not self._changed.wait_for(lambda: len(self._deadlines) == count, _WAIT):
                raise AssertionError(f"{len(self._deadlines)} sleepers, expected {count}")
            return sorted(self._deadlines)

    @property
    def waits(self) -> list[float]:
        """A copy of every requested wait duration, in call order."""
        with self._changed:
            return list(self._sleeps)
