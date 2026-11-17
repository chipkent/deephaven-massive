"""How long the WebSocket client waits before it tries to connect again.

The client in `ws.client` keeps one `Backoff` and asks it for a wait after every attempt or
connection that ended without `close()` and without a fatal error. The policy protects the
account's connection allowance: Massive takes roughly 10 to 30 seconds to release a dropped
connection, and an attempt made earlier can be refused for holding the slot it just lost.

- The first wait is the initial backoff. Each further consecutive failure doubles it, up to the
  maximum.
- Jitter lengthens a wait by up to 20% and is clamped to the maximum. Once the doubling reaches
  the maximum, the wait is drawn from `[max(initial, maximum / 1.2), maximum]`, so every wait
  lies in `[initial, maximum]` and clients that lost their connections together spread out.
- The count of consecutive failures resets only when a connection has stayed authenticated for
  `STABLE_UPTIME` seconds. Authentication alone proves nothing: a server that accepts the key
  and drops the socket at once must not be retried at the initial backoff forever.

This module imports only the standard library. Its state is confined to the client's loop
thread, like the rest of the client's state.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["JITTER", "STABLE_UPTIME", "Backoff"]

STABLE_UPTIME = 60.0
"""Seconds a connection must stay authenticated before the failure count resets."""

JITTER = 0.2
"""The largest fraction by which jitter lengthens a wait."""

# Jitter needs no secrecy, but the operating system's source keeps no state a thread could share.
_system_uniform = random.SystemRandom().random


class Backoff:
    """The reconnect waits of one WebSocket client, and the failures they follow.

    The client creates one at construction and keeps it for its whole life. Before each wait it
    calls `next_wait`, passing how long the connection that just ended stayed authenticated
    (`None` for an attempt that never authenticated). Not thread-safe: only the client's loop
    thread uses it.

    Example:
        ::

            from deephaven_massive._massive.ws.backoff import Backoff

            backoff = Backoff(30.0, 120.0, uniform=lambda: 0.0)
            assert [backoff.next_wait(None) for _ in range(4)] == [30.0, 60.0, 100.0, 100.0]
            assert backoff.next_wait(uptime=75.0) == 30.0  # a stable connection resets the count
    """

    def __init__(
        self,
        initial: float,
        maximum: float,
        *,
        uniform: Callable[[], float] = _system_uniform,
    ) -> None:
        """Create the policy, with no failures counted yet.

        Args:
            initial: Seconds of the first wait. Positive.
            maximum: Seconds the doubling stops at, and the longest wait. At least `initial`.
            uniform: A source of numbers uniform in `[0, 1)`, which scales the jitter. Tests
                pass a constant to make the waits exact. The default draws from the operating
                system, which any thread may call without sharing a generator's state.

        Raises:
            ValueError: If `initial` is not positive or `maximum` is below `initial`.
        """
        if not initial > 0:
            raise ValueError(f"initial={initial!r} is not a positive number of seconds.")
        if maximum < initial:
            raise ValueError(f"maximum={maximum!r} is below initial={initial!r}.")
        self._initial = initial
        self._maximum = maximum
        self._uniform = uniform
        self._failures = 0

    def next_wait(self, uptime: float | None) -> float:
        """Count one more failure and return the wait before the next attempt.

        Args:
            uptime: Seconds the connection that just ended stayed authenticated, or `None` when
                the attempt never authenticated. At least `STABLE_UPTIME` resets the count
                first, so the wait is the initial backoff again.

        Returns:
            The wait in seconds, within `[initial, maximum]`.
        """
        if uptime is not None and uptime >= STABLE_UPTIME:
            self._failures = 0
        self._failures += 1
        # Doubling stops at the cap; the exponent is bounded so it never overflows a float.
        base = self._initial * 2.0 ** min(self._failures - 1, 64)
        if base >= self._maximum:
            base = max(self._initial, self._maximum / (1 + JITTER))
        return min(base * (1 + JITTER * self._uniform()), self._maximum)
