"""The retry schedule every Massive wire client follows, and the clock it waits on.

A request that fails for a reason that may pass is tried again, at most `MAX_RETRIES` times
(so at most four attempts). This module decides how long to wait before each retry; the
clients do the waiting, through a `Clock`, so tests replace time with a fake and never sleep:

- **Transient failures** (HTTP 500, 502, 503, 504, and connection failures) wait 0.5, 1, then
  2 seconds, each lengthened by up to 20 % of random jitter, so clients that failed together
  do not retry together.
- **Rate limits** (HTTP 429) are not retries: the REST client's `RequestGovernor` pauses the
  refused subscription's requests and resumes them, for as long as each read is willing to
  wait, and they never count against `MAX_RETRIES`.
  This module gives the pause of a subscription whose requests are not paced
  (`rate_limit_wait`): what the response's `Retry-After` header asks, in either of its forms
  (a number of seconds or an HTTP date), and without a usable header 15, 30, then 60 seconds
  by the number of refusals in the run.
- **TLS certificate failures** are never retried: the certificate will not change between
  attempts. `net.errors.is_tls_failure` recognises one however deeply a library wrapped it.
- **Proxy credential refusals** (a proxy answering a tunnel request with HTTP 407) are never
  retried either: the same credentials would be refused again, and a corporate proxy may lock
  the account after a few refusals. `net.errors.is_proxy_auth_failure` recognises one.

Everything else (401, 403, 404, a body that is not JSON) is not retryable, and the clients
fail at once. The functions here are pure and keep no state, so they are safe from any
thread. This module imports only the standard library.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import threading

__all__ = [
    "JITTER",
    "MAX_RETRIES",
    "RATE_LIMIT_WAITS",
    "SYSTEM_CLOCK",
    "TRANSIENT_STATUSES",
    "TRANSIENT_WAITS",
    "UNTIMED_CHUNK",
    "Clock",
    "SystemClock",
    "http_date_seconds",
    "rate_limit_wait",
    "retry_after_seconds",
    "transient_wait",
]

_log = logging.getLogger(__name__)

MAX_RETRIES = 3
"""The most retries of one request after its first attempt."""

TRANSIENT_STATUSES = frozenset({500, 502, 503, 504})
"""The HTTP statuses that mean the server failed for now and a retry may succeed."""

TRANSIENT_WAITS = (0.5, 1.0, 2.0)
"""The base wait in seconds before each retry of a transient failure, before jitter."""

JITTER = 0.2
"""The most a transient wait is lengthened, as a fraction of its base wait."""

RATE_LIMIT_WAITS = (15.0, 30.0, 60.0)
"""The pause in seconds after the first, the second, and every later 429 of a run that carries
no usable `Retry-After`, for a subscription whose requests are not paced."""


class Clock(Protocol):
    """The time source a wire client reads and waits on.

    The clients take one at construction so tests can substitute a fake that records each
    wait and advances virtual time instead of sleeping. Every wait is interruptible: a client
    passes its closed event, so closing it ends every wait at once. An implementation must be
    safe to call from any number of threads at once.
    """

    def monotonic(self) -> float:
        """Return a monotonic time in seconds, for measuring waits and deadlines."""
        ...

    def time(self) -> float:
        """Return the wall-clock time in seconds since the Unix epoch, for HTTP dates."""
        ...

    def wait(self, event: threading.Event, seconds: float) -> bool:
        """Block the calling thread for `seconds`, or until `event` is set.

        `seconds` is a positive number, or `math.inf` to wait until `event` is set however
        long that takes. Returns True when `event` is set (the wait was interrupted), False
        when the time ran out.
        """
        ...


class SystemClock:
    """The real clock: `time.monotonic`, `time.time`, and `threading.Event.wait`.

    It has no state, so the one shared instance, `SYSTEM_CLOCK`, is safe from every thread.
    """

    def monotonic(self) -> float:
        """Return `time.monotonic()`.

        Returns:
            Seconds on the process's monotonic clock.
        """
        return time.monotonic()

    def time(self) -> float:
        """Return `time.time()`.

        Returns:
            Seconds since the Unix epoch.
        """
        return time.time()

    def wait(self, event: threading.Event, seconds: float) -> bool:
        """Block the calling thread on `event.wait(seconds)`.

        Args:
            event: Ends the wait early when set, such as a client's closed event.
            seconds: The longest the wait lasts; `math.inf` waits for `event` alone, in
                chunks of `UNTIMED_CHUNK` seconds, so Ctrl-C interrupts it on every operating
                system (an untimed lock wait may ignore it on Windows).

        Returns:
            True if `event` was set, False if the time ran out.
        """
        if not math.isinf(seconds):
            return event.wait(seconds)
        while not event.wait(UNTIMED_CHUNK):
            pass
        return True


UNTIMED_CHUNK = 0.5
"""Seconds of each wait of `SystemClock` without a deadline: short enough that Ctrl-C ends it
promptly on every operating system."""

SYSTEM_CLOCK = SystemClock()
"""The clock every client uses unless a test passes another."""


def transient_wait(retry: int, jitter: float) -> float:
    """Return the wait before a retry of a transient failure (a 5xx or a connection failure).

    Args:
        retry: Which retry this is, from 0 (the first) to `MAX_RETRIES - 1`; any other
            value raises `IndexError`.
        jitter: A fraction in [0, 1), normally `random.random()`; the wait grows by
            `jitter * JITTER` of its base. The caller draws it, so this function stays pure.

    Returns:
        The wait in seconds: 0.5, 1, or 2, lengthened by up to 20 %.
    """
    return TRANSIENT_WAITS[retry] * (1.0 + JITTER * jitter)


def http_date_seconds(value: str | None) -> float | None:
    """Return the time an HTTP date names, such as the value of a `Date` header.

    The REST client measures a `Retry-After` date against the response's own `Date` with it,
    and `retry_after_seconds` reads a `Retry-After` date with it, so both read dates alike.

    Args:
        value: The header's text, such as `"Wed, 21 Oct 2026 07:28:00 GMT"`, or `None` when
            the response had none.

    Returns:
        Seconds since the Unix epoch, or `None` when `value` is `None` or not an HTTP date. A
        date without a zone (`-0000`) is read as GMT, as HTTP dates always are. Nothing is
        logged: the caller decides what an unusable date means.
    """
    if value is None:
        return None
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return when.timestamp()


def retry_after_seconds(value: str | None, now: float) -> float | None:
    """Return the wait a `Retry-After` header asks for, or `None` when it is absent or unusable.

    Args:
        value: The header's text: a non-negative integer number of seconds (`"30"`) or an
            HTTP date (`"Wed, 21 Oct 2026 07:28:00 GMT"`); `None` when the response had none.
        now: The current wall-clock time in seconds since the Unix epoch, to turn a date into
            a wait.

    Returns:
        The wait in seconds, never negative (a date in the past asks for no wait), or `None`
        when `value` is `None` or is neither form. The wait is as asked, however long (more
        than 308 digits read as `math.inf`): each caller caps what it honours. An unusable
        value is logged at WARNING, quoted, on this module's logger, so the caller's fallback
        wait is never silent. A date is read as `http_date_seconds` reads it.
    """
    if value is None:
        return None
    text = value.strip()
    if text.isascii() and text.isdigit():
        return float(text)
    when = http_date_seconds(text)
    if when is None:
        _log.warning("ignoring a Retry-After that is neither seconds nor an HTTP date: %r", value)
        return None
    return max(0.0, when - now)


def rate_limit_wait(refusal: int, retry_after: float | None) -> float:
    """Return the pause after an HTTP 429 of a subscription whose requests are not paced.

    Args:
        refusal: Which refusal of the run this is, counted from 1. Every refusal after the
            third pauses as long as the third.
        retry_after: The wait the response's `Retry-After` asked for, from
            `retry_after_seconds`, or `None` when it gave none.

    Returns:
        `retry_after` when given, otherwise 15, 30, or 60 seconds by `refusal`.
    """
    if retry_after is not None:
        return retry_after
    return RATE_LIMIT_WAITS[min(refusal, len(RATE_LIMIT_WAITS)) - 1]
