"""The retry schedule every Massive wire client follows, and the clock it waits on.

A request that fails for a reason that may pass is tried again, at most `MAX_RETRIES` times
(so at most four attempts). This module decides how long to wait before each retry; the
clients do the waiting, through a `Clock`, so tests replace time with a fake and never sleep:

- **Transient failures** (HTTP 500, 502, 503, 504, and connection failures) wait 0.5, 1, then
  2 seconds, each lengthened by up to 20 % of random jitter, so clients that failed together
  do not retry together.
- **Rate limits** (HTTP 429) wait what the response's `Retry-After` header asks, in either of
  its forms (a number of seconds or an HTTP date). Without a usable header they wait 15, 30,
  then 60 seconds. A request to wait longer than `MAX_RETRY_AFTER` (60 s) is not honoured:
  the client gives up at once rather than stall a caller for minutes.
- **TLS certificate failures** are never retried: the certificate will not change between
  attempts. `is_tls_failure` recognises one however deeply a library wrapped it.
- **Proxy credential refusals** (a proxy answering a tunnel request with HTTP 407) are never
  retried either: the same credentials would be refused again, and a corporate proxy may lock
  the account after a few refusals. `is_proxy_auth_failure` recognises one.

Everything else (401, 403, 404, a body that is not JSON) is not retryable, and the clients
fail at once. The functions here are pure and keep no state, so they are safe from any
thread. This module imports only the standard library.
"""

from __future__ import annotations

import logging
import ssl
import time
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import threading
    from collections.abc import Iterator

__all__ = [
    "JITTER",
    "MAX_RETRIES",
    "MAX_RETRY_AFTER",
    "RATE_LIMIT_WAITS",
    "SYSTEM_CLOCK",
    "TRANSIENT_STATUSES",
    "TRANSIENT_WAITS",
    "Clock",
    "SystemClock",
    "is_proxy_auth_failure",
    "is_tls_failure",
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
"""The wait in seconds before each retry of a 429 that carries no usable `Retry-After`."""

MAX_RETRY_AFTER = 60.0
"""The longest `Retry-After` wait in seconds a client honours; a longer one fails at once."""


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
        """Block the calling thread for `seconds` (a positive number), or until `event` is set.

        Returns True when `event` is set (the wait was interrupted), False when the time ran
        out.
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
            seconds: The longest the wait lasts.

        Returns:
            True if `event` was set, False if the time ran out.
        """
        return event.wait(seconds)


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


def retry_after_seconds(value: str | None, now: float) -> float | None:
    """Return the wait a `Retry-After` header asks for, or `None` when it is absent or unusable.

    Args:
        value: The header's text: a non-negative integer number of seconds (`"30"`) or an
            HTTP date (`"Wed, 21 Oct 2026 07:28:00 GMT"`); `None` when the response had none.
        now: The current wall-clock time in seconds since the Unix epoch, to turn a date into
            a wait.

    Returns:
        The wait in seconds, never negative (a date in the past asks for no wait), or `None`
        when `value` is `None` or is neither form. An unusable value is logged at WARNING,
        quoted, on this module's logger, so the caller's fallback wait is never silent. A date
        without a zone (`-0000`) is read as GMT, as HTTP dates always are.
    """
    if value is None:
        return None
    text = value.strip()
    if text.isascii() and text.isdigit():
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        _log.warning("ignoring a Retry-After that is neither seconds nor an HTTP date: %r", value)
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, when.timestamp() - now)


def rate_limit_wait(retry: int, retry_after: float | None) -> float:
    """Return the wait before a retry of an HTTP 429.

    The caller compares a `Retry-After` wait with `MAX_RETRY_AFTER` first and gives up when it
    is longer; this function only chooses between the header and the fallback schedule.

    Args:
        retry: Which retry this is, from 0 (the first) to `MAX_RETRIES - 1`; any other
            value raises `IndexError`.
        retry_after: The wait the response's `Retry-After` asked for, from
            `retry_after_seconds`, or `None` when it gave none.

    Returns:
        `retry_after` when given, otherwise 15, 30, or 60 seconds by `retry`.
    """
    if retry_after is not None:
        return retry_after
    return RATE_LIMIT_WAITS[retry]


def is_tls_failure(error: BaseException) -> bool:
    """Return whether `error` is, or wraps, a failed TLS certificate verification.

    HTTP libraries wrap `ssl.SSLCertVerificationError` in their own exceptions, as an argument
    (urllib3's `SSLError`) or as the cause or context of a later one, so the search follows all
    three.

    Args:
        error: The exception a request raised.

    Returns:
        True when an `ssl.SSLCertVerificationError` is found among `error`, its arguments, its
        `__cause__`, and its `__context__`, recursively.
    """
    return any(isinstance(item, ssl.SSLCertVerificationError) for item in _linked(error))


_PROXY_AUTH_REFUSED = "Tunnel connection failed: 407 "
"""How `http.client` words a tunnel request the proxy answered with HTTP 407."""


def is_proxy_auth_failure(error: BaseException) -> bool:
    """Return whether `error` is, or wraps, a proxy's refusal of the client's credentials.

    An HTTPS request through a proxy first asks it for a tunnel (`CONNECT`). A proxy that
    rejects the credentials answers HTTP 407, which `http.client` raises as
    `OSError("Tunnel connection failed: 407 <reason>")`, its only report of the status (every
    supported Python words it so); urllib3 wraps that in its `ProxyError` and botocore in its
    `ProxyConnectionError`.

    Args:
        error: The exception a request raised.

    Returns:
        True when an `OSError` whose message starts `Tunnel connection failed: 407 ` is found
        among `error`, its arguments, its `__cause__`, and its `__context__`, recursively.
    """
    return any(
        isinstance(item, OSError) and str(item).startswith(_PROXY_AUTH_REFUSED)
        for item in _linked(error)
    )


def _linked(error: BaseException) -> Iterator[BaseException]:
    """Yield `error` and every exception linked to it, each once.

    Args:
        error: The exception to start from.

    Yields:
        `error`, then the exceptions among its arguments, its `__cause__`, and its
        `__context__`, recursively; a cycle ends the search.
    """
    seen: set[int] = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        linked = (current.__cause__, current.__context__, *current.args)
        pending.extend(item for item in linked if isinstance(item, BaseException))
