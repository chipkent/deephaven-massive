"""The REST client: one connection pool per session, shared by every thread that calls Massive.

`RestClient` sends GET requests to Massive's REST API and returns decoded `Page`s. It owns the
transport (urllib3, with TLS and proxy settings from `net.http`), bearer authentication,
the retry schedule of `net.retry`, `next_url` paging, and the client-wide back-off after a
rate limit. Encoding lives in `rest.query`, decoding and error mapping in `rest.response`, and
conversion to Arrow in `rest.batch`.

**Thread-safety.** One client serves a whole session: the caller thread, every poller, and
every fan-out worker call it at once. Its shared mutable state is urllib3's pool, which locks
internally, and the **not-before instant** under a leaf lock: after a 429, the client sets an
instant before which no thread sends, so concurrent workers back off together instead of
multiplying 429s. The lock is held only to read or raise that instant, never across a wait or
a request. No call modifies its arguments.

This module imports only the standard library, `urllib3`, `net`, and `spec`.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass, replace
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import urllib3

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.net.http import https_proxy, proxy_credentials, ssl_context
from deephaven_massive._massive.net.retry import (
    MAX_RETRIES,
    MAX_RETRY_AFTER,
    SYSTEM_CLOCK,
    TRANSIENT_STATUSES,
    is_tls_failure,
    rate_limit_wait,
    retry_after_seconds,
    transient_wait,
)
from deephaven_massive._massive.rest.query import (
    next_target,
    origin_of,
    request_target,
)
from deephaven_massive._massive.rest.response import Request, decode_page, http_error

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven_massive._massive.net.retry import Clock
    from deephaven_massive._massive.rest.response import Page
    from deephaven_massive._massive.spec.types import RestPath

__all__ = ["CONNECT_TIMEOUT", "READ_TIMEOUT", "RestClient"]

_log = logging.getLogger(__name__)

# The source of retry jitter. `SystemRandom` draws from the OS and keeps no state, so threads
# share it safely; jitter needs no cryptographic strength, only independence.
_JITTER = random.SystemRandom()

CONNECT_TIMEOUT = 10.0
"""Seconds to wait for a connection (and its TLS handshake) to Massive or the proxy."""

READ_TIMEOUT = 30.0
"""Seconds to wait for each read of a response."""

_NO_RETRIES = urllib3.Retry(total=False, redirect=False)
"""urllib3's own retries, all off: the client retries itself and never follows a redirect.

An explicit `Retry`, because urllib3 stores a bare `retries=False` as given in some releases
and converts it in others; this value means the same in every supported release.
"""


@dataclass(frozen=True)
class _Retryable:
    """An attempt that failed in a way a retry may fix.

    Attributes:
        error: The error to raise if no retry is left.
        rate_limited: True for a 429, whose wait becomes the client-wide not-before instant;
            False for a transient failure, whose wait is this request's own.
        retry_after: For a 429, the wait its `Retry-After` asked for (at most 60 s), or
            `None` when it gave none.
    """

    error: WireError
    rate_limited: bool = False
    retry_after: float | None = None


class RestClient:
    """A client for Massive's REST API, safe to share between any number of threads.

    The session creates one on first use, with plain arguments, and closes it when the
    session closes. Every request is a GET with `Authorization: Bearer <key>`; the key never
    appears in a URL or a log. Requests go to the base URL's host only: a `next_url` that
    points elsewhere is refused. Failures are retried per `net.retry` and then raised as
    `WireError`, `WireAuthError`, or `WireEntitlementError`; no urllib3 or JSON exception
    escapes.

    Example:
        ::

            from deephaven_massive._massive import RestClient
            from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath

            trades = RestPath(
                key="/v3/trades/{stockTicker}",
                fields=(),
                params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
            )
            client = RestClient("my-key", base_url="https://api.massive.com", max_concurrency=8)
            try:
                for page in client.pages(trades, {"stockTicker": "AAPL"}, {}, limit=10_000):
                    print(page.status, len(page.rows))
            finally:
                client.close()
    """

    def __init__(
        self, api_key: str, *, base_url: str, max_concurrency: int, clock: Clock = SYSTEM_CLOCK
    ) -> None:
        """Create the client and its connection pool; no request is sent.

        The proxy (`HTTPS_PROXY`, `NO_PROXY`) and the certificate authorities
        (`SSL_CERT_FILE`, `SSL_CERT_DIR`, else `certifi`) are read from the environment once,
        here, through `net.http`. Credentials in the proxy URL go to the proxy as a
        `Proxy-Authorization` header; the pool holds the proxy URL without them.

        Args:
            api_key: The Massive API key, sent as a bearer token.
            base_url: The scheme and host of the REST API, with an optional port and no path,
                such as `"https://api.massive.com"`.
            max_concurrency: The most requests the caller sends at once (a fan-out's
                workers). The pool keeps that many connections to the host, so none is
                discarded and reopened under load. At least 1.
            clock: The time source for retry waits and the not-before instant. Tests pass a
                fake; the default is the real clock.

        Raises:
            ValueError: If `base_url` is not an `http` or `https` URL of a host alone, or
                `max_concurrency` is less than 1.
        """
        origin = origin_of(base_url)
        parts = urlsplit(base_url)
        if parts.path not in {"", "/"} or parts.query or parts.fragment or parts.username:
            raise ValueError(f"base_url {base_url!r} must name only a scheme, host, and port")
        if max_concurrency < 1:
            raise ValueError(f"max_concurrency must be at least 1, not {max_concurrency}")
        self._origin = origin
        self._base = f"{parts.scheme}://{parts.netloc}"
        self._host = origin[1]
        self._clock = clock
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
        }
        proxy = https_proxy(self._host)
        timeout = urllib3.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT)
        self._pool: urllib3.PoolManager
        if proxy:
            proxy_url, proxy_headers = proxy_credentials(proxy)
            self._pool = urllib3.ProxyManager(
                proxy_url,
                proxy_headers=proxy_headers,
                maxsize=max_concurrency,
                retries=_NO_RETRIES,
                timeout=timeout,
                ssl_context=ssl_context(),
            )
        else:
            self._pool = urllib3.PoolManager(
                maxsize=max_concurrency,
                retries=_NO_RETRIES,
                timeout=timeout,
                ssl_context=ssl_context(),
            )
        self._lock = threading.Lock()
        self._not_before = float("-inf")
        self._closed = threading.Event()

    def get(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
    ) -> Page:
        """Request one page of a REST path.

        Blocks the calling thread for the request and any retry waits: up to three retries,
        0.5 to 2.4 s each for a 5xx or connection failure, and up to 60 s each for a 429.

        Errors: `WireError` when the request fails after its retries, a 429 asks for more
        than 60 s, TLS verification fails, the body is not a valid page, or the client is
        closed; `WireAuthError` for 401; `WireEntitlementError` for 403 or a
        `NOT_AUTHORIZED` envelope. Parameters that do not fit the path raise `ValueError`,
        and a value of a type its kind does not accept raises `TypeError` (see `rest.query`).

        Args:
            rest_path: The path, from the spec.
            path_params: A value for each placeholder of the template, by wire name.
            query_params: Query values by wire name; `None` values are left out. See
                `rest.query` for how each kind of value is sent.

        Returns:
            The decoded page.
        """
        return self._fetch(rest_path, request_target(rest_path, path_params, query_params))

    def pages(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
        *,
        limit: int | None = None,
    ) -> Iterator[Page]:
        """Request a REST path and follow its `next_url` pages, one page at a time.

        The parameters are checked and encoded now, so a mistake raises here rather than at
        the first iteration. Each page is requested only when the iterator reaches it, so
        only one page of rows is held at a time. A failure while paging (a connection reset,
        a 5xx, a 429) retries that page's `next_url`, never the first page.

        Args:
            rest_path: The path, from the spec.
            path_params: A value for each placeholder of the template, by wire name.
            query_params: Query values of the first request, by wire name; later pages carry
                the query in their `next_url`.
            limit: The most rows to yield in total, or `None` for every page. Paging stops
                once that many rows are yielded, and the last page is cut to fit.

        Returns:
            An iterator of pages. Iterating raises what `get` raises, and `WireError` when a
            `next_url` points to a scheme, host, or port other than the base URL's (it is
            never requested).

        Raises:
            ValueError: When `limit` is less than 1, or the parameters do not fit the path
                (a parameter value of a type its kind does not accept raises `TypeError`).
        """
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, not {limit}")
        return self._pages(rest_path, request_target(rest_path, path_params, query_params), limit)

    def close(self) -> None:
        """Close the pool's connections; every later request raises `WireError`.

        Safe from any thread and idempotent. A request already in flight finishes or fails
        on its own.
        """
        self._closed.set()
        self._pool.clear()

    def _pages(self, rest_path: RestPath, target: str, limit: int | None) -> Iterator[Page]:
        """Yield the pages of a request, following `next_url`.

        Args:
            rest_path: The path.
            target: The first request's target.
            limit: The most rows to yield, or `None`.

        Yields:
            Each page, the last cut to `limit`.

        Raises:
            WireError: If a `next_url` leaves the base URL's origin.
        """
        remaining = limit
        while True:
            page = self._fetch(rest_path, target)
            if remaining is not None and len(page.rows) >= remaining:
                yield replace(page, rows=page.rows[:remaining])
                return
            if remaining is not None:
                remaining -= len(page.rows)
            yield page
            next_url = page.body.get("next_url") if isinstance(page.body, dict) else None
            if not isinstance(next_url, str) or not next_url:
                return
            try:
                target = next_target(next_url, self._origin)
            except ValueError as error:
                raise WireError(
                    f"{rest_path.template}: refusing to follow {error}", host=self._host
                ) from error

    def _fetch(self, rest_path: RestPath, target: str) -> Page:
        """Request one target, retrying per the schedule.

        A transient failure sleeps this thread; a 429 moves the not-before instant, which the
        next attempt (and every other thread's) waits for. A 429 on the last attempt still
        moves the instant (by its `Retry-After`, else 60 s) before its error is raised.

        Args:
            rest_path: The path, for decoding.
            target: The path and query to request.

        Returns:
            The decoded page. When no retry is left, the last attempt's error is raised; see
            `get`.
        """
        request = Request(self._host, target)
        retry = 0
        while True:
            outcome = self._attempt(rest_path, request)
            if not isinstance(outcome, _Retryable):
                return outcome
            # The last attempt's wait goes unused, except that a 429 still moves the shared
            # instant, so the client's other requests back off even when this one gives up.
            step = min(retry, MAX_RETRIES - 1)
            if outcome.rate_limited:
                wait = rate_limit_wait(step, outcome.retry_after)
                self._defer(wait)
            else:
                wait = transient_wait(step, _JITTER.random())
            if retry == MAX_RETRIES:
                _log.error("%s; giving up after %d attempts", outcome.error, retry + 1)
                raise outcome.error
            _log.info("%s; retry %d of %d in %.2f s", outcome.error, retry + 1, MAX_RETRIES, wait)
            if not outcome.rate_limited:
                self._clock.sleep(wait)
            retry += 1

    def _attempt(self, rest_path: RestPath, request: Request) -> Page | _Retryable:
        """Send one attempt of a request, after the not-before instant.

        Args:
            rest_path: The path, for decoding.
            request: The request.

        Returns:
            The page, or the failure to retry.

        Raises:
            WireError: When the failure is not retryable: the client is closed, TLS
                verification failed, a 429 asked for more than 60 s, or the status is
                neither 200, 429, nor a transient 5xx (`http_error` chooses the subclass).
        """
        self._wait_not_before()
        if self._closed.is_set():
            raise WireError(f"GET {request.target}: the REST client is closed", host=self._host)
        started = self._clock.monotonic()
        try:
            response = self._pool.request(
                "GET",
                self._base + request.target,
                headers=self._headers,
                redirect=False,
            )
        except urllib3.exceptions.HTTPError as error:
            if is_tls_failure(error):
                raise WireError(
                    f"GET {request.target}: TLS certificate verification failed for "
                    f"{self._host} ({error}); if a proxy or private CA signs it, set "
                    "SSL_CERT_FILE to a bundle that trusts it",
                    host=self._host,
                    path=request.target,
                ) from error
            return _Retryable(
                WireError(
                    f"GET {request.target} on {self._host} failed: {error}",
                    host=self._host,
                    path=request.target,
                )
            )
        elapsed = self._clock.monotonic() - started
        if response.status == HTTPStatus.OK:
            page = decode_page(response.data, rest_path, request)
            _log.debug(
                "GET %s -> 200 (request_id %s): %d rows in %.3f s",
                request.target,
                page.request_id,
                len(page.rows),
                elapsed,
            )
            return page
        failure = http_error(response.status, response.data, request)
        _log.debug(
            "GET %s -> %d (request_id %s) in %.3f s",
            request.target,
            response.status,
            failure.request_id,
            elapsed,
        )
        if response.status == HTTPStatus.TOO_MANY_REQUESTS:
            return self._rate_limited(failure, response.headers.get("Retry-After"))
        if response.status in TRANSIENT_STATUSES:
            return _Retryable(failure)
        raise failure

    def _rate_limited(self, error: WireError, retry_after: str | None) -> _Retryable:
        """Read a 429's `Retry-After`, refusing a wait longer than the client honours.

        Args:
            error: The 429's error, raised if no retry is left.
            retry_after: The response's `Retry-After` header, or `None`.

        Returns:
            The failure to retry, with the wait the header asked for.

        Raises:
            WireError: If `Retry-After` asks for more than 60 s; status 429, naming the wait.
        """
        asked = retry_after_seconds(retry_after, self._clock.time())
        if asked is not None and asked > MAX_RETRY_AFTER:
            raise WireError(
                f"{error.message} (Massive asked to wait {asked:.0f} s, more than the "
                f"{MAX_RETRY_AFTER:.0f} s the client waits)",
                status=429,
                host=error.host,
                path=error.path,
                request_id=error.request_id,
            )
        return _Retryable(error, rate_limited=True, retry_after=asked)

    def _defer(self, wait: float) -> None:
        """Move the not-before instant to at least `wait` seconds from now.

        The instant only moves later: a shorter wait from another thread never shortens one
        already set. The lock is held only for the comparison.

        Args:
            wait: The seconds every thread must wait before its next request.
        """
        until = self._clock.monotonic() + wait
        with self._lock:
            self._not_before = max(self._not_before, until)

    def _wait_not_before(self) -> None:
        """Block until the not-before instant has passed; return at once if it has.

        The instant may move later while a thread waits (another 429), so the wait repeats
        until the instant read is in the past. The lock is held only to read it.
        """
        while True:
            with self._lock:
                not_before = self._not_before
            delay = not_before - self._clock.monotonic()
            if delay <= 0:
                return
            self._clock.sleep(delay)
