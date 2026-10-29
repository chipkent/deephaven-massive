"""The REST client: one connection pool per session, shared by every thread that calls Massive.

`RestClient` sends GET requests to Massive's REST API and returns decoded `Page`s. It owns the
transport (urllib3, with TLS and proxy settings from `net.http`), bearer authentication, the
retry schedule of `net.retry` for transient failures, and `next_url` paging. When each attempt
may start is the `RequestGovernor`'s decision (`rest.governor`): the client takes a permit of the
request's bucket (its Massive subscription) before every attempt and reports how it ended, so
a Basic subscription is paced and a refused one pauses, together with every other client that
shares the governor. Encoding lives in `rest.query`, decoding and error mapping in
`rest.response`, and conversion to Arrow in `rest.batch`.

**Thread-safety.** One client serves a whole session: the caller thread, every poller, and
every fan-out worker call it at once. Its shared mutable state is urllib3's pool, which locks
internally, the governor, which locks its own state, and the closed `Cancel` token. Every
request listens to a token of its own, made from the client's and the caller's, so closing
the client ends its waits and nobody else's. No call modifies its arguments.

This module imports only the standard library, `urllib3`, `net`, and `spec`.
"""

from __future__ import annotations

import ipaddress
import logging
import random
import re
from dataclasses import dataclass, replace
from datetime import UTC
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, Protocol, cast
from urllib.parse import urlsplit

import urllib3

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.errors import (
    WireError,
    is_proxy_auth_failure,
    is_tls_failure,
    linked_errors,
)
from deephaven_massive._massive.net.http import (
    TlsConsumer,
    https_proxy,
    proxy_credentials,
    ssl_context,
)
from deephaven_massive._massive.net.origins import origin_of
from deephaven_massive._massive.net.retry import (
    MAX_RETRIES,
    TRANSIENT_STATUSES,
    retry_after_seconds,
    transient_wait,
)
from deephaven_massive._massive.rest.governor import is_basic_refusal
from deephaven_massive._massive.rest.query import next_target, request_target
from deephaven_massive._massive.rest.response import Request, decode_page, http_error

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from urllib3 import BaseHTTPResponse

    from deephaven_massive._massive.net.origins import Origin
    from deephaven_massive._massive.rest.governor import Admission, Permit, RequestGovernor
    from deephaven_massive._massive.rest.response import Page
    from deephaven_massive._massive.spec.types import RestPath

__all__ = ["CONNECT_TIMEOUT", "READ_TIMEOUT", "RefusalHook", "RestClient"]

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


_VISIBLE_ASCII = re.compile(r"[!-~]+")
"""A non-empty run of visible ASCII: what an API key, sent in a header, may contain."""


def _check_api_key(api_key: str) -> None:
    """Refuse an API key that cannot be sent in a header, without ever showing it.

    A key read from a file often ends in a newline; sent as is, `http.client` raises an error
    that quotes the header, key included. Checking here keeps the key out of every message.

    Args:
        api_key: The key.

    Raises:
        ValueError: If the key is empty or holds anything but visible ASCII (a space, a tab,
            a line break, a non-ASCII letter); the message never includes the key.
    """
    if not _VISIBLE_ASCII.fullmatch(api_key):
        raise ValueError(
            "api_key must be non-empty visible ASCII, with no whitespace or line break "
            "(check for a trailing newline)"
        )


def _is_loopback(host: str) -> bool:
    """Return whether a host name is this machine: `localhost` or a loopback address.

    Args:
        host: A lower-case host name or IP address, without brackets.

    Returns:
        True for `localhost`, `127.0.0.0/8`, and `::1`.
    """
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _check_base_url(base_url: str) -> Origin:
    """Check the REST base URL and return its origin.

    Args:
        base_url: The base URL given to the client.

    Returns:
        The URL's `(scheme, host, port)`.

    Raises:
        ValueError: If `base_url` is not an `http` or `https` URL of a host alone (with no
            user information, path, query, or fragment), or is `http` for a host that is not
            loopback, which would send the API key in clear text.
    """
    origin = origin_of(base_url)
    parts = urlsplit(base_url)
    if parts.path not in {"", "/"} or parts.query or parts.fragment or "@" in parts.netloc:
        raise ValueError(f"base_url {base_url!r} must name only a scheme, host, and port")
    if origin[0] == "http" and not _is_loopback(origin[1]):
        raise ValueError(
            f"base_url {base_url!r} would send the API key in clear text; use https "
            "(http is allowed only for a loopback host, such as a local test server)"
        )
    return origin


class RefusalHook(Protocol):
    """What a `RestClient` calls for every HTTP 429, before its governor pauses the bucket."""

    def __call__(self, bucket: str, error: WireError, *, basic: bool) -> None:
        """Note a refusal; called on the requesting thread, it must not block or raise.

        Args:
            bucket: The refused request's bucket.
            error: The refusal: status 429, the path, the request id, and Massive's message.
            basic: Whether the message named the per-minute maximum, the mark of a Basic
                subscription (`rest.governor.is_basic_refusal`).
        """


@dataclass(frozen=True)
class _Retryable:
    """An attempt that failed in a way a retry may fix: a transient status or connection failure.

    Attributes:
        error: The error to raise if no retry is left.
    """

    error: WireError


@dataclass(frozen=True, eq=False)
class _Job:
    """What every page and attempt of one `get` or `pages` call shares.

    Compared by identity: a call without a cancellation token is a governor lane of its own.

    Attributes:
        rest_path: The path, for decoding.
        bucket: The governor's bucket every attempt is charged to.
        cancel: The caller's cancellation, or None.
    """

    rest_path: RestPath
    bucket: str
    cancel: Cancel | None


_REFUSED: Final = object()
"""The outcome of an attempt Massive refused with HTTP 429: try again, once the governor lets
the request start, without counting a retry."""


class RestClient:
    """A client for Massive's REST API, safe to share between any number of threads.

    The session creates one on first use, with plain arguments and the governor its API key
    shares, and closes it when the session closes. Every request is a GET with
    `Authorization: Bearer <key>`; the key never appears in a URL or a log. Requests go to the
    base URL's host only: a `next_url` that points elsewhere is refused. Every attempt first
    takes a permit of its subscription's bucket from the `RequestGovernor` (which paces a
    Basic subscription and pauses one Massive refused); transient failures are retried per
    `net.retry`, and failures are raised as `WireError`, `WireAuthError`, or
    `WireEntitlementError`; no urllib3 or JSON exception escapes.

    Example:
        ::

            from deephaven_massive._massive import RequestGovernor, RestClient
            from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath

            trades = RestPath(
                key="/v3/trades/{stockTicker}",
                fields=(),
                params=(Param(name="stockTicker", kind=ParamKind.STRING, in_path=True),),
            )
            governor = RequestGovernor(("stocks", "other"))
            client = RestClient(
                "my-key", base_url="https://api.massive.com", max_concurrency=8, governor=governor
            )
            try:
                pages = client.pages(trades, {"stockTicker": "AAPL"}, {}, bucket="stocks")
                for page in pages:
                    print(page.status, len(page.rows))
            finally:
                client.close()
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str,
        max_concurrency: int,
        governor: RequestGovernor,
        on_refused: RefusalHook | None = None,
    ) -> None:
        """Create the client and its connection pool; no request is sent.

        The proxy (`HTTPS_PROXY`, `NO_PROXY`) and the certificate authorities
        (`SSL_CERT_FILE`, `SSL_CERT_DIR`, else `certifi`) are read from the environment once,
        here, through `net.http`. Credentials in the proxy URL go to the proxy as a
        `Proxy-Authorization` header; the pool holds the proxy URL without them.

        Args:
            api_key: The Massive API key, sent as a bearer token: non-empty visible ASCII,
                with no whitespace (strip a key read from a file).
            base_url: The scheme and host of the REST API, with an optional port and no path,
                such as `"https://api.massive.com"`. `http` is accepted only for a loopback
                host (a local test server), so the key never travels in clear text.
            max_concurrency: The most requests the caller sends at once (a fan-out's
                workers). The pool keeps that many connections to the host, so none is
                discarded and reopened under load. At least 1.
            governor: Admits every attempt and pauses a bucket Massive refused. It may be
                shared with other clients of the same API key; its clock is the time source of
                this client's retry waits too.
            on_refused: Called on the requesting thread for every HTTP 429, before the
                governor pauses the bucket (see `RefusalHook`); None to call nothing.

        Raises:
            ValueError: If `max_concurrency` is less than 1. A bad `api_key` or `base_url`
                raises `ValueError` too, from `_check_api_key` and `_check_base_url`: an empty
                key or one with whitespace (never quoted), a base URL that is not an origin
                alone, or an `http` base URL for a host that is not loopback.
        """
        _check_api_key(api_key)
        origin = _check_base_url(base_url)
        if max_concurrency < 1:
            raise ValueError(f"max_concurrency must be at least 1, not {max_concurrency}")
        self._origin = origin
        self._base = f"{origin[0]}://{urlsplit(base_url).netloc}"
        self._host = origin[1]
        self._governor = governor
        self._clock = governor.clock
        self._on_refused = on_refused
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
                proxy_ssl_context=ssl_context(TlsConsumer.URLLIB3),
                maxsize=max_concurrency,
                retries=_NO_RETRIES,
                timeout=timeout,
                ssl_context=ssl_context(TlsConsumer.URLLIB3),
            )
        else:
            self._pool = urllib3.PoolManager(
                maxsize=max_concurrency,
                retries=_NO_RETRIES,
                timeout=timeout,
                ssl_context=ssl_context(TlsConsumer.URLLIB3),
            )
        self._closed = Cancel()

    def get(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
        *,
        bucket: str,
        cancel: Cancel | None = None,
    ) -> Page:
        """Request one page of a REST path.

        Blocks the calling thread for the request and its waits: for the governor's permit
        (a paced bucket's slot, or a pause after a 429, until a run's pauses add up to more
        than 10 minutes), and for up to three retries of a 5xx or connection failure, 0.5 to
        2.4 s each. A 429 is not a retry: the attempt is made again once the governor allows.

        Errors: `WireError` when the request fails after its retries, Massive keeps refusing
        it for longer than the governor waits (status 429), TLS verification fails, the proxy
        refuses its credentials (status 407, never retried), the body is not a valid page,
        the client is closed, or `cancel` is set; `WireAuthError` for 401;
        `WireEntitlementError` for 403 or a `NOT_AUTHORIZED` envelope. Parameters that do not
        fit the path raise `ValueError`, and a value of a type its kind does not accept raises
        `TypeError` (see `rest.query`). An unknown `bucket` raises `ValueError` when the
        request would start.

        Args:
            rest_path: The path, from the spec.
            path_params: A value for each placeholder of the template, by wire name.
            query_params: Query values by wire name; `None` values are left out. See
                `rest.query` for how each kind of value is sent.
            bucket: The governor's bucket the request is charged to: its subscription.
            cancel: Ends every wait of the request when set (the request then raises
                `WireError`); None when only `close()` should.

        Returns:
            The decoded page.
        """
        target = request_target(rest_path, path_params, query_params)
        return self._fetch(_Job(rest_path, bucket, cancel), target)

    def pages(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
        *,
        bucket: str,
        cancel: Cancel | None = None,
        limit: int | None = None,
    ) -> Iterator[Page]:
        """Request a REST path and follow its `next_url` pages, one page at a time.

        The parameters are checked and encoded now, so a mistake raises here rather than at
        the first iteration. Each page is requested only when the iterator reaches it, so
        only one page of rows is held at a time. Each page is a request of its own to the
        governor. A failure while paging (a connection reset, a 5xx, a 429) tries that page's
        `next_url` again, never the first page.

        Args:
            rest_path: The path, from the spec.
            path_params: A value for each placeholder of the template, by wire name.
            query_params: Query values of the first request, by wire name; later pages carry
                the query in their `next_url`.
            bucket: The governor's bucket every page is charged to: its subscription.
            cancel: Ends every wait of every page when set; None when only `close()` should.
            limit: The most rows to yield in total, or `None` for every page. Paging stops
                once that many rows are yielded, and the last page is cut to fit.

        Returns:
            An iterator of pages. Iterating raises what `get` raises, and `WireError` when a
            `next_url` points to a scheme, host, or port other than the base URL's, or names
            a page this pagination has already fetched (a cycle); such a `next_url` is never
            requested. A new call to `pages` starts with no fetched pages.

        Raises:
            ValueError: When `limit` is less than 1, or the parameters do not fit the path
                (a parameter value of a type its kind does not accept raises `TypeError`).
        """
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, not {limit}")
        target = request_target(rest_path, path_params, query_params)
        return self._pages(_Job(rest_path, bucket, cancel), target, limit)

    @property
    def governor(self) -> RequestGovernor:
        """The governor that admits this client's requests, shared with other clients."""
        return self._governor

    def close(self) -> None:
        """Close the pool's connections; every later request raises `WireError`.

        Safe from any thread and idempotent. A thread waiting for the governor or a retry
        stops waiting at once and raises `WireError`; a request already on the wire finishes
        or fails on its own (within the read timeout). The governor and the other clients
        that share it are not affected.
        """
        self._closed.set()
        self._pool.clear()

    def _pages(self, job: _Job, target: str, limit: int | None) -> Iterator[Page]:
        """Yield the pages of a request, following `next_url`.

        Args:
            job: The call's path, bucket, and cancellation.
            target: The first request's target.
            limit: The most rows to yield, or `None`.

        Yields:
            Each page, the last cut to `limit`.

        Raises:
            WireError: If a `next_url` leaves the base URL's origin, or names a target this
                pagination has already fetched (a cycle such as A to B to A, which would
                page forever). The check keeps one target string per page until paging ends.
        """
        remaining = limit
        fetched = {target}
        while True:
            page = self._fetch(job, target)
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
                following = next_target(next_url, self._origin)
            except ValueError as error:
                raise WireError(
                    f"{job.rest_path.template}: refusing to follow {error}", host=self._host
                ) from error
            if following in fetched:
                raise WireError(
                    f"{job.rest_path.template}: next_url {following} repeats a page this "
                    "pagination already fetched; refusing to loop",
                    host=self._host,
                    path=following,
                )
            fetched.add(following)
            target = following

    def _fetch(self, job: _Job, target: str) -> Page:
        """Request one target: each attempt after the governor's permit, retrying per schedule.

        A transient failure waits on this thread and counts a retry; a 429 waits in the
        governor and counts none.

        Args:
            job: The call's path, bucket, and cancellation.
            target: The path and query to request.

        Returns:
            The decoded page. When no retry is left, the last attempt's error is raised; see
            `get`.
        """
        request = Request(self._host, target)
        parents = (self._closed,) if job.cancel is None else (self._closed, job.cancel)
        with Cancel(*parents) as stop:
            retry = 0
            while True:
                outcome = self._attempt(job, request, stop)
                if outcome is _REFUSED:
                    continue
                if not isinstance(outcome, _Retryable):
                    return cast("Page", outcome)
                if retry == MAX_RETRIES:
                    _log.error("%s; giving up after %d attempts", outcome.error, retry + 1)
                    raise outcome.error
                wait = transient_wait(retry, _JITTER.random())
                _log.info(
                    "%s; retry %d of %d in %.2f s", outcome.error, retry + 1, MAX_RETRIES, wait
                )
                self._clock.wait(stop, wait)
                retry += 1

    def _attempt(self, job: _Job, request: Request, stop: Cancel) -> Page | _Retryable | object:
        """Send one attempt of a request, once the governor admits it.

        Args:
            job: The call's path, bucket, and cancellation.
            request: The request.
            stop: Set when the client closes or the caller cancels.

        Returns:
            The page; a `_Retryable` for a transient failure; or `_REFUSED` for a 429, which
            the governor has recorded.

        A failure that is not retryable raises `WireError`: a status that is neither 200, 429,
        nor a transient 5xx (`http_error` chooses the subclass), and from `_permit`,
        `_unanswered`, and `_refused` a closed client or a cancelled caller, a failed TLS
        verification, a proxy that refused its credentials (HTTP 407), or a bucket Massive
        kept refusing for longer than the governor waits (status 429).
        """
        lane = job if job.cancel is None else job.cancel
        # The claim is open before anything is granted, and its exit reports the permit if
        # nothing else did, so no interrupt can leave the permit unreported.
        with self._governor.admission(job.bucket, lane=lane) as admission:
            permit = self._permit(request, admission, stop)
            if stop.is_set():  # cancelled just as it was admitted: it is never sent
                self._governor.release(permit)
                raise self._stopped(request)
            return self._send(job, request, permit)

    def _send(self, job: _Job, request: Request, permit: Permit) -> Page | _Retryable | object:
        """Send an admitted attempt and report its end to the governor.

        Args:
            job: The call's path, bucket, and cancellation.
            request: The request.
            permit: The attempt's permit: released when the request never left (the
                connection could not be made), refused for a 429, and done otherwise.

        Returns:
            What `_attempt` returns.

        A failure that is not retryable raises `WireError`, as `_attempt` describes.
        """
        started = self._clock.monotonic()
        try:
            response = self._pool.request(
                "GET",
                self._base + request.target,
                headers=self._headers,
                redirect=False,
            )
        except urllib3.exceptions.HTTPError as error:
            if _never_sent(error):
                self._governor.release(permit)  # no connection, so nothing reached Massive
            return self._unanswered(request, error)
        elapsed = self._clock.monotonic() - started
        if response.status == HTTPStatus.TOO_MANY_REQUESTS:
            self._refused(permit, http_error(response.status, response.data, request), response)
            return _REFUSED
        self._governor.done(permit, answered=True)
        if response.status == HTTPStatus.OK:
            page = decode_page(response.data, job.rest_path, request)
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
        if response.status in TRANSIENT_STATUSES:
            return _Retryable(failure)
        raise failure

    def _permit(self, request: Request, admission: Admission, stop: Cancel) -> Permit:
        """Wait for the governor to admit an attempt, in the call's lane.

        The lane is the caller's cancellation token, which a read shares among its calls, or
        the call itself when it has none, so the governor takes the bucket's lanes in turn.

        Args:
            request: The request, for messages.
            admission: The attempt's claim on its bucket, in the call's lane.
            stop: Set when the client closes or the caller cancels.

        Returns:
            The attempt's permit.

        Raises:
            WireError: If `stop` is set before or during the wait: "the REST client is
                closed" when the client closed, "cancelled" otherwise (`_attempt` releases a
                permit granted just as `stop` was set). With status 429 when the bucket has
                given up on a run of refusals, quoting the latest refusal.
        """
        if stop.is_set():
            raise self._stopped(request)
        try:
            return admission.acquire(stop)
        except WireError as error:
            if error.status is None:  # the wait was cancelled
                raise self._stopped(request) from None
            raise WireError(
                error.message,
                status=error.status,
                host=self._host,
                path=request.target,
                request_id=error.request_id,
            ) from None

    def _stopped(self, request: Request) -> WireError:
        """Return the error of a request whose client closed or whose caller cancelled.

        Args:
            request: The request.

        Returns:
            A `WireError` without a status.
        """
        reason = "the REST client is closed" if self._closed.is_set() else "cancelled"
        return WireError(f"GET {request.target}: {reason}", host=self._host, path=request.target)

    def _unanswered(self, request: Request, error: urllib3.exceptions.HTTPError) -> _Retryable:
        """Classify an attempt that got no response.

        Args:
            request: The request.
            error: What urllib3 raised.

        Returns:
            The transient failure to retry.

        Raises:
            WireError: At once, for a failed TLS certificate verification or a proxy's
                refusal of its credentials (HTTP 407 to the tunnel request): neither changes
                on a retry, and repeating refused credentials can lock a corporate account.
        """
        if is_tls_failure(error):
            raise WireError(
                f"GET {request.target}: TLS certificate verification failed for "
                f"{self._host} ({error}); if a proxy or private CA signs it, set "
                "SSL_CERT_FILE to a bundle that trusts it",
                host=self._host,
                path=request.target,
            ) from error
        if is_proxy_auth_failure(error):
            raise WireError(
                f"GET {request.target}: the proxy refused its credentials (HTTP 407 for a "
                f"tunnel to {self._host}); check the user and password in HTTPS_PROXY",
                status=HTTPStatus.PROXY_AUTHENTICATION_REQUIRED,
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

    def _refused(self, permit: Permit, failure: WireError, response: BaseHTTPResponse) -> None:
        """Hand a 429 to the hook and the governor, which pauses the bucket.

        `Retry-After` is read in either form; an HTTP date is measured against the response's
        own `Date` header when it has a usable one, so the wait does not depend on this
        machine's clock, and against the local wall clock otherwise.

        Args:
            permit: The refused attempt's permit.
            failure: The 429's error.
            response: The response, for its headers.

        The governor hears of the refusal even when the hook or the log raises (an interrupt
        while the WARNING is written), so the bucket pauses and the permit is reported. When
        the bucket gives up on its run of refusals, the `WireError` of `_pause` is raised:
        status 429, quoting the latest refusal and naming this request.
        """
        basic = is_basic_refusal(failure.message)
        try:
            _log.info("%s; pausing %s requests", failure, permit.bucket)
            if self._on_refused is not None:
                self._on_refused(permit.bucket, failure, basic=basic)
        finally:
            give_up = self._pause(permit, failure, response)
        if give_up is not None:
            _log.error("%s", give_up)
            raise give_up

    def _pause(
        self, permit: Permit, failure: WireError, response: BaseHTTPResponse
    ) -> WireError | None:
        """Report a 429 to the governor, which pauses the bucket.

        Args:
            permit: The refused attempt's permit.
            failure: The 429's error.
            response: The response, for its `Date` and `Retry-After` headers.

        Returns:
            None, or the error to raise when the bucket gives up: status 429, this request's
            host and path, the refusal's request id, and the governor's message.
        """
        now = _http_time(response.headers.get("Date"))
        asked = retry_after_seconds(
            response.headers.get("Retry-After"), self._clock.time() if now is None else now
        )
        try:
            self._governor.refused(
                permit, retry_after=asked, message=failure.message, request_id=failure.request_id
            )
        except WireError as error:
            given_up = WireError(
                error.message,
                status=error.status,
                host=self._host,
                path=failure.path,
                request_id=error.request_id,
            )
            given_up.__cause__ = failure
            return given_up
        return None


_UNSENT: Final = (urllib3.exceptions.ConnectTimeoutError, urllib3.exceptions.ProxyError)
"""What urllib3 raises when the request never left for Massive: a connection that could not be
opened or timed out (`NewConnectionError` is one), or any failure to reach the proxy or set up
its tunnel (a `ProxyError`: a refused connection, a TLS handshake with the proxy, or HTTP 407 to
the tunnel request)."""


def _never_sent(error: BaseException) -> bool:
    """Tell whether a failed attempt never reached Massive, so its slot can be freed at once.

    The cause may be wrapped, so the whole chain is searched (`net.errors.linked_errors`). A TLS
    error on its own does not count: without a proxy it may come after the request was sent,
    and only one inside a `ProxyError` happened before the tunnel existed.

    Args:
        error: What urllib3 raised.

    Returns:
        True when a connection could not be made, or the proxy or its tunnel failed.
    """
    return any(isinstance(item, _UNSENT) for item in linked_errors(error))


def _http_time(value: str | None) -> float | None:
    """Return the time an HTTP `Date` header names, or None when it is absent or unusable.

    Args:
        value: The header's text, such as `"Wed, 21 Oct 2026 07:28:00 GMT"`, or None.

    Returns:
        Seconds since the Unix epoch (a date without a zone is read as GMT), or None.
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
