"""The REST client: one per session, shared by every thread that calls Massive.

`RestClient` sends GET requests to Massive's REST API and returns decoded `Page`s. It owns the
transport (urllib3, with TLS and proxy settings from `net.http`, and a cap on the decompressed
size of a response body), bearer authentication, the retry schedule of `net.retry` for
transient failures, and `next_url` paging. When each attempt may start is the
`RequestGovernor`'s decision (`rest.governor`): the client takes a permit of the request's
bucket (its Massive subscription) before every attempt and reports how it ended, so
a Basic subscription is paced and a refused one pauses, together with every other client that
shares the governor. Encoding lives in `rest.query`, decoding and error mapping in
`rest.response`, and conversion to Arrow in `rest.batch`.

**Thread-safety.** One client serves a whole session: the caller thread, every poller, and
every fan-out worker call it at once. Its shared mutable state is the governor, which locks its
own state, the connection slots (`net.slots`), which bound the requests in flight to the number
of connections, the list of idle connection pools, behind a leaf lock, and the closed `Cancel`
token. Every request listens to a token of its own, made from the client's and the caller's, so
closing the client ends its waits and nobody else's. No call modifies its arguments.

**One TLS context per connection.** The client keeps up to `max_concurrency` urllib3 pools of
one connection each, and each pool has an `ssl.SSLContext` of its own
(`net.http.new_ssl_context`). A request takes a connection slot and, with it, an idle pool, or
builds one when none is idle; it uses that pool alone and gives both back together, so no two
threads ever open connections on one context, and there are never more pools than slots.
urllib3 writes the ALPN protocol list onto the context of every connection it opens, while
OpenSSL may be copying that list for another thread's handshake with the GIL released: CPython
does not guard the copy, so a context shared between threads can corrupt a handshake
(`TLSV1_ALERT_DECODE_ERROR`) or crash the process (python/cpython#141012). No CPython or urllib3
release avoids it, the free-threaded build included. A pool is built when a request first needs
it, not with the client: a context takes about 15 ms and 0.75 MB to build, so building every
pool of a `max_concurrency` of 64 up front would cost about a second and 48 MB.

**Logging.** Each attempt logs one DEBUG line on this module's logger: the full URL, the status,
the `request_id`, and the time it took, with the row count for a 200 and, for any other status,
the body (up to 2,000 characters, the key replaced by `***`). A retry logs at INFO, and giving
up at DEBUG: the call that fails raises the error, which the library logs at the level its
cause gives, so a failure shows once in the console. Nothing logs a header.

This module imports only the standard library, `urllib3`, `net`, and `spec`.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass, replace
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, Protocol, cast
from urllib.parse import urlsplit

import urllib3

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.api_keys import check_api_key
from deephaven_massive._massive.net.base_urls import check_rest_base_url
from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.capture import capture_response
from deephaven_massive._massive.net.errors import (
    WireError,
    is_proxy_auth_failure,
    is_tls_failure,
    linked_errors,
)
from deephaven_massive._massive.net.http import (
    check_certificate_directories,
    https_proxy,
    new_ssl_context,
    proxy_credentials,
    trust_description,
)
from deephaven_massive._massive.net.redaction import Redactor
from deephaven_massive._massive.net.retry import (
    MAX_RETRIES,
    TRANSIENT_STATUSES,
    http_date_seconds,
    retry_after_seconds,
    transient_wait,
)
from deephaven_massive._massive.net.slots import ConnectionSlots
from deephaven_massive._massive.rest.governor import UNLIMITED, is_basic_refusal
from deephaven_massive._massive.rest.query import next_target, request_target
from deephaven_massive._massive.rest.response import (
    RequestTarget,
    body_excerpt,
    decode_page,
    http_error,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from urllib3 import BaseHTTPResponse

    from deephaven_massive._massive.rest.governor import (
        Admission,
        Patience,
        Permit,
        RequestGovernor,
    )
    from deephaven_massive._massive.rest.response import Page
    from deephaven_massive._massive.spec.types import RestPath

__all__ = [
    "CONNECT_TIMEOUT",
    "MAX_BODY_BYTES",
    "READ_TIMEOUT",
    "REST_BASE_URL",
    "RefusalHook",
    "RestClient",
]

_log = logging.getLogger(__name__)

REST_BASE_URL: Final = "https://api.massive.com"
"""Massive's REST API, the base URL every session's `RestClient` is given."""

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


MAX_BODY_BYTES = 256 * 1024 * 1024
"""The most bytes a response body may hold once decompressed; a larger one is refused.

The client asks for gzip, and a few kilobytes of gzip can expand to gigabytes: a broken or
hostile server or proxy could exhaust the process's memory with one reply, and urllib3 sets no
limit of its own. The body is read and decompressed in `_READ_CHUNK` pieces and refused as
soon as it passes the cap.

The cap sits well above the largest page the library asks for, estimated from each
endpoint's row schema and page size. A page of short rows (trades, quotes, bars, ratings: at
most 50,000 rows of a few hundred bytes) decompresses to some 10 to 40 MB. Datasets whose rows
are long ask for fewer at a time, so their pages stay at tens of megabytes: Benzinga news 1,000
(each article carries its whole HTML body), analyst insights and filing risk factors 5,000,
financial statements 10,000, and 10-K sections and 8-K texts 100. The cap bounds a
decompression bomb, not every memory risk: a body under it still parses into Python objects
several times its size.
"""

_READ_CHUNK = 1024 * 1024
"""Bytes of decompressed body read at a time."""


def _new_pool(proxy: str | None) -> urllib3.PoolManager:
    """Build one connection slot's pool: one connection, with a TLS context of its own.

    Args:
        proxy: The HTTPS proxy URL from `net.http.https_proxy`, credentials included, or
            `None` to connect directly.

    Returns:
        A `ProxyManager` through `proxy`, or a `PoolManager`, keeping one connection per host,
        with urllib3's retries off and the client's timeouts. A new `net.http.new_ssl_context`
        verifies both Massive and an HTTPS proxy, so the two legs of a request never see two
        trust stores; they are opened one after the other by the one thread that holds the
        slot. The proxy's user and password go in `Proxy-Authorization`, never in the pool.
    """
    timeout = urllib3.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT)
    context = new_ssl_context()
    if proxy is None:
        return urllib3.PoolManager(
            maxsize=1, retries=_NO_RETRIES, timeout=timeout, ssl_context=context
        )
    proxy_url, proxy_headers = proxy_credentials(proxy)
    return urllib3.ProxyManager(
        proxy_url,
        proxy_headers=proxy_headers,
        proxy_ssl_context=context,
        maxsize=1,
        retries=_NO_RETRIES,
        timeout=timeout,
        ssl_context=context,
    )


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
        patience: The patience of the call's read, charged by its waits for a pause.
    """

    rest_path: RestPath
    bucket: str
    cancel: Cancel | None
    patience: Patience


_REFUSED: Final = object()
"""The outcome of an attempt Massive refused with HTTP 429: try again, once the governor lets
the request start, without counting a retry."""


class RestClient:
    """A client for Massive's REST API, safe to share between any number of threads.

    The session creates one on first use, with plain arguments and the governor its API key
    shares, and closes it when the session closes. Every request is a GET with
    `Authorization: Bearer <key>`; the key never appears in a URL or a log, and a response body
    that echoes it has it replaced with `***` before it is decoded (`net.redaction`), so no row,
    error, or log line can quote it. Requests go to the base URL's host only: a `next_url` that
    points elsewhere is refused. Every attempt first takes a permit of its subscription's bucket
    from the `RequestGovernor` (which paces a Basic subscription and pauses one Massive
    refused); transient failures are retried per `net.retry`, and failures are raised as
    `WireError`, `WireAuthError`, or `WireEntitlementError`; no urllib3 or JSON exception
    escapes.

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
        """Create the client; no request is sent, and no connection pool is built yet.

        The proxy (`HTTPS_PROXY`, `NO_PROXY`) is read from the environment once, here, through
        `net.http`. Credentials in the proxy URL go to the proxy as a `Proxy-Authorization`
        header; the pools hold the proxy URL without them. Each of the up to `max_concurrency`
        pools gets a TLS context of its own, built with the pool the first time a request finds
        no idle pool: about 15 ms of CPU each, to parse the certificate bundle. Each context
        trusts the certificate authorities (`SSL_CERT_FILE`, `SSL_CERT_DIR`, else `certifi`)
        as the environment and files name them when it is built. A directory in
        `SSL_CERT_DIR` that cannot be listed, or an entry of one that cannot be examined, logs
        a WARNING here, once per process for each path and error
        (`net.http.check_certificate_directories`).

        Args:
            api_key: The Massive API key, sent as a bearer token: non-empty visible ASCII,
                with no whitespace (strip a key read from a file).
            base_url: The scheme and host of the REST API, with an optional port and no path,
                such as `"https://api.massive.com"`. `http` is accepted only for a loopback
                host (a local test server), so the key never travels in clear text.
            max_concurrency: The most requests the client has in flight at once, whatever
                its callers send: the client keeps that many pools of one connection to the
                host, and a request waits for one to be free (`net.slots`), so none is
                discarded and reopened under load however many fan-outs overlap. At least 1.
            governor: Admits every attempt and pauses a bucket Massive refused. It may be
                shared with other clients of the same API key; its clock is the time source of
                this client's retry waits too.
            on_refused: Called on the requesting thread for every HTTP 429, before the
                governor pauses the bucket (see `RefusalHook`); None to call nothing.

        Raises:
            ValueError: If `max_concurrency` is less than 1. A bad `api_key` or `base_url`
                raises `ValueError` too, from `net.api_keys.check_api_key` and
                `net.base_urls.check_rest_base_url`: an empty key or one with whitespace (never
                quoted), a base URL that is not an origin alone, or an `http` base URL for a
                host that is not loopback.
        """
        check_api_key(api_key, setting="api_key")
        origin = check_rest_base_url(base_url, setting="base_url")
        if max_concurrency < 1:
            raise ValueError(f"max_concurrency must be at least 1, not {max_concurrency}")
        self._origin = origin
        self._base = f"{origin[0]}://{urlsplit(base_url).netloc}"
        self._host = origin[1]
        self._governor = governor
        self._clock = governor.clock
        self._on_refused = on_refused
        # Cleans every response body of a key the server echoes (`net.redaction`).
        self._redactor = Redactor(api_key)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
        }
        proxy = https_proxy(self._host)
        # Once per client, not once per pool: the pools' contexts do not examine the
        # certificate directories, so this is where a configured one that is unreadable warns.
        check_certificate_directories()
        self._proxy = proxy
        # Every pool built, for `close()`: one per connection slot at most, each with a TLS
        # context of its own (the module docstring says why no two threads may open
        # connections on one context). Guarded by `_idle_lock`, a leaf lock.
        self._pools: list[urllib3.PoolManager] = []
        # The pools no request holds. A request takes one only with a slot, and builds one
        # only when none is idle, so every pool is idle or held by the one request whose slot
        # it goes with. Guarded by `_idle_lock`.
        self._idle: list[urllib3.PoolManager] = []
        self._idle_lock = threading.Lock()
        self._closed = Cancel()
        # Every request of every caller takes one, and an idle pool with it, so the session
        # never has more requests in flight than it keeps connections, however many fan-outs
        # overlap.
        self._slots = ConnectionSlots(max_concurrency)
        _log.debug(
            "REST client for %s: up to %d connection pools of one connection, each built on "
            "first need with its own TLS context trusting %s, through %s",
            self._base,
            max_concurrency,
            trust_description(),
            "no proxy" if proxy is None else f"the proxy {proxy_credentials(proxy)[0]}",
        )

    def get(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
        *,
        bucket: str,
        cancel: Cancel | None = None,
        patience: Patience = UNLIMITED,
    ) -> Page:
        """Request one page of a REST path.

        Blocks the calling thread for the request and its waits: for the governor's permit
        (a paced bucket's slot, or a pause after a 429, unless `patience` cannot cover the
        pause), then for a free connection while the client's other requests use all
        `max_concurrency` of them, and for up to three retries of a 5xx or connection
        failure, 0.5 to 2.4 s each. A 429 is not a retry: the attempt is made again once the
        governor allows.

        Errors: `WireError` when the request fails after its retries, its bucket is paused
        for longer than `patience` covers (status 429, saying when its next request goes), TLS
        verification fails, the proxy refuses its credentials (status 407, never retried), the
        body is not a valid page or decompresses to more than `MAX_BODY_BYTES` or not at all
        (never retried, even for a 429, which still pauses the bucket; a 429 whose body breaks
        off is a refusal like any other), the client is closed, or `cancel` is set;
        `WireAuthError` for 401; `WireEntitlementError` for 403 or a `NOT_AUTHORIZED`
        envelope. Parameters that do not fit the path raise `ValueError`, and a value of a
        type its kind does not accept raises `TypeError` (see `rest.query`). An unknown
        `bucket` raises `ValueError` when the request would start.

        Args:
            rest_path: The path, from the spec.
            path_params: A value for each placeholder of the template, by wire name.
            query_params: Query values by wire name; `None` values are left out. See
                `rest.query` for how each kind of value is sent.
            bucket: The governor's bucket the request is charged to: its subscription.
            cancel: Ends every wait of the request when set (the request then raises
                `WireError`); None when only `close()` should.
            patience: The patience of the request's read, shared by its requests: the most
                time they wait out pauses (`RequestGovernor.admission`). `UNLIMITED`, the
                default, waits however long.

        Returns:
            The decoded page.
        """
        target = request_target(rest_path, path_params, query_params)
        return self._fetch(_Job(rest_path, bucket, cancel, patience), target)

    def pages(
        self,
        rest_path: RestPath,
        path_params: Mapping[str, object],
        query_params: Mapping[str, object],
        *,
        bucket: str,
        cancel: Cancel | None = None,
        patience: Patience = UNLIMITED,
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
            patience: The patience every page's pause waits are charged to, as for `get`.
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
        return self._pages(_Job(rest_path, bucket, cancel, patience), target, limit)

    @property
    def governor(self) -> RequestGovernor:
        """The governor that admits this client's requests, shared with other clients."""
        return self._governor

    def close(self) -> None:
        """Close the pools' connections; every later request raises `WireError`.

        Safe from any thread and idempotent. A thread waiting for the governor or a retry
        stops waiting at once and raises `WireError`; a request already on the wire finishes
        or fails on its own (within the read timeout). The governor and the other clients
        that share it are not affected.
        """
        self._closed.set()
        with self._idle_lock:
            pools = list(self._pools)
        for pool in pools:  # outside the leaf lock: closing a connection may block
            pool.clear()

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
        request = RequestTarget(self._host, target)
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
                    _log.debug("%s; giving up after %d attempts", outcome.error, retry + 1)
                    raise outcome.error
                wait = transient_wait(retry, _JITTER.random())
                _log.info(
                    "%s; retry %d of %d in %.2f s", outcome.error, retry + 1, MAX_RETRIES, wait
                )
                self._clock.wait(stop, wait)
                retry += 1

    def _attempt(
        self, job: _Job, request: RequestTarget, stop: Cancel
    ) -> Page | _Retryable | object:
        """Send one attempt of a request, once the governor admits it.

        Args:
            job: The call's path, bucket, and cancellation.
            request: The request.
            stop: Set when the client closes or the caller cancels.

        Returns:
            The page; a `_Retryable` for a transient failure; or `_REFUSED` for a 429, which
            the governor has recorded.

        A failure that is not retryable raises `WireError`: a status that is neither 200, 429,
        nor a transient 5xx (`http_error` chooses the subclass), a body that decompresses to
        more than `MAX_BODY_BYTES` or does not decompress (`_unreadable`), and from `_permit`,
        `_unanswered`, and `_refused` a closed client or a cancelled caller, a failed TLS
        verification, a proxy that refused its credentials (HTTP 407), or a pause longer than
        the call's patience still covers (status 429).
        """
        lane = job if job.cancel is None else job.cancel
        # The claim is open before anything is granted, and its exit reports the permit if
        # nothing else did, so no interrupt can leave the permit unreported.
        with self._governor.admission(job.bucket, lane=lane, patience=job.patience) as admission:
            queued = self._clock.monotonic()
            permit = self._permit(request, admission, stop)
            # A connection of the pool next, taken only once admitted, so no request holds
            # one through a pause; cancelled while waiting for either, it is never sent.
            if not self._slots.acquire(stop):
                self._governor.release(permit)
                raise self._stopped(request)
            try:
                # The slot's pool: no other thread uses it, or its TLS context, until it is back.
                pool = self._take_pool()
            except BaseException:
                self._slots.release()
                self._governor.release(permit)
                raise
            try:
                return self._send(
                    job, request, permit, pool=pool, waited=self._clock.monotonic() - queued
                )
            finally:
                with self._idle_lock:
                    self._idle.append(pool)
                self._slots.release()

    def _take_pool(self) -> urllib3.PoolManager:
        """Take an idle pool, or build one; the caller holds a connection slot without a pool.

        Every pool is idle or held by the one request whose slot it goes with, so a caller
        that finds none idle holds a slot that has no pool yet, and the pool it builds keeps
        the count at one per slot at most. The pool is built outside the leaf lock: its TLS
        context takes a few milliseconds to parse the certificate bundle.

        Returns:
            A pool, and its TLS context, that no other request holds.
        """
        with self._idle_lock:
            if self._idle:
                return self._idle.pop()
        pool = _new_pool(self._proxy)
        with self._idle_lock:
            self._pools.append(pool)
        return pool

    def _send(
        self,
        job: _Job,
        request: RequestTarget,
        permit: Permit,
        *,
        pool: urllib3.PoolManager,
        waited: float,
    ) -> Page | _Retryable | object:
        """Send an admitted attempt and report its end to the governor.

        Args:
            job: The call's path, bucket, and cancellation.
            request: The request.
            permit: The attempt's permit: released when the request never left (the
                connection could not be made), refused for a 429, and done otherwise.
            pool: The pool of the connection slot the attempt holds, used by no other thread.
            waited: Seconds the attempt waited for its permit (a slot, a pause, a probe, or
                its lane's turn), for the attempt's log line.

        Returns:
            What `_attempt` returns.

        A failure that is not retryable raises `WireError`, as `_attempt` describes.
        """
        started = self._clock.monotonic()
        try:
            response = pool.request(
                "GET",
                self._base + request.target,
                headers=self._headers,
                redirect=False,
                preload_content=False,
            )
        except urllib3.exceptions.HTTPError as error:
            if _never_sent(error):
                self._governor.release(permit)  # no connection, so nothing reached Massive
            return self._unanswered(request, error)
        body: bytearray | None = None
        unreadable: WireError | None = None
        try:
            body = self._body(response)
        except urllib3.exceptions.DecodeError as error:  # a subclass of HTTPError: first
            unreadable = self._unreadable(request, response, f"does not decompress ({error})")
        except urllib3.exceptions.HTTPError as error:  # the body broke off or timed out
            if response.status == HTTPStatus.TOO_MANY_REQUESTS:
                # Massive refused before the body broke off: the refusal stands.
                self._refused(permit, http_error(response.status, b"", request), response)
                return _REFUSED
            return self._unanswered(request, error)
        if body is None:
            if unreadable is None:
                unreadable = self._unreadable(
                    request,
                    response,
                    f"decompresses to more than {MAX_BODY_BYTES} bytes, the most the client reads",
                )
            # The call that fails logs the one error; this records which reply it was. Never the
            # body: it was not read, or not in full.
            _log.debug("%s (HTTP %d); not retried", unreadable, response.status)
            # The status arrived whole, so the governor hears of it: a refusal pauses the bucket,
            # and any other status is an answer.
            if response.status == HTTPStatus.TOO_MANY_REQUESTS:
                self._refused(permit, http_error(response.status, b"", request), response)
            else:
                self._governor.done(permit, answered=True)
            raise unreadable
        elapsed = self._clock.monotonic() - started
        return self._answered(
            job,
            request,
            permit=permit,
            response=response,
            body=body,
            elapsed=elapsed,
            waited=waited,
        )

    def _answered(
        self,
        job: _Job,
        request: RequestTarget,
        *,
        permit: Permit,
        response: BaseHTTPResponse,
        body: bytearray,
        elapsed: float,
        waited: float,
    ) -> Page | _Retryable | object:
        """Clean a body read in full, log the attempt, and turn it into what `_attempt` returns.

        The body is cleaned of the key once, here, before anything reads it, so no row, error,
        or log line can quote it; the cleaned body, whatever its status, also goes to the
        reply-capture sink when one is installed (`net.capture`). A 200 is decoded into a page;
        any other status is logged at DEBUG with its body, then a 429 goes to the governor as a
        refusal, and every other status is reported answered.

        Args:
            job: The call's path, bucket, and cancellation.
            request: The request.
            permit: The attempt's permit: refused for a 429, done otherwise.
            response: The response, for its status and headers.
            body: Its whole body, as read.
            elapsed: Seconds from sending the request to reading its body.
            waited: Seconds the attempt waited for its permit before it was sent.

        Returns:
            What `_attempt` returns.

        Raises:
            WireError: For a failure that is not retryable, as `_attempt` describes, including a
                200 whose page cannot be read, which is logged at DEBUG with its body first.
        """
        data = self._redactor.received(body)
        if response.status == HTTPStatus.OK:
            self._governor.done(permit, answered=True)
            try:
                page = decode_page(data, job.rest_path, request)
            except WireError as unreadable:
                capture_response(
                    host=self._host, target=request.target, status=200, request_id=None, body=data
                )
                # The attempt's line still names it, with the body the page could not be read
                # from; the call that fails logs the error itself.
                if _log.isEnabledFor(logging.DEBUG):
                    _log.debug(
                        "GET %s -> 200 in %.3f s, waited %.1f s for a permit, but the page "
                        "cannot be read (%s); body: %s",
                        self._base + request.target,
                        elapsed,
                        waited,
                        unreadable,
                        body_excerpt(data),
                    )
                raise
            capture_response(
                host=self._host,
                target=request.target,
                status=200,
                request_id=page.request_id,
                body=data,
            )
            _log.debug(
                "GET %s -> 200 (request_id %s): %d rows in %.3f s, waited %.1f s for a permit",
                self._base + request.target,
                page.request_id,
                len(page.rows),
                elapsed,
                waited,
            )
            return page
        failure = http_error(response.status, data, request)
        capture_response(
            host=self._host,
            target=request.target,
            status=response.status,
            request_id=failure.request_id,
            body=data,
        )
        if _log.isEnabledFor(logging.DEBUG):
            # The whole body (up to 2,000 characters, already cleaned of the key), so the line
            # shows every word Massive or a proxy said; the error quotes only its message.
            _log.debug(
                "GET %s -> %d (request_id %s) in %.3f s, waited %.1f s for a permit; body: %s",
                self._base + request.target,
                response.status,
                failure.request_id,
                elapsed,
                waited,
                body_excerpt(data),
            )
        if response.status == HTTPStatus.TOO_MANY_REQUESTS:
            self._refused(permit, failure, response)
            return _REFUSED
        self._governor.done(permit, answered=True)
        if response.status in TRANSIENT_STATUSES:
            return _Retryable(failure)
        raise failure

    def _body(self, response: BaseHTTPResponse) -> bytearray | None:
        """Read a response's whole body, decompressed, and hand its connection back to the pool.

        The body is read in `_READ_CHUNK` pieces into one buffer, and abandoned once it passes
        `MAX_BODY_BYTES`, so a small compressed reply that expands without bound never fills
        memory. A body read in full returns its connection to the pool for reuse; a body
        abandoned part way, by the cap or by an error, closes its connection first, because
        the unread rest of the body would otherwise be read as the next response. urllib3's
        own errors pass through unchanged for `_send`: a `DecodeError` when the body does not
        decompress, another `HTTPError` when it breaks off or times out.

        Args:
            response: The response, its body not yet read.

        Returns:
            The decompressed body, or None when it passes `MAX_BODY_BYTES`.
        """
        body = bytearray()
        whole = False
        try:
            for chunk in response.stream(_READ_CHUNK, decode_content=True):
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    break
                body += chunk
            else:
                whole = True
        finally:
            if not whole:
                response.close()  # the rest of the body is unread, so the connection is not reused
            response.release_conn()
        return body if whole else None

    def _unreadable(
        self, request: RequestTarget, response: BaseHTTPResponse, reason: str
    ) -> WireError:
        """Return the error of a response whose body arrived but cannot be read.

        A body unreadable for its content, one that does not decompress or decompresses past
        `MAX_BODY_BYTES`, is never retried: it is no page of Massive's, and the same request
        would bring the same reply. (A body that breaks off or times out is a transient
        failure instead, retried like a reset.) A 429 is no exception: one with an unreadable
        body pauses its bucket and then fails the read, while one whose body breaks off is a
        refusal like any other: it pauses the bucket and is tried again once the pause ends,
        unless the call's patience cannot cover the pause (status 429).

        Args:
            request: The request.
            response: The response, for its status.
            reason: What is wrong with the body, completing "the response from <host> ...".

        Returns:
            A `WireError` with the response's status that names the request and the reason,
            never the body, marked `unreadable`: the body arrived whole.
        """
        return WireError(
            f"GET {request.target}: the response from {self._host} {reason}",
            status=response.status,
            host=self._host,
            path=request.target,
            unreadable=True,
        )

    def _permit(self, request: RequestTarget, admission: Admission, stop: Cancel) -> Permit:
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
                permit granted just as `stop` was set). With status 429 when the bucket is
                paused for longer than the call's patience still covers, quoting the latest
                refusal.
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

    def _stopped(self, request: RequestTarget) -> WireError:
        """Return the error of a request whose client closed or whose caller cancelled.

        Args:
            request: The request.

        Returns:
            A `WireError` without a status.
        """
        reason = "the REST client is closed" if self._closed.is_set() else "cancelled"
        return WireError(f"GET {request.target}: {reason}", host=self._host, path=request.target)

    def _unanswered(
        self, request: RequestTarget, error: urllib3.exceptions.HTTPError
    ) -> _Retryable:
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

        The governor logs the refusal, once, with the pause it chose; it hears of the refusal
        even when the hook raises, so the bucket pauses and the permit is reported. The
        attempt is then made again once the governor allows; the next permit fails instead,
        with status 429, if the call's patience cannot cover the pause.
        """
        basic = is_basic_refusal(failure.message)
        try:
            if self._on_refused is not None:
                self._on_refused(permit.bucket, failure, basic=basic)
        finally:
            self._pause(permit, failure, response)

    def _pause(self, permit: Permit, failure: WireError, response: BaseHTTPResponse) -> None:
        """Report a 429 to the governor, which pauses the bucket.

        Args:
            permit: The refused attempt's permit.
            failure: The 429's error.
            response: The response, for its `Date` and `Retry-After` headers.
        """
        now = http_date_seconds(response.headers.get("Date"))
        asked = retry_after_seconds(
            response.headers.get("Retry-After"), self._clock.time() if now is None else now
        )
        self._governor.refused(permit, retry_after=asked, message=failure.message)


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
