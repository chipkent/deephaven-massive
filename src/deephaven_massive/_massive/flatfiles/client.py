"""The client of Massive's flat-file bucket: one boto3 S3 client, its errors, and its close.

`FlatFileClient` opens the objects of Massive's S3 flat-file bucket (`flatfiles` at
`https://files.massive.com`) for `FlatFile` to decode. It takes plain arguments, never the
library's configuration, and maps every S3 failure to the wire errors of `net.errors`:

- a 404 is no error: `open` returns None, and the library applies its missing-file policy;
- a 403 whose S3 error code says the credentials themselves are wrong (`InvalidAccessKeyId`,
  `SignatureDoesNotMatch`) is `WireAuthError`;
- any other 403 is `WireEntitlementError`;
- anything else (another status, an unreachable endpoint, a client that cannot be created) is
  `WireError`.

Each error carries the HTTP status, the endpoint's host, the object key, and the
`x-amz-request-id` of the response as its `request_id`. Its message states what happened to
which object; what a user should do about it is the library's to say. Credentials are never
logged or put in a message.

The client follows the network rules the REST and WebSocket clients follow, from `net`:

- **Certificate authorities.** boto3 verifies the endpoint (and an HTTPS proxy) against
  `net.http.ca_file()`: `SSL_CERT_FILE` when set, otherwise `certifi`'s bundle. boto3 takes
  only a bundle file, so `SSL_CERT_DIR` does not apply to flat files.
- **Proxy.** `net.http.https_proxy` chooses the proxy (`HTTPS_PROXY`, `NO_PROXY`, or the
  system's settings), and botocore sends a user and password in its URL as
  `Proxy-Authorization`.
- **Retries.** botocore's own retries are off (one attempt per call). A request that fails
  before the first byte of the object arrives is retried on the schedule of `net.retry`, at
  most `MAX_RETRIES` times, with every wait taken on the client's `Clock`: HTTP 500, 502,
  503 (S3's `SlowDown`), and 504, S3's `RequestTimeout` error (HTTP 400), and a connection
  that cannot be made, is reset, or times out. A TLS failure is never retried, and neither is a
  failure part-way through a body: rows already handed on cannot be taken back, so the read
  fails instead.

The body of an object reaches `FlatFile` through a small stream that turns botocore's read
errors (a connection dropped part-way through the body) into `OSError`, so the reader needs no
botocore. This module imports only the standard library, `boto3`, `botocore`, and the rest of
`flatfiles` and `net`; the package's interface resolves it only when `FlatFileClient` is first
used, so a session that reads no flat file never imports boto3.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, HTTPClientError, SSLError
from botocore.exceptions import ConnectionError as BotoConnectionError

from deephaven_massive._massive.flatfiles.reader import FlatFile
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._massive.net.http import ca_file, https_proxy
from deephaven_massive._massive.net.retry import (
    MAX_RETRIES,
    SYSTEM_CLOCK,
    TRANSIENT_STATUSES,
    is_tls_failure,
    transient_wait,
)

if TYPE_CHECKING:
    from botocore.response import StreamingBody
    from types_boto3_s3.client import S3Client

    from deephaven_massive._massive.net.retry import Clock


__all__ = ["FlatFileClient"]

_log = logging.getLogger(__name__)

# The source of retry jitter. `SystemRandom` draws from the OS and keeps no state, so threads
# share it safely; jitter needs no cryptographic strength, only independence.
_JITTER = random.SystemRandom()

_CREDENTIAL_ERRORS: Final = frozenset({"InvalidAccessKeyId", "SignatureDoesNotMatch"})
"""S3 error codes, answered with HTTP 403, that mean the S3 credentials themselves are wrong."""

_TRANSIENT_CODES: Final = frozenset({"RequestTimeout"})
"""S3 error codes, answered with a status that is not transient (400), that a retry may fix."""

_CONNECTION_ERRORS: Final = (BotoConnectionError, HTTPClientError)
"""botocore's errors for a connection that cannot be made, is reset, or times out."""


@dataclass(frozen=True)
class _Retryable:
    """An attempt that failed before the object's first byte, in a way a retry may fix.

    Attributes:
        error: The error to raise if no retry is left.
        cause: The botocore error behind it, chained as the raised error's cause.
    """

    error: WireError
    cause: Exception


class _Body:
    """An object's response body, read as a plain binary stream.

    botocore raises its own errors when the connection drops part-way through a body; this
    stream raises them as `OSError` (chained to the original), the error every other stream
    raises for a failed read. Used by one reader thread; `close()` is safe from any thread.
    """

    __slots__ = ("_body",)

    def __init__(self, body: StreamingBody) -> None:
        """Wrap a response body.

        Args:
            body: The body of a successful `GetObject` response.
        """
        self._body = body

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes, or the rest of the body when `size` is negative.

        Args:
            size: The most bytes to return.

        Returns:
            The bytes read; empty at the end of the body.

        Raises:
            OSError: If the connection fails while the body is read.
        """
        try:
            return self._body.read(None if size < 0 else size)
        except BotoCoreError as error:
            raise OSError(str(error)) from error

    def close(self) -> None:
        """Close the body, which returns or drops its connection. Idempotent."""
        self._body.close()


class FlatFileClient:
    """The connection to Massive's flat-file bucket: one S3 client, created on first use.

    The library's session creates one on its first flat-file read and closes it when the
    session closes. The boto3 client is created by the first `open`, under the client's own
    leaf lock (no network call and no wait happen while it is held, since the credentials are
    given and boto3 need not look for them), and shared by every reader thread, since boto3
    clients are thread-safe. Once `close()` has run, no request starts; a request already in
    flight when the client closed is answered, and its connection pool is closed once more as
    it returns, so nothing it opened outlives the close.

    Example:
        ::

            from datetime import date

            from deephaven_massive._massive import FlatFileClient, object_key

            client = FlatFileClient(
                access_key_id="AKID...",
                secret_access_key="...",
                endpoint_url="https://files.massive.com",
                bucket="flatfiles",
                max_pool=8,
            )
            try:
                key = object_key("us_stocks_sip/trades_v1", date(2025, 3, 3))
                file = client.open(key)  # None if the bucket has no such file
            finally:
                client.close()
    """

    __slots__ = (
        "_access_key_id",
        "_bucket",
        "_client",
        "_clock",
        "_closed",
        "_endpoint_url",
        "_host",
        "_lock",
        "_max_pool",
        "_secret_access_key",
    )

    def __init__(
        self,
        *,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: str,
        bucket: str,
        max_pool: int,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        """Remember the settings; no client is created and nothing touches the network.

        The certificate authorities and the proxy are read from the environment later, once,
        when the first `open` creates the boto3 client.

        Args:
            access_key_id: The S3 access key id.
            secret_access_key: The S3 secret access key.
            endpoint_url: The S3 endpoint, such as `"https://files.massive.com"`.
            bucket: The bucket, such as `"flatfiles"`.
            max_pool: The most connections the client keeps open: one per concurrent reader.
            clock: The time source retry waits are taken on. Tests pass a fake; the default is
                the real clock.
        """
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._endpoint_url = endpoint_url
        self._host = urlsplit(endpoint_url).hostname
        self._bucket = bucket
        self._max_pool = max_pool
        self._clock = clock
        self._lock = threading.Lock()
        self._client: S3Client | None = None
        self._closed = threading.Event()

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        return self._closed.is_set()

    def open(self, key: str) -> FlatFile | None:
        """Request one object and return it for reading, or None if it does not exist.

        Blocks the calling thread for the request and any retry waits: one HTTP round trip
        when it succeeds, and up to three retries (0.5, 1, then 2 seconds apart, plus up to
        20 % jitter) when it fails before the object's first byte for a reason that may pass,
        such as HTTP 503 `SlowDown` or a reset connection. The body is read later, through the
        returned file, and a failure while it is read is never retried. `close()` ends a retry
        wait at once. The first call creates the boto3 client.

        Errors: `WireError` when the client is closed (no request is made) or cannot be
        created, TLS fails (never retried), a retryable failure persists after the last retry,
        or S3 answers any other status but 200 and 404. On HTTP 403 it is the subclass
        `WireAuthError` when S3 rejects the credentials themselves, and otherwise
        `WireEntitlementError`: the plan lacks the files. The message names the object and
        quotes S3's error code and message, such as `"HTTP 403 for s3://flatfiles/<key> at
        https://files.massive.com (AccessDenied: Access Denied)"`.

        Args:
            key: The object key (`object_key`).

        Returns:
            The object, open for reading, or None if the bucket answered 404.
        """
        location = f"s3://{self._bucket}/{key} at {self._endpoint_url}"
        retry = 0
        while True:
            outcome = self._attempt(key, location)
            if not isinstance(outcome, _Retryable):
                return outcome
            if retry == MAX_RETRIES:
                _log.error("%s; giving up after %d attempts", outcome.error, retry + 1)
                raise outcome.error from outcome.cause
            wait = transient_wait(retry, _JITTER.random())
            _log.info("%s; retry %d of %d in %.2f s", outcome.error, retry + 1, MAX_RETRIES, wait)
            # A close ends the wait at once; the next attempt then refuses to start.
            self._clock.wait(self._closed, wait)
            retry += 1

    def _attempt(self, key: str, location: str) -> FlatFile | _Retryable | None:
        """Send one attempt of a `GetObject` request.

        Args:
            key: The object key.
            location: The object's location, for messages.

        Returns:
            The object, open for reading; None for HTTP 404; or the failure to retry.

        Raises:
            WireError: When the failure is not retryable; see `open`.
        """
        client = self._client_for(key)
        try:
            body = client.get_object(Bucket=self._bucket, Key=key)["Body"]
        except ClientError as error:
            refusal = self._refusal(error, key=key, location=location)
            if refusal is None:
                return None
            details = error.response.get("Error", {})
            if refusal.status in TRANSIENT_STATUSES or details.get("Code") in _TRANSIENT_CODES:
                return _Retryable(refusal, error)
            raise refusal from error
        except BotoCoreError as error:
            if is_tls_failure(error):
                raise WireError(
                    f"cannot read {location}: TLS certificate verification failed for "
                    f"{self._host} ({error}); if a proxy or private CA signs it, set "
                    "SSL_CERT_FILE to a bundle that trusts it",
                    host=self._host,
                    path=key,
                ) from error
            if isinstance(error, SSLError):
                # Any other TLS failure (a protocol mismatch, a handshake the server ends) is
                # a configuration problem a retry cannot fix either.
                raise WireError(
                    f"cannot read {location}: TLS failed for {self._host} ({error})",
                    host=self._host,
                    path=key,
                ) from error
            failure = WireError(f"cannot read {location}: {error}", host=self._host, path=key)
            if isinstance(error, _CONNECTION_ERRORS):
                return _Retryable(failure, error)
            raise failure from error
        finally:
            if self._closed.is_set():
                # The client was closed while this request was in flight, and the request may
                # have opened a new connection pool: close it again now that it has returned.
                client.close()
        return FlatFile(_Body(body), key, location=location, host=self._host)

    def close(self) -> None:
        """Close the boto3 client, if one was created, and refuse every further request.

        Idempotent and safe from any thread. Reads in progress keep their open bodies until
        their readers close them; a request in flight closes the client again as it returns.
        """
        with self._lock:
            self._closed.set()
            client, self._client = self._client, None
        if client is not None:
            client.close()

    def _client_for(self, key: str) -> S3Client:
        """Return the boto3 client, creating it on the first call.

        The certificate bundle and the proxy are read first, outside the client's lock: on
        macOS, the system's proxy exceptions can make the proxy lookup resolve the endpoint's
        host name, and the lock is a leaf lock under which nothing waits on the network.
        Creating the client then reads local AWS configuration but makes no request, so it is
        done under the lock; concurrent first calls create exactly one client.

        Args:
            key: The key about to be requested, for the errors' `path`.

        Returns:
            The shared boto3 S3 client.

        Raises:
            WireError: If the client is closed, or boto3 cannot create the client (for
                example, `AWS_PROFILE` names a profile that does not exist).
        """
        with self._lock:
            self._refuse_if_closed(key)
            if self._client is not None:
                return self._client
        bundle = ca_file()
        proxy = https_proxy(self._host or "")
        with self._lock:
            # Another thread may have created the client, or closed this one, meanwhile.
            self._refuse_if_closed(key)
            if self._client is None:
                try:
                    self._client = self._create(bundle, proxy)
                except BotoCoreError as error:
                    raise WireError(
                        f"cannot create the S3 client for {self._endpoint_url}: {error}",
                        host=self._host,
                    ) from error
            return self._client

    def _refuse_if_closed(self, key: str) -> None:
        """Raise if `close()` has run; the caller holds the client's lock.

        Args:
            key: The key about to be requested, for the error's `path`.

        Raises:
            WireError: If the client is closed.
        """
        if self._closed.is_set():
            raise WireError(
                f"the flat-file client for {self._endpoint_url} is closed",
                host=self._host,
                path=key,
            )

    def _create(self, bundle: str, proxy: str | None) -> S3Client:
        """Create the boto3 S3 client.

        A dedicated boto3 session is created, because boto3's default session is not safe to
        create from several threads. Requests are signed with Signature Version 4, as Massive's
        endpoint requires, and the connection pool holds `max_pool` connections. botocore
        makes one attempt per call, so that `open` alone decides what is retried and every
        wait goes through the client's clock. The retry mode is fixed at `standard` because
        `AWS_RETRY_MODE=adaptive` would otherwise add botocore's client-side rate limiter,
        which sleeps on its own clock.

        Args:
            bundle: The certificate bundle (`ca_file()`) that verifies the endpoint and an
                HTTPS proxy.
            proxy: The proxy URL (`https_proxy()`), or None to connect directly.

        Returns:
            A client for the endpoint; it makes no request until it is used.
        """
        session = boto3.session.Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
        )
        return session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            verify=bundle,
            config=Config(
                signature_version="s3v4",
                max_pool_connections=self._max_pool,
                proxies=None if proxy is None else {"https": proxy},
                proxies_config={"proxy_ca_bundle": bundle},
                retries={"total_max_attempts": 1, "mode": "standard"},
            ),
        )

    def _refusal(self, error: ClientError, *, key: str, location: str) -> WireError | None:
        """Map an S3 error response to the wire error it means, or None for a missing object.

        Args:
            error: The SDK error, carrying the HTTP status, S3's code and message, and the
                response headers.
            key: The key that was requested.
            location: The object's location, for messages.

        Returns:
            None for HTTP 404. Otherwise the error to raise: `WireAuthError` for a 403 that
            rejects the credentials themselves, `WireEntitlementError` for any other 403, and
            `WireError` for any other status, each with the status, host, key, and
            `x-amz-request-id`.
        """
        metadata = error.response.get("ResponseMetadata", {})
        status = metadata.get("HTTPStatusCode")
        if status == HTTPStatus.NOT_FOUND:
            return None
        details = error.response.get("Error", {})
        code = details.get("Code", "")
        server = f"{code}: {details.get('Message', '')}"
        context: dict[str, Any] = {
            "status": status,
            "host": self._host,
            "path": key,
            "request_id": metadata.get("HTTPHeaders", {}).get("x-amz-request-id"),
        }
        if status != HTTPStatus.FORBIDDEN:
            return WireError(f"cannot read {location}: HTTP {status} ({server})", **context)
        refused = f"HTTP 403 for {location} ({server})"
        if code in _CREDENTIAL_ERRORS:
            return WireAuthError(refused, **context)
        return WireEntitlementError(refused, **context)
