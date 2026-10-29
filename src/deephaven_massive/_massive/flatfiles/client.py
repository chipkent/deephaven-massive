"""The client of Massive's flat-file bucket: one boto3 S3 client, its errors, and its close.

`FlatFileClient` opens the objects of Massive's S3 flat-file bucket (`flatfiles` at
`https://files.massive.com`) for `FlatFile` to decode, and lists the bucket's top-level
prefixes for the refresh's drift check (`prefixes`). It takes plain arguments, never the
library's configuration, and maps every S3 failure to the wire errors of `net.errors`:

- a 404 whose body is S3's error with the code `NoSuchKey` is no error: `open` returns None,
  and the library applies its missing-file policy; any other 404 (another S3 code, no code, or
  a body that is not S3's, such as the HTML "404 Not Found" page Massive's front end serves
  for a path it does not route) is a `WireError` naming the bucket and the key, so a wrong
  bucket or endpoint never looks like a missing file;
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
  `Proxy-Authorization`. With no proxy, botocore is told so explicitly and never consults the
  environment itself. Messages name the proxy by its URL without the user and password.
- **Retries.** botocore's own retries are off (one attempt per call). A request that fails
  before the first byte of the object arrives is retried on the schedule of `net.retry`, at
  most `MAX_RETRIES` times, with every wait taken on the client's `Clock`: HTTP 500, 502,
  503 (S3's `SlowDown`), and 504, S3's `RequestTimeout` error (HTTP 400), a connection that
  cannot be made, is reset, or times out, and an error response whose body is cut short or
  stalls. Each retry is logged at INFO and giving up at WARNING. A TLS failure is never
  retried, nor is a proxy's refusal of its credentials (HTTP 407: the same credentials would be
  refused again, and a corporate proxy may lock the account).
- **Rereads.** A failure part-way through an object's body is not retried here: the reader has
  already handed blocks on, which the client cannot take back. The read fails and
  `FlatFile.dropped` says why; a caller that has kept the file's rows to itself can discard
  them and read the object again from its start, waiting first with `wait_to_reread`, which
  follows the same schedule and limit.
- **Logging.** Each object request logs one DEBUG line on this module's logger: the object's
  location, `200` with its size or `404 NoSuchKey`, and S3's `x-amz-request-id`. A retry logs
  at INFO and giving up at WARNING (the call that fails logs the one ERROR); an error names
  S3's code and message, with the secret replaced by `***` should S3 ever quote it.

botocore parses the JSON files that describe S3 once per process, not once per client: every
client's botocore session is given the same model loader, which stays in memory for the life of
the process.

The body of an object reaches `FlatFile` through a small stream that turns botocore's and
urllib3's read errors (a connection dropped, reset, or broken at the TLS layer part-way
through the body) into `OSError`, so the reader needs no
botocore. This module imports only the standard library, `boto3`, `botocore`, `urllib3`, and
the rest of `flatfiles` and `net`; the package's interface resolves it only when
`FlatFileClient` is first used, so a session that reads no flat file never imports boto3.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import random
import threading
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final, TypeVar
from urllib.parse import urlsplit

import boto3
import botocore.loaders
import botocore.session
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ProxyConnectionError,
    ReadTimeoutError,
    SSLError,
)
from botocore.exceptions import ConnectionError as BotoConnectionError
from urllib3.exceptions import HTTPError, ProtocolError
from urllib3.exceptions import ReadTimeoutError as ResponseReadTimeoutError

from deephaven_massive._massive.flatfiles.reader import FlatFile
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
    is_proxy_auth_failure,
    is_tls_failure,
)
from deephaven_massive._massive.net.http import ca_file, https_proxy, proxy_credentials
from deephaven_massive._massive.net.redaction import redact
from deephaven_massive._massive.net.retry import (
    MAX_RETRIES,
    SYSTEM_CLOCK,
    TRANSIENT_STATUSES,
    transient_wait,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from botocore.response import StreamingBody
    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import ListObjectsV2OutputTypeDef

    from deephaven_massive._massive.net.retry import Clock


__all__ = ["FlatFileClient"]

_log = logging.getLogger(__name__)

# What one request gives its caller: an object's body, or a page of a listing.
_Answer = TypeVar("_Answer")

# The source of retry jitter. `SystemRandom` draws from the OS and keeps no state, so threads
# share it safely; jitter needs no cryptographic strength, only independence.
_JITTER = random.SystemRandom()

_MISSING_KEY: Final = "NoSuchKey"
"""The S3 error code, answered with HTTP 404, of an object the bucket does not hold."""

_CREDENTIAL_ERRORS: Final = frozenset({"InvalidAccessKeyId", "SignatureDoesNotMatch"})
"""S3 error codes, answered with HTTP 403, that mean the S3 credentials themselves are wrong."""

_TRANSIENT_CODES: Final = frozenset({"RequestTimeout"})
"""S3 error codes, answered with a status that is not transient (400), that a retry may fix."""

_CONNECTION_ERRORS: Final = (BotoConnectionError, ConnectionClosedError, ReadTimeoutError)
"""botocore's errors for a connection that cannot be made, is reset, or times out.

Its other `HTTPClientError`s are not among them: botocore raises the bare class for any
unexpected exception inside the HTTP layer, which a retry would only repeat."""

_RESPONSE_CUT_SHORT: Final = (ProtocolError, ResponseReadTimeoutError)
"""urllib3's errors for an error response whose body is reset or stalls.

botocore reads an error response's body outside its own error handling, so these reach the
caller unwrapped. No byte of the object has arrived, so the request is retried."""

_CONNECT_TIMEOUT: Final = 60.0
"""Seconds to wait for a connection to the endpoint or the proxy (botocore's default)."""

_READ_TIMEOUT: Final = 60.0
"""Seconds to wait for each read of a response (botocore's default)."""


class _ModelLoader:
    """botocore's model loader, shared by every S3 client the process builds.

    botocore describes S3, its endpoints, and its retry rules in JSON files it ships, and a
    fresh botocore session reads and parses them again for every client it builds: about
    100 ms of CPU per client. One loader, registered on each client's botocore session, reads
    them once per process; every later client is built in about a tenth of that time.

    The loader is created with this object, when the module is imported, which reads no file;
    the first client build parses the models, and the loader keeps them in memory for the life
    of the process (about 12 MB). It searches botocore's own data and `~/.aws/models`, as
    botocore does by default; the `data_path` setting (`AWS_DATA_PATH`) is not consulted.

    Each boto3 session appends boto3's own data directory to the loader's search paths when it
    is created. `building()` removes the copies a build adds, keeping each directory once, so
    the list stays the length it had after the first build however many clients are built.

    Thread-safety: botocore's loader is not safe to share between threads that are loading.
    It fills its cache without a lock, on the first load of S3's model it merges S3's extra
    definitions into the cached copy in place, and boto3 writes its search paths. Every use of
    the loader therefore happens inside `building()`, which holds this object's lock while one
    client is built. A built client reads the loader again only for paginators and waiters,
    which `FlatFileClient` never uses. The lock is taken under `FlatFileClient`'s own lock and
    takes no other lock, and no network call or wait happens while it is held.
    """

    __slots__ = ("_loader", "_lock")

    def __init__(self) -> None:
        """Create the holder and its loader; nothing is read until the first build."""
        self._lock = threading.Lock()
        self._loader = botocore.loaders.create_loader()

    @contextlib.contextmanager
    def building(self) -> Iterator[botocore.loaders.Loader]:
        """Hold the lock for building one client and yield the shared loader.

        The caller registers the loader on the client's botocore session and builds the client
        before leaving the block, so no two clients read the loader at once. On leaving, every
        search path the build added that the loader already had is removed again (boto3 adds
        its data directory on each session), even if the build failed.

        Yields:
            The process-wide loader.
        """
        with self._lock:
            paths = self._loader.search_paths
            before = list(paths)
            try:
                yield self._loader
            finally:
                kept = list(before)
                for path in paths[len(before) :]:
                    if path not in kept:
                        kept.append(path)
                paths[:] = kept


_MODEL_LOADER: Final = _ModelLoader()
"""The one botocore model loader of the process, shared by every `FlatFileClient`."""


@dataclass(frozen=True)
class _Retryable:
    """An attempt that failed before the object's first byte, in a way a retry may fix.

    Attributes:
        error: The error to raise if no retry is left.
        cause: The botocore error behind it, chained as the raised error's cause; None when
            its message quotes the secret, so the chain never shows it.
    """

    error: WireError
    cause: Exception | None


class _Body:
    """An object's response body, read as a plain binary stream.

    botocore raises its own errors when the connection drops part-way through a body, and
    lets some of urllib3's through unwrapped (a TLS failure while reading, which urllib3
    raises as its `SSLError`); this stream raises all of them as `OSError` (chained to the
    original), the error every other stream
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
        except (BotoCoreError, HTTPError) as error:
            raise OSError(str(error)) from error

    def close(self) -> None:
        """Close the body, which returns or drops its connection. Idempotent."""
        self._body.close()


class FlatFileClient:
    """The connection to Massive's flat-file bucket: one S3 client, created on first use.

    The library's session creates one on its first flat-file read and closes it when the
    session closes. The boto3 client is created by the first request, under the client's own
    lock (no network call and no wait happen while it is held: the credentials are given, the
    proxy is decided before the lock is taken, and the `legacy` defaults mode keeps boto3 from
    asking the instance-metadata service for a region), and shared by every reader thread,
    since boto3 clients are thread-safe. Every client in the process reads botocore's model
    files through one shared loader, so only the first pays to parse them; building a client
    takes that loader's lock, a leaf lock, inside the client's own. Once `close()` has run, no
    request starts; a request already in flight when the client closed is answered, and its
    connection pool is closed once more as it returns, so nothing it opened outlives the close.

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
        "_proxy",
        "_rereads",
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
        when the first request creates the boto3 client.

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
        # The proxy the client was created with, without its user and password, for messages.
        self._proxy: str | None = None
        self._closed = threading.Event()
        # The cancel event of every `wait_to_reread` in progress, which `close()` sets. A list,
        # since the readers of one call share their event; guarded by `_lock`.
        self._rereads: list[threading.Event] = []

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
        the endpoint answers a 404 that is not S3's `NoSuchKey` (for a wrong bucket, Massive's
        front end serves an HTML page, which reads as `"cannot read s3://flatfile/<key> at
        https://files.massive.com: HTTP 404 (404: Not Found)"`), or S3 answers any other
        status but 200 and 404. On HTTP 403 it is the subclass `WireAuthError` when S3 rejects the
        credentials themselves, and otherwise `WireEntitlementError`: the plan lacks the files.
        The message names the object and quotes S3's error code and message, such as `"HTTP
        403 for s3://flatfiles/<key> at https://files.massive.com (AccessDenied: Access
        Denied)"`.

        Args:
            key: The object key (`object_key`).

        Returns:
            The object, open for reading, or None if S3 answered 404 `NoSuchKey`.
        """
        location = f"s3://{self._bucket}/{key} at {self._endpoint_url}"

        def get(client: S3Client) -> StreamingBody | None:
            try:
                answer = client.get_object(Bucket=self._bucket, Key=key)
            except ClientError as error:
                code = error.response.get("Error", {}).get("Code")
                if _status(error) == HTTPStatus.NOT_FOUND and code == _MISSING_KEY:
                    _log.debug(
                        "GET %s -> 404 NoSuchKey (request_id %s)", location, _request_id(error)
                    )
                    return None
                raise
            _log.debug(
                "GET %s -> 200, %s bytes (request_id %s)",
                location,
                answer.get("ContentLength"),
                answer.get("ResponseMetadata", {}).get("RequestId"),
            )
            return answer["Body"]

        body = self._request(get, key=key, location=location, verb="read")
        if body is None:
            return None
        return FlatFile(_Body(body), key, location=location, host=self._host)

    def wait_to_reread(self, error: WireError, retry: int, cancel: threading.Event) -> bool:
        """Wait before reading an object again whose body failed part-way, if a reread is left.

        A caller that read an object, kept every row of it to itself, and lost the connection
        part-way through (`FlatFile.dropped`) calls this before it opens the object again and
        reads it from the start. Rereads follow the schedule of `open`'s retries: at most
        three (`retry` 0, 1, 2), after 0.5, 1, then 2 seconds plus up to 20 % jitter, on the
        client's clock. Each is logged at INFO and giving up at WARNING, naming the failure.
        Blocks the calling thread for the wait, which ends early once `cancel` is set, by the
        caller or by `close()`; the caller then finds the call stopped or the client closed.
        When `cancel` is already set or the client already closed, it returns at once, with
        no wait and no log line, since no reread will follow.

        Args:
            error: The failed read's error, for the log.
            retry: How many rereads of this object have already happened.
            cancel: The caller's event that ends the wait. `close()` sets it, so it should be
                one that stops the caller's work, never one shared with unrelated work.

        Returns:
            True once the wait has ended: read the object again. False when the rereads are
            spent, after logging at WARNING, without waiting: raise `error`.
        """
        if retry == MAX_RETRIES:
            _log.warning("%s; giving up after %d reads", error, retry + 1)
            return False
        with self._lock:
            if self._closed.is_set() or cancel.is_set():
                return True
            self._rereads.append(cancel)
        wait = transient_wait(retry, _JITTER.random())
        _log.info(
            "%s; reading it again from its start, reread %d of %d in %.2f s",
            error,
            retry + 1,
            MAX_RETRIES,
            wait,
        )
        try:
            self._clock.wait(cancel, wait)
        finally:
            with self._lock:
                self._rereads.remove(cancel)
        return True

    def prefixes(self) -> list[str]:
        """List the bucket's top-level prefixes: the first segment of every object key.

        The refresh's drift check calls it to find what the bucket holds that nothing names.
        It sends `ListObjectsV2` with the delimiter `/` and follows every continuation token,
        so a bucket with more prefixes than one page holds (1000) is listed whole. Each page is
        one request, retried on the schedule `open` uses when it fails before its answer
        arrives, and refused once the client is closed; each blocks the calling thread for its
        round trip and any retry waits, and `close()` ends a retry wait at once. The first
        request of either method creates the boto3 client.

        Errors: as for `open`, except that every HTTP 404 is a `WireError`, whatever its
        code. A message names the bucket instead of an object, such as `"HTTP 403 for
        s3://flatfiles/ at https://files.massive.com (AccessDenied: Access Denied)"`, and its
        `path` is `""`. A page that says the listing goes on but gives no continuation token,
        or repeats the one it was asked with, is a `WireError` too: following it would stop
        early or never end.

        Returns:
            The prefixes without their trailing `/` (`"us_stocks_sip"`), in the order S3 lists
            them (UTF-8 binary order).

        Raises:
            WireError: For a truncated page without a new continuation token; the request
                errors, described above, propagate from each page's request.
        """
        location = f"s3://{self._bucket}/ at {self._endpoint_url}"
        found: list[str] = []
        token: str | None = None
        while True:
            page = self._request(
                functools.partial(self._list_page, token=token),
                key="",
                location=location,
                verb="list",
            )
            found += [entry["Prefix"].removesuffix("/") for entry in page.get("CommonPrefixes", [])]
            # Read as plain data: a server other than S3 may leave either field out.
            fields: Mapping[str, object] = page
            if not fields.get("IsTruncated"):
                return found
            following = fields.get("NextContinuationToken")
            if not isinstance(following, str) or not following or following == token:
                fault = "the same" if isinstance(following, str) and following else "no"
                raise WireError(
                    f"cannot list {location}: a truncated page gave {fault} continuation token",
                    host=self._host,
                )
            token = following

    def _list_page(self, client: S3Client, *, token: str | None) -> ListObjectsV2OutputTypeDef:
        """Request one page of the bucket's top-level prefixes.

        Args:
            client: The boto3 client.
            token: The continuation token the previous page ended with; None for the first.

        Returns:
            The page as botocore parses it.
        """
        if token is None:
            return client.list_objects_v2(Bucket=self._bucket, Delimiter="/")
        return client.list_objects_v2(Bucket=self._bucket, Delimiter="/", ContinuationToken=token)

    def _request(
        self, call: Callable[[S3Client], _Answer], *, key: str, location: str, verb: str
    ) -> _Answer:
        """Send one request, retrying a failure before its answer on the shared schedule.

        Args:
            call: Sends the request on the boto3 client and returns what the caller needs of
                the answer. A `ClientError` it lets through is mapped to a wire error here.
            key: The object key the request concerns, or `""` for the bucket, for the errors'
                `path`.
            location: The object's or bucket's location, for messages.
            verb: What the request does (`"read"`, `"list"`), for messages.

        A failure no retry can fix raises its `WireError` at once, as `open` describes, and so
        does a retryable one, the last attempt's, once the retries are spent.

        Returns:
            What `call` returned.
        """
        retry = 0
        while True:
            outcome = self._attempt(call, key=key, location=location, verb=verb)
            if not isinstance(outcome, _Retryable):
                return outcome
            if retry == MAX_RETRIES:
                _log.warning("%s; giving up after %d attempts", outcome.error, retry + 1)
                raise outcome.error from outcome.cause
            wait = transient_wait(retry, _JITTER.random())
            _log.info("%s; retry %d of %d in %.2f s", outcome.error, retry + 1, MAX_RETRIES, wait)
            # A close ends the wait at once; the next attempt then refuses to start.
            self._clock.wait(self._closed, wait)
            retry += 1

    def _attempt(
        self, call: Callable[[S3Client], _Answer], *, key: str, location: str, verb: str
    ) -> _Answer | _Retryable:
        """Send one attempt of a request.

        Args:
            call: Sends the request on the boto3 client.
            key: The object key, or `""` for the bucket.
            location: The object's or bucket's location, for messages.
            verb: What the request does, for messages.

        Returns:
            What `call` returned, or the failure to retry.

        Raises:
            WireError: When the failure is not retryable; see `open`.
        """
        client, proxy = self._client_for(key)
        try:
            return call(client)
        except ClientError as error:
            refusal = self._refusal(error, key=key, location=location, verb=verb)
            details = error.response.get("Error", {})
            # botocore's own message quotes S3's words as sent; were the secret among them,
            # chaining it would put the secret back in the traceback, so it is left out.
            quotes_secret = redact(str(error), self._secret_access_key) != str(error)
            cause = None if quotes_secret else error
            if refusal.status in TRANSIENT_STATUSES or details.get("Code") in _TRANSIENT_CODES:
                return _Retryable(refusal, cause)
            raise refusal from cause
        except BotoCoreError as error:
            if is_tls_failure(error):
                raise WireError(
                    f"cannot {verb} {location}: TLS certificate verification failed for "
                    f"{self._host} ({error}); if a proxy or private CA signs it, set "
                    "SSL_CERT_FILE to a bundle that trusts it",
                    host=self._host,
                    path=key,
                ) from error
            if is_proxy_auth_failure(error):
                # The same credentials would be refused again, and a corporate proxy may lock
                # the account after a few refusals.
                raise WireError(
                    f"cannot {verb} {location}: the proxy {proxy} refused its credentials "
                    "(HTTP 407); check the user and password in HTTPS_PROXY",
                    status=HTTPStatus.PROXY_AUTHENTICATION_REQUIRED,
                    host=self._host,
                    path=key,
                ) from error
            if isinstance(error, SSLError):
                # Any other TLS failure (a protocol mismatch, a handshake the server ends) is
                # a configuration problem a retry cannot fix either.
                raise WireError(
                    f"cannot {verb} {location}: TLS failed for {self._host} ({error})",
                    host=self._host,
                    path=key,
                ) from error
            failure = WireError(
                f"cannot {verb} {location}: {_describe(error, proxy)}", host=self._host, path=key
            )
            if isinstance(error, _CONNECTION_ERRORS):
                return _Retryable(failure, error)
            raise failure from error
        except _RESPONSE_CUT_SHORT as error:
            return _Retryable(
                WireError(
                    f"cannot {verb} {location}: the error response was cut short ({error})",
                    host=self._host,
                    path=key,
                ),
                error,
            )
        finally:
            if self._closed.is_set():
                # The client was closed while this request was in flight, and the request may
                # have opened a new connection pool: close it again now that it has returned.
                client.close()

    def close(self) -> None:
        """Close the boto3 client, if one was created, and refuse every further request.

        Idempotent and safe from any thread. Reads in progress keep their open bodies until
        their readers close them; a request in flight closes the client again as it returns.
        Every `wait_to_reread` in progress ends at once: its `cancel` event is set.
        """
        with self._lock:
            self._closed.set()
            client, self._client = self._client, None
            waiting = list(self._rereads)
        for cancel in waiting:
            cancel.set()
        if client is not None:
            client.close()

    def _client_for(self, key: str) -> tuple[S3Client, str | None]:
        """Return the boto3 client and its proxy, creating the client on the first call.

        The certificate bundle and the proxy are read first, outside the client's lock: on
        macOS, the system's proxy exceptions can make the proxy lookup resolve the endpoint's
        host name, and nothing waits on the network under the lock. Creating the client then
        reads local AWS configuration but makes no request, so it is done under the lock
        (taking the shared model loader's leaf lock inside it); concurrent first calls create
        exactly one client.

        Args:
            key: The key about to be requested, for the errors' `path`.

        Returns:
            The shared boto3 S3 client, and the URL of its proxy without the user and password
            (None when it connects directly), for messages.

        Raises:
            WireError: If the client is closed, or boto3 cannot create the client (for
                example, `AWS_PROFILE` names a profile that does not exist).
        """
        with self._lock:
            self._refuse_if_closed(key)
            if self._client is not None:
                return self._client, self._proxy
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
                self._proxy = None if proxy is None else proxy_credentials(proxy)[0]
            return self._client, self._proxy

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
        which sleeps on its own clock. With no proxy, `proxies` is an empty mapping rather than
        None, because None makes botocore look the proxy up itself, under the client's lock
        and from more variables (`HTTP_PROXY`, `ALL_PROXY`) than the other clients read. The
        session's defaults mode is fixed at `legacy` because `AWS_DEFAULTS_MODE=auto` would
        make boto3 ask the instance-metadata service for the region, a network call under the
        lock. The session reads botocore's model files through the process-wide loader
        (`_ModelLoader`), whose lock is held, under the client's lock, while the client is
        built: the first client of the process parses the files, and every later one reuses
        them. The boto3 session appends boto3's data directory to the shared loader's search
        paths; leaving the loader's block removes that copy again once the path is listed.

        Args:
            bundle: The certificate bundle (`ca_file()`) that verifies the endpoint and an
                HTTPS proxy.
            proxy: The proxy URL (`https_proxy()`), or None to connect directly.

        Returns:
            A client for the endpoint; it makes no request until it is used.
        """
        with _MODEL_LOADER.building() as loader:
            core = botocore.session.get_session()
            core.register_component("data_loader", loader)
            # A session setting outranks AWS_DEFAULTS_MODE and the user's AWS configuration.
            core.set_config_variable("defaults_mode", "legacy")
            session = boto3.session.Session(
                aws_access_key_id=self._access_key_id,
                aws_secret_access_key=self._secret_access_key,
                botocore_session=core,
            )
            return session.client(
                "s3",
                endpoint_url=self._endpoint_url,
                verify=bundle,
                config=Config(
                    signature_version="s3v4",
                    max_pool_connections=self._max_pool,
                    connect_timeout=_CONNECT_TIMEOUT,
                    read_timeout=_READ_TIMEOUT,
                    proxies={} if proxy is None else {"https": proxy},
                    proxies_config={"proxy_ca_bundle": bundle},
                    retries={"total_max_attempts": 1, "mode": "standard"},
                ),
            )

    def _refusal(self, error: ClientError, *, key: str, location: str, verb: str) -> WireError:
        """Map an S3 error response to the wire error it means.

        Args:
            error: The SDK error, carrying the HTTP status, S3's code and message, and the
                response headers.
            key: The key that was requested, or `""` for the bucket.
            location: The object's or bucket's location, for messages.
            verb: What the request did (`"read"`, `"list"`), for messages.

        Returns:
            The error to raise: `WireAuthError` for a 403 that rejects the credentials
            themselves, `WireEntitlementError` for any other 403, and `WireError` for any other
            status, each with the status, host, key, and `x-amz-request-id`.
        """
        status = _status(error)
        details = error.response.get("Error", {})
        code = details.get("Code", "")
        # S3 never echoes the secret, but its words are the server's, so they are cleaned.
        server = redact(f"{code}: {details.get('Message', '')}", self._secret_access_key)
        context: dict[str, Any] = {
            "status": status,
            "host": self._host,
            "path": key,
            "request_id": _request_id(error),
        }
        if status != HTTPStatus.FORBIDDEN:
            return WireError(f"cannot {verb} {location}: HTTP {status} ({server})", **context)
        refused = f"HTTP 403 for {location} ({server})"
        if code in _CREDENTIAL_ERRORS:
            return WireAuthError(refused, **context)
        return WireEntitlementError(refused, **context)


def _request_id(error: ClientError) -> str | None:
    """Return S3's `x-amz-request-id` of an error response.

    Args:
        error: The SDK error.

    Returns:
        The request id, or None when the response had none.
    """
    request_id: str | None = (
        error.response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-request-id")
    )
    return request_id


def _status(error: ClientError) -> int | None:
    """Return the HTTP status of an S3 error response.

    Args:
        error: The SDK error.

    Returns:
        The status botocore recorded, or None when it recorded none.
    """
    status: int | None = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status


def _describe(error: BotoCoreError, proxy: str | None) -> str:
    """Describe a botocore failure for a message, never with a proxy's user and password.

    botocore masks the password in a proxy URL by replacing its first occurrence, so a
    password that also appears earlier in the URL (`http://h:h@proxy`) stays in its message.
    A failure to reach the proxy is therefore described from the proxy URL without its user
    and password, and the underlying error, which names no credential.

    Args:
        error: The failure.
        proxy: The proxy URL without its user and password, or None.

    Returns:
        The description.
    """
    if isinstance(error, ProxyConnectionError):
        return f"cannot connect through the proxy {proxy} ({error.kwargs.get('error')})"
    return str(error)
