"""The client of Massive's flat-file bucket: one boto3 S3 client, its errors, and its close.

`FlatFileClient` opens the objects of Massive's S3 flat-file bucket (`flatfiles` at
`https://files.massive.com`) for `FlatFile` to decode. It takes plain arguments, never the
library's configuration, and maps every S3 failure to the wire errors of `net.errors`:

- a 404 is no error: `open` returns None, and the library applies its missing-file policy;
- a 403 whose S3 error code says the credentials themselves are wrong (`InvalidAccessKeyId`,
  `SignatureDoesNotMatch`) is `WireAuthError`;
- any other 403 is `WireEntitlementError`, naming the futures venue when there is one;
- anything else (another status, an unreachable endpoint, a client that cannot be created) is
  `WireError`.

Each error carries the HTTP status, the endpoint's host, the object key, and the
`x-amz-request-id` of the response as its `request_id`. Credentials are never logged or put in
a message. This module imports only the standard library, `boto3`, `botocore`, and the rest of
`flatfiles` and `net`.
"""

from __future__ import annotations

import threading
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final, NamedTuple
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from deephaven_massive._massive.flatfiles.reader import FlatFile
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = ["FlatFileClient"]

_CREDENTIAL_ERRORS: Final = frozenset({"InvalidAccessKeyId", "SignatureDoesNotMatch"})
"""S3 error codes, answered with HTTP 403, that mean the S3 credentials themselves are wrong."""


class _S3(NamedTuple):
    """What the client uses of a boto3 S3 client, as bound methods of the client.

    Holding the two methods rather than the client keeps the stubs' own types
    (`types_boto3_s3`) out of this package's imports while boto3's stubs still check, where the
    client is created, that both methods exist.
    """

    get_object: Callable[..., Mapping[str, Any]]
    """`S3Client.get_object`: request one object."""
    close: Callable[[], None]
    """`S3Client.close`: close the client's connection pool."""


class FlatFileClient:
    """The connection to Massive's flat-file bucket: one S3 client, created on first use.

    The library creates one per session (`SessionCore`) and closes it when the session closes.
    The boto3 client is created by the first `open`, under the client's own leaf lock (no
    network call and no wait happen while it is held), and shared by every reader thread,
    since boto3 clients are thread-safe. Once `close()` has run, no request starts; a request
    already in flight when the client closed is answered, and its connection pool is closed
    once more as it returns, so nothing it opened outlives the close.

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
        access_key_id: str | None,
        secret_access_key: str | None,
        endpoint_url: str,
        bucket: str,
        max_pool: int,
    ) -> None:
        """Remember the settings; no client is created and nothing touches the network.

        Args:
            access_key_id: The S3 access key id, or None to let boto3 find credentials itself.
            secret_access_key: The S3 secret access key, or None.
            endpoint_url: The S3 endpoint, such as `"https://files.massive.com"`.
            bucket: The bucket, such as `"flatfiles"`.
            max_pool: The most connections the client keeps open: one per concurrent reader.
        """
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._endpoint_url = endpoint_url
        self._host = urlsplit(endpoint_url).hostname
        self._bucket = bucket
        self._max_pool = max_pool
        self._lock = threading.Lock()
        self._client: _S3 | None = None
        self._closed = threading.Event()

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        return self._closed.is_set()

    def open(self, key: str, *, venue: str | None = None) -> FlatFile | None:
        """Request one object and return it for reading, or None if it does not exist.

        Blocks for one HTTP round trip; the body is read later, through the returned file. The
        first call creates the boto3 client.

        Args:
            key: The object key (`object_key`).
            venue: The futures venue of the key (`"cme"`, ...), named in an entitlement error;
                None otherwise.

        Returns:
            The object, open for reading, or None if the bucket answered 404.

        Raises:
            WireError: If the client is closed (no request is made), cannot be created (for
                example, `AWS_PROFILE` names a profile that does not exist), the endpoint is
                unreachable, or S3 answers any status but 200 and 404. On HTTP 403 it is the
                subclass `WireAuthError` when S3 rejects the credentials themselves (the message
                names `s3_access_key_id` and `s3_secret_access_key`), and otherwise
                `WireEntitlementError`: the plan lacks the files, and for a futures key the
                message names the venue and the `venue=` parameter. S3's error code and message
                are quoted.
        """
        client = self._client_for(key)
        location = f"s3://{self._bucket}/{key} at {self._endpoint_url}"
        try:
            body = client.get_object(Bucket=self._bucket, Key=key)["Body"]
        except ClientError as error:
            refusal = self._refusal(error, key=key, venue=venue, location=location)
            if refusal is None:
                return None
            raise refusal from error
        except BotoCoreError as error:
            raise WireError(
                f"cannot read {location}: {error}", host=self._host, path=key
            ) from error
        finally:
            if self._closed.is_set():
                # The client was closed while this request was in flight, and the request may
                # have opened a new connection pool: close it again now that it has returned.
                client.close()
        return FlatFile(body, key, location=location, host=self._host)

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

    def _client_for(self, key: str) -> _S3:
        """Return the boto3 client, creating it on the first call.

        Creating the client reads local AWS configuration but makes no request, so it is done
        under the client's lock; concurrent first calls create exactly one client.

        Args:
            key: The key about to be requested, for the errors' `path`.

        Returns:
            The shared boto3 S3 client.

        Raises:
            WireError: If the client is closed, or boto3 cannot create the client.
        """
        with self._lock:
            if self._closed.is_set():
                raise WireError(
                    f"the flat-file client for {self._endpoint_url} is closed",
                    host=self._host,
                    path=key,
                )
            if self._client is None:
                try:
                    self._client = self._create()
                except BotoCoreError as error:
                    raise WireError(
                        f"cannot create the S3 client for {self._endpoint_url}: {error}",
                        host=self._host,
                    ) from error
            return self._client

    def _create(self) -> _S3:
        """Create the boto3 S3 client.

        A dedicated boto3 session is created, because boto3's default session is not safe to
        create from several threads. Requests are signed with Signature Version 4, as Massive's
        endpoint requires, and the connection pool holds `max_pool` connections.

        Returns:
            A client for the endpoint; it makes no request until it is used.
        """
        session = boto3.session.Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
        )
        client = session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            config=Config(signature_version="s3v4", max_pool_connections=self._max_pool),
        )
        return _S3(client.get_object, client.close)

    def _refusal(
        self, error: ClientError, *, key: str, venue: str | None, location: str
    ) -> WireError | None:
        """Map an S3 error response to the wire error it means, or None for a missing object.

        Args:
            error: The SDK error, carrying the HTTP status, S3's code and message, and the
                response headers.
            key: The key that was requested.
            venue: The futures venue of the key, or None.
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
        if code in _CREDENTIAL_ERRORS:
            return WireAuthError(
                f"S3 rejected the flat-file credentials: HTTP 403 for {location} ({server}); "
                "check MassiveConfig s3_access_key_id and s3_secret_access_key",
                **context,
            )
        if venue is None:
            return WireEntitlementError(
                f"the plan does not include these flat files: HTTP 403 for {location} ({server})",
                **context,
            )
        return WireEntitlementError(
            f"the plan does not include {venue.upper()} futures flat files: HTTP 403 for "
            f"{location} ({server}); pass venue=dm.FuturesVenue.<VENUE> for a venue the "
            "plan includes",
            **context,
        )
