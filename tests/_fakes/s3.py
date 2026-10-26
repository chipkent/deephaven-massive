"""A fake of Massive's flat-file S3 endpoint, served inside botocore's own request pipeline.

The library builds a real boto3 S3 client from its configuration; `FakeS3.attach` registers two
botocore event handlers on that client, so every `GetObject` is answered in process, after the
SDK has validated and serialized the call and before anything is signed or sent. Answers have
the shape S3 itself gives: a `StreamingBody` for an object, and for an error an HTTP status
with S3's error code and message, which botocore raises as `ClientError`. Nothing leaves the
process.

Objects are stored by key. A key can also be refused with any status (403 `AccessDenied` for a
plan without the dataset, 500 for a server error), fail before any response (a connection
error), or fail part-way through its body (a dropped connection while streaming). Every request
is recorded. Imported lazily by the `fake_s3` fixture in `tests/conftest.py`; thread-safe, since
the library reads several keys at once.
"""

from __future__ import annotations

import io
import threading
from typing import TYPE_CHECKING, Any, cast

from botocore.awsrequest import AWSResponse, HTTPHeaders
from botocore.response import StreamingBody
from urllib3.exceptions import ProtocolError

from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable

    from types_boto3_s3.client import S3Client

_CONTEXT_KEY = "fake_s3_object"


class _DroppedStream(io.BytesIO):
    """A response stream that serves its bytes and then fails as a dropped connection does."""

    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            raise ProtocolError("Connection broken: IncompleteRead")
        return chunk


class FakeS3:
    """An in-memory flat-file bucket that answers the library's S3 clients."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._objects: dict[str, bytes] = {}
        self._dropped: dict[str, bytes] = {}
        self._refusals: dict[str, tuple[int, str, str]] = {}
        self._failures: dict[str, Exception] = {}
        self.requests: list[tuple[str, str]] = []
        self.clients: list[dict[str, object]] = []

    def put(self, key: str, data: bytes) -> None:
        """Store an object."""
        with self._lock:
            self._objects[key] = data

    def put_dropped(self, key: str, data: bytes) -> None:
        """Store an object whose body fails after `data`, as a connection dropped mid-read."""
        with self._lock:
            self._dropped[key] = data

    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None:
        """Answer every key under `prefix` with an S3 error response."""
        with self._lock:
            self._refusals[prefix] = (status, code, message)

    def fail(self, key: str, error: Exception) -> None:
        """Raise `error` for `key` before any response, as a connection failure does."""
        with self._lock:
            self._failures[key] = error

    def keys(self) -> list[str]:
        """Return the keys requested so far.

        Returns:
            The keys, in request order.
        """
        with self._lock:
            return [key for _, key in self.requests]

    def attach(self, client: S3Client) -> S3Client:
        """Serve `client`'s `GetObject` calls from this fake.

        Returns:
            The client, now answered by the fake.
        """
        events = client.meta.events
        events.register("before-parameter-build.s3.GetObject", self._remember)
        # A `before-call` handler that returns a response replaces the HTTP request.
        events.register("before-call.s3.GetObject", cast("Callable[..., None]", self._answer))
        # The client's settings, read through private attributes the type stubs do not declare.
        settings = cast("Any", client.meta.config)
        credentials = cast("Any", client)._request_signer._credentials
        recorded: dict[str, object] = {
            "endpoint_url": client.meta.endpoint_url,
            "access_key": credentials.access_key,
            "secret_key": credentials.secret_key,
            "signature_version": settings.signature_version,
            "max_pool_connections": settings.max_pool_connections,
        }
        with self._lock:
            self.clients.append(recorded)
        return client

    def _remember(self, params: dict[str, Any], context: dict[str, Any], **_: object) -> None:
        context[_CONTEXT_KEY] = (params["Bucket"], params["Key"])

    def _answer(self, context: dict[str, Any], **_: object) -> tuple[AWSResponse, dict[str, Any]]:
        bucket, key = context[_CONTEXT_KEY]
        with self._lock:
            self.requests.append((bucket, key))
            failure = self._failures.get(key)
            refusal = next(
                (r for prefix, r in self._refusals.items() if key.startswith(prefix)), None
            )
            data = self._objects.get(key)
            dropped = self._dropped.get(key)
        if failure is not None:
            raise failure
        if refusal is not None:
            status, code, message = refusal
            return _error(status, code, message)
        if dropped is not None:
            return _ok(StreamingBody(_DroppedStream(dropped), len(dropped) + 1))
        if data is None:
            return _error(404, "NoSuchKey", "The specified key does not exist.")
        return _ok(StreamingBody(io.BytesIO(data), len(data)))


def _ok(body: StreamingBody) -> tuple[AWSResponse, dict[str, Any]]:
    return AWSResponse("", 200, HTTPHeaders(), None), {
        "Body": body,
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }


def _error(status: int, code: str, message: str) -> tuple[AWSResponse, dict[str, Any]]:
    return AWSResponse("", status, HTTPHeaders(), None), {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
