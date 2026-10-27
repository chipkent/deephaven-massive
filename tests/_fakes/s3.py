"""A fake of Massive's flat-file S3 endpoint, served inside botocore's own request pipeline.

The library builds a real boto3 S3 client from its configuration. `FakeS3.install` wraps
boto3's client factory for the length of a test, so every S3 client created meanwhile is passed
to `FakeS3.attach`, which registers two botocore event handlers on it: every `GetObject` is then
answered in process, after the SDK has validated and serialized the call and before anything is
signed or sent. Answers have the shape S3 itself gives: a `StreamingBody` for an object, and for
an error an HTTP status with S3's error code and message and an `x-amz-request-id` header,
which botocore raises as `ClientError`. Nothing leaves the process.

Objects are stored by key. A key can also be refused with any status (403 `AccessDenied` for a
plan without the dataset, 500 for a server error), fail before any response by name
(`"unreachable"`: the endpoint cannot be reached), or fail part-way through its body (a dropped
connection while streaming). Every request, every client close, and every body close is
recorded. Built by the `fake_s3` fixture in `tests/conftest.py`; thread-safe, since the library
reads several keys at once.
"""

from __future__ import annotations

import io
import threading
from typing import TYPE_CHECKING, Any, Final, cast

import boto3
from botocore.awsrequest import AWSResponse, HTTPHeaders
from botocore.exceptions import EndpointConnectionError
from botocore.response import StreamingBody
from urllib3.exceptions import ProtocolError

from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest
    from types_boto3_s3.client import S3Client

_CONTEXT_KEY = "fake_s3_object"

REQUEST_ID: Final = "4442587FB7D0A2F9"
"""The `x-amz-request-id` of every error answer."""

_FAILURES: Final = frozenset({"unreachable"})
"""The failures `FakeS3.fail` raises by name before any response."""


class _DroppedStream(io.BytesIO):
    """A response stream that serves its bytes and then fails as a dropped connection does."""

    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            raise ProtocolError("Connection broken: IncompleteRead")
        return chunk


class _RecordedBody(StreamingBody):
    """A response body that tells the fake when it is closed."""

    def __init__(self, raw: io.BytesIO, length: int, closed: Callable[[], None]) -> None:
        super().__init__(raw, length)
        self._closed = closed

    @override
    def close(self) -> None:
        self._closed()
        super().close()


class FakeS3:
    """An in-memory flat-file bucket that answers the library's S3 clients."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._objects: dict[str, bytes] = {}
        self._dropped: dict[str, bytes] = {}
        self._refusals: dict[str, tuple[int, str, str]] = {}
        self._failures: dict[str, str] = {}
        self.requests: list[tuple[str, str]] = []
        self.clients: list[dict[str, object]] = []
        self.closes = 0
        self.closed_bodies: list[str] = []
        self.request_id = REQUEST_ID
        # Called with the key on the requesting thread before each answer, so a test can hold a
        # read in flight or act at that moment; set it before the library's first read.
        self.before_answer: Callable[[str], object] | None = None

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Attach this fake to every S3 client boto3 creates until the test ends.

        The library's own client factory still runs (credentials, endpoint, signing
        configuration); only the requests are answered here.

        Args:
            monkeypatch: The test's monkeypatch, which undoes the wrapping afterwards.
        """
        create = cast("Callable[..., object]", boto3.session.Session.client)

        def client(session: boto3.session.Session, *args: object, **kwargs: object) -> object:
            return self.attach(cast("S3Client", create(session, *args, **kwargs)))

        monkeypatch.setattr(boto3.session.Session, "client", client)

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

    def fail(self, key: str, failure: str) -> None:
        """Fail `key` before any response with the named failure.

        Args:
            key: The key whose requests fail.
            failure: `"unreachable"`: the endpoint cannot be reached.

        Raises:
            ValueError: If the failure has no such name.
        """
        if failure not in _FAILURES:
            raise ValueError(f"unknown failure {failure!r}; known: {sorted(_FAILURES)}")
        with self._lock:
            self._failures[key] = failure

    def keys(self) -> list[str]:
        """Return the keys requested so far.

        Returns:
            The keys, in request order.
        """
        with self._lock:
            return [key for _, key in self.requests]

    def attach(self, client: S3Client) -> S3Client:
        """Serve `client`'s `GetObject` calls from this fake, and record its closes.

        Returns:
            The client, now answered by the fake.
        """
        events = client.meta.events
        events.register("before-parameter-build.s3.GetObject", self._remember)
        # A `before-call` handler that returns a response replaces the HTTP request.
        events.register("before-call.s3.GetObject", cast("Callable[..., None]", self._answer))
        close = client.close

        def recorded_close() -> None:
            with self._lock:
                self.closes += 1
            close()

        # The library takes the client's `close` when it creates the client, after this runs.
        cast("Any", client).close = recorded_close
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
        if self.before_answer is not None:
            self.before_answer(key)
        with self._lock:
            self.requests.append((bucket, key))
            failure = self._failures.get(key)
            refusal = next(
                (r for prefix, r in self._refusals.items() if key.startswith(prefix)), None
            )
            data = self._objects.get(key)
            dropped = self._dropped.get(key)
        if failure is not None:
            raise EndpointConnectionError(endpoint_url="https://files.massive.com")
        if refusal is not None:
            status, code, message = refusal
            return _error(status, code, message)
        if dropped is not None:
            return _ok(self._body(key, _DroppedStream(dropped), len(dropped) + 1))
        if data is None:
            return _error(404, "NoSuchKey", "The specified key does not exist.")
        return _ok(self._body(key, io.BytesIO(data), len(data)))

    def _body(self, key: str, raw: io.BytesIO, length: int) -> StreamingBody:
        def closed() -> None:
            with self._lock:
                self.closed_bodies.append(key)

        return _RecordedBody(raw, length, closed)


def _ok(body: StreamingBody) -> tuple[AWSResponse, dict[str, Any]]:
    return AWSResponse("", 200, HTTPHeaders(), None), {
        "Body": body,
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }


def _error(status: int, code: str, message: str) -> tuple[AWSResponse, dict[str, Any]]:
    # botocore reads the request id from this header into ResponseMetadata, as S3 sends it.
    headers = {"x-amz-request-id": REQUEST_ID}
    raw = HTTPHeaders()
    raw["x-amz-request-id"] = REQUEST_ID
    return AWSResponse("", status, raw, None), {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {
            "HTTPStatusCode": status,
            "RequestId": REQUEST_ID,
            "HTTPHeaders": headers,
        },
    }
