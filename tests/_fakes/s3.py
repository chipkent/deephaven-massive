"""A fake of Massive's flat-file S3 endpoint, served inside botocore's own request pipeline.

The library builds a real boto3 S3 client from its configuration. `FakeS3.install` wraps
boto3's client factory for the length of a test, so every S3 client created meanwhile is passed
to `FakeS3.attach`, which registers botocore event handlers on it: every `GetObject` and
`ListObjectsV2` is then answered in process, after the SDK has validated and serialized the call
and before anything is signed or sent. Answers have the shape S3 itself gives: a
`StreamingBody` for an object; for a listing with the delimiter `/`, the top-level prefixes of
the stored keys as `CommonPrefixes`, `page_size` at a time, with a continuation token while
more remain; and for an error an HTTP status with S3's error code and message and an
`x-amz-request-id` header, which botocore raises as `ClientError`. Nothing leaves the process.

Objects are stored by key, as bytes or, through `put_csv`, as a flat file with any header (to
reorder, add, or drop columns by name). A key can also be refused with any status (403
`AccessDenied` for a plan without the dataset, 500 for a server error), fail before any response
by name (`"unreachable"`: the endpoint cannot be reached), or fail part-way through its body (a
dropped connection while streaming): always (`put_dropped`), or for its first few requests and
then serve it whole (`drop`); `before_drop` can hold such a failure until the reader has used
the bytes served before it. Every request, every client close, and every body close is
recorded. A listing is recorded in `listings` by the continuation token it sent, and `refuse`,
`fail`, and `before_answer` treat it as a request for the key `""`. Built by the `fake_s3`
fixture in `tests/conftest.py`; thread-safe, since the library reads several keys at once.
"""

from __future__ import annotations

import csv
import gzip
import io
import threading
from typing import TYPE_CHECKING, Any, Final, cast

import boto3
from botocore.awsrequest import AWSResponse, HTTPHeaders
from botocore.exceptions import EndpointConnectionError
from botocore.response import StreamingBody
from urllib3.exceptions import ProtocolError

from deephaven_massive._override import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    import pytest
    from types_boto3_s3.client import S3Client

_CONTEXT_KEY = "fake_s3_object"

REQUEST_ID: Final = "4442587FB7D0A2F9"
"""The `x-amz-request-id` of every error answer."""

_FAILURES: Final = frozenset({"unreachable"})
"""The failures `FakeS3.fail` raises by name before any response."""


class _CopyingStream(io.BytesIO):
    """A response stream whose every read returns new bytes, as a socket read does.

    A plain `io.BytesIO` built from a bytes object hands that very object back from a read of
    the whole stream, so a reader that read a whole body at once would cost no memory in a
    test. Copying on each read, rather than once when the stream is made, gives each read the
    cost it has against a real connection.
    """

    @override
    def read(self, size: int | None = -1, /) -> bytes:
        return bytes(memoryview(super().read(size)))


class _DroppedStream(io.BytesIO):
    """A response stream that serves its bytes and then fails as a dropped connection does.

    Before the failing read it calls `failing`, on the reading thread, so a test can hold the
    failure until the reader has used the bytes served before it.
    """

    def __init__(self, data: bytes, failing: Callable[[], object]) -> None:
        super().__init__(data)
        self._failing = failing

    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            self._failing()
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
        # Per stored key: how many more requests drop, and after how many bytes.
        self._drops: dict[str, tuple[int, int]] = {}
        self._refusals: dict[str, tuple[int, str, str]] = {}
        self._failures: dict[str, str] = {}
        self.requests: list[tuple[str, str]] = []
        self.clients: list[dict[str, object]] = []
        self.closes = 0
        self.closed_bodies: list[str] = []
        self.request_id = REQUEST_ID
        # The continuation token of each listing, None for a first page, in request order.
        self.listings: list[str | None] = []
        # The most prefixes one listing page holds; S3's own pages hold 1000.
        self.page_size = 1000
        # How listing pages bend S3's pagination fields, as a server other than S3 might:
        # "missing" sends no token on a truncated page, "repeat" sends back the token it was
        # asked with (after a first page), and "bare" leaves both fields out of a last page.
        self.pagination: str | None = None
        # Called with the key on the requesting thread before each answer, so a test can hold a
        # read in flight or act at that moment; set it before the library's first read.
        self.before_answer: Callable[[str], object] | None = None
        # Called with the key on the reading thread when a dropped body (`put_dropped`, `drop`)
        # has served its bytes, just before the read that fails. A reader may read ahead on a
        # thread of its own and report the failure before it has used the bytes it read, so a
        # test that needs the reader to have used some of them first holds the failure here.
        self.before_drop: Callable[[str], object] | None = None

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

    def data(self, key: str) -> bytes:
        """Return the bytes stored under `key` with `put` or `put_csv`."""
        with self._lock:
            return self._objects[key]

    def put_csv(
        self, key: str, header: Sequence[str], rows: Iterable[Mapping[str, object]]
    ) -> None:
        """Store a gzip CSV flat file with exactly the given header, in the given order.

        Each row is written by header name, so the same rows can be stored under a reordered
        header, a header with an extra name, or one that lacks a name: a name the header lacks
        is not written, and a header name the row lacks is an empty field (Massive's null).

        Args:
            key: The object key.
            header: The header names, in file order.
            rows: The rows, each mapping header names to values.
        """
        text = io.StringIO()
        writer = csv.DictWriter(text, header, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        self.put(key, gzip.compress(text.getvalue().encode()))

    def put_dropped(self, key: str, data: bytes) -> None:
        """Store an object whose body fails after `data`, as a connection dropped mid-read."""
        with self._lock:
            self._dropped[key] = data

    def drop(self, key: str, *, times: int, at: int) -> None:
        """Cut the body of a stored object short for its next `times` requests, then serve it.

        Args:
            key: A key stored with `put` or `put_csv`.
            times: How many requests, from the next one, drop the connection.
            at: How many bytes of the object each of them serves before the drop.
        """
        with self._lock:
            self._drops[key] = (times, at)

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
        events.register("before-parameter-build.s3.ListObjectsV2", self._remember_listing)
        events.register(
            "before-call.s3.ListObjectsV2", cast("Callable[..., None]", self._answer_listing)
        )
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
            # None when boto3 found no credentials: the fake answers unsigned requests too.
            "access_key": None if credentials is None else credentials.access_key,
            "secret_key": None if credentials is None else credentials.secret_key,
            "signature_version": settings.signature_version,
            "max_pool_connections": settings.max_pool_connections,
        }
        with self._lock:
            self.clients.append(recorded)
        return client

    def _remember(self, params: dict[str, Any], context: dict[str, Any], **_: object) -> None:
        context[_CONTEXT_KEY] = (params["Bucket"], params["Key"])

    def _remember_listing(
        self, params: dict[str, Any], context: dict[str, Any], **_: object
    ) -> None:
        context[_CONTEXT_KEY] = (params.get("Delimiter"), params.get("ContinuationToken"))

    def _answer_listing(
        self, context: dict[str, Any], **_: object
    ) -> tuple[AWSResponse, dict[str, Any]]:
        delimiter, token = context[_CONTEXT_KEY]
        if delimiter != "/":
            raise AssertionError("the fake lists only top-level prefixes (Delimiter='/')")
        if self.before_answer is not None:
            self.before_answer("")
        with self._lock:
            self.listings.append(token)
            failure = self._failures.get("")
            refusal = self._refusals.get("")
            stored = {*self._objects, *self._dropped}
            size = self.page_size
            pagination = self.pagination
        if failure is not None:
            raise EndpointConnectionError(endpoint_url="https://files.massive.com")
        if refusal is not None:
            return _error(*refusal)
        prefixes = sorted({key.partition("/")[0] + "/" for key in stored if "/" in key})
        start = 0 if token is None else int(token.removeprefix("after-"))
        more = start + size < len(prefixes)
        answer: dict[str, Any] = {
            "IsTruncated": more,
            "CommonPrefixes": [{"Prefix": prefix} for prefix in prefixes[start : start + size]],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        if more and pagination != "missing":
            answer["NextContinuationToken"] = (
                (token or f"after-{start + size}")
                if pagination == "repeat"
                else f"after-{start + size}"
            )
        if not more and pagination == "bare":
            del answer["IsTruncated"]
        return AWSResponse("", 200, HTTPHeaders(), None), answer

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
            times, at = self._drops.get(key, (0, 0))
            if data is not None and times:
                self._drops[key] = (times - 1, at)
                dropped = data[:at]
        if failure is not None:
            raise EndpointConnectionError(endpoint_url="https://files.massive.com")
        if refusal is not None:
            status, code, message = refusal
            return _error(status, code, message)
        if dropped is not None:
            stream = _DroppedStream(dropped, lambda: self._before_drop(key))
            length = len(dropped) + 1
            return _ok(self._body(key, stream, length), length)
        if data is None:
            return _error(404, "NoSuchKey", "The specified key does not exist.")
        return _ok(self._body(key, _CopyingStream(data), len(data)), len(data))

    def _before_drop(self, key: str) -> None:
        if self.before_drop is not None:
            self.before_drop(key)

    def _body(self, key: str, raw: io.BytesIO, length: int) -> StreamingBody:
        def closed() -> None:
            with self._lock:
                self.closed_bodies.append(key)

        return _RecordedBody(raw, length, closed)


def _ok(body: StreamingBody, length: int) -> tuple[AWSResponse, dict[str, Any]]:
    # As S3 answers: the object's size, and the request id botocore reads from its header.
    return AWSResponse("", 200, HTTPHeaders(), None), {
        "Body": body,
        "ContentLength": length,
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": REQUEST_ID},
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
