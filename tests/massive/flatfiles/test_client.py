"""Tests for the flat-file client: against the in-process S3 fake, and over real sockets.

Most tests answer the client inside botocore through the `fake_s3` fixture. The network tests
point a real client at the fake REST server, serving TLS on 127.0.0.1 and acting as a proxy,
to show the certificate authorities, the proxy, and the retries working end to end. Time is
virtual: every client gets a `FakeClock`, so retry waits are recorded, never slept.
"""

from __future__ import annotations

import io
import logging
import random
import socket
import ssl
import struct
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import boto3
import botocore.utils
import certifi
import pyarrow as pa
import pytest
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ParamValidationError,
    ProxyConnectionError,
    ReadTimeoutError,
    SSLError,
)
from botocore.response import StreamingBody
from urllib3.exceptions import ProtocolError

from deephaven_massive._massive.flatfiles import client as client_module
from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._types import override
from tests._fakes.clock import FakeClock
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


TIMEOUT = 30.0
KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"
FUTURES_KEY = "us_futures_nymex/trades_v1/2025/03/2025-03-04.csv.gz"
ACCESS_KEY = "AKIDFLATFILES"
PRIVATE_KEY = "flat-file-secret"
OWNER = "massive/flatfiles"
ENDPOINT = "https://files.massive.com"
DATA = Path(__file__).resolve().parents[2] / "data" / OWNER
_CLIENT_JITTER = client_module._JITTER
"""The client's own jitter source, which the autouse `_half_jitter` fixture replaces."""
WAITS = [0.55, 1.1, 2.2]
"""The three transient waits, 0.5, 1, and 2 s, each 10 % longer with the jitter fixed at 0.5."""


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    requests: list[tuple[str, str]]
    clients: list[dict[str, object]]
    closes: int
    closed_bodies: list[str]
    request_id: str
    before_answer: Callable[[str], object] | None
    listings: list[str | None]
    page_size: int
    pagination: str | None

    def put(self, key: str, data: bytes) -> None: ...
    def put_dropped(self, key: str, data: bytes) -> None: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...
    def fail(self, key: str, failure: str) -> None: ...
    def keys(self) -> list[str]: ...


def make_client(
    *,
    endpoint_url: str = ENDPOINT,
    bucket: str = "flatfiles",
    max_pool: int = 4,
    clock: FakeClock | None = None,
) -> FlatFileClient:
    return FlatFileClient(
        access_key_id=ACCESS_KEY,
        secret_access_key=PRIVATE_KEY,
        endpoint_url=endpoint_url,
        bucket=bucket,
        max_pool=max_pool,
        clock=clock or FakeClock(),
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(fake_s3: FakeS3, clock: FakeClock) -> Iterator[FlatFileClient]:
    flat_files = make_client(clock=clock)
    yield flat_files
    flat_files.close()


class _Jitter:
    """A stand-in for the client's jitter source that always draws `value`."""

    def __init__(self, value: float) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


@pytest.fixture(autouse=True)
def _half_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Draw jitter 0.5, so each transient wait is exactly 10 % above its base."""
    monkeypatch.setattr(client_module, "_JITTER", _Jitter(0.5))


@pytest.fixture
def network_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Unset every proxy and certificate variable, so a test sets only the ones it is about.

    Returns:
        The monkeypatch, for the test's own settings.
    """
    for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    return monkeypatch


def stocks_plan(block_size: int = 1 << 20) -> ReadPlan:
    return ReadPlan(
        columns=(
            ReadColumn("ticker", "Symbol", pa.string()),
            ReadColumn("sequence_number", "SequenceNumber", pa.int64()),
        ),
        symbol_header="ticker",
        symbols=None,
        time_header="sip_timestamp",
        window_ns=None,
        to_ns=lambda values: values,
        session=None,
        block_size=block_size,
        required=frozenset({"ticker"}),
    )


def test_an_existing_object_opens_and_a_missing_one_is_none(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    fake_s3.put(KEY, b"data")
    file = client.open(KEY)
    assert isinstance(file, FlatFile)
    with file:
        assert file.key == KEY
        assert file.location == f"s3://flatfiles/{KEY} at https://files.massive.com"
    assert fake_s3.closed_bodies == [KEY]
    assert client.open(KEY.replace("03-03", "03-04")) is None
    assert fake_s3.requests == [("flatfiles", KEY), ("flatfiles", KEY.replace("03-03", "03-04"))]


def test_the_client_is_created_once_on_first_use_from_its_settings(fake_s3: FakeS3) -> None:
    client = make_client(endpoint_url="https://s3.example.com", bucket="other", max_pool=3)
    try:
        assert fake_s3.clients == []  # nothing is created before the first request
        assert not client.closed
        client.open(KEY)
        client.open(KEY)
    finally:
        client.close()
    assert fake_s3.clients == [
        {
            "endpoint_url": "https://s3.example.com",
            "access_key": "AKIDFLATFILES",
            "secret_key": PRIVATE_KEY,
            "signature_version": "s3v4",
            "max_pool_connections": 3,
        }
    ]
    assert {bucket for bucket, _ in fake_s3.requests} == {"other"}


def test_concurrent_first_requests_create_one_client(fake_s3: FakeS3) -> None:
    client = make_client()
    start = threading.Barrier(8, timeout=TIMEOUT)

    def first_request() -> None:  # a failure leaves its request out of the count below
        start.wait()
        client.open(KEY)

    threads = [threading.Thread(target=first_request) for _ in range(8)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
    finally:
        client.close()
    assert len(fake_s3.clients) == 1
    assert len(fake_s3.requests) == 8


def test_a_client_created_while_the_network_settings_are_read_is_shared(
    *, client: FlatFileClient, fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The settings are read outside the lock, so another request can create the client
    # meanwhile; this one then uses it rather than creating a second.
    lookups: list[str] = []

    def proxy_for(host: str) -> None:
        lookups.append(host)
        if len(lookups) == 1:
            client.open(KEY)  # the other request, which creates the client

    monkeypatch.setattr(client_module, "https_proxy", proxy_for)
    client.open(KEY)
    assert lookups == ["files.massive.com"] * 2
    assert len(fake_s3.clients) == 1
    assert fake_s3.keys() == [KEY] * 2


def test_a_close_while_the_network_settings_are_read_creates_no_client(
    *, client: FlatFileClient, fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    def proxy_for(_host: str) -> None:
        client.close()

    monkeypatch.setattr(client_module, "https_proxy", proxy_for)
    with pytest.raises(WireError, match="is closed"):
        client.open(KEY)
    assert (fake_s3.clients, fake_s3.keys()) == ([], [])


@pytest.mark.parametrize("code", ["InvalidAccessKeyId", "SignatureDoesNotMatch"])
def test_rejected_credentials_are_a_wire_auth_error(
    *, client: FlatFileClient, fake_s3: FakeS3, code: str
) -> None:
    fake_s3.refuse("us_", status=403, code=code, message="Rejected")
    with pytest.raises(WireAuthError) as info:
        client.open(FUTURES_KEY)
    error = info.value
    # The message says what happened to which object; the remedy is the library's to add.
    assert error.message == (
        f"HTTP 403 for s3://flatfiles/{FUTURES_KEY} at https://files.massive.com ({code}: Rejected)"
    )
    assert (error.status, error.host, error.path) == (403, "files.massive.com", FUTURES_KEY)
    assert error.request_id == fake_s3.request_id
    assert PRIVATE_KEY not in error.message


def test_another_403_is_a_wire_entitlement_error(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    fake_s3.refuse("us_", status=403, code="AccessDenied", message="Access Denied")
    with pytest.raises(WireEntitlementError) as info:
        client.open(FUTURES_KEY)
    assert info.value.message == (
        f"HTTP 403 for s3://flatfiles/{FUTURES_KEY} at https://files.massive.com "
        "(AccessDenied: Access Denied)"
    )
    assert (info.value.status, info.value.path) == (403, FUTURES_KEY)
    assert info.value.request_id == fake_s3.request_id
    assert not isinstance(info.value, WireAuthError)


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "InvalidArgument"),
        (501, "NotImplemented"),
        # Only `NoSuchKey` is a missing file: a wrong bucket must not read as one.
        (404, "NoSuchBucket"),
        (404, "NotFound"),
    ],
)
def test_another_status_is_a_wire_error_at_once(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock, status: int, code: str
) -> None:
    fake_s3.refuse("us_", status=status, code=code, message="No")
    with pytest.raises(WireError) as info:
        client.open(KEY)
    error = info.value
    assert type(error) is WireError
    assert error.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: HTTP {status} ({code}: No)"
    )
    assert (error.status, error.path, error.request_id) == (status, KEY, fake_s3.request_id)
    assert isinstance(error.__cause__, ClientError)
    assert (fake_s3.keys(), clock.waits) == ([KEY], [])


# Retries.


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (500, "InternalError"),
        (502, "BadGateway"),
        (503, "SlowDown"),
        (504, "GatewayTimeout"),
        (400, "RequestTimeout"),
    ],
)
def test_a_transient_answer_is_retried_three_times_on_the_shared_schedule(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
    status: int,
    code: str,
) -> None:
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    fake_s3.refuse("us_", status=status, code=code, message="Try again")
    with pytest.raises(WireError) as info:
        client.open(KEY)
    error = info.value
    assert type(error) is WireError
    assert error.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: HTTP {status} "
        f"({code}: Try again)"
    )
    assert (error.status, error.request_id) == (status, fake_s3.request_id)
    assert isinstance(error.__cause__, ClientError)
    assert fake_s3.keys() == [KEY] * 4
    assert clock.waits == WAITS
    logged = [
        (r.levelno, r.getMessage()) for r in caplog.records if r.name == client_module.__name__
    ]
    assert logged == [
        (logging.INFO, f"{error}; retry 1 of 3 in 0.55 s"),
        (logging.INFO, f"{error}; retry 2 of 3 in 1.10 s"),
        (logging.INFO, f"{error}; retry 3 of 3 in 2.20 s"),
        (logging.ERROR, f"{error}; giving up after 4 attempts"),
    ]


@pytest.mark.parametrize("jitter", [0.0, 0.99])
def test_the_drawn_jitter_lengthens_each_wait(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    jitter: float,
) -> None:
    monkeypatch.setattr(client_module, "_JITTER", _Jitter(jitter))
    fake_s3.fail(KEY, "unreachable")
    with pytest.raises(WireError):
        client.open(KEY)
    assert clock.waits == pytest.approx([base * (1 + 0.2 * jitter) for base in (0.5, 1.0, 2.0)])


def test_the_jitter_source_is_the_operating_system() -> None:
    # Read at import, before the autouse fixture puts its stand-in in place.
    assert isinstance(_CLIENT_JITTER, random.SystemRandom)


def test_an_object_that_answers_after_a_transient_failure_opens(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock
) -> None:
    fake_s3.put(KEY, b"data")
    answers = [(503, "SlowDown"), (500, "InternalError")]

    def busy(_key: str) -> None:
        if answers:
            status, code = answers.pop(0)
            raise ClientError(
                {
                    "Error": {"Code": code, "Message": "Try again"},
                    "ResponseMetadata": {
                        "HTTPStatusCode": status,
                        "RequestId": "",
                        "HostId": "",
                        "HTTPHeaders": {},
                        "RetryAttempts": 0,
                    },
                },
                "GetObject",
            )

    fake_s3.before_answer = busy
    file = client.open(KEY)
    assert isinstance(file, FlatFile)
    file.close()
    assert clock.waits == WAITS[:2]
    assert fake_s3.keys() == [KEY]


def _connection_failures() -> list[BotoCoreError]:
    url = f"{ENDPOINT}/flatfiles/{KEY}"
    return [
        EndpointConnectionError(endpoint_url=url),
        ConnectTimeoutError(endpoint_url=url),
        ConnectionClosedError(endpoint_url=url),
        ReadTimeoutError(endpoint_url=url),
    ]


@pytest.mark.parametrize("failure", _connection_failures(), ids=lambda error: type(error).__name__)
def test_a_connection_failure_before_the_first_byte_is_retried(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock, failure: BotoCoreError
) -> None:
    fake_s3.put(KEY, b"data")
    pending = [failure]

    def fail_once(_key: str) -> None:
        if pending:
            raise pending.pop()

    fake_s3.before_answer = fail_once
    file = client.open(KEY)
    assert isinstance(file, FlatFile)
    file.close()
    assert clock.waits == WAITS[:1]
    # The failed attempt never reached the bucket; the retry did.
    assert fake_s3.keys() == [KEY]


def test_an_unreachable_endpoint_is_a_wire_error_after_the_retries(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock
) -> None:
    fake_s3.fail(KEY, "unreachable")
    with pytest.raises(WireError, match=f"^cannot read s3://flatfiles/{KEY} at ") as info:
        client.open(KEY)
    assert type(info.value) is WireError
    assert (info.value.status, info.value.request_id, info.value.path) == (None, None, KEY)
    assert isinstance(info.value.__cause__, EndpointConnectionError)
    assert fake_s3.keys() == [KEY] * 4
    assert clock.waits == WAITS


@pytest.mark.parametrize(
    "failure",
    [
        ParamValidationError(report="Invalid bucket name"),
        # botocore's wrapper for an unexpected exception inside its HTTP layer.
        HTTPClientError(error=RuntimeError("a bug in the HTTP layer")),
    ],
    ids=lambda error: type(error).__name__,
)
def test_a_failure_no_retry_can_fix_is_not_retried(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock, failure: BotoCoreError
) -> None:
    def fail(_key: str) -> None:
        raise failure

    fake_s3.before_answer = fail
    with pytest.raises(WireError, match=f"^cannot read s3://flatfiles/{KEY} at ") as info:
        client.open(KEY)
    assert info.value.__cause__ is failure
    assert clock.waits == []


def _certificate_rejected(_key: str) -> None:
    # botocore's error for a failed handshake, raised while handling the certificate failure.
    try:
        raise ssl.SSLCertVerificationError(1, "certificate verify failed")
    except ssl.SSLError as error:
        raise SSLError(endpoint_url=ENDPOINT, error=error) from error


def _certificate_rejected_by_way_of_the_proxy(_key: str) -> None:
    # A certificate failure that botocore reports as another error.
    try:
        raise ssl.SSLCertVerificationError(1, "certificate verify failed")
    except ssl.SSLError as error:
        raise ProxyConnectionError(proxy_url="https://proxy.corp:3129", error=error) from error


def _handshake_refused(_key: str) -> None:
    raise SSLError(endpoint_url=ENDPOINT, error=ssl.SSLError(1, "wrong version number"))


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (
            _certificate_rejected,
            (
                r"TLS certificate verification failed for files\.massive\.com \(.*\); if a proxy "
                r"or private CA signs it, set SSL_CERT_FILE to a bundle that trusts it$"
            ),
        ),
        (
            _certificate_rejected_by_way_of_the_proxy,
            r"TLS certificate verification failed for files\.massive\.com \(.*proxy\.corp.*\);",
        ),
        (
            _handshake_refused,
            (
                r"TLS failed for files\.massive\.com \(SSL validation failed for "
                r"https://files\.massive\.com .*wrong version number.*\)$"
            ),
        ),
    ],
    ids=["certificate", "certificate-at-the-proxy", "handshake"],
)
def test_a_tls_failure_is_never_retried(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    clock: FakeClock,
    failure: Callable[[str], None],
    message: str,
) -> None:
    attempts: list[str] = []

    def fail(key: str) -> None:
        attempts.append(key)
        failure(key)

    fake_s3.before_answer = fail
    with pytest.raises(
        WireError, match=f"^cannot read s3://flatfiles/{KEY} at .*: {message}"
    ) as info:
        client.open(KEY)
    assert (info.value.status, info.value.path) == (None, KEY)
    assert isinstance(info.value.__cause__, BotoCoreError)
    assert (attempts, clock.waits) == ([KEY], [])


def test_closing_the_client_ends_a_retry_wait(fake_s3: FakeS3) -> None:
    clock = FakeClock(manual=True)
    client = make_client(clock=clock)
    fake_s3.fail(KEY, "unreachable")
    raised: list[WireError] = []

    def read() -> None:
        try:
            client.open(KEY)
        except WireError as error:
            raised.append(error)

    # A daemon, and closed in a `finally`, so a regression fails the test instead of hanging
    # the process at exit.
    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        clock.wait_for_sleepers(1)
    finally:
        client.close()
    reader.join(TIMEOUT)
    assert not reader.is_alive()
    (error,) = raised
    assert error.message == "the flat-file client for https://files.massive.com is closed"
    assert fake_s3.keys() == [KEY]
    assert clock.monotonic() == 0.0  # the wait ended without time passing


def test_a_dropped_body_fails_the_read_and_is_never_requested_again(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    clock: FakeClock,
    make_csv_gz: Callable[[str], bytes],
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # botocore's own error for a body cut short reaches the reader as an OSError.
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    fake_s3.put_dropped(KEY, data[: len(data) // 2])
    file = client.open(KEY)
    assert file is not None
    with file, pytest.raises(WireError) as info:
        list(file.batches(stocks_plan()))
    assert info.value.message.startswith(f"cannot read s3://flatfiles/{KEY} at ")
    assert isinstance(info.value.__cause__, OSError)
    assert file.dropped
    assert (fake_s3.keys(), clock.waits) == ([KEY], [])
    assert fake_s3.closed_bodies == [KEY]


def test_a_broken_file_is_not_a_dropped_read(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    # gzip's own error for a body that is not gzip is an `OSError` too, yet the transfer was
    # whole: reading the file again would fail the same way.
    fake_s3.put(KEY, b"not gzip")
    file = client.open(KEY)
    assert file is not None
    with file, pytest.raises(WireError, match="Not a gzipped file"):
        list(file.batches(stocks_plan()))
    assert not file.dropped


def test_a_reread_waits_on_the_shared_schedule_and_gives_up_after_three(
    *, client: FlatFileClient, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    error = WireError("cannot read s3://flatfiles/k at here: dropped")
    cancel = threading.Event()
    assert [client.wait_to_reread(error, retry, cancel) for retry in range(4)] == [
        True,
        True,
        True,
        False,
    ]
    assert clock.waits == WAITS  # giving up does not wait
    logged = [
        (r.levelno, r.getMessage()) for r in caplog.records if r.name == client_module.__name__
    ]
    assert logged == [
        (logging.INFO, f"{error}; reading it again from its start, reread 1 of 3 in 0.55 s"),
        (logging.INFO, f"{error}; reading it again from its start, reread 2 of 3 in 1.10 s"),
        (logging.INFO, f"{error}; reading it again from its start, reread 3 of 3 in 2.20 s"),
        (logging.ERROR, f"{error}; giving up after 4 reads"),
    ]


def test_a_reread_wait_ends_on_cancel_and_on_close() -> None:
    clock = FakeClock(manual=True)
    client = make_client(clock=clock)
    error = WireError("dropped")
    try:
        # The readers of one call share their cancel event; each waits on it.
        cancel = threading.Event()
        results: list[bool] = []
        waiters = [
            threading.Thread(target=lambda: results.append(client.wait_to_reread(error, 0, cancel)))
            for _ in range(2)
        ]
        for waiter in waiters:
            waiter.start()
        clock.wait_for_sleepers(2)
        client.close()  # ends both waits by setting the call's event
        for waiter in waiters:
            waiter.join(TIMEOUT)
        assert cancel.is_set()
        assert results == [True, True]
        assert clock.monotonic() == 0.0  # no time passed
        # Once closed, a reread waits no more: the caller's next request finds the close.
        assert client.wait_to_reread(error, 1, threading.Event())
        assert clock.waits == [0.55, 0.55]
    finally:
        client.close()
    # A cancel by the caller ends a wait too.
    clock = FakeClock(manual=True)
    client = make_client(clock=clock)
    cancel = threading.Event()
    waiter = threading.Thread(target=client.wait_to_reread, args=(error, 2, cancel))
    waiter.start()
    clock.wait_for_sleepers(1)
    cancel.set()
    waiter.join(TIMEOUT)
    assert not waiter.is_alive()
    client.close()
    assert clock.monotonic() == 0.0


class _DropAfterABlock(io.BytesIO):
    # A raw HTTP stream that serves its bytes, then, once the reader has handed on a block,
    # fails as a dropped connection does.
    def __init__(self, data: bytes, delivered: threading.Event) -> None:
        super().__init__(data)
        self.delivered = delivered

    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            assert self.delivered.wait(TIMEOUT)
            raise ProtocolError("Connection broken: IncompleteRead")
        return chunk


def test_a_drop_after_the_first_block_fails_loudly_and_repeats_no_row(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # The blocks handed on before the drop stand; the read then fails instead of starting over,
    # so no row arrives twice.
    total = 20_000
    rows = "".join(f"AAPL,{n},{1_741_012_200_000_000_000 + n}\n" for n in range(total))
    data = make_csv_gz("ticker,sequence_number,sip_timestamp\n" + rows)[:-40_000]
    delivered = threading.Event()
    raw = _DropAfterABlock(data, delivered)
    body = client_module._Body(StreamingBody(raw, len(data) + 1))
    seen: list[int] = []

    def read(file: FlatFile) -> None:
        for batch, _ in file.batches(stocks_plan(block_size=16 << 10)):
            seen.extend(batch["SequenceNumber"].to_pylist())
            delivered.set()

    with FlatFile(body, KEY, location="here", host=None) as file, pytest.raises(WireError) as info:
        read(file)
    assert info.value.message.startswith("cannot read here: ")
    assert isinstance(info.value.__cause__, OSError)
    assert file.dropped
    assert 0 < len(seen) < total
    assert seen == list(range(len(seen)))  # in order, none twice


def test_a_client_that_cannot_be_created_is_a_wire_error(
    *, fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_PROFILE", "no-such-profile")
    client = make_client()
    with pytest.raises(
        WireError,
        match=r"cannot create the S3 client for https://files\.massive\.com: .*no-such-profile",
    ) as info:
        client.open(KEY)
    assert info.value.host == "files.massive.com"
    assert fake_s3.clients == []


def test_a_closed_client_makes_no_request(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    client.open(KEY)
    client.close()
    assert client.closed
    assert fake_s3.closes == 1
    client.close()  # idempotent
    assert fake_s3.closes == 1
    with pytest.raises(WireError, match=r"the flat-file client for .* is closed") as info:
        client.open(KEY)
    assert info.value.path == KEY
    assert fake_s3.keys() == [KEY]


def test_closing_a_client_never_used_closes_nothing(fake_s3: FakeS3) -> None:
    client = make_client()
    client.close()
    assert client.closed
    assert (fake_s3.clients, fake_s3.closes) == ([], 0)


@pytest.mark.parametrize("stored", [True, False], ids=["an-object", "a-missing-key"])
def test_a_request_in_flight_at_close_closes_the_client_again(
    *, client: FlatFileClient, fake_s3: FakeS3, stored: bool
) -> None:
    if stored:
        fake_s3.put(KEY, b"data")
    fake_s3.before_answer = lambda _key: client.close()
    file = client.open(KEY)
    # The answer still arrives; the caller sees the close through `closed`.
    assert (file is not None) is stored
    assert client.closed
    assert fake_s3.closes == 2
    if file is not None:
        file.close()


class _BrokenRaw(io.BytesIO):
    # A raw HTTP stream whose connection drops on the first read.
    @override
    def read(self, size: int | None = -1, /) -> bytes:
        raise ProtocolError("Connection broken: IncompleteRead")


def test_a_failed_body_read_is_an_os_error() -> None:
    # botocore turns the dropped connection into its own error; the reader sees an OSError.
    body = client_module._Body(StreamingBody(_BrokenRaw(), 10))
    with pytest.raises(OSError, match="IncompleteRead") as info:
        body.read(4)
    assert isinstance(info.value.__cause__, BotoCoreError)


# Certificate authorities, proxies, and botocore's own retries.


@pytest.fixture
def factory_calls(fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    """Record the keyword arguments of every boto3 client created, around the fake's factory.

    Returns:
        The arguments of each `Session.client` call, in order.
    """
    calls: list[dict[str, object]] = []
    create = cast("Callable[..., object]", boto3.session.Session.client)

    def client(session: boto3.session.Session, *args: object, **kwargs: object) -> object:
        calls.append(kwargs)
        return create(session, *args, **kwargs)

    monkeypatch.setattr(boto3.session.Session, "client", client)
    return calls


@pytest.mark.parametrize(
    ("environment", "bundle", "proxy"),
    [
        ({}, certifi.where(), None),
        ({"SSL_CERT_DIR": "/etc/ssl/certs"}, certifi.where(), None),
        ({"SSL_CERT_FILE": "/corp/ca.pem"}, "/corp/ca.pem", None),
        ({"HTTPS_PROXY": "http://proxy.corp:3128"}, certifi.where(), "http://proxy.corp:3128"),
        (
            {"HTTPS_PROXY": "http://proxy.corp:3128", "NO_PROXY": "files.massive.com"},
            certifi.where(),
            None,
        ),
    ],
    ids=["certifi", "cert-dir-ignored", "cert-file", "proxy", "no-proxy"],
)
def test_the_client_follows_the_shared_tls_and_proxy_rules_and_never_retries_itself(
    *,
    factory_calls: list[dict[str, object]],
    network_env: pytest.MonkeyPatch,
    environment: dict[str, str],
    bundle: str,
    proxy: str | None,
) -> None:
    for name, value in environment.items():
        network_env.setenv(name, value)
    client = make_client()
    try:
        client.open(KEY)
    finally:
        client.close()
    (kwargs,) = factory_calls
    assert kwargs["verify"] == bundle
    config = cast("Any", kwargs["config"])  # botocore declares no type for its settings
    # An empty mapping, not None: None would make botocore look the proxy up itself.
    assert config.proxies == ({} if proxy is None else {"https": proxy})
    assert config.proxies_config == {"proxy_ca_bundle": bundle}
    assert config.retries == {"total_max_attempts": 1, "mode": "standard"}
    assert (config.connect_timeout, config.read_timeout) == (60.0, 60.0)


def test_botocore_never_looks_the_proxy_up_itself(
    *, fake_s3: FakeS3, network_env: pytest.MonkeyPatch
) -> None:
    # The proxy is decided outside the client's lock; botocore's own lookup would run under it,
    # and on macOS it can resolve the host name.
    network_env.setenv("HTTPS_PROXY", "http://proxy.corp:3128")
    network_env.setenv("NO_PROXY", "files.massive.com")
    lookups: list[str] = []
    network_env.setattr(botocore.utils, "proxy_bypass", lookups.append)
    client = make_client()
    try:
        client.open(KEY)
    finally:
        client.close()
    assert lookups == []
    assert len(fake_s3.clients) == 1


def test_the_defaults_mode_never_asks_the_instance_metadata_service(
    *, fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `auto` would look the region up on the instance-metadata service, under the client's lock.
    monkeypatch.setenv("AWS_DEFAULTS_MODE", "auto")
    monkeypatch.delenv("AWS_EC2_METADATA_DISABLED")
    asked: list[object] = []

    def provide(provider: object) -> None:
        asked.append(provider)

    monkeypatch.setattr(botocore.utils.IMDSRegionProvider, "provide", provide)
    client = make_client()
    try:
        client.open(KEY)
    finally:
        client.close()
    assert asked == []
    assert len(fake_s3.clients) == 1


@pytest.fixture
def tls_server() -> Iterator[FakeRestServer]:
    server = FakeRestServer(tls=(DATA / "tls_cert.pem", DATA / "tls_key.pem"))
    server.start()
    yield server
    server.close()


@pytest.fixture
def trusted(network_env: pytest.MonkeyPatch, no_aws_settings: None) -> pytest.MonkeyPatch:
    """Trust the TLS fake's certificate, with no proxy and none of the machine's AWS settings.

    Returns:
        The monkeypatch, for further environment changes.
    """
    network_env.setenv("SSL_CERT_FILE", str(DATA / "tls_cert.pem"))
    return network_env


_SLOW_DOWN = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>SlowDown</Code>'
    b"<Message>Please reduce your request rate.</Message></Error>"
)
_NO_SUCH_KEY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>NoSuchKey</Code>'
    b"<Message>The specified key does not exist.</Message></Error>"
)


def _symbols(file: FlatFile | None) -> list[str]:
    assert file is not None
    with file:
        return [s for batch, _ in file.batches(stocks_plan()) for s in batch["Symbol"].to_pylist()]


def test_a_certificate_no_bundle_trusts_fails_at_once_over_the_network(
    *, tls_server: FakeRestServer, network_env: pytest.MonkeyPatch, no_aws_settings: None
) -> None:
    clock = FakeClock()
    client = make_client(endpoint_url=tls_server.url, clock=clock)
    try:
        with pytest.raises(
            WireError, match=r"certificate verification failed for 127\.0\.0\.1 .*SSL_CERT_FILE"
        ):
            client.open(KEY)
    finally:
        client.close()
    assert (tls_server.requests, clock.waits) == ([], [])


def test_ssl_cert_file_is_trusted_and_the_shared_schedule_retries_over_the_network(
    *,
    tls_server: FakeRestServer,
    trusted: pytest.MonkeyPatch,
    make_csv_gz: Callable[[str], bytes],
    load_fixture: Callable[[str, str], bytes],
) -> None:
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    target = f"/flatfiles/{KEY}"
    tls_server.reply(
        target,
        Reply(_SLOW_DOWN, status=503),
        Reply(reset=True),
        Reply(data),
        Reply(_NO_SUCH_KEY, status=404),
    )
    clock = FakeClock()
    client = make_client(endpoint_url=tls_server.url, clock=clock)
    try:
        assert _symbols(client.open(KEY)) == ["AAPL", "MSFT", "AAPL", "SPY"]
        assert client.open(KEY) is None
    finally:
        client.close()
    # One request per attempt: botocore retried nothing itself, and every wait was the client's.
    assert [r.target for r in tls_server.requests] == [target] * 4
    assert clock.waits == WAITS[:2]


def test_the_proxy_carries_its_credentials_and_no_secret_is_logged(
    *,
    tls_server: FakeRestServer,
    fake_rest: FakeRestServer,
    trusted: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    proxy = fake_rest.url.removeprefix("http://")
    trusted.setenv("HTTPS_PROXY", f"http://alice:s%40cret@{proxy}")
    tls_server.reply(f"/flatfiles/{KEY}", Reply(reset=True), Reply(_NO_SUCH_KEY, status=404))
    client = make_client(endpoint_url=tls_server.url)
    try:
        assert client.open(KEY) is None
    finally:
        client.close()
    # One tunnel per attempt: the reset closed the first.
    connects = fake_rest.requests
    assert [r.method for r in connects] == ["CONNECT", "CONNECT"]
    # base64 of "alice:s@cret"
    assert {r.headers["proxy-authorization"] for r in connects} == {"Basic YWxpY2U6c0BjcmV0"}
    assert len(tls_server.requests) == 2
    assert all("proxy-authorization" not in r.headers for r in tls_server.requests)
    assert any(r.name == client_module.__name__ for r in caplog.records)  # the retry
    # botocore's own DEBUG records are among those checked: the signed request is logged.
    assert any(
        r.name.startswith("botocore") and r.levelno == logging.DEBUG and "Authorization" in text
        for r in caplog.records
        for text in [r.getMessage()]
    )
    for record in caplog.records:
        text = record.getMessage()
        # The secret and the proxy password never appear, botocore's DEBUG records included.
        assert PRIVATE_KEY not in text
        assert "s%40cret" not in text
        assert "s@cret" not in text
        assert "YWxpY2U6c0BjcmV0" not in text  # base64 of "alice:s@cret"
        # The documented exception: botocore's DEBUG records, enabled only on request, may show
        # the access key id and the request signature. The library's own records never do.
        if record.name.startswith("deephaven_massive"):
            assert ACCESS_KEY not in text


def _closed_port() -> int:
    """Return a loopback port nothing listens on: bound by this process, then released."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


@pytest.mark.parametrize(
    ("user", "password"),
    [("alice", "topsecret"), ("h", "h")],
    ids=["a-password", "a-password-that-appears-earlier-in-the-url"],
)
def test_an_unreachable_proxy_is_retried_and_its_password_stays_out_of_the_error(
    *, trusted: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, user: str, password: str
) -> None:
    # botocore masks a proxy password by replacing its first occurrence in the URL, which for
    # `http://h:h@...` is the `h` of `http`; the client names the proxy without credentials.
    caplog.set_level(logging.DEBUG)
    port = _closed_port()
    trusted.setenv("HTTPS_PROXY", f"http://{user}:{password}@127.0.0.1:{port}")
    clock = FakeClock()
    client = make_client(clock=clock)
    try:
        with pytest.raises(WireError) as info:
            client.open(KEY)
    finally:
        client.close()
    assert clock.waits == WAITS
    assert info.value.message.startswith(
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: cannot connect "
        f"through the proxy http://127.0.0.1:{port} ("
    )
    secret = f"{user}:{password}@"
    assert secret not in info.value.message
    assert "***" not in info.value.message
    assert not any(secret in r.getMessage() for r in caplog.records)
    assert any(r.name == client_module.__name__ for r in caplog.records)


def test_a_proxy_that_refuses_the_credentials_is_never_retried(
    *,
    tls_server: FakeRestServer,
    fake_rest: FakeRestServer,
    trusted: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Repeating credentials the proxy refused could lock the account on a corporate proxy.
    caplog.set_level(logging.DEBUG)
    proxy = fake_rest.url.removeprefix("http://")
    trusted.setenv("HTTPS_PROXY", f"http://alice:wr0ng@{proxy}")
    fake_rest.refuse_tunnels(407)
    clock = FakeClock()
    client = make_client(endpoint_url=tls_server.url, clock=clock)
    try:
        with pytest.raises(WireError) as info:
            client.open(KEY)
    finally:
        client.close()
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at {tls_server.url}: the proxy {fake_rest.url} "
        "refused its credentials (HTTP 407); check the user and password in HTTPS_PROXY"
    )
    assert (info.value.status, info.value.path) == (407, KEY)
    assert [r.method for r in fake_rest.requests] == ["CONNECT"]
    assert (tls_server.requests, clock.waits) == ([], [])
    assert not any("wr0ng" in r.getMessage() for r in caplog.records)


class _ErrorBodyServer:
    """An HTTP server on 127.0.0.1 whose answers break while an error response's body is sent.

    Each connection takes the next scripted behaviour: `"reset"` sends the head of a 503 and
    part of its body, then resets the connection; `"stall"` sends the same and then nothing
    more until the server closes; any `bytes` is a 200 response with that body.
    """

    _HEAD = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 500\r\n\r\n<Error><Code>Slow"

    def __init__(self, *behaviours: str | bytes) -> None:
        self._behaviours = list(behaviours)
        self._lock = threading.Lock()
        self._released = threading.Event()
        self.connections = 0
        self._socket = socket.socket()
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen(8)
        self.url = f"http://127.0.0.1:{self._socket.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        self._released.set()
        self._socket.close()

    def _accept(self) -> None:
        while True:
            try:
                connection, _ = self._socket.accept()
            except OSError:
                return
            with self._lock:
                self.connections += 1
                behaviour = self._behaviours.pop(0)
            threading.Thread(target=self._serve, args=(connection, behaviour), daemon=True).start()

    def _serve(self, connection: socket.socket, behaviour: str | bytes) -> None:
        with connection:
            request = b""
            while b"\r\n\r\n" not in request:
                request += connection.recv(65536)
            if isinstance(behaviour, bytes):
                head = f"HTTP/1.1 200 OK\r\nContent-Length: {len(behaviour)}\r\n\r\n"
                connection.sendall(head.encode() + behaviour)
                self._released.wait(TIMEOUT)
                return
            connection.sendall(self._HEAD)
            if behaviour == "stall":
                self._released.wait(TIMEOUT)
                return
            # Linger zero: the close resets the connection rather than ending it in order.
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))


@pytest.fixture
def error_body_env(network_env: pytest.MonkeyPatch, no_aws_settings: None) -> pytest.MonkeyPatch:
    """Clear the network and AWS settings for a test against `_ErrorBodyServer`.

    The read timeout keeps its production value: a reset ends the client's read at once, so a
    test of one must not depend on how soon the server thread answers. Only the stall test
    shortens it.

    Returns:
        The monkeypatch.
    """
    return network_env


def _open_after_one_failure(behaviour: str) -> None:
    """Open a file from a server whose first answer fails with `behaviour`, then succeeds."""
    server = _ErrorBodyServer(behaviour, b"data")
    clock = FakeClock()
    client = make_client(endpoint_url=server.url, clock=clock)
    try:
        file = client.open(KEY)
        assert isinstance(file, FlatFile)
        file.close()
    finally:
        client.close()
        server.close()
    assert server.connections == 2
    assert clock.waits == WAITS[:1]


def test_an_error_response_reset_partway_is_retried_over_the_network(
    error_body_env: pytest.MonkeyPatch,
) -> None:
    _open_after_one_failure("reset")


def test_an_error_response_that_stalls_is_retried_after_the_read_timeout(
    error_body_env: pytest.MonkeyPatch,
) -> None:
    # The read timeout is real time, and it also bounds the wait for each answer's head, so it
    # leaves a busy runner a full second to schedule the server thread.
    error_body_env.setattr(client_module, "_READ_TIMEOUT", 1.0)
    _open_after_one_failure("stall")


def test_an_error_response_cut_short_every_time_is_a_wire_error_after_the_retries(
    error_body_env: pytest.MonkeyPatch,
) -> None:
    server = _ErrorBodyServer(*["reset"] * 4)
    clock = FakeClock()
    client = make_client(endpoint_url=server.url, clock=clock)
    try:
        with pytest.raises(WireError) as info:
            client.open(KEY)
    finally:
        client.close()
        server.close()
    assert info.value.message.startswith(
        f"cannot read s3://flatfiles/{KEY} at {server.url}: the error response was cut short ("
    )
    assert isinstance(info.value.__cause__, ProtocolError)
    assert (info.value.status, info.value.path) == (None, KEY)
    assert server.connections == 4
    assert clock.waits == WAITS


# Listing the bucket's prefixes.

BUCKET = "s3://flatfiles/ at https://files.massive.com"


def _store_prefixes(fake_s3: FakeS3, *prefixes: str) -> None:
    for prefix in prefixes:
        fake_s3.put(f"{prefix}/trades_v1/2025/03/2025-03-03.csv.gz", b"data")


def test_prefixes_lists_every_top_level_prefix_across_pages(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    _store_prefixes(fake_s3, "us_stocks_sip", "us_options_opra", "global_crypto")
    fake_s3.put("us_stocks_sip/quotes_v1/2025/03/2025-03-03.csv.gz", b"data")
    fake_s3.put("README.txt", b"not below a prefix")
    fake_s3.page_size = 2
    assert client.prefixes() == ["global_crypto", "us_options_opra", "us_stocks_sip"]
    # The second page was asked for with the token the first one ended with.
    assert fake_s3.listings == [None, "after-2"]
    assert fake_s3.keys() == []


def test_prefixes_of_a_bucket_that_fits_one_page_is_one_request(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    _store_prefixes(fake_s3, "us_stocks_sip")
    assert client.prefixes() == ["us_stocks_sip"]
    assert client.prefixes() == ["us_stocks_sip"]
    assert fake_s3.listings == [None, None]
    assert len(fake_s3.clients) == 1


def test_a_last_page_without_its_pagination_fields_ends_the_listing(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    _store_prefixes(fake_s3, "a", "b", "c")
    fake_s3.page_size = 2
    fake_s3.pagination = "bare"
    assert client.prefixes() == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("pagination", "listings", "message"),
    [
        ("missing", [None], "a truncated page gave no continuation token"),
        ("repeat", [None, "after-2"], "a truncated page gave the same continuation token"),
    ],
)
def test_a_truncated_page_without_a_new_token_is_a_wire_error(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    pagination: str,
    listings: list[str | None],
    message: str,
) -> None:
    _store_prefixes(fake_s3, "a", "b", "c", "d", "e")
    fake_s3.page_size = 2
    fake_s3.pagination = pagination
    with pytest.raises(WireError) as info:
        client.prefixes()
    assert info.value.message == f"cannot list {BUCKET}: {message}"
    assert info.value.host == "files.massive.com"
    assert fake_s3.listings == listings


def test_prefixes_of_an_empty_bucket_is_empty(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    assert client.prefixes() == []


def test_a_listing_page_is_retried_on_the_shared_schedule(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock
) -> None:
    _store_prefixes(fake_s3, "a", "b", "c")
    fake_s3.page_size = 2
    pending = [503, 500]

    def busy_on_the_second_page(_key: str) -> None:
        if fake_s3.listings and pending:
            status = pending.pop(0)
            raise ClientError(
                {
                    "Error": {"Code": "SlowDown", "Message": "Try again"},
                    "ResponseMetadata": {
                        "HTTPStatusCode": status,
                        "RequestId": "",
                        "HostId": "",
                        "HTTPHeaders": {},
                        "RetryAttempts": 0,
                    },
                },
                "ListObjectsV2",
            )

    fake_s3.before_answer = busy_on_the_second_page
    assert client.prefixes() == ["a", "b", "c"]
    assert clock.waits == WAITS[:2]
    # The failed attempts never reached the bucket; the first page was not asked for again.
    assert fake_s3.listings == [None, "after-2"]


def test_a_listing_that_stays_unreachable_is_a_wire_error_after_the_retries(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    fake_s3.fail("", "unreachable")
    with pytest.raises(WireError, match=f"^cannot list {BUCKET}: ") as info:
        client.prefixes()
    assert (info.value.status, info.value.path) == (None, "")
    assert isinstance(info.value.__cause__, EndpointConnectionError)
    assert clock.waits == WAITS
    assert [r.levelno for r in caplog.records if r.name == client_module.__name__] == [
        logging.INFO,
        logging.INFO,
        logging.INFO,
        logging.ERROR,
    ]


@pytest.mark.parametrize(
    ("status", "code", "error_type", "message"),
    [
        (403, "AccessDenied", WireEntitlementError, f"HTTP 403 for {BUCKET} (AccessDenied: No)"),
        (
            403,
            "InvalidAccessKeyId",
            WireAuthError,
            f"HTTP 403 for {BUCKET} (InvalidAccessKeyId: No)",
        ),
        (404, "NoSuchBucket", WireError, f"cannot list {BUCKET}: HTTP 404 (NoSuchBucket: No)"),
    ],
)
def test_a_refused_listing_is_a_wire_error_at_once(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    clock: FakeClock,
    status: int,
    code: str,
    error_type: type[WireError],
    message: str,
) -> None:
    fake_s3.refuse("", status=status, code=code, message="No")
    with pytest.raises(WireError) as info:
        client.prefixes()
    assert type(info.value) is error_type
    assert info.value.message == message
    assert (info.value.status, info.value.path, info.value.request_id) == (
        status,
        "",
        fake_s3.request_id,
    )
    assert PRIVATE_KEY not in info.value.message
    assert (fake_s3.listings, clock.waits) == ([None], [])


def test_a_closed_client_lists_nothing(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    client.close()
    with pytest.raises(WireError, match=r"the flat-file client for .* is closed") as info:
        client.prefixes()
    assert info.value.path == ""
    assert (fake_s3.listings, fake_s3.clients) == ([], [])


def test_closing_the_client_ends_a_listing_retry_wait(fake_s3: FakeS3) -> None:
    clock = FakeClock(manual=True)
    client = make_client(clock=clock)
    fake_s3.fail("", "unreachable")
    raised: list[WireError] = []

    def list_prefixes() -> None:
        try:
            client.prefixes()
        except WireError as error:
            raised.append(error)

    lister = threading.Thread(target=list_prefixes, daemon=True)
    lister.start()
    try:
        clock.wait_for_sleepers(1)
    finally:
        client.close()
    lister.join(TIMEOUT)
    assert not lister.is_alive()
    (error,) = raised
    assert error.message == "the flat-file client for https://files.massive.com is closed"
    assert fake_s3.listings == [None]
