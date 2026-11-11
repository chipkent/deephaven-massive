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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import boto3
import botocore.loaders
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
from urllib3.exceptions import SSLError as Urllib3SSLError

from deephaven_massive._massive.flatfiles import client as client_module
from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._massive.net.slots import ConnectionSlots
from deephaven_massive._override import override
from tests._fakes.clock import FakeClock
from tests._fakes.rest import REFUSED_PORT, FakeRestServer, Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import TypeAlias


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


def _body(streaming: StreamingBody) -> client_module._Body:
    # A response body as the client wraps it, holding one of its own connection slots.
    slots = ConnectionSlots(1)
    assert slots.acquire(Cancel())
    return client_module._Body(streaming, slots.release)


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
        to_ns=lambda values: (values, ""),
        block_size=block_size,
        chunk_size=16 * block_size,
        parse_threads=3,
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


def test_each_object_request_is_logged_at_debug(
    *, client: FlatFileClient, fake_s3: FakeS3, caplog: pytest.LogCaptureFixture
) -> None:
    fake_s3.put(KEY, b"data")
    missing = KEY.replace("03-03", "03-04")
    refused = KEY.replace("03-03", "03-05")
    fake_s3.refuse(refused, status=403, code="AccessDenied", message="Access Denied")
    with caplog.at_level(logging.DEBUG, logger=client_module.__name__):
        file = client.open(KEY)
        assert client.open(missing) is None
        with pytest.raises(WireError):
            client.open(refused)
    assert file is not None
    file.close()
    at = "at https://files.massive.com"
    request_id = fake_s3.request_id
    # A refused request is logged too, with its status, S3's code, and its request id.
    # The first object is still open while the next is requested, so a second client is built.
    built = f"S3 client {{}} of at most 4 for https://files.massive.com (one per connection; {''}"
    assert caplog.messages == [
        "built " + built.format(1) + "proxy None)",
        f"GET s3://flatfiles/{KEY} {at} -> 200, 4 bytes (request_id {request_id})",
        "built " + built.format(2) + "proxy None)",
        f"GET s3://flatfiles/{missing} {at} -> 404 NoSuchKey (request_id {request_id})",
        f"GET s3://flatfiles/{refused} {at} -> 403 AccessDenied (request_id {request_id})",
    ]


def test_a_client_is_created_on_first_use_from_its_settings_and_reused(fake_s3: FakeS3) -> None:
    client = make_client(endpoint_url="https://s3.example.com", bucket="other", max_pool=3)
    try:
        assert fake_s3.clients == []  # nothing is created before the first request
        assert not client.closed
        client.open(KEY)
        client.open(KEY)  # the first request gave its client back, so this one reuses it
    finally:
        client.close()
    assert fake_s3.clients == [
        {
            "endpoint_url": "https://s3.example.com",
            "access_key": "AKIDFLATFILES",
            "secret_key": PRIVATE_KEY,
            "signature_version": "s3v4",
            "max_pool_connections": 1,  # one connection per client: one client per slot
        }
    ]
    assert {bucket for bucket, _ in fake_s3.requests} == {"other"}


def test_concurrent_first_requests_build_one_session_and_a_client_per_connection(
    fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions: list[object] = []
    session_for = FlatFileClient._session_for

    def counted(self: FlatFileClient, bundle: str, proxy: str | None) -> object:
        sessions.append(bundle)
        return session_for(self, bundle, proxy)

    monkeypatch.setattr(FlatFileClient, "_session_for", counted)
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
    assert len(sessions) == 1
    assert 1 <= len(fake_s3.clients) <= 4  # never more clients than the pool's 4 connections
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


class _OwnedLock:
    """A lock that records which thread holds it, standing in for the shared loader's lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.owner: int | None = None  # the holder's thread id; None while free

    def __enter__(self) -> None:
        self._lock.acquire()
        self.owner = threading.get_ident()

    def __exit__(self, *_: object) -> None:
        self.owner = None
        self._lock.release()


class _CheckedLoader(botocore.loaders.Loader):
    """botocore's loader, checking that each model load holds the shared lock, alone."""

    def __init__(self, lock: _OwnedLock) -> None:
        super().__init__()
        self._lock = lock
        self._guard = threading.Lock()
        self._active = 0
        self.unlocked_loads = 0  # loads by a thread that did not hold the shared lock
        self.most_at_once = 0  # the most loads in progress at the same time

    @override
    def load_service_model(
        self, service_name: str, type_name: str, api_version: str | None = None
    ) -> object:
        with self._guard:
            if self._lock.owner != threading.get_ident():
                self.unlocked_loads += 1
            self._active += 1
            self.most_at_once = max(self.most_at_once, self._active)
        try:
            return super().load_service_model(service_name, type_name, api_version)
        finally:
            with self._guard:
                self._active -= 1


@pytest.fixture
def loaders(monkeypatch: pytest.MonkeyPatch) -> list[_CheckedLoader]:
    """Give the test a fresh shared model loader and record every loader it creates.

    Returns:
        The loaders created, in order; the process-wide one is restored after the test.
    """
    lock = _OwnedLock()
    created: list[_CheckedLoader] = []
    guard = threading.Lock()

    def create_loader() -> _CheckedLoader:
        loader = _CheckedLoader(lock)
        with guard:
            created.append(loader)
        return loader

    monkeypatch.setattr(botocore.loaders, "create_loader", create_loader)
    shared = client_module._ModelLoader()
    monkeypatch.setattr(shared, "_lock", lock)  # records the owner for the checks
    monkeypatch.setattr(client_module, "_MODEL_LOADER", shared)
    return created


def _loader_of(client: FlatFileClient) -> object:
    """Return the botocore model loader the client's first boto3 client was built with."""
    built: Any = client._clients[0]
    return built._loader


def test_every_client_shares_one_model_loader(
    *, fake_s3: FakeS3, loaders: list[_CheckedLoader]
) -> None:
    first, second = make_client(), make_client(endpoint_url="https://s3.example.com")
    try:
        first.open(KEY)
        second.open(KEY)
        assert _loader_of(first) is _loader_of(second)
        assert loaders == [_loader_of(first)]  # created once, with the holder
    finally:
        first.close()
        second.close()
    assert len(fake_s3.clients) == 2
    assert loaders[0].unlocked_loads == 0


def test_building_clients_leaves_the_search_paths_as_the_first_build_did(
    *, fake_s3: FakeS3, loaders: list[_CheckedLoader]
) -> None:
    clients = [make_client() for _ in range(3)]
    try:
        clients[0].open(KEY)
        after_first = list(loaders[0].search_paths)
        clients[1].open(KEY)
        clients[2].open(KEY)
    finally:
        for client in clients:
            client.close()
    assert loaders[0].search_paths == after_first
    assert len(set(after_first)) == len(after_first)  # boto3's data directory listed once
    assert any(path.startswith(str(Path(boto3.__file__).parent)) for path in after_first)


def test_clients_built_at_once_create_one_model_loader_and_never_load_at_once(
    *, fake_s3: FakeS3, loaders: list[_CheckedLoader]
) -> None:
    clients = [make_client() for _ in range(8)]
    start = threading.Barrier(len(clients), timeout=TIMEOUT)

    def first_request(client: FlatFileClient) -> None:  # a failure leaves its client unbuilt
        start.wait()
        client.open(KEY)

    threads = [threading.Thread(target=first_request, args=(client,)) for client in clients]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
        assert not any(thread.is_alive() for thread in threads)
        assert len(loaders) == 1
        assert all(_loader_of(client) is loaders[0] for client in clients)
    finally:
        for client in clients:
            client.close()
    assert len(fake_s3.clients) == len(clients)
    assert (loaders[0].unlocked_loads, loaders[0].most_at_once) == (0, 1)


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
        (404, "404"),
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
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
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
        (r.levelno, r.getMessage())
        for r in caplog.records
        if r.name == client_module.__name__ and (r.levelno > logging.DEBUG or "giving up" in r.msg)
    ]
    assert logged == [
        (logging.INFO, f"{error}; retry 1 of 3 in 0.55 s"),
        (logging.INFO, f"{error}; retry 2 of 3 in 1.10 s"),
        (logging.INFO, f"{error}; retry 3 of 3 in 2.20 s"),
        (logging.DEBUG, f"{error}; giving up after 4 attempts"),
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
    assert not info.value.unreadable  # a transport failure, not the file's content
    assert (fake_s3.keys(), clock.waits) == ([KEY], [])
    assert fake_s3.closed_bodies == [KEY]


def test_a_broken_file_is_not_a_dropped_read(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    # gzip's own error for a body that is not gzip is an `OSError` too, yet the transfer was
    # whole: reading the file again would fail the same way.
    fake_s3.put(KEY, b"not gzip")
    file = client.open(KEY)
    assert file is not None
    with file, pytest.raises(WireError, match="Not a gzipped file") as info:
        list(file.batches(stocks_plan()))
    assert not file.dropped
    assert info.value.unreadable  # the file's content: a gap in the library


def test_a_reread_waits_on_the_shared_schedule_and_gives_up_after_three(
    *, client: FlatFileClient, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
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
        (r.levelno, r.getMessage())
        for r in caplog.records
        if r.name == client_module.__name__ and (r.levelno > logging.DEBUG or "giving up" in r.msg)
    ]
    assert logged == [
        (logging.INFO, f"{error}; reading it again from its start, reread 1 of 3 in 0.55 s"),
        (logging.INFO, f"{error}; reading it again from its start, reread 2 of 3 in 1.10 s"),
        (logging.INFO, f"{error}; reading it again from its start, reread 3 of 3 in 2.20 s"),
        (logging.DEBUG, f"{error}; giving up after 4 reads"),
    ]


def test_a_reread_is_not_waited_for_once_the_call_is_cancelled(
    *, client: FlatFileClient, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    cancel = threading.Event()
    cancel.set()
    # No reread will follow, so there is no wait and no line promising one.
    assert client.wait_to_reread(WireError("dropped"), 0, cancel)
    assert clock.waits == []
    assert [r for r in caplog.records if r.name == client_module.__name__] == []


Start: TypeAlias = "Callable[..., threading.Thread]"
"""Starts `wait_to_reread(error, retry, cancel)` on a thread of its own and returns it."""


@pytest.fixture
def waiting() -> Iterator[tuple[FakeClock, FlatFileClient, Start]]:
    """A client on a manual clock, and a way to start a reread wait on a thread of its own.

    However the test ends, the clock is then moved far ahead and the client closed, so no
    waiter outlives the test even when the wait it tests fails to end.

    Yields:
        The clock, the client, and the function that starts a wait: `start(cancel, retry=0)`.
    """
    clock = FakeClock(manual=True)
    client = make_client(clock=clock)
    threads: list[threading.Thread] = []

    def start(cancel: threading.Event, retry: int = 0) -> threading.Thread:
        thread = threading.Thread(
            target=client.wait_to_reread, args=(WireError("dropped"), retry, cancel)
        )
        thread.start()
        threads.append(thread)
        return thread

    try:
        yield clock, client, start
    finally:
        clock.advance(3600)
        client.close()
        for thread in threads:
            thread.join(TIMEOUT)


def _ended(*threads: threading.Thread) -> bool:
    for thread in threads:
        thread.join(TIMEOUT)
    return not any(thread.is_alive() for thread in threads)


def test_a_reread_wait_ends_when_the_client_closes(
    waiting: tuple[FakeClock, FlatFileClient, Start],
) -> None:
    clock, client, start = waiting
    # The readers of one call share their cancel event; each waits on it.
    cancel = threading.Event()
    first, second = start(cancel), start(cancel)
    clock.wait_for_sleepers(2)
    client.close()  # ends both waits by setting the call's event
    assert _ended(first, second)
    assert cancel.is_set()
    # Once closed, a reread waits no more: the caller's next request finds the close.
    late = threading.Event()
    assert _ended(start(late))
    assert not late.is_set()
    assert clock.monotonic() == 0.0  # every wait ended without time passing


def test_a_reread_wait_ends_when_the_call_is_cancelled(
    waiting: tuple[FakeClock, FlatFileClient, Start],
) -> None:
    clock, _, start = waiting
    cancel = threading.Event()
    waiter = start(cancel)
    clock.wait_for_sleepers(1)
    cancel.set()
    assert _ended(waiter)
    assert clock.monotonic() == 0.0


def test_a_close_still_ends_a_wait_whose_partner_on_the_same_event_has_ended(
    waiting: tuple[FakeClock, FlatFileClient, Start],
) -> None:
    clock, client, start = waiting
    cancel = threading.Event()
    short, long = start(cancel, 0), start(cancel, 2)  # 0.55 s and 2.2 s
    clock.wait_for_sleepers(2)
    clock.advance(1.0)
    assert _ended(short)
    client.close()  # the event is still registered for the reader that still waits
    assert _ended(long)
    assert clock.monotonic() == 1.0


def test_a_close_leaves_the_event_of_a_finished_wait_alone(
    waiting: tuple[FakeClock, FlatFileClient, Start],
) -> None:
    clock, client, start = waiting
    finished = threading.Event()
    waiter = start(finished)
    clock.wait_for_sleepers(1)
    clock.advance(1.0)
    assert _ended(waiter)
    client.close()
    assert not finished.is_set()  # the ended wait no longer holds its event


class _CloseDuringCheck(threading.Event):
    """A cancel event whose first `is_set()` runs `close()` on another thread meanwhile.

    The close gets `_CLOSE_RACE` seconds to finish before the check goes on. Under the client's
    lock it cannot finish, and runs as soon as the wait has registered the event; a check made
    without the lock would let it finish first and miss the wait.
    """

    def __init__(self, client: FlatFileClient) -> None:
        super().__init__()
        self.client = client
        self.fired = False

    @override
    def is_set(self) -> bool:
        if not self.fired:
            self.fired = True
            closer = threading.Thread(target=self.client.close)
            closer.start()
            closer.join(_CLOSE_RACE)
        return super().is_set()


_CLOSE_RACE = 0.2
"""Seconds a racing `close()` gets to finish inside the wait's check."""


def test_a_close_racing_the_check_of_a_reread_wait_still_ends_it(
    waiting: tuple[FakeClock, FlatFileClient, Start],
) -> None:
    clock, client, start = waiting
    cancel = _CloseDuringCheck(client)
    assert _ended(start(cancel))
    assert cancel.fired
    assert clock.monotonic() == 0.0  # the close ended the wait; no time passed


class _TlsDrop(io.BytesIO):
    # A raw HTTP stream that serves its bytes, then fails as a TLS record that cannot be
    # decrypted does: urllib3 raises its own `SSLError`, which botocore lets through.
    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            raise Urllib3SSLError("[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] bad record mac")
        return chunk


def test_a_tls_failure_part_way_through_the_body_is_a_dropped_read(
    make_csv_gz: Callable[[str], bytes], load_fixture: Callable[[str, str], bytes]
) -> None:
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    raw = _TlsDrop(data[: len(data) // 2])
    body = _body(StreamingBody(raw, len(data)))
    with FlatFile(body, KEY, location="here", host=None) as file, pytest.raises(WireError) as info:
        list(file.batches(stocks_plan()))
    assert info.value.message.startswith("cannot read here: ")
    assert isinstance(info.value.__cause__, OSError)
    assert file.dropped


class _DropAtTheEnd(io.BytesIO):
    # A raw HTTP stream that serves its bytes, then fails as a dropped connection does.
    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            raise ProtocolError("Connection broken: IncompleteRead")
        return chunk


def test_a_drop_after_the_first_block_fails_loudly_and_repeats_no_row(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # The blocks read before the drop are handed on first, in order; the read then fails
    # instead of starting over, so no row arrives twice.
    total = 20_000
    rows = "".join(f"AAPL,{n},{1_741_012_200_000_000_000 + n}\n" for n in range(total))
    data = make_csv_gz("ticker,sequence_number,sip_timestamp\n" + rows)[:-40_000]
    body = _body(StreamingBody(_DropAtTheEnd(data), len(data) + 1))
    seen: list[int] = []

    def read(file: FlatFile) -> None:
        for batch, _ in file.batches(stocks_plan(block_size=16 << 10)):
            seen.extend(batch["SequenceNumber"].to_pylist())

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
    if file is not None:
        assert fake_s3.closes == 1  # the open object still holds its client
        file.close()
    assert fake_s3.closes == 2  # closed again when it was given back


class _BrokenRaw(io.BytesIO):
    # A raw HTTP stream whose connection drops on the first read.
    @override
    def read(self, size: int | None = -1, /) -> bytes:
        raise ProtocolError("Connection broken: IncompleteRead")


def test_a_failed_body_read_is_an_os_error() -> None:
    # botocore turns the dropped connection into its own error; the reader sees an OSError.
    body = _body(StreamingBody(_BrokenRaw(), 10))
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


def test_a_404_page_that_is_not_s3s_is_a_wire_error_over_the_network(
    *, tls_server: FakeRestServer, trusted: pytest.MonkeyPatch
) -> None:
    # Massive's front end answers a path it does not route, such as a wrong bucket, with its
    # own HTML page; only S3's `NoSuchKey` is a missing file.
    page = (DATA / "massive_404.html").read_bytes()
    tls_server.reply(
        f"/flatfile/{KEY}", Reply(page, status=404, headers={"Content-Type": "text/html"})
    )
    clock = FakeClock()
    client = make_client(endpoint_url=tls_server.url, bucket="flatfile", clock=clock)
    try:
        with pytest.raises(WireError) as info:
            client.open(KEY)
    finally:
        client.close()
    error = info.value
    assert type(error) is WireError
    assert error.message == (
        f"cannot read s3://flatfile/{KEY} at {tls_server.url}: HTTP 404 (404: Not Found)"
    )
    assert (error.status, error.path) == (404, KEY)
    assert clock.waits == []


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
    port = REFUSED_PORT
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
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    fake_s3.fail("", "unreachable")
    with pytest.raises(WireError, match=f"^cannot list {BUCKET}: ") as info:
        client.prefixes()
    assert (info.value.status, info.value.path) == (None, "")
    assert isinstance(info.value.__cause__, EndpointConnectionError)
    assert clock.waits == WAITS
    assert [
        r.levelno
        for r in caplog.records
        if r.name == client_module.__name__ and (r.levelno > logging.DEBUG or "giving up" in r.msg)
    ] == [
        logging.INFO,
        logging.INFO,
        logging.INFO,
        logging.DEBUG,
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


# --- connection slots ---------------------------------------------------------------------------


def _waits_seen(client: FlatFileClient) -> threading.Event:
    # Set as soon as a request waits for a connection, so a test sees it blocked without a sleep.
    waiting = threading.Event()
    wait = client._slots._ready.wait

    def noted(timeout: float | None = None) -> bool:
        waiting.set()
        return wait(timeout)

    client._slots._ready.wait = noted  # type: ignore[method-assign]  # the point: see the wait
    return waiting


def test_an_open_file_keeps_its_connection_until_it_is_closed(
    fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    fake_s3.put(KEY, make_csv_gz("ticker,sip_timestamp\nAAPL,1\n"))
    fake_s3.put(FUTURES_KEY, make_csv_gz("ticker,sip_timestamp\nAAPL,1\n"))
    client = make_client(max_pool=1)
    waiting = _waits_seen(client)
    first = client.open(KEY)
    assert first is not None
    opened: list[FlatFile | None] = []
    second = threading.Thread(target=lambda: opened.append(client.open(FUTURES_KEY)))
    second.start()
    assert waiting.wait(TIMEOUT)  # the second object waits for the first to be closed
    assert [key for _, key in fake_s3.requests] == [KEY]
    first.close()
    first.close()  # idempotent: the connection is given back once
    second.join(TIMEOUT)
    [later] = opened
    assert later is not None
    later.close()
    client.close()


def test_a_cancel_ends_the_wait_for_a_connection_without_a_request(
    fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    fake_s3.put(KEY, make_csv_gz("ticker,sip_timestamp\nAAPL,1\n"))
    client = make_client(max_pool=1)
    waiting = _waits_seen(client)
    held = client.open(KEY)
    assert held is not None
    cancel = Cancel()
    opened: list[FlatFile | None] = []
    waiter = threading.Thread(target=lambda: opened.append(client.open(KEY, cancel=cancel)))
    waiter.start()
    assert waiting.wait(TIMEOUT)
    cancel.set()
    waiter.join(TIMEOUT)
    assert opened == [None]
    assert len(fake_s3.requests) == 1  # only the held object was requested
    held.close()
    client.close()


def test_a_close_ends_the_wait_for_a_connection_of_open_and_prefixes(
    fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    fake_s3.put(KEY, make_csv_gz("ticker,sip_timestamp\nAAPL,1\n"))
    client = make_client(max_pool=1)
    waiting = _waits_seen(client)
    held = client.open(KEY)
    assert held is not None
    errors: list[WireError] = []

    def wait_for(call: Callable[[], object]) -> None:
        try:
            call()
        except WireError as error:
            errors.append(error)

    waiters = [
        threading.Thread(target=wait_for, args=(lambda: client.open(KEY),)),
        threading.Thread(target=wait_for, args=(client.prefixes,)),
    ]
    for waiter in waiters:
        waiter.start()
    assert waiting.wait(TIMEOUT)
    client.close()
    for waiter in waiters:
        waiter.join(TIMEOUT)
    assert (
        sorted(error.message for error in errors)
        == ["the flat-file client for https://files.massive.com is closed"] * 2
    )
    held.close()


def test_overlapping_reads_share_the_pool_without_discarding_connections(
    *,
    tls_server: FakeRestServer,
    trusted: pytest.MonkeyPatch,
    make_csv_gz: Callable[[str], bytes],
    load_fixture: Callable[[str, str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Two reads of two files each, on a client of two connections: two objects are open at
    # once while the others wait for one to be closed, so urllib3 never opens an extra
    # connection only to discard it ("Connection pool is full").
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    keys = [KEY.replace("03-03", f"03-0{day}") for day in range(3, 7)]
    for key in keys:
        tls_server.reply(f"/flatfiles/{key}", Reply(data))
    ready = threading.Condition()
    hold = threading.Event()
    in_flight = 0

    def held(_received: object) -> None:
        nonlocal in_flight
        with ready:
            in_flight += 1
            ready.notify_all()
        hold.wait(TIMEOUT)

    tls_server.on_request(held)
    client = make_client(endpoint_url=tls_server.url, max_pool=2)
    waiting = _waits_seen(client)
    caplog.set_level(logging.WARNING, logger="urllib3")
    with ThreadPoolExecutor(2) as one, ThreadPoolExecutor(2) as other:
        reads = [
            pool.submit(lambda key: _symbols(client.open(key)), key)
            for pool, key in zip([one, one, other, other], keys, strict=True)
        ]
        with ready:
            assert ready.wait_for(lambda: in_flight == 2, TIMEOUT)
        assert waiting.wait(TIMEOUT)  # a third object waits for a connection
        hold.set()
        symbols = [read.result(TIMEOUT) for read in reads]
    client.close()
    assert symbols == [["AAPL", "MSFT", "AAPL", "SPY"]] * 4
    assert in_flight == 4
    assert [r.getMessage() for r in caplog.records if "pool is full" in r.getMessage()] == []


def _ssl_context_of(client: object) -> ssl.SSLContext:
    """Return the `SSLContext` a boto3 client's connections to the endpoint are made with."""
    # botocore keeps the client's urllib3 pool manager, and the context it gives every
    # connection, in private attributes with no public accessor; reading them is the only way
    # to check that no two connection slots share a context.
    manager: Any = cast("Any", client)._endpoint.http_session._manager
    context: ssl.SSLContext = manager.connection_pool_kw["ssl_context"]
    return context


def test_no_two_held_connection_slots_share_an_ssl_context(fake_s3: FakeS3) -> None:
    # urllib3 writes to a connection's SSLContext as it opens it, while OpenSSL may be copying
    # that state for another thread's handshake with the GIL released, which corrupts the
    # handshake or crashes. So each slot has its own client, with one connection, and the
    # objects held open at once never share a context.
    keys = [KEY.replace("03-03", f"03-0{day}") for day in range(3, 6)]
    for key in keys:
        fake_s3.put(key, b"data")
    client = make_client(max_pool=3)
    files = [client.open(key) for key in keys]
    try:
        built = list(client._clients)
        assert (len(built), client._idle) == (3, [])  # one client per held slot
        pools = [cast("Any", each.meta.config).max_pool_connections for each in built]
        assert pools == [1, 1, 1]
        assert len({id(_ssl_context_of(each)) for each in built}) == 3
        cast("FlatFile", files[0]).close()
        files[0] = client.open(keys[0])  # takes the slot given back, and its client
        assert client._clients == built
    finally:
        for file in files:
            cast("FlatFile", file).close()
        client.close()


def test_an_endpoint_s_user_and_password_never_reach_a_message(
    *, fake_s3: FakeS3, clock: FakeClock
) -> None:
    # botocore names the endpoint as configured when it cannot reach it.
    endpoint = "https://reader:hunter2@files.massive.com"

    def unreachable(_key: str) -> None:
        raise EndpointConnectionError(endpoint_url=f"{endpoint}/flatfiles/{KEY}")

    fake_s3.put(KEY, b"data")
    fake_s3.before_answer = unreachable
    flat_files = make_client(endpoint_url=endpoint, clock=clock)
    try:
        with pytest.raises(WireError) as info:
            flat_files.open(KEY)
    finally:
        flat_files.close()
    message = str(info.value)
    assert message.startswith(
        f"cannot read s3://flatfiles/{KEY} at https://***@files.massive.com: "
    )
    assert "hunter2" not in message
    assert "reader" not in message
    assert "https://***@files.massive.com/flatfiles/" in message
    with pytest.raises(WireError, match=r"^the flat-file client for https://\*\*\*@files"):
        flat_files.open(KEY)


class _UnclosableRaw(io.BytesIO):
    # A raw stream whose first close fails, as a connection's teardown can.
    failed = False

    @override
    def close(self) -> None:
        if not self.failed:
            self.failed = True
            raise OSError("the connection broke while closing")
        super().close()


def test_a_body_whose_close_raises_still_gives_its_slot_back() -> None:
    slots = ConnectionSlots(1)
    assert slots.acquire(Cancel())
    body = client_module._Body(StreamingBody(_UnclosableRaw(b"data"), 4), slots.release)
    with pytest.raises(OSError, match="broke while closing"):
        body.close()
    assert slots._free == 1  # given back


def test_an_attempt_never_waits_for_the_lock_a_client_build_holds(
    client: FlatFileClient,
) -> None:
    # Building a boto3 client for another slot holds the client's lock while it reads local
    # AWS configuration. An attempt on a client its caller already holds needs nothing the
    # lock guards, so it is sent while the lock is held elsewhere.
    def attempt() -> object:
        return client._attempt(
            cast("Any", None), lambda _: "sent", key="k", location="s3://bucket/k", verb="read"
        )

    with ThreadPoolExecutor(1) as pool, client._lock:  # the lock is let go first
        assert pool.submit(attempt).result(timeout=10) == "sent"
