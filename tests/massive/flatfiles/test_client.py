"""Tests for the flat-file client: against the in-process S3 fake, and over real sockets.

Most tests answer the client inside botocore through the `fake_s3` fixture. The network tests
point a real client at the fake REST server, serving TLS on 127.0.0.1 and acting as a proxy,
to show the certificate authorities, the proxy, and the retries working end to end. Time is
virtual: every client gets a `FakeClock`, so retry waits are recorded, never slept.
"""

from __future__ import annotations

import io
import logging
import ssl
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import boto3
import certifi
import pyarrow as pa
import pytest
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
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
def no_proxy_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
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


@pytest.mark.parametrize(("status", "code"), [(400, "InvalidArgument"), (501, "NotImplemented")])
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


def test_a_failure_no_retry_can_fix_is_not_retried(
    *, client: FlatFileClient, fake_s3: FakeS3, clock: FakeClock
) -> None:
    def invalid(_key: str) -> None:
        raise ParamValidationError(report="Invalid bucket name")

    fake_s3.before_answer = invalid
    with pytest.raises(WireError, match="Invalid bucket name") as info:
        client.open(KEY)
    assert isinstance(info.value.__cause__, ParamValidationError)
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

    reader = threading.Thread(target=read)
    reader.start()
    clock.wait_for_sleepers(1)
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
    assert (fake_s3.keys(), clock.waits) == ([KEY], [])
    assert fake_s3.closed_bodies == [KEY]


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
        for batch in file.batches(stocks_plan(block_size=16 << 10)):
            seen.extend(batch["SequenceNumber"].to_pylist())
            delivered.set()

    with FlatFile(body, KEY, location="here", host=None) as file, pytest.raises(WireError) as info:
        read(file)
    assert info.value.message.startswith("cannot read here: ")
    assert isinstance(info.value.__cause__, OSError)
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
    no_proxy_env: pytest.MonkeyPatch,
    environment: dict[str, str],
    bundle: str,
    proxy: str | None,
) -> None:
    for name, value in environment.items():
        no_proxy_env.setenv(name, value)
    client = make_client()
    try:
        client.open(KEY)
    finally:
        client.close()
    (kwargs,) = factory_calls
    assert kwargs["verify"] == bundle
    config = cast("Any", kwargs["config"])  # botocore declares no type for its settings
    assert config.proxies == (None if proxy is None else {"https": proxy})
    assert config.proxies_config == {"proxy_ca_bundle": bundle}
    assert config.retries == {"total_max_attempts": 1, "mode": "standard"}


@pytest.fixture
def tls_server() -> Iterator[FakeRestServer]:
    server = FakeRestServer(tls=(DATA / "tls_cert.pem", DATA / "tls_key.pem"))
    server.start()
    yield server
    server.close()


@pytest.fixture
def trusted(no_proxy_env: pytest.MonkeyPatch, no_aws_settings: None) -> pytest.MonkeyPatch:
    """Trust the TLS fake's certificate, with no proxy and none of the machine's AWS settings.

    Returns:
        The monkeypatch, for further environment changes.
    """
    no_proxy_env.setenv("SSL_CERT_FILE", str(DATA / "tls_cert.pem"))
    return no_proxy_env


_SLOW_DOWN = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>SlowDown</Code>'
    b"<Message>Please reduce your request rate.</Message></Error>"
)


def _symbols(file: FlatFile | None) -> list[str]:
    assert file is not None
    with file:
        return [s for batch in file.batches(stocks_plan()) for s in batch["Symbol"].to_pylist()]


def test_a_certificate_no_bundle_trusts_fails_at_once_over_the_network(
    *, tls_server: FakeRestServer, no_proxy_env: pytest.MonkeyPatch, no_aws_settings: None
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
        target, Reply(_SLOW_DOWN, status=503), Reply(reset=True), Reply(data), Reply(status=404)
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
    tls_server.reply(f"/flatfiles/{KEY}", Reply(reset=True), Reply(status=404))
    client = make_client(endpoint_url=tls_server.url)
    try:
        assert client.open(KEY) is None
    finally:
        client.close()
    connects = fake_rest.requests
    assert connects
    assert {r.method for r in connects} == {"CONNECT"}
    # base64 of "alice:s@cret"
    assert {r.headers["proxy-authorization"] for r in connects} == {"Basic YWxpY2U6c0BjcmV0"}
    assert len(tls_server.requests) == 2
    assert all("proxy-authorization" not in r.headers for r in tls_server.requests)
    assert any(r.name == client_module.__name__ for r in caplog.records)  # the retry
    for record in caplog.records:
        text = record.getMessage()
        assert PRIVATE_KEY not in text
        assert "s%40cret" not in text
        assert "s@cret" not in text
        if record.name.startswith("deephaven_massive"):
            assert ACCESS_KEY not in text
