import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Protocol

import pytest

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.reader import FlatFile
from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError

TIMEOUT = 30.0
KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"
FUTURES_KEY = "us_futures_nymex/trades_v1/2025/03/2025-03-04.csv.gz"
PRIVATE_KEY = "flat-file-secret"


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    requests: list[tuple[str, str]]
    clients: list[dict[str, object]]
    closes: int
    request_id: str
    before_answer: Callable[[str], object] | None

    def put(self, key: str, data: bytes) -> None: ...
    def refuse(self, prefix: str, *, status: int, code: str, message: str) -> None: ...
    def fail(self, key: str, failure: str) -> None: ...
    def keys(self) -> list[str]: ...


def make_client(
    *, endpoint_url: str = "https://files.massive.com", bucket: str = "flatfiles", max_pool: int = 4
) -> FlatFileClient:
    return FlatFileClient(
        access_key_id="AKIDFLATFILES",
        secret_access_key=PRIVATE_KEY,
        endpoint_url=endpoint_url,
        bucket=bucket,
        max_pool=max_pool,
    )


@pytest.fixture
def client(fake_s3: FakeS3) -> Iterator[FlatFileClient]:
    flat_files = make_client()
    yield flat_files
    flat_files.close()


def test_an_existing_object_opens_and_a_missing_one_is_none(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    fake_s3.put(KEY, b"data")
    file = client.open(KEY)
    assert isinstance(file, FlatFile)
    with file:
        assert file.key == KEY
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
            "secret_key": "flat-file-secret",
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


@pytest.mark.parametrize("code", ["InvalidAccessKeyId", "SignatureDoesNotMatch"])
def test_rejected_credentials_are_a_wire_auth_error(
    *, client: FlatFileClient, fake_s3: FakeS3, code: str
) -> None:
    fake_s3.refuse("us_", status=403, code=code, message="Rejected")
    with pytest.raises(WireAuthError) as info:
        client.open(FUTURES_KEY, venue="nymex")
    error = info.value
    assert error.message == (
        f"S3 rejected the flat-file credentials: HTTP 403 for s3://flatfiles/{FUTURES_KEY} at "
        f"https://files.massive.com ({code}: Rejected); check MassiveConfig s3_access_key_id "
        "and s3_secret_access_key"
    )
    assert (error.status, error.host, error.path) == (403, "files.massive.com", FUTURES_KEY)
    assert error.request_id == fake_s3.request_id
    assert PRIVATE_KEY not in error.message


def test_another_403_is_a_wire_entitlement_error(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    fake_s3.refuse("us_", status=403, code="AccessDenied", message="Access Denied")
    with pytest.raises(WireEntitlementError) as info:
        client.open(KEY)
    assert info.value.message == (
        f"the plan does not include these flat files: HTTP 403 for s3://flatfiles/{KEY} at "
        "https://files.massive.com (AccessDenied: Access Denied)"
    )
    assert (info.value.status, info.value.request_id) == (403, fake_s3.request_id)
    assert not isinstance(info.value, WireAuthError)


def test_a_refused_futures_key_names_its_venue(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    fake_s3.refuse("us_futures_nymex/", status=403, code="AccessDenied", message="Access Denied")
    with pytest.raises(WireEntitlementError) as info:
        client.open(FUTURES_KEY, venue="nymex")
    assert info.value.message == (
        f"the plan does not include NYMEX futures flat files: HTTP 403 for "
        f"s3://flatfiles/{FUTURES_KEY} at https://files.massive.com (AccessDenied: Access "
        "Denied); pass venue=dm.FuturesVenue.<VENUE> for a venue the plan includes"
    )


def test_another_status_is_a_wire_error(*, client: FlatFileClient, fake_s3: FakeS3) -> None:
    fake_s3.refuse("us_", status=500, code="InternalError", message="Try again")
    with pytest.raises(WireError) as info:
        client.open(KEY)
    error = info.value
    assert type(error) is WireError
    assert error.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: HTTP 500 "
        "(InternalError: Try again)"
    )
    assert (error.status, error.path, error.request_id) == (500, KEY, fake_s3.request_id)


def test_an_unreachable_endpoint_is_a_wire_error(
    *, client: FlatFileClient, fake_s3: FakeS3
) -> None:
    fake_s3.fail(KEY, "unreachable")
    with pytest.raises(WireError, match=f"^cannot read s3://flatfiles/{KEY} at ") as info:
        client.open(KEY)
    assert type(info.value) is WireError
    assert (info.value.status, info.value.request_id, info.value.path) == (None, None, KEY)


def test_a_client_that_cannot_be_created_is_a_wire_error(
    *, fake_s3: FakeS3, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    empty = tmp_path / "aws-config"
    empty.write_text("")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
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
