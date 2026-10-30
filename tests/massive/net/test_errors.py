import ssl

import pytest

from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._massive.net import errors
from deephaven_massive._massive.net.errors import (
    LIBRARY_BUG_REPORT,
    WireAuthError,
    WireEntitlementError,
    WireError,
)

ALL_CLASSES = [WireError, WireAuthError, WireEntitlementError]


@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_every_attribute_is_kept(cls: type[WireError]) -> None:
    error = cls(
        "HTTP 403 from GET /v3/quotes/AAPL (request_id r1): not entitled",
        status=403,
        host="api.massive.com",
        path="/v3/quotes/AAPL",
        request_id="r1",
    )
    assert error.message == "HTTP 403 from GET /v3/quotes/AAPL (request_id r1): not entitled"
    assert str(error) == error.message
    assert error.status == 403
    assert error.host == "api.massive.com"
    assert error.path == "/v3/quotes/AAPL"
    assert error.request_id == "r1"


@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_the_context_defaults_to_none(cls: type[WireError]) -> None:
    error = cls("connection reset")
    assert (error.status, error.host, error.path, error.request_id) == (None, None, None, None)


@pytest.mark.parametrize("attribute", ["message", "status", "host", "path", "request_id"])
def test_attributes_are_read_only(attribute: str) -> None:
    error = WireError("x", status=500)
    with pytest.raises(AttributeError):
        setattr(error, attribute, "changed")


def test_the_message_survives_a_change_to_args() -> None:
    error = WireError("HTTP 503 from GET /v3/trades/AAPL", status=503)
    error.args = ("rewritten",)
    assert error.message == "HTTP 503 from GET /v3/trades/AAPL"


def test_the_hierarchy() -> None:
    assert issubclass(WireAuthError, WireError)
    assert issubclass(WireEntitlementError, WireError)
    assert not issubclass(WireAuthError, WireEntitlementError)
    assert not issubclass(WireEntitlementError, WireAuthError)
    assert issubclass(WireError, Exception)


def test_a_certificate_failure_is_recognised_however_it_is_wrapped() -> None:
    cert = ssl.SSLCertVerificationError(1, "certificate verify failed")
    assert errors.is_tls_failure(cert)
    as_argument = OSError(cert)
    assert errors.is_tls_failure(as_argument)
    as_cause = ConnectionError("wrapped")
    as_cause.__cause__ = cert
    assert errors.is_tls_failure(as_cause)
    as_context = RuntimeError("during handling")
    as_context.__context__ = as_argument
    assert errors.is_tls_failure(as_context)


def test_other_failures_are_not_tls_failures() -> None:
    assert not errors.is_tls_failure(ConnectionResetError("reset"))
    assert not errors.is_tls_failure(ssl.SSLError("handshake failure"))
    assert not errors.is_tls_failure(ValueError(ValueError("nested"), "text"))


def test_a_cycle_of_exceptions_ends_the_search() -> None:
    first = ValueError("first")
    second = ValueError(first)
    first.__context__ = second
    assert not errors.is_tls_failure(first)
    assert not errors.is_proxy_auth_failure(first)


def test_a_proxy_credential_refusal_is_recognised_however_it_is_wrapped() -> None:
    # http.client's report of a CONNECT answered with 407, wrapped as urllib3 and botocore do.
    refused = OSError("Tunnel connection failed: 407 Proxy Authentication Required")
    assert errors.is_proxy_auth_failure(refused)
    as_argument = ConnectionError("Unable to connect to proxy", refused)
    assert errors.is_proxy_auth_failure(as_argument)
    as_context = RuntimeError("Failed to connect to proxy URL")
    as_context.__context__ = as_argument
    assert errors.is_proxy_auth_failure(as_context)


@pytest.mark.parametrize(
    "error",
    [
        OSError("Tunnel connection failed: 403 Forbidden"),
        OSError("Tunnel connection failed: 4070 Unknown"),
        ValueError("Tunnel connection failed: 407 Proxy Authentication Required"),
        ConnectionRefusedError(61, "Connection refused"),
    ],
    ids=["another-status", "a-longer-status", "not-an-os-error", "refused"],
)
def test_other_failures_are_not_proxy_credential_refusals(error: BaseException) -> None:
    assert not errors.is_proxy_auth_failure(error)


def test_the_wire_bug_report_text_is_the_library_s() -> None:
    # The wire package keeps its own copy, since it imports nothing else of the library.
    assert LIBRARY_BUG_REPORT == LIBRARY_BUG
