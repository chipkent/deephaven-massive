import functools
import inspect
import ssl
from collections.abc import Callable

import pytest
from urllib3.util.ssl_match_hostname import CertificateError

from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._massive.net import errors
from deephaven_massive._massive.net.errors import (
    LIBRARY_BUG_REPORT,
    WireAuthError,
    WireEntitlementError,
    WireError,
    WireFailure,
)

ALL_ERRORS: list[Callable[..., WireError]] = [
    functools.partial(WireError, kind=WireFailure.REFUSED),
    WireAuthError,
    WireEntitlementError,
]
"""What makes each wire error class, as a client answered with a refusal would."""

REFUSED = (
    "Massive answered GET /v3/quotes/AAPL on api.massive.com with HTTP 403 (request_id r1): "
    '"You are not entitled to this data".'
)
"""A refusal's message, as the REST response module words it."""


@pytest.mark.parametrize("make", ALL_ERRORS)
def test_every_attribute_is_kept(make: Callable[..., WireError]) -> None:
    error = make(
        REFUSED,
        status=403,
        host="api.massive.com",
        path="/v3/quotes/AAPL",
        request_id="r1",
    )
    assert error.message == REFUSED
    assert str(error) == error.message
    assert error.status == 403
    assert error.host == "api.massive.com"
    assert error.path == "/v3/quotes/AAPL"
    assert error.request_id == "r1"


@pytest.mark.parametrize("cls", [WireAuthError, WireEntitlementError])
def test_the_context_defaults_to_none_and_a_credential_error_is_a_refusal(
    cls: type[WireAuthError] | type[WireEntitlementError],
) -> None:
    error = cls("rejected")
    assert (error.status, error.host, error.path, error.request_id) == (None, None, None, None)
    assert error.kind is WireFailure.REFUSED  # Massive's answer, by definition


@pytest.mark.parametrize("kind", list(WireFailure))
def test_a_wire_error_keeps_the_kind_its_client_chose(kind: WireFailure) -> None:
    assert WireError("failed", kind=kind).kind is kind


def test_only_the_client_marks_an_answer_that_means_retry() -> None:
    assert not WireError("HTTP 400", status=400, kind=WireFailure.REFUSED).means_retry
    timeout = WireError("HTTP 400", status=400, kind=WireFailure.REFUSED, means_retry=True)
    assert timeout.means_retry


def test_a_wire_error_has_no_kind_by_default() -> None:
    # A client must say what kind of failure it raises: none is taken for Massive's answer.
    assert inspect.signature(WireError).parameters["kind"].default is inspect.Parameter.empty


@pytest.mark.parametrize("attribute", ["message", "status", "host", "path", "request_id", "kind"])
def test_attributes_are_read_only(attribute: str) -> None:
    error = WireError("x", status=500, kind=WireFailure.REFUSED)
    with pytest.raises(AttributeError):
        setattr(error, attribute, "changed")


def test_the_message_survives_a_change_to_args() -> None:
    error = WireError(REFUSED, status=503, kind=WireFailure.REFUSED)
    error.args = ("rewritten",)
    assert error.message == REFUSED


def test_the_hierarchy() -> None:
    assert issubclass(WireAuthError, WireError)
    assert issubclass(WireEntitlementError, WireError)
    assert not issubclass(WireAuthError, WireEntitlementError)
    assert not issubclass(WireEntitlementError, WireAuthError)
    assert issubclass(WireError, Exception)


def test_a_certificate_failure_is_recognised_however_it_is_wrapped() -> None:
    cert = ssl.SSLCertVerificationError(1, "certificate verify failed")
    assert errors.is_certificate_failure(cert)
    as_argument = OSError(cert)
    assert errors.is_certificate_failure(as_argument)
    as_cause = ConnectionError("wrapped")
    as_cause.__cause__ = cert
    assert errors.is_certificate_failure(as_cause)
    as_context = RuntimeError("during handling")
    as_context.__context__ = as_argument
    assert errors.is_certificate_failure(as_context)


def test_urllib3s_own_name_mismatch_is_a_certificate_failure() -> None:
    # botocore turns OpenSSL's name check off, so urllib3 checks the name and raises its own
    # error. urllib3 passes it to its `SSLError`, and botocore raises its `SSLError` while
    # handling that one, so the mismatch is an argument of the context.
    mismatch = CertificateError("hostname 'localhost' doesn't match '127.0.0.1'")
    botocore_error = ConnectionError("SSL validation failed")
    botocore_error.__context__ = OSError(mismatch)
    assert errors.is_certificate_failure(botocore_error)


def test_other_failures_are_not_certificate_failures() -> None:
    assert not errors.is_certificate_failure(ConnectionResetError("reset"))
    assert not errors.is_certificate_failure(ssl.SSLError("handshake failure"))
    assert not errors.is_certificate_failure(ssl.SSLEOFError(8, "EOF occurred"))
    assert not errors.is_certificate_failure(ValueError(ValueError("nested"), "text"))


def test_a_cycle_of_exceptions_ends_the_search() -> None:
    first = ValueError("first")
    second = ValueError(first)
    first.__context__ = second
    assert not errors.is_certificate_failure(first)
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
