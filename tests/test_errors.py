import pytest

from deephaven_massive._errors import (
    AuthenticationError,
    EntitlementError,
    MassiveError,
    PermanentError,
    from_wire,
)
from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError

ALL_ERRORS = [MassiveError, AuthenticationError, EntitlementError, PermanentError]


@pytest.mark.parametrize("error", ALL_ERRORS)
def test_status_and_request_id_default_to_none(error: type[MassiveError]) -> None:
    raised = error("closed")
    assert (raised.status, raised.request_id) == (None, None)
    assert raised.args == ("closed",)


@pytest.mark.parametrize("error", ALL_ERRORS)
def test_status_and_request_id_are_kept(error: type[MassiveError]) -> None:
    raised = error("rate limited", status=429, request_id="r1")
    assert (raised.status, raised.request_id) == (429, "r1")
    assert str(raised) == "rate limited"


@pytest.mark.parametrize("attribute", ["status", "request_id"])
def test_status_and_request_id_are_read_only(attribute: str) -> None:
    raised = MassiveError("x", status=500, request_id="r1")
    with pytest.raises(AttributeError):
        setattr(raised, attribute, None)


@pytest.mark.parametrize(
    ("wire", "public"),
    [
        (WireError, MassiveError),
        (WireAuthError, AuthenticationError),
        (WireEntitlementError, EntitlementError),
    ],
)
def test_from_wire_maps_each_wire_error(wire: type[WireError], public: type[MassiveError]) -> None:
    converted = from_wire(
        wire(
            "HTTP 403 from GET /v3/quotes/AAPL (request_id r9): not entitled",
            status=403,
            host="api.massive.com",
            path="/v3/quotes/AAPL",
            request_id="r9",
        )
    )
    assert type(converted) is public
    assert str(converted) == "HTTP 403 from GET /v3/quotes/AAPL (request_id r9): not entitled"
    assert (converted.status, converted.request_id) == (403, "r9")


def test_from_wire_keeps_missing_context_as_none() -> None:
    converted = from_wire(WireError("connection reset by peer"))
    assert (converted.status, converted.request_id) == (None, None)
    assert converted.args == ("connection reset by peer",)


@pytest.mark.parametrize("error", [AuthenticationError, EntitlementError, PermanentError])
def test_specific_errors_are_massive_errors(error: type[MassiveError]) -> None:
    with pytest.raises(MassiveError, match="detail"):
        raise error("detail")


def test_errors_are_distinct() -> None:
    assert not issubclass(AuthenticationError, EntitlementError)
    assert not issubclass(EntitlementError, AuthenticationError)
    # A permanent failure is neither: a poll() fails on it as on them, for its own reason.
    assert not issubclass(PermanentError, (AuthenticationError, EntitlementError))
