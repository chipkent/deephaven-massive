import pytest

from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError

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
