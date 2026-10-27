import importlib.metadata

from deephaven_massive._massive import net
from deephaven_massive._massive.net import errors


def test_user_agent_names_the_installed_version() -> None:
    expected = f"deephaven-massive/{importlib.metadata.version('deephaven-massive')}"
    assert expected == net.USER_AGENT


def test_the_wire_errors_are_re_exported() -> None:
    assert sorted(net.__all__) == [
        "USER_AGENT",
        "WireAuthError",
        "WireEntitlementError",
        "WireError",
    ]
    assert net.WireError is errors.WireError
    assert net.WireAuthError is errors.WireAuthError
    assert net.WireEntitlementError is errors.WireEntitlementError
