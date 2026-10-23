import pytest

from deephaven_massive._errors import AuthenticationError, EntitlementError, MassiveError


@pytest.mark.parametrize("error", [AuthenticationError, EntitlementError])
def test_specific_errors_are_massive_errors(error: type[MassiveError]) -> None:
    with pytest.raises(MassiveError, match="detail"):
        raise error("detail")


def test_errors_are_distinct() -> None:
    assert not issubclass(AuthenticationError, EntitlementError)
    assert not issubclass(EntitlementError, AuthenticationError)
