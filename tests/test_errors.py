import pytest

from deephaven_massive._errors import (
    LIBRARY_BUG,
    AuthenticationError,
    EntitlementError,
    ErrorCause,
    MassiveError,
    PermanentError,
    cause_of,
    failure_text,
    from_wire,
    library_bug,
    user_defect,
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


@pytest.mark.parametrize("error", ALL_ERRORS)
def test_an_error_is_expected_unless_its_cause_says_otherwise(error: type[MassiveError]) -> None:
    assert error("x").cause is ErrorCause.EXPECTED
    assert error("x", cause=ErrorCause.USER_DEFECT).cause is ErrorCause.USER_DEFECT


def test_a_subclass_sets_the_cause_of_its_every_instance() -> None:
    class UnreadableError(MassiveError):
        default_cause = ErrorCause.LIBRARY_DEFECT

    assert UnreadableError("x").cause is ErrorCause.LIBRARY_DEFECT
    assert UnreadableError("x", cause=ErrorCause.EXPECTED).cause is ErrorCause.EXPECTED


def test_the_cause_is_read_only() -> None:
    with pytest.raises(AttributeError):
        MassiveError("x").cause = ErrorCause.LIBRARY_DEFECT  # type: ignore[misc]  # read-only


def test_from_wire_carries_the_cause_the_caller_knows() -> None:
    assert from_wire(WireError("x")).cause is ErrorCause.EXPECTED
    # Data Massive delivered that the client could not read is the library's to fix, and so
    # is a failure of the client itself.
    assert from_wire(WireError("bad file", unreadable=True)).cause is ErrorCause.LIBRARY_DEFECT
    assert from_wire(WireError("bug", client_defect=True)).cause is ErrorCause.LIBRARY_DEFECT
    rejected = from_wire(WireError("bad row"), cause=ErrorCause.LIBRARY_DEFECT)
    assert rejected.cause is ErrorCause.LIBRARY_DEFECT


@pytest.mark.parametrize(
    ("error", "cause"),
    [
        (MassiveError("x", cause=ErrorCause.USER_DEFECT), ErrorCause.USER_DEFECT),
        (MassiveError("x"), ErrorCause.EXPECTED),
        (KeyError("x"), ErrorCause.LIBRARY_DEFECT),  # the library does not expect it of itself
        (KeyboardInterrupt(), ErrorCause.EXPECTED),  # an interrupt is no defect
    ],
)
def test_cause_of_reads_any_exception(error: BaseException, cause: ErrorCause) -> None:
    assert cause_of(error) is cause


def test_a_library_bug_message_ends_with_how_to_report_it() -> None:
    assert library_bug("the flush thread died") == f"the flush thread died. {LIBRARY_BUG}"
    assert library_bug("it died.") == f"it died. {LIBRARY_BUG}"
    assert LIBRARY_BUG.startswith("This is a bug in deephaven-massive.")
    assert "dm.enable_debug_logging()" in LIBRARY_BUG
    assert "https://github.com/chipkent/deephaven-massive/issues" in LIBRARY_BUG


def test_a_user_defect_message_names_the_fix_and_asks_for_no_report() -> None:
    text = user_defect("the symbol table failed upstream: boom", "fix that table")
    assert text == "the symbol table failed upstream: boom; fix that table"
    assert LIBRARY_BUG not in text


def test_failure_text_adds_the_report_only_to_a_library_defect() -> None:
    assert failure_text(MassiveError("refused")) == "refused"
    assert failure_text(MassiveError("fix it", cause=ErrorCause.USER_DEFECT)) == "fix it"
    defect = MassiveError("row broke", cause=ErrorCause.LIBRARY_DEFECT)
    assert failure_text(defect) == f"row broke. {LIBRARY_BUG}"
    already = MassiveError(library_bug("row broke"), cause=ErrorCause.LIBRARY_DEFECT)
    assert failure_text(already) == f"row broke. {LIBRARY_BUG}"  # never twice
    assert failure_text(KeyError("k")) == f"KeyError: 'k'. {LIBRARY_BUG}"
