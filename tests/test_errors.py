import ast
import builtins
import dataclasses
import functools
import importlib
import inspect
import pkgutil
import re
import textwrap
from collections.abc import Callable, Iterable, Iterator, Sequence
from pathlib import Path
from typing import TypeGuard

import pytest
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    RAISES_THROUGH,
    READ_STOPPED,
    AuthenticationError,
    BrokenGuaranteeError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    PermanentError,
    RowDataError,
    SessionClosedError,
    copy_error,
    every_symbol_failed,
    failure_text,
    from_wire,
    is_bug_of,
    library_bug,
    raises_through,
    required,
    required_instance,
    session_closed,
    source_of,
    user_defect,
)
from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError, WireFailure
from deephaven_massive._session import MassiveSession
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.queries import Query

REFUSED = (
    "Massive answered GET /v3/quotes/AAPL on api.massive.com with HTTP 403 (request_id r9): "
    '"You are not entitled to this data".'
)
"""A wire error's message, as the REST response module words Massive's answer."""

ALL_ERRORS = [
    MassiveError,
    AuthenticationError,
    EntitlementError,
    PermanentError,
    SessionClosedError,
]


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
        (functools.partial(WireError, kind=WireFailure.REFUSED), MassiveError),
        (WireAuthError, AuthenticationError),
        (WireEntitlementError, EntitlementError),
    ],
)
def test_from_wire_maps_each_wire_error(
    wire: Callable[..., WireError], public: type[MassiveError]
) -> None:
    converted = from_wire(
        wire(REFUSED, status=403, host="api.massive.com", path="/v3/quotes/AAPL", request_id="r9")
    )
    assert type(converted) is public
    assert str(converted) == REFUSED
    assert (converted.status, converted.request_id) == (403, "r9")


@pytest.mark.parametrize(
    "message",
    ["stocks.trades history: the request was cancelled", "stocks.trades history: cancelled."],
    ids=["without-a-period", "with-a-period"],
)
def test_a_stopped_read_says_why_it_stopped_and_what_to_do(message: str) -> None:
    converted = from_wire(WireError(message, kind=WireFailure.STOPPED))
    assert str(converted) == (
        f"{message.rstrip('.')}. The read stopped because its table was released or its session "
        "closed. Call the method again for a new table."
    )
    assert READ_STOPPED == (
        "The read stopped because its table was released or its session closed. Call the method "
        "again for a new table."
    )
    assert converted.source is ErrorSource.USER


def test_from_wire_keeps_missing_context_as_none() -> None:
    converted = from_wire(WireError("connection reset by peer", kind=WireFailure.NO_ANSWER))
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


@pytest.mark.parametrize("error", [MassiveError, AuthenticationError, EntitlementError])
def test_an_error_is_massives_unless_its_source_says_otherwise(error: type[MassiveError]) -> None:
    assert error("x").source is ErrorSource.MASSIVE
    assert error("x", source=ErrorSource.USER).source is ErrorSource.USER


def test_a_row_the_library_cannot_read_is_massive_s_data_and_no_bug() -> None:
    error = RowDataError("stocks.trades: Massive's flat file has a malformed line.")
    assert (error.source, error.is_bug, error.is_retryable) == (ErrorSource.MASSIVE, False, False)
    assert str(error) == "stocks.trades: Massive's flat file has a malformed line."


def test_a_subclass_sets_the_source_and_bug_flag_of_its_every_instance() -> None:
    class UnreadableError(MassiveError):
        default_source = ErrorSource.LIBRARY

    class MisuseError(MassiveError):
        default_source = ErrorSource.USER
        default_is_bug = True

    assert (UnreadableError("x").source, UnreadableError("x").is_bug) == (ErrorSource.LIBRARY, True)
    assert UnreadableError("x", source=ErrorSource.MASSIVE).source is ErrorSource.MASSIVE
    assert (MisuseError("x").source, MisuseError("x").is_bug) == (ErrorSource.USER, True)
    assert not MisuseError("x", is_bug=False).is_bug


def test_a_library_error_is_always_a_bug_and_only_it_is_by_default() -> None:
    assert MassiveError("x", source=ErrorSource.LIBRARY, is_bug=False).is_bug
    for source in (ErrorSource.MASSIVE, ErrorSource.NETWORK, ErrorSource.USER):
        assert not MassiveError("x", source=source).is_bug
    assert MassiveError("your table failed", source=ErrorSource.USER, is_bug=True).is_bug


@pytest.mark.parametrize("attribute", ["source", "is_retryable", "is_bug"])
def test_the_source_and_flags_are_read_only(attribute: str) -> None:
    with pytest.raises(AttributeError):
        setattr(MassiveError("x"), attribute, None)


def test_every_source_has_a_lowercase_value() -> None:
    assert [s.value for s in ErrorSource] == ["massive", "network", "user", "library"]


@pytest.mark.parametrize(
    ("wire", "source"),
    [
        (WireError("HTTP 404", status=404, kind=WireFailure.REFUSED), ErrorSource.MASSIVE),
        # A WebSocket status message refusing a subscription: Massive's answer, with no status.
        (WireError("max connections", kind=WireFailure.REFUSED), ErrorSource.MASSIVE),
        (WireError("connection reset", kind=WireFailure.NO_ANSWER), ErrorSource.NETWORK),
        (
            WireError("TLS certificate verification failed", kind=WireFailure.LOCAL),
            ErrorSource.USER,
        ),
        # Data Massive delivered that the client could not read is the library's to fix, and
        # so is a failure of the client itself; a file whose every line is malformed is
        # Massive's data.
        (WireError("bad file", kind=WireFailure.UNREADABLE), ErrorSource.LIBRARY),
        (WireError("bad lines", kind=WireFailure.UNREADABLE_ROWS), ErrorSource.MASSIVE),
        (WireError("bug", kind=WireFailure.CLIENT_DEFECT), ErrorSource.LIBRARY),
    ],
)
def test_from_wire_takes_the_source_the_wire_client_knows(
    wire: WireError, source: ErrorSource
) -> None:
    assert from_wire(wire).source is source


def test_from_wire_takes_the_source_the_caller_knows() -> None:
    rejected = from_wire(WireError("bad row", kind=WireFailure.REFUSED), source=ErrorSource.LIBRARY)
    assert (rejected.source, rejected.is_bug) == (ErrorSource.LIBRARY, True)


@pytest.mark.parametrize(
    ("error", "source", "is_bug"),
    [
        (MassiveError("x", source=ErrorSource.USER), ErrorSource.USER, False),
        (MassiveError("x"), ErrorSource.MASSIVE, False),
        (KeyError("x"), ErrorSource.LIBRARY, True),  # the library does not expect it of itself
        (KeyboardInterrupt(), ErrorSource.USER, False),  # an interrupt is no bug
    ],
)
def test_source_of_and_is_bug_of_read_any_exception(
    error: BaseException, source: ErrorSource, is_bug: bool
) -> None:
    assert source_of(error) is source
    assert is_bug_of(error) is is_bug


@pytest.mark.parametrize(
    ("status", "retryable"),
    [
        (429, True),
        (500, True),
        (502, True),
        (503, True),
        (504, True),
        (400, False),
        (401, False),
        (403, False),
        (404, False),
        (501, False),  # not implemented: the server will not do it later either
        (None, False),
    ],
)
def test_is_retryable_follows_the_status(status: int | None, retryable: bool) -> None:
    assert MassiveError("x", status=status).is_retryable is retryable


def test_a_network_error_is_retryable_and_the_raiser_may_say_otherwise() -> None:
    assert MassiveError("connection reset", source=ErrorSource.NETWORK).is_retryable
    assert MassiveError("not published yet", is_retryable=True).is_retryable
    assert not MassiveError("rate limited", status=429, is_retryable=False).is_retryable


def test_a_bug_is_never_retryable() -> None:
    assert not MassiveError("x", status=503, source=ErrorSource.LIBRARY).is_retryable
    assert not MassiveError("x", status=503, source=ErrorSource.USER, is_bug=True).is_retryable
    assert not MassiveError("x", is_retryable=True, is_bug=True).is_retryable


@pytest.mark.parametrize(
    ("wire", "retryable"),
    [
        (WireError("HTTP 503", status=503, kind=WireFailure.REFUSED), True),
        (WireError("rate limited", status=429, kind=WireFailure.REFUSED), True),
        # S3's RequestTimeout: an HTTP 400 that means try again.
        (
            WireError(
                "HTTP 400 (RequestTimeout)", status=400, kind=WireFailure.REFUSED, means_retry=True
            ),
            True,
        ),
        (WireError("connection reset", kind=WireFailure.NO_ANSWER), True),
        (WireError("TLS certificate verification failed", kind=WireFailure.LOCAL), False),
        (WireError("HTTP 404", status=404, kind=WireFailure.REFUSED), False),
        (WireAuthError("HTTP 401", status=401), False),
        (WireEntitlementError("HTTP 403", status=403), False),
        # Data the client could not read is a bug, whatever its status.
        (WireError("bad body", status=200, kind=WireFailure.UNREADABLE), False),
    ],
)
def test_from_wire_says_whether_the_call_may_succeed_later(
    wire: WireError, retryable: bool
) -> None:
    assert from_wire(wire).is_retryable is retryable


def test_a_closed_session_error_is_public_the_users_and_never_retryable() -> None:
    assert dm.SessionClosedError is SessionClosedError
    error = SessionClosedError(session_closed())
    assert isinstance(error, dm.MassiveError)
    assert (error.source, error.is_bug, error.is_retryable) == (ErrorSource.USER, False, False)


def test_a_closed_session_message_says_so_and_names_the_fix() -> None:
    assert (
        session_closed() == "This session is closed. Open a new dm.MassiveSession() to read again."
    )
    assert NEW_SESSION == "Open a new dm.MassiveSession() to read again."


def test_a_new_session_reads_again_after_one_is_closed() -> None:
    # The fix the message names: a call the closed session refuses, a new session serves.
    config = dm.MassiveConfig(api_key="test-key")
    closed = dm.MassiveSession(config)
    closed.close()
    with pytest.raises(dm.SessionClosedError):
        closed.stocks.dividends("AAPL").fetch()
    with dm.MassiveSession(config) as m:
        assert m.stocks.dividends("AAPL") is not None


def test_a_copy_keeps_the_class_and_every_attribute() -> None:
    error = EntitlementError("no", status=403, request_id="r1", source=ErrorSource.USER)
    copied = copy_error(error, "where: no")
    assert type(copied) is EntitlementError
    assert copied is not error
    assert str(copied) == "where: no"
    assert (copied.status, copied.request_id, copied.source) == (403, "r1", ErrorSource.USER)
    assert (copied.is_retryable, copied.is_bug) == (False, False)
    assert str(copy_error(error)) == "no"


def test_every_symbol_failing_is_retryable_only_when_each_symbol_is() -> None:
    busy = MassiveError("AAPL: HTTP 503", status=503)
    gone = MassiveError("MSFT: HTTP 404", status=404)
    assert every_symbol_failed([busy, busy]).is_retryable
    first = every_symbol_failed([busy, gone])
    assert (str(first), first.status, first.is_retryable) == ("AAPL: HTTP 503", 503, False)


def test_every_symbol_failing_reports_a_bug_among_them() -> None:
    busy = MassiveError("AAPL: HTTP 429", status=429)
    broken = MassiveError("MSFT: a row broke", source=ErrorSource.LIBRARY)
    first = every_symbol_failed([busy, broken])
    # A bug is never hidden behind another symbol's rate limit.
    assert (str(first), first.source, first.is_bug) == (
        f"MSFT: a row broke. {LIBRARY_BUG}",
        ErrorSource.LIBRARY,
        True,
    )
    assert not first.is_retryable


def test_a_library_bug_message_ends_with_how_to_report_it() -> None:
    assert library_bug("the flush thread died") == f"the flush thread died. {LIBRARY_BUG}"
    assert library_bug("it died.") == f"it died. {LIBRARY_BUG}"
    assert LIBRARY_BUG.startswith("This is a bug in deephaven-massive.")
    assert "dm.enable_debug_logging()" in LIBRARY_BUG
    assert "https://github.com/chipkent/deephaven-massive/issues" in LIBRARY_BUG


def test_a_user_defect_message_names_the_fix_and_asks_for_no_report() -> None:
    problem = (
        "The symbol table you passed to stocks.quotes has failed, so this table can no longer "
        "follow it"
    )
    text = user_defect(problem, "Fix that table, then call stocks.quotes again")
    assert text == (
        "The symbol table you passed to stocks.quotes has failed, so this table can no longer "
        "follow it. Fix that table, then call stocks.quotes again."
    )
    assert user_defect(f"{problem}.", "Fix that table, then call stocks.quotes again.") == text
    assert LIBRARY_BUG not in text


def test_required_returns_a_value_and_names_a_missing_one_a_library_defect() -> None:
    assert required(0, "a count") == 0  # a falsy value is present
    with pytest.raises(BrokenGuaranteeError) as raised:
        required(None, "stocks.trades's history path")
    assert str(raised.value) == f"stocks.trades's history path is missing. {LIBRARY_BUG}"
    # A user's `except dm.MassiveError` catches it, as the library bug it is.
    assert isinstance(raised.value, MassiveError)
    assert (raised.value.source, raised.value.is_bug) == (ErrorSource.LIBRARY, True)
    assert not raised.value.is_retryable


def test_required_instance_returns_a_value_and_names_another_type_a_library_defect() -> None:
    assert required_instance(3, int, "a count") == 3
    with pytest.raises(BrokenGuaranteeError) as raised:
        required_instance(None, str, "stocks.trades's history path")
    assert str(raised.value) == (
        f"stocks.trades's history path is NoneType, not str. {LIBRARY_BUG}"
    )
    assert failure_text(raised.value) == str(raised.value)  # its message says it is a bug


def test_failure_text_adds_the_report_only_to_a_library_defect() -> None:
    assert failure_text(MassiveError("refused")) == "refused"
    assert failure_text(MassiveError("fix it", source=ErrorSource.USER)) == "fix it"
    defect = MassiveError("row broke", source=ErrorSource.LIBRARY)
    assert failure_text(defect) == f"row broke. {LIBRARY_BUG}"
    already = MassiveError(library_bug("row broke"), source=ErrorSource.LIBRARY)
    assert failure_text(already) == f"row broke. {LIBRARY_BUG}"  # never twice
    assert failure_text(KeyError("k")) == f"KeyError: 'k'. {LIBRARY_BUG}"


# --- Raises sections ----------------------------------------------------------------------------
#
# A function's `Raises:` section must list exactly what it raises itself and what the helpers it
# declares with `raises_through` list in theirs. Ruff's DOC502 cannot check a section whose
# errors come from helpers, so it is off, and this check covers the public API instead.

_SECTION_ENTRY = re.compile(r"^    ([A-Za-z_][\w.]*):")


def _documented(function: Callable[..., object]) -> frozenset[str]:
    """Return the exception names of a function's Google `Raises:` section, without module paths."""
    names: set[str] = set()
    inside = False
    for line in (inspect.getdoc(function) or "").splitlines():
        if line == "Raises:":
            inside = True
        elif inside and line and not line.startswith(" "):
            inside = False
        elif inside and (entry := _SECTION_ENTRY.match(line)) is not None:
            names.add(entry.group(1).rpartition(".")[2])
    return frozenset(names)


def _own_statements(node: ast.AST) -> Iterator[ast.AST]:
    """Yield a function's nodes, leaving out the bodies of functions and classes defined in it."""
    for child in ast.iter_child_nodes(node):
        if not isinstance(
            child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef
        ):
            yield child
            yield from _own_statements(child)


def _raised_directly(function: Callable[..., object]) -> frozenset[str]:
    """Return the names of the exception classes a function's own `raise` statements name.

    A `raise` of a variable or of a call's result (`raise error`, `raise self._refusal()`) names
    no class and is left out: such a function declares the helper the exception comes from.
    """
    target = inspect.unwrap(function)
    tree = ast.parse(textwrap.dedent(inspect.getsource(target)))
    found: set[str] = set()
    for node in _own_statements(tree.body[0]):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        named = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
        if not isinstance(named, ast.Name):
            continue
        value = target.__globals__.get(named.id, getattr(builtins, named.id, None))
        if isinstance(value, type) and issubclass(value, BaseException):
            found.add(value.__name__)
    return frozenset(found)


def _public_name(error: str, helper: Callable[..., object]) -> str:
    """Name the exception a public function documents for one its helper lists.

    A helper may list a private exception class, such as `TimeTextError`, a `ValueError` that
    lets the library tell one bad input from another. A public docstring names the nearest
    class a user can catch: a builtin, or one `dm` exports.

    Returns:
        The name of that class; `error` itself when it names no class the helper can see.
    """
    raised = inspect.unwrap(helper).__globals__.get(error, getattr(builtins, error, None))
    if not isinstance(raised, type):
        return error
    # Every exception class derives from a builtin one, so the search always ends.
    return next(
        cls.__name__
        for cls in raised.__mro__
        if hasattr(builtins, cls.__name__) or cls.__name__ in dm.__all__
    )


def _raises_problems(function: Callable[..., object], *, public: bool = False) -> list[str]:
    """Say how a function's `Raises:` section differs from what it and its helpers raise.

    Args:
        function: The function to check.
        public: True for a function of the public API, whose section names only exceptions a
            user can catch (`_public_name`).

    Returns:
        One line per difference; empty when the section is exact.
    """
    expected = set(_raised_directly(function))
    for helper in getattr(function, RAISES_THROUGH, ()):
        listed = _documented(helper)
        expected |= {_public_name(e, helper) for e in listed} if public else listed
    documented = _documented(function)
    name = function.__qualname__
    return [
        *(
            f"{name} does not list {error}, which it or a helper raises"
            for error in sorted(expected - documented)
        ),
        *(
            f"{name} lists {error}, which neither it nor a declared helper raises"
            for error in sorted(documented - expected)
        ),
    ]


def _public_methods(cls: type) -> Iterator[Callable[..., object]]:
    """Yield the public methods a class defines itself, not those it inherits."""
    for name, value in vars(cls).items():
        if not name.startswith("_") and inspect.isfunction(value):
            yield value


def _subclasses(cls: type) -> Iterator[type]:
    """Yield every subclass of a class, at any depth."""
    sub: type
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)


_WIRE_PACKAGE = f"{dm.__name__}._massive"
"""The wire package, which only its own tests import beyond its interface; it declares nothing."""


def _library_modules(path: Sequence[str], prefix: str) -> Iterator[str]:
    """Yield the names of the library's modules below a package, leaving the wire package out.

    `pkgutil.walk_packages` would import every subpackage to look inside it, the wire package's
    too; this walk never enters it.
    """
    for info in pkgutil.iter_modules(path, prefix):
        if info.name == _WIRE_PACKAGE:
            continue
        yield info.name
        if info.ispkg:
            package = importlib.import_module(info.name)
            yield from _library_modules(package.__path__, f"{info.name}.")


def _declared() -> Iterator[Callable[..., object]]:
    """Yield every function and method of the library that carries a `raises_through` record."""
    for name in _library_modules(dm.__path__, f"{dm.__name__}."):
        module = importlib.import_module(name)
        for value in vars(module).values():
            members = vars(value).values() if isinstance(value, type) else (value,)
            for member in members:
                function = member.fget if isinstance(member, property) else member
                if (
                    callable(function)
                    and getattr(function, "__module__", None) == name
                    and RAISES_THROUGH in getattr(function, "__dict__", {})
                ):
                    yield function


def _public_api() -> dict[str, Callable[..., object]]:
    """Return the public API's functions and methods by qualified name.

    They are the functions `dm` exports, and the public methods of the session, of every
    namespace, and of the query classes.
    """
    found: list[Callable[..., object]] = [
        value for name in dm.__all__ if inspect.isfunction(value := getattr(dm, name))
    ]
    for cls in (MassiveSession, *_subclasses(Namespace), Query, *_subclasses(Query)):
        if cls.__module__.startswith(f"{dm.__name__}."):  # not the stand-ins other tests define
            found.extend(_public_methods(cls))
    return {f"{f.__module__}.{f.__qualname__}": f for f in found}


def _checked_functions() -> dict[str, Callable[..., object]]:
    """Return the public API's functions and methods, and every declared one, by qualified name."""
    declared = {f"{f.__module__}.{f.__qualname__}": f for f in _declared()}
    return {**_public_api(), **declared}


_RAISE_NOTHING = frozenset(
    {
        "deephaven_massive._dataset_list.datasets",
        "deephaven_massive._debug_log.disable_debug_logging",
    }
)
"""The public functions that take no argument a caller can get wrong and raise nothing."""


def test_every_public_function_documents_its_errors() -> None:
    # A public function with no Raises section must be one that raises nothing; otherwise a new
    # dataset method that forgets its `raises_through` declaration would pass the check above.
    silent = {name for name, f in _public_api().items() if not _documented(f)}
    assert silent == _RAISE_NOTHING


def test_every_raises_section_lists_exactly_what_its_function_and_helpers_raise() -> None:
    public = _public_api()
    problems = [
        p
        for name, f in _checked_functions().items()
        for p in _raises_problems(f, public=name in public)
    ]
    assert not problems, "\n".join(problems)


def test_the_check_covers_the_public_api() -> None:
    checked = _checked_functions()
    for name in (
        "deephaven_massive.namespaces._stocks.StocksNamespace.trades",
        "deephaven_massive.queries._fetch.FetchQuery.fetch",
        "deephaven_massive.queries._history.HistoryQuery.history",
        "deephaven_massive._session.MassiveSession.release",
        "deephaven_massive._datasets.symbols.canonical_symbol_forex",
    ):
        assert name in checked


def _parse_number(text: str) -> int:
    return int(text)


_parse_number.__doc__ = """Read a number.

Raises:
    ValueError: If the text is no number.
"""


def test_a_helper_error_the_function_does_not_list_is_reported() -> None:
    @raises_through(_parse_number)
    def count(text: str) -> int:
        if not text:
            raise TypeError("empty")
        return _parse_number(text)

    count.__doc__ = "Count.\n\nRaises:\n    TypeError: If the text is empty.\n"
    assert _raises_problems(count) == [
        (
            "test_a_helper_error_the_function_does_not_list_is_reported.<locals>.count does not "
            "list ValueError, which it or a helper raises"
        )
    ]


def test_a_listed_error_nothing_raises_is_reported() -> None:
    @raises_through(_parse_number)
    def count(text: str) -> int:
        return _parse_number(text)

    count.__doc__ = (
        "Count.\n\nRaises:\n    ValueError: If the text is no number.\n"
        "    dm.EntitlementError: Never.\n"
    )
    assert _raises_problems(count) == [
        (
            "test_a_listed_error_nothing_raises_is_reported.<locals>.count lists EntitlementError, "
            "which neither it nor a declared helper raises"
        )
    ]


def test_an_undeclared_function_may_list_only_what_it_raises_itself() -> None:
    def count(text: str) -> int:
        def inner() -> None:
            raise KeyError(text)  # a nested function's raise is not the function's own

        error = OSError("not raised by class name")
        if not text:
            raise error
        return int(text)

    count.__doc__ = "Count.\n\nRaises:\n    ValueError: If the text is no number.\n"
    assert _raises_problems(count) == [
        (
            "test_an_undeclared_function_may_list_only_what_it_raises_itself.<locals>.count lists "
            "ValueError, which neither it nor a declared helper raises"
        )
    ]
    count.__doc__ = "Count, with no Raises section."
    assert _raises_problems(count) == []


def test_raises_through_records_the_helpers_and_keeps_the_function() -> None:
    def count(text: str) -> int:
        return _parse_number(text)

    assert raises_through(_parse_number)(count) is count
    assert getattr(count, RAISES_THROUGH) == (_parse_number,)


def test_the_walk_never_enters_the_wire_package() -> None:
    names = list(_library_modules(dm.__path__, f"{dm.__name__}."))
    assert f"{dm.__name__}.namespaces._stocks" in names
    assert not [name for name in names if name.startswith(_WIRE_PACKAGE)]


def test_a_public_function_names_the_public_base_of_a_private_helper_error() -> None:
    def helper() -> None:
        """Raise a private error.

        Raises:
            _PrivateValueError: Always.
        """

    def public() -> None:
        """Pass a helper's private error through.

        Raises:
            ValueError: Always, from the helper.
        """

    helper.__globals__["_PrivateValueError"] = type("_PrivateValueError", (ValueError,), {})
    raises_through(helper)(public)
    try:
        assert _raises_problems(public, public=True) == []
        name = public.__qualname__
        assert _raises_problems(public) == [
            f"{name} does not list _PrivateValueError, which it or a helper raises",
            f"{name} lists ValueError, which neither it nor a declared helper raises",
        ]
    finally:
        del helper.__globals__["_PrivateValueError"]


def test_the_row_error_is_permanent() -> None:
    # A poll() table fails on it rather than retrying every cycle.
    assert issubclass(RowDataError, PermanentError)


# --- The layout of a message ----------------------------------------------------------------------
#
# A message a user reads is sentences: no semicolon, no newline, no list rendered by
# `bounded_list`, and no site over `MESSAGE_LITERAL_LIMIT` characters of literal text. The check
# reads each module with `ast` and judges only what the source spells, never a value Massive or
# a user sends, so a `request_id` or a quoted answer can never trip it.
#
# A message site is a `raise`, a call that builds an exception (a callee whose name ends in
# `Error`), a log call at WARNING or above (a callee ending in `warning`, `error`, `exception`, or
# `log`, except `log(logging.DEBUG, ...)` and `log(logging.INFO, ...)`), and a `log_once` or
# `log_failure` call. Every function body, and the module body, that holds one is a scope, and
# every string literal in the scope is a message's, except docstrings and the literals of
# `debug`, `info`, and DEBUG or INFO `log` calls; a nested function is its own scope. The limit
# and the `bounded_list` fault apply per site: to what its expression holds, plus what every local
# variable or module constant it names holds, resolved within the scope. A name bound to a dict or
# a set is not resolved: a message renders its keys or one entry, never its literal text whole.

SOURCE_ROOT = Path(__file__).resolve().parent.parent / "src" / "deephaven_massive"
"""The library's package, whose modules the layout check reads."""

MESSAGE_FIXTURE = Path(__file__).resolve().parent / "data" / "messages_fixture.py.txt"
"""Python source holding one instance of each fault and the cases that pass, read as text so
that no tool imports or lints it as a module."""

MESSAGE_LITERAL_LIMIT = 400
"""The most characters of literal text one message site may hold: a longer message fails the
rule that a message is sentences with the fix last, and its tutorial belongs in the docs."""

EXEMPT_MODULES = frozenset(
    {
        "_dataset_list.py",
        "_datasets/catalog.py",
        "_datasets/column_reference.py",
        "_datasets/readme_plans.py",
        "_datasets/schema.py",
        "_datasets/source_columns.py",
        "_tables/cause_reference.py",
        "_massive/refresh/",
        "_massive/spec/",
    }
)
"""The modules the layout check leaves out: developer and contract checks that fail at import or
in scripts/refresh.py or scripts/render_reference.py; their reader holds the source. Each is a
path under `src/deephaven_massive/`, and a trailing slash names a package."""

_LOG_CALLEES = frozenset({"warning", "error", "exception", "log", "log_once", "log_failure"})
"""The callee names that make a call a log record at WARNING or above (`log` unless quiet)."""

_QUIET_CALLEES = frozenset({"debug", "info"})
"""The callee names of a log record below WARNING, whose literals are not a message's."""

_QUIET_LEVELS = frozenset({"DEBUG", "INFO"})
"""The `logging` levels that make a `log(level, ...)` call quiet."""


@dataclasses.dataclass(frozen=True)
class MessageFault:
    """One fault the layout check found: where, what, and the text.

    Attributes:
        file: The module, relative to the package, or the fixture's name.
        line: The line of the literal or the site.
        fault: What is wrong: `semicolon`, `newline`, `bounded_list`, or the characters over
            the limit.
        text: The literal, or the site's source for a fault of the site.
    """

    file: str
    line: int
    fault: str
    text: str

    @override
    def __str__(self) -> str:
        return f"{self.file}:{self.line}: {self.fault}: {self.text}"


def _callee(call: ast.Call) -> str | None:
    """Return the last name of a call's callee.

    Args:
        call: The call.

    Returns:
        `warning` for `logger.warning(...)` and `log_once` for `log_once(...)`; None for a
        callee that is neither an attribute nor a name, such as `make()(...)`.
    """
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _is_quiet(call: ast.Call) -> bool:
    """Tell whether a call logs below WARNING, so its literals are not a message's.

    Args:
        call: The call.

    Returns:
        True for a `debug` or `info` call, and for `log(logging.DEBUG, ...)` or
        `log(logging.INFO, ...)`.
    """
    name = _callee(call)
    if name in _QUIET_CALLEES:
        return True
    if name != "log" or not call.args:
        return False
    level = call.args[0]
    return isinstance(level, ast.Attribute) and level.attr in _QUIET_LEVELS


def _is_site(node: ast.AST) -> TypeGuard[ast.Raise | ast.Call]:
    """Tell whether a node is a message site.

    Args:
        node: The node.

    Returns:
        True for a `raise`, a call whose callee's name ends in `Error`, a `log_once` or
        `log_failure` call, and a log call at WARNING or above.
    """
    if isinstance(node, ast.Raise):
        return True
    if not isinstance(node, ast.Call):
        return False
    name = _callee(node)
    if name is None:
        return False
    return name.endswith("Error") or (name in _LOG_CALLEES and not _is_quiet(node))


def _is_scope(node: ast.AST) -> bool:
    """Return whether a node starts a scope of its own: a function or a lambda."""
    return isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda)


def _scope_nodes(scope: ast.AST) -> Iterator[ast.AST]:
    """Yield the nodes of a scope, leaving out the nested scopes and their nodes."""
    for child in ast.iter_child_nodes(scope):
        if not _is_scope(child):
            yield child
            yield from _scope_nodes(child)


def _scopes(tree: ast.Module) -> Iterator[ast.AST]:
    """Yield the module and every function or lambda in it, at any depth."""
    yield tree
    yield from (node for node in ast.walk(tree) if _is_scope(node))


@dataclasses.dataclass(frozen=True)
class _Literal:
    """One string literal of a module: where it is, and its text."""

    line: int
    column: int
    text: str


def _string_literals(node: ast.AST) -> Iterator[_Literal]:
    """Yield the string literals under a node, the node included."""
    for found in ast.walk(node):
        if isinstance(found, ast.Constant) and isinstance(found.value, str):
            yield _Literal(found.lineno, found.col_offset, found.value)


def _assigned(scope: ast.AST) -> dict[str, list[ast.expr]]:
    """Return the values assigned to each name in a scope, in order."""
    found: dict[str, list[ast.expr]] = {}
    for node in _scope_nodes(scope):
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = node.targets
            value: ast.expr | None = node.value
        elif isinstance(node, ast.AnnAssign | ast.AugAssign | ast.NamedExpr):
            targets = [node.target]
            value = node.value
        else:
            continue
        if value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                found.setdefault(target.id, []).append(value)
    return found


_UNRENDERED_VALUES = (ast.Dict, ast.DictComp, ast.Set, ast.SetComp)
"""The values whose literals a message never renders whole: it renders a key or one entry."""


@dataclasses.dataclass(frozen=True)
class _Rendered:
    """What one message site renders: its literal text and its `bounded_list` calls."""

    literals: frozenset[_Literal]
    lists: frozenset[ast.Call]


def _site_rendered(expression: ast.AST, assigned: Sequence[dict[str, list[ast.expr]]]) -> _Rendered:
    """Return what a site renders: under its expression, and in the names it uses.

    A name assigned more than once in its scope (one message per branch) contributes the
    literals of the assignment with the most literal text, since a message renders one of them,
    and the `bounded_list` calls of every assignment, since any of them may be the one rendered.
    A name bound to a dict or a set contributes nothing.

    Args:
        expression: The site's expression.
        assigned: The assignments of the site's scope, then of the module.

    Returns:
        The literals and the `bounded_list` calls.
    """
    names: set[str] = set()

    def gather(node: ast.AST) -> _Rendered:
        literals = set(_string_literals(node))
        lists = {
            call
            for call in ast.walk(node)
            if isinstance(call, ast.Call) and _callee(call) == "bounded_list"
        }
        for name in ast.walk(node):
            if not isinstance(name, ast.Name) or name.id in names:
                continue
            names.add(name.id)  # each name once per site, so a cycle of names ends
            values = next((a[name.id] for a in assigned if name.id in a), [])
            choices = [
                gather(value) for value in values if not isinstance(value, _UNRENDERED_VALUES)
            ]
            if choices:
                literals |= max((choice.literals for choice in choices), key=_text_length)
                lists |= {call for choice in choices for call in choice.lists}
        return _Rendered(frozenset(literals), frozenset(lists))

    return gather(expression)


def _text_length(literals: Iterable[_Literal]) -> int:
    """Return how many characters of text some literals hold."""
    return sum(len(literal.text) for literal in literals)


def _site_expression(node: ast.Raise | ast.Call) -> ast.AST:
    """Return what a site renders: a `raise`'s exception, or the call itself."""
    if isinstance(node, ast.Raise):
        return node.exc if node.exc is not None else node
    return node


def message_faults(source: str, file: str) -> list[MessageFault]:
    """Return the layout faults of a module's messages.

    Args:
        source: The module's source.
        file: How to name it in each fault.

    Returns:
        The faults in order of line: a `;` or a newline in a message's literal, a
        `bounded_list` call a message site renders, and a site whose literals pass
        `MESSAGE_LITERAL_LIMIT` characters.
    """
    tree = ast.parse(source)
    module_assigned = _assigned(tree)
    faults: list[MessageFault] = []
    for scope in _scopes(tree):
        nodes = list(_scope_nodes(scope))
        raised = {id(node.exc) for node in nodes if isinstance(node, ast.Raise)}
        sites = [n for n in nodes if _is_site(n) and id(n) not in raised]
        if not sites:
            continue
        quiet_calls = [node for node in nodes if isinstance(node, ast.Call) and _is_quiet(node)]
        quiet = {literal for call in quiet_calls for literal in _string_literals(call)}
        docstrings = {
            literal
            for node in nodes
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
            for literal in _string_literals(node.value)
        }
        assigned = [_assigned(scope), module_assigned] if scope is not tree else [module_assigned]
        literals = (
            {literal for node in nodes for literal in _string_literals(node)} - quiet - docstrings
        )
        lists: set[ast.Call] = set()
        for site in sites:
            expression = _site_expression(site)
            rendered = _site_rendered(expression, assigned)
            literals |= rendered.literals
            lists |= rendered.lists
            total = _text_length(rendered.literals)
            if total > MESSAGE_LITERAL_LIMIT:
                faults.append(
                    MessageFault(
                        file,
                        site.lineno,
                        f"{total} characters of literal text, over {MESSAGE_LITERAL_LIMIT}",
                        ast.unparse(expression)[:60],
                    )
                )
        for literal in literals:
            if ";" in literal.text:
                faults.append(MessageFault(file, literal.line, "semicolon", literal.text))
            if "\n" in literal.text:
                faults.append(MessageFault(file, literal.line, "newline", literal.text))
        faults.extend(
            MessageFault(file, call.lineno, "bounded_list", ast.unparse(call)) for call in lists
        )
    return sorted(faults, key=lambda fault: (fault.line, fault.fault, fault.text))


def is_exempt(module: str) -> bool:
    """Tell whether a module's messages are developer checks the layout check leaves out.

    Args:
        module: The module's path under `src/deephaven_massive/`, with `/` separators.

    Returns:
        True when `EXEMPT_MODULES` names it or a package it is in.
    """
    return module in EXEMPT_MODULES or any(
        module.startswith(package) for package in EXEMPT_MODULES if package.endswith("/")
    )


def library_modules() -> list[tuple[str, str]]:
    """Return each module the layout check reads, as its path under the package and its source."""
    return [
        (path.relative_to(SOURCE_ROOT).as_posix(), path.read_text(encoding="utf-8"))
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if not is_exempt(path.relative_to(SOURCE_ROOT).as_posix())
    ]


def test_the_fixture_shows_each_fault_once_and_passes_every_other_case() -> None:
    found = message_faults(MESSAGE_FIXTURE.read_text(encoding="utf-8"), "fixture")
    assert [str(fault) for fault in found] == [
        "fixture:13: semicolon: pass the spelling Massive uses; a module constant holds the fix.",
        "fixture:17: semicolon:  is not a ticker; pass the spelling Massive uses.",
        "fixture:21: newline: The first line.\nThe second line.",
        "fixture:25: bounded_list: bounded_list(symbols)",
        "fixture:29: semicolon: remove it; or pass on_error.",
        (
            "fixture:34: 419 characters of literal text, over 400: ValueError(f'{dataset}: "
            "Massive sent rows the library cannot"
        ),
        "fixture:48: bounded_list: bounded_list(symbols)",
    ]


def test_the_exempt_modules_exist_and_nothing_else_is_exempt() -> None:
    for module in EXEMPT_MODULES:
        assert (SOURCE_ROOT / module).exists(), module
    read = {module for module, _ in library_modules()}
    assert "_errors.py" in read
    assert not any(is_exempt(module) for module in read)
    assert is_exempt("_massive/spec/types.py")
    assert not is_exempt("_massive/rest/response.py")


def test_every_message_in_the_library_is_sentences_within_the_limit() -> None:
    # Every message a user or an agent reads is one line of sentences: a semicolon chain, a
    # newline, a `bounded_list` count-and-brackets list, or a tutorial past the limit fails here,
    # with each fault's module, line, and text, so the next message written keeps the form.
    faults = [
        str(fault)
        for module, source in library_modules()
        for fault in message_faults(source, module)
    ]
    assert faults == []
