import ast
import builtins
import functools
import importlib
import inspect
import pkgutil
import re
import textwrap
from collections.abc import Callable, Iterator, Sequence

import pytest

import deephaven_massive as dm
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    RAISES_THROUGH,
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
    source_of,
    user_defect,
)
from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError, WireFailure
from deephaven_massive._session import MassiveSession
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.queries import Query

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
        # so is a failure of the client itself.
        (WireError("bad file", kind=WireFailure.UNREADABLE), ErrorSource.LIBRARY),
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
    error = SessionClosedError("MassiveSession #1 is closed; " + NEW_SESSION)
    assert isinstance(error, dm.MassiveError)
    assert (error.source, error.is_bug, error.is_retryable) == (ErrorSource.USER, False, False)


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
    text = user_defect("the symbol table failed upstream: boom", "fix that table")
    assert text == "the symbol table failed upstream: boom; fix that table"
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
        "deephaven_massive.namespaces._options.OptionsNamespace.admin_messages",
        "deephaven_massive.namespaces._reference.ReferenceNamespace.market_holidays",
        "deephaven_massive.namespaces._reference.ReferenceNamespace.market_status",
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
