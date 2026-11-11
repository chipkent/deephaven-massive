import ast
import builtins
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
    RAISES_THROUGH,
    AuthenticationError,
    EntitlementError,
    ErrorCause,
    MassiveError,
    PermanentError,
    RowDataError,
    cause_of,
    failure_text,
    from_wire,
    library_bug,
    raises_through,
    user_defect,
)
from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._session import MassiveSession
from deephaven_massive.namespaces._query_building import Namespace
from deephaven_massive.queries import Query

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
        "deephaven_massive.namespaces._options.options.admin_messages",
        "deephaven_massive.namespaces._reference.reference.market_holidays",
        "deephaven_massive.namespaces._reference.reference.market_status",
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
        "deephaven_massive.namespaces._stocks.stocks.trades",
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
