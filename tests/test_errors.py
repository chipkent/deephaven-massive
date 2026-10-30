import ast
import builtins
import importlib
import inspect
import pkgutil
import re
import textwrap
from collections.abc import Callable, Iterator

import pytest

import deephaven_massive as dm
from deephaven_massive._errors import (
    RAISES_THROUGH,
    AuthenticationError,
    EntitlementError,
    MassiveError,
    PermanentError,
    from_wire,
    raises_through,
)
from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError
from deephaven_massive._namespaces.base import Namespace
from deephaven_massive._query import QueryBase
from deephaven_massive._session import MassiveSession

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


# --- Raises sections ----------------------------------------------------------------------------
#
# A function's `Raises:` section must list exactly what it raises itself and what the helpers it
# declares with `raises_through` list in theirs. Ruff's DOC502 cannot check a section whose
# errors come from helpers, so it is off for the modules of the public API, and this check
# replaces it there.

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


def _raises_problems(function: Callable[..., object]) -> list[str]:
    """Say how a function's `Raises:` section differs from what it and its helpers raise.

    Returns:
        One line per difference; empty when the section is exact.
    """
    expected = set(_raised_directly(function))
    for helper in getattr(function, RAISES_THROUGH, ()):
        expected |= _documented(helper)
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


def _declared() -> Iterator[Callable[..., object]]:
    """Yield every function and method of the package that carries a `raises_through` record."""
    for info in pkgutil.walk_packages(dm.__path__, f"{dm.__name__}."):
        module = importlib.import_module(info.name)
        for value in vars(module).values():
            members = vars(value).values() if isinstance(value, type) else (value,)
            for member in members:
                function = member.fget if isinstance(member, property) else member
                if (
                    callable(function)
                    and getattr(function, "__module__", None) == info.name
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
    for cls in (MassiveSession, *_subclasses(Namespace), QueryBase, *_subclasses(QueryBase)):
        if cls.__module__.startswith(f"{dm.__name__}."):  # not the stand-ins other tests define
            found.extend(_public_methods(cls))
    return {f"{f.__module__}.{f.__qualname__}": f for f in found}


def _checked_functions() -> dict[str, Callable[..., object]]:
    """Return the public API's functions and methods, and every declared one, by qualified name."""
    declared = {f"{f.__module__}.{f.__qualname__}": f for f in _declared()}
    return {**_public_api(), **declared}


_RAISE_NOTHING = frozenset(
    {
        "deephaven_massive._debug_log.disable_debug_logging",
        "deephaven_massive._namespaces.reference._Reference.market_holidays",
        "deephaven_massive._namespaces.reference._Reference.market_status",
    }
)
"""The public functions that take no argument a caller can get wrong and raise nothing."""


def test_every_public_function_documents_its_errors() -> None:
    # A public function with no Raises section must be one that raises nothing; otherwise a new
    # dataset method that forgets its `raises_through` declaration would pass the check above.
    silent = {name for name, f in _public_api().items() if not _documented(f)}
    assert silent == _RAISE_NOTHING


def test_every_raises_section_lists_exactly_what_its_function_and_helpers_raise() -> None:
    problems = [p for f in _checked_functions().values() for p in _raises_problems(f)]
    assert not problems, "\n".join(problems)


def test_the_check_covers_the_public_api() -> None:
    checked = _checked_functions()
    for name in (
        "deephaven_massive._namespaces.stocks._Stocks.trades",
        "deephaven_massive._query.Query.fetch",
        "deephaven_massive._query.HistoryQuery.history",
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
