"""The base class of the namespace classes, and the call each of their queries shows.

Every namespace on a session, such as `m.stocks`, is an instance of a subclass of `Namespace`,
which holds the session core the dataset methods pass to the shared factory. When a subclass is
defined, `Namespace` wraps each of its dataset methods in `shows_call`, which records on the
query the method returns the call a user typed, such as `m.stocks.bars('AAPL',
dm.Timespan.DAY)`; that call is the query's `repr`, so a printed query, a log line, and a row
of `m.streams()` all show it. The session's own dataset method, `m.snapshots`, is wrapped the
same way.
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import replace
from typing import TYPE_CHECKING, ClassVar, ParamSpec, TypeVar, get_overloads, overload

from typing_extensions import override

from deephaven_massive.queries import Query
from deephaven_massive.queries._call import QueryCall

if TYPE_CHECKING:
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["Namespace", "shows_call"]

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_Q = TypeVar("_Q", bound=Query)


def _is_default(value: object, default: object) -> bool:
    """Tell whether an argument has its parameter's default, so a query's repr leaves it out.

    Args:
        value: The argument as passed.
        default: The parameter's default, or `inspect.Parameter.empty` for none.

    Returns:
        True for the default object itself, or a value equal to a default other than None
        (`{dm.FuturesVenue.CME, ...}` equals the default `frozenset`). None is compared by
        identity, so a symbol table is never compared with `==`. An array-like value, whose
        `==` answers element by element (a numpy array, a pandas series), is never the
        default: only a plain `bool` answer counts, since the truth of an array raises.
    """
    if value is default:
        return True
    if default is None or default is inspect.Parameter.empty:
        return False
    equal: object = value == default  # an array's `==` is an array
    return isinstance(equal, bool) and equal


def _frozen(value: object) -> object:
    """Return an argument as a query's call keeps it, safe from later changes by the caller.

    Args:
        value: The argument as passed.

    Returns:
        A list as a tuple, a set as a `frozenset`, a mapping as the tuple of its items, and
        anything else unchanged.
    """
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, AbstractSet):
        return frozenset(value)
    if isinstance(value, Mapping):
        return tuple(value.items())
    return value


def shows_call(path: str) -> Callable[[Callable[_P, _Q]], Callable[_P, _Q]]:
    """Make a dataset method record its call on the query it returns, as the query's `repr`.

    The wrapper binds the arguments to the method's signature after the method has returned
    (so a refused call raises the method's own error) and stores a `QueryCall` on the query:
    the method's path from the session, and the arguments that differ from their defaults
    (`_is_default`), in signature order (`_frozen`), and the parameters it requires, which the
    repr writes by position; the repr writes each argument from the query's normalized request
    where that holds it (`QueryCall.text`). Equality of queries is
    unchanged: the call is not part of it. `Namespace` applies it to every public method of
    each namespace class, and `MassiveSession` to its own dataset method, `snapshots`. An
    exception the method raises, a refused argument's `TypeError` or `ValueError`, is logged
    at DEBUG with the method's path and re-raised.

    Args:
        path: The method's owner as reached from a session, such as `"m.options"`.

    Returns:
        A decorator; its wrapper has the method's name, docstring, signature, and
        `raises_through` helpers (`functools.wraps`).
    """

    def decorate(method: Callable[_P, _Q]) -> Callable[_P, _Q]:
        signature = inspect.signature(method)
        called = f"{path}.{method.__name__}"
        required = frozenset(
            name
            for name, param in list(signature.parameters.items())[1:]
            if param.default is inspect.Parameter.empty
            and param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        )

        @functools.wraps(method)
        def factory(*args: _P.args, **kwargs: _P.kwargs) -> _Q:
            try:
                query = method(*args, **kwargs)
            except Exception as error:
                # The caller receives the refusal; the log keeps which method refused, and why.
                logger.debug("%s refused: %s: %s", called, type(error).__name__, error)
                raise
            given = signature.bind(*args, **kwargs).arguments
            arguments = tuple(
                (name, _frozen(given[name]))
                for name, param in list(signature.parameters.items())[1:]
                if name in given and not _is_default(given[name], param.default)
            )
            return replace(query, _call=QueryCall(called, arguments, required))

        return factory

    return decorate


class Namespace:
    """Base class of the namespace classes reached from a session, such as `m.stocks`.

    A namespace is created once per session by `MassiveSession.__init__` and only holds the
    session core. Its methods are the dataset factories; they validate arguments and return
    queries, and never touch the network. Each subclass's public methods are wrapped in
    `shows_call` when the class is defined, so every query a namespace returns shows the call
    that built it. Thread-safe: it has no mutable state.
    """

    __slots__ = ("_core",)

    _path: ClassVar[str] = ""
    """The namespace's path from the session, such as `"stocks"` or `"partners.benzinga"`."""

    @override
    def __init_subclass__(cls, **kwargs: object) -> None:
        """Name each public method by its path, such as `stocks.bars`, and wrap it in `shows_call`.

        Python names a function by its `__qualname__` in the `TypeError` of a wrong call
        (`stocks.bars() takes from 2 to 3 positional arguments but 5 were given`). A user reached
        the method as `m.stocks.bars`, so that path, not the class name `StocksNamespace`, is the
        name the message should show: each public function the class defines itself is renamed to
        its path from the session when the class is created, at import. `typing` files a function's
        overloads under its `__qualname__`, so each overload is renamed too and registered again
        under the new name, which keeps `typing.get_overloads` finding them.

        Every public method of a namespace class is a dataset factory, so each is then wrapped
        in `shows_call`, whose wrapper keeps the new name; a sub-namespace is a property, which
        is left alone.

        Args:
            **kwargs: Passed on to `object.__init_subclass__`.
        """
        super().__init_subclass__(**kwargs)
        record = shows_call(f"m.{cls._path}")
        for name, member in list(vars(cls).items()):
            if not name.startswith("_") and inspect.isfunction(member):
                path = f"{cls._path}.{name}"
                for variant in get_overloads(member):
                    variant.__qualname__ = path
                    overload(variant)
                member.__qualname__ = path
                setattr(cls, name, record(member))

    def __init__(self, _core: SessionCore) -> None:
        """Bind the namespace to its session.

        Args:
            _core: The session's internal core. The leading underscore marks it, and its
                private type, as no part of the public signature: users never construct a
                namespace.
        """
        self._core = _core

    @override
    def __repr__(self) -> str:
        """Name the session and the namespace's path.

        Returns:
            For example `<MassiveSession #1 stocks>` or `<MassiveSession #1 partners.tmx>`.
        """
        return f"<{self._core.label} {self._path}>"
