"""`Namespace`: the base class of the objects that hold a session's datasets, such as `m.stocks`.

Every namespace on a session is an instance of a subclass of `Namespace`, which holds the
session core its dataset methods pass to the shared factory. When a subclass is defined,
`Namespace` wraps each of its dataset methods in `shows_call` (`queries/_call.py`), so every
query a namespace returns shows, as its `repr`, the call a user typed.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, ClassVar

from typing_extensions import override

from deephaven_massive.queries._call import shows_call

if TYPE_CHECKING:
    from deephaven_massive._session import MassiveSession

__all__ = ["Namespace"]


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
        """Wrap each dataset method the subclass defines in `shows_call`.

        Every public function of a namespace class is a dataset factory, so each is wrapped
        when the class is created, at import; the wrapper keeps the method's name, docstring,
        and signature. A sub-namespace is a property, which is left alone.

        Args:
            **kwargs: Passed on to `object.__init_subclass__`.
        """
        super().__init_subclass__(**kwargs)
        record = shows_call(f"m.{cls._path}")
        for name, member in list(vars(cls).items()):
            if not name.startswith("_") and inspect.isfunction(member):
                setattr(cls, name, record(member))

    def __init__(self, session: MassiveSession) -> None:
        """Bind the namespace to the session it belongs to.

        Users reach a namespace through its session, such as `m.stocks`, and never construct
        one. The namespace keeps the session's core, which its dataset methods pass to the
        shared factory, and not the session itself.

        Args:
            session: The session the namespace belongs to.
        """
        self._core = session._core  # noqa: SLF001 - a namespace belongs to its session and shares its core

    @override
    def __repr__(self) -> str:
        """Name the session and the namespace's path.

        Returns:
            For example `<MassiveSession #1 stocks>` or `<MassiveSession #1 partners.tmx>`.
        """
        return f"<{self._core.label} {self._path}>"
