"""`Cancel`, the cancellation token every wait of the wire clients can be ended by.

A REST request can wait a long time before it is sent: for a slot of a paced subscription, or
for a pause after Massive refused a request with HTTP 429. Several parties may need to end such
a wait: the client when it closes, a fan-out when one of its requests failed, a poller when its
table is released. Each owns its own token and sets only that one; a token made from others
(`Cancel(*parents)`) is set as soon as any of them is, so one wait listens to all of them
without anyone setting an event it does not own.

`Cancel` is a `threading.Event`, so every `Clock.wait(event, seconds)` accepts it. Unlike a
plain event it is set once and for all (`clear` raises), and it runs callbacks when it is set
(`on_set`), which is how a child follows its parents and how the REST governor wakes a waiting
request at once.

This module imports only the standard library.
"""

from __future__ import annotations

import functools
import itertools
import threading
from typing import TYPE_CHECKING, Self

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType


__all__ = ["Cancel"]


def _nothing() -> None:
    """Do nothing: the remover of a callback that already ran."""


class Cancel(threading.Event):
    """A one-way cancellation flag that can follow other tokens and run callbacks when set.

    Whoever creates a token owns it and is the only one to `set` it; everyone else waits on it
    or reads `is_set()`. A token created with parents is set when any parent is set, and is
    born set when a parent already is, so a request started just after its client closed still
    sees the close. Call `close()` (or leave its `with` block) once a child is no longer
    needed: a parent keeps a reference to each child until then.

    Thread-safe: one leaf lock per token guards its callbacks and orders `set` against
    `on_set`. Callbacks run on the thread that sets the token, after the lock is released, so
    a callback may set other tokens or take other leaf locks; it must not block or raise.

    Example:
        ::

            from deephaven_massive._massive import Cancel

            closed = Cancel()  # owned by a client
            stop = Cancel()  # owned by a poller
            with Cancel(closed, stop) as request:  # one request listens to both
                stop.set()
                assert request.is_set()
            assert not closed.is_set()
    """

    def __init__(self, *parents: Cancel) -> None:
        """Create a token, unset unless a parent is already set.

        Args:
            *parents: Tokens this one follows: setting any of them sets this one. None for a
                token only its owner sets.
        """
        super().__init__()
        self._lock = threading.Lock()
        self._callbacks: dict[int, Callable[[], None]] = {}
        self._keys = itertools.count()
        self._removers = [parent.on_set(self.set) for parent in parents]

    @override
    def set(self) -> None:
        """Set the token, wake its waiters, and run its callbacks once; idempotent.

        The callbacks run on this thread, in registration order, after the token's lock is
        released. Setting a token never sets its parents.
        """
        with self._lock:
            if self.is_set():
                return
            super().set()
            callbacks = list(self._callbacks.values())
            self._callbacks.clear()
        for callback in callbacks:
            callback()

    @override
    def clear(self) -> None:
        """Refuse: a cancellation cannot be undone.

        Raises:
            RuntimeError: Always.
        """
        raise RuntimeError("a Cancel token cannot be cleared; create a new one")

    def on_set(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Run `callback` once when the token is set, or now if it already is.

        Args:
            callback: Called without arguments, on the thread that sets the token (or on this
                one, at once, when the token is already set). It must not block or raise.

        Returns:
            A function that unregisters the callback; it does nothing once the callback has
            run, and may be called any number of times from any thread.
        """
        with self._lock:
            if not self.is_set():
                key = next(self._keys)
                self._callbacks[key] = callback
                return functools.partial(self._forget, key)
        callback()
        return _nothing

    def close(self) -> None:
        """Stop following the parents, so they no longer refer to this token; idempotent.

        The token keeps its state: a token set before `close` stays set, and one that was not
        is no longer set by its parents.
        """
        removers, self._removers = self._removers, []
        for remove in removers:
            remove()

    def __enter__(self) -> Self:
        """Use the token for the length of a `with` block.

        Returns:
            The token.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop following the parents (`close`); the token is not set.

        Args:
            exc_type: The exception type, if the block raised.
            exc: The exception, if the block raised.
            traceback: Its traceback.
        """
        self.close()

    def _forget(self, key: int) -> None:
        """Drop a callback that has not run.

        Args:
            key: The callback's key; an unknown key (a callback that ran) is ignored.
        """
        with self._lock:
            self._callbacks.pop(key, None)
