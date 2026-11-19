"""`ConnectionSlots`: the bound on the requests one wire client has open at once.

A REST client and a flat-file client each keep a pool of connections of a fixed size. Their
callers are many threads with no common bound: every `fetch()` fan-out, every poller, every
background read, and every `history()` call has threads of its own. Without a bound, two
overlapping fan-outs send more requests at once than the pool holds; urllib3 then opens an
extra connection for each, and closes it again when it comes back, logging "Connection pool is
full, discarding connection" each time. So each client takes a slot before it sends a request
(the REST client for one attempt, the flat-file client for as long as an object's body is
open) and gives it back after, and the number of slots is the size of its pool: every request
reuses a pooled connection, and none is discarded.

A slot is held only while a request is on the wire or a body is being read, never across a
wait for the rate governor or a retry, so a thread that waits for a slot waits only for other
requests to finish. Waiting ends at once when the caller's `Cancel` token is set.

This module imports only the standard library and `net.cancel`.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deephaven_massive._massive.net.cancel import Cancel

__all__ = ["ConnectionSlots"]


class ConnectionSlots:
    """A fixed number of slots, one per pooled connection, that requests take and give back.

    Each client creates one with its pool's size. Thread-safe: one leaf lock guards the count,
    and no call is made while it is held but to wake waiting threads.

    Example:
        ::

            from deephaven_massive._massive.net.cancel import Cancel
            from deephaven_massive._massive.net.slots import ConnectionSlots

            slots = ConnectionSlots(8)
            stop = Cancel()
            if slots.acquire(stop):
                try:
                    ...  # send the request
                finally:
                    slots.release()
    """

    __slots__ = ("_free", "_ready", "_size")

    def __init__(self, size: int) -> None:
        """Create the slots, all free.

        Args:
            size: How many slots: the size of the client's connection pool, at least 1.

        Raises:
            ValueError: If `size` is less than 1.
        """
        if size < 1:
            raise ValueError(f"size={size!r} is not at least 1 connection slot.")
        self._size = size
        self._free = size
        self._ready = threading.Condition(threading.Lock())

    def acquire(self, stop: Cancel) -> bool:
        """Take a slot, waiting until one is free or `stop` is set.

        Blocks the calling thread while every slot is taken: until another request gives its
        slot back, or `stop` is set (by the caller, or by the client as it closes). Never call
        it holding a lock another request needs to finish.

        Args:
            stop: Ends the wait when set; a token already set takes no slot.

        Returns:
            True with a slot taken, which the caller must `release`; False when `stop` was set
            first, with no slot taken.
        """
        forget = stop.on_set(self._wake)
        try:
            with self._ready:
                while not stop.is_set():
                    if self._free:
                        self._free -= 1
                        return True
                    self._ready.wait()
                return False
        finally:
            forget()

    def release(self) -> None:
        """Give a slot back, waking the threads that wait for one.

        Raises:
            RuntimeError: If every slot is already free: a slot was given back twice.
        """
        with self._ready:
            if self._free == self._size:
                raise RuntimeError("A connection slot was given back that was never taken.")
            self._free += 1
            # Every waiter, since one woken whose `stop` is set leaves without the slot.
            self._ready.notify_all()

    def _wake(self) -> None:
        """Wake every waiting thread, so one whose `stop` was just set returns."""
        with self._ready:
            self._ready.notify_all()
