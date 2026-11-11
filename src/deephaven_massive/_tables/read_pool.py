"""The session's read pool: the threads, at most `max_concurrency`, that run every download.

Each `fetch()` or `history()` that reads in the background (`_tables/background_read.py`) and
each read of a followed read's symbols (`_tables/follow_coordinator.py`) is one *task* of this
pool, so a session never runs more than `MassiveConfig.max_concurrency` downloads at once,
however many calls it makes: the rest wait their turn in order, and a waiting background read
shows `LOADING` in `m.streams()` meanwhile. A transport's own fan-out within one download
(`massive #1 call-7 rest_0`) has threads of its own and never waits for this pool, so no task
waits for another task that cannot start.

**Threads.** The pool starts no thread until its first task, and then a new one only when no
thread is idle, up to the limit, as `ThreadPoolExecutor` does; the threads are named
`massive #1 read_0` and up. They are daemon threads, unlike `ThreadPoolExecutor`'s, which
Python joins at exit: a script whose last line starts a download of a gigabyte of flat files
must still exit when it ends. A thread enters no execution context itself: each task enters the
session's on its own (`entered`).

**Stop.** `stop` (the session's close, or its collection) drops the tasks still waiting and ends
each thread once its running task returns; it waits for none of them. Every task's table has
been failed or released by then, so a dropped task has no one left to tell. A task submitted
after the stop is refused, and its owner fails its table.

**Errors.** A task is expected to handle its own errors. One that escapes is a defect of the
library: it is logged at ERROR with its traceback, and the thread carries on.

`submit` and `stop` are safe from any thread and never block: they hold the pool's lock only
around a queue put and a thread start.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING, Final

from deephaven_massive._errors import LIBRARY_BUG

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["ReadPool"]

logger = logging.getLogger(__name__)

_STOP: Final = None
"""The queue item that ends one thread."""


class ReadPool:
    """A bounded pool of daemon threads that run the session's downloads; see the module docstring.

    Created with the session's core, which stops it when the session closes or is collected.
    """

    __slots__ = (
        "_idle",
        "_lock",
        "_max_workers",
        "_queue",
        "_stopped",
        "_thread_prefix",
        "_threads",
    )

    def __init__(self, thread_prefix: str, max_workers: int) -> None:
        """Prepare the pool; no thread starts yet.

        Args:
            thread_prefix: The session's thread prefix, such as `"massive #1"`.
            max_workers: The most threads, the session's `max_concurrency`.
        """
        self._thread_prefix = thread_prefix
        self._max_workers = max_workers
        self._queue: queue.SimpleQueue[Callable[[], None] | None] = queue.SimpleQueue()
        # Released by a thread each time it finishes a task, so `submit` starts a thread only
        # when none is idle; as ThreadPoolExecutor counts its idle threads.
        self._idle = threading.Semaphore(0)
        # Guards the thread list and the stop flag.
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._stopped = False

    @property
    def threads(self) -> int:
        """How many threads the pool has started."""
        with self._lock:
            return len(self._threads)

    def submit(self, task: Callable[[], None]) -> bool:
        """Queue a task, starting a thread for it if none is idle and the pool has room.

        Args:
            task: Runs one download; it handles its own errors.

        Returns:
            False, and the task is dropped, when the pool has stopped.
        """
        with self._lock:
            if self._stopped:
                return False
            self._queue.put(task)
            if self._idle.acquire(blocking=False):
                return True
            busy = len(self._threads) >= self._max_workers
            if not busy:
                thread = threading.Thread(
                    target=self._work,
                    name=f"{self._thread_prefix} read_{len(self._threads)}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)  # only once started: a failed start counts none
        if busy:  # logged outside the lock, which is a leaf
            logger.debug(
                "%s read pool: all %d threads are busy; a read waits its turn",
                self._thread_prefix,
                self._max_workers,
            )
        return True

    def stop(self) -> None:
        """Drop the waiting tasks and end every thread once its task returns; idempotent.

        Waits for nothing.
        """
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            count = len(self._threads)
            dropped = 0
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                dropped += item is not None
            for _ in range(count):
                self._queue.put(_STOP)
        logger.debug(
            "%s read pool: stopped, %d threads end, %d waiting tasks dropped",
            self._thread_prefix,
            count,
            dropped,
        )

    def _work(self) -> None:
        """A thread: run tasks in order until the stop item, then end."""
        while True:
            task = self._queue.get()
            if task is _STOP:
                return
            try:
                task()
            except BaseException:  # a task handles its own errors: this is a defect
                logger.exception(
                    "%s read pool: a task failed. %s", self._thread_prefix, LIBRARY_BUG
                )
            del task  # hold nothing while idle
            self._idle.release()
