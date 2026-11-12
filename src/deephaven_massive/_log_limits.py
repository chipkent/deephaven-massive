"""Log records kept bounded: once per process, per value up to a limit, per failure, or by length.

Some facts are worth logging once rather than on every event, some bad values can arrive
without end, and some lists can hold thousands of items. The helpers here keep these records
from flooding the log:

- `log_once` logs a fact the first time its key is seen in the process, and never again: the
  unit the FMV magnitude rule detected, the WebSocket host chosen for a Business key, a futures
  one-second bar whose end from Massive is not its start plus one second.
- `BoundedLog` logs a record at its level the first time each value is seen, for at most a
  fixed number of distinct values, so a stream of bad values that may never end (a watchlist
  row typed wrong, a symbol spelling Massive keeps sending) keeps both the log and the memory
  it uses bounded.
- `log_failure` logs a failure that fails a live table, once per failure object, at the level
  its `is_bug` gives (`_errors.is_bug_of`): one connection failure that fails a hundred streams
  is one record, not a hundred.
- `bounded_list` renders a list of any length as its count and its first `LIST_LIMIT` items,
  for a record that names symbols, topics, or stream ids: a stream of 5,000 symbols logs one
  short line, not 5,000 names.

All are thread-safe. Each holds a leaf lock only while it updates the keys, values, or failures
it has seen, never while it logs. The module imports only the standard library and `_errors`,
so every layer above the wire may use it; it is private, and nothing in it is exported from
`deephaven_massive`.
"""

from __future__ import annotations

import logging
import threading
import weakref
from typing import TYPE_CHECKING, Final

from deephaven_massive._errors import MassiveError, failure_text, is_bug_of

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = [
    "LIST_LIMIT",
    "BoundedLog",
    "bounded_list",
    "log_failure",
    "log_once",
    "mark_failure_logged",
]

LIST_LIMIT: Final = 20
"""How many items `bounded_list` shows before it only counts the rest."""


_logged_lock = threading.Lock()
_logged: set[str] = set()


def log_once(
    log: logging.Logger, key: str, message: str, *args: object, level: int = logging.INFO
) -> None:
    """Log a message the first time `key` is seen in the process, and never again.

    For facts worth stating once rather than per event: the JSON codec chosen, the unit the
    FMV magnitude rule detected, the rate-limit advice. Thread-safe: concurrent first calls
    log exactly once.

    Args:
        log: The logger to write to.
        key: Identifies the fact; later calls with the same key do nothing.
        message: The `%`-style message.
        *args: The message's arguments.
        level: The logging level; INFO by default.
    """
    with _logged_lock:
        if key in _logged:
            return
        _logged.add(key)
    log.log(level, message, *args)


def bounded_list(items: Iterable[object], limit: int = LIST_LIMIT) -> str:
    """Render a list for a log record: its count and its first `limit` items.

    Args:
        items: The items, in the order to show them; each is shown by `str`.
        limit: How many items to show; `LIST_LIMIT` (20) by default.

    Returns:
        `"<count> [<item>, <item>, ...]"`, with `", +<N> more"` before the bracket when items
        are left out: `"3 [AAPL, MSFT, NVDA]"`, `"0 []"`, or `"25 [A, B, ..., T, +5 more]"`.

    Example:
        ::

            logger.debug("subscribed %s", bounded_list(topics))
    """
    listed = list(items)
    shown = [str(item) for item in listed[:limit]]
    if len(listed) > limit:
        shown.append(f"+{len(listed) - limit} more")
    return f"{len(listed)} [{', '.join(shown)}]"


class BoundedLog:
    """Log a record the first time each value is seen, for at most `limit` distinct values.

    For a stream of bad values that may never end (a watchlist row typed wrong, a symbol
    spelling Massive keeps sending): each value is reported once, at the level the owner gives,
    and once `limit` values have been reported, one last record at the same level says that
    further values are not logged, so the set it remembers and the log stay bounded. Created by
    its owner (a module, or one call of `canonicalize_symbols`) and shared by every thread that
    reports: one leaf lock guards the set and is never held while logging.

    Example:
        ::

            import logging

            from deephaven_massive._log_limits import BoundedLog

            bad = BoundedLog(logging.getLogger("example"), limit=10, level=logging.WARNING)
            bad.log("BRK/B", "%r is not a ticker; its rows are null", "BRK/B")
    """

    __slots__ = ("_level", "_limit", "_lock", "_log", "_seen")

    def __init__(self, log: logging.Logger, *, limit: int, level: int) -> None:
        """Start with no value reported.

        Args:
            log: The logger to write to.
            limit: The most distinct values reported; a positive count.
            level: The level of every record, such as `logging.WARNING`.
        """
        self._log = log
        self._limit = limit
        self._level = level
        self._lock = threading.Lock()
        self._seen: set[str] = set()

    def log(self, value: str, message: str, *args: object) -> None:
        """Log `message` at the owner's level if `value` is new and the limit is not reached.

        Args:
            value: Identifies what is reported; a value already seen logs nothing.
            message: The `%`-style message.
            *args: The message's arguments.
        """
        with self._lock:
            if value in self._seen or len(self._seen) > self._limit:
                return
            self._seen.add(value)
            last = len(self._seen) > self._limit
        if last:
            self._log.log(
                self._level, "%d values were reported; further ones are not logged", self._limit
            )
        else:
            self._log.log(self._level, message, *args)


_failures_lock = threading.Lock()
_failures_logged: weakref.WeakSet[MassiveError] = weakref.WeakSet()


def mark_failure_logged(error: BaseException) -> bool:
    """Record that a failure is being logged, so `log_failure` logs it only at DEBUG from now on.

    For a caller that logs a failure itself, with its trace, before the failure reaches the
    tables it fails (`logger.exception` in an `except` block).

    Args:
        error: The failure; only a `MassiveError` is remembered.

    Returns:
        True the first time a `MassiveError` is marked, and always for any other exception;
        False when the failure had been logged already.
    """
    if not isinstance(error, MassiveError):
        return True
    with _failures_lock:
        first = error not in _failures_logged
        _failures_logged.add(error)
    return first


def log_failure(
    log: logging.Logger,
    error: BaseException,
    message: str,
    *args: object,
    expected: int = logging.WARNING,
    exc_info: BaseException | None = None,
) -> bool:
    """Log a failure that fails a live table, once per failure, at the level its `is_bug` gives.

    A bug (`MassiveError.is_bug`, and any exception that is not a `MassiveError`) logs at
    ERROR, a library bug with how to report it (`_errors.failure_text`); any other failure
    logs at `expected`. One `MassiveError` object
    is logged once: the connection that fails a hundred streams with one error logs one
    record, and every later call with the same object logs at DEBUG instead. Thread-safe; the
    set of failures logged holds them weakly, so it never keeps one alive.

    Args:
        log: The logger to write to.
        error: The failure.
        message: The `%`-style message; its last `%s` receives the failure's text.
        *args: The message's other arguments, before the failure's text.
        expected: The level of an expected failure; WARNING by default, DEBUG for one the
            caller also raises.
        exc_info: The exception whose traceback the record carries, or None for none.

    Returns:
        True when this call logged the failure at its level; False when the same failure had
        been logged already, and this call logged at DEBUG.
    """
    first = mark_failure_logged(error)
    level = expected if not is_bug_of(error) else logging.ERROR
    log.log(
        level if first else logging.DEBUG,
        message,
        *args,
        failure_text(error),
        exc_info=exc_info if first else None,
        stacklevel=2,  # the record names the caller's file and line, not this one's
    )
    return first
