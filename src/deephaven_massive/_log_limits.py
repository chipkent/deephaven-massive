"""Log records that are rate-limited: once per process, or once per value up to a limit.

Some facts are worth logging once rather than on every event, and some bad values can arrive
without end. The two helpers here keep both kinds of record from flooding the log:

- `log_once` logs a fact the first time its key is seen in the process, and never again: the
  unit the FMV magnitude rule detected, the WebSocket host chosen for a Business key, a futures
  one-second bar whose end from Massive is not its start plus one second.
- `BoundedLog` logs a record at its level the first time each value is seen, for at most a
  fixed number of distinct values, so a stream of bad values that may never end (a watchlist
  row typed wrong, a symbol spelling Massive keeps sending) keeps both the log and the memory
  it uses bounded.

Both are thread-safe. Each holds a leaf lock only while it updates the keys or values it has
seen, never while it logs. The module imports only the standard library, so every layer of the
library may use it; it is private, and nothing in it is exported from `deephaven_massive`.
"""

from __future__ import annotations

import logging
import threading

__all__ = ["BoundedLog", "log_once"]


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
