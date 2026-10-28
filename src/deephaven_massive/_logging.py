"""The switch that turns on the library's log: `enable_debug_logging` and two environment variables.

Every module of the library logs through `logging.getLogger(__name__)`, so every library logger
is a child of `deephaven_massive` and a record's logger name says which file wrote it. Nothing
here runs at import: until the log is turned on, WARNING and ERROR records reach `sys.stderr`
through Python's last-resort handler, as any library's do, and lower levels are dropped.

Two ways turn the log on; both add one handler to the `deephaven_massive` logger and set that
logger's level:

- **Environment variables**, read once per process, when the first `MassiveSession` is created
  (`configure_from_environment`). `DEEPHAVEN_MASSIVE_LOG` takes a level name (`DEBUG`, `INFO`,
  `WARNING`, `ERROR`, `CRITICAL`, in any case), and `DEEPHAVEN_MASSIVE_LOG_FILE`, when also set,
  sends the lines to that file instead of stderr. They are read at the first session rather
  than at import because a script that starts `deephaven_server.Server` may import the library
  before the server replaces `sys.stderr`: a handler made then would keep writing to the old
  stream, which Deephaven never sees. A session needs the running server, so by then
  `sys.stderr` is final.
- **`enable_debug_logging(path=None)`**, for a console that is already running, where a new
  environment variable takes effect only after a worker restart.

The handler writes one line per record, with the time in UTC:
`2026-10-28 14:03:07,112Z DEBUG [massive #1 call-7 rest_0] deephaven_massive._massive.rest.client:
GET ...`. The thread name says which session and which call or stream a line belongs to. On
stderr, Deephaven copies the lines into its own log, the container's log, and the web IDE's log
panel. Turning the log on again replaces the handler and closes the old one. The logger keeps
`propagate` on, so pytest's `caplog` and any handler a user put on the root logger still see
every record; Deephaven installs no root handler, so nothing is printed twice there.

Setting the level alone (`logging.getLogger("deephaven_massive").setLevel(logging.DEBUG)`) shows
nothing below WARNING, because the last-resort handler drops lower levels; that is what this
module's handler is for. A user who configures logging already only sets the level.

This module imports only the standard library and no other part of the library, so
`dm.enable_debug_logging` works before a Deephaven server exists.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "FILE_VARIABLE",
    "FORMAT",
    "LEVEL_VARIABLE",
    "configure_from_environment",
    "enable_debug_logging",
]

LEVEL_VARIABLE: Final = "DEEPHAVEN_MASSIVE_LOG"
"""The environment variable that turns the log on at a level, such as `DEBUG`."""

FILE_VARIABLE: Final = "DEEPHAVEN_MASSIVE_LOG_FILE"
"""The environment variable that sends the log to a file instead of stderr."""

FORMAT: Final = "%(asctime)sZ %(levelname)s [%(threadName)s] %(name)s: %(message)s"
"""The line format; `asctime` is in UTC, hence the `Z`."""

_LEVELS: Final = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
"""The level names `LEVEL_VARIABLE` accepts, in any case."""

logger = logging.getLogger(__name__)
_library_logger = logging.getLogger("deephaven_massive")


class _Switch:
    """What this module has done in the process: the handler it installed and the variables read.

    One instance, `_switch`, under its own leaf lock: only the handler swap happens under it,
    and a replaced handler is closed after it is released.

    Attributes:
        handler: The handler on the `deephaven_massive` logger, or None before the first.
        environment_read: True once the environment variables were read, or need not be.
    """

    def __init__(self) -> None:
        """Start with nothing installed and the variables unread."""
        self.lock = threading.Lock()
        self.handler: logging.Handler | None = None
        self.environment_read = False


_switch = _Switch()


def enable_debug_logging(path: str | Path | None = None) -> None:
    """Send every record of the library, DEBUG and up, to stderr or to a file.

    Call it from a Deephaven console when something goes wrong, run the failing code again, and
    attach the lines to the bug report: they hold what the library decided and why, the
    requests it made with their status and `request_id`, and the values it could not read.
    They never hold the API key or the S3 secret.

    It adds one handler to the `deephaven_massive` logger and sets that logger to DEBUG. A
    second call replaces the handler and closes the old one; the environment variables are
    then no longer read at the first session. Lines go to `sys.stderr` as it is at the call,
    which Deephaven copies into its log and the web IDE's log panel.

    A `path` that cannot be opened for appending raises its `OSError` (from `open`), and the
    previous handler, if any, stays.

    Args:
        path: A file to append the lines to, in UTF-8, created if missing; None (the default)
            writes them to stderr.

    Example:
        ::

            import deephaven_massive as dm

            dm.enable_debug_logging()  # or dm.enable_debug_logging("/tmp/massive.log")
            m = dm.MassiveSession()
            t = m.stocks.trades(["AAPL"]).fetch()  # its requests are now in the log
    """
    _install(logging.DEBUG, path, environment=False)


def configure_from_environment() -> None:
    """Apply `DEEPHAVEN_MASSIVE_LOG` and `DEEPHAVEN_MASSIVE_LOG_FILE`, once per process.

    `MassiveSession()` calls it when a session is created; only the first call reads the
    variables, and only when `enable_debug_logging` has not run. An unset or blank
    `DEEPHAVEN_MASSIVE_LOG` changes nothing (`DEEPHAVEN_MASSIVE_LOG_FILE` alone is ignored).

    A `DEEPHAVEN_MASSIVE_LOG_FILE` that cannot be opened for appending raises its `OSError`
    (from `open`).

    Raises:
        ValueError: If `DEEPHAVEN_MASSIVE_LOG` is not a level name; the message names the
            variable, its value, and the accepted names. The variables count as read either
            way, so a later session does not raise again.
    """
    with _switch.lock:
        if _switch.environment_read:
            return
        _switch.environment_read = True
    value = os.environ.get(LEVEL_VARIABLE, "").strip()
    if not value:
        return
    name = value.upper()
    if name not in _LEVELS:
        raise ValueError(
            f"{LEVEL_VARIABLE}={value!r} is not a logging level; use one of {', '.join(_LEVELS)}"
        )
    path = os.environ.get(FILE_VARIABLE, "").strip() or None
    _install(logging.getLevelNamesMapping()[name], path, environment=True)


def _install(level: int, path: str | Path | None, *, environment: bool) -> None:
    """Replace the library's handler with a new one and set the library logger's level.

    Args:
        level: The level of the `deephaven_massive` logger.
        path: The file to append to, or None for stderr.
        environment: True when the environment variables asked for it: the handler is then
            not installed if `enable_debug_logging` has run meanwhile.

    The `OSError` of opening `path` propagates before anything changes.
    """
    handler: logging.Handler = (
        logging.StreamHandler(sys.stderr)
        if path is None
        else logging.FileHandler(path, encoding="utf-8")
    )
    formatter = logging.Formatter(FORMAT)
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    with _switch.lock:
        previous = _switch.handler
        # The function wins over the variables, even when it ran while they were being read.
        superseded = environment and previous is not None
        if not superseded:
            _switch.handler = handler
            _switch.environment_read = True
            _library_logger.addHandler(handler)
            if previous is not None:
                _library_logger.removeHandler(previous)
            _library_logger.setLevel(level)
    if superseded:
        handler.close()
        return
    if previous is not None:
        previous.close()

    logger.info(
        "logging deephaven_massive at %s to %s (from %s)",
        logging.getLevelName(level),
        "stderr" if path is None else os.fspath(path),
        "the environment" if environment else "enable_debug_logging()",
    )
