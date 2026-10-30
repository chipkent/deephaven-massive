"""The library's debug log: the switch that turns it on, and the rules every change follows.

Every module of the library logs through `logging.getLogger(__name__)`, so every library logger
is a child of `deephaven_massive` and a record's logger name says which file wrote it. Nothing
here runs at import: until the log is turned on, WARNING and ERROR records reach `sys.stderr`
through Python's last-resort handler, as any library's do, and lower levels are dropped.

Two ways turn the log on; both add one handler to the `deephaven_massive` logger and set that
logger's level:

- **Environment variables**, read once per process, when the first `MassiveSession` is created
  (`configure_from_environment`). `DEEPHAVEN_MASSIVE_LOG` takes a level name (`DEBUG`, `INFO`,
  `WARN`, `WARNING`, `ERROR`, `CRITICAL`, in any case), and `DEEPHAVEN_MASSIVE_LOG_FILE`, when
  also set, sends the lines to that file instead of stderr. They are read at the first session
  rather than at import because a script that starts `deephaven_server.Server` may import the
  library before the server replaces `sys.stderr`: a handler made then would keep writing to
  the old stream, which Deephaven never sees. A session needs the running server, so by then
  `sys.stderr` is final.
- **`enable_debug_logging(output=LogOutput.STDERR)`**, for a console that is already running,
  where a new environment variable takes effect only after a worker restart.

`disable_debug_logging()` undoes either: it removes and closes the handler and sets the logger's
level back to `NOTSET`, as it was at import.

Each time the log is turned on, by either way, it logs one INFO line describing the process:
the library's version, Python's and whether its GIL is enabled, the version of every dependency
the library declares, the date of the Massive specification snapshot the schemas came from, and
whether `HTTPS_PROXY` and `SSL_CERT_FILE` are set (never their values). Each new session then
logs its own configuration, so the log of a failing run says exactly what ran.

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

**The rules every change follows.** A log from a failing run, with
the issue text, must be enough for an agent to reproduce and fix the bug:

- A decision is a branch on configuration, server data, or input that changes what is
  requested, routed, or returned. Each decision logs at DEBUG with its inputs and outcome.
- A failure logs at ERROR, and a degradation at WARNING. Each names what an agent needs to
  reproduce it: the offending field and value, the dataset, the endpoint or channel, the
  symbol, and the call or stream id. Every layer logs what it knows; repeats are fine. Log too
  much rather than too little.
- Nothing is logged per row or per frame except a failure.
- No log call is passed the API key, the S3 secret, a header, or the authentication frame. A
  wire client cleans what it receives of its own secret (`_massive/net/redaction.py`) before
  anything reads it, so text built from a server's answer is safe to log.
- Log calls use `%` arguments. Every new thread or pool is named after its owner:
  `massive #<session> <role>` for a session's (`SessionCore.thread_prefix`), and
  `massive group #<group> <role>` for a connection group's (`ConnectionGroup.label`). Nothing is
  logged while `registry_lock` or another leaf lock is held.

Tools check what they can: ruff's `G` and `LOG` rules; the logging rules of
`tests/test_layout.py` (no secret name in a logging call, every `threading.Thread` named and
every `ThreadPoolExecutor` given a prefix, no logging call inside a `with` over a leaf lock);
the planted-secret test, `tests/massive/net/test_redaction.py`, which drives every wire client
with distinctive secrets and DEBUG on every logger and finds them in no record or error; and the
thread-name tests of `tests/ingest/test_ws.py` and `tests/tables/test_poll.py`.
Reviewers check the rest: that each new decision and failure logs, with its values. Ask: could
an agent fix this from the log alone?

This module imports the standard library and the wire package's interface (for the snapshot
date), which loads no client and no Deephaven, so `dm.enable_debug_logging` works before a
Deephaven server exists.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from enum import Enum
from importlib.metadata import PackageNotFoundError, requires, version
from typing import Final

from deephaven_massive import _massive

__all__ = [
    "FILE_VARIABLE",
    "FORMAT",
    "LEVEL_VARIABLE",
    "LogOutput",
    "configure_from_environment",
    "disable_debug_logging",
    "enable_debug_logging",
]

LEVEL_VARIABLE: Final = "DEEPHAVEN_MASSIVE_LOG"
"""The environment variable that turns the log on at a level, such as `DEBUG`."""

FILE_VARIABLE: Final = "DEEPHAVEN_MASSIVE_LOG_FILE"
"""The environment variable that sends the log to a file instead of stderr."""

FORMAT: Final = "%(asctime)sZ %(levelname)s [%(threadName)s] %(name)s: %(message)s"
"""The line format; `asctime` is in UTC, hence the `Z`."""

_LEVELS: Final = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
"""The level names `LEVEL_VARIABLE` accepts, in any case, and their levels."""

# What ends the distribution name of a requirement such as "orjson>=3.9; extra == 'orjson'".
_AFTER_NAME: Final = re.compile(r"[\s;<>=!~\[(@]")

logger = logging.getLogger(__name__)
_library_logger = logging.getLogger("deephaven_massive")


class LogOutput(Enum):
    """Where `enable_debug_logging` writes the log, when it is not a file.

    Pass a member, or a file path (`str` or `os.PathLike`), as `output`.

    Example:
        ::

            import deephaven_massive as dm

            dm.enable_debug_logging(dm.LogOutput.STDERR)  # the default
            dm.enable_debug_logging("/tmp/massive.log")  # a file instead
    """

    STDERR = "stderr"
    """`sys.stderr` as it is when the log is turned on, which Deephaven copies into its log,
    the container's log, and the web IDE's log panel."""


class _Switch:
    """What this module has done in the process: the handler it installed and the variables read.

    One instance, `_switch`, under its own leaf lock: only the handler swap happens under it,
    and a replaced handler is closed after it is released.

    Attributes:
        handler: The handler on the `deephaven_massive` logger, or None before the first.
        environment_read: True once the environment variables were read, or need not be.
        overridden: True once `enable_debug_logging` or `disable_debug_logging` has run: an
            environment read still in flight then installs nothing, so the function wins.
    """

    def __init__(self) -> None:
        """Start with nothing installed and the variables unread."""
        self.lock = threading.Lock()
        self.handler: logging.Handler | None = None
        self.environment_read = False
        self.overridden = False


_switch = _Switch()


def enable_debug_logging(output: LogOutput | str | os.PathLike[str] = LogOutput.STDERR) -> None:
    """Send every record of the library, DEBUG and up, to stderr or to a file.

    Call it from a Deephaven console when something goes wrong, run the failing code again, and
    attach the lines to the bug report: they hold what the library decided and why, the
    requests it made with their status and `request_id`, and the values it could not read.
    They never hold the API key or the S3 secret.

    It adds one handler to the `deephaven_massive` logger, sets that logger to DEBUG, and logs
    the environment line at INFO. A second call replaces the handler and closes the old one;
    the environment variables are then no longer read at the first session.

    A file that cannot be opened for appending raises its `OSError` (from `open`), and the
    previous handler, if any, stays.

    Args:
        output: `LogOutput.STDERR` (the default) for `sys.stderr` as it is at the call, or a
            file to append the lines to, in UTF-8, created if missing.

    Raises:
        TypeError: If `output` is neither a `LogOutput` nor a path.

    Example:
        ::

            import deephaven_massive as dm

            dm.enable_debug_logging()  # or dm.enable_debug_logging("/tmp/massive.log")
            m = dm.MassiveSession()
            t = m.stocks.last_trade(["AAPL"]).fetch()  # its requests are now in the log
    """
    if not isinstance(output, LogOutput | str | os.PathLike):
        raise TypeError(
            f"output must be a dm.LogOutput or a file path, not {type(output).__name__}"
        )
    _install(logging.DEBUG, output, environment=False)


def disable_debug_logging() -> None:
    """Turn the library's log off again, whichever switch turned it on and at whatever level.

    Call it once a bug report has its lines, or to quiet a console after
    `enable_debug_logging()` or `DEEPHAVEN_MASSIVE_LOG`. It logs one INFO line saying the log
    is off, then removes and closes the handler that either switch added to the
    `deephaven_massive` logger and sets that logger's level back to `NOTSET`. The library is
    then as it was at import: WARNING and ERROR records reach `sys.stderr` through Python's
    last-resort handler, and lower levels are dropped. A handler a user added to the logger
    themselves stays, but the level is reset to `NOTSET` whoever set it. The environment
    variables count as read afterwards, so a later first session does not turn the log back
    on, and a read of them already under way on another thread installs nothing;
    `enable_debug_logging()` does turn it on again. With the log already off it changes
    nothing.

    Example:
        ::

            import deephaven_massive as dm

            dm.enable_debug_logging()
            # ... run the failing code, save the lines ...
            dm.disable_debug_logging()
    """
    with _switch.lock:
        _switch.environment_read = True
        _switch.overridden = True  # an environment read in flight installs nothing now
        handler = _switch.handler
        if handler is None:
            return
    # Logged before the handler goes, so the log itself says where it ended.
    logger.info("logging deephaven_massive off (from disable_debug_logging())")
    with _switch.lock:
        if _switch.handler is not handler:  # another switch call ran meanwhile; it wins
            return
        _switch.handler = None
        _library_logger.removeHandler(handler)
        _library_logger.setLevel(logging.NOTSET)
    handler.close()


def configure_from_environment() -> None:
    """Apply `DEEPHAVEN_MASSIVE_LOG` and `DEEPHAVEN_MASSIVE_LOG_FILE`, once per process.

    `MassiveSession()` calls it when a session is created; only the first call reads the
    variables, and only when `enable_debug_logging` has not run. An unset or blank
    `DEEPHAVEN_MASSIVE_LOG` changes nothing (`DEEPHAVEN_MASSIVE_LOG_FILE` alone is ignored).
    The variables count as read even when they raise, so a later session does not raise again.

    Raises:
        ValueError: If `DEEPHAVEN_MASSIVE_LOG` is not a level name; the message names the
            variable, its value, and the accepted names.
        OSError: If `DEEPHAVEN_MASSIVE_LOG_FILE` cannot be opened for appending; the message
            names the variable, its value, and the reason.
    """
    with _switch.lock:
        if _switch.environment_read:
            return
        _switch.environment_read = True
    value = os.environ.get(LEVEL_VARIABLE, "").strip()
    if not value:
        return
    level = _LEVELS.get(value.upper())
    if level is None:
        raise ValueError(
            f"{LEVEL_VARIABLE}={value!r} is not a logging level; use one of {', '.join(_LEVELS)}"
        )
    path = os.environ.get(FILE_VARIABLE, "").strip()
    try:
        _install(level, path or LogOutput.STDERR, environment=True)
    except OSError as error:
        raise OSError(
            f"{FILE_VARIABLE}={path!r} cannot be opened for appending: {error.strerror or error}"
        ) from error


def _install(level: int, output: LogOutput | str | os.PathLike[str], *, environment: bool) -> None:
    """Replace the library's handler with a new one, set the logger's level, log the environment.

    Args:
        level: The level of the `deephaven_massive` logger.
        output: `LogOutput.STDERR`, or the file to append to.
        environment: True when the environment variables asked for it: the handler is then
            not installed if `enable_debug_logging` has run meanwhile.

    The `OSError` of opening a file propagates before anything changes.
    """
    handler: logging.Handler = (
        logging.StreamHandler(sys.stderr)
        if isinstance(output, LogOutput)
        else logging.FileHandler(output, encoding="utf-8")
    )
    formatter = logging.Formatter(FORMAT)
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    with _switch.lock:
        previous = _switch.handler
        # The function wins over the variables, even when it ran while they were being read.
        superseded = environment and _switch.overridden
        if not superseded:
            _switch.handler = handler
            _switch.environment_read = True
            _switch.overridden = _switch.overridden or not environment
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
        output.value if isinstance(output, LogOutput) else os.fspath(output),
        "the environment" if environment else "enable_debug_logging()",
    )
    _log_environment()


def _log_environment() -> None:
    """Log, at INFO, what a bug report needs to know about the process.

    The line names the library's version; Python's, with whether the GIL is enabled (it is off
    on a free-threaded build run with `PYTHON_GIL=0`); the version of every dependency the
    library declares (`orjson` installed means it decodes JSON, else the standard library
    does); the date of the Massive specification snapshot the schemas were generated from; and
    whether `HTTPS_PROXY` and `SSL_CERT_FILE` are set, never their values, since a proxy URL may
    hold a password. Nothing is computed when INFO is off for this logger.
    """
    if not logger.isEnabledFor(logging.INFO):
        return
    gil = "on" if getattr(sys, "_is_gil_enabled", lambda: True)() else "off"
    logger.info(
        "deephaven-massive %s, Python %s (GIL %s), %s; Massive spec snapshot of %s; "
        "HTTPS_PROXY %s, SSL_CERT_FILE %s",
        _version("deephaven-massive"),
        sys.version.split()[0],
        gil,
        _dependency_versions(),
        _massive.provenance().fetched,
        "set" if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") else "unset",
        "set" if os.environ.get("SSL_CERT_FILE") else "unset",
    )


def _version(distribution: str) -> str:
    """Return the installed version of a distribution, for the environment line.

    Args:
        distribution: The distribution's name, such as `"pyarrow"`.

    Returns:
        Its version, or `"not installed"`.
    """
    try:
        return version(distribution)
    except PackageNotFoundError:
        return "not installed"


def _dependency_versions() -> str:
    """Return the installed version of every dependency the library declares, optional ones too.

    Returns:
        `"<name> <version>"` per dependency in the order the package metadata lists them, joined
        by `", "`, such as `"boto3 1.40.1, ..., orjson not installed"`.
    """
    names = dict.fromkeys(
        _AFTER_NAME.split(requirement, maxsplit=1)[0]
        for requirement in requires("deephaven-massive") or ()
    )
    return ", ".join(f"{name} {_version(name)}" for name in names)
