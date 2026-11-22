"""The library's debug log: the switch that turns it on, and the rules every change follows.

Every module of the library logs through `logging.getLogger(__name__)`, so every library logger
is a child of `deephaven_massive` and a record's logger name says which file wrote it. Nothing
here runs at import: until the log is turned on, WARNING and ERROR records reach `sys.stderr`
through Python's last-resort handler, as any library's do, and lower levels are dropped.

Two ways turn the log on; both add one handler to the `deephaven_massive` logger and set that
logger's level:

- **Environment variables**, read once per process, when the first `MassiveSession` is created
  (`configure_from_environment`). `DEEPHAVEN_MASSIVE_LOG` takes a level name (`DEBUG`, `INFO`,
  `WARN`, `WARNING`, `ERROR`, `CRITICAL`, in any case), and `DEEPHAVEN_MASSIVE_LOG_FILE` sends
  the lines to that file instead of stderr; the file alone turns the log on at DEBUG. They are
  read at the first session rather than at import because a script that starts
  `deephaven_server.Server` may import the library before the server replaces `sys.stderr`: a
  handler made then would keep writing to the old stream, which Deephaven never sees. A session
  needs the running server, so by then `sys.stderr` is final.
- **`enable_debug_logging(output=LogOutput.STDERR)`**, for a console that is already running,
  where a new environment variable takes effect only after a server restart.

`disable_debug_logging()` undoes either: it removes and closes the handler and sets the logger's
level back to `NOTSET`, as it was at import.

Each time the log is turned on, by either way, it logs one INFO line describing the process:
the library's version and the git commit it was installed from, Python's and whether its GIL is
enabled, the operating system and the local time's offset from UTC, the version of every
dependency the library declares, the date of the Massive specification snapshot the schemas
came from, and whether `HTTPS_PROXY` and `SSL_CERT_FILE` are set (never their values). Each
new session then logs its own configuration, so the log of a failing run says exactly what ran.

The handler writes one line per record, with the time in UTC:
`2026-10-28 14:03:07,112Z DEBUG [massive #1 call-7 rest_0] deephaven_massive._massive.rest.client:
GET ...`. The thread name says which session and which call or stream a line belongs to. On
stderr, Deephaven copies the lines into its own log, the container's log, and the Deephaven IDE's
log panel. Turning the log on again replaces the handler and closes the old one, and the library
never rotates the file. The logger keeps `propagate` on, so pytest's `caplog` and any handler a
user put on the root logger still see every record; Deephaven installs no root handler, so
nothing is printed twice there.

Setting the level alone (`logging.getLogger("deephaven_massive").setLevel(logging.DEBUG)`) shows
nothing below WARNING, because the last-resort handler drops lower levels; that is what this
module's handler is for. A user who configures logging already only sets the level.

**The rules every change follows.** A log from a failing run, with
the issue text, must be enough for an agent to reproduce and fix the bug:

- A decision is a branch on configuration, server data, or input that changes what is
  requested, routed, or returned. Each decision logs at DEBUG with its inputs and outcome.
- A level says what the user must do, since users see WARNING and above. ERROR is only for a
  defect that broke a result, and its text says whose it is: a defect of the library ends with
  `_errors.LIBRARY_BUG` (how to report it), and a defect in the user's code or input names what
  to fix (`_errors.user_defect`). A defect logs at ERROR even when it is also raised. WARNING
  is for live data that is stale or wrong, or something the user must act on, once per
  episode, with the recovery at INFO. INFO is for decisions and progress. Everything else is
  DEBUG, including an expected error that is also raised to the caller. Expected conditions
  (refusals, plan and rate limits, network failures, the user's own `close()`) never log at
  ERROR. A `MassiveError` carries its `is_bug`; `_log_limits.log_failure` logs a failure that
  fails live tables once, at the level its `is_bug` gives.
- Each record names what an agent needs to reproduce it: the offending field and value, the
  dataset, the endpoint or channel, the symbol, and the call or stream id. Every layer logs
  what it knows at DEBUG; repeats there are fine. Log too much rather than too little.
- The common path of each WebSocket frame, row decode, and flush makes no log call at any
  level; its exceptional branches log through the bounded helpers of `_log_limits`, never on
  every event.
- No log call is passed the API key, the S3 secret, a header, or the authentication frame. A
  wire client cleans what it receives of its own secret (`_massive/net/redaction.py`) before
  anything reads it, so text built from a server's answer is safe to log. A configured URL
  shows `***@` in place of a user and password in it, in every log line and message
  (`without_userinfo` in `_massive/net/redaction.py`, `MassiveConfig.__repr__`).
- The log records what explains a slow or failed run: each rate-limit pause and permit wait,
  why each connection closed, and how long it was up.
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
an agent fix this from the log alone? How a WARNING or ERROR record's text reads is the
`writing-error-messages` skill's subject.

This module imports the standard library and the wire package's interface (for the snapshot
date), which loads no client and no Deephaven, so `dm.enable_debug_logging` works before a
Deephaven server exists.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import sys
import threading
import time
from datetime import datetime
from enum import Enum
from importlib.metadata import PackageNotFoundError, distribution, requires, version
from pathlib import Path
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
"""The environment variable that sends the log to a file instead of stderr (at DEBUG, alone)."""

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
    the container's log, and the Deephaven IDE's log panel."""


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

    It sets the `deephaven_massive` logger to DEBUG and logs one INFO line describing the
    environment. A second call replaces the first one's output. Once you call it, the
    environment variable `DEEPHAVEN_MASSIVE_LOG` no longer applies.

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
    _install(logging.DEBUG, output, environment=False, source="enable_debug_logging()")


def disable_debug_logging() -> None:
    """Turn the library's log off again, whichever switch turned it on and at whatever level.

    Call it once a bug report has its lines, or to quiet a console after
    `enable_debug_logging()` or `DEEPHAVEN_MASSIVE_LOG`. It logs one INFO line saying the log
    is off, removes the output either switch added, and sets the `deephaven_massive` logger's
    level back to `NOTSET`. WARNING and ERROR records then reach stderr through Python's
    last-resort handler, as they do at import, and lower levels are dropped. A handler you
    added yourself stays, but the logger's level goes back to `NOTSET` even if you set it. A
    later first session doesn't turn the log back on from the environment variables;
    `enable_debug_logging()` does. With the log already off, it changes nothing.

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


def _environment_value(name: str) -> str:
    """Return a log variable's value exactly as set, or an empty string when unset or blank.

    The same rule as `_config.environment_text`, which this module cannot import: loading the
    configuration would load modules that need Deephaven, and the log must switch on without it.

    Args:
        name: The variable, `DEEPHAVEN_MASSIVE_LOG` or `DEEPHAVEN_MASSIVE_LOG_FILE`.

    Returns:
        The value, or `""` when the variable is unset or blank.

    Raises:
        ValueError: If the value has leading or trailing whitespace; the message names the
            variable and the value.
    """
    value = os.environ.get(name, "")
    if not value.strip():
        return ""
    if value != value.strip():
        raise ValueError(
            f"{name}={value!r} has leading or trailing whitespace. Set {name} to {value.strip()!r}."
        )
    return value


def configure_from_environment() -> None:
    """Apply `DEEPHAVEN_MASSIVE_LOG` and `DEEPHAVEN_MASSIVE_LOG_FILE`, once per process.

    `MassiveSession()` calls it when a session is created; only the first call reads the
    variables, and only when `enable_debug_logging` has not run. With both unset or blank it
    changes nothing. `DEEPHAVEN_MASSIVE_LOG_FILE` alone turns the log on at DEBUG, since
    naming a file asks for a log in it, and the line that says the log is on names the default.
    The variables count as read even when they raise, so a later session does not raise again.

    Raises:
        ValueError: If `DEEPHAVEN_MASSIVE_LOG` is not a level name (the message names the
            variable, its value, and the accepted names), or either variable has leading or
            trailing whitespace, which is never trimmed.
        OSError: If `DEEPHAVEN_MASSIVE_LOG_FILE` cannot be opened for appending; the message
            names the variable, its value, and the reason.
    """
    with _switch.lock:
        if _switch.environment_read:
            return
        _switch.environment_read = True
    value = _environment_value(LEVEL_VARIABLE)
    path = _environment_value(FILE_VARIABLE)
    if not value and not path:
        return
    level = _LEVELS.get(value.upper()) if value else logging.DEBUG
    if level is None:
        *others, last = _LEVELS
        raise ValueError(
            f"{LEVEL_VARIABLE}={value!r} is not a logging level. Set it to "
            f"{', '.join(others)}, or {last}."
        )
    source = "the environment" if value else f"the environment, with {LEVEL_VARIABLE} unset"
    try:
        _install(level, path or LogOutput.STDERR, environment=True, source=source)
    except OSError as error:
        raise OSError(
            f"{FILE_VARIABLE}={path!r} cannot be opened for appending: "
            f"{error.strerror or error}. Set it to a file this process can write."
        ) from error


def _install(
    level: int, output: LogOutput | str | os.PathLike[str], *, environment: bool, source: str
) -> None:
    """Replace the library's handler with a new one, set the logger's level, log the environment.

    The INFO line that says the log is on names the level, where the lines go (`stderr`, or the
    file's absolute path with every symbolic link resolved, so a relative path in a variable
    still says which file), and `source`.

    Args:
        level: The level of the `deephaven_massive` logger.
        output: `LogOutput.STDERR`, or the file to append to.
        environment: True when the environment variables asked for it: the handler is then
            not installed if `enable_debug_logging` has run meanwhile.
        source: What turned the log on, for the line, such as `"enable_debug_logging()"`.

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
        output.value if isinstance(output, LogOutput) else Path(output).resolve(),
        source,
    )
    _log_environment()


def _log_environment() -> None:
    """Log, at INFO, what a bug report needs to know about the process.

    The line names the library's version and the git commit it was installed from (`_commit`);
    Python's, with whether the GIL is enabled (it is off on a free-threaded build run with
    `PYTHON_GIL=0`); the operating system and machine (`platform.platform()`); the local
    time's offset from UTC and its zone name, which say how the log's UTC times map to the
    clock of the machine that wrote it; the version of every dependency the library declares
    (`orjson` installed means it decodes JSON, else the standard library does); the date of
    the Massive specification snapshot the schemas were generated from; and whether
    `HTTPS_PROXY` and `SSL_CERT_FILE` are set, never their values, since a proxy URL may hold a
    password. Nothing is computed when INFO is off for this logger.
    """
    if not logger.isEnabledFor(logging.INFO):
        return
    gil = "on" if getattr(sys, "_is_gil_enabled", lambda: True)() else "off"
    local = datetime.now().astimezone()
    logger.info(
        "deephaven-massive %s (git commit %s), Python %s (GIL %s), %s, local time UTC%s (%s); "
        "%s; Massive spec snapshot of %s; HTTPS_PROXY %s, SSL_CERT_FILE %s",
        _version("deephaven-massive"),
        _commit(),
        sys.version.split()[0],
        gil,
        platform.platform(),
        local.strftime("%z"),
        local.tzname(),
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


def _commit() -> str:
    """Return the git commit the library was installed from, for the environment line.

    An installer given a version-control URL (`git+https://...@<branch>`) records the commit it
    checked out in the distribution's `direct_url.json` (PEP 610) as `vcs_info.commit_id`. A
    release from an index, or an install from a local directory, records none.

    Returns:
        The commit id, or `"not recorded"` when the distribution, the file, or the field is
        missing, or the file is not the JSON object PEP 610 describes.
    """
    try:
        text = distribution("deephaven-massive").read_text("direct_url.json")
        commit = json.loads(text or "{}").get("vcs_info", {}).get("commit_id")
    except (PackageNotFoundError, ValueError, AttributeError):
        commit = None
    return commit if isinstance(commit, str) and commit else "not recorded"


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
