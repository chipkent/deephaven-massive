"""The live tests' own debug log: a DEBUG log file for every pytest run that selects one.

A live run started straight from pytest (`uv run pytest -m "integration or integration_smoke"`)
would otherwise leave no library log behind, so a failure seen only on Massive's real servers
could not be diagnosed afterwards. When a run selects any live test, `tests/conftest.py` calls
`turn_on` once collection has finished, before any test opens a session: it creates a new
folder, `live-debug-logs/<UTC time>-<pid>/`, which git ignores, and points
`DEEPHAVEN_MASSIVE_LOG_FILE` at `debug.log` inside it. The library reads that variable when the
run's first `MassiveSession` is created, and logs at DEBUG to the file (unless
`DEEPHAVEN_MASSIVE_LOG` names another level).

A run whose environment already sets `DEEPHAVEN_MASSIVE_LOG_FILE` keeps its own file: the
live-run program (`live_run`) sets it for each child, so its logs stay in the attempt's folder.

The library's own rules keep the secrets out of every log line (`deephaven_massive._debug_log`).
At the end of the run `summary_lines` checks that anyway: it scans the folder for the API key
and the S3 pair (`secret_scrub.scan`), and the terminal summary names the log's absolute path,
or warns not to send a log in which a secret was found. A run that opened no session, such as
one whose live tests all skipped without a key, wrote no log: its empty folder is removed then.

`turn_on` creates the folder and sets the variable, and `summary_lines` removes the folder when
it stayed empty; nothing else here changes anything.
"""

from __future__ import annotations

import contextlib
import os
from datetime import UTC
from typing import TYPE_CHECKING, Final

from tests._live.live_run.secret_scrub import SecretFilter, scan

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping
    from datetime import datetime
    from pathlib import Path

FILE_VARIABLE: Final = "DEEPHAVEN_MASSIVE_LOG_FILE"
"""The variable that sends the library's log to a file, and turns it on at DEBUG."""

FOLDER: Final = "live-debug-logs"
"""The folder in the repository root that holds one folder per run; `.gitignore` names it."""


def turn_on(environ: MutableMapping[str, str], *, root: Path, now: datetime) -> Path | None:
    """Point the library's log at a new per-run file, unless the environment names one.

    Args:
        environ: The process's environment; `DEEPHAVEN_MASSIVE_LOG_FILE` is set in it.
        root: The repository root, where `live-debug-logs/` is.
        now: The current time, aware; it names the run's folder, in UTC.

    Returns:
        The new log's absolute path, or None when `DEEPHAVEN_MASSIVE_LOG_FILE` was already set
        (even blank, which the library then refuses as it always does), which is left as it is.
    """
    if FILE_VARIABLE in environ:
        return None
    stamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = (root / FOLDER / f"{stamp}-{os.getpid()}").resolve()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "debug.log"
    environ[FILE_VARIABLE] = str(path)
    return path


def summary_lines(path: Path, environ: Mapping[str, str]) -> list[str]:
    """Describe the run's debug log for the terminal summary, after a scan for the secrets.

    Args:
        path: The log `turn_on` chose.
        environ: The environment, whose secrets (`secret_scrub.SECRET_VARIABLES`) are looked
            for in every file of the log's folder.

    Returns:
        The log's absolute path and what to do with it; when a secret was found, a warning
        not to send it, and each place it was found. When no log was written, a line saying
        so; the log's folder is then removed if it holds nothing else.
    """
    if not path.exists():
        with contextlib.suppress(OSError):  # missing, or holding another file: it stays
            path.parent.rmdir()
        return [f"debug log: none was written at {path} (no session was opened)"]
    finds = scan(path.parent, SecretFilter.from_environment(environ))
    if finds:
        return [
            f"debug log: {path}",
            "a secret was found in it: do not send it; send the owner these places instead:",
            *(f"  {find}" for find in finds),
        ]
    return [f"debug log: {path}", "send this file with a report of any failure above"]
