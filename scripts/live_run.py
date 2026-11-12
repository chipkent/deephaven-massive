"""Run every live check a Massive key allows, for days, and record everything for diagnosis.

The owner's tester runs this with their own Massive keys during the user test; it records
Massive's raw replies and the library's results, so anything wrong can be diagnosed afterwards
without another live run. Run it from a source checkout, from the repository root:

    uv run python scripts/live_run.py check
    uv run python scripts/live_run.py run --out ~/massive-live [--any-time] [--redo GROUP,...]
    uv run python scripts/live_run.py status --out ~/massive-live
    uv run python scripts/live_run.py pack --out ~/massive-live
    uv run python scripts/live_run.py upgrade-check stocks --out ~/massive-live

The secrets come only from the environment (`MASSIVE_API_KEY`, and optionally
`MASSIVE_DELAYED_API_KEY` and the S3 pair `MASSIVE_S3_ACCESS_KEY_ID` and
`MASSIVE_S3_SECRET_ACCESS_KEY`) and are never written: every printed line and written file is
cleaned of them, and `pack` refuses to zip a folder in which its scan finds one.

All the logic is in `tests/_live/live_run/` (`cli`, `commands`, and the modules they name),
where the unit tests run it against the fake servers; this script only puts the repository
root on the import path, checks for Java (`server.java_problem`, so a missing Java gets the
`check` report's line instead of a failed server start), starts the embedded Deephaven server,
sized by `server.jvm_args`, before it imports the command line (which imports the library, and
so Deephaven, which needs a running JVM), and leaves through `os._exit`, since a normal
interpreter shutdown after the JVM has run can crash.

Exit status: 0 on success, 1 when a check, a pack, or an upgrade check failed, 2 when a
command was refused (a folder in use, another commit, a rejected key), 130 after Control-C.
"""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from pathlib import Path

from deephaven_server import Server

ROOT = Path(__file__).resolve().parent.parent
"""The repository root: every child runs there, and `tests._live` is imported from it."""


def main() -> int:
    """Start the server, then run the command named on the command line.

    Returns:
        Its exit status, or 1 when Java is missing or older than 17.
    """
    sys.path.insert(0, str(ROOT))
    # `tests._live` is importable only once the root is on the path, so it is imported by name.
    server = importlib.import_module("tests._live.live_run.server")
    problem = server.java_problem()
    if problem is not None:
        sys.stdout.write(problem + "\n")
        return 1
    # Port 0 asks the OS for a free port, so the server never clashes with another one.
    Server(port=0, jvm_args=server.jvm_args(sys.argv[1:])).start()
    cli = importlib.import_module("tests._live.live_run.cli")
    status: int = cli.main(sys.argv[1:], root=ROOT, script=Path(__file__).resolve())
    return status


if __name__ == "__main__":
    # A process that has started a Deephaven server must skip the interpreter's shutdown,
    # whatever happens, so every way out ends in os._exit.
    code = 1
    try:
        code = main()
    except BaseException:
        traceback.print_exc()  # the finally below exits before the interpreter could print it
        raise
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
