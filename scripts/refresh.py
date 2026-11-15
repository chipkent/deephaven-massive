"""Run the spec refresh, which keeps the wire schemas in step with Massive's published API.

The refresh lives in the library (`deephaven_massive._massive.refresh`), and importing the
library imports Deephaven, which needs a running JVM. So this launcher starts an embedded
Deephaven server first, then runs the refresh's command line with the arguments it was given.
Run it from a source checkout, from the repository root:

    uv run python scripts/refresh.py                            # regenerate spec/*.json
    uv run python scripts/refresh.py --fetch                    # replace the vendored snapshot
    uv run python scripts/refresh.py --check --report drift.md  # report drift; changes nothing

`docs/dev/spec-refresh.md` says when to run each. The server prints its own startup lines to
standard output, which is why `--check` writes its report to the file `--report` names.

Exit status: the refresh's own (`deephaven_massive._massive.refresh.__main__`), or 1 when
anything else fails, after the traceback is printed. The process leaves through `os._exit`
once its output is flushed, because a normal interpreter shutdown after the embedded JVM has
run can crash on macOS.
"""

from __future__ import annotations

import os
import runpy
import sys
import traceback

from deephaven_server import Server

_REFRESH = "deephaven_massive._massive.refresh"
"""The refresh package, run as `python -m` would run it."""


def main() -> int:
    """Start the server, then run the refresh with this process's command line.

    Returns:
        The refresh's exit status.
    """
    # Port 0 asks the OS for an ephemeral port, so the command never clashes with a server.
    Server(port=0, jvm_args=["-Xmx1g"]).start()
    try:
        runpy.run_module(_REFRESH, run_name="__main__", alter_sys=True)
    except SystemExit as stop:
        if stop.code is None:
            return 0
        return stop.code if isinstance(stop.code, int) else 1
    return 0


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
