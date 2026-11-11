"""Check the tester's setup, run the guide's examples in one go, or bundle a bug report.

`docs/tester-guide.md` uses these three commands. Run them from the repository root, in the
terminal that holds `MASSIVE_API_KEY`:

    uv run python scripts/tester.py check         # both setups and the key; saves setup-check.txt
    uv run python scripts/tester.py examples      # writes ~/massive-test/examples-output/
    uv run python scripts/tester.py bug-report    # writes ~/massive-test/bug-report-<time>.zip

Each takes `--folder DIR` for a folder other than `~/massive-test`; `check` also takes
`--console-python PATH` or `--docker-image NAME` for the console install. The logic is in
`tests/_live/tester/` (`cli` and the modules it names), where the unit tests run it against
the fake servers; this script only puts the repository root on the import path, checks for
Java (so a missing Java gets the `check` report's line instead of a failed server start),
starts the embedded Deephaven server, sized by `tests/_live/tester/server.py`, before it
imports the command line (which imports the library, and so Deephaven, which needs a running
JVM), and leaves through `os._exit`, since a normal interpreter shutdown after the JVM has run
can crash.

Exit status: 0 on success; 1 when a check failed, an example failed, Java is missing, or a
secret blocked the bug report; 2 when `MASSIVE_API_KEY` is not set.
"""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from pathlib import Path

from deephaven_server import Server

ROOT = Path(__file__).resolve().parent.parent
"""The repository root: the examples are read there, and `tests._live` is imported from it."""


def main() -> int:
    """Start the server, then run the command named on the command line.

    Returns:
        Its exit status, or 1 when Java is missing or older than 17.
    """
    sys.path.insert(0, str(ROOT))
    # `tests._live` is importable only once the root is on the path, so it is imported by name.
    live_server = importlib.import_module("tests._live.live_run.server")
    problem = live_server.java_problem()
    if problem is not None:
        sys.stdout.write(problem + "\n")
        return 1
    sizing = importlib.import_module("tests._live.tester.server")
    # Port 0 asks the OS for a free port.
    Server(port=0, jvm_args=sizing.jvm_args(sys.argv[1:])).start()
    cli = importlib.import_module("tests._live.tester.cli")
    status: int = cli.main(sys.argv[1:], root=ROOT)
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
