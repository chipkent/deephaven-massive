"""Regenerate the column reference, `docs/datasets/`: the whole directory and nothing else.

Run it from a source checkout:

    uv run python scripts/render_reference.py

It starts an embedded Deephaven server first, because the library's session module, which
builds the dataset catalog, imports Deephaven, and the library itself never starts a server.
Then it renders one page per namespace from that catalog and the provenance of the generated
wire schemas, makes `docs/datasets/` hold exactly those pages (deleting any other file or
directory there, so no orphaned page survives a rename), and prints what it wrote and deleted.
All the logic is in `deephaven_massive._datasets.column_reference`, where the unit tests and
the coverage gate reach it; this script only wires it up.

Exit status: 0 on success; 1 when rendering or syncing raises, after the interpreter has
printed the traceback. The process leaves through `os._exit` once its output is flushed,
because a normal interpreter shutdown after the embedded JVM has run can crash on macOS.
"""

from __future__ import annotations

import atexit
import importlib
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from deephaven_server import Server

# Neither module imports Deephaven (the package's own __init__.py imports lazily), so both
# import before the server starts.
from deephaven_massive import _massive
from deephaven_massive._datasets.column_reference import render, sync

if TYPE_CHECKING:
    from deephaven_massive import _session

_DIRECTORY = Path(__file__).resolve().parent.parent / "docs" / "datasets"
"""The column reference's directory, found from this script's place in the checkout."""


def main() -> None:
    """Start the server, render the pages, sync them into `docs/datasets/`, and report.

    Side effects: starts an embedded Deephaven server in this process, deletes and writes
    files under `docs/datasets/`, and writes one line per change to standard output.
    """
    # Registered first, so a server start that fails after the JVM loaded still exits safely.
    atexit.register(_exit)
    # Port 0 asks the OS for an ephemeral port, so the command never clashes with a server.
    Server(port=0, jvm_args=["-Xmx1g"]).start()
    # The session module, which builds the catalog, imports Deephaven, so it is imported only
    # once the server runs. The type checker reads the static import above instead, so a
    # renamed module or `CATALOG` fails the type check.
    if TYPE_CHECKING:
        session = _session
    else:
        session = importlib.import_module("deephaven_massive._session")
    result = sync(_DIRECTORY, render(session.CATALOG, _massive.provenance()))
    shown = _DIRECTORY.relative_to(_DIRECTORY.parent.parent)
    for name in result.written:
        sys.stdout.write(f"wrote {shown / name}\n")
    for name in result.deleted:
        sys.stdout.write(f"deleted {shown / name}\n")
    if not result.written and not result.deleted:
        sys.stdout.write(f"{shown}/ is up to date\n")


def _exit() -> None:
    """Flush the output and leave, skipping the JVM's and jpy's finalizers.

    Runs at interpreter exit. The status is 1 when an exception escaped `main` (the
    interpreter has printed it and set `sys.last_value`), and 0 otherwise.
    """
    status = 1 if getattr(sys, "last_value", None) is not None else 0
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)


if __name__ == "__main__":
    main()
