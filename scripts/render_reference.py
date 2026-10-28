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

from deephaven_server import Server

_DIRECTORY = Path(__file__).resolve().parent.parent / "docs" / "datasets"
"""The column reference's directory, found from this script's place in the checkout."""


def main() -> None:
    """Start the server, render the pages, sync them into `docs/datasets/`, and report.

    Side effects: starts an embedded Deephaven server in this process, deletes and writes
    files under `docs/datasets/`, and writes one line per change to standard output.
    """
    # Port 0 asks the OS for an ephemeral port, so the command never clashes with a server.
    Server(port=0, jvm_args=["-Xmx1g"]).start()
    atexit.register(_exit)
    # Imported only once the server runs: the session module imports Deephaven.
    wire = importlib.import_module("deephaven_massive._massive")
    reference = importlib.import_module("deephaven_massive._datasets.column_reference")
    catalog = importlib.import_module("deephaven_massive._session").CATALOG
    result = reference.sync(_DIRECTORY, reference.render(catalog, wire.provenance()))
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
