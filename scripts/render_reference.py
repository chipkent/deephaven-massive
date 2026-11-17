"""Regenerate the generated docs: `docs/datasets/`, settings, problem causes, README plans.

Run it from a source checkout:

    uv run python scripts/render_reference.py

It starts an embedded Deephaven server first, before it imports the library, because importing
the library imports Deephaven, which needs a running JVM, and the library itself never starts
a server. Then it renders an index and one page per dataset from the dataset catalog, each
dataset's call (its method's signature and verbs), and the provenance of the generated wire
schemas (their vendor pages and those pages' plan tables), makes `docs/datasets/` hold exactly
those pages (deleting any other file or directory there, so no orphaned page survives a
rename), and prints which columns each dataset gained or lost, then what it wrote and deleted.
Run after a refresh of Massive's specifications, the column report is how the refresh shows
what it does to each table.

It also rewrites the generated part of `docs/configuration.md`, between its two marker lines,
from `MassiveConfig`: each setting's settings-file key, docstring entry, values, and default.
The rest of that page is written by hand and kept as it is. It rewrites the table of problem
causes in `docs/troubleshooting.md` the same way, from `problem_events.CAUSES`, and README.md's
plans table, between its two marker lines, from the plan tables of Massive's pages.
All the logic is in `deephaven_massive._datasets.column_reference`,
`deephaven_massive._datasets.readme_plans`, `deephaven_massive._tables.cause_reference`, and
`deephaven_massive._config`, where the unit tests and the coverage gate reach it; this script
only wires it up.

Exit status: 0 on success; 1 when rendering or syncing raises, after the interpreter has
printed the traceback. The process leaves through `os._exit` once its output is flushed,
because a normal interpreter shutdown after the embedded JVM has run can crash on macOS.
"""

from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from deephaven_server import Server

if TYPE_CHECKING:
    from collections.abc import Callable


def _exit() -> None:
    """Flush the output and leave, skipping the JVM's and jpy's finalizers.

    Runs at interpreter exit. The status is 1 when an exception escaped (the interpreter has
    printed it and set `sys.last_value`), and 0 otherwise.
    """
    status = 1 if getattr(sys, "last_value", None) is not None else 0
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)


# Registered first, so a server start that fails after the JVM loaded still exits safely.
atexit.register(_exit)
# Port 0 asks the OS for an ephemeral port, so the command never clashes with a server.
Server(port=0, jvm_args=["-Xmx1g"]).start()

# The library imports Deephaven, which needs the server started above.
from deephaven_massive import _config, _dataset_list, _massive, _session  # noqa: E402 - needs the server above
from deephaven_massive._datasets.column_reference import (  # noqa: E402 - needs the server above
    column_changes,
    render,
    sync,
)
from deephaven_massive._datasets.readme_plans import with_plans_table  # noqa: E402 - needs the server above
from deephaven_massive._tables.cause_reference import (  # noqa: E402 - needs the server above
    with_causes_reference,
)

_DIRECTORY = Path(__file__).resolve().parent.parent / "docs" / "datasets"
"""The dataset reference's directory, found from this script's place in the checkout."""

_CONFIGURATION_PAGE = _DIRECTORY.parent / "configuration.md"
"""The settings page, whose generated part this script rewrites."""

_TROUBLESHOOTING_PAGE = _DIRECTORY.parent / "troubleshooting.md"
"""The troubleshooting page, whose table of problem causes this script rewrites."""
_README = _DIRECTORY.parent.parent / "README.md"
"""The README, whose plans table this script rewrites."""


def main() -> None:
    """Render the pages, sync them into `docs/datasets/`, and report.

    Side effects: reads, deletes, and writes files under `docs/datasets/`, rewrites
    `docs/configuration.md` when its settings changed, and writes one line per dataset whose
    columns changed and one per file changed to standard output.
    """
    pages = render(_session.CATALOG, _massive.provenance(), _dataset_list.dataset_calls())
    committed = {p.name: p.read_text() for p in sorted(_DIRECTORY.glob("*.md")) if p.is_file()}
    for line in column_changes(committed, pages):
        sys.stdout.write(f"columns: {line}\n")
    result = sync(_DIRECTORY, pages)
    shown = _DIRECTORY.relative_to(_DIRECTORY.parent.parent)
    for name in result.written:
        sys.stdout.write(f"wrote {shown / name}\n")
    for name in result.deleted:
        sys.stdout.write(f"deleted {shown / name}\n")
    if not result.written and not result.deleted:
        sys.stdout.write(f"{shown}/ is up to date\n")
    _rewrite(_CONFIGURATION_PAGE, _config.with_settings_reference)
    _rewrite(_TROUBLESHOOTING_PAGE, with_causes_reference)
    provenance = _massive.provenance()
    _rewrite(_README, lambda page: with_plans_table(page, _session.CATALOG, provenance))


def _rewrite(path: Path, render_part: Callable[[str], str]) -> None:
    """Rewrite a page's generated part, and say whether it changed.

    Args:
        path: The page.
        render_part: Returns the page with its generated part rendered again.
    """
    page = path.read_text(encoding="utf-8")
    rendered = render_part(page)
    shown_page = path.relative_to(_DIRECTORY.parent.parent)
    if rendered == page:
        sys.stdout.write(f"{shown_page} is up to date\n")
    else:
        path.write_text(rendered, encoding="utf-8")
        sys.stdout.write(f"wrote {shown_page}\n")


if __name__ == "__main__":
    main()
