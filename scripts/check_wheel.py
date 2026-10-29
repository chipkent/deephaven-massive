"""Check that an installed wheel of the library imports and works, inside a Deephaven server.

CI's build job builds the wheel, installs it with `deephaven-server` into a fresh virtual
environment, and runs this script with that environment's interpreter, from outside the
checkout's `src/`:

    <venv>/bin/python -I scripts/check_wheel.py docs/datasets

To run it locally, install `dist/*.whl` and `deephaven-server` into a new environment with
`uv venv` and `uv pip install`, then run, from the repository root:

    uv run --no-project --python <venv>/bin/python python -I scripts/check_wheel.py docs/datasets

`import deephaven_massive` alone proves little, since the package imports its modules lazily.
So the script starts an embedded Deephaven server (the library imports Deephaven, and the
library itself never starts a server) and then:

1. checks that `deephaven_massive` comes from the installed wheel, not a source checkout;
2. imports every module the wheel ships, and checks that the refresh package, which runs only
   from a checkout, is not among them;
3. resolves every public name of the package and of its wire package;
4. renders the column reference from the installed catalog, which loads every generated
   schema and description the wheel carries as package data, and compares it with the
   committed pages in the directory given as the only argument;
5. opens a session and closes it; nothing reaches the network.

Exit status: 0 when every check passes; 1 when one fails, after the script has printed the
failure's traceback (a `WheelCheckError` naming the check, or any other exception). The process
leaves through `os._exit` once its output is flushed, because a normal interpreter shutdown
after the embedded JVM has run can crash on macOS.
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

from deephaven_server import Server

# Neither module imports Deephaven (the package's own __init__.py imports lazily), so both
# import before the server starts.
import deephaven_massive
from deephaven_massive import _massive

if TYPE_CHECKING:
    from types import TracebackType

    from deephaven_massive import _session
    from deephaven_massive._datasets import column_reference

_CHECKOUT_SOURCE = Path(__file__).resolve().parent.parent / "src"
"""The checkout's source tree, which must not be where the library is imported from."""

_REFRESH = "deephaven_massive._massive.refresh"
"""The package that regenerates the wire schemas; it runs from a checkout and is not shipped."""


class WheelCheckError(Exception):
    """A check failed; the message says which and what was found."""


def main() -> None:
    """Run every check and print one line per check that passed.

    It starts an embedded Deephaven server in this process, opens and closes a session, and
    writes to standard output. It reads, and never writes, the directory of committed pages.

    Raises:
        WheelCheckError: If the arguments are not one existing directory, or at the first
            check that fails.
    """
    arguments = sys.argv[1:]
    if len(arguments) != 1:
        raise WheelCheckError("usage: check_wheel.py <directory of the committed pages>")
    committed = Path(arguments[0])
    if not committed.is_dir():
        raise WheelCheckError(f"{committed}, the directory of committed pages, does not exist")
    package_dir = Path(deephaven_massive.__file__).resolve().parent
    if package_dir.is_relative_to(_CHECKOUT_SOURCE):
        raise WheelCheckError(
            f"deephaven_massive was imported from {package_dir}, the checkout, not the wheel"
        )
    if "site-packages" not in package_dir.parts:
        raise WheelCheckError(
            f"deephaven_massive was imported from {package_dir}, which is not an installed wheel"
        )
    _report(f"deephaven_massive {deephaven_massive.__version__} from {package_dir}")

    # Port 0 asks the OS for an ephemeral port, so the check never clashes with a server.
    Server(port=0, jvm_args=["-Xmx1g"]).start()

    names = sorted(
        module.name
        for module in pkgutil.walk_packages(deephaven_massive.__path__, "deephaven_massive.")
    )
    shipped_refresh = [name for name in names if name.startswith(_REFRESH)]
    if shipped_refresh:
        raise WheelCheckError(f"the wheel ships the refresh package: {shipped_refresh}")
    for name in names:
        importlib.import_module(name)
    _report(f"imported {len(names)} modules")

    for package in (deephaven_massive, _massive):
        for name in package.__all__:
            getattr(package, name)
    _report("resolved every public name")

    # Imported only once the server runs, since both import Deephaven. The type checker reads
    # the static imports above instead, so a renamed module or function fails the type check.
    if TYPE_CHECKING:
        session, reference = _session, column_reference
    else:
        session = importlib.import_module("deephaven_massive._session")
        reference = importlib.import_module("deephaven_massive._datasets.column_reference")
    pages = reference.render(session.CATALOG, _massive.provenance())
    on_disk = {path.name: path.read_text(encoding="utf-8") for path in committed.glob("*.md")}
    if pages != on_disk:
        differ = sorted(n for n in pages.keys() | on_disk.keys() if pages.get(n) != on_disk.get(n))
        raise WheelCheckError(
            f"the installed catalog renders pages that differ from {committed}: {differ}"
        )
    _report(f"rendered the column reference of {len(session.CATALOG.by_name)} datasets")

    m = deephaven_massive.MassiveSession(deephaven_massive.MassiveConfig(api_key="wheel-check"))
    m.close()
    _report("opened and closed a session")


def _report(line: str) -> None:
    """Print one passed check to standard output."""
    sys.stdout.write(f"ok: {line}\n")


def _exit(status: int) -> None:
    """Flush the output and leave with `status`, skipping the JVM's and jpy's finalizers."""
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)


def _fail(kind: type[BaseException], value: BaseException, tb: TracebackType | None) -> None:
    """Print an exception that escaped `main` and leave with status 1.

    Installed as `sys.excepthook`, and called for a `SystemExit`, which skips the hook, so every
    failure, an interrupt or an exit included, fails the check.
    """
    traceback.print_exception(kind, value, tb)
    _exit(1)


if __name__ == "__main__":
    sys.excepthook = _fail
    try:
        main()
    except SystemExit as stop:
        _fail(type(stop), stop, stop.__traceback__)
    _exit(0)
