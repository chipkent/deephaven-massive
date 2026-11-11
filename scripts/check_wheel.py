"""Check that an installed wheel of the library imports and works, inside a Deephaven server.

CI's build job builds the wheel, installs it with `deephaven-server` into a fresh virtual
environment, and runs this script with that environment's interpreter, from outside the
checkout's `src/`:

    <venv>/bin/python -I scripts/check_wheel.py docs/datasets

The release passes the version it is releasing, and the sdist built beside the wheel:

    <venv>/bin/python -I scripts/check_wheel.py docs/datasets \
        --version 0.1.0rc1 --sdist dist/deephaven_massive-0.1.0rc1.tar.gz

To run it locally, install `dist/*.whl` and `deephaven-server` into a new environment with
`uv venv` and `uv pip install`, then run, from the repository root:

    uv run --no-project --python <venv>/bin/python python -I scripts/check_wheel.py docs/datasets

The script first starts an embedded Deephaven server, before it imports the library: importing
the library imports Deephaven, which needs a running JVM, and the library itself never starts a
server. Then it:

1. checks that `deephaven_massive` comes from the installed wheel, not a source checkout, and,
   given `--version`, that `deephaven_massive.__version__` is exactly that version and, given
   also `--sdist`, that the sdist's `PKG-INFO` names the same version (the version comes from
   the git tag at build time, so this proves the files carry the version being released);
2. imports every module the wheel ships, and checks that the refresh package, which runs only
   from a checkout, is not among them;
3. resolves every public name of the package and of its wire package;
4. renders the dataset reference from the installed catalog and dataset methods, which loads
   every generated schema, description, and plan table the wheel carries as package data, and
   compares it with the committed pages in the directory given as the first argument;
5. opens a session and closes it; nothing reaches the network.

Exit status: 0 when every check passes; 1 when one fails, after the script has printed the
failure's traceback (a `WheelCheckError` naming the check, or any other exception). The process
leaves through `os._exit` once its output is flushed, because a normal interpreter shutdown
after the embedded JVM has run can crash on macOS.
"""

from __future__ import annotations

import argparse
import importlib
import os
import pkgutil
import sys
import tarfile
import traceback
from email.parser import HeaderParser
from pathlib import Path
from typing import TYPE_CHECKING

from deephaven_server import Server

if TYPE_CHECKING:
    from types import TracebackType

# Port 0 asks the OS for an ephemeral port, so the check never clashes with a server.
Server(port=0, jvm_args=["-Xmx1g"]).start()

# The library imports Deephaven, which needs the server started above.
import deephaven_massive  # noqa: E402
from deephaven_massive import _dataset_list, _massive, _session  # noqa: E402
from deephaven_massive._datasets import column_reference  # noqa: E402

_CHECKOUT_SOURCE = Path(__file__).resolve().parent.parent / "src"
"""The checkout's source tree, which must not be where the library is imported from."""

_REFRESH = "deephaven_massive._massive.refresh"
"""The package that regenerates the wire schemas; it runs from a checkout and is not shipped."""


class WheelCheckError(Exception):
    """A check failed; the message says which and what was found."""


def main() -> None:
    """Run every check and print one line per check that passed.

    It opens and closes a session, in the embedded Deephaven server this script started, and
    writes to standard output. It reads, and never writes, the directory of committed pages.

    Raises:
        WheelCheckError: If the directory of committed pages does not exist, if `--sdist` is
            given without `--version`, or at the first check that fails.
        SystemExit: If the arguments cannot be parsed; `argparse` prints the usage.
    """
    parser = argparse.ArgumentParser(description="Check an installed wheel of the library.")
    parser.add_argument("committed", type=Path, help="the directory of committed dataset pages")
    parser.add_argument(
        "--version", help="the exact version the installed wheel, and the sdist, must carry"
    )
    parser.add_argument("--sdist", type=Path, help="the sdist built beside the wheel")
    arguments = parser.parse_args()
    committed: Path = arguments.committed
    if arguments.sdist is not None and arguments.version is None:
        raise WheelCheckError("--sdist needs --version, the version the sdist must carry")
    if not committed.is_dir():
        raise WheelCheckError(f"{committed}, the directory of committed pages, does not exist")
    _check_installed_wheel()
    if arguments.version is not None:
        _check_version(arguments.version, arguments.sdist)

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

    pages = column_reference.render(
        _session.CATALOG, _massive.provenance(), _dataset_list.dataset_calls()
    )
    on_disk = {path.name: path.read_text(encoding="utf-8") for path in committed.glob("*.md")}
    if pages != on_disk:
        differ = sorted(n for n in pages.keys() | on_disk.keys() if pages.get(n) != on_disk.get(n))
        raise WheelCheckError(
            f"the installed catalog renders pages that differ from {committed}: {differ}"
        )
    _report(f"rendered the dataset reference of {len(_session.CATALOG.by_name)} datasets")

    m = deephaven_massive.MassiveSession(deephaven_massive.MassiveConfig(api_key="wheel-check"))
    m.close()
    _report("opened and closed a session")


def _check_installed_wheel() -> None:
    """Check that `deephaven_massive` is imported from an installed wheel, not the checkout.

    Raises:
        WheelCheckError: If it is imported from the checkout's `src/` or from outside any
            `site-packages` directory.
    """
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


def _check_version(expected: str, sdist: Path | None) -> None:
    """Check that the installed wheel, and the sdist when given, carry exactly `expected`.

    Args:
        expected: The version being released, such as `"0.1.0rc1"`.
        sdist: The sdist built beside the wheel, or `None` to check the wheel alone.

    Raises:
        WheelCheckError: If `deephaven_massive.__version__` or the sdist's version differs.
    """
    if deephaven_massive.__version__ != expected:
        raise WheelCheckError(
            f"the installed wheel is version {deephaven_massive.__version__}, not {expected}"
        )
    _report(f"the wheel is version {expected}")
    if sdist is not None:
        sdist_version = _sdist_version(sdist)
        if sdist_version != expected:
            raise WheelCheckError(f"the sdist {sdist} is version {sdist_version}, not {expected}")
        _report(f"the sdist is version {expected}")


def _sdist_version(sdist: Path) -> str:
    """Return the `Version` field of the sdist's top-level `PKG-INFO`.

    Raises:
        WheelCheckError: If the archive has no top-level `PKG-INFO` or it names no version.
    """
    with tarfile.open(sdist) as archive:
        for member in archive.getmembers():
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO"):
                extracted = archive.extractfile(member)
                if extracted is None:
                    break
                fields = HeaderParser().parsestr(extracted.read().decode("utf-8"))
                version = fields.get("Version")
                if version:
                    return str(version)
                break
    raise WheelCheckError(f"the sdist {sdist} has no PKG-INFO naming a version")


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
