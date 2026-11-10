"""The live run's checks of the machine: tools, secrets, clock, disk, memory, and keep-awake.

Before the live-run program (`live_run`) spends a request, it checks that the machine can run
for days: Java 17 or later, `uv`, the checkout's commit and whether its tree is clean, the
versions of Python, the library and its dependencies, the secrets, the clock against Massive's,
the free disk and the memory, and a way to keep the machine awake. Each item is `OK`, `WARN`, or
`FAIL`; a failed required item makes the machine not ready. The secrets are reported only as
set or unset, never by value, and so are `HTTPS_PROXY` and `SSL_CERT_FILE`.

`check_machine` runs the commands it needs (`java -version`, `uv --version`, `git`) through
`children.run_command`; every other function is pure.
"""

from __future__ import annotations

import dataclasses
import os
import platform
import re
import shutil
import sys
from dataclasses import dataclass
from importlib import metadata
from typing import TYPE_CHECKING, Final

from tests._live.live_run.children import run_command
from tests._live.live_run.secret_scrub import SECRET_VARIABLES

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

GIB: Final = 1 << 30

REQUIRED_DISK: Final = 20 * GIB
"""Free disk the results folder needs: a stream day's Parquet files and logs."""

RECOMMENDED_MEMORY: Final = 8 * GIB
"""Memory below which a stream day's JVM may run short."""

CLOCK_WARN: Final = 2.0
CLOCK_FAIL: Final = 30.0
"""Clock offsets, in seconds, above which the clock is a warning and a failure."""

OK: Final = "OK"
WARN: Final = "WARN"
FAIL: Final = "FAIL"


@dataclass(frozen=True)
class Item:
    """One checked item.

    Attributes:
        name: What was checked, such as `"java"`.
        verdict: `OK`, `WARN`, or `FAIL`.
        detail: What was found, and for a problem how to fix it.
    """

    name: str
    verdict: str
    detail: str


def java_item(version_text: str) -> Item:
    """Judge `java -version`'s output: Java 17 or later is required.

    Args:
        version_text: What `java -version` printed, `""` when Java is missing.

    Returns:
        The item.
    """
    found = re.search(r'version "(\d+)(?:\.(\d+))?', version_text)
    if found is None:
        return Item("java", FAIL, "Java not found: install JDK 17 or later")
    major = int(found.group(1))
    if major == 1 and found.group(2):  # the old "1.8" spelling
        major = int(found.group(2))
    if major < 17:
        return Item("java", FAIL, f"Java {major}: install JDK 17 or later")
    return Item("java", OK, f"Java {major}")


def secrets_item(environ: Mapping[str, str]) -> Item:
    """Check the secrets: the API key set, and the S3 pair both set or both unset.

    Args:
        environ: The environment.

    Returns:
        The item, naming only which variables are set.
    """
    present = [name for name in SECRET_VARIABLES if environ.get(name, "").strip()]
    shown = ", ".join(present) or "none"
    if "MASSIVE_API_KEY" not in present:
        return Item("secrets", FAIL, f"MASSIVE_API_KEY is not set in this terminal; set: {shown}")
    pair = [name for name in SECRET_VARIABLES[2:] if name in present]
    if len(pair) == 1:
        return Item("secrets", FAIL, f"set both S3 variables or neither; set: {shown}")
    return Item("secrets", OK, f"set: {shown}")


def clock_item(offset: float | None) -> Item:
    """Judge the clock's offset from Massive's server time.

    Args:
        offset: The offset in seconds, or None when Massive's time was not read.

    Returns:
        The item: a warning above 2 s or when unknown, a failure above 30 s.
    """
    if offset is None:
        return Item("clock", WARN, "Massive's server time was not read")
    text = f"{offset:+.1f} s from Massive's server time"
    if abs(offset) > CLOCK_FAIL:
        return Item("clock", FAIL, f"{text}: set the clock automatically")
    if abs(offset) > CLOCK_WARN:
        return Item("clock", WARN, text)
    return Item("clock", OK, text)


def disk_item(free: int) -> Item:
    """Judge the free disk under the results folder.

    Args:
        free: Free bytes.

    Returns:
        The item: a failure below 20 GB.
    """
    text = f"{free / GIB:.0f} GB free"
    return Item("disk", OK if free >= REQUIRED_DISK else FAIL, text)


def memory_item(total: int | None) -> Item:
    """Judge the machine's memory.

    Args:
        total: Total bytes, or None when unknown.

    Returns:
        The item: a warning below 8 GB or when unknown.
    """
    if total is None:
        return Item("memory", WARN, "unknown")
    text = f"{total / GIB:.0f} GB"
    return Item("memory", OK if total >= RECOMMENDED_MEMORY else WARN, text)


def keep_awake_item(system: str, which: Callable[[str], str | None]) -> Item:
    """Check for the tool that keeps the machine awake.

    Args:
        system: `platform.system()`, such as `"Darwin"` or `"Linux"`.
        which: Finds a program on the path (`shutil.which`).

    Returns:
        The item: a warning when neither `caffeinate` nor `systemd-inhibit` is found.
    """
    tool = "caffeinate" if system == "Darwin" else "systemd-inhibit"
    if which(tool) is None:
        return Item("keep-awake", WARN, f"{tool} not found: keep the machine awake yourself")
    return Item("keep-awake", OK, tool)


def memory_total() -> int | None:
    """Return the machine's memory in bytes, when the system says.

    Returns:
        Total physical memory, or None.
    """
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        return None


def dependency_versions() -> dict[str, str]:
    """Return the version of the library and of each dependency it declares.

    Returns:
        Each distribution's version, by name, the library first.
    """
    found = {"deephaven-massive": metadata.version("deephaven-massive")}
    for requirement in metadata.requires("deephaven-massive") or []:
        name = re.split(r"[\s;<>=!~\[]", requirement, maxsplit=1)[0]
        if "extra ==" in requirement or not name:
            continue
        try:
            found[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            found[name] = "not installed"
    return found


def check_machine(
    *, root: Path, disk_path: Path, environ: Mapping[str, str], clock_offset: float | None
) -> tuple[list[Item], dict[str, object]]:
    """Check the machine, and describe its environment for `environment.json`.

    Args:
        root: The repository root, for its commit.
        disk_path: Where the results go, for its free disk.
        environ: The environment.
        clock_offset: The probe's clock offset, or None.

    Returns:
        The items, and the environment: OS, Python, Java, the commit and whether the tree is
        clean, the versions, which secrets are set, and whether a proxy or a certificate file
        is set; never a secret's value.
    """
    java = run_command([shutil.which("java") or "java", "-version"])
    uv = run_command([shutil.which("uv") or "uv", "--version"]).strip()
    git = shutil.which("git") or "git"
    commit = run_command([git, "-C", str(root), "rev-parse", "HEAD"]).strip()
    dirty = run_command([git, "-C", str(root), "status", "--porcelain"]).strip()
    items = [
        java_item(java),
        Item(
            "uv",
            OK if uv else FAIL,
            uv or "uv not found: install it from https://docs.astral.sh/uv/",
        ),
        Item(
            "commit",
            OK if commit else FAIL,
            f"{commit or 'unknown'}, tree {'changed' if dirty else 'clean'}",
        ),
        secrets_item(environ),
        clock_item(clock_offset),
        disk_item(shutil.disk_usage(disk_path).free),
        memory_item(memory_total()),
        keep_awake_item(platform.system(), shutil.which),
    ]
    environment: dict[str, object] = {
        "os": platform.platform(),
        "python": sys.version.split()[0],
        "java": java.strip().splitlines()[0] if java.strip() else "",
        "uv": uv,
        "commit": commit,
        "tree_clean": not dirty,
        "versions": dependency_versions(),
        "set": {
            name: bool(environ.get(name, "").strip())
            for name in (*SECRET_VARIABLES, "HTTPS_PROXY", "SSL_CERT_FILE")
        },
        "items": [dataclasses.asdict(item) for item in items],
    }
    return items, environment


def ready(items: list[Item]) -> bool:
    """Tell whether no item failed.

    Args:
        items: The checked items.

    Returns:
        True when the machine is ready.
    """
    return all(item.verdict != FAIL for item in items)
