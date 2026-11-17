r"""Choose how many pytest-xdist workers the local check runs, from the machine's CPUs and memory.

`scripts/check.sh full` runs pytest on this many workers. Each worker starts its own embedded
Deephaven JVM, with a heap of at most 2 GiB (`tests/embedded_server.py`), so the count is
bounded by memory as well as by CPUs:

    workers = max(1, min(cpus - 2, available memory // WORKER_MEMORY))

Two CPUs stay free for pytest's controller process, which starts a JVM of its own, and the rest
of the machine. Available memory is what the system can hand out without paging: Linux's
`MemAvailable`, or the free, inactive, and speculative pages macOS's `vm_stat` reports. When it
cannot be read, the CPUs alone decide. `--workers N`, a positive integer, overrides the rule.

It prints the count on standard output and one line on standard error that says how it was
chosen. It uses only the standard library.

    uv run python scripts/worker_count.py [--workers N]

The examples below are doctests, which `check.sh full` runs before it asks for a count:

>>> GiB = 2**30
>>> choose(cpus=10, available=64 * GiB, override=None)
Choice(workers=8, reason='10 CPUs, 64.0 GiB available, 3.0 GiB per worker')
>>> choose(cpus=10, available=7 * GiB, override=None).workers
2
>>> choose(cpus=2, available=64 * GiB, override=None).workers
1
>>> choose(cpus=4, available=None, override=None)
Choice(workers=2, reason='4 CPUs, available memory unknown')
>>> choose(cpus=10, available=64 * GiB, override="3")
Choice(workers=3, reason='--workers 3')
>>> choose(cpus=10, available=64 * GiB, override="0")
Traceback (most recent call last):
...
ValueError: --workers must be a positive integer, not '0'
>>> parse_vm_stat(
...     "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
...     "Pages free:                               10.\n"
...     "Pages active:                             99.\n"
...     "Pages inactive:                           20.\n"
...     "Pages speculative:                         2.\n"
... )
524288
>>> parse_meminfo("MemTotal:  100 kB\nMemAvailable:  64 kB\n")
65536
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys
from typing import NamedTuple

#: The memory one worker may take: its JVM's 2 GiB heap, the JVM's own memory beyond the heap,
#: and the Python process around it.
WORKER_MEMORY = 3 * 2**30

#: The option that sets the count outright.
OVERRIDE = "--workers"

#: The CPUs left for pytest's controller and the rest of the machine.
_SPARE_CPUS = 2

_MEMINFO = pathlib.Path("/proc/meminfo")
_VM_STAT_FREE = ("Pages free", "Pages inactive", "Pages speculative")


class Choice(NamedTuple):
    """The worker count, and the reason the summary line gives for it."""

    workers: int
    reason: str


def choose(*, cpus: int, available: int | None, override: str | None) -> Choice:
    """Return the worker count for a machine.

    Args:
        cpus: The machine's logical CPUs.
        available: The bytes of memory available, or `None` when unknown.
        override: The value of `--workers`, or `None` when it is not given or blank.

    Returns:
        The override when given; else at most `cpus - 2` and at most one worker per
        `WORKER_MEMORY` of available memory, and at least 1.

    Raises:
        ValueError: The override is not a positive integer.
    """
    if override is not None:
        if not override.isdigit() or int(override) < 1:
            raise ValueError(f"{OVERRIDE} must be a positive integer, not {override!r}")
        return Choice(int(override), f"{OVERRIDE} {override}")
    by_cpu = cpus - _SPARE_CPUS
    if available is None:
        return Choice(max(1, by_cpu), f"{cpus} CPUs, available memory unknown")
    by_memory = available // WORKER_MEMORY
    memory = f"{available / 2**30:.1f} GiB available, {WORKER_MEMORY / 2**30:.1f} GiB per worker"
    return Choice(max(1, min(by_cpu, by_memory)), f"{cpus} CPUs, {memory}")


def parse_meminfo(text: str) -> int | None:
    """Return Linux's `MemAvailable` in bytes, from the text of `/proc/meminfo`, or `None`."""
    match = re.search(r"^MemAvailable:\s+(\d+) kB$", text, re.MULTILINE)
    return int(match[1]) * 1024 if match else None


def parse_vm_stat(text: str) -> int | None:
    """Return the free, inactive, and speculative bytes of macOS's `vm_stat` output, or `None`."""
    size = re.search(r"page size of (\d+) bytes", text)
    counts = [re.search(rf"^{name}:\s+(\d+)\.$", text, re.MULTILINE) for name in _VM_STAT_FREE]
    if size is None or None in counts:
        return None
    return sum(int(count[1]) for count in counts if count is not None) * int(size[1])


def available_memory() -> int | None:
    """Return the bytes of memory the system can hand out without paging, or `None`."""
    if _MEMINFO.is_file():
        return parse_meminfo(_MEMINFO.read_text(encoding="utf-8"))
    vm_stat = shutil.which("vm_stat")
    if vm_stat is None:
        return None
    result = subprocess.run(  # noqa: S603 - vm_stat with no arguments, no shell
        [vm_stat],
        capture_output=True,
        text=True,
        check=False,
    )
    return parse_vm_stat(result.stdout) if result.returncode == 0 else None


def main(argv: list[str] | None = None) -> int:
    """Print the count on standard output and the summary line on standard error.

    Args:
        argv: The arguments, without the program's name; the command line's by default.

    Returns:
        0, or 2 when `--workers` is not a positive integer (argparse exits 2 itself on an
        unknown argument).
    """
    parser = argparse.ArgumentParser(
        prog="worker_count.py", description="Choose how many pytest-xdist workers to run."
    )
    parser.add_argument(OVERRIDE, metavar="N", default="", help="the count, overriding the rule")
    override = parser.parse_args(argv).workers.strip() or None
    try:
        choice = choose(cpus=os.cpu_count() or 1, available=available_memory(), override=override)
    except ValueError as error:
        sys.stderr.write(f"check.sh: {error}\n")
        return 2
    sys.stdout.write(f"{choice.workers}\n")
    plural = "" if choice.workers == 1 else "s"
    sys.stderr.write(f"check.sh: {choice.workers} test worker{plural} ({choice.reason})\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
