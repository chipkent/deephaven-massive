"""The embedded Deephaven server a live-run child starts before it imports the library.

Importing the library imports Deephaven, which needs a running JVM. The live run's parent (the
commands a person types, `cli`) imports neither, and starts no JVM: it lives for days, and
runs everything that reads Massive in a child of its own script. Only those children, the
hidden commands of `child_commands`, import this module: `start_server` starts their server,
with the JVM arguments `jvm_args` gives for their command line, and the script imports the
library only after it runs. This module imports neither the library nor Deephaven.

A built-in group's child (`_group`) runs the group's sessions, so its heap is sized to the
group: half the machine's memory up to 16 GB for a group that streams, 4 GB for the others; a
JVM crash writes its log into the attempt's folder, so the crash travels with the results. The
probe's child (`_probe`) opens no session, so it runs with `PROBE_HEAP`.

The server needs Java 17 or later. `check` says when it is missing, and the parent starts no
server, so `check` always reaches its report. A child checks first too: `java_problem` runs the
same Java check as `check` does (`machine_checks.java_item`) before the server starts, so a
child without Java prints its `FAIL java` line and stops instead of failing to start one.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

from deephaven_server import Server

from tests._live.live_run import machine_checks
from tests._live.live_run.children import run_command

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "PROBE_HEAP",
    "SMALL_HEAP",
    "STREAM_GROUPS",
    "STREAM_HEAP_LIMIT",
    "java_problem",
    "jvm_args",
    "start_server",
]

STREAM_HEAP_LIMIT: Final = 16 << 30
"""The most heap a streaming group's child takes, in bytes."""

SMALL_HEAP: Final = 4 << 30
"""The heap of a group's child that does not stream, in bytes."""

PROBE_HEAP: Final = 1 << 30
"""The heap of a process that only probes the key, in bytes: it opens no session."""

STREAM_GROUPS: Final = frozenset({"stream-day", "throughput", "stream-night"})
"""The groups whose child streams for hours and gets the larger heap."""


def jvm_args(argv: Sequence[str]) -> list[str]:
    """Return the JVM arguments of the server a child's command line starts.

    Args:
        argv: A child's command line, without the program's name, as `child_commands.main`
            will read it.

    Returns:
        For `_group --out OUT --attempt ATTEMPT ...`, the group's heap (read from the attempt's
        `request.json`) and `-XX:ErrorFile` in `OUT/ATTEMPT`; for `_probe`, a heap of
        `PROBE_HEAP` alone.
    """
    if argv[0] != "_group":
        return [f"-Xmx{PROBE_HEAP >> 20}m"]
    child = argparse.ArgumentParser(add_help=False)
    child.add_argument("--out", type=Path, required=True)
    child.add_argument("--attempt", required=True)
    arguments, _ = child.parse_known_args(argv[1:])
    folder = arguments.out.expanduser().resolve() / arguments.attempt
    request = json.loads((folder / "request.json").read_text(encoding="utf-8"))
    memory = machine_checks.memory_total() or SMALL_HEAP * 2
    streams = request["group"] in STREAM_GROUPS
    heap = min(memory // 2, STREAM_HEAP_LIMIT) if streams else SMALL_HEAP
    return [f"-Xmx{heap >> 20}m", f"-XX:ErrorFile={folder}/hs_err_pid%p.log"]


def java_problem(environ: Mapping[str, str] = os.environ) -> str | None:
    """Say why the embedded server cannot start for want of Java, before a script starts it.

    Args:
        environ: The environment, whose `JAVA_HOME` names the JDK the server starts.

    Returns:
        The `check` report's line for Java, such as `"FAIL java: Java not found: install JDK
        17 or later"`, when `java -version` finds no Java 17 or later; None when it does.
    """
    java = machine_checks.java_command(environ)
    item = machine_checks.java_item(run_command([java, "-version"]))
    return None if item.verdict == machine_checks.OK else f"{item.verdict} java: {item.detail}"


def start_server(argv: Sequence[str], *, environ: Mapping[str, str] = os.environ) -> str | None:
    """Start a child's embedded server, sized to its command line, unless Java is missing.

    Args:
        argv: The child's command line, without the program's name (`jvm_args`).
        environ: The environment, whose `JAVA_HOME` names the JDK the server starts.

    Returns:
        The `check` report's line for Java (`java_problem`) when no server could start for want
        of Java 17 or later; None once the server runs.
    """
    problem = java_problem(environ)
    if problem is None:
        # Port 0 asks the OS for a free port, so the server never clashes with another one.
        Server(port=0, jvm_args=jvm_args(argv)).start()
    return problem
