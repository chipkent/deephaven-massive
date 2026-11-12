"""Size the embedded Deephaven server a live-run process starts before it imports the library.

Importing the library imports Deephaven, which needs a running JVM, and the live run's command
line (`cli`) imports the library. So `scripts/live_run.py` starts the server first, with the
JVM arguments `jvm_args` gives for its command line, and only then imports `cli`. This module
imports neither the library nor Deephaven, so the script can call it before the server runs.

A built-in group's child (the hidden `_group` command) runs the group's sessions, so its heap
is sized to the group: half the machine's memory up to 16 GB for a group that streams, 4 GB
for the others; a JVM crash writes its log into the attempt's folder, so the crash travels
with the results. Every other command opens no session (a group's sessions run in its child),
so it runs with `PARENT_HEAP`.

The server needs Java 17 or later, and `check` exists to say when it is missing; a server
that cannot start would end the program before `check` could say so. `java_problem` runs the
same Java check as `check` does (`machine_checks.java_item`) before the server starts, so a
script prints its `FAIL java` line and stops instead.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

from tests._live.live_run import machine_checks
from tests._live.live_run.children import run_command

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "PARENT_HEAP",
    "SMALL_HEAP",
    "STREAM_GROUPS",
    "STREAM_HEAP_LIMIT",
    "java_problem",
    "jvm_args",
]

STREAM_HEAP_LIMIT: Final = 16 << 30
"""The most heap a streaming group's child takes, in bytes."""

SMALL_HEAP: Final = 4 << 30
"""The heap of a group's child that does not stream, in bytes."""

PARENT_HEAP: Final = 1 << 30
"""The heap of every command but `_group`, in bytes: it opens no session."""

STREAM_GROUPS: Final = frozenset({"stream-day", "throughput", "stream-night"})
"""The groups whose child streams for hours and gets the larger heap."""


def jvm_args(argv: Sequence[str]) -> list[str]:
    """Return the JVM arguments of the server a live-run command line starts.

    Args:
        argv: The command line, without the program's name, as `cli.main` will read it.

    Returns:
        For `_group --out OUT --attempt ATTEMPT ...`, the group's heap (read from the attempt's
        `request.json`) and `-XX:ErrorFile` in `OUT/ATTEMPT`; for any other command line, a
        heap of `PARENT_HEAP` alone.
    """
    if not argv or argv[0] != "_group":
        return [f"-Xmx{PARENT_HEAP >> 20}m"]
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
