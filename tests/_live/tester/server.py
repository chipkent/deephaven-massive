"""Size the embedded Deephaven server a tester command starts before it imports the library.

Importing the library imports Deephaven, which needs a running JVM, and every tester command
imports it: `check` and `bug-report` probe the key through the library's REST client, and
`examples` runs the guide's examples. So `scripts/tester.py` starts the server first, with
the JVM arguments `jvm_args` gives for its command line, and only then imports the command
line. This module imports neither the library nor Deephaven, so the script can call it before
the server runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from tests._live.live_run.server import PARENT_HEAP

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["EXAMPLES_HEAP", "jvm_args"]

EXAMPLES_HEAP: Final = 4 << 30
"""The heap of `examples`, in bytes: the guide's examples build their tables in it."""


def jvm_args(argv: Sequence[str]) -> list[str]:
    """Return the JVM arguments of the server a tester command line starts.

    Args:
        argv: The command line, without the program's name, as `cli.main` will read it.

    Returns:
        A heap of `EXAMPLES_HEAP` for `examples`, which builds the guide's tables, and of
        `PARENT_HEAP` for every other command, which only probes the key.
    """
    heap = EXAMPLES_HEAP if argv[:1] == ["examples"] else PARENT_HEAP
    return [f"-Xmx{heap >> 20}m"]
