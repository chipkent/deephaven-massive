"""Start the test run's embedded Deephaven server, before anything imports the library.

`import deephaven_massive` imports Deephaven, which needs a running JVM, and `conftest.py`
imports the library at module level. pytest imports a `-p` plugin before it loads any
conftest, so this module, named by `-p tests.embedded_server` in the `addopts` of
`pyproject.toml`, starts the server when pytest imports it: once per process, in the main
process and in each pytest-xdist worker alike. Test modules never import it.

The JVM runs the update graph on a 25 ms cycle (`CYCLE_MILLIS`) instead of Deephaven's
1000 ms, so ticking-table tests finish fast; `conftest.py` checks that the JVM applied it.
"""

from __future__ import annotations

import faulthandler
from pathlib import Path
from typing import Final

from deephaven_server import Server

__all__ = ["CYCLE_MILLIS"]

CYCLE_MILLIS: Final = 25
"""The update-graph cycle of the test JVM, in milliseconds (Deephaven's default is 1000)."""

_JVM_ARGS: Final = [
    "-Xmx2g",
    # pytest's conftest checks the cycle took effect: the JVM ignores an unknown property.
    f"-DPeriodicUpdateGraph.targetCycleDurationMillis={CYCLE_MILLIS}",
    # A JVM crash writes its hs_err log to the repository root, where CI collects it, instead
    # of the working directory or, if that is not writable, the temporary directory.
    f"-XX:ErrorFile={Path(__file__).resolve().parent.parent}/hs_err_pid%p.log",
]

# The JVM raises SIGSEGV on purpose (implicit null checks, safepoints) and handles it, chaining
# to the handler it found installed. Enabled first, faulthandler is that handler; pytest's own
# faulthandler plugin enables it again later, which then only changes the file it writes to.
# Enabled after the JVM, faulthandler would replace the JVM's handlers, and the first such
# signal would kill the process.
faulthandler.enable()
# Port 0 asks the OS for an ephemeral port, so two test runs can share a machine.
Server(port=0, jvm_args=_JVM_ARGS).start()
