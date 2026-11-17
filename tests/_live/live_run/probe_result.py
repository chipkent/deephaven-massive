"""A probe's result, as the live run's parent gets it: from the probe run in a child process.

The probe (`run_probe.probe_key`) reads Massive through the library's wire package, and
importing the library imports Deephaven, which needs a running JVM. The parent of
`live_run.py run` lives for days and opens no session, so it starts no JVM: it runs each probe
in a child of its own script (the hidden `_probe` command, which starts an embedded server),
and reads the `ProbeResult` the child writes as JSON (`probe_in_child`). This module imports
neither the library nor Deephaven, so the parent can import it.
"""

from __future__ import annotations

import dataclasses
import json
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from tests._live.live_run import children

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tests._live.live_run.secret_scrub import SecretFilter

PROBE_LIMIT: Final = timedelta(minutes=15)
"""How long the probe's child may run, its server's start included, before it is stopped: a
probe sends two requests, so it takes a few minutes at most, its server's start included."""

SHOWN_OUTPUT: Final = 20
"""The last lines of a failed probe child's output that its error shows."""


@dataclass
class ProbeResult:
    """What one probe found.

    Attributes:
        when: When it ran, ISO UTC.
        clock_offset: The machine clock minus Massive's `serverTime`, in seconds, or None.
        key_rejected: Whether Massive rejected the API key.
        error: Why the market status could not be read, `""` when it was.
        closures: Massive's upcoming market closures: each ISO date's status, `closed` or
            `early-close` (a date with both, on different exchanges, is `closed`); None when
            they could not be read.
        requests: Each request made: its path, query, status, and request id.
    """

    when: str
    clock_offset: float | None = None
    key_rejected: bool = False
    error: str = ""
    closures: dict[str, str] | None = None
    requests: list[dict[str, Any]] = field(default_factory=list)


class ProbeChildError(Exception):
    """The probe's child process ended without writing a result, or did not end in time."""


def write_result(path: Path, result: ProbeResult) -> None:
    """Write a probe's result for the parent to read: the probe child's last step.

    Args:
        path: The file the parent named on the child's command line.
        result: The result.
    """
    path.write_text(json.dumps(dataclasses.asdict(result)) + "\n", encoding="utf-8")


def read_result(path: Path) -> ProbeResult:
    """Read the result a probe child wrote.

    Args:
        path: The file `write_result` wrote.

    Returns:
        The result.
    """
    return ProbeResult(**json.loads(path.read_text(encoding="utf-8")))


def probe_in_child(
    *,
    python: str,
    script: Path,
    root: Path,
    environ: Mapping[str, str],
    secrets: SecretFilter,
    limit: timedelta = PROBE_LIMIT,
) -> ProbeResult:
    """Probe the key in a child process, `script _probe --result FILE`, and read its result.

    The child inherits the environment, secrets included, since it probes with them. Its
    last output lines, cleaned of the secrets, are shown only in the error of a failed or
    stopped child. Control-C stops the child (`children.wait`).

    Args:
        python: The child's interpreter.
        script: `scripts/live_run.py`.
        root: The repository root, the child's working directory.
        environ: The environment, secrets included.
        secrets: Cleans the child's output.
        limit: How long the child may run before it is stopped.

    Returns:
        The probe's result.

    Raises:
        ProbeChildError: When the child wrote no result (a missing Java, a server that could
            not start, an exception in the probe), or was stopped at `limit`.
    """
    with tempfile.TemporaryDirectory(prefix="live-run-probe-") as folder:
        path = Path(folder) / "probe.json"
        lines: list[str] = []
        child = children.start(
            [python, str(script), "_probe", "--result", str(path)],
            cwd=root,
            env=environ,
            log=Path(folder) / "probe.log",
            secrets_filter=secrets,
            echo=lines.append,
        )
        deadline = datetime.now(UTC) + limit
        code, stopped = children.wait(child, deadline=deadline, now=lambda: datetime.now(UTC))
        shown = " | ".join(line.strip() for line in lines[-SHOWN_OUTPUT:] if line.strip())
        if stopped:
            msg = f"the probe's child did not end within {limit}: {shown}"
            raise ProbeChildError(msg)
        if code != 0 or not path.exists():
            msg = f"the probe's child exited with status {code}: {shown}"
            raise ProbeChildError(msg)
        return read_result(path)
