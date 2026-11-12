"""The live-run program's parent, run on the real context with fake children, in a fresh process.

A test of `tests/_live/live_run/cli.py` runs this with `python -m tests._fakes.live_parent`
from the repository root, to see which modules the parent imports while it runs: the test's
own process already runs a JVM, so only a fresh interpreter can tell. It runs `run --out OUT
--any-time` through `cli.main` on the real context (`cli.real_context`), with three parts
replaced so nothing reaches Massive or the machine: every child, the probe's included, runs
under the interpreter `PYTHON` (the test's wrapper of `live_child.py`, scripted by
`FAKE_LIVE_CHILD`); nothing keeps the machine awake; and the second sleep stops the run, as
Control-C would. `OUT` and `PYTHON` come from the variables `LIVE_PARENT_OUT` and
`LIVE_PARENT_PYTHON`. It then prints one JSON object: the exit status, and every module of
Deephaven, `jpy`, or the library the process imported.
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

from tests._live.live_run import cli
from tests._live.live_run.probe_result import probe_in_child

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from tests._live.live_run.context import RunContext

ROOT: Final = Path(__file__).resolve().parents[2]
"""The repository root."""

WATCHED: Final = frozenset({"deephaven", "deephaven_server", "deephaven_massive", "jpy"})
"""The top-level packages the parent must never import."""

SLEEPS: Final = 2
"""The sleep that stops the run."""


def main() -> int:
    """Run the parent, and print its status and the watched modules it imported.

    Returns:
        0; the run's own status is in the printed object.
    """
    out, python = Path(os.environ["LIVE_PARENT_OUT"]), os.environ["LIVE_PARENT_PYTHON"]
    script = ROOT / "scripts" / "live_run.py"
    sleeps: list[datetime] = []

    def sleep_until(moment: datetime) -> None:
        sleeps.append(moment)
        if len(sleeps) >= SLEEPS:
            raise KeyboardInterrupt

    def make_context(
        folder: Path, *, root: Path, script: Path, environ: Mapping[str, str], stdout: IO[str]
    ) -> RunContext:
        ctx = cli.real_context(folder, root=root, script=script, environ=environ, stdout=stdout)
        return dataclasses.replace(
            ctx,
            python=python,
            probe=lambda: probe_in_child(
                python=python, script=script, root=root, environ=environ, secrets=ctx.secrets
            ),
            keep_awake=lambda: lambda: None,
            sleep_until=sleep_until,
        )

    status = cli.main(
        ["run", "--out", str(out), "--any-time"],
        root=ROOT,
        script=script,
        environ=os.environ,
        make_context=make_context,
    )
    imported = sorted(m for m in sys.modules if m.split(".")[0] in WATCHED)
    sys.stdout.write(json.dumps({"status": status, "imported": imported}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
