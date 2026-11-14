"""What a live-run command needs from the world, and what one `run` keeps in memory.

Every command of the live-run program (`live_run`) works through a `RunContext`: the results
folder, the repository, the environment and its secret filter, the probe, the machine checks,
the clock, and the terminal. `scripts/live_run.py` builds it from the real machine; the unit
tests build it with fakes, so the whole program runs against the fake servers and a fake
clock. `RunMemory` holds what one `run` remembers between steps without writing it down: the
latest probe and its retries, and the pause before the next group.

Only the program's main thread uses either.
"""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tests._live.calendar import NEW_YORK
from tests._live.live_run.children import STOP_GRACE

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tests._live.live_run.machine_checks import Item
    from tests._live.live_run.probe_result import ProbeResult
    from tests._live.live_run.secret_scrub import SecretFilter


class RunRefusedError(Exception):
    """A command that cannot go on: another run holds the folder, another commit, a bad key.

    Raised by the commands; `scripts/live_run.py` prints its message and exits with status 2.
    """


def _no_sleep(_moment: datetime) -> None:
    """Return at once; the real program sleeps with `children.sleep_until`."""


def _nothing() -> None:
    """Do nothing: the keep-awake stopper when nothing keeps the machine awake."""


def _no_echo(_line: str) -> None:
    """Print nothing; the real program prints to the terminal."""


@dataclass
class RunContext:
    """What a command needs from the world, so tests can stand in for each part.

    Created once per command by `scripts/live_run.py`, or by a test with fakes.

    Attributes:
        out: The results folder.
        root: The repository root, every child's working directory.
        environ: The environment, secrets included; never written.
        secrets: The filter applied to everything printed and written.
        commit: The checkout's commit, which a resumed run must match.
        probe: Probes the key now (`probe_result.probe_in_child`).
        machine: Runs the machine checks for a folder, given the probe's clock offset
            (`machine_checks.check_machine`).
        keep_awake: Starts keeping the machine awake, and returns the function that stops it.
        now: The current time, aware.
        sleep_until: Sleeps until an aware time.
        python: The interpreter of every child.
        script: `scripts/live_run.py`, the built-in groups' child.
        echo: Prints one line to the terminal.
        read_enter: Waits until the person presses Enter.
        stop_grace: Seconds a stopped child gets before it is killed.
        free_disk: The free bytes on the disk that holds a folder.
    """

    out: Path
    root: Path
    environ: Mapping[str, str]
    secrets: SecretFilter
    commit: str
    probe: Callable[[], ProbeResult]
    machine: Callable[[Path, float | None], tuple[list[Item], dict[str, object]]]
    keep_awake: Callable[[], Callable[[], None]] = lambda: _nothing
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    sleep_until: Callable[[datetime], None] = _no_sleep
    python: str = sys.executable
    script: Path = Path("scripts/live_run.py")
    echo: Callable[[str], None] = _no_echo
    read_enter: Callable[[], str] = input
    stop_grace: float = STOP_GRACE
    free_disk: Callable[[Path], int] = lambda folder: shutil.disk_usage(folder).free

    def say(self, line: str) -> None:
        """Print a line stamped with the New York time, and append it to `run.log`.

        Both are cleaned of the secrets first. The log is opened for each line, so a crash
        loses nothing and no file stays open.

        Args:
            line: The line.
        """
        stamped = self.secrets.clean(f"{self.now().astimezone(NEW_YORK):%H:%M %Z} {line}")
        self.echo(stamped)
        self.out.mkdir(parents=True, exist_ok=True)
        with (self.out / "run.log").open("a", encoding="utf-8") as log:
            log.write(stamped + "\n")


@dataclass
class RunMemory:
    """What one `run` keeps in memory beside the state file.

    Attributes:
        latest_probe: The latest probe that could tell the tiers, as a record.
        probed_at: When the latest probe ran, whatever its answer.
        retries: Probes in a row that could not tell the tiers.
        not_before: No group starts before this (the pause after a probe).
        reminded: The New York date the flat-files reminder was last printed, ISO.
        finished_since_pack: Whether an attempt finished since the last pack.
    """

    latest_probe: dict[str, Any] | None = None
    probed_at: datetime | None = None
    retries: int = 0
    not_before: datetime | None = None
    reminded: str = ""
    finished_since_pack: bool = False
