"""The live-run program's command line, and its wiring to the real machine.

`scripts/live_run.py` calls `main` with the command line. `main` parses it, builds a
`RunContext` on the real machine (`real_context`: the environment's secrets, the probe on
Massive's servers, the machine checks, the checkout's commit, the keep-awake tool, the real
clock and terminal), and runs one command of `live_run`. A refused command prints why and
exits 2.

This is the parent's side, which starts no JVM: it imports neither the library nor Deephaven,
and runs everything that does in a child of its own script (`child_commands`): each probe of
the key (`probe_result.probe_in_child`) and each built-in group (`attempts`).
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

from tests._live.live_run import children, commands, machine_checks
from tests._live.live_run.context import RunContext, RunRefusedError
from tests._live.live_run.probe_result import probe_in_child
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.live_run.stream_units import SUBSCRIPTIONS

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

GROUPS: Final = (
    "rate-limits",
    "evidence",
    "luld",
    "smoke",
    "responses",
    "limits-ws",
    "limits-second-connection",
    "stream-day",
    "throughput",
    "stream-day-reconcile",
    "stream-night",
    "stream-night-reconcile",
    "half-day",
)
"""The groups `--redo` accepts."""

REFUSED: Final = 2
"""The exit status of a refused command."""


def nearest_folder(folder: Path) -> Path:
    """Return a folder, or its nearest ancestor that exists, for the free-disk check.

    Args:
        folder: The results folder, which may not exist yet.

    Returns:
        The nearest existing folder.
    """
    while not folder.exists():
        folder = folder.parent
    return folder


def real_context(
    out: Path, *, root: Path, script: Path, environ: Mapping[str, str], stdout: IO[str]
) -> RunContext:
    """Build a command's context on the real machine.

    Args:
        out: The results folder.
        root: The repository root.
        script: `scripts/live_run.py`.
        environ: The environment, secrets included.
        stdout: Where the program prints.

    Returns:
        The context.
    """
    secrets = SecretFilter.from_environment(environ)
    git = shutil.which("git") or "git"

    def echo(line: str) -> None:
        stdout.write(line + "\n")
        stdout.flush()

    def keep_awake() -> Callable[[], None]:
        argv = children.keep_awake_argv(platform.system(), pid=os.getpid(), which=shutil.which)
        if argv is None:
            echo("warning: nothing keeps the machine awake; keep it awake yourself")
        out.mkdir(parents=True, exist_ok=True)
        return children.keep_awake(argv, log=out / "keep-awake.log", secrets_filter=secrets)

    return RunContext(
        out=out,
        root=root,
        environ=environ,
        secrets=secrets,
        commit=children.run_command([git, "-C", str(root), "rev-parse", "HEAD"]).strip(),
        probe=lambda: probe_in_child(
            python=sys.executable, script=script, root=root, environ=environ, secrets=secrets
        ),
        machine=lambda folder, offset: machine_checks.check_machine(
            root=root, disk_path=nearest_folder(folder), environ=environ, clock_offset=offset
        ),
        keep_awake=keep_awake,
        sleep_until=lambda moment: children.sleep_until(moment, now=lambda: datetime.now(UTC)),
        script=script,
        echo=echo,
    )


def parser() -> argparse.ArgumentParser:
    """Return the command-line parser.

    Returns:
        The parser of `check`, `run`, `status`, `pack`, and `upgrade-check`; the hidden
        commands of a child have their own (`child_commands.parser`).
    """
    top = argparse.ArgumentParser(
        prog="live_run.py",
        description="Run every live check the key's subscriptions allow, and record the results.",
    )
    commands = top.add_subparsers(
        dest="command", required=True, metavar="{check,run,status,pack,upgrade-check}"
    )
    out_help = "the results folder, such as ~/massive-live"
    check = commands.add_parser("check", help="check the machine and probe the key; writes nothing")
    check.add_argument(
        "--out",
        type=Path,
        default=Path.home(),
        help="where the free disk is checked (default: your home folder)",
    )
    run = commands.add_parser("run", help="run every check by the timetable, until stopped")
    run.add_argument("--out", type=Path, required=True, help=out_help)
    run.add_argument(
        "--any-time", action="store_true", help="run now the groups that need no market hours"
    )
    run.add_argument(
        "--redo",
        default="",
        help="comma-separated groups whose checks all run again: " + ", ".join(GROUPS),
    )
    status = commands.add_parser("status", help="print the latest probe and every check")
    status.add_argument("--out", type=Path, required=True, help=out_help)
    pack = commands.add_parser("pack", help="zip the results to send to the owner")
    pack.add_argument("--out", type=Path, required=True, help=out_help)
    upgrade = commands.add_parser(
        "upgrade-check", help="the attended upgrade check; only when the owner asks"
    )
    upgrade.add_argument(
        "subscription", choices=SUBSCRIPTIONS, help="the subscription you will upgrade"
    )
    upgrade.add_argument("--out", type=Path, required=True, help=out_help)
    return top


def main(
    argv: list[str],
    *,
    root: Path,
    script: Path,
    environ: Mapping[str, str] = os.environ,
    stdout: IO[str] = sys.stdout,
    make_context: Callable[..., RunContext] = real_context,
) -> int:
    """Parse the command line and run the command.

    Args:
        argv: The arguments, without the program's name.
        root: The repository root.
        script: `scripts/live_run.py`.
        environ: The environment.
        stdout: Where the program prints.
        make_context: Builds the context (`real_context`); tests pass fakes.

    Returns:
        The exit status: 0, 1 when a check, a pack, or an upgrade check failed, 2 when the
        command was refused, 130 after Control-C. `--help` returns 0 after the usage, and a
        command line the parser refuses returns 2 after the usage and the error, so the script
        prints no traceback for either.
    """
    try:
        arguments = parser().parse_args(argv)
    except SystemExit as stop:
        return int(stop.code or 0)
    out = arguments.out.expanduser().resolve()
    ctx = make_context(out, root=root, script=script, environ=environ, stdout=stdout)
    actions: dict[str, Callable[[], int]] = {
        "check": lambda: commands.command_check(ctx),
        "run": lambda: commands.command_run(
            ctx, any_time=arguments.any_time, redo=_redo(arguments.redo)
        ),
        "status": lambda: commands.command_status(ctx),
        "pack": lambda: commands.command_pack(ctx),
        "upgrade-check": lambda: commands.command_upgrade_check(
            ctx, getattr(arguments, "subscription", "")
        ),
    }
    try:
        return actions[arguments.command]()
    except RunRefusedError as refused:
        ctx.echo(f"refused: {refused}")
        return REFUSED


def _redo(text: str) -> tuple[str, ...]:
    """Read `--redo`.

    Args:
        text: Comma-separated group names.

    Returns:
        The groups.

    Raises:
        RunRefusedError: When one names no group.
    """
    groups = tuple(g.strip() for g in text.split(",") if g.strip())
    unknown = sorted(set(groups) - set(GROUPS))
    if unknown:
        msg = f"--redo names no group: {', '.join(unknown)}; the groups: {', '.join(GROUPS)}"
        raise RunRefusedError(msg)
    return groups
