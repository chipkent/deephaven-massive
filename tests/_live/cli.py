"""The live-run program's command line, and its wiring to the real machine.

`scripts/live_run.py` calls `main` with the command line. `main` parses it, builds a
`RunContext` on the real machine (`real_context`: the environment's secrets, the probe on
Massive's servers, the machine checks, the checkout's commit, the keep-awake tool, the real
clock and terminal), and runs one command of `live_run`. A refused command prints why and
exits 2.

The hidden `_group` command is the child of a built-in group (`attempts`): it starts an
embedded Deephaven server, opens sessions on the key (declaring its Basic subscriptions,
except for the upgrade check, whose sessions declare nothing), and runs the group
(`group_child`). The script then leaves through `os._exit`.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

import deephaven_massive as dm
from tests._live import children, group_child, live_run, machine_checks
from tests._live.run_context import RunContext, RunRefusedError
from tests._live.run_probe import Credentials, ProbeResult, probe_key
from tests._live.secret_scrub import SecretFilter
from tests._live.upgrade_check import SUBSCRIPTIONS

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive import MassiveSession

GROUPS: Final = (
    "evidence",
    "luld",
    "smoke",
    "responses",
    "limits-ws",
    "limits-second-connection",
    "stream-day",
    "throughput",
    "stream-day-reconcile",
    "half-day",
)
"""The groups `--redo` accepts."""

REFUSED: Final = 2
"""The exit status of a refused command."""

STREAM_HEAP_LIMIT: Final = 16 << 30
SMALL_HEAP: Final = 4 << 30
"""A built-in child's heap: half the memory up to 16 GB for streams, 4 GB for the others."""


def credentials(environ: Mapping[str, str]) -> Credentials:
    """Read the probe's credentials from the environment.

    Args:
        environ: The environment.

    Returns:
        The API key, and the S3 pair when both are set.
    """
    access, secret = (environ.get(name, "").strip() for name in children.S3_VARIABLES)
    return Credentials(
        api_key=environ.get("MASSIVE_API_KEY", "").strip(),
        s3_pair=(access, secret) if access and secret else None,
    )


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
        probe=lambda: probe_key(credentials(environ), now=datetime.now(UTC)),
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
        The parser of `check`, `run`, `status`, `pack`, `upgrade-check`, and the hidden
        `_group`.
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
    child = commands.add_parser("_group")
    child.add_argument("--out", type=Path, required=True)
    child.add_argument("--attempt", required=True)
    child.add_argument("--token", required=True)
    return top


def run_group_child(
    out: Path, attempt: str, *, environ: Mapping[str, str], start_server: Callable[[int], None]
) -> None:
    """Be a built-in group's child: start the server, then run the group.

    Args:
        out: The results folder.
        attempt: The attempt's folder, relative to `out`.
        environ: The environment, for the probe of the upgrade check.
        start_server: Starts an embedded Deephaven server with a heap of so many bytes.
    """
    request = json.loads((out / attempt / "request.json").read_text(encoding="utf-8"))
    group = request["group"]
    memory = machine_checks.memory_total() or SMALL_HEAP * 2
    streams = group in {"stream-day", "throughput"}
    start_server(min(memory // 2, STREAM_HEAP_LIMIT) if streams else SMALL_HEAP)
    basic = frozenset(dm.Subscription(n) for n, t in request["tiers"].items() if t == "basic")

    def open_session() -> MassiveSession:
        return dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=basic))

    def tier_of(subscription: str) -> str | None:
        probe: ProbeResult = probe_key(credentials(environ), now=datetime.now(UTC))
        return None if probe.tiers is None else probe.tiers[subscription]

    group_child.run_group(out, attempt, open_session=open_session, tier_of=tier_of)


def main(
    argv: list[str],
    *,
    root: Path,
    script: Path,
    environ: Mapping[str, str] = os.environ,
    stdout: IO[str] = sys.stdout,
    start_server: Callable[[int], None],
    make_context: Callable[..., RunContext] = real_context,
) -> int:
    """Parse the command line and run the command.

    Args:
        argv: The arguments, without the program's name.
        root: The repository root.
        script: `scripts/live_run.py`.
        environ: The environment.
        stdout: Where the program prints.
        start_server: Starts an embedded Deephaven server with a heap of so many bytes; only
            `_group` calls it.
        make_context: Builds the context (`real_context`); tests pass fakes.

    Returns:
        The exit status: 0, 1 when a check, a pack, or an upgrade check failed, 2 when the
        command was refused, 130 after Control-C.
    """
    arguments = parser().parse_args(argv)
    out = arguments.out.expanduser().resolve()
    if arguments.command == "_group":
        run_group_child(out, arguments.attempt, environ=environ, start_server=start_server)
        return 0
    ctx = make_context(out, root=root, script=script, environ=environ, stdout=stdout)
    commands: dict[str, Callable[[], int]] = {
        "check": lambda: live_run.command_check(ctx),
        "run": lambda: live_run.command_run(
            ctx, any_time=arguments.any_time, redo=_redo(arguments.redo)
        ),
        "status": lambda: live_run.command_status(ctx),
        "pack": lambda: live_run.command_pack(ctx),
        "upgrade-check": lambda: live_run.command_upgrade_check(
            ctx, getattr(arguments, "subscription", "")
        ),
    }
    try:
        return commands[arguments.command]()
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
