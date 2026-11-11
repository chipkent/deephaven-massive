"""The live-run program's command line, and its wiring to the real machine.

`scripts/live_run.py` calls `main` with the command line. `main` parses it, builds a
`RunContext` on the real machine (`real_context`: the environment's secrets, the probe on
Massive's servers, the machine checks, the checkout's commit, the keep-awake tool, the real
clock and terminal), and runs one command of `live_run`. A refused command prints why and
exits 2.

The hidden `_group` command is the child of a built-in group (`attempts`): in the embedded
Deephaven server the script started, sized to the group (`server.jvm_args`), it opens sessions
on the key (declaring its Basic subscriptions,
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
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

import deephaven_massive as dm
from tests._live.live_run import children, commands, group_child, machine_checks
from tests._live.live_run.context import RunContext, RunRefusedError
from tests._live.live_run.run_probe import Credentials, ProbeResult, probe_key
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.live_run.stream_units import DELAYED, RECENCY
from tests._live.live_run.upgrade_check import SUBSCRIPTIONS

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive import MassiveSession

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
    out: Path,
    attempt: str,
    *,
    environ: Mapping[str, str],
    watch: Callable[[], None],
) -> None:
    """Be a built-in group's child: watch the parent, then run the group.

    Args:
        out: The results folder.
        attempt: The attempt's folder, relative to `out`.
        environ: The environment, for the probe of the upgrade check.
        watch: Starts watching the parent (`start_parent_watch`).
    """
    watch()
    request = json.loads((out / attempt / "request.json").read_text(encoding="utf-8"))
    tiers = request["tiers"].items()
    basic = frozenset(dm.Subscription(n) for n, t in tiers if t == "basic")
    # The library serves delayed data only when asked: a delayed plan's streams ask for it.
    delayed = frozenset(dm.Subscription(n) for n, t in tiers if RECENCY.get((n, t)) == DELAYED)

    def open_session() -> MassiveSession:
        config = dm.MassiveConfig(rest_basic_subscriptions=basic, delayed_subscriptions=delayed)
        return dm.MassiveSession(config)

    def tier_of(subscription: str) -> str | None:
        probe: ProbeResult = probe_key(credentials(environ), now=datetime.now(UTC))
        return None if probe.tiers is None else probe.tiers[subscription]

    group_child.run_group(out, attempt, open_session=open_session, tier_of=tier_of)


PARENT_CHECK: Final = 5.0
"""Seconds between a built-in child's checks that its parent still runs."""


def start_parent_watch() -> None:
    """Start a thread that ends this child at once if its parent goes away.

    The child runs in a session of its own, so a closed terminal never reaches it; without
    the watch it would outlive the program that would stop it, holding the key's connection
    and writing into a folder a resumed run uses again.
    """
    threading.Thread(
        target=watch_parent,
        args=(os.getppid(),),
        kwargs={"orphaned": lambda: os._exit(3), "wait": threading.Event().wait},
        name="live run parent watch",
        daemon=True,
    ).start()


def watch_parent(
    parent: int, *, orphaned: Callable[[], None], wait: Callable[[float], bool]
) -> None:
    """Call `orphaned` once the process's parent is no longer `parent`.

    Args:
        parent: The parent's pid when the child started.
        orphaned: Called once, when the parent is gone.
        wait: Waits a number of seconds between checks.
    """
    while os.getppid() == parent:
        wait(PARENT_CHECK)
    orphaned()


def main(
    argv: list[str],
    *,
    root: Path,
    script: Path,
    environ: Mapping[str, str] = os.environ,
    stdout: IO[str] = sys.stdout,
    make_context: Callable[..., RunContext] = real_context,
    watch: Callable[[], None] = start_parent_watch,
) -> int:
    """Parse the command line and run the command.

    Args:
        argv: The arguments, without the program's name.
        root: The repository root.
        script: `scripts/live_run.py`.
        environ: The environment.
        stdout: Where the program prints.
        make_context: Builds the context (`real_context`); tests pass fakes.
        watch: Starts a `_group` child's watch of its parent (`start_parent_watch`).

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
    if arguments.command == "_group":
        run_group_child(out, arguments.attempt, environ=environ, watch=watch)
        return 0
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
