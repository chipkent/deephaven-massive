"""The hidden commands of the live run's child processes: a built-in group, and one probe.

`scripts/live_run.py` runs one of these in the embedded Deephaven server it started, sized by
`server.jvm_args`, and then leaves through `os._exit`. They import the library, and so
Deephaven, so only a child that started a server imports this module; the parent never does.
Every child first watches the parent the script saw before its server started
(`start_parent_watch`), so it ends soon after that parent does.

- `_group --out OUT --attempt FOLDER --token T`: a built-in group's child (`attempts`). It
  opens sessions on the key (declaring its Basic subscriptions, except for
  the upgrade check, whose sessions declare nothing), and runs the group (`group_child`).
- `_probe --result FILE`: one probe of the key (`run_probe.probe_key`), its result written to
  `FILE` for the parent to read (`probe_result.probe_in_child`).
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

import deephaven_massive as dm
from tests._live.live_run import group_child
from tests._live.live_run.probe_result import write_result
from tests._live.live_run.run_probe import credentials, probe_key
from tests._live.live_run.stream_units import DELAYED, RECENCY

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive import MassiveSession

PARENT_CHECK: Final = 5.0
"""Seconds between a built-in child's checks that its parent still runs."""


def parser() -> argparse.ArgumentParser:
    """Return the parser of the child commands.

    Returns:
        The parser of `_group` and `_probe`.
    """
    top = argparse.ArgumentParser(prog="live_run.py")
    commands = top.add_subparsers(dest="command", required=True)
    child = commands.add_parser("_group")
    child.add_argument("--out", type=Path, required=True)
    child.add_argument("--attempt", required=True)
    child.add_argument("--token", required=True)
    probe = commands.add_parser("_probe")
    probe.add_argument("--result", type=Path, required=True)
    return top


def run_group_child(
    out: Path,
    attempt: str,
    *,
    environ: Mapping[str, str],
) -> None:
    """Be a built-in group's child: run the group.

    Args:
        out: The results folder.
        attempt: The attempt's folder, relative to `out`.
        environ: The environment, for the probe of the upgrade check.
    """
    request = json.loads((out / attempt / "request.json").read_text(encoding="utf-8"))
    tiers = request["tiers"].items()
    basic = frozenset(dm.Subscription(n) for n, t in tiers if t == "basic")
    # The library serves delayed data only when asked: a delayed plan's streams ask for it.
    delayed = frozenset(dm.Subscription(n) for n, t in tiers if RECENCY.get((n, t)) == DELAYED)

    def open_session() -> MassiveSession:
        config = dm.MassiveConfig(rest_basic_subscriptions=basic, delayed_subscriptions=delayed)
        return dm.MassiveSession(config)

    def tier_of(subscription: str) -> str | None:
        probe = probe_key(credentials(environ), now=datetime.now(UTC))
        return None if probe.tiers is None else probe.tiers[subscription]

    group_child.run_group(out, attempt, open_session=open_session, tier_of=tier_of)


def run_probe_child(result: Path, *, environ: Mapping[str, str]) -> None:
    """Be the probe's child: probe the key once, and write the result for the parent.

    Args:
        result: The file the parent reads (`probe_result.read_result`).
        environ: The environment, whose secrets the probe uses.
    """
    write_result(result, probe_key(credentials(environ), now=datetime.now(UTC)))


def start_parent_watch(parent: int) -> None:
    """Start a thread that ends this child once its parent goes away.

    The child runs in a session of its own, so a closed terminal never reaches it; without
    the watch it would outlive the program that would stop it, holding its server, the key's
    connection, and a folder a resumed run uses again.

    Args:
        parent: The parent's pid, read by the script before the server started: a parent that
            ends while the server starts leaves the child a new parent, which this pid tells
            apart.
    """
    threading.Thread(
        target=watch_parent,
        args=(parent,),
        kwargs={"orphaned": lambda: os._exit(3), "wait": threading.Event().wait},
        name="live run parent watch",
        daemon=True,
    ).start()


def watch_parent(
    parent: int, *, orphaned: Callable[[], None], wait: Callable[[float], bool]
) -> None:
    """Call `orphaned` once the process's parent is no longer `parent`.

    Args:
        parent: The parent's pid when the script started.
        orphaned: Called once, when the parent is gone.
        wait: Waits a number of seconds between checks.
    """
    while os.getppid() == parent:
        wait(PARENT_CHECK)
    orphaned()


def main(
    argv: list[str],
    *,
    parent: int,
    environ: Mapping[str, str] = os.environ,
    watch: Callable[[int], None] = start_parent_watch,
) -> int:
    """Parse a child's command line and run it.

    Args:
        argv: The arguments, without the program's name.
        parent: The parent's pid, read by the script before its server started.
        environ: The environment, secrets included.
        watch: Starts the child's watch of `parent` (`start_parent_watch`).

    Returns:
        0 once the command finished; 2 when the parser refused the command line, after the
        usage and the error, so the script prints no traceback.
    """
    try:
        arguments = parser().parse_args(argv)
    except SystemExit as stop:
        return int(stop.code or 0)
    watch(parent)
    if arguments.command == "_group":
        out = arguments.out.expanduser().resolve()
        run_group_child(out, arguments.attempt, environ=environ)
    else:
        run_probe_child(arguments.result, environ=environ)
    return 0
