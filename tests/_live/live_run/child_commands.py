"""The hidden commands of the live run's child processes: a built-in group, and one probe.

`scripts/live_run.py` runs one of these in the embedded Deephaven server it started, sized by
`server.jvm_args`, and then leaves through `os._exit`. They import the library, and so
Deephaven, so only a child that started a server imports this module; the parent never does.
Every child first watches the parent the script saw before its server started
(`start_parent_watch`), so it ends soon after that parent does.

- `_group --out OUT --attempt FOLDER --token T`: a built-in group's child (`attempts`). It
  opens sessions on the key with the owner's settings, as the live tests do
  (`session_config`; the upgrade check's sessions declare no request limit), and runs the
  group (`group_child`).
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
from deephaven_massive._config import CONFIG_FILE_VARIABLE
from tests._live.live_run import group_child
from tests._live.live_run.probe_result import write_result
from tests._live.live_run.run_probe import credentials, probe_key

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive import MassiveConfig

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


def session_config(environ: Mapping[str, str], *, declare_limits: bool) -> MassiveConfig:
    """Build a child's session configuration from the owner's settings file, as the live tests do.

    When `DEEPHAVEN_MASSIVE_CONFIG_FILE` names the owner's settings file, its
    `delayed_subscriptions`, `rest_requests_per_minute`, and `websocket_hosts` are passed
    through, and every other setting keeps its default; the key comes from the environment.

    Args:
        environ: The environment, which names the settings file.
        declare_limits: Whether the file's `rest_requests_per_minute` is passed through; False
            for the upgrade check, whose sessions must learn the limit from Massive.

    Returns:
        The configuration; the defaults alone without the file.
    """
    path = environ.get(CONFIG_FILE_VARIABLE, "").strip()
    if not path:
        bare: MassiveConfig = dm.MassiveConfig()
        return bare
    settings = dm.MassiveConfig.from_file(path)
    limited = settings.rest_requests_per_minute if declare_limits else {}
    configured: MassiveConfig = dm.MassiveConfig(
        delayed_subscriptions=settings.delayed_subscriptions,
        rest_requests_per_minute=limited,
        websocket_hosts=settings.websocket_hosts,
    )
    return configured


def run_group_child(out: Path, attempt: str, *, environ: Mapping[str, str]) -> None:
    """Be a built-in group's child: run the group.

    Args:
        out: The results folder.
        attempt: The attempt's folder, relative to `out`.
        environ: The environment, which names the owner's settings file.
    """
    request = json.loads((out / attempt / "request.json").read_text(encoding="utf-8"))
    config = session_config(environ, declare_limits=request["group"] != "upgrade-check")
    group_child.run_group(
        out,
        attempt,
        open_session=lambda: dm.MassiveSession(config),
        delayed=frozenset(s.value for s in config.delayed_subscriptions),
    )


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
