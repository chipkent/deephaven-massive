"""The command line of `scripts/tester.py`: check the setup, run the examples, bundle a report.

`scripts/tester.py` calls `main` with the command line. Three commands, each one step of
`docs/tester-guide.md`:

- `check [--folder DIR] [--console-python PATH | --docker-image NAME]` checks both setups and
  the key (`setup_check.check_setup`) and prints one line per item, then `ready: yes` or
  `ready: no`. The console install is the Python at `PATH`
  (`~/massive-test/dh-venv/bin/python` by default) or the Docker image `NAME`. Exit 0 when
  no item failed, else 1.
- `examples [--folder DIR]` starts an embedded Deephaven server, runs every example of
  `docs/tester-examples/` in order (`examples.run_examples`), and saves what each did under
  `DIR/examples-output/`. The examples write their debug log and files to `DIR` too, through
  the `MASSIVE_TEST_FOLDER` variable this command sets. Exit 0 when no example failed (a
  refusal by the key's plan is not a failure), else 1.
- `bug-report [--folder DIR]` writes `DIR/bug-report-<UTC time>.zip` (`bug_report`). Exit 0
  when the zip was written, 1 when a secret was found and nothing was written.

`DIR` is `~/massive-test` unless given. Every command refuses, with exit status 2 and a line
that says how to set it, when `MASSIVE_API_KEY` is not set. The secrets come only from the
environment, and every line printed is cleaned of them.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

from tests._live.live_run.children import run_command
from tests._live.live_run.cli import credentials
from tests._live.live_run.machine_checks import check_machine, ready
from tests._live.live_run.run_probe import probe_key
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.tester.bug_report import build_bundle
from tests._live.tester.setup_check import check_setup

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

    from tests._live.live_run.machine_checks import Item

REFUSED: Final = 2
"""The exit status when the API key is not set."""

FOLDER_VARIABLE: Final = "MASSIVE_TEST_FOLDER"
"""The variable that tells the examples where the tester's folder is."""

EXAMPLES_FOLDER: Final = Path("docs") / "tester-examples"
"""The examples, relative to the repository root."""

CONSOLE_PYTHON: Final = Path("~/massive-test/dh-venv/bin/python")
"""The console install's Python, as the guide makes it."""

NO_KEY: Final = (
    "MASSIVE_API_KEY is not set in this terminal. Set it, then run the command again: "
    "printf 'Massive API key: '; read -rs MASSIVE_API_KEY; echo; export MASSIVE_API_KEY"
)
"""What a command says when the API key is not set."""


def parser() -> argparse.ArgumentParser:
    """Return the command-line parser.

    Returns:
        The parser of `check`, `examples`, and `bug-report`.
    """
    top = argparse.ArgumentParser(
        prog="tester.py", description="The tester guide's tools: setup check, examples, reports."
    )
    commands = top.add_subparsers(dest="command", required=True)
    folder_help = "the tester's folder (default: ~/massive-test)"
    for name, text in (
        ("check", "check both setups and the key; changes nothing"),
        ("examples", "run every example of docs/tester-examples/ and save what each did"),
        ("bug-report", "zip the logs and diagnostics for a bug report, cleaned of secrets"),
    ):
        command = commands.add_parser(name, help=text)
        command.add_argument(
            "--folder", type=Path, default=Path("~/massive-test"), help=folder_help
        )
        if name == "check":
            console = command.add_mutually_exclusive_group()
            console.add_argument(
                "--console-python",
                type=Path,
                default=CONSOLE_PYTHON,
                help="the console install's Python (default: ~/massive-test/dh-venv/bin/python)",
            )
            console.add_argument(
                "--docker-image", default="", help="the console install's Docker image instead"
            )
    return top


def describe_machine(
    root: Path, folder: Path, environ: MutableMapping[str, str]
) -> dict[str, object]:
    """Describe the machine for a bug report, with the live-run program's checks.

    Args:
        root: The repository root, for its commit.
        folder: The tester's folder, for its free disk.
        environ: The environment, for which secrets are set.

    Returns:
        The environment description of `machine_checks.check_machine`.
    """
    _, environment = check_machine(root=root, disk_path=folder, environ=environ, clock_offset=None)
    return environment


def check_both(
    root: Path, arguments: argparse.Namespace, environ: MutableMapping[str, str]
) -> list[Item]:
    """Check both setups and the key on the real machine.

    Args:
        root: The repository root.
        arguments: The parsed `check` command line.
        environ: The environment.

    Returns:
        The items of `setup_check.check_setup`.
    """
    return check_setup(
        root=root,
        folder=arguments.folder.expanduser().resolve(),
        environ=environ,
        console_python=arguments.console_python.expanduser(),
        docker_image=arguments.docker_image,
        run=run_command,
        probe=lambda: probe_key(credentials(environ), now=datetime.now(UTC)),
    )


def main(
    argv: list[str],
    *,
    root: Path,
    start_server: Callable[[], None],
    environ: MutableMapping[str, str] = os.environ,
    stdout: IO[str] = sys.stdout,
    check: Callable[[Path, argparse.Namespace, MutableMapping[str, str]], list[Item]] = (
        check_both
    ),
    describe: Callable[[Path, Path, MutableMapping[str, str]], dict[str, object]] = (
        describe_machine
    ),
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    """Parse the command line and run the command.

    Args:
        argv: The arguments, without the program's name.
        root: The repository root.
        start_server: Starts an embedded Deephaven server; only `examples` calls it.
        environ: The environment; `examples` sets `MASSIVE_TEST_FOLDER` in it.
        stdout: Where the command prints.
        check: Checks both setups and the key (`check_both`).
        describe: Describes the machine for a bug report (`describe_machine`).
        now: Reads the time, which names a bug report's zip.

    Returns:
        The exit status: 0; 1 when an item of the check failed, an example failed, or a
        secret blocked the zip; 2 when the API key is not set.
    """
    arguments = parser().parse_args(argv)
    folder = arguments.folder.expanduser().resolve()
    secrets = SecretFilter.from_environment(environ)

    def echo(line: str) -> None:
        stdout.write(secrets.clean(line) + "\n")
        stdout.flush()

    if not environ.get("MASSIVE_API_KEY", "").strip():
        echo(f"refused: {NO_KEY}")
        return REFUSED
    folder.mkdir(parents=True, exist_ok=True)
    if arguments.command == "check":
        items = check(root, arguments, environ)
        for item in items:
            echo(f"{item.verdict:4} {item.name}: {item.detail}")
        echo(f"ready: {'yes' if ready(items) else 'no'}")
        return 0 if ready(items) else 1
    if arguments.command == "examples":
        environ[FOLDER_VARIABLE] = str(folder)
        start_server()
        examples = importlib.import_module("tests._live.tester.examples")
        outcomes = examples.run_examples(
            examples.example_files(root / EXAMPLES_FOLDER),
            folder / "examples-output",
            secrets=secrets,
            echo=echo,
        )
        failed = [o.name for o in outcomes if o.status == examples.FAILED]
        echo(f"saved to {folder / 'examples-output'}")
        echo(f"failed: {', '.join(failed)}" if failed else "no example failed")
        return 1 if failed else 0
    stamp = now().strftime("%Y%m%dT%H%M%SZ")
    environment = describe(root, folder, environ)
    target, finds = build_bundle(folder, environment=environment, secrets=secrets, stamp=stamp)
    if target is None:
        echo("bug-report refused: a secret was found in these files, so no zip was written:")
        for find in finds:
            echo(f"  {find}")
        return 1
    echo(f"wrote {target}")
    return 0
