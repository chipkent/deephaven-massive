"""Tests of the command line of `scripts/tester.py` (`tests/_live/tester/cli.py`).

The commands run with their machine-facing parts replaced (the check, the machine's
description, the server start), or, for `examples`, on a small folder of examples in a
temporary copy of the repository's layout, so nothing reaches Massive.
"""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests._live.live_run.commands import NO_KEY
from tests._live.live_run.machine_checks import FAIL, OK, Item
from tests._live.live_run.run_probe import ProbeResult
from tests._live.tester import cli

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable, MutableMapping

KEY = "PLANTED-cli-key-5d21"
ROOT = Path(__file__).resolve().parents[3]


def _no_server() -> None:
    pytest.fail("only examples starts a server")


def _describe(_root: Path, _folder: Path, _environ: MutableMapping[str, str]) -> dict[str, object]:
    return {"os": "test"}


def _check(
    _root: Path, _arguments: argparse.Namespace, _environ: MutableMapping[str, str]
) -> list[Item]:
    return [Item("java", OK, "Java 21")]


def _run(
    argv: list[str],
    environ: dict[str, str],
    *,
    root: Path = ROOT,
    start_server: Callable[[], None] = _no_server,
    check: Callable[[Path, argparse.Namespace, MutableMapping[str, str]], list[Item]] = _check,
    plans: Callable[[MutableMapping[str, str]], dict[str, str] | None] = lambda _e: None,
) -> tuple[int, list[str]]:
    out = io.StringIO()
    status = cli.main(
        argv,
        root=root,
        start_server=start_server,
        environ=environ,
        stdout=out,
        check=check,
        describe=_describe,
        now=lambda: datetime(2026, 10, 30, 14, 15, tzinfo=UTC),
        plans=plans,
    )
    return status, out.getvalue().splitlines()


@pytest.mark.parametrize("command", ["check", "examples", "bug-report"])
def test_every_command_refuses_without_the_key(command: str, tmp_path: Path) -> None:
    status, lines = _run([command, "--folder", str(tmp_path)], {"MASSIVE_API_KEY": " "})
    assert status == cli.REFUSED
    assert lines == [f"refused: {NO_KEY}"]


@pytest.mark.parametrize(("argv", "status"), [(["--help"], 0), (["chek"], 2)])
def test_help_and_a_refused_command_line_return_their_status(
    argv: list[str], status: int, capsys: pytest.CaptureFixture[str]
) -> None:
    # The script exits through os._exit, so argparse's SystemExit must come back as a status,
    # not reach the script as an exception it prints with a traceback.
    assert _run(argv, {"MASSIVE_API_KEY": "k"})[0] == status
    printed = capsys.readouterr()
    assert "usage: tester.py" in printed.out + printed.err
    assert "Traceback" not in printed.out + printed.err


def test_check_prints_each_item_and_whether_the_setup_is_ready(tmp_path: Path) -> None:
    seen: list[argparse.Namespace] = []

    def check(
        root: Path, arguments: argparse.Namespace, environ: MutableMapping[str, str]
    ) -> list[Item]:
        seen.append(arguments)
        return [Item("java", OK, "Java 21"), Item("key", OK, f"accepted {KEY}")]

    environ = {"MASSIVE_API_KEY": KEY}
    status, lines = _run(
        ["check", "--folder", str(tmp_path), "--docker-image", "img"], environ, check=check
    )
    assert status == 0
    assert lines == ["OK   java: Java 21", "OK   key: accepted ***", "ready: yes"]
    # The bug report collects the same lines, cleaned of the key.
    saved = (tmp_path / cli.CHECK_OUTPUT).read_text(encoding="utf-8")
    assert saved == "\n".join(lines) + "\n"
    assert seen[0].docker_image == "img"


def test_check_says_not_ready_when_an_item_failed(tmp_path: Path) -> None:
    def check(
        root: Path, arguments: argparse.Namespace, environ: MutableMapping[str, str]
    ) -> list[Item]:
        return [Item("console", FAIL, "missing")]

    status, lines = _run(
        ["check", "--folder", str(tmp_path)], {"MASSIVE_API_KEY": KEY}, check=check
    )
    assert (status, lines[-1]) == (1, "ready: no")


def test_check_both_runs_the_setup_check_on_this_machine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    accepted = ProbeResult(when="t", tiers={"stocks": "basic"}, history_start={"stocks": "x"})
    monkeypatch.setattr(cli, "probe_key", lambda *_args, **_options: accepted)
    arguments = cli.parser().parse_args(
        ["check", "--folder", str(tmp_path), "--console-python", "/nowhere/python"]
    )
    items = cli.check_both(ROOT, arguments, {"MASSIVE_API_KEY": KEY})
    names = [item.name for item in items]
    assert "console" in names
    assert "plan stocks" in names


def test_the_parser_defaults_to_the_guides_console_python() -> None:
    arguments = cli.parser().parse_args(["check"])
    assert arguments.console_python == cli.CONSOLE_PYTHON
    assert arguments.docker_image == ""


def test_examples_runs_the_folder_and_reports_the_failures(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    examples = root / cli.EXAMPLES_FOLDER
    examples.mkdir(parents=True)
    (examples / "01_a.py").write_text("print('ok')\n", encoding="utf-8")
    (examples / "02_b.py").write_text("raise RuntimeError('broke')\n", encoding="utf-8")
    started: list[bool] = []
    environ = {"MASSIVE_API_KEY": KEY}
    folder = tmp_path / "massive-test"
    status, lines = _run(
        ["examples", "--folder", str(folder)],
        environ,
        root=root,
        start_server=lambda: started.append(True),
    )
    assert status == 1
    assert started == [True]
    assert environ[cli.FOLDER_VARIABLE] == str(folder.resolve())
    assert lines[-2:] == [f"saved to {folder.resolve() / 'examples-output'}", "failed: 02_b.py"]
    assert (folder / "examples-output" / "01_a.txt").read_text() == "ok\n"


def test_examples_says_when_nothing_failed(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    (root / cli.EXAMPLES_FOLDER).mkdir(parents=True)
    (root / cli.EXAMPLES_FOLDER / "01_a.py").write_text("pass\n", encoding="utf-8")
    status, lines = _run(
        ["examples", "--folder", str(tmp_path / "t")],
        {"MASSIVE_API_KEY": KEY},
        root=root,
        start_server=lambda: None,
    )
    assert (status, lines[-1]) == (0, "no example failed")


def test_bug_report_writes_the_zip(tmp_path: Path) -> None:
    (tmp_path / "massive-debug.log").write_text(f"key {KEY}\n", encoding="utf-8")
    status, lines = _run(
        ["bug-report", "--folder", str(tmp_path)],
        {"MASSIVE_API_KEY": KEY},
    )
    target = tmp_path / "bug-report-20261030T141500Z.zip"
    assert (status, lines) == (0, [f"wrote {target}"])
    with zipfile.ZipFile(target) as archive:
        assert archive.read("massive-debug.log") == b"key ***\n"


def test_bug_report_refuses_when_a_secret_survives(tmp_path: Path) -> None:
    (tmp_path / "server.log").write_text("UExBTlRFRC1jbGkta2V5LTVkMjE=\n", encoding="utf-8")
    status, lines = _run(
        ["bug-report", "--folder", str(tmp_path)],
        {"MASSIVE_API_KEY": KEY},
    )
    assert status == 1
    assert lines == [
        "bug-report refused: a secret was found in these files, so no zip was written:",
        "  server.log: line 1",
    ]


def test_describe_machine_describes_this_machine(tmp_path: Path) -> None:
    environment = cli.describe_machine(ROOT, tmp_path, {"MASSIVE_API_KEY": KEY})
    assert environment["set"] == {
        "MASSIVE_API_KEY": True,
        "MASSIVE_DELAYED_API_KEY": False,
        "MASSIVE_S3_ACCESS_KEY_ID": False,
        "MASSIVE_S3_SECRET_ACCESS_KEY": False,
        "HTTPS_PROXY": False,
        "SSL_CERT_FILE": False,
    }


def test_examples_writes_the_settings_the_keys_plans_need(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    (root / cli.EXAMPLES_FOLDER).mkdir(parents=True)
    (root / cli.EXAMPLES_FOLDER / "01_a.py").write_text("pass\n", encoding="utf-8")
    tiers = {"stocks": "starter", "options": "basic"}
    status, lines = _run(
        ["examples", "--folder", str(tmp_path / "t")],
        {"MASSIVE_API_KEY": KEY},
        root=root,
        start_server=lambda: None,
        plans=lambda _environ: tiers,
    )
    assert status == 0
    assert lines[0] == "settings: delayed_subscriptions: [stocks]"
    settings = tmp_path / "t" / "examples-output" / "settings.yaml"
    assert settings.read_text(encoding="utf-8") == "delayed_subscriptions: [stocks]\n"


def test_examples_runs_as_on_the_free_plan_when_the_plans_are_unknown(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    (root / cli.EXAMPLES_FOLDER).mkdir(parents=True)
    (root / cli.EXAMPLES_FOLDER / "01_a.py").write_text("pass\n", encoding="utf-8")
    _, lines = _run(
        ["examples", "--folder", str(tmp_path / "t")],
        {"MASSIVE_API_KEY": KEY},
        root=root,
        start_server=lambda: None,
    )
    assert lines[:2] == [
        "could not tell the key's plans; the examples run as on the free plan",
        "settings: delayed_subscriptions: []",
    ]
