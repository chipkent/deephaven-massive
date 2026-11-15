"""Tests of the live-run program's command line (`tests/_live/live_run/cli.py`).

The commands run on a context of fakes or on the real context with its probe and machine
checks replaced, so nothing reaches Massive. The parent's own imports are checked in a fresh
interpreter, since this test process already runs a JVM.
"""

from __future__ import annotations

import dataclasses
import io
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests._live.live_run import children, cli, commands, machine_checks
from tests._live.live_run import state as run_state
from tests._live.live_run.context import RunContext, RunRefusedError
from tests._live.live_run.probe_result import ProbeResult
from tests._live.live_run.secret_scrub import SecretFilter

if TYPE_CHECKING:
    from tests._live.live_run.machine_checks import Item

KEY = "PLANTED-cli-key-6b0e"
S3_PAIR = ("PLANTED-cli-access", "PLANTED-cli-s3-value")
ROOT = Path(__file__).resolve().parents[3]


def _probe() -> ProbeResult:
    return ProbeResult(when="2026-11-04T17:00:00Z", tiers=None, error="not used")


def _machine(_folder: Path, _offset: float | None) -> tuple[list[Item], dict[str, object]]:
    return [], {}


class Recorder:
    """Builds a fake context per command, and remembers what it was asked for."""

    def __init__(self) -> None:
        self.contexts: list[RunContext] = []
        self.lines: list[str] = []

    def __call__(self, out: Path, **_options: object) -> RunContext:
        ctx = RunContext(
            out=out,
            root=ROOT,
            environ={},
            secrets=SecretFilter(()),
            commit="c",
            probe=_probe,
            machine=_machine,
            echo=self.lines.append,
        )
        self.contexts.append(ctx)
        return ctx


def _main(argv: list[str], recorder: Recorder) -> int:
    return cli.main(
        argv,
        root=ROOT,
        script=ROOT / "scripts" / "live_run.py",
        make_context=recorder,
    )


@pytest.mark.parametrize(("argv", "status"), [(["--help"], 0), (["run", "--bogus"], 2)])
def test_help_and_a_refused_command_line_return_their_status(
    argv: list[str], status: int, capsys: pytest.CaptureFixture[str]
) -> None:
    # The script exits through os._exit, so argparse's SystemExit must come back as a status,
    # not reach the script as an exception it prints with a traceback.
    assert _main(argv, Recorder()) == status
    printed = capsys.readouterr()
    assert "usage: live_run.py" in printed.out + printed.err
    assert "Traceback" not in printed.out + printed.err


@pytest.mark.parametrize(
    ("argv", "command", "options"),
    [
        (["check"], "command_check", {}),
        (["status", "--out", "{out}"], "command_status", {}),
        (["pack", "--out", "{out}"], "command_pack", {}),
        (
            ["run", "--out", "{out}", "--any-time", "--redo", "smoke, evidence"],
            "command_run",
            {"any_time": True, "redo": ("smoke", "evidence")},
        ),
        (["run", "--out", "{out}"], "command_run", {"any_time": False, "redo": ()}),
        (
            ["upgrade-check", "options", "--out", "{out}"],
            "command_upgrade_check",
            {"subscription": "options"},
        ),
    ],
)
def test_each_command_reaches_its_function(
    *,
    argv: list[str],
    command: str,
    options: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[tuple[RunContext, dict[str, object]]] = []

    def fake(ctx: RunContext, *args: object, **kwargs: object) -> int:
        called.append((ctx, {"subscription": args[0]} if args else kwargs))
        return 7

    monkeypatch.setattr(commands, command, fake)
    recorder = Recorder()
    assert _main([a.replace("{out}", str(tmp_path / "o")) for a in argv], recorder) == 7
    [(ctx, seen)] = called
    assert seen == options
    expected = Path.home().resolve() if argv == ["check"] else tmp_path.resolve() / "o"
    assert ctx.out == expected


def test_a_refused_command_prints_why_and_exits_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(_ctx: RunContext) -> int:
        raise RunRefusedError("another run is using it")

    monkeypatch.setattr(commands, "command_status", refuse)
    recorder = Recorder()
    assert _main(["status", "--out", str(tmp_path)], recorder) == cli.REFUSED
    assert recorder.lines == ["refused: another run is using it"]


def test_an_unknown_redo_group_is_refused(tmp_path: Path) -> None:
    recorder = Recorder()
    assert (
        _main(["run", "--out", str(tmp_path), "--redo", "evidence,nope"], recorder) == cli.REFUSED
    )
    assert recorder.lines[0].startswith(
        "refused: --redo names no group: nope; the groups: rate-limits, evidence"
    )


def test_the_free_disk_is_read_from_the_nearest_existing_folder(tmp_path: Path) -> None:
    assert cli.nearest_folder(tmp_path / "a" / "b") == tmp_path


def test_the_real_context_wires_the_machine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probes: list[dict[str, object]] = []

    def probe_in_child(**options: object) -> ProbeResult:
        probes.append(options)
        return _probe()

    machines: list[Path] = []

    def check_machine(
        *, root: Path, disk_path: Path, environ: object, clock_offset: float | None
    ) -> tuple[list[Item], dict[str, object]]:
        machines.append(disk_path)
        return [], {"offset": clock_offset}

    monkeypatch.setattr(cli, "probe_in_child", probe_in_child)
    monkeypatch.setattr(machine_checks, "check_machine", check_machine)
    stdout = io.StringIO()
    environ = {"MASSIVE_API_KEY": KEY}
    ctx = cli.real_context(
        tmp_path / "out", root=ROOT, script=Path("s.py"), environ=environ, stdout=stdout
    )
    assert len(ctx.commit) == 40
    assert ctx.secrets.values == (KEY,)
    ctx.probe()
    [options] = probes
    assert (options["python"], options["script"], options["root"]) == (
        sys.executable,
        Path("s.py"),
        ROOT,
    )
    assert options["environ"] == environ
    assert options["secrets"] is ctx.secrets
    assert ctx.machine(tmp_path / "out", 1.5) == ([], {"offset": 1.5})
    assert machines == [tmp_path]
    ctx.echo("hello")
    assert stdout.getvalue() == "hello\n"
    ctx.sleep_until(datetime.now(UTC) - timedelta(seconds=1))


def test_the_real_keep_awake_starts_and_stops_its_tool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeper = [sys.executable, "-c", "import time; time.sleep(60)"]
    tools: list[list[str] | None] = [sleeper, None]
    monkeypatch.setattr(children, "keep_awake_argv", lambda *_args, **_options: tools.pop(0))
    stdout = io.StringIO()
    ctx = cli.real_context(
        tmp_path / "out", root=ROOT, script=Path("s.py"), environ={}, stdout=stdout
    )
    stop = ctx.keep_awake()
    stop()
    assert (tmp_path / "out" / "keep-awake.log").exists()
    ctx.keep_awake()()
    assert stdout.getvalue() == "warning: nothing keeps the machine awake; keep it awake yourself\n"


# --- the parent starts no JVM -----------------------------------------------------------------

WATCHED = ("deephaven", "deephaven_server", "deephaven_massive", "jpy")
"""The packages the parent must never import: Deephaven's, its JVM bridge, and the library."""


def _watched_imports(done: subprocess.CompletedProcess[str]) -> list[str]:
    """Read the modules of `WATCHED` a script run with `-X importtime` imported.

    `-X importtime` prints every module the process imports to its standard error, so the
    script's own `os._exit` hides nothing.

    Returns:
        Those modules, sorted.
    """
    names = [
        line.rsplit("|", 1)[-1].strip()
        for line in done.stderr.splitlines()
        if line.startswith("import time:")
    ]
    # `importlib` imports `cli` unlogged, but `cli` imports `commands` with a statement.
    assert "tests._live.live_run.commands" in names  # the script did reach the parent
    return sorted(n for n in names if n.split(".")[0] in WATCHED)


def test_the_scripts_help_imports_no_deephaven() -> None:
    done = subprocess.run(
        [sys.executable, "-X", "importtime", "scripts/live_run.py", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "usage: live_run.py" in done.stdout
    assert _watched_imports(done) == []


def test_the_scripts_commands_import_no_deephaven() -> None:
    # Without the key, `check` is refused once the parent has imported every command's module.
    environ = {k: v for k, v in os.environ.items() if k != "MASSIVE_API_KEY"}
    done = subprocess.run(
        [sys.executable, "-X", "importtime", "scripts/live_run.py", "check"],
        cwd=ROOT,
        env=environ,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == cli.REFUSED, done.stdout + done.stderr
    assert "MASSIVE_API_KEY is not set" in done.stdout
    assert _watched_imports(done) == []


def test_a_run_imports_no_deephaven_while_it_probes_and_runs_groups(tmp_path: Path) -> None:
    wrapper = tmp_path / "python"
    child = ROOT / "tests" / "_fakes" / "live_child.py"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{child}" "$@"\n')
    wrapper.chmod(0o755)
    # No subscription on Basic, so no pause after the probe holds the first group back.
    tiers = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "starter")
    probe = ProbeResult(when="2026-11-04T17:00:00Z", tiers=tiers, clock_offset=0.5)
    script = {
        "probe": dataclasses.asdict(probe),
        "results": [{"name": "market-status", "status": "passed", "reason": ""}],
    }
    out = tmp_path / "out"
    environ = {k: v for k, v in os.environ.items() if k != "CI"} | {
        "MASSIVE_API_KEY": KEY,
        "FAKE_LIVE_CHILD": json.dumps(script),
        "LIVE_PARENT_OUT": str(out),
        "LIVE_PARENT_PYTHON": str(wrapper),
    }
    done = subprocess.run(
        [sys.executable, "-m", "tests._fakes.live_parent"],
        cwd=ROOT,
        env=environ,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    report = json.loads(done.stdout.splitlines()[-1])
    assert report == {"status": commands.INTERRUPTED, "imported": []}
    # The run probed through its child and ran groups in theirs, so the check means something.
    [probed] = (out / "probes").glob("2026*.json")
    assert json.loads(probed.read_text())["tiers"] == tiers
    state = run_state.load(out)
    assert state is not None
    assert state.attempts
