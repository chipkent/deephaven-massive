"""Tests of the live-run program's command line (`tests/_live/cli.py`).

The commands run on a context of fakes or on the real context with its probe and machine
checks replaced, so nothing reaches Massive; the built-in groups' child runs with a fake server
start and a stand-in for the group.
"""

from __future__ import annotations

import io
import json
import os
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests._live import children, cli, group_child, live_run, machine_checks
from tests._live.run_context import RunContext, RunRefusedError
from tests._live.run_probe import Credentials, ProbeResult
from tests._live.secret_scrub import SecretFilter

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive import MassiveSession
    from tests._live.machine_checks import Item

KEY = "PLANTED-cli-key-6b0e"
S3_PAIR = ("PLANTED-cli-access", "PLANTED-cli-s3-value")
ROOT = Path(__file__).resolve().parents[2]


def _probe() -> ProbeResult:
    return ProbeResult(when="2026-11-04T17:00:00Z", tiers=None, error="not used")


def _machine(_folder: Path, _offset: float | None) -> tuple[list[Item], dict[str, object]]:
    return [], {}


def _no_server(_heap: int, _folder: Path) -> None:
    pytest.fail("only _group starts a server")


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
        start_server=_no_server,
        make_context=recorder,
    )


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

    monkeypatch.setattr(live_run, command, fake)
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

    monkeypatch.setattr(live_run, "command_status", refuse)
    recorder = Recorder()
    assert _main(["status", "--out", str(tmp_path)], recorder) == cli.REFUSED
    assert recorder.lines == ["refused: another run is using it"]


def test_an_unknown_redo_group_is_refused(tmp_path: Path) -> None:
    recorder = Recorder()
    assert (
        _main(["run", "--out", str(tmp_path), "--redo", "evidence,nope"], recorder) == cli.REFUSED
    )
    assert recorder.lines[0].startswith(
        "refused: --redo names no group: nope; the groups: evidence"
    )


def test_credentials_come_from_the_environment_and_need_both_s3_values() -> None:
    environ = {"MASSIVE_API_KEY": f" {KEY} ", "MASSIVE_S3_ACCESS_KEY_ID": S3_PAIR[0]}
    assert cli.credentials(environ) == Credentials(api_key=KEY, s3_pair=None)
    environ["MASSIVE_S3_SECRET_ACCESS_KEY"] = S3_PAIR[1]
    assert cli.credentials(environ).s3_pair == S3_PAIR


def test_the_free_disk_is_read_from_the_nearest_existing_folder(tmp_path: Path) -> None:
    assert cli.nearest_folder(tmp_path / "a" / "b") == tmp_path


def test_the_real_context_wires_the_machine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probes: list[Credentials] = []

    def probe_key(credentials: Credentials, *, now: datetime) -> ProbeResult:
        probes.append(credentials)
        return _probe()

    machines: list[Path] = []

    def check_machine(
        *, root: Path, disk_path: Path, environ: object, clock_offset: float | None
    ) -> tuple[list[Item], dict[str, object]]:
        machines.append(disk_path)
        return [], {"offset": clock_offset}

    monkeypatch.setattr(cli, "probe_key", probe_key)
    monkeypatch.setattr(machine_checks, "check_machine", check_machine)
    stdout = io.StringIO()
    environ = {"MASSIVE_API_KEY": KEY}
    ctx = cli.real_context(
        tmp_path / "out", root=ROOT, script=Path("s.py"), environ=environ, stdout=stdout
    )
    assert len(ctx.commit) == 40
    assert ctx.secrets.values == (KEY,)
    ctx.probe()
    assert probes == [Credentials(api_key=KEY, s3_pair=None)]
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


@pytest.mark.parametrize(
    ("group", "memory", "heap"),
    [
        ("responses", 64 << 30, cli.SMALL_HEAP),
        ("stream-day", 64 << 30, cli.STREAM_HEAP_LIMIT),
        ("throughput", 12 << 30, 6 << 30),
        ("stream-day", None, cli.SMALL_HEAP),
    ],
)
def test_the_group_child_starts_a_server_sized_to_its_group(
    *, group: str, memory: int | None, heap: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "request.json").write_text(
        json.dumps({"group": group, "tiers": {"stocks": "basic"}})
    )
    monkeypatch.setattr(machine_checks, "memory_total", lambda: memory)
    ran: list[str] = []
    monkeypatch.setattr(
        group_child, "run_group", lambda _out, attempt, **_options: ran.append(attempt)
    )
    servers: list[tuple[int, Path]] = []
    watched: list[str] = []
    code = cli.main(
        ["_group", "--out", str(tmp_path), "--attempt", "a", "--token", "t"],
        root=ROOT,
        script=Path("s.py"),
        start_server=lambda heap, folder: servers.append((heap, folder)),
        watch=lambda: watched.append("watching"),
    )
    assert code == 0
    assert (servers, ran, watched) == ([(heap, tmp_path / "a")], ["a"], ["watching"])


def test_the_group_childs_sessions_declare_the_basic_subscriptions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    request = {"group": "upgrade-check", "tiers": {"stocks": "basic", "options": "starter"}}
    (tmp_path / "a" / "request.json").write_text(json.dumps(request))
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    seen: dict[str, Callable[..., Any]] = {}

    def run_group(
        _out: Path,
        _attempt: str,
        *,
        open_session: Callable[[], MassiveSession],
        tier_of: Callable[[str], str | None],
    ) -> None:
        seen["open_session"], seen["tier_of"] = open_session, tier_of

    tiers: list[dict[str, str] | None] = [{"stocks": "starter"}, None]

    def probe_key(_credentials: Credentials, *, now: datetime) -> ProbeResult:
        return ProbeResult(when="", tiers=tiers.pop(0))

    monkeypatch.setattr(group_child, "run_group", run_group)
    monkeypatch.setattr(cli, "probe_key", probe_key)
    cli.run_group_child(
        tmp_path,
        "a",
        environ={"MASSIVE_API_KEY": KEY},
        start_server=lambda _heap, _folder: None,
        watch=lambda: None,
    )
    open_session = seen["open_session"]
    with open_session() as session:
        assert {str(s) for s in session._core.config.rest_basic_subscriptions} == {"stocks"}
    assert seen["tier_of"]("stocks") == "starter"
    assert seen["tier_of"]("stocks") is None


def test_a_group_child_ends_once_its_parent_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    parents = [100, 100, 1]
    monkeypatch.setattr(os, "getppid", lambda: parents.pop(0))
    waits: list[float] = []
    ended: list[str] = []

    def wait(seconds: float) -> bool:
        waits.append(seconds)
        return False

    cli.watch_parent(100, orphaned=lambda: ended.append("ended"), wait=wait)
    assert (waits, ended) == ([cli.PARENT_CHECK, cli.PARENT_CHECK], ["ended"])


def test_the_parent_watch_runs_on_a_thread_of_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[int] = []
    done = threading.Event()

    def watch_parent(parent: int, **_options: object) -> None:
        seen.append(parent)
        done.set()

    monkeypatch.setattr(cli, "watch_parent", watch_parent)
    cli.start_parent_watch()
    assert done.wait(10.0)
    assert seen == [os.getppid()]
