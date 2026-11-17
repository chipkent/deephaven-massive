"""Tests of the live run's child processes (`tests/_live/live_run/children.py`).

Each test starts a small real child, a `python -c` script of this interpreter, and checks what
the parent sees: its output cleaned of a planted secret in the log and the echo, its end or its
stop at a deadline, SIGKILL after the grace period for a child that ignores SIGINT, and the
token check that tells a recorded child from another process.
"""

from __future__ import annotations

import os
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests._live.live_run import children
from tests._live.live_run.secret_scrub import REDACTED, SecretFilter

PLANTED = "PLANTED-child-key-31f0"
SECRETS = SecretFilter((PLANTED,))

SLEEPER = "import time; print('ready', flush=True); time.sleep(60)"
STUBBORN = (
    "import signal, time; signal.signal(signal.SIGINT, signal.SIG_IGN); "
    "print('ready', flush=True); time.sleep(60)"
)


def _now() -> datetime:
    return datetime.now(UTC)


def _start(
    tmp_path: Path, script: str, *, with_stdin: bool = False
) -> tuple[children.Child, list[str], threading.Event]:
    lines: list[str] = []
    ready = threading.Event()

    def echo(line: str) -> None:
        lines.append(line)
        if line == "ready":
            ready.set()

    child = children.start(
        [sys.executable, "-c", script, "--token-abc"],
        cwd=tmp_path,
        env=os.environ,
        log=tmp_path / "group.log",
        secrets_filter=SECRETS,
        echo=echo,
        with_stdin=with_stdin,
    )
    return child, lines, ready


def test_a_child_output_is_cleaned_in_the_log_and_the_echo(tmp_path: Path) -> None:
    child, lines, _ = _start(tmp_path, f"print('key {PLANTED}'); print('done')")
    code, stopped = children.wait(child, deadline=_now() + timedelta(seconds=30), now=_now)
    assert (code, stopped) == (0, False)
    assert lines == [f"key {REDACTED}", "done"]
    log = (tmp_path / "group.log").read_text()
    assert PLANTED not in log
    assert log == f"key {REDACTED}\ndone\n"


def test_a_child_past_its_deadline_is_stopped(tmp_path: Path) -> None:
    child, _, ready = _start(tmp_path, SLEEPER)
    assert ready.wait(30)
    code, stopped = children.wait(child, deadline=_now(), now=_now, grace=30)
    assert stopped
    assert code != 0


def test_a_child_that_ignores_sigint_is_killed_after_the_grace(tmp_path: Path) -> None:
    child, _, ready = _start(tmp_path, STUBBORN)
    assert ready.wait(30)
    children.stop(child, grace=0.2)
    assert child.process.returncode == -9


def test_stopping_a_child_that_already_ended_is_harmless(tmp_path: Path) -> None:
    child, _, _ = _start(tmp_path, "pass")
    children.wait(child, deadline=_now() + timedelta(seconds=30), now=_now)
    children.stop(child, grace=0.2)
    assert child.process.returncode == 0


def test_control_c_stops_the_child_and_goes_on(tmp_path: Path) -> None:
    child, _, ready = _start(tmp_path, SLEEPER)
    assert ready.wait(30)

    def interrupted() -> datetime:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        children.wait(child, deadline=_now(), now=interrupted, grace=30)
    assert child.process.returncode is not None


def test_a_line_reaches_the_child_input(tmp_path: Path) -> None:
    child, lines, ready = _start(
        tmp_path, "print('ready', flush=True); print('got ' + input())", with_stdin=True
    )
    assert ready.wait(30)
    children.write_line(child, "enter")
    children.wait(child, deadline=_now() + timedelta(seconds=30), now=_now)
    assert lines == ["ready", "got enter"]


def test_a_recorded_child_is_alive_only_with_its_token(tmp_path: Path) -> None:
    child, _, ready = _start(tmp_path, SLEEPER)
    assert ready.wait(30)
    try:
        assert children.is_alive(child.process.pid, "--token-abc")
        assert not children.is_alive(child.process.pid, "--token-other")
        assert not children.is_alive(0, "--token-abc")
        assert not children.is_alive(child.process.pid, "")
    finally:
        children.stop(child, grace=30)


def test_a_command_output_or_nothing() -> None:
    assert children.run_command([sys.executable, "-c", "print('hi')"]) == "hi\n"
    assert children.run_command(["/nonexistent/program"]) == ""
    slow = [sys.executable, "-c", "import time; time.sleep(30)"]
    assert children.run_command(slow, timeout=0.2) == ""


def test_the_child_environment() -> None:
    base = {"CI": "true", "MASSIVE_S3_ACCESS_KEY_ID": "a", "MASSIVE_S3_SECRET_ACCESS_KEY": "b"}
    env = children.child_environment(base, debug_log=Path("debug.log"))
    assert "CI" not in env
    assert env["MASSIVE_S3_ACCESS_KEY_ID"] == "a"
    assert not any(name.startswith("MASSIVE_LIVE") for name in env)
    assert env["DEEPHAVEN_MASSIVE_LOG"] == "DEBUG"
    assert env["DEEPHAVEN_MASSIVE_LOG_FILE"].startswith("/")
    bare = children.child_environment(base, debug_log=Path("debug.log"), with_s3=False)
    assert not set(children.S3_VARIABLES) & set(bare)


def test_tokens_are_fresh() -> None:
    first, second = children.new_token(), children.new_token()
    assert len(first) == 16
    assert first != second


def test_the_keep_awake_command_of_each_system() -> None:
    def found(name: str) -> str | None:
        return f"/usr/bin/{name}"

    def missing(_name: str) -> str | None:
        return None

    assert children.keep_awake_argv("Darwin", pid=5, which=found) == [
        "/usr/bin/caffeinate",
        "-i",
        "-w",
        "5",
    ]
    linux = children.keep_awake_argv("Linux", pid=5, which=found)
    assert linux is not None
    assert linux[:2] == ["/usr/bin/systemd-inhibit", "--what=sleep"]
    assert children.keep_awake_argv("Darwin", pid=5, which=missing) is None
    assert children.keep_awake_argv("Linux", pid=5, which=missing) is None


def test_keep_awake_starts_and_stops_its_tool(tmp_path: Path) -> None:
    children.keep_awake(None, log=tmp_path / "k.log", secrets_filter=SECRETS)()
    stop = children.keep_awake(
        [sys.executable, "-c", SLEEPER], log=tmp_path / "k.log", secrets_filter=SECRETS
    )
    stop()
    assert (tmp_path / "k.log").exists()


def test_sleep_until_returns_once_the_moment_passed() -> None:
    moment = _now()
    times = iter([moment - timedelta(milliseconds=1), moment])
    children.sleep_until(moment, now=lambda: next(times), step=0.01)


def test_a_deadline_passed_during_a_wait_slice_stops_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child, _, ready = _start(tmp_path, SLEEPER)
    assert ready.wait(30)
    monkeypatch.setattr(children, "WAIT_SLICE", 0.05)
    start = _now()
    times = iter([start, start + timedelta(hours=9)])  # the machine slept through the deadline

    def clock() -> datetime:
        return next(times, start + timedelta(hours=9))

    code, stopped = children.wait(child, deadline=start + timedelta(hours=1), now=clock, grace=30)
    assert stopped
    assert code != 0


def test_signalling_a_group_whose_leader_exited_unreaped_is_harmless(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child, _, _ = _start(tmp_path, "pass")
    child.process.wait()

    def refuse(_pid: int, _number: int) -> None:
        raise PermissionError(1, "Operation not permitted")  # macOS, before the reap

    monkeypatch.setattr(os, "killpg", refuse)
    children.stop(child, grace=0.2)
    assert child.process.returncode == 0


def test_signalling_a_running_group_that_refuses_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child, _, ready = _start(tmp_path, SLEEPER)
    assert ready.wait(30)
    real_killpg = os.killpg

    def refuse(_pid: int, _number: int) -> None:
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(os, "killpg", refuse)
    with pytest.raises(PermissionError):
        children.stop(child, grace=0.2)
    real_killpg(child.process.pid, 9)
    child.process.wait()
    child.output.join()


def test_a_left_behind_group_is_killed_and_a_missing_one_ignored(tmp_path: Path) -> None:
    child, _, ready = _start(tmp_path, STUBBORN)
    assert ready.wait(30)
    children.kill_group(child.process.pid)
    assert child.process.wait(timeout=10) == -9
    child.output.join()
    children.kill_group(child.process.pid)  # gone now: nothing to do
