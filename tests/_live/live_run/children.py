"""The live run's child processes: starting one, filtering its output, and stopping it.

Every attempt of the live-run program (`live_run`) runs in a child process, so a Java crash or
a hang cannot take the program down. This module owns that process:

- **Start.** `start` launches the child in a session of its own (`start_new_session=True`), so
  the terminal's Control-C reaches only the parent, with its output piped. A thread reads the
  output line by line, passes each through the run's `SecretFilter`, appends it to the
  attempt's `group.log`, and hands it to the parent's echo.
- **Identity.** Each child's command line carries a random token. A later start finds a child
  recorded as running alive only when `ps` shows that pid with that token (`is_alive`), so a pid
  reused after a reboot is never taken for the child.
- **Stop.** `stop` sends SIGINT to the child's process group, waits up to a grace period, then
  sends SIGKILL to the group: a hard stop and a Control-C both end a child this way. `wait`
  waits for the child to end or for its deadline, stopping it at the deadline.
- **Environment.** `child_environment` is the parent's environment with `CI` removed (the live
  tests skip under CI), the probed plans, and the library's debug log sent to the attempt.

The parent's main thread calls every function; each child's output thread only writes its log.
"""

from __future__ import annotations

import contextlib
import os
import secrets
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime
    from pathlib import Path

    from tests._live.live_run.secret_scrub import SecretFilter

STOP_GRACE: Final = 60.0
"""Seconds a child gets to end after SIGINT before its process group is killed."""

PLANS_VARIABLE: Final = "MASSIVE_LIVE_PLANS"
"""The variable that hands the probed plans to the live tests (`plans.PLANS_VARIABLE`)."""

S3_VARIABLES: Final = ("MASSIVE_S3_ACCESS_KEY_ID", "MASSIVE_S3_SECRET_ACCESS_KEY")
"""The S3 pair, which a child that must not read flat files does not get."""


def new_token() -> str:
    """Return a fresh random token for a child's command line.

    Returns:
        16 hex digits.
    """
    return secrets.token_hex(8)


def child_environment(
    base: Mapping[str, str],
    *,
    plans: str,
    debug_log: Path,
    extra: Mapping[str, str],
    with_s3: bool = True,
) -> dict[str, str]:
    """Return the environment of a child.

    Args:
        base: The parent's environment.
        plans: The probed plans in the `MASSIVE_LIVE_PLANS` grammar, such as
            `"stocks=starter,s3"`.
        debug_log: The attempt's debug log; the library writes its DEBUG log there.
        extra: Further variables, such as the outcomes file of a pytest child.
        with_s3: Whether the child gets the S3 pair; False keeps it from reading flat files.

    Returns:
        The environment: `base` without `CI` (and without the S3 pair when asked), with the
        plans, the debug log, and `extra`.
    """
    env = {k: v for k, v in base.items() if k != "CI"}
    if not with_s3:
        for name in S3_VARIABLES:
            env.pop(name, None)
    env[PLANS_VARIABLE] = plans
    env["DEEPHAVEN_MASSIVE_LOG"] = "DEBUG"
    env["DEEPHAVEN_MASSIVE_LOG_FILE"] = str(debug_log.resolve())
    env.update(extra)
    return env


@dataclass
class Child:
    """One running child, and the thread that copies its output.

    Created by `start`; the parent's main thread waits for it and stops it.

    Attributes:
        process: The child process.
        output: The thread that filters its output into the log and the echo.
    """

    process: subprocess.Popen[bytes]
    output: threading.Thread


def start(
    argv: list[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    log: Path,
    secrets_filter: SecretFilter,
    echo: Callable[[str], None],
    with_stdin: bool = False,
) -> Child:
    """Start a child in its own session, its output filtered into a log and the echo.

    Args:
        argv: The command line, its token included.
        cwd: The working directory, the repository root.
        env: The environment (`child_environment`).
        log: The attempt's `group.log`, appended to.
        secrets_filter: Cleans every line before it is written or echoed.
        echo: Receives each cleaned line, without its newline.
        with_stdin: Whether the parent may write to the child's standard input (the upgrade
            check's Enter); otherwise it reads nothing.

    Returns:
        The running child.
    """
    # The command line is the program's own: this interpreter and a fixed module or script,
    # with arguments the program built, never text from outside.
    process = subprocess.Popen(  # noqa: S603 - the program builds every child command line
        argv,
        cwd=cwd,
        env=dict(env),
        stdin=subprocess.PIPE if with_stdin else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    assert process.stdout is not None  # piped just above
    output = threading.Thread(
        target=_copy_output,
        args=(process.stdout,),
        kwargs={"log": log, "secrets_filter": secrets_filter, "echo": echo},
        name=f"live run output {process.pid}",
        daemon=True,
    )
    output.start()
    return Child(process=process, output=output)


def _copy_output(
    stream: IO[bytes], *, log: Path, secrets_filter: SecretFilter, echo: Callable[[str], None]
) -> None:
    """Copy a child's output, cleaned line by line, into its log and the echo.

    Args:
        stream: The child's standard output, its standard error merged in.
        log: The log, appended to and flushed after each line.
        secrets_filter: Cleans each line.
        echo: Receives each cleaned line.

    The stream is closed once the child's output ends.
    """
    with stream, log.open("a", encoding="utf-8") as file:
        for raw in stream:
            line = secrets_filter.clean(raw.decode("utf-8", errors="replace").rstrip("\r\n"))
            file.write(line + "\n")
            file.flush()
            echo(line)


WAIT_SLICE: Final = 30.0
"""The longest single wait on a child, in seconds: the wall clock is checked after each, so a
deadline is kept even when the machine slept, which a single long timeout would not count."""


def stop(child: Child, *, grace: float = STOP_GRACE) -> None:
    """Stop a child: SIGINT to its process group, then SIGKILL after the grace period.

    Args:
        child: The child.
        grace: Seconds to wait after SIGINT.
    """
    _signal_group(child.process, signal.SIGINT)
    try:
        child.process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        _signal_group(child.process, signal.SIGKILL)
        child.process.wait()
    _finish(child)


def _finish(child: Child) -> None:
    """Wait for an ended child's output to be copied, and close its input pipe if it has one.

    Args:
        child: A child whose process has ended.
    """
    child.output.join()
    if child.process.stdin is not None:
        child.process.stdin.close()


def _signal_group(process: subprocess.Popen[bytes], signal_number: signal.Signals) -> None:
    """Send a signal to a child's process group, which may be gone already.

    On macOS, `killpg` of a group whose leader has exited but not yet been reaped raises
    `PermissionError` rather than `ProcessLookupError`; either means there is nothing left to
    signal once `poll()` reaps the leader.

    Args:
        process: The child, whose pid is its group's id.
        signal_number: The signal.

    Raises:
        PermissionError: When the group is still running but may not be signalled.
    """
    try:
        os.killpg(process.pid, signal_number)
    except ProcessLookupError:
        return
    except PermissionError:
        if process.poll() is None:
            raise


def kill_group(pid: int) -> None:
    """Kill a process group left behind by an earlier run of the program.

    Args:
        pid: The recorded child's pid, its group's id.
    """
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)


def wait(
    child: Child, *, deadline: datetime, now: Callable[[], datetime], grace: float = STOP_GRACE
) -> tuple[int, bool]:
    """Wait for a child to end, stopping it at its deadline or when the parent is interrupted.

    It waits in slices of at most `WAIT_SLICE` seconds and checks the wall clock after each,
    so a deadline passed while the machine slept stops the child at once on waking.

    Args:
        child: The child.
        deadline: When it is stopped if still running (an aware datetime).
        now: The current time, aware.
        grace: Seconds a stopped child gets after SIGINT (`stop`).

    Returns:
        The exit status (negative for a signal) and whether the child was stopped. Whatever
        interrupts the wait (Control-C, a hang-up, an error) stops the child, then goes on.
    """
    try:
        while True:
            remaining = (deadline - now()).total_seconds()
            if remaining <= 0:
                stop(child, grace=grace)
                return child.process.returncode, True
            try:
                code = child.process.wait(timeout=min(remaining, WAIT_SLICE))
            except subprocess.TimeoutExpired:
                continue
            _finish(child)
            return code, False
    except BaseException:
        stop(child, grace=grace)
        raise


def is_alive(pid: int, token: str) -> bool:
    """Tell whether a recorded child still runs: its pid is alive and carries its token.

    Args:
        pid: The recorded pid.
        token: The recorded token.

    Returns:
        True when `ps` shows the pid with a command line that holds the token.
    """
    if pid <= 0 or not token:
        return False
    return token in run_command(["/bin/ps", "-o", "command=", "-p", str(pid)])


def run_command(argv: list[str], *, timeout: float = 60.0) -> str:
    """Run a short command and return what it printed, for the environment checks.

    Args:
        argv: The command line, its program an absolute path or this interpreter.
        timeout: Seconds before it is abandoned.

    Returns:
        Its standard output and standard error, joined; `""` when it cannot be started or
        does not end in time.
    """
    try:
        # A fixed command the program names (ps, git, java, uv), never text from outside.
        done = subprocess.run(  # noqa: S603 - a fixed system tool the program names
            argv, capture_output=True, text=True, check=False, timeout=timeout
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return done.stdout + done.stderr


def write_line(child: Child, text: str) -> None:
    """Write one line to a child's standard input.

    Args:
        child: A child started `with_stdin`.
        text: The line, without its newline.
    """
    assert child.process.stdin is not None  # started with_stdin
    child.process.stdin.write(text.encode() + b"\n")
    child.process.stdin.flush()


def sleep_until(moment: datetime, *, now: Callable[[], datetime], step: float = 30.0) -> None:
    """Sleep until a moment, in short steps so Control-C and a changed clock are noticed.

    Args:
        moment: When to wake (an aware datetime).
        now: The current time, aware.
        step: The longest single sleep, in seconds.
    """
    while (remaining := (moment - now()).total_seconds()) > 0:
        time.sleep(min(remaining, step))


def keep_awake_argv(
    system: str, *, pid: int, which: Callable[[str], str | None]
) -> list[str] | None:
    """Return the command that keeps the machine awake while a process lives.

    Args:
        system: `platform.system()`.
        pid: The process whose life the assertion lasts.
        which: Finds a program on the path (`shutil.which`).

    Returns:
        `caffeinate -i -w <pid>` on macOS, `systemd-inhibit --what=sleep ... sleep infinity`
        on Linux, or None when the tool is missing.
    """
    if system == "Darwin":
        tool = which("caffeinate")
        return None if tool is None else [tool, "-i", "-w", str(pid)]
    tool = which("systemd-inhibit")
    if tool is None:
        return None
    return [tool, "--what=sleep", "--why=massive live run", "sleep", "infinity"]


def keep_awake(
    argv: list[str] | None, *, log: Path, secrets_filter: SecretFilter
) -> Callable[[], None]:
    """Start keeping the machine awake, and return the function that stops it.

    Neither tool stops a laptop sleeping when its lid closes. A tool that is missing or that
    the system refuses (a `systemd-inhibit` without permission) only means the machine may
    sleep: the run goes on, and the tool's own words are in the log.

    Args:
        argv: The command (`keep_awake_argv`), or None when there is none.
        log: Where the tool's output goes.
        secrets_filter: Cleans that output.

    Returns:
        A function that ends the assertion.
    """
    if argv is None:
        return lambda: None
    child = start(
        argv,
        cwd=log.parent,
        env=os.environ,
        log=log,
        secrets_filter=secrets_filter,
        echo=lambda _line: None,
    )
    return lambda: stop(child, grace=5.0)
