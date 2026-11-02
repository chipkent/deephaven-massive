"""The live-run program's commands: check, run, status, pack, and the upgrade check.

`scripts/live_run.py` is what the tester runs, with their own Massive keys, to record every
live check their subscriptions allow; it parses the command line, builds a `RunContext` from
the real machine, and calls one of these:

- `check`: the machine checks and one probe, printed; writes nothing. Exit 0 when ready.
- `run`: holds `run.lock`, probes, and follows the timetable until stopped (`run_schedule`,
  `attempts`), keeping the machine awake. It resumes from `state.json`, refusing another
  commit; `--any-time` runs the groups that need no market hours at once, and `--redo G` makes
  every check of group G due again. When nothing can run it prints when the next group starts,
  packs if anything finished since the last pack, and sleeps.
- `status`: the latest probe, every check, and a summary per group. Reads only.
- `pack`: `checks.csv`, `REPORT.md`, the secret scan, and the zip; refused while a run holds
  the lock or without `MASSIVE_API_KEY`, since the scan would then prove nothing.
- `upgrade-check SUBSCRIPTION`: the attended upgrade check, holding the lock; refused unless
  the probe shows the subscription on Basic. A run that ends `tier stayed Basic` was a
  rehearsal and is recorded as one, not as the check's result.

Every printed line also goes to `run.log`, cleaned of the secrets. Times are New York time.
"""

from __future__ import annotations

import contextlib
import csv
import dataclasses
import fcntl
import json
import logging
import secrets
import signal
import threading
import traceback
from datetime import UTC, date, timedelta
from typing import TYPE_CHECKING, Final, NoReturn

from tests._live.calendar import NEW_YORK
from tests._live.live_run import attempts, children, results, timetable
from tests._live.live_run import state as run_state
from tests._live.live_run.context import RunMemory, RunRefusedError
from tests._live.live_run.machine_checks import ready
from tests._live.live_run.outcomes import read_outcomes
from tests._live.live_run.run_probe import ProbeResult
from tests._live.live_run.schedule import PYTEST_GROUPS, next_start_text, next_step
from tests._live.live_run.secret_scrub import fingerprints
from tests._live.live_run.state import RunState, check_of, is_due, record
from tests._live.live_run.stream_units import night_units, stream_units
from tests._live.live_run.upgrade_check import PROMPT, SUBSCRIPTIONS

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    from tests._live.live_run.context import RunContext

logger = logging.getLogger(__name__)

BASIC_PAUSE: Final = timedelta(seconds=65)
"""The pause after a probe before any group starts, while some subscription is on Basic: the
next group then starts on an unspent minute."""

ERROR_BACKOFF: Final = timedelta(minutes=5)
"""How long the run waits after a step that raised before it tries the next step."""

INTERRUPTED: Final = 130
"""The exit status after Control-C."""

REHEARSAL: Final = "tier stayed Basic"
"""The upgrade check's verdict when no upgrade happened: a rehearsal."""


@contextlib.contextmanager
def held_lock(out: Path) -> Iterator[None]:
    """Hold `run.lock` for a command's whole life; the system releases it if the process dies.

    Args:
        out: The results folder, created if missing.

    Yields:
        Control while the lock is held.

    Raises:
        RunRefusedError: When another process holds it.
    """
    out.mkdir(parents=True, exist_ok=True)
    with (out / "run.lock").open("a") as file:
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            msg = f"a run or an upgrade check is using {out}; stop it first"
            raise RunRefusedError(msg) from error
        yield


def is_locked(out: Path) -> bool:
    """Tell whether another process holds a results folder's lock.

    Args:
        out: The results folder.

    Returns:
        True while a run or an upgrade check holds `run.lock`.
    """
    path = out / "run.lock"
    if not path.exists():
        return False
    with path.open("a") as file:
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
    return False


# --- probing ----------------------------------------------------------------------------------


def apply_probe(
    ctx: RunContext, state: RunState, memory: RunMemory, *, result: ProbeResult
) -> None:
    """Record a probe, adopt its tiers, and plan a stream day and a stream night when due.

    Writes `probes/<UTC time>.json` and a line of `probes/entitlements.csv`. A probe that could
    not tell the tiers keeps the previous ones, Basic everywhere before the first, and is tried
    again later (`run_schedule`).

    Args:
        ctx: The command's context.
        state: The run's state, changed and saved.
        memory: The run's memory.
        result: The probe.

    Raises:
        RunRefusedError: When Massive rejected the API key.
    """
    now = ctx.now()
    folder = ctx.out / "probes"
    folder.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dataclasses.asdict(result), indent=1)
    path = folder / f"{result.when.replace(':', '')}.json"
    path.write_text(ctx.secrets.clean(text) + "\n", encoding="utf-8")
    memory.probed_at = now
    if result.key_rejected:
        msg = "the API key was rejected: copy it again from Massive's dashboard"
        raise RunRefusedError(msg)
    if result.tiers is None:
        memory.retries += 1
        ctx.say(f"the probe could not tell the subscriptions ({result.error}); keeping the last")
        if not state.current:
            basic = dict.fromkeys(SUBSCRIPTIONS, "basic")
            state.current = run_state.entitlement_id(state, basic, [])
    else:
        memory.retries = 0
        memory.latest_probe = dataclasses.asdict(result)
        if result.closures is not None:
            state.closures = result.closures
        state.current = run_state.entitlement_id(state, result.tiers, result.flat_files)
        _append_probe_line(folder, state, result)
        ctx.say(f"subscriptions: {run_state.tiers_text(state, state.current)}")
    tiers = state.entitlements[state.current]["tiers"]
    if "basic" in tiers.values():
        memory.not_before = now + BASIC_PAUSE
    _plan_stream_day(ctx, state, tiers)
    _plan_stream_night(ctx, state, tiers)
    _remind_flat_files(ctx, memory, result)
    run_state.save(ctx.out, state)


def _append_probe_line(folder: Path, state: RunState, result: ProbeResult) -> None:
    """Append one probe's line to `probes/entitlements.csv`.

    Args:
        folder: The `probes` folder.
        state: The run's state, for the entitlement's text.
        result: The probe.
    """
    path = folder / "entitlements.csv"
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        if new:
            writer.writerow(["when", "entitlement", "subscriptions", "history", "clock_offset_s"])
        history = " ".join(f"{k}={v}" for k, v in result.history_start.items())
        subscriptions = run_state.tiers_text(state, state.current)
        writer.writerow([result.when, state.current, subscriptions, history, result.clock_offset])


def _plan_stream_day(ctx: RunContext, state: RunState, tiers: dict[str, str]) -> None:
    """Plan a stream day when a streaming unit is due and none is planned ahead.

    Args:
        ctx: The command's context.
        state: The run's state.
        tiers: The current tiers.
    """
    today = ctx.now().astimezone(NEW_YORK).date()
    due = [u for u in stream_units(tiers) if is_due(state, check_of(state, "stream-day", u))]
    if due and (not state.stream_day or state.stream_day < today.isoformat()):
        planned = timetable.next_stream_day(today, state.stream_days, night=state.stream_night)
        state.stream_day = planned.isoformat()
        ctx.say(f"stream day planned for {state.stream_day}: keep the machine on and awake")


def _plan_stream_night(ctx: RunContext, state: RunState, tiers: dict[str, str]) -> None:
    """Plan a stream night when a night unit is due and no planned night is still to end.

    A night already under way is never moved: a probe between two of its children's starts
    finds its units still due.

    Args:
        ctx: The command's context.
        state: The run's state.
        tiers: The current tiers.
    """
    now = ctx.now()
    units = [u for u in night_units(tiers) if is_due(state, check_of(state, "stream-night", u))]
    planned = state.stream_night
    if not units or (planned and now < timetable.night_end(date.fromisoformat(planned))):
        return
    evening = timetable.next_stream_night(
        now.astimezone(NEW_YORK).date(), stream_day=state.stream_day, closures=state.closures
    )
    state.stream_night = evening.isoformat()
    ctx.say(
        f"stream night planned from {timetable.night_start(evening):%a %Y-%m-%d %H:%M} to "
        f"{timetable.night_end(evening):%H:%M} the next morning: "
        "keep the machine on and awake overnight"
    )


def _remind_flat_files(ctx: RunContext, memory: RunMemory, result: ProbeResult) -> None:
    """Print, once a day, that a paid tier includes flat files while the S3 pair is not set.

    Args:
        ctx: The command's context.
        memory: The run's memory, which notes the day of the reminder.
        result: The probe.
    """
    if result.tiers is None or any(ctx.environ.get(v, "").strip() for v in children.S3_VARIABLES):
        return
    today = ctx.now().astimezone(NEW_YORK).date().isoformat()
    paid = [f"{name.capitalize()} {tier}" for name, tier in result.tiers.items() if tier != "basic"]
    if paid and memory.reminded != today:
        memory.reminded = today
        ctx.say(
            f"{paid[0]} includes flat files, but the S3 pair is not set: "
            "set MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY, then restart"
        )


# --- commands ---------------------------------------------------------------------------------


def command_check(ctx: RunContext) -> int:
    """Check the machine and probe the key, printing both; write nothing.

    Args:
        ctx: The command's context; its `out` is where the disk is checked.

    Returns:
        0 when the machine is ready and the key accepted, else 1.
    """
    require_key(ctx)
    result = safe_probe(ctx)
    items, _ = ctx.machine(ctx.out, result.clock_offset)
    for item in items:
        ctx.echo(f"{item.verdict:4} {item.name}: {item.detail}")
    if result.key_rejected:
        ctx.echo("FAIL key: the API key was rejected: copy it again from Massive's dashboard")
    elif result.tiers is None:
        ctx.echo(f"WARN probe: could not tell the subscriptions ({result.error})")
    else:
        for name, tier in result.tiers.items():
            history = result.history_start.get(name, "")
            ctx.echo(f"     {name}: {tier}, history from {history}")
        ctx.echo(f"     flat files: {', '.join(result.flat_files) or 'none'}")
    ok = ready(items) and not result.key_rejected
    ctx.echo(f"ready: {'yes' if ok else 'no'}")
    return 0 if ok else 1


def _open_state(ctx: RunContext) -> RunState:
    """Load the folder's state, or start one; refuse another commit.

    On resume, whatever the last run left running is stopped and its results kept
    (`attempts.recover_stale_attempts`).

    Args:
        ctx: The command's context.

    Returns:
        The state.

    Raises:
        RunRefusedError: When the state was started on another commit.
    """
    state = run_state.load(ctx.out)
    if state is None:
        state = RunState(commit=ctx.commit)
        ctx.say(f"results folder: {ctx.out}")
    elif state.commit != ctx.commit:
        msg = (
            f"{ctx.out} was started on commit {state.commit}: to resume, run "
            f"`git -C {ctx.root} checkout {state.commit}`; or give a new --out folder"
        )
        raise RunRefusedError(msg)
    else:
        attempts.recover_stale_attempts(ctx, state)
        ctx.say("resuming")
    return state


def require_key(ctx: RunContext) -> None:
    """Refuse to start without the API key, before anything is probed or written.

    Args:
        ctx: The command's context.

    Raises:
        RunRefusedError: When `MASSIVE_API_KEY` is unset or blank, saying how to set it.
    """
    if not ctx.environ.get("MASSIVE_API_KEY", "").strip():
        msg = (
            "MASSIVE_API_KEY is not set in this terminal. Set it, then run the command again: "
            "printf 'Massive API key: '; read -rs MASSIVE_API_KEY; echo; export MASSIVE_API_KEY"
        )
        raise RunRefusedError(msg)


def safe_probe(ctx: RunContext) -> ProbeResult:
    """Probe the key, turning any failure of the probe itself into an unknown result.

    Args:
        ctx: The command's context.

    Returns:
        The probe's result; on an exception, a result with no tiers and the exception as its
        error, which keeps the previous tiers (`apply_probe`).
    """
    try:
        return ctx.probe()
    except Exception as error:
        # Never printed: the run's own record of it is the probe file the error goes to.
        logger.debug("the probe failed", exc_info=True)
        when = ctx.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return ProbeResult(when=when, error=f"the probe failed: {type(error).__name__}: {error}")


@contextlib.contextmanager
def hang_ups_interrupt() -> Iterator[None]:
    """Treat a hang-up (a closed terminal) and a termination like Control-C while running.

    Yields:
        Control while SIGHUP and SIGTERM raise `KeyboardInterrupt` in the main thread.
    """

    def interrupt(_signal: int, _frame: object) -> None:
        raise KeyboardInterrupt

    saved = {number: signal.signal(number, interrupt) for number in (signal.SIGHUP, signal.SIGTERM)}
    try:
        yield
    finally:
        for number, handler in saved.items():
            signal.signal(number, handler)


def command_run(ctx: RunContext, *, any_time: bool = False, redo: tuple[str, ...] = ()) -> int:
    """Run every check the key allows, by the timetable, until stopped.

    It refuses (`RunRefusedError`) without the API key, when the folder is in use, was started
    on another commit, or the key was rejected. Control-C, a closed terminal (SIGHUP), and
    SIGTERM all stop it the same way: the running child is stopped and the state saved.

    Args:
        ctx: The command's context.
        any_time: Run the groups that need no market hours at once. It takes effect the first
            time it is given for a folder; a resume finishes those groups and ignores it.
        redo: Groups whose every check is made due again.

    Returns:
        `INTERRUPTED` after Control-C, the only way it ends but a refusal.
    """
    require_key(ctx)
    with held_lock(ctx.out), hang_ups_interrupt():
        return _run_locked(ctx, any_time=any_time, redo=redo)


def _run_locked(ctx: RunContext, *, any_time: bool, redo: tuple[str, ...]) -> int:
    """Run the program while holding the lock, keeping the machine awake.

    Args:
        ctx: The command's context.
        any_time: Run the groups that need no market hours at once, once per folder.
        redo: Groups whose every check is made due again.

    Returns:
        `INTERRUPTED` after Control-C.
    """
    state = _open_state(ctx)
    for group in redo:
        run_state.redo(state, group)
    if any_time and state.any_time_used:
        ctx.say("--any-time was used before for this folder; following the timetable")
    elif any_time:
        state.any_time, state.any_time_used = list(timetable.ANY_TIME_GROUPS), True
    state.salt = state.salt or secrets.token_hex(16)
    state.fingerprints = fingerprints(ctx.environ, state.salt)
    run_state.save(ctx.out, state)
    memory = RunMemory()
    stop_keep_awake = ctx.keep_awake()
    try:
        first = safe_probe(ctx)
        _, environment = ctx.machine(ctx.out, first.clock_offset)
        (ctx.out / "preflight").mkdir(exist_ok=True)
        text = ctx.secrets.clean(json.dumps(environment, indent=1, default=str))
        (ctx.out / "preflight" / "environment.json").write_text(text + "\n", encoding="utf-8")
        apply_probe(ctx, state, memory, result=first)
        _loop(ctx, state, memory)
    except KeyboardInterrupt:
        _save_quietly(ctx, state)
        ctx.say(
            f"interrupted; to resume, run: uv run python scripts/live_run.py run --out {ctx.out}"
        )
        return INTERRUPTED
    finally:
        stop_keep_awake()


def _save_quietly(ctx: RunContext, state: RunState) -> None:
    """Save the state, saying so instead of raising when the disk refuses.

    Args:
        ctx: The command's context.
        state: The run's state.
    """
    try:
        run_state.save(ctx.out, state)
    except OSError as error:
        ctx.echo(f"could not save the state: {error}")


def _loop(ctx: RunContext, state: RunState, memory: RunMemory) -> NoReturn:
    """Probe, run, and sleep, step by step, until Control-C.

    A step that raises (a full disk, a bug) does not end the run: its traceback, cleaned of
    the secrets, goes to `run.log`, the state is saved if the disk allows, and the loop waits
    `ERROR_BACKOFF` before its next step.

    Args:
        ctx: The command's context.
        state: The run's state.
        memory: The run's memory.
    """
    while True:
        try:
            _step(ctx, state, memory)
        except Exception:
            logger.debug("a step failed", exc_info=True)  # run.log gets the cleaned traceback
            text = ctx.secrets.clean(traceback.format_exc())
            ctx.echo(text.rstrip().splitlines()[-1])
            with contextlib.suppress(OSError):
                ctx.say("a step failed; trying again in 5 minutes:\n" + text)
            _save_quietly(ctx, state)
            ctx.sleep_until(ctx.now() + ERROR_BACKOFF)


def _step(ctx: RunContext, state: RunState, memory: RunMemory) -> None:
    """Take the run's next step: probe, run one group, or sleep.

    Args:
        ctx: The command's context.
        state: The run's state.
        memory: The run's memory.
    """

    def reprobe() -> None:
        apply_probe(ctx, state, memory, result=safe_probe(ctx))

    step = next_step(state, memory, ctx.now())
    if step.action == "probe":
        reprobe()
        return
    assert step.until is not None  # a run has a stop and a sleep an end
    if step.action == "run":
        ctx.say(f"start {step.group}")
        if step.group in PYTEST_GROUPS:
            attempts.run_pytest_group(
                ctx, state, memory, group=step.group, until=step.until, reprobe=reprobe
            )
        else:
            attempts.run_built_in_group(ctx, state, memory, group=step.group, until=step.until)
        if step.group in state.any_time:
            state.any_time.remove(step.group)
            run_state.save(ctx.out, state)
        ctx.say(f"end {step.group}")
        return
    if memory.finished_since_pack:
        memory.finished_since_pack = False
        _pack_quietly(ctx, state)
    until = step.until.astimezone(NEW_YORK)
    ctx.say(f"idle: nothing more to run until {until:%a %H:%M %Z} ({step.group})")
    ctx.sleep_until(step.until)


PACK_REFUSED: Final = (
    "pack refused: a secret was found in the files below, so no zip was written. Do not send "
    "the folder; send the owner this list, and the owner will say how to clean those files:"
)


def _pack_quietly(ctx: RunContext, state: RunState) -> None:
    """Pack the results while idle, saying only where the zip went or what blocked it.

    Args:
        ctx: The command's context.
        state: The run's state.
    """
    shortage = _disk_shortage(ctx)
    if shortage:
        ctx.say(shortage)
        return
    day = ctx.now().astimezone(NEW_YORK).date().isoformat()
    target, finds = results.pack(ctx.out, state, ctx.secrets, day=day)
    if target is None:
        ctx.say(f"{PACK_REFUSED} {', '.join(finds)}")
    else:
        ctx.say(f"packed {target.name}")


def _disk_shortage(ctx: RunContext) -> str:
    """Tell whether the disk lacks room for a zip of the results folder.

    Args:
        ctx: The command's context.

    Returns:
        Why a pack cannot run, or `""` when the free disk exceeds the folder's size.
    """
    size = sum(p.stat().st_size for p in ctx.out.rglob("*") if p.is_file())
    free = ctx.free_disk(ctx.out)
    if free > size:
        return ""
    return (
        f"pack: the zip needs about {size >> 20} MB, and {free >> 20} MB is free: free some space"
    )


def command_status(ctx: RunContext) -> int:
    """Print the run's status; read only.

    Args:
        ctx: The command's context.

    Returns:
        0, or 1 when the folder holds no run.
    """
    state = run_state.load(ctx.out)
    if state is None:
        ctx.echo(f"no run in {ctx.out}")
        return 1
    probes = sorted((ctx.out / "probes").glob("*.json"))
    latest = None
    for path in reversed(probes):
        stored = json.loads(path.read_text(encoding="utf-8"))
        if stored.get("tiers") is not None:
            latest = stored
            break
    now = ctx.now()
    lines = results.status_lines(
        state,
        latest_probe=latest,
        next_start=lambda group: next_start_text(state, group, now),
    )
    for line in lines:
        ctx.echo(line)
    return 0


def command_pack(ctx: RunContext) -> int:
    """Write the report files, scan for secrets, and zip the results.

    Args:
        ctx: The command's context.

    Returns:
        0 when the zip was written, else 1. It refuses while a run holds the folder, when the
        terminal does not hold exactly the secrets the run used (by their fingerprints), since
        a scan for other values would prove nothing, and when the disk lacks room for the zip.
    """
    if is_locked(ctx.out):
        ctx.echo("pack: a run is using this folder; stop it with Control-C first")
        return 1
    state = run_state.load(ctx.out)
    if state is None:
        ctx.echo(f"no run in {ctx.out}")
        return 1
    mismatch = secrets_mismatch(state, ctx.environ)
    shortage = _disk_shortage(ctx)
    if mismatch or shortage:
        ctx.echo(mismatch or shortage)
        return 1
    day = ctx.now().astimezone(NEW_YORK).date().isoformat()
    target, finds = results.pack(ctx.out, state, ctx.secrets, day=day)
    if target is None:
        ctx.echo(PACK_REFUSED)
        for find in finds:
            ctx.echo(f"  {find}")
        return 1
    ctx.echo(f"wrote {target}")
    return 0


def secrets_mismatch(state: RunState, environ: Mapping[str, str]) -> str:
    """Tell whether this terminal holds the secrets the run used, by their fingerprints.

    Args:
        state: The run's state, with the latest run's fingerprints.
        environ: This terminal's environment.

    Returns:
        `""` when the same secret variables are set to the same values; otherwise what to set,
        naming every variable that is missing, extra, or different.
    """
    held = fingerprints(environ, state.salt) if state.salt else {}
    if state.salt and held == state.fingerprints:
        return ""
    needed = ", ".join(sorted(state.fingerprints)) or "MASSIVE_API_KEY"
    missing = sorted(set(state.fingerprints) - set(held))
    extra = sorted(set(held) - set(state.fingerprints))
    different = sorted(
        n for n in held if n in state.fingerprints and held[n] != state.fingerprints[n]
    )
    details = [
        f"{what}: {', '.join(names)}"
        for what, names in (
            ("not set", missing),
            ("not used by the run", extra),
            ("a different value", different),
        )
        if names
    ]
    return (
        f"pack: set exactly the secrets the run used, with the same values ({needed}), in "
        f"this terminal, then pack again; {'; '.join(details) or 'no run recorded them'}"
    )


def command_upgrade_check(ctx: RunContext, subscription: str) -> int:
    """Run the attended upgrade check of one subscription.

    Args:
        ctx: The command's context.
        subscription: One of `upgrade_check.SUBSCRIPTIONS`.

    Returns:
        0 when it passed or was a rehearsal, 1 when it failed, `INTERRUPTED` after Control-C
        (nothing is recorded then).

    Raises:
        RunRefusedError: When the folder is in use, the commit differs, the key was rejected,
            or the subscription is not on Basic.
    """
    require_key(ctx)
    with held_lock(ctx.out), hang_ups_interrupt():
        state = _open_state(ctx)
        memory = RunMemory()
        apply_probe(ctx, state, memory, result=safe_probe(ctx))
        tier = state.entitlements[state.current]["tiers"][subscription]
        if tier != "basic":
            msg = f"{subscription} is on {tier}, not Basic: the upgrade check starts from Basic"
            raise RunRefusedError(msg)
        attempt = attempts.new_attempt(ctx, state, "upgrade-check")
        folder = ctx.out / attempt.folder
        request = {
            "group": "upgrade-check",
            "due": [subscription],
            "tiers": {},
            "entitlement": state.current,
            "until": ctx.now().isoformat(),
        }
        (folder / "request.json").write_text(json.dumps(request) + "\n", encoding="utf-8")
        try:
            verdict = _attended_child(ctx, state, attempt)
        except KeyboardInterrupt:
            run_state.save(ctx.out, state)
            ctx.echo("upgrade check interrupted; nothing recorded")
            return INTERRUPTED
        day = ctx.now().astimezone(NEW_YORK).date().isoformat()
        status, reason = verdict
        if reason == REHEARSAL:
            ctx.say(f"upgrade check: failed: {REHEARSAL} (recorded as a rehearsal)")
        else:
            check = check_of(state, "upgrade-check", subscription)
            record(state, check, status=status, reason=reason, day=day)
            ctx.say(f"upgrade check: {status}{': ' + reason if reason else ''}")
        run_state.save(ctx.out, state)
    return 0 if status == "passed" or reason == REHEARSAL else 1


def _attended_child(
    ctx: RunContext, state: RunState, attempt: run_state.Attempt
) -> tuple[str, str]:
    """Run the upgrade check's child, passing the person's Enter to it at its prompt.

    The child runs in its own session and never reads the terminal: the parent watches its
    output for the prompt, waits for Enter, and writes a line to the child's input.

    Args:
        ctx: The command's context.
        state: The run's state.
        attempt: The attempt.

    Returns:
        The check's `(status, reason)` from the child's results; `("error", ...)` when it
        reported none.

    Raises:
        KeyboardInterrupt: After stopping the child, on Control-C.
    """
    folder = ctx.out / attempt.folder
    prompted = threading.Event()

    def echo(line: str) -> None:
        ctx.echo(f"  | {line}")
        if line.startswith(PROMPT.split("{", maxsplit=1)[0]):
            prompted.set()

    env = attempts.environment(ctx, state, folder=folder, extra={})
    argv = [
        ctx.python,
        str(ctx.script),
        "_group",
        "--out",
        str(ctx.out),
        "--attempt",
        attempt.folder,
        "--token",
        attempt.token,
    ]
    child = children.start(
        argv,
        cwd=ctx.root,
        env=env,
        log=folder / "group.log",
        secrets_filter=ctx.secrets,
        echo=echo,
        with_stdin=True,
    )
    attempt.pid = child.process.pid
    run_state.save(ctx.out, state)
    try:
        while child.process.poll() is None:
            if prompted.wait(0.5):
                prompted.clear()
                ctx.read_enter()
                children.write_line(child, "")
        children.wait(child, deadline=ctx.now() + timedelta(minutes=1), now=ctx.now)
    except KeyboardInterrupt:
        children.stop(child, grace=ctx.stop_grace)
        attempt.status = "interrupted"
        raise
    attempt.status, attempt.exit_code = "finished", child.process.returncode
    lines = read_outcomes(folder / "results.jsonl")
    if not lines:
        return "error", "the upgrade check reported nothing"
    return lines[-1]["status"], lines[-1]["reason"]
