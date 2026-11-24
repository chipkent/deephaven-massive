"""The live run's attempts: one run of a group in a child process, and its recorded results.

An *attempt* runs one group once, in its own folder, `<group>/<New York date>-<n>`, holding
`group.log` (the child's filtered output), `debug.log` (the library's DEBUG log), and the
group's own files. This module starts each kind of attempt and records what it found:

- **A pytest group** (`run_pytest_group`) first lists its tests with `--collect-only` and the
  outcomes plugin (`collection.json`), then runs only the due ones, writing `outcomes.jsonl`.
  Every evidence file (`live-evidence-<time>.log`, one per pytest run) left in the repository
  root moves to `orphans/` before the run, and the run's own moves into the attempt after it.
  A test Massive refused skips quoting the refusal, and is recorded `refused`.
- **A built-in group** (`run_built_in_group`) writes `request.json` (the due checks, its
  stop) and runs `scripts/live_run.py _group`, whose child writes one line per
  check to `results.jsonl` (`group_child`). The `responses` child gets no S3 pair.

A check without a result is recorded `interrupted` (still due) when the program stopped the
child, and `error` when the child ended by itself. Only the main thread calls these.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

from tests._live.calendar import NEW_YORK
from tests._live.live_run import children, outcomes
from tests._live.live_run import state as run_state
from tests._live.live_run.machine_checks import REQUIRED_DISK
from tests._live.live_run.outcomes import (
    COLLECTION_OPTION,
    OUTCOMES_OPTION,
    check_outcome,
    read_outcomes,
)
from tests._live.live_run.schedule import PYTEST_GROUPS, PYTEST_PATHS, due_names, half_day_suffix
from tests._live.live_run.state import Attempt, RunState, check_of, is_due, record

if TYPE_CHECKING:
    from pathlib import Path

    from tests._live.live_run.context import RunContext, RunMemory

BUILT_IN_GROUPS: Final = (
    "responses",
    "stream-day",
    "throughput",
    "stream-day-reconcile",
    "stream-night",
    "stream-night-reconcile",
)
"""The groups the program runs in a child of its own script."""

STREAM_GRACE: Final = timedelta(minutes=10)
"""How long after its own end a streaming child gets before the program stops it."""

STREAM_GROUPS: Final = ("stream-day", "throughput", "stream-night")
"""The groups whose child stops itself, two minutes before the group's stop."""

RECONCILED: Final = {
    "stream-day-reconcile": "stream-day",
    "stream-night-reconcile": "stream-night",
}
"""The streaming group whose folders and days each reconciliation compares with history."""

DISK_RETRY: Final = timedelta(minutes=5)
"""How long a stream day or night short of free disk waits before it checks the disk again."""


def new_attempt(ctx: RunContext, state: RunState, group: str) -> Attempt:
    """Create a group's next attempt and its folder.

    A stream day's later attempts of the same day reuse its first folder, so the day's files
    stay together and a restarted child records its gap there. A stream night's attempts are
    dated by the night's evening, even those that start after midnight, and share its folder
    the same way.

    Args:
        ctx: The command's context.
        state: The run's state; the attempt is appended.
        group: The group.

    Returns:
        The attempt, running, with a fresh token.
    """
    now = ctx.now()
    day = now.astimezone(NEW_YORK).date().isoformat()
    if group == "stream-night":
        day = state.stream_night
    same_day = [a for a in state.attempts if a.group == group and a.day == day]
    if group in {"stream-day", "stream-night"} and same_day:
        folder = same_day[0].folder
    else:
        folder = f"{group}/{day}-{len(same_day) + 1}"
    (ctx.out / folder).mkdir(parents=True, exist_ok=True)
    attempt = Attempt(
        group=group, folder=folder, day=day, started=now.isoformat(), token=children.new_token()
    )
    state.attempts.append(attempt)
    return attempt


def run_child(
    ctx: RunContext,
    state: RunState,
    attempt: Attempt,
    *,
    argv: list[str],
    env: dict[str, str],
    until: datetime,
) -> bool:
    """Start an attempt's child, wait for it until its stop, and record how it ended.

    Args:
        ctx: The command's context.
        state: The run's state, saved once the child's pid is known.
        attempt: The attempt; its pid, exit code, end, and status are set.
        argv: The child's command line.
        env: The child's environment.
        until: When the child is stopped.

    Returns:
        Whether the program stopped the child at `until`.

    Whatever interrupts the run once the child has started (Control-C, a hang-up, an error),
    even while its pid is being saved, stops the child, marks the attempt interrupted, and goes
    on.
    """
    child = children.start(
        argv,
        cwd=ctx.root,
        env=env,
        log=ctx.out / attempt.folder / "group.log",
        secrets_filter=ctx.secrets,
        echo=lambda line: None if is_startup_noise(line) else ctx.echo(f"  | {line[:96]}"),
    )
    try:
        attempt.pid = child.process.pid
        run_state.save(ctx.out, state)
        code, stopped = children.wait(child, deadline=until, now=ctx.now, grace=ctx.stop_grace)
    except BaseException:
        if child.process.poll() is None:  # interrupted before the wait, which stops it itself
            children.stop(child, grace=ctx.stop_grace)
        attempt.status, attempt.ended = "interrupted", ctx.now().isoformat()
        raise
    attempt.exit_code, attempt.ended, attempt.status = code, ctx.now().isoformat(), "finished"
    return stopped


STARTUP_NOISE: Final = (
    "# Starting io.deephaven",
    "# io.deephaven.",
    "deephaven.cacheDir=",
    "deephaven.configDir=",
    "deephaven.dataDir=",
    "Server started on port",
)
"""The starts of the lines an embedded Deephaven server prints as it starts. They still go to
`group.log`, but the terminal shows only what the tester may act on."""


def is_startup_noise(line: str) -> bool:
    """Tell whether a child's line is the embedded server's start-up chatter.

    Args:
        line: The line.

    Returns:
        True for a line starting with one of `STARTUP_NOISE`, or a JVM's count of compiler
        directives.
    """
    return line.startswith(STARTUP_NOISE) or line.endswith("compiler directives added")


def environment(ctx: RunContext, *, folder: Path, with_s3: bool = True) -> dict[str, str]:
    """Return a child's environment.

    Args:
        ctx: The command's context.
        folder: The attempt's folder, for its debug log.
        with_s3: Whether the child gets the S3 pair.

    Returns:
        The environment (`children.child_environment`).
    """
    return children.child_environment(ctx.environ, debug_log=folder / "debug.log", with_s3=with_s3)


def _pytest_argv(ctx: RunContext, attempt: Attempt, *, group: str, rest: list[str]) -> list[str]:
    """Return a pytest child's command line.

    Args:
        ctx: The command's context.
        attempt: The attempt, for its token.
        group: The group, for its selection.
        rest: Further arguments: `--collect-only -q` and its file, or the outcomes file and
            the due node ids.

    Returns:
        The command line.
    """
    plugins = ["-p", outcomes.__name__, "-p", "no:cacheprovider"]
    token = ["--live-run-token", attempt.token]
    return [ctx.python, "-m", "pytest", *plugins, *token, *PYTEST_GROUPS[group], *rest]


def collect(
    ctx: RunContext, state: RunState, attempt: Attempt, *, group: str, until: datetime
) -> dict[str, Any]:
    """List a pytest group's tests, and remember their node ids.

    Args:
        ctx: The command's context.
        state: The run's state; `known[group]` is replaced.
        attempt: The attempt.
        group: The group.
        until: When the collection is stopped.

    Returns:
        Each test's items, by node id; empty when the collection failed (its output is in
        `group.log`).
    """
    folder = ctx.out / attempt.folder
    target = folder / "collection.json"
    rest = ["--collect-only", "-q", COLLECTION_OPTION, str(target), *PYTEST_PATHS.get(group, ())]
    argv = _pytest_argv(ctx, attempt, group=group, rest=rest)
    env = environment(ctx, folder=folder)
    run_child(ctx, state, attempt, argv=argv, env=env, until=until)
    attempt.status = "running"
    collection: dict[str, Any] = {}
    if target.exists():
        collection = json.loads(target.read_text(encoding="utf-8"))
        state.known[group] = sorted(collection)
    return collection


EVIDENCE_FILES: Final = "live-evidence-*.log"
"""The names of the evidence files a pytest run writes in the repository root, one per run."""


def move_evidence(root: Path, folder: Path) -> None:
    """Move every evidence file of the repository root into a folder, never over one there.

    A file whose name the folder already holds gets a number before its suffix, such as
    `live-evidence-20261104T163000Z-2.log`.

    Args:
        root: The repository root, which may hold none.
        folder: Where they go; created when there is one to move.
    """
    for source in sorted(root.glob(EVIDENCE_FILES)):
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / source.name
        number = 1
        while target.exists():
            number += 1
            target = folder / f"{source.stem}-{number}{source.suffix}"
        shutil.move(source, target)


def run_pytest_group(
    ctx: RunContext,
    state: RunState,
    memory: RunMemory,
    *,
    group: str,
    until: datetime,
) -> None:
    """Run one attempt of a pytest group: collect, run the due tests, record each outcome.

    Args:
        ctx: The command's context.
        state: The run's state, saved after every change.
        memory: The run's memory.
        group: The group.
        until: When the attempt is stopped.

    Control-C stops the child; what finished is recorded, and the `KeyboardInterrupt` goes on.
    """
    attempt = new_attempt(ctx, state, group)
    folder = ctx.out / attempt.folder
    today = ctx.now().astimezone(NEW_YORK).date()
    suffix = half_day_suffix(today) if group == "half-day" else ""
    collection = collect(ctx, state, attempt, group=group, until=until)
    for nodeid, entry in collection.items():
        check = check_of(state, group, nodeid + suffix)
        check.items, check.issue = list(entry["items"]), entry["issue"]
    due = [
        n
        for n in sorted(collection)
        if is_due(check_of(state, group, n + suffix), today.isoformat())
    ]
    if not due:
        attempt.status, attempt.ended = "finished", ctx.now().isoformat()
        ctx.say(f"{group}: nothing due")
        run_state.save(ctx.out, state)
        return
    ctx.say(f"{group}: {len(due)} checks")
    move_evidence(ctx.root, ctx.out / "orphans")
    outcomes = folder / "outcomes.jsonl"
    rest = ["--tb=short", OUTCOMES_OPTION, str(outcomes), *due]
    argv = _pytest_argv(ctx, attempt, group=group, rest=rest)
    env = environment(ctx, folder=folder)
    stopped = True
    try:
        stopped = run_child(ctx, state, attempt, argv=argv, env=env, until=until)
    finally:
        move_evidence(ctx.root, folder)
        found = _pytest_results(outcomes, due)
        _record_all(
            ctx,
            state,
            memory,
            group=group,
            found={n + suffix: r for n, r in found.items()},
            stopped=stopped,
        )


def _pytest_results(outcomes: Path, due: list[str]) -> dict[str, tuple[str, str] | None]:
    """Turn the outcomes file into each due test's status and reason.

    Args:
        outcomes: The attempt's `outcomes.jsonl`.
        due: The tests run.

    Returns:
        Each test's `(status, reason)`, or None when it left no record.
    """
    by_test: dict[str, list[dict[str, Any]]] = {}
    for line in read_outcomes(outcomes):
        by_test.setdefault(line["nodeid"], []).append(line)
    return {n: check_outcome(by_test[n]) if n in by_test else None for n in due}


def record_results(
    state: RunState,
    *,
    group: str,
    found: dict[str, tuple[str, str] | None],
    stopped: bool,
    day: str,
) -> None:
    """Record each check's result.

    A check that left no record never started: it is `interrupted`, still due and not counted
    as a failure. One that started and never finished is an `error` when the child ended by
    itself, and `interrupted` when the program stopped it.

    Args:
        state: The run's state.
        group: The group.
        found: Each check's `(status, reason)`, or None when it left no record.
        stopped: Whether the program stopped the child.
        day: The attempt's New York date, ISO.
    """
    for name, result in found.items():
        status, reason = result if result is not None else ("interrupted", "")
        if status == "error" and stopped:
            status, reason = "interrupted", ""
        record(state, check_of(state, group, name), status=status, reason=reason, day=day)


def _record_all(
    ctx: RunContext,
    state: RunState,
    memory: RunMemory,
    *,
    group: str,
    found: dict[str, tuple[str, str] | None],
    stopped: bool,
) -> None:
    """Record each check's result (`record_results`), and save the state.

    Args:
        ctx: The command's context.
        state: The run's state.
        memory: The run's memory; it notes that a pack is due.
        group: The group.
        found: Each check's `(status, reason)`, or None.
        stopped: Whether the program stopped the child.
    """
    day = ctx.now().astimezone(NEW_YORK).date().isoformat()
    record_results(state, group=group, found=found, stopped=stopped, day=day)
    memory.finished_since_pack = True
    run_state.save(ctx.out, state)


def recover_stale_attempts(ctx: RunContext, state: RunState) -> list[Attempt]:
    """On resume, end what an earlier run left behind and keep what its children recorded.

    An attempt recorded as running belongs to a run that ended without stopping its child (a
    closed terminal, a crash). A child still alive, its token on its command line, is killed
    with its whole process group, so two children never share the key or the folder. Then the
    results its child wrote are recorded: each test's outcomes, or each built-in check's
    result. Checks without a result stay due.

    Args:
        ctx: The command's context.
        state: The run's state.

    Returns:
        The attempts recovered, now `interrupted`.
    """
    stale = [a for a in state.attempts if a.status == "running"]
    for attempt in stale:
        if children.is_alive(attempt.pid, attempt.token):
            children.kill_group(attempt.pid)
            ctx.say(f"stopped a child the last run left running ({attempt.group})")
        folder = ctx.out / attempt.folder
        found: dict[str, tuple[str, str] | None] = {}
        if attempt.group in PYTEST_GROUPS:
            by_test: dict[str, list[dict[str, Any]]] = {}
            for line in read_outcomes(folder / "outcomes.jsonl"):
                by_test.setdefault(line["nodeid"], []).append(line)
            day = date.fromisoformat(attempt.day)
            suffix = half_day_suffix(day) if attempt.group == "half-day" else ""
            found = {n + suffix: check_outcome(lines) for n, lines in by_test.items()}
        else:
            found = {
                r["name"]: (r["status"], r["reason"])
                for r in read_outcomes(folder / "results.jsonl")
            }
        record_results(state, group=attempt.group, found=found, stopped=True, day=attempt.day)
        attempt.status = "interrupted"
    return stale


def reconciled_stream(
    out: Path, streams: list[Attempt], due: list[str]
) -> tuple[Attempt, list[str]]:
    """Choose the streamed day or night a reconciliation compares, and the units it covers.

    Each due unit belongs to the latest attempt whose folder's `results.jsonl` reports it
    passed, or to the latest attempt when none does. The earliest of those attempts is
    compared, with its own units; the others stay due for a later attempt. So a reconciliation
    that runs only after the next night or day has streamed still compares each unit with the
    folder and date it streamed, not the newer ones.

    Args:
        out: The results folder.
        streams: The streaming group's attempts, oldest first; at least one.
        due: The reconciliation's due units.

    Returns:
        The attempt whose folder and day are compared, and the due units it streamed.
    """
    passed: dict[str, int] = {}
    for index, attempt in enumerate(streams):
        for line in read_outcomes(out / attempt.folder / "results.jsonl"):
            if line["status"] == "passed":
                passed[line["name"]] = index
    owner = {unit: passed.get(unit, len(streams) - 1) for unit in due}
    first = min(owner.values(), default=len(streams) - 1)
    return streams[first], [unit for unit in due if owner[unit] == first]


def _stream_source(
    out: Path, state: RunState, *, group: str, due: list[str]
) -> tuple[str, str, list[str]]:
    """Return the streamed folder and day a built-in group's request names, and its units.

    Args:
        out: The results folder.
        state: The run's state.
        group: The group.
        due: Its due units.

    Returns:
        The absolute folder of the streaming group's latest attempt, `""` before any; its latest
        day, ISO, `""` before any; and `due`. For a reconciliation with an attempt to compare,
        the folder and day of the streamed day or night it compares (`reconciled_stream`), and
        only the units it covers.
    """
    streamed = RECONCILED.get(group, group)
    streams = [a for a in state.attempts if a.group == streamed]
    days = state.stream_nights if streamed == "stream-night" else state.stream_days
    if group in RECONCILED and streams:
        source, due = reconciled_stream(out, streams, due)
        return str(out / source.folder), source.day, due
    folder = str(out / streams[-1].folder) if streams else ""
    return folder, days[-1] if days else "", due


def run_built_in_group(
    ctx: RunContext, state: RunState, memory: RunMemory, *, group: str, until: datetime
) -> None:
    """Run one attempt of a built-in group in a child of the program's own script.

    Args:
        ctx: The command's context.
        state: The run's state, saved after every change.
        memory: The run's memory.
        group: The group.
        until: When the group ends; a streaming child ends itself two minutes before, and is
            stopped `STREAM_GRACE` after.

    A stream day or night does not start with less free disk than
    `machine_checks.REQUIRED_DISK`: it creates no attempt, so the shortage spends none of its
    starts, and waits `DISK_RETRY` (never past its stop) before the run checks again; its
    units stay due. A reconciliation compares one streamed day or night per attempt
    (`reconciled_stream`). Control-C stops the child; what finished is recorded, and the
    `KeyboardInterrupt` goes on.
    """
    today = ctx.now().astimezone(NEW_YORK).date()
    due = due_names(state, group, today, any_time=group in state.any_time) or []
    if group in {"stream-day", "stream-night"} and (free := ctx.free_disk(ctx.out)) < REQUIRED_DISK:
        ctx.say(
            f"{group}: only {free >> 30} GB free under {ctx.out}; it needs "
            f"{REQUIRED_DISK >> 30} GB: free some space; the disk is checked again in "
            f"{DISK_RETRY.seconds // 60} minutes"
        )
        ctx.sleep_until(min(ctx.now() + DISK_RETRY, until))
        return
    attempt = new_attempt(ctx, state, group)
    folder = ctx.out / attempt.folder
    if group == "stream-day" and today.isoformat() not in state.stream_days:
        state.stream_days.append(today.isoformat())
    if group == "stream-night" and state.stream_night not in state.stream_nights:
        state.stream_nights.append(state.stream_night)
    stream_folder, stream_day, due = _stream_source(ctx.out, state, group=group, due=due)
    request = {
        "group": group,
        "due": due,
        "until": until.isoformat(),
        "stream_folder": stream_folder,
        "stream_day": stream_day,
    }
    (folder / "request.json").write_text(json.dumps(request, indent=1) + "\n", encoding="utf-8")
    ctx.say(f"{group}: {len(due)} checks")
    argv = [ctx.python, str(ctx.script), "_group", "--out", str(ctx.out)]
    argv += ["--attempt", attempt.folder, "--token", attempt.token]
    env = environment(ctx, folder=folder, with_s3=group != "responses")
    stop = until + STREAM_GRACE if group in STREAM_GROUPS else until
    stopped = True
    try:
        stopped = run_child(ctx, state, attempt, argv=argv, env=env, until=stop)
    finally:
        reported = {r["name"]: r for r in read_outcomes(folder / "results.jsonl")}
        found = {
            n: (reported[n]["status"], reported[n]["reason"]) if n in reported else None
            for n in due
        }
        _record_all(ctx, state, memory, group=group, found=found, stopped=stopped)
