"""The live run's state: every check's status, the attempts, and the subscriptions seen.

The live-run program (`live_run`) keeps everything it must remember between starts in one
file, `state.json` in the results folder, written atomically (a temporary file, then a rename)
after every change, so a crash or a Control-C never leaves it half written. It holds:

- the commit the run started on, which a resume must match;
- the planned stream day, and the days a stream day ran;
- the planned stream night, the nights that ran, and the market closures Massive listed;
- each attempt: one run of a group, in its own folder, with its child's pid and token;
- each check: its status (`passed`, `failed`, `error`, `refused`, `skipped`, or `not run`), its
  reason, how many times it failed, and, once Massive refused it or it skipped because the
  owner's settings do not list its subscription as delayed, the subscription that names.

The program never knows what the key holds: it learns it from Massive's answers. A check
Massive refused, or skipped for its delayed listing, runs again at its group's first attempt of
each day, and when another check of the same subscription stops being refused or unlisted (the
key gained it, or the settings now list it), at once: its group may then run again the same
evening (`reruns`). `is_due` decides whether a check runs again; `record` stores
a result by the rules that keep a pass from being undone by a later skip. Everything here is
pure but `load` and `save`.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import tempfile
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from tests._live.classes import class_of_path
from tests._live.live_run.stream_units import NIGHT_UNITS, SUBSCRIPTIONS
from tests._live.summary import NOT_LISTED

STATE_FILE: Final = "state.json"
"""The state's file name in the results folder."""

STATE_VERSION: Final = 2
"""The layout of `state.json`; a file of another version is refused."""

_REQUEST_PATH: Final = re.compile(r" GET (/[^\s?]+)")
"""Where a refusal's reason names the refused request's path (`refusals.refusal_line`)."""

_SUBSCRIPTION_WORD: Final = re.compile(
    r"(?<![a-z])(stocks|options|indices|currencies|futures|crypto|forex)(?![a-z])"
)
"""A subscription, or a namespace of the Currencies subscription, named in a check's name."""

MAX_FAILURES: Final = 2
"""Failures after which a check is final until `--redo`."""

MAX_CONDITION_SKIPS: Final = 3
"""Attempts a check skipped for a condition gets before the skip is final."""

ROW_RULES: Final = 1
"""The version of the rule that a stream check passes only when rows reached its table. Before
it, a stream passed on Massive's acknowledgement alone, which Massive sends with the market
closed too, so a pass recorded under an older version proves nothing and runs again."""

DELAYED_FEED_TEST: Final = "test_whether_massive_serves_a_real_time_key_on_the_delayed_feed"
"""The check of a real-time key on the delayed feed, which now also requires rows."""


@dataclass
class Check:
    """One check: a live test, a documentation example, or a streaming unit.

    Attributes:
        group: The group that runs it, such as `"evidence"`.
        name: Its name in the group: a pytest node id, an example's name, or a unit.
        status: `passed`, `failed`, `error`, `refused`, `skipped`, `problem present`, `problem
            fixed`, or `not run`.
        reason: Why it failed or was skipped; `""` otherwise.
        failures: How many of its results were `failed` or `error` (a stopped child's
            unfinished checks are not counted).
        condition_skips: How many times it was skipped for a condition of the moment.
        last_run: The New York date of its last run, ISO.
        items: The open verification items it answers (`settles` and `verifies`).
        issue: The GitHub issue of the open Massive problem it checks (`massive_issue`), or
            None for a check of none.
        passed_rules: The `ROW_RULES` version in force at its last pass; 0 for a pass recorded
            before the version existed, or none.
        subscription: The subscription its latest refusal, or skip for its delayed listing,
            names (`refused_subscription`); `""` when it had neither or the reason names none.
        rerun: Whether another check of its subscription stopped being refused or unlisted
            since its last run, which makes it due at once.
    """

    group: str
    name: str
    status: str = "not run"
    reason: str = ""
    failures: int = 0
    condition_skips: int = 0
    last_run: str = ""
    items: list[str] = field(default_factory=list)
    issue: int | None = None
    passed_rules: int = 0
    subscription: str = ""
    rerun: bool = False


@dataclass
class Attempt:
    """One run of a group in its own child process and folder.

    Attributes:
        group: The group.
        folder: The attempt's folder, relative to the results folder, such as
            `"evidence/2026-11-03-1"`.
        day: The New York date it started, ISO.
        started: When it started, ISO UTC.
        status: `running`, `finished`, or `interrupted`.
        pid: The child's process id, 0 before it starts.
        token: The random token on the child's command line, which proves a pid is still the
            child.
        ended: When it ended, ISO UTC; `""` while running.
        exit_code: The child's exit status, None while running or when it was killed.
    """

    group: str
    folder: str
    day: str
    started: str
    status: str = "running"
    pid: int = 0
    token: str = ""
    ended: str = ""
    exit_code: int | None = None


@dataclass
class RunState:
    """Everything the live run remembers, as stored in `state.json`.

    Created by `live_run` on a first start, then read back with `load` on every later one.
    Only the program's main thread reads or changes it.

    Attributes:
        commit: The repository commit of the first start.
        stream_day: The planned stream day, ISO, or `""`.
        stream_days: The days a stream day ran, ISO.
        stream_night: The planned stream night's evening, ISO, or `""`.
        stream_nights: The evenings a stream night ran, ISO.
        closures: Massive's upcoming market closures at the latest probe that read them: each
            ISO date's status, `closed` or `early-close` (`run_probe.ProbeResult.closures`).
        attempts: Every attempt, oldest first.
        checks: Every check, by `key(group, name)`.
        known: Each pytest group's node ids at its latest collection.
        any_time: The `--any-time` groups not yet run. `--any-time` takes effect once per
            results folder: a resume finishes this list and ignores the flag.
        any_time_used: Whether `--any-time` was ever given for this folder.
        salt: The random salt of the secrets' fingerprints.
        fingerprints: A salted HMAC of each secret variable the latest run had set, by name
            (`secret_scrub.fingerprints`), never a value: `pack` requires the same ones.
    """

    commit: str
    stream_day: str = ""
    stream_days: list[str] = field(default_factory=list)
    stream_night: str = ""
    stream_nights: list[str] = field(default_factory=list)
    closures: dict[str, str] = field(default_factory=dict)
    attempts: list[Attempt] = field(default_factory=list)
    checks: dict[str, Check] = field(default_factory=dict)
    known: dict[str, list[str]] = field(default_factory=dict)
    any_time: list[str] = field(default_factory=list)
    any_time_used: bool = False
    salt: str = ""
    fingerprints: dict[str, str] = field(default_factory=dict)


def key(group: str, name: str) -> str:
    """Return the key of a check in `RunState.checks`.

    Args:
        group: The check's group.
        name: Its name.

    Returns:
        `"<group> <name>"`.
    """
    return f"{group} {name}"


def load(folder: Path) -> RunState | None:
    """Read the state of a results folder.

    Args:
        folder: The results folder.

    Returns:
        The state, or None when the folder has none yet.

    Raises:
        ValueError: When the file is of another layout version.
    """
    path = folder / STATE_FILE
    if not path.exists():
        return None
    stored = json.loads(path.read_text(encoding="utf-8"))
    if stored.get("version") != STATE_VERSION:
        msg = f"{path} has layout version {stored.get('version')}, expected {STATE_VERSION}"
        raise ValueError(msg)
    del stored["version"]
    stored["attempts"] = [Attempt(**a) for a in stored["attempts"]]
    stored["checks"] = {k: Check(**c) for k, c in stored["checks"].items()}
    return RunState(**stored)


def save(folder: Path, state: RunState) -> None:
    """Write the state atomically: to a temporary file in the folder, then renamed over it.

    Args:
        folder: The results folder.
        state: The state.
    """
    text = json.dumps({"version": STATE_VERSION, **dataclasses.asdict(state)}, indent=1)
    handle, temporary = tempfile.mkstemp(dir=folder, prefix=".state-", suffix=".json")
    with os.fdopen(handle, "w", encoding="utf-8") as file:
        file.write(text + "\n")
    Path(temporary).replace(folder / STATE_FILE)


def refused_subscription(name: str, reason: str) -> str:
    """Name the subscription a refused or unlisted check's reason is about.

    Args:
        name: The check's name: a node id, an example, or a streaming unit.
        reason: The refusal, as Massive worded it (`refusals.refusal_line`), or the skip for
            the delayed listing (`summary.NOT_LISTED` and the library's message).

    Returns:
        The class of the refused request's path when the reason names one
        (`classes.class_of_path`); else a streaming unit's subscription; else the first
        subscription the name, then the reason, names as a word (`crypto` and `forex` are
        the Currencies subscription's); else `""`.
    """
    path = _REQUEST_PATH.search(reason)
    found = class_of_path(urllib.parse.unquote(path.group(1))) if path is not None else None
    if found is not None:
        return found
    if name in SUBSCRIPTIONS or name in NIGHT_UNITS:
        return NIGHT_UNITS.get(name, name)
    word = _SUBSCRIPTION_WORD.search(name) or _SUBSCRIPTION_WORD.search(reason)
    if word is None:
        return ""
    return {"crypto": "currencies", "forex": "currencies"}.get(word.group(1), word.group(1))


PROBLEM_STATUSES: Final = frozenset({"problem present", "problem fixed"})
"""The statuses of a check of an open Massive problem that ran: the problem still shows, or
Massive fixed it."""


def is_daily(check: Check) -> bool:
    """Tell whether a check's result may change only with what the key holds or the settings say.

    Args:
        check: The check.

    Returns:
        True when Massive refused it, or it skipped because the owner's settings do not list
        its subscription as delayed (`summary.NOT_LISTED`).
    """
    return check.status == "refused" or (
        check.status == "skipped" and check.reason.startswith(NOT_LISTED)
    )


def needs_rows(check: Check) -> bool:
    """Tell whether a check must see rows to pass: a stream that once passed on its acknowledgement.

    Args:
        check: The check.

    Returns:
        True for a stream-day unit, a smoke tier stream case (its id ends `-stream]`), and the
        check of a real-time key on the delayed feed.
    """
    return (
        check.group == "stream-day"
        or check.name.endswith("-stream]")
        or DELAYED_FEED_TEST in check.name
    )


def is_due(check: Check, today: str) -> bool:
    """Decide whether a check runs at its group's next attempt.

    A check is due unless it passed; it showed Massive's fix of an open problem (`problem
    fixed`); it showed the problem still present and already ran today, since Massive may fix
    it any day; it failed `MAX_FAILURES` times; it was refused, or skipped for its delayed
    listing (`is_daily`), and already ran today, unless another check of its subscription
    stopped being refused or unlisted since (`Check.rerun`); or it was skipped for a condition
    `MAX_CONDITION_SKIPS` times. A check that needs rows (`needs_rows`) and passed
    under an older `ROW_RULES` proved nothing, so it is due until it passes again.

    Args:
        check: The check.
        today: The New York date of the attempt, ISO.

    Returns:
        True when it should run.
    """
    if check.status == "passed":
        return needs_rows(check) and check.passed_rules < ROW_RULES
    if check.status == "problem fixed" or check.failures >= MAX_FAILURES:
        return False
    if is_daily(check) or check.status == "problem present":
        return check.last_run != today or check.rerun
    if check.status == "skipped":
        return check.condition_skips < MAX_CONDITION_SKIPS
    return True


def check_of(state: RunState, group: str, name: str) -> Check:
    """Return a check, adding it as `not run` when it is new.

    Args:
        state: The run's state.
        group: Its group.
        name: Its name.

    Returns:
        The stored check.
    """
    return state.checks.setdefault(key(group, name), Check(group=group, name=name))


def record(state: RunState, check: Check, *, status: str, reason: str, day: str) -> None:
    """Store one result of a check.

    A skip never replaces a pass. The unfinished checks of a child stopped on purpose
    (Control-C, a hard stop) come as `interrupted` and are not stored, so they stay due
    without counting as a failure. A refusal, or a skip whose reason starts with `NOT_LISTED`,
    stores the subscription it names (`refused_subscription`). When a refused check's new
    result is one Massive answered (a pass, a failure, a check of an open Massive problem that
    ran, or a skip whose reason starts with
    `NOT_LISTED`), the key gained something; when an unlisted check's is a pass or a failure,
    the settings now list its subscription. Either way every other refused or unlisted check
    of the same subscription (`is_daily`) is marked to run again at once (`Check.rerun`). A
    skip for any other reason, such as a closed market or a stream that sent nothing, says
    nothing of the key and marks nothing.

    Args:
        state: The run's state.
        check: The check.
        status: `passed`, `failed`, `error`, `refused`, `skipped`, `problem present` or
            `problem fixed` (a check of an open Massive problem), or `interrupted` (left as it
            was, still due).
        reason: The reason; `""` for a pass.
        day: The New York date of the run, ISO.

    Raises:
        ValueError: When the status is none of those above; the check is left as it was.
    """
    if status == "interrupted":
        return
    if status == "skipped" and check.status == "passed":
        check.last_run, check.rerun = day, False
        return
    unlisted = status == "skipped" and reason.startswith(NOT_LISTED)
    cleared = status in {"passed", "failed", *PROBLEM_STATUSES} or (
        unlisted and check.status == "refused"
    )
    freed = check.subscription if is_daily(check) and cleared else ""
    if status == "refused" or unlisted:
        check.subscription = refused_subscription(check.name, reason)
    elif status == "passed":
        check.passed_rules = ROW_RULES
    elif status in {"failed", "error"}:
        check.failures += 1
    elif status == "skipped":
        check.condition_skips += 1
    elif status not in PROBLEM_STATUSES:
        msg = (
            f"{check.name}: unknown status {status!r}; expected passed, failed, error, "
            "refused, skipped, problem present, problem fixed, or interrupted"
        )
        raise ValueError(msg)
    check.last_run, check.rerun = day, False
    check.status, check.reason = status, reason
    if freed:
        for other in state.checks.values():
            if other is not check and is_daily(other):
                other.rerun = other.rerun or other.subscription == freed


def reruns(state: RunState, group: str) -> bool:
    """Tell whether a group must run again today: a refused or unlisted subscription cleared.

    Args:
        state: The run's state.
        group: The group.

    Returns:
        True when one of its checks is marked to run again at once (`Check.rerun`).
    """
    return any(c.rerun for c in state.checks.values() if c.group == group)


def redo(state: RunState, group: str) -> None:
    """Make every check of a group due again, failed ones included; earlier attempts stay.

    Args:
        state: The run's state.
        group: The group named by `--redo`.
    """
    for check in state.checks.values():
        if check.group == group:
            check.status, check.reason = "not run", ""
            check.failures = check.condition_skips = 0
