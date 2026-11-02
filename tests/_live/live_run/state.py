"""The live run's state: every check's status, the attempts, and the subscriptions seen.

The live-run program (`live_run`) keeps everything it must remember between starts in one
file, `state.json` in the results folder, written atomically (a temporary file, then a rename)
after every change, so a crash or a Control-C never leaves it half written. It holds:

- the commit the run started on, which a resume must match;
- the *entitlements*: each distinct combination of the subscriptions' tiers and the flat-file
  prefixes the S3 pair reaches gets an id (`e1`, `e2`, ...), and the latest probe's id is the
  current one;
- the planned stream day, and the days a stream day ran;
- the planned stream night, the nights that ran, and the market closures Massive listed;
- each attempt: one run of a group, in its own folder, with its child's pid and token;
- each check: its status (`passed`, `failed`, `error`, `refused`, `skipped`, or `not run`), its
  reason, how many times it failed, and the entitlement of its last run and of its last pass.

`is_due` decides whether a check runs again; `record` stores a result by the rules that keep a
pass from being undone by a later skip. Everything here is pure but `load` and `save`.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from tests._live.plans import TIERS

STATE_FILE: Final = "state.json"
"""The state's file name in the results folder."""

STATE_VERSION: Final = 1
"""The layout of `state.json`; a file of another version is refused."""

NEED_PREFIXES: Final = ("needs ", "not entitled", "outside the plan's history")
"""The starts of a skip reason that names a subscription the key lacks, rather than a
condition of the moment (a closed market, a frame that did not arrive)."""

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
        status: `passed`, `failed`, `error`, `refused`, `skipped`, or `not run`.
        reason: Why it failed or was skipped; `""` otherwise.
        failures: How many of its results were `failed` or `error` (a stopped child's
            unfinished checks are not counted).
        condition_skips: How many times it was skipped for a condition of the moment.
        entitlement: The entitlement id of its last run.
        passed_under: The entitlement id of its last pass, `""` if it never passed.
        last_run: The New York date of its last run, ISO.
        items: The design's open questions it answers (`settles` and `verifies`).
        passed_rules: The `ROW_RULES` version in force at its last pass; 0 for a pass recorded
            before the version existed, or none.
    """

    group: str
    name: str
    status: str = "not run"
    reason: str = ""
    failures: int = 0
    condition_skips: int = 0
    entitlement: str = ""
    passed_under: str = ""
    last_run: str = ""
    items: list[str] = field(default_factory=list)
    passed_rules: int = 0


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
        entitlements: Each entitlement id's tiers (`{"stocks": "starter", ...}`) and flat-file
            prefixes, as `{"tiers": ..., "flat_files": [...]}`.
        current: The latest probe's entitlement id, `""` before the first probe.
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
    entitlements: dict[str, dict[str, Any]] = field(default_factory=dict)
    current: str = ""
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


def entitlement_id(state: RunState, tiers: dict[str, str], flat_files: list[str]) -> str:
    """Return the id of a combination of tiers and flat-file prefixes, adding it if new.

    Args:
        state: The run's state; a new id is added to it.
        tiers: Each subscription's tier.
        flat_files: The prefixes the S3 pair reaches.

    Returns:
        Its id, such as `"e2"`.
    """
    wanted = {"tiers": dict(tiers), "flat_files": sorted(flat_files)}
    for name, stored in state.entitlements.items():
        if stored == wanted:
            return name
    name = f"e{len(state.entitlements) + 1}"
    state.entitlements[name] = wanted
    return name


def tier_rose(state: RunState, before: str, after: str) -> bool:
    """Tell whether any subscription's tier is higher under one entitlement than another.

    Args:
        state: The run's state.
        before: The earlier entitlement id; `""` counts as nothing known, so nothing rose.
        after: The later entitlement id.

    Returns:
        True when some subscription's tier, in each class's order cheapest first, rose.
    """
    if not before or not after or before == after:
        return False
    old = state.entitlements[before]["tiers"]
    new = state.entitlements[after]["tiers"]
    return any(TIERS[name].index(new[name]) > TIERS[name].index(old[name]) for name in TIERS)


def is_need(reason: str) -> bool:
    """Tell whether a skip names a subscription the key lacks.

    Args:
        reason: The skip's reason.

    Returns:
        True when it starts with one of `NEED_PREFIXES`.
    """
    return reason.startswith(NEED_PREFIXES)


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


def is_due(state: RunState, check: Check) -> bool:
    """Decide whether a check runs at its group's next attempt.

    A check is due unless it passed and no tier rose since; it failed `MAX_FAILURES` times;
    it was refused, or skipped for a subscription the key lacks, and the entitlement has not
    changed since; or it was skipped for a condition `MAX_CONDITION_SKIPS` times. A check that
    needs rows (`needs_rows`) and passed under an older `ROW_RULES` proved nothing, so it is
    due until it passes again.

    Args:
        state: The run's state.
        check: The check.

    Returns:
        True when it should run.
    """
    if check.status == "passed":
        if needs_rows(check) and check.passed_rules < ROW_RULES:
            return True
        return tier_rose(state, check.passed_under, state.current)
    if check.failures >= MAX_FAILURES:
        return False
    if check.status == "refused" or (check.status == "skipped" and is_need(check.reason)):
        return check.entitlement != state.current
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
    """Store one result of a check, under the current entitlement.

    A skip never replaces a pass: the pass stays, and counts as made under the current
    entitlement, so a raised tier's rerun that skips is not run again. The unfinished checks
    of a child stopped on purpose (Control-C, a hard stop) come as `interrupted` and are not
    stored, so they stay due without counting as a failure.

    Args:
        state: The run's state.
        check: The check.
        status: `passed`, `failed`, `error`, `refused`, `skipped`, or `interrupted` (left as
            it was, still due).
        reason: The reason; `""` for a pass.
        day: The New York date of the run, ISO.
    """
    if status == "interrupted":
        return
    check.last_run = day
    if status == "skipped" and check.status == "passed":
        check.passed_under = state.current
        check.entitlement = state.current
        return
    check.status, check.reason, check.entitlement = status, reason, state.current
    if status == "passed":
        check.passed_under = state.current
        check.passed_rules = ROW_RULES
    elif status in {"failed", "error"}:
        check.failures += 1
    elif status == "skipped" and not is_need(reason):
        check.condition_skips += 1


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
            check.passed_under = ""


def tiers_text(state: RunState, entitlement: str) -> str:
    """Spell out an entitlement's subscriptions for a person.

    Args:
        state: The run's state.
        entitlement: The entitlement id, or `""`.

    Returns:
        Such as `"stocks=starter, options=basic, ...; flat files: us_stocks_sip/day_aggs_v1"`,
        or `""` for no entitlement.
    """
    if not entitlement:
        return ""
    stored = state.entitlements[entitlement]
    tiers = ", ".join(f"{name}={tier}" for name, tier in stored["tiers"].items())
    files = ", ".join(stored["flat_files"]) or "none"
    return f"{tiers}; flat files: {files}"
