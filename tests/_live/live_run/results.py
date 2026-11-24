"""The live run's results for people: the status view, `checks.csv`, `REPORT.md`, and the zip.

The live-run program (`live_run`) keeps every check's status in its state (`run_state`). This
module turns that state into what the tester and the owner read:

- `status_lines`, the `status` command's view: the latest probe, the next stream day and
  stream night, every check with its status and its reason (for a refusal, Massive's own
  words), and when its group runs next, then one summary row per group.
- `write_checks_csv`, one line per check, and `write_report`, `REPORT.md`: the environment, the
  probes over time, one table per group, what Massive refused, by day (`refused_lines`), each
  stream night's findings (`night_lines`), the checks of open Massive problems, by GitHub
  issue, fixed or still present (`problem_lines`), the open verification items of
  `docs/dev/verification-items/` (from the tests' `settles` and `verifies` markers) with each
  one's status, and every check still unverified, grouped by why.
- `pack`, which writes both, scans every file of the results folder for the secrets
  (`secret_scrub.scan`), and only when none is found zips the folder to
  `live-results-<New York date>.zip`.

Every function reads or writes only the results folder.
"""

from __future__ import annotations

import csv
import json
import zipfile
from datetime import date
from typing import TYPE_CHECKING, Final

from tests._live import massive_problems
from tests._live.live_run import timetable
from tests._live.live_run.secret_scrub import scan
from tests._live.live_run.state import PROBLEM_STATUSES, is_daily
from tests._live.massive_problems import FIXED, PRESENT
from tests._live.summary import item_order

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from tests._live.live_run.secret_scrub import SecretFilter
    from tests._live.live_run.state import Check, RunState

STATUSES: Final = (
    "passed",
    "failed",
    "error",
    "refused",
    "skipped",
    "problem present",
    "problem fixed",
    "not run",
)
"""Every status a check can show, in the order of the summary's columns."""

GROUP_ORDER: Final = (
    "rate-limits",
    "evidence",
    "luld",
    "smoke",
    "responses",
    "limits-ws",
    "limits-second-connection",
    "stream-day",
    "throughput",
    "stream-day-reconcile",
    "stream-night",
    "stream-night-reconcile",
    "half-day",
    "upgrade-check",
)
"""The groups in the order the report lists them."""

UNLISTED_ITEMS: Final = frozenset({"13", "17", "18"})
"""Open verification items with no live test: checked by CI, retired, and settled without a
key, so the report leaves them out."""


def status_text(check: Check) -> str:
    """Describe a check's status in one phrase.

    Args:
        check: The check.

    Returns:
        Such as `"passed"`, `"refused: HTTP 403 from GET ..."` (a reason that starts with the
        status says it once), or, for the upgrade check that never ran, `"not started
        (optional)"`.
    """
    if check.group == "upgrade-check" and check.status == "not run":
        return "not started (optional)"
    reason = check.reason.removeprefix(f"{check.status}: ")
    return f"{check.status}: {reason}" if reason else check.status


def why_unverified(check: Check) -> str:
    """Name why a check is not verified, for grouping in the report.

    Args:
        check: A check that has not passed.

    Returns:
        `"refused by Massive (<subscription>)"` for a refusal (without the subscription when
        the refusal names none), `"not listed as delayed"` for a skip of the owner's delayed
        listing, else its status.
    """
    if not is_daily(check):
        return check.status
    if check.status == "skipped":
        return "not listed as delayed"
    named = f" ({check.subscription})" if check.subscription else ""
    return f"refused by Massive{named}"


def _ordered(checks: Iterable[Check]) -> list[Check]:
    """Sort checks by group, in report order, then name.

    Args:
        checks: The checks.

    Returns:
        The sorted checks.
    """
    order = {group: index for index, group in enumerate(GROUP_ORDER)}
    return sorted(checks, key=lambda c: (order.get(c.group, len(order)), c.name))


def night_text(state: RunState) -> str:
    """Describe the planned stream night for a person.

    Args:
        state: The run's state.

    Returns:
        Such as `"Sun 2026-11-08 16:30 EST to Mon 09:30 EST"`, or `"none planned"`.
    """
    if not state.stream_night:
        return "none planned"
    evening = date.fromisoformat(state.stream_night)
    start, end = timetable.night_start(evening), timetable.night_end(evening)
    return f"{start:%a %Y-%m-%d %H:%M %Z} to {end:%a %H:%M %Z}"


def night_lines(folder: Path, state: RunState) -> list[str]:
    """Return the `REPORT.md` section of every stream night that wrote its summary.

    Args:
        folder: The results folder.
        state: The run's state, whose attempts name each night's folder.

    Returns:
        Per night: its gaps, the most reconnects per connection, each unit's quiet hours, and
        a table of the rows on each side of each boundary with their `Date` values; a line
        saying none ran when no night wrote a summary.
    """
    lines: list[str] = []
    nights = dict.fromkeys(a.folder for a in state.attempts if a.group == "stream-night")
    for name in nights:
        path = folder / name / "summary.json"
        if not path.exists():
            continue
        night = json.loads(path.read_text(encoding="utf-8"))
        reconnects = ", ".join(f"{k} {v}" for k, v in night["reconnects"].items()) or "none"
        lines += ["", f"### {night['night']} ({name})", ""]
        lines.append(f"- Gaps: {night['gaps_minutes']} minutes. Reconnects: {reconnects}.")
        for unit, hours in night["quiet_hours"].items():
            lines.append(f"- {unit}: quiet hours (UTC): {', '.join(hours) or 'none'}.")
        header = ["stream", "boundary", "side", "rows", "dates", "expected", "wrong"]
        lines.append("")
        lines += _table(header, ([str(b[h]) for h in header] for b in night["boundaries"]))
    return lines or ["", "No stream night has run."]


def summary_rows(state: RunState) -> list[dict[str, object]]:
    """Count each group's checks by status.

    Args:
        state: The run's state.

    Returns:
        One row per group that has checks: the group, then one count per status.
    """
    counts: dict[str, dict[str, object]] = {}
    for check in _ordered(state.checks.values()):
        row = counts.setdefault(check.group, {"group": check.group} | dict.fromkeys(STATUSES, 0))
        row[check.status] = int(str(row[check.status])) + 1
    return list(counts.values())


def status_lines(
    state: RunState, *, latest_probe: dict[str, object] | None, next_start: Callable[[str], str]
) -> list[str]:
    """Return the `status` command's view of a run.

    Args:
        state: The run's state.
        latest_probe: The latest probe's record, or None before the first probe.
        next_start: When a group next runs, as printable text, such as `"Tue 2026-11-03
            11:30 EST"`.

    Returns:
        The lines to print.
    """
    lines = []
    if latest_probe is None:
        lines.append("latest probe: none yet")
    else:
        lines.append(f"latest probe: {latest_probe['when']}")
    lines.append(f"next stream day: {state.stream_day or 'none planned'}")
    lines.append(f"next stream night: {night_text(state)}")
    group = ""
    for check in _ordered(state.checks.values()):
        if check.group != group:
            group = check.group
            lines.append(f"{group} (next: {next_start(group)})")
        lines.append(f"  {check.name}: {status_text(check)}")
    lines.append("")
    header = ["group", *STATUSES]
    lines.append(" | ".join(header))
    lines.extend(" | ".join(str(row[h]) for h in header) for row in summary_rows(state))
    return lines


def write_checks_csv(folder: Path, state: RunState) -> None:
    """Write `checks.csv`: one line per check.

    Args:
        folder: The results folder.
        state: The run's state.
    """
    with (folder / "checks.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["group", "name", "status", "reason", "last_run", "refused_subscription"])
        for check in _ordered(state.checks.values()):
            writer.writerow(
                [
                    check.group,
                    check.name,
                    check.status,
                    check.reason,
                    check.last_run,
                    check.subscription if check.status == "refused" else "",
                ]
            )


def item_status(checks: list[Check]) -> str:
    """Return an open verification item's status from its checks' latest outcomes.

    Args:
        checks: The checks that answer the item.

    Returns:
        `evidence recorded` when all passed, `partly run` when some never ran, why the others
        are unverified (`why_unverified`) when they were refused or not listed as delayed and
        the rest passed or skipped, else `failed`.
    """
    if all(c.status == "passed" for c in checks):
        return "evidence recorded"
    if any(c.status == "not run" for c in checks):
        return "partly run"
    why = sorted({why_unverified(c) for c in checks if is_daily(c)})
    if why and all(c.status in {"passed", "skipped", "refused"} for c in checks):
        return "; ".join(why)
    return "failed"


def refused_lines(checks: Iterable[Check]) -> list[str]:
    """Return the `REPORT.md` table of what Massive refused, by the day of its latest refusal.

    Args:
        checks: The checks.

    Returns:
        A table of each refused check's day, subscription, name, and Massive's refusal,
        oldest day first; a line saying Massive refused nothing when none is refused.
    """
    refused = sorted(
        (c for c in checks if c.status == "refused"), key=lambda c: (c.last_run, c.group, c.name)
    )
    if not refused:
        return ["Massive refused nothing."]
    rows = [[c.last_run, c.subscription, f"{c.group}: {c.name}", c.reason] for c in refused]
    return _table(["day", "subscription", "check", "refusal"], rows)


def problem_lines(checks: Iterable[Check]) -> list[str]:
    """Return the `REPORT.md` lines of the checks of open Massive problems that ran.

    Every check of an issue that any check ran for is listed, those not run too, so an issue
    is FIXED only when every check of it showed the fix.

    Args:
        checks: The checks.

    Returns:
        The issues sorted by what their checks showed (`massive_problems.problem_lines`), as a
        text block, then a table of each such check's issue, name, status, and reason
        (Massive's values for a problem still present); a line saying none ran when none did.
    """
    marked = [(c.issue, c) for c in checks if c.issue is not None]
    attempted = {issue for issue, c in marked if c.status != "not run"}
    listed = [(issue, c) for issue, c in marked if issue in attempted]
    if not listed:
        return ["No check of an open Massive problem ran."]
    shown = {"problem present": PRESENT, "problem fixed": FIXED}
    outcomes: dict[str, tuple[int, str]] = {}
    for issue, c in listed:
        # Any other status (failed, refused, skipped, error, not run) is what happened, as it is.
        happened = f"{c.status}: {c.reason}" if c.reason else c.status
        outcomes[f"{c.group}: {c.name}"] = (issue, shown.get(c.status, happened))
    rows = [[f"#{issue}", f"{c.group}: {c.name}", c.status, c.reason] for issue, c in listed]
    return [
        "```text",
        *massive_problems.problem_lines(outcomes),
        "```",
        "",
        *_table(["issue", "check", "status", "Massive's values or what to do"], rows),
    ]


def _table(header: list[str], rows: Iterable[list[str]]) -> list[str]:
    """Return a Markdown table.

    Args:
        header: The column names.
        rows: The rows.

    Returns:
        Its lines.
    """
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines.extend("| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |" for row in rows)
    return lines


def write_report(folder: Path, state: RunState) -> None:
    """Write `REPORT.md`, the run's results for a person.

    Args:
        folder: The results folder.
        state: The run's state.
    """
    lines = ["# Live run report", "", f"Commit: `{state.commit}`", "", "## Environment", ""]
    environment = folder / "preflight" / "environment.json"
    if environment.exists():
        stored = json.loads(environment.read_text(encoding="utf-8"))
        rows = [
            [str(i["name"]), str(i["verdict"]), str(i["detail"])] for i in stored.get("items", [])
        ]
        lines.extend(_table(["item", "verdict", "detail"], rows))
    else:
        lines.append("No environment was recorded.")
    lines.extend(["", "## Probes", ""])
    probes = folder / "probes" / "probes.csv"
    if probes.exists():
        with probes.open(encoding="utf-8") as file:
            records = list(csv.reader(file))
        lines.extend(_table(records[0], records[1:]))
    else:
        lines.append("No probe was recorded.")
    checks = _ordered(state.checks.values())
    for group in dict.fromkeys(c.group for c in checks):
        lines.extend(["", f"## {group}", ""])
        rows = [[c.name, status_text(c), c.last_run] for c in checks if c.group == group]
        lines.extend(_table(["check", "status", "last run"], rows))
    lines.extend(["", "## What Massive refused, by day", "", *refused_lines(checks)])
    lines.extend(["", "## Stream nights", *night_lines(folder, state)])
    lines.extend(["", "## Open verification items", ""])
    items: dict[str, list[Check]] = {}
    for check in checks:
        for item in check.items:
            if item not in UNLISTED_ITEMS:
                items.setdefault(item, []).append(check)
    rows = [[item, item_status(items[item])] for item in sorted(items, key=item_order)]
    lines.extend(_table(["item", "status"], rows))
    lines.extend(["", "## Massive problems, by GitHub issue", "", *problem_lines(checks)])
    lines.extend(["", "## Still unverified", ""])
    waiting: dict[str, list[str]] = {}
    for check in checks:
        if check.status != "passed" and check.status not in PROBLEM_STATUSES:
            why = why_unverified(check)
            waiting.setdefault(why, []).append(f"{check.group}: {check.name}")
    lines.extend(f"- **{why}**: " + ", ".join(waiting[why]) for why in sorted(waiting))
    (folder / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def pack(
    folder: Path, state: RunState, secrets: SecretFilter, *, day: str
) -> tuple[Path | None, list[str]]:
    """Write the report files, check that no secret is anywhere, and zip the results.

    Args:
        folder: The results folder.
        state: The run's state.
        secrets: The run's secrets; the scan looks for each of them.
        day: The New York date, ISO, which names the zip.

    Returns:
        The zip's path and no finds; or None and every find, when a secret was found and
        nothing was zipped. Parquet files, already compressed, are stored as they are. Once
        the new zip is written, the older ones are deleted, since each zip holds everything.
    """
    write_checks_csv(folder, state)
    write_report(folder, state)
    skip = [*folder.glob("live-results-*.zip"), folder / "run.lock"]
    finds = scan(folder, secrets, skip=skip)
    if finds:
        return None, finds
    target = folder / f"live-results-{day}.zip"
    partial = target.with_suffix(".zip.partial")
    with zipfile.ZipFile(partial, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in folder.rglob("*") if p.is_file()):
            if path in skip or path == partial or path.name.startswith(".state-"):
                continue
            stored = zipfile.ZIP_STORED if path.suffix == ".parquet" else zipfile.ZIP_DEFLATED
            archive.write(path, path.relative_to(folder).as_posix(), compress_type=stored)
    partial.replace(target)
    for older in folder.glob("live-results-*.zip"):
        if older != target:
            older.unlink()
    return target, []
