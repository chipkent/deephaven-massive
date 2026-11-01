"""The live run's results for people: the status view, `checks.csv`, `REPORT.md`, and the zip.

The live-run program (`live_run`) keeps every check's status in its state (`run_state`). This
module turns that state into what the tester and the owner read:

- `status_lines`, the `status` command's view: the latest probe, the next stream day, every
  check with its status, its reason, the subscriptions of its last run, and when its group runs
  next, then one summary row per group. Skip reasons drop the monthly prices the live tests
  add, so they read `skipped: needs Stocks Starter`.
- `write_checks_csv`, one line per check, and `write_report`, `REPORT.md`: the environment, the
  probes over time, one table per group, the design's open verification items (from the tests'
  `settles` and `verifies` markers) with each one's status, and every check still unverified,
  grouped by what it needs.
- `pack`, which writes both, scans every file of the results folder for the secrets
  (`secret_scrub.scan`), and only when none is found zips the folder to
  `live-results-<New York date>.zip`.

Every function reads or writes only the results folder.
"""

from __future__ import annotations

import csv
import json
import re
import zipfile
from typing import TYPE_CHECKING, Final

from tests._live.live_run.secret_scrub import scan
from tests._live.live_run.state import is_need, tiers_text
from tests._live.summary import item_order

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from tests._live.live_run.secret_scrub import SecretFilter
    from tests._live.live_run.state import Check, RunState

STATUSES: Final = ("passed", "failed", "error", "refused", "skipped", "not run")
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
    "half-day",
    "upgrade-check",
)
"""The groups in the order the report lists them."""

UNLISTED_ITEMS: Final = frozenset({"13", "17", "18"})
"""Open verification items with no live test: checked by CI, retired, and settled without a
key, so the report leaves them out."""

_PRICE: Final = re.compile(r" \((?:\$[^)]*|free|price on request)\)")
"""A plan's price in a skip reason, such as ` ($29/month)`."""


def shown_reason(reason: str) -> str:
    """Return a reason without the plans' prices.

    Args:
        reason: The reason.

    Returns:
        Such as `"needs Stocks Starter"` for `"needs Stocks Starter ($29/month)"`.
    """
    return _PRICE.sub("", reason)


def status_text(check: Check) -> str:
    """Describe a check's status in one phrase.

    Args:
        check: The check.

    Returns:
        Such as `"passed"`, `"skipped: needs Stocks Starter"`, or, for the upgrade check that
        never ran, `"not started (optional)"`.
    """
    if check.group == "upgrade-check" and check.status == "not run":
        return "not started (optional)"
    reason = shown_reason(check.reason)
    return f"{check.status}: {reason}" if reason else check.status


def _ordered(checks: Iterable[Check]) -> list[Check]:
    """Sort checks by group, in report order, then name.

    Args:
        checks: The checks.

    Returns:
        The sorted checks.
    """
    order = {group: index for index, group in enumerate(GROUP_ORDER)}
    return sorted(checks, key=lambda c: (order.get(c.group, len(order)), c.name))


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
        lines.append(f"  subscriptions: {tiers_text(state, state.current)}")
    lines.append(f"next stream day: {state.stream_day or 'none planned'}")
    group = ""
    for check in _ordered(state.checks.values()):
        if check.group != group:
            group = check.group
            lines.append(f"{group} (next: {next_start(group)})")
        last = f" [{tiers_text(state, check.entitlement)}]" if check.entitlement else ""
        lines.append(f"  {check.name}: {status_text(check)}{last}")
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
        writer.writerow(["group", "name", "status", "reason", "last_run", "subscriptions"])
        for check in _ordered(state.checks.values()):
            writer.writerow(
                [
                    check.group,
                    check.name,
                    check.status,
                    shown_reason(check.reason),
                    check.last_run,
                    tiers_text(state, check.entitlement),
                ]
            )


def item_status(checks: list[Check]) -> str:
    """Return an open verification item's status from its checks' latest outcomes.

    Args:
        checks: The checks that answer the item.

    Returns:
        `evidence recorded` when all passed, `partly run` when some never ran, `needs <...>`
        when the rest were skipped for what the key lacks, else `failed`.
    """
    if all(c.status == "passed" for c in checks):
        return "evidence recorded"
    if any(c.status == "not run" for c in checks):
        return "partly run"
    needs = sorted(
        {shown_reason(c.reason) for c in checks if c.status == "skipped" and is_need(c.reason)}
    )
    if needs and all(c.status in {"passed", "skipped"} for c in checks):
        return "; ".join(needs)
    return "failed"


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
    probes = folder / "probes" / "entitlements.csv"
    if probes.exists():
        with probes.open(encoding="utf-8") as file:
            records = list(csv.reader(file))
        lines.extend(_table(records[0], records[1:]))
    else:
        lines.append("No probe was recorded.")
    checks = _ordered(state.checks.values())
    for group in dict.fromkeys(c.group for c in checks):
        lines.extend(["", f"## {group}", ""])
        rows = [
            [c.name, status_text(c), c.last_run, tiers_text(state, c.entitlement)]
            for c in checks
            if c.group == group
        ]
        lines.extend(_table(["check", "status", "last run", "subscriptions"], rows))
    lines.extend(["", "## Open verification items", ""])
    items: dict[str, list[Check]] = {}
    for check in checks:
        for item in check.items:
            if item not in UNLISTED_ITEMS:
                items.setdefault(item, []).append(check)
    rows = [[item, item_status(items[item])] for item in sorted(items, key=item_order)]
    lines.extend(_table(["item", "status"], rows))
    lines.extend(["", "## Still unverified", ""])
    waiting: dict[str, list[str]] = {}
    for check in checks:
        if check.status != "passed":
            need = shown_reason(check.reason) if is_need(check.reason) else check.status
            waiting.setdefault(need, []).append(f"{check.group}: {check.name}")
    lines.extend(f"- **{need}**: " + ", ".join(waiting[need]) for need in sorted(waiting))
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
