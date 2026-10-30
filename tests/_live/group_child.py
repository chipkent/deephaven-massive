"""The child side of the live run's built-in groups: run one, and report each check.

`scripts/live_run.py _group --out DIR --attempt FOLDER --token T` starts an embedded Deephaven
server, then calls `run_group` here. It reads the attempt's `request.json`, which the program
wrote (`attempts.run_built_in_group`): the group, its due checks, the key's tiers, the
entitlement id, the group's end, and for a reconciliation the stream day and its folder. It
runs the group, and appends one line per check to `results.jsonl` as each finishes, so a child
that dies keeps what it finished. The upgrade check also writes its verdict there.

Runs on the child's main thread, with the session the script's `open_session` opens.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Final

from tests._live import reconcile, replies, stream_capture, upgrade_check
from tests._live.calendar import NEW_YORK
from tests._live.outcomes import append_line
from tests._live.plans import LivePlans

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive import MassiveSession

SELF_STOP: Final = timedelta(minutes=2)
"""How long before the group's end a streaming child stops itself."""


def run_group(
    out: Path,
    attempt_folder: str,
    *,
    open_session: Callable[[], MassiveSession],
    tier_of: Callable[[str], str | None],
) -> None:
    """Run the built-in group an attempt's `request.json` names.

    Args:
        out: The results folder.
        attempt_folder: The attempt's folder, relative to `out`.
        open_session: Opens a session on the key, its Basic subscriptions declared (or, for
            the upgrade check, none declared).
        tier_of: Probes one subscription's tier now (the upgrade check); None when unknown.
    """
    folder = out / attempt_folder
    request = json.loads((folder / "request.json").read_text(encoding="utf-8"))
    group: str = request["group"]
    tiers: dict[str, str] = request["tiers"]
    due: list[str] = request["due"]
    until = datetime.fromisoformat(request["until"]) - SELF_STOP

    def report(name: str, status: str, reason: str) -> None:
        append_line(folder / "results.jsonl", {"name": name, "status": status, "reason": reason})

    today = datetime.now(UTC).astimezone(NEW_YORK).date()
    if group == "responses":
        plans = LivePlans(tiers=tiers, s3=False, flat_files=(), source="probed")
        examples = [e for e in replies.EXAMPLES if e.name in due]
        replies.run_responses(
            examples,
            plans=plans,
            open_session=open_session,
            folder=folder,
            today=today,
            entitlement=request["entitlement"],
            report=report,
        )
    elif group == "upgrade-check":
        [subscription] = due
        verdict = upgrade_check.run_upgrade_check(
            subscription,
            open_session=open_session,
            tier_of=lambda: tier_of(subscription),
            folder=folder,
        ).verdict
        status, _, reason = verdict.partition(": ")
        report(subscription, status, reason)
    else:
        with open_session() as m:
            if group == "stream-day":
                stream_capture.run_stream_day(
                    m, units=due, tiers=tiers, folder=folder, until=until, report=report
                )
            elif group == "throughput":
                stream_capture.run_throughput(
                    m, units=due, folder=folder, until=until, report=report
                )
            else:
                reconcile.run_reconcile(
                    m,
                    units=due,
                    stream_folder=Path(request["stream_folder"]),
                    day=date.fromisoformat(request["stream_day"]),
                    folder=folder,
                    report=report,
                )
