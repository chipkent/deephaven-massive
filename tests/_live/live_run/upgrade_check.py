"""The live run's attended upgrade check: does pacing end once a subscription is upgraded?

When a free subscription's 5 requests a minute run out, the library learns that the
subscription is on Basic and paces it. This check, run by hand at the moment of an upgrade
(`live_run.py upgrade-check SUBSCRIPTION`), tests that a new session after the upgrade is no
longer limited:

1. Session A, which declares nothing, makes one-request calls of the subscription until
   `m.rate_limits()` shows its limit `LEARNED`.
2. The child prints `Now upgrade <subscription> on the dashboard, then press Enter` and reads a
   line from its standard input, which the parent writes when the person presses Enter.
3. A's row is recorded and A is closed. The subscription is probed once a minute until its tier
   is above Basic; then, 65 seconds later, session B, which declares nothing, sends ten
   one-request calls, recording each one's send and answer times and B's rows.
4. One round whose ten calls are all answered within 30 seconds with no refusal passes. A
   refusal (Massive still limits) closes B and repeats step 3 a minute later; the whole wait
   after Enter is limited to 15 minutes.

The verdicts: `passed`; `failed: library paced` (no refusal, but a call waited more than 30 s);
`failed: Massive kept the free limit` (the 15 minutes ended on refusals); and `failed: tier
stayed Basic` (no probe saw the upgrade), which records a rehearsal, not the check's result.
Everything goes to `upgrade-check.json` in the attempt's folder. It judges by timings, never by
states seen in a refreshing table.
"""

from __future__ import annotations

import dataclasses
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

import deephaven_massive as dm
from deephaven_massive._massive import set_reply_capture
from tests._live.live_run.stream_capture import diagnostics_rows, wait_for_diagnostics

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven_massive import MassiveSession

SUBSCRIPTIONS: Final = ("stocks", "options", "indices", "currencies", "futures")

CALLS: Final[dict[str, Callable[[MassiveSession], object]]] = {
    "stocks": lambda m: m.stocks.previous_day_bar("AAPL").fetch(),
    # Options Basic includes a contract's bars, not the chain; an expired contract answers 404,
    # which counts against the minute the same.
    "options": lambda m: m.options.previous_day_bar("O:SPY261218C00600000").fetch(),
    "indices": lambda m: m.indices.previous_day_bar("I:NDX").fetch(),
    "currencies": lambda m: m.forex.previous_day_bar("C:EURUSD").fetch(),
    "futures": lambda m: m.futures.market_status(product_code="ES", limit=1).fetch(),
}
"""One call of each subscription that sends exactly one request."""

ROUND_CALLS: Final = 10
ANSWER_LIMIT: Final = 30.0
"""A round's calls, and the seconds each may take to count as unpaced."""

WAIT_LIMIT: Final = timedelta(minutes=15)
"""How long after Enter the check waits for the upgrade to take effect."""

FRESH_MINUTE: Final = 65.0
"""Seconds between seeing the upgrade and session B's first call, so no old request counts."""

TOO_MANY_REQUESTS: Final = 429
"""The HTTP status of Massive's refusal for the per-minute limit."""

PROMPT: Final = "Now upgrade {subscription} on the dashboard, then press Enter"


@dataclass
class Waits:
    """The time source and the waits, so tests can run the check without waiting.

    Attributes:
        now: The current time, aware.
        monotonic: Seconds for measuring a call.
        sleep: Waits a number of seconds.
        read_line: Reads the line the parent writes when Enter is pressed.
    """

    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    read_line: Callable[[], str] = input


@dataclass
class Record:
    """What the check saw, written to `upgrade-check.json`.

    Attributes:
        subscription: The subscription upgraded.
        learned_calls: How many calls session A made before its limit read `LEARNED`.
        row_before: A's rate-limit row after Enter.
        rounds: Each round of session B: its calls (send time, seconds, error) and its row.
        verdict: `passed`, or the failure.
    """

    subscription: str
    learned_calls: int = 0
    row_before: dict[str, Any] = field(default_factory=dict)
    rounds: list[dict[str, Any]] = field(default_factory=list)
    verdict: str = ""


def _row(m: MassiveSession, subscription: str) -> dict[str, Any]:
    """Return the session's rate-limit row of a subscription, values as text.

    Args:
        m: The session.
        subscription: The subscription, the row's `Bucket`.

    Returns:
        The row, or an empty one when the bucket has none yet.
    """
    rows = [r for r in diagnostics_rows(m, m.rate_limits()) if r["Bucket"] == subscription]
    return {
        k: v if isinstance(v, int | float | str | None) else str(v)
        for k, v in (rows or [{}])[0].items()
    }


def learn_limit(m: MassiveSession, subscription: str, *, limit: int = 30) -> int:
    """Make one-request calls until the session has learned the subscription is on Basic.

    After each call it waits until the call's effect shows in `m.rate_limits()`, which takes
    an update cycle after the call (`wait_for_diagnostics`), then reads the subscription's
    row once, so it makes no call more than the limit needs.

    Args:
        m: Session A, which declares nothing.
        subscription: The subscription.
        limit: The most calls to make.

    Returns:
        How many calls it took.

    Raises:
        RuntimeError: When the limit was not learned within `limit` calls: the subscription is
            not on Basic.
    """
    for count in range(1, limit + 1):
        CALLS[subscription](m)
        wait_for_diagnostics(m)
        if _row(m, subscription).get("LimitSource") == "LEARNED":
            return count
    msg = f"{subscription}: no limit was learned after {limit} calls; is it on Basic?"
    raise RuntimeError(msg)


def round_of_calls(
    open_session: Callable[[], MassiveSession], subscription: str, waits: Waits
) -> dict[str, Any]:
    """Open session B, send ten one-request calls, and record each call and B's row.

    Massive's refusals are counted from the replies themselves, through the wire package's
    reply-capture hook, so the verdict never depends on when the diagnostics table catches up.

    Args:
        open_session: Opens a session that declares nothing.
        subscription: The subscription.
        waits: The time source.

    Returns:
        The round: its calls (`sent`, `seconds`, `error`), B's row, the HTTP 429 replies
        (`refusals`), whether Massive refused a request (`refused`), and whether every call
        was answered within 30 s (`fast`).
    """
    calls: list[dict[str, Any]] = []
    statuses: list[int | None] = []
    set_reply_capture(lambda reply: statuses.append(reply.status))
    try:
        with open_session() as m:
            for _ in range(ROUND_CALLS):
                sent, started = waits.now().isoformat(), waits.monotonic()
                error = ""
                try:
                    CALLS[subscription](m)
                except dm.MassiveError as raised:
                    error = f"{type(raised).__name__}: {raised}"
                seconds = round(waits.monotonic() - started, 3)
                calls.append({"sent": sent, "seconds": seconds, "error": error})
            row = _row(m, subscription)
    finally:
        set_reply_capture(None)
    refusals = statuses.count(TOO_MANY_REQUESTS)
    fast = all(c["seconds"] <= ANSWER_LIMIT for c in calls)
    return {"calls": calls, "row": row, "refusals": refusals, "refused": refusals > 0, "fast": fast}


def run_upgrade_check(
    subscription: str,
    *,
    open_session: Callable[[], MassiveSession],
    tier_of: Callable[[], str | None],
    folder: Path,
    waits: Waits | None = None,
) -> Record:
    """Run the attended upgrade check, and write `upgrade-check.json`.

    Args:
        subscription: One of `SUBSCRIPTIONS`.
        open_session: Opens a session that declares nothing.
        tier_of: Probes the subscription's tier now; None when the probe cannot tell.
        folder: The attempt's folder.
        waits: The time source and waits; real ones by default.

    Returns:
        What the check saw, its verdict included.
    """
    waits = waits or Waits()
    record = Record(subscription)
    with open_session() as m:
        record.learned_calls = learn_limit(m, subscription)
        sys.stdout.write(PROMPT.format(subscription=subscription) + "\n")
        sys.stdout.flush()
        waits.read_line()
        record.row_before = _row(m, subscription)
    deadline = waits.now() + WAIT_LIMIT
    upgraded = False
    while waits.now() < deadline:
        tier = tier_of()
        if tier is None or tier == "basic":
            waits.sleep(60.0)
            continue
        upgraded = True
        waits.sleep(FRESH_MINUTE)
        result = round_of_calls(open_session, subscription, waits)
        record.rounds.append(result)
        if not result["refused"]:
            record.verdict = "passed" if result["fast"] else "failed: library paced"
            break
        waits.sleep(60.0)
    if not record.verdict:
        record.verdict = (
            "failed: Massive kept the free limit" if upgraded else "failed: tier stayed Basic"
        )
    text = json.dumps(dataclasses.asdict(record), indent=1, default=str)
    (folder / "upgrade-check.json").write_text(text + "\n", encoding="utf-8")
    return record
