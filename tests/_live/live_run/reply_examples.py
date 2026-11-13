"""The documentation examples the `responses` group records: what each needs and expects.

Each example is a small piece of user code whose replies from Massive the group records
(`replies`, which holds each example's code in `replies.CALLS`, by name). The catalog lives here,
apart from that code, because the live run's parent schedules the examples by name and starts no
JVM, and the code imports the library, which imports Deephaven. This module imports neither.

An example is skipped, with what it needs, when the key's subscriptions lack it
(`skip_reason`): a tier, or history further back than the tier keeps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final

from tests._live.plans import TIERS, LivePlans, history_start, plan_text, unmet

DATA: Final = "data"
REFUSAL: Final = "refusal"
REPLY: Final = "reply"
"""What an example expects: data, Massive's refusal, or any reply at all."""


@dataclass(frozen=True)
class Example:
    """One documentation example: user code whose replies are recorded.

    Attributes:
        name: The file name of its replies, such as `"nvda-daily-2024-06-adjusted"`, and the key
            of its code in `replies.CALLS`.
        needs: The subscriptions it needs, in the live tests' `needs` grammar.
        history: The class and the earliest day it reads, when it reads a fixed past day.
        expects: `DATA`, `REFUSAL`, or `REPLY`.
        stream: Whether it is a stream, held for `replies.STREAM_SECONDS` (left out by
            `--any-time`).
        only_tier: A class and tier the key must hold exactly: the examples that record a
            refusal of the next tier up.
    """

    name: str
    needs: tuple[str, ...] = ()
    history: tuple[str, date] | None = None
    expects: str = DATA
    stream: bool = False
    only_tier: tuple[str, str] | None = None


EXAMPLES: Final = (
    Example("nvda-daily-2024-06-adjusted", history=("stocks", date(2024, 6, 3))),
    Example("nvda-daily-2024-06-as-traded", history=("stocks", date(2024, 6, 3))),
    Example("nvda-splits"),
    Example("market-status"),
    Example("market-holidays"),
    Example("ticker-details"),
    Example("daily-bars-last-year"),
    Example("dividends-last-year"),
    Example("nvda-minute-bars-last-trading-day"),
    Example("nvda-previous-day-bar"),
    Example("treasury-yields-last-month"),
    Example("unknown-ticker-details", expects=REPLY),
    Example("refusal-stocks-starter", expects=REFUSAL, only_tier=("stocks", "basic")),
    Example("refusal-stocks-developer", expects=REFUSAL, only_tier=("stocks", "starter")),
    Example("refusal-stocks-advanced", expects=REFUSAL, only_tier=("stocks", "developer")),
    Example("refusal-options-starter", expects=REFUSAL, only_tier=("options", "basic")),
    Example("refusal-indices-starter", expects=REFUSAL, only_tier=("indices", "basic")),
    Example("refusal-currencies-starter", expects=REFUSAL, only_tier=("currencies", "basic")),
    Example(
        "refusal-currencies-business",
        expects=REFUSAL,
        stream=True,
        only_tier=("currencies", "starter"),
    ),
    Example("refusal-futures-starter", expects=REFUSAL, only_tier=("futures", "basic")),
    Example("stream-stock-minute-bars", needs=("stocks:starter",), stream=True),
    Example("stream-crypto-minute-bars", needs=("currencies:starter",), stream=True),
    Example("stream-forex-quotes", needs=("currencies:starter",), stream=True),
)
"""Every example, in the order they run."""


def history_need(name: str, day: date, today: date) -> str:
    """Name the cheapest tier of a class whose history reaches a day.

    Args:
        name: The class.
        day: The earliest day an example reads.
        today: The run's New York date.

    Returns:
        Such as `"needs Stocks Starter ($29/month) (history back to 2024-06-03)"`; the last
        tier of every class keeps all history, so one always reaches it.
    """
    tier = next(t for t in TIERS[name][1:] if _keeps(name, t, day=day, today=today))
    return f"needs {plan_text(f'{name}:{tier}')} (history back to {day.isoformat()})"


def _keeps(name: str, tier: str, *, day: date, today: date) -> bool:
    """Tell whether a tier of a class keeps a day, by the documented history windows.

    Args:
        name: The class.
        tier: The tier.
        day: The day.
        today: The run's New York date.

    Returns:
        True when the day is inside the tier's window.
    """
    first = history_start(LivePlans({name: tier}, s3=False, flat_files=(), source=""), name, today)
    return first is None or day >= first


def skip_reason(example: Example, plans: LivePlans, today: date) -> str | None:
    """Decide whether the key can run an example.

    Args:
        example: The example.
        plans: The key's tiers.
        today: The run's New York date.

    Returns:
        Why it is skipped (starting `needs`, or `not this tier` for a refusal example of
        another tier), or None when it runs.
    """
    if example.only_tier is not None:
        name, tier = example.only_tier
        if plans.tiers[name] != tier:
            return f"not this tier: records the refusal on {name} {tier}"
    missing = unmet(example.needs, plans)
    if missing:
        return "needs " + "; ".join(plan_text(r) for r in missing)
    if example.history is not None:
        name, day = example.history
        if not _keeps(name, plans.tiers[name], day=day, today=today):
            return history_need(name, day, today)
    return None
