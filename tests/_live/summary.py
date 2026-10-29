"""The open-questions summary of a live run: which questions it verified, ran, or left open.

Each live test names the open questions it answers: `verifies` for those whose answer it
asserts (it fails the day Massive stops giving the answer the library assumes) and `settles`
for those it only records evidence for, which a person reads. At the end of a run the summary
sorts every question the run touched:

- verified: every test of it ran and passed, and at least one asserts it;
- ran, evidence to read: every test of it passed, but none asserts it;
- needs a plan: a test of it was skipped for a plan the key lacks, named with its price;
- not verified: failed or skipped otherwise, or partly run (a `-k` or path selection left out
  some of its tests).

A run whose paths may leave tests uncollected reports its counts "in this selection". Every
function is pure; `tests/conftest.py`'s terminal summary gathers the outcomes and writes the
lines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests._live.plans import LivePlans, plan_text, plans_text, unmet

if TYPE_CHECKING:
    from collections.abc import Mapping


def item_order(item: str) -> tuple[int, str]:
    """Sort numbered items first, by number, then named ones.

    Args:
        item: An item, such as `"21"` or `"429 text"`.

    Returns:
        The sort key.
    """
    return (int(item), "") if item.isdigit() else (10_000, item)


def question_lines(
    settled: Mapping[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]],
    outcome: Mapping[str, str],
    plans: LivePlans | None,
    *,
    narrowed: bool,
) -> list[str]:
    """Sort the questions a run touched and describe them, one line per group.

    Args:
        settled: Each test's `settles` items, `verifies` items, and `needs`, by test id.
        outcome: The outcome of each test that ran, by test id: `"passed"`, `"failed"`,
            `"skipped"`, or `"error"`.
        plans: The run's plans, which name the plan a skipped test needs; None when no live
            test resolved them.
        narrowed: Whether the run's paths may have left some live tests uncollected; its
            counts are then reported "in this selection".

    Returns:
        The plans line (when there are plans), the verified and ran lines, one line per plan
        some questions need, and one per kind of question not verified; empty when no test
        that ran answers a question.
    """
    verified: list[str] = []
    evidence: list[str] = []
    needing: dict[str, list[str]] = {}
    unverified: dict[str, list[str]] = {"failed or skipped": [], "partly run": []}
    ran = {i for t, (s, v, _) in settled.items() if t in outcome for i in (*s, *v)}
    if not ran:
        return []
    for item in sorted(ran, key=item_order):
        every = [(t, v, n) for t, (s, v, n) in settled.items() if item in s or item in v]
        tests = [(t, v, n) for t, v, n in every if t in outcome]
        if len(tests) < len(every):
            unverified["partly run"].append(f"{item} ({len(tests)} of {len(every)} tests ran)")
        elif all(outcome[t] == "passed" for t, _, _ in tests):
            (verified if any(item in v for _, v, _ in tests) else evidence).append(item)
        else:
            skipped = [n for t, _, n in tests if outcome[t] == "skipped"]
            missing = [r for needs in skipped for r in (unmet(needs, plans) if plans else [])]
            if missing:
                plan = ", ".join(plan_text(r) for r in dict.fromkeys(missing))
                needing.setdefault(plan, []).append(item)
            else:
                unverified["failed or skipped"].append(item)
    total = len(ran)
    selection = " in this selection (run the full live suite to verify)" if narrowed else ""
    lines = [f"plans: {plans_text(plans)}"] if plans is not None else []
    lines.append(f"verified{selection} {len(verified)} of {total}: {', '.join(verified)}")
    lines.append(
        f"ran{selection}, evidence to read, {len(evidence)} of {total}: {', '.join(evidence)}"
    )
    lines.extend(f"{len(items)} need {plan}: {', '.join(items)}" for plan, items in needing.items())
    lines.extend(
        f"{len(items)} not verified ({why}): {', '.join(items)}"
        for why, items in unverified.items()
        if items
    )
    return lines
