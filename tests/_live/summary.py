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

Each group's line gives its count, and each question follows on a line of its own, its number
beside its one-line title (the first line of the docstring of the first test that answers it),
so a reader need not look the numbers up. A run whose paths may leave tests uncollected reports
its counts "in this selection". Every function is pure; `tests/conftest.py`'s terminal summary
gathers the outcomes and writes the lines.
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


def _titled(items: list[str], titles: Mapping[str, str]) -> list[str]:
    """Return one indented line per question: its item, then its title when it has one.

    Args:
        items: The questions, such as `"70"`, or `"5 (1 of 2 tests ran)"` for a partly run one.
        titles: Each question's one-line title, by item.

    Returns:
        Such as `"    70: Whether Massive serves a real-time key on its delayed feed."`.
    """
    lines = []
    for item in items:
        title = titles.get(item.partition(" ")[0], "")
        lines.append(f"    {item}: {title}" if title else f"    {item}")
    return lines


def question_lines(
    settled: Mapping[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]],
    outcome: Mapping[str, str],
    plans: LivePlans | None,
    *,
    titles: Mapping[str, str],
    narrowed: bool,
) -> list[str]:
    """Sort the questions a run touched and describe them, one heading line per group.

    Args:
        settled: Each test's `settles` items, `verifies` items, and `needs`, by test id.
        outcome: The outcome of each test that ran, by test id: `"passed"`, `"failed"`,
            `"skipped"`, or `"error"`.
        plans: The run's plans, which name the plan a skipped test needs; None when no live
            test resolved them.
        titles: Each question's one-line title, by item; a question without one is listed by
            its item alone.
        narrowed: Whether the run's paths may have left some live tests uncollected; its
            counts are then reported "in this selection".

    Returns:
        The plans line (when there are plans), the verified and ran headings, one heading per
        plan some questions need, and one per kind of question not verified, each heading
        followed by its questions (`_titled`); empty when no test that ran answers a question.
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
    groups = [
        (f"verified{selection} {len(verified)} of {total}", verified),
        (f"ran{selection}, evidence to read, {len(evidence)} of {total}", evidence),
        *((f"{len(items)} need {plan}", items) for plan, items in needing.items()),
        *((f"{len(items)} not verified ({why})", items) for why, items in unverified.items()),
    ]
    lines = [f"plans: {plans_text(plans)}"] if plans is not None else []
    for index, (heading, items) in enumerate(groups):
        if items or index < 2:
            lines.append(f"{heading}:")
            lines.extend(_titled(items, titles))
    return lines
