"""The open-questions summary of a live run: which questions it verified, ran, or left open.

Each live test names the open questions it answers: `verifies` for those whose answer it
asserts (it fails the day Massive stops giving the answer the library assumes) and `settles`
for those it only records evidence for, which a person reads. At the end of a run the summary
sorts every question the run touched:

- verified: every test of it ran and passed, and at least one asserts it;
- ran, evidence to read: every test of it passed, but none asserts it;
- refused by Massive: a test of it skipped because Massive refused a request (its skip reason
  starts with `REFUSED`), and every other test of it passed or skipped so too;
- not listed as delayed: a test of it skipped because Massive labels the data delayed and the
  owner's settings file does not list its subscription in `delayed_subscriptions` (its skip
  reason starts with `NOT_LISTED`), and every other test of it passed or skipped as one of
  these two;
- not verified: failed or skipped otherwise, or partly run (a `-k` or path selection left out
  some of its tests).

Each group's line gives its count, and each question follows on a line of its own, its number
beside its one-line title (`question_titles`: the first line of a test's docstring that asks
it alone, or first), so a reader need not look the numbers up. Under a refused or unlisted
question, each distinct skip reason of its tests follows, Massive's refusal or the library's
message quoted. A run whose paths may leave tests uncollected reports its counts "in this
selection". Every function is pure; `tests/conftest.py`'s terminal summary gathers the
outcomes and writes the lines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

REFUSED: Final = "refused: "
"""The start of the skip reason of a live test whose request Massive refused, followed by the
refusal on one line (`refusals.refusal_line`)."""

NOT_LISTED: Final = "not listed as delayed: "
"""The start of the skip reason of a live test whose data Massive labels delayed while the
owner's settings do not list its subscription in `delayed_subscriptions`, followed by the
library's message, which names the setting to add."""


def item_order(item: str) -> tuple[int, str]:
    """Sort numbered items first, by number, then named ones.

    Args:
        item: An item, such as `"21"` or `"429 text"`.

    Returns:
        The sort key.
    """
    return (int(item), "") if item.isdigit() else (10_000, item)


def question_titles(tests: Iterable[tuple[tuple[str, ...], str]]) -> dict[str, str]:
    """Title each question by the docstring of a test that is about it.

    A test that answers several questions has one docstring, whose first line names the first
    of them, so it titles only that one. A question takes the title of the first test that
    answers it alone; failing that, of the first test that names it first; failing that, none,
    and the summary lists it by its item alone rather than under another question's title.

    Args:
        tests: Each test's questions (its `settles` items, then its `verifies` items) and its
            docstring, in collection order.

    Returns:
        Each titled question's one-line title, by item.
    """
    alone: dict[str, str] = {}
    leading: dict[str, str] = {}
    for answered, doc in tests:
        if not answered:
            continue
        title = doc.partition("\n")[0]
        leading.setdefault(answered[0], title)
        if len(answered) == 1:
            alone.setdefault(answered[0], title)
    return leading | alone


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
    settled: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]],
    outcome: Mapping[str, str],
    reasons: Mapping[str, str],
    *,
    titles: Mapping[str, str],
    narrowed: bool,
) -> list[str]:
    """Sort the questions a run touched and describe them, one heading line per group.

    Args:
        settled: Each test's `settles` items and `verifies` items, by test id.
        outcome: The outcome of each test that ran, by test id: `"passed"`, `"failed"`,
            `"skipped"`, or `"error"`.
        reasons: The skip reason of each skipped test, by test id.
        titles: Each question's one-line title, by item; a question without one is listed by
            its item alone.
        narrowed: Whether the run's paths may have left some live tests uncollected; its
            counts are then reported "in this selection".

    Returns:
        The verified and ran headings, then the refused, unlisted, and not verified ones that
        have questions, each heading followed by its questions (`_titled`), and a refused or
        unlisted question by its tests' skip reasons; empty when no test that ran answers a
        question.
    """
    ran = {i for t, (s, v) in settled.items() if t in outcome for i in (*s, *v)}
    if not ran:
        return []
    verified: list[str] = []
    evidence: list[str] = []
    refused: dict[str, list[str]] = {}
    unlisted: dict[str, list[str]] = {}
    unverified: dict[str, list[str]] = {"failed or skipped otherwise": [], "partly run": []}
    for item in sorted(ran, key=item_order):
        every = [(t, v) for t, (s, v) in settled.items() if item in s or item in v]
        tests = [(t, v) for t, v in every if t in outcome]
        others = [reasons.get(t, "") for t, _ in tests if outcome[t] != "passed"]
        if len(tests) < len(every):
            unverified["partly run"].append(f"{item} ({len(tests)} of {len(every)} tests ran)")
        elif not others:
            (verified if any(item in v for _, v in tests) else evidence).append(item)
        elif not all(r.startswith((REFUSED, NOT_LISTED)) for r in others):
            unverified["failed or skipped otherwise"].append(item)
        elif any(r.startswith(REFUSED) for r in others):
            refused[item] = list(dict.fromkeys(others))
        else:
            unlisted[item] = list(dict.fromkeys(others))
    total = len(ran)
    selection = " in this selection (run the full live suite to verify)" if narrowed else ""
    lines = [f"verified{selection} {len(verified)} of {total}:", *_titled(verified, titles)]
    lines += [f"ran{selection}, evidence to read, {len(evidence)} of {total}:"]
    lines += _titled(evidence, titles)
    hint = "add to delayed_subscriptions in your settings file"
    skipped = (
        (f"refused by Massive, {len(refused)} of {total}:", refused),
        (f"not listed as delayed, {len(unlisted)} of {total} ({hint}):", unlisted),
    )
    for heading, quoted in skipped:
        if quoted:
            lines.append(heading)
            for item, quotes in quoted.items():
                lines.extend(_titled([item], titles))
                lines.extend(f"        {quote}" for quote in quotes)
    for why, items in unverified.items():
        if items:
            lines.append(f"not verified ({why}), {len(items)} of {total}:")
            lines.extend(_titled(items, titles))
    return lines
