"""The checks of open Massive problems: each tracks a GitHub issue and says when Massive fixed it.

Massive has confirmed problems in its data and documentation and not yet fixed them. Each has a
GitHub issue in this repository, and one or more checks marked `massive_issue(N)` with its
number. A check asserts what Massive's fix brings: it first asserts its evidence with plain
`assert`s (the rows came back, a control read worked), then calls `expect_fixed` once per part
of the problem. At collection `track` makes every marked check a strict expected failure of
`ProblemShownError`, so:

- the problem still shows: `expect_fixed` raises `ProblemShownError`, and pytest reports an
  expected failure (XFAIL), outcome `PRESENT`;
- Massive fixed it: the check passes, and pytest reports the strict unexpected pass (XPASS) as a
  failure whose reason names the issue to close, outcome `FIXED`;
- Massive refused a request: the live tests' refusal hook skips the check, quoting Massive;
- anything else fails (a network error, an empty answer, a failed precondition): an exception
  other than `ProblemShownError` is a plain failure, never taken for the problem or its fix.

`problem_outcome` tells the two outcomes from a phase's report, and `problem_lines` sorts the
issues a run checked into fixed, partly fixed, still present, and not judged, for the terminal
summary and the live-run program's report.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

MARKER: Final = "massive_issue"
"""The marker that names the GitHub issue of the Massive problem a check tracks."""

PRESENT: Final = "present"
"""The outcome of a check whose problem still shows."""

FIXED: Final = "fixed"
"""The outcome of a check whose problem no longer shows."""

PROPERTY: Final = "massive_problem"
"""The name of the `user_properties` entry of a check's report (`report_property`), which
reaches the terminal summary from a worker process too."""


class ProblemShownError(Exception):
    """Massive's data or documentation still shows the problem a check looks for."""


def expect_fixed(*, fixed: bool, shown: str) -> None:
    """Raise `ProblemShownError` unless Massive's fix shows.

    Args:
        fixed: Whether what Massive sent is what its fix brings.
        shown: What Massive sent, with its values, for the expected failure's message.

    Raises:
        ProblemShownError: When `fixed` is false, with `shown` as its message.
    """
    if not fixed:
        raise ProblemShownError(shown)


def issue_of(item: pytest.Item) -> int | None:
    """Return the issue a test's `massive_issue` marker names.

    Args:
        item: The test.

    Returns:
        The issue's number, or None for a test without the marker.

    Raises:
        pytest.UsageError: When the marker names other than one positive issue number, or the
            test carries it more than once.
    """
    markers = list(item.iter_markers(MARKER))
    if not markers:
        return None
    args = [marker.args for marker in markers]
    issue = args[0][0] if len(markers) == 1 and len(args[0]) == 1 else None
    # A `bool` is an `int` to Python, but never an issue number.
    if not isinstance(issue, int) or isinstance(issue, bool) or issue <= 0:
        msg = (
            f"{item.nodeid} has {MARKER} markers with {args}. Give a check one marker, "
            f"@pytest.mark.{MARKER}(N), with N the number of the GitHub issue it tracks."
        )
        raise pytest.UsageError(msg)
    return issue


def reason(issue: int) -> str:
    """Return the reason pytest shows beside a check of an issue, expected failure or not.

    Args:
        issue: The issue's number.

    Returns:
        The reason, naming the issue to close when the check passes.
    """
    return (
        f"a check of the open Massive problem of issue #{issue}: it fails while the problem "
        f"lasts, and a pass means Massive fixed it, so close issue #{issue}"
    )


def track(item: pytest.Item) -> None:
    """Make a check of an open Massive problem a strict expected failure of `ProblemShownError`.

    Args:
        item: A collected test; one without the `massive_issue` marker is left as it is.
    """
    issue = issue_of(item)
    if issue is not None:
        item.add_marker(
            pytest.mark.xfail(strict=True, raises=ProblemShownError, reason=reason(issue))
        )


def judge(report: pytest.TestReport, call: pytest.CallInfo[None]) -> str:
    """Tell whether a check's call showed its problem, its fix, or neither.

    Args:
        report: The phase's report, made under the check's expected failure.
        call: The phase's result: no exception when the check's own code passed.

    Returns:
        `PRESENT` for an expected failure, `FIXED` for a call that passed against its
        expectation, else `""`: another phase, a skip, or another failure.
    """
    if report.when != "call":
        return ""
    if report.skipped and hasattr(report, "wasxfail"):
        return PRESENT
    if report.failed and call.excinfo is None:
        return FIXED
    return ""


def report_property(issue: int, outcome: str) -> tuple[str, str]:
    """Return the `user_properties` entry of a check's phase report.

    Args:
        issue: The check's issue.
        outcome: The phase's outcome (`judge`).

    Returns:
        `(PROPERTY, "<issue> <outcome>")`, which `checks_run` reads back.
    """
    return PROPERTY, f"{issue} {outcome}"


def checks_run(reports: Iterable[pytest.TestReport]) -> dict[str, tuple[int, str]]:
    """Gather each check's issue and outcome from a run's reports.

    Args:
        reports: Every phase's report of the run; those without a `PROPERTY` entry (tests that
            are not checks of a Massive problem) are passed over.

    Returns:
        Each check that ran, by its test's name: its issue, and `PRESENT` or `FIXED` when its
        call showed either, else the first phase that failed or skipped (`"failed"`,
        `"error"`, or `"skipped: <reason>"`); a check whose phases all passed (which a strict
        expected failure never allows) is left out.
    """
    found: dict[str, tuple[int, str]] = {}
    for report in reports:
        marked = [str(value) for name, value in report.user_properties if name == PROPERTY]
        if not marked or report.passed:
            continue
        number, _, outcome = marked[0].partition(" ")
        issue = int(number)
        name = report.nodeid.rpartition("::")[2]
        if outcome:
            found[name] = (issue, outcome)
            continue
        if report.skipped:
            longrepr = report.longrepr
            text = str(longrepr[2]) if isinstance(longrepr, tuple) else str(longrepr)
            happened = f"skipped: {text.removeprefix('Skipped: ')}"
        else:
            happened = "failed" if report.when == "call" else "error"
        found.setdefault(name, (issue, happened))
    return found


def problem_lines(checks: Mapping[str, tuple[int, str]]) -> list[str]:
    """Sort the issues a run checked by what their checks showed.

    An issue is FIXED when every check of it that ran showed the fix, partly fixed when some
    showed the fix and the rest the problem, still present when every one showed the problem,
    and not judged when one failed otherwise, was skipped, or was refused: then neither its
    problem nor its fix is shown.

    Args:
        checks: Each check that ran, by name: its issue and its outcome, `PRESENT`, `FIXED`,
            or what else happened (such as `"failed"` or `"skipped: refused: ..."`).

    Returns:
        A heading per group that has issues, with its count, each issue on a line of its own
        under it, by number, with its checks: `#803: test_a, test_b`, and for a check not
        judged its outcome in brackets. Empty when no check ran.
    """
    by_issue: dict[int, dict[str, str]] = {}
    for name, (issue, outcome) in checks.items():
        by_issue.setdefault(issue, {})[name] = outcome
    groups: dict[str, list[int]] = {
        "Massive problems FIXED, close the issue": [],
        "Massive problems partly fixed, read each check": [],
        "Massive problems still present": [],
        "Massive problems not judged (a check failed, was skipped, or was refused)": [],
    }
    headings = list(groups)
    for issue in sorted(by_issue):
        outcomes = set(by_issue[issue].values())
        if outcomes == {FIXED}:
            groups[headings[0]].append(issue)
        elif outcomes == {FIXED, PRESENT}:
            groups[headings[1]].append(issue)
        elif outcomes == {PRESENT}:
            groups[headings[2]].append(issue)
        else:
            # Any other outcome (a failure, an error, a skip, a refusal) judges nothing.
            groups[headings[3]].append(issue)
    lines: list[str] = []
    total = len(by_issue)
    for heading, issues in groups.items():
        if not issues:
            continue
        lines.append(f"{heading}, {len(issues)} of {total}:")
        for issue in issues:
            named = [
                name if outcome in {FIXED, PRESENT} else f"{name} [{outcome}]"
                for name, outcome in sorted(by_issue[issue].items())
            ]
            lines.append(f"    #{issue}: {', '.join(named)}")
    return lines
