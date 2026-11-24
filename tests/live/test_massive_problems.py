"""Tests of the checks of open Massive problems (`tests/_live/massive_problems.py`).

How pytest reports such a check, end to end, is tested in a real pytest child by
`tests/live/live_run/test_outcomes.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import pytest

from tests._live import massive_problems
from tests._live.massive_problems import FIXED, PRESENT

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class _Item:
    nodeid: str
    markers: list[pytest.Mark] = field(default_factory=list)

    def iter_markers(self, name: str) -> Iterator[pytest.Mark]:
        return (m for m in self.markers if m.name == name)

    def add_marker(self, marker: pytest.MarkDecorator) -> None:
        self.markers.append(marker.mark)


def _issue_of(*markers: pytest.Mark) -> int | None:
    item: Any = _Item("t.py::t", list(markers))
    return massive_problems.issue_of(item)


def test_a_fix_raises_nothing() -> None:
    massive_problems.expect_fixed(fixed=True, shown="volume 12 against 12")


def test_a_problem_still_shown_raises_with_massive_s_values() -> None:
    with pytest.raises(massive_problems.ProblemShownError, match=r"^volume 12 against 13$"):
        massive_problems.expect_fixed(fixed=False, shown="volume 12 against 13")


def test_a_marker_names_its_issue_and_an_unmarked_test_none() -> None:
    assert _issue_of(pytest.mark.massive_issue(797).mark) == 797
    assert _issue_of(pytest.mark.verifies("21").mark) is None


def _marker(args: tuple[object, ...]) -> pytest.Mark:
    return pytest.mark.massive_issue(*args).mark


@pytest.mark.parametrize(
    "markers",
    [
        [_marker(("797",))],
        [_marker((True,))],
        [_marker((0,))],
        [_marker(())],
        [_marker((797, 803))],
        [_marker((797,)), _marker((803,))],
    ],
)
def test_a_marker_without_one_issue_number_is_refused(
    markers: list[pytest.Mark],
) -> None:
    with pytest.raises(pytest.UsageError) as caught:
        _issue_of(*markers)
    assert str(caught.value).endswith(
        "Give a check one marker, @pytest.mark.massive_issue(N), with N the number of the "
        "GitHub issue it tracks."
    )
    assert str(caught.value).startswith("t.py::t has massive_issue markers with [")


def test_a_tracked_check_is_a_strict_expected_failure_naming_its_issue() -> None:
    item: Any = _Item("t.py::t", [pytest.mark.massive_issue(803).mark])
    massive_problems.track(item)
    [xfail] = item.iter_markers("xfail")
    assert xfail.kwargs == {
        "strict": True,
        "raises": massive_problems.ProblemShownError,
        "reason": (
            "a check of the open Massive problem of issue #803: it fails while the problem "
            "lasts, and a pass means Massive fixed it, so close issue #803"
        ),
    }


def test_an_untracked_test_is_left_as_it_is() -> None:
    item: Any = _Item("t.py::t")
    massive_problems.track(item)
    assert item.markers == []


@dataclass
class _ExcInfo:
    typename: str


@dataclass
class _Call:
    excinfo: _ExcInfo | None


def _report(
    outcome: Literal["passed", "failed", "skipped"],
    longrepr: tuple[str, int, str] | str | None = None,
    *,
    when: Literal["setup", "call", "teardown"] = "call",
    properties: list[tuple[str, object]] | None = None,
    **extra: object,
) -> pytest.TestReport:
    report = pytest.TestReport(
        "t.py::test_x", ("t.py", 1, "t"), {}, outcome, longrepr, when, user_properties=properties
    )
    for name, value in extra.items():
        setattr(report, name, value)
    return report


@pytest.mark.parametrize(
    ("report", "call", "expected"),
    [
        (_report("skipped", "x", wasxfail="r"), _Call(_ExcInfo("ProblemShownError")), PRESENT),
        (_report("failed", "[XPASS(strict)] r"), _Call(None), FIXED),
        (_report("failed", "AssertionError"), _Call(_ExcInfo("AssertionError")), ""),
        (_report("skipped", ("t.py", 1, "Skipped: refused: HTTP 403")), _Call(None), ""),
        (_report("failed", "boom", when="setup"), _Call(None), ""),
        (_report("passed", when="teardown"), _Call(None), ""),
    ],
)
def test_a_call_shows_the_problem_its_fix_or_neither(
    report: pytest.TestReport, call: _Call, expected: str
) -> None:
    calls: Any = call
    assert massive_problems.judge(report, calls) == expected


def test_a_run_s_reports_give_each_check_s_issue_and_outcome() -> None:
    def tagged(issue: int, outcome: str) -> list[tuple[str, object]]:
        return [massive_problems.report_property(issue, outcome)]

    reports = [
        _report("skipped", "x", properties=tagged(797, PRESENT)),
        _report("failed", "x", properties=tagged(803, FIXED)),
        _report(
            "skipped",
            ("t.py", 1, "Skipped: refused: 403"),
            when="setup",
            properties=tagged(778, ""),
        ),
        _report("failed", "x", properties=tagged(800, "")),
        _report("failed", "x", when="teardown", properties=tagged(800, "")),
        _report("failed", "x", when="setup", properties=tagged(799, "")),
        _report("skipped", "Skipped: no spaced combo", properties=tagged(777, "")),
        _report("passed", when="setup", properties=tagged(801, "")),
        _report("failed", "x"),
    ]
    for report, name in zip(
        reports,
        ["shows", "gone", "refused", "breaks", "breaks", "errs", "nothing", "setup", "plain"],
        strict=True,
    ):
        report.nodeid = f"t.py::{name}"
    assert massive_problems.checks_run(reports) == {
        "shows": (797, PRESENT),
        "gone": (803, FIXED),
        "refused": (778, "skipped: refused: 403"),
        "breaks": (800, "failed"),
        "errs": (799, "error"),
        "nothing": (777, "skipped: no spaced combo"),
    }


def test_issues_sort_by_what_their_checks_showed() -> None:
    lines = massive_problems.problem_lines(
        {
            "b": (803, FIXED),
            "a": (803, FIXED),
            "docs": (779, FIXED),
            "data": (779, PRESENT),
            "x": (797, PRESENT),
            "y": (798, PRESENT),
            "z": (798, "skipped: refused: 403"),
        }
    )
    assert lines == [
        "Massive problems FIXED, close the issue, 1 of 4:",
        "    #803: a, b",
        "Massive problems partly fixed, read each check, 1 of 4:",
        "    #779: data, docs",
        "Massive problems still present, 1 of 4:",
        "    #797: x",
        "Massive problems not judged (a check failed, was skipped, or was refused), 1 of 4:",
        "    #798: y, z [skipped: refused: 403]",
    ]


def test_a_run_with_no_check_has_no_problem_lines() -> None:
    assert massive_problems.problem_lines({}) == []
