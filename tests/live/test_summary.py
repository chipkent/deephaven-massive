"""Tests of the open-questions summary (`tests/_live/summary.py`)."""

from __future__ import annotations

from tests._live import plans, summary

_SETTLED = {
    "asserts": ((), ("2",), ()),
    "records": (("1",), (), ()),
    "also 2": (("2",), (), ()),
    "needs": ((), ("3",), ("stocks:starter",)),
    "fails": (("4",), (), ()),
    "half": (("5",), (), ()),
    "other half": (("5",), (), ()),
    "skipped": (("6",), (), ("stocks:basic",)),
    "silent": ((), (), ()),
}

_TITLES = {"1": "Whether one thing holds.", "5": "Whether another holds."}


def test_a_question_takes_the_title_of_a_test_about_it() -> None:
    tests = [
        (("52", "60"), "How index bars are stamped.\n\nMore."),
        (("7",), "Which labels the snapshot carries."),
        ((), "Answers nothing."),
        (("60",), "The index day zone."),
        (("7", "69"), "Something else."),
        (("70", "71"), "Whether a key is served."),
    ]
    assert summary.question_titles(tests) == {
        "52": "How index bars are stamped.",
        "60": "The index day zone.",
        "7": "Which labels the snapshot carries.",
        "70": "Whether a key is served.",
    }


def _lines(*, narrowed: bool = False, held: plans.LivePlans | None = None) -> list[str]:
    outcome = {
        "asserts": "passed",
        "records": "passed",
        "also 2": "passed",
        "needs": "skipped",
        "fails": "failed",
        "half": "passed",
        "skipped": "skipped",
        "silent": "passed",
    }
    return summary.question_lines(
        _SETTLED,
        outcome,
        held or plans.declared_plans(""),
        titles=_TITLES,
        narrowed=narrowed,
    )


def test_each_question_is_sorted_by_its_tests_and_shown_with_its_title() -> None:
    assert _lines()[1:] == [
        "verified 1 of 6:",
        "    2",
        "ran, evidence to read, 1 of 6:",
        "    1: Whether one thing holds.",
        "1 need Stocks Starter ($29/month):",
        "    3",
        "2 not verified (failed or skipped):",
        "    4",
        "    6",
        "1 not verified (partly run):",
        "    5 (1 of 2 tests ran): Whether another holds.",
    ]


def test_the_plans_head_the_lines() -> None:
    assert _lines()[0].startswith("plans: stocks=basic")


def test_a_narrowed_run_counts_in_its_selection() -> None:
    assert _lines(narrowed=True)[1].startswith("verified in this selection (run the full")


def test_without_plans_a_skip_is_not_verified() -> None:
    lines = summary.question_lines(_SETTLED, {"needs": "skipped"}, None, titles={}, narrowed=False)
    assert lines == [
        "verified 0 of 1:",
        "ran, evidence to read, 0 of 1:",
        "1 not verified (failed or skipped):",
        "    3",
    ]


def test_no_lines_without_a_question() -> None:
    lines = summary.question_lines(_SETTLED, {"silent": "passed"}, None, titles={}, narrowed=False)
    assert lines == []


def test_numbered_items_sort_first() -> None:
    assert sorted(["b", "10", "2", "a"], key=summary.item_order) == ["2", "10", "a", "b"]


def test_a_question_with_a_failed_test_is_not_verified() -> None:
    settled = {"passes": ((), ("7",), ()), "fails": ((), ("7",), ())}
    lines = summary.question_lines(
        settled, {"passes": "passed", "fails": "failed"}, None, titles={}, narrowed=False
    )
    assert lines == [
        "verified 0 of 1:",
        "ran, evidence to read, 0 of 1:",
        "1 not verified (failed or skipped):",
        "    7",
    ]
