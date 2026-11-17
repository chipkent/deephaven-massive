"""Tests for the troubleshooting page's table of problem causes."""

import re
from pathlib import Path

import pytest

from deephaven_massive._tables.cause_reference import (
    CAUSES_REFERENCE_END,
    CAUSES_REFERENCE_START,
    causes_reference,
    with_causes_reference,
)
from deephaven_massive._tables.problem_events import ProblemCause

PAGE = Path(__file__).resolve().parents[2] / "docs" / "troubleshooting.md"


def test_the_table_has_a_row_per_cause() -> None:
    lines = causes_reference().splitlines()
    assert lines[0] == "| `Cause` | `Source` | `Action` | Lasts | What happened | What to do |"
    assert [line.split(" | ")[0] for line in lines[2:]] == [
        f"| `{cause.value}`" for cause in ProblemCause
    ]
    assert lines[2].startswith("| `noncanonical_symbols` | `user` | `SKIPPED` | yes | A value ")
    assert lines[2].endswith(
        " | Fix or delete the row. Massive's lists, such as `m.reference.tickers()`, "
        "show its spelling. |"
    )
    assert any(
        line.startswith("| `schema_drift` | `massive` | `PUBLISHED` | no | ") for line in lines
    )


def test_the_committed_page_is_current() -> None:
    page = PAGE.read_text(encoding="utf-8")
    assert page == with_causes_reference(page), (
        "docs/troubleshooting.md is stale: run `uv run python scripts/render_reference.py`"
    )


def test_a_rewrite_keeps_the_rest_of_the_page_and_is_stable() -> None:
    page = f"intro\n{CAUSES_REFERENCE_START}\nold\n{CAUSES_REFERENCE_END}\nafter\n"
    rewritten = with_causes_reference(page)
    assert rewritten.startswith(f"intro\n{CAUSES_REFERENCE_START}\n\n| `Cause`")
    assert rewritten.endswith(f"\n\n{CAUSES_REFERENCE_END}\nafter\n")
    assert with_causes_reference(rewritten) == rewritten


@pytest.mark.parametrize(
    "page", ["no markers", CAUSES_REFERENCE_END + CAUSES_REFERENCE_START, CAUSES_REFERENCE_START]
)
def test_a_page_without_both_markers_in_order_is_refused(page: str) -> None:
    refused = (
        "The troubleshooting page needs the lines CAUSES_REFERENCE_START and "
        "CAUSES_REFERENCE_END, in that order, around its generated table."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(refused)}$"):
        with_causes_reference(page)
