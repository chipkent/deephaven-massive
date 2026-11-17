"""Tests for the README's plans table: parity, its columns and cells, and its refusals.

The parity test renders the table from the process's catalog and the committed provenance and
fails when README.md holds anything else between its markers, naming the command that fixes
it. The other tests change one page's plan table in a copy of the real provenance and check
what the table does with it.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._datasets.column_reference import COMMAND
from deephaven_massive._datasets.readme_plans import (
    PLANS_TABLE_END,
    PLANS_TABLE_START,
    ROWS,
    plans_table,
    with_plans_table,
)
from deephaven_massive._massive import Provenance, provenance
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Callable

README = Path(__file__).resolve().parents[2] / "README.md"
BARS_HISTORY = ROWS[0].urls[0]
SNAPSHOTS = ROWS[-1].urls[0]


def _with_page(base: Provenance, url: str, plans: dict[str, str]) -> Provenance:
    """Return `base` with one page's plan table replaced.

    Args:
        base: The provenance to copy.
        url: The page whose plan table changes.
        plans: Each plan's recency on the page, with access `"Included"` and no history.

    Returns:
        The copy.
    """
    page = base.plans[url]
    terms = page.plans[0]
    table = tuple(
        dataclasses.replace(terms, plan=plan, access="Included", recency=recency, history=None)
        for plan, recency in plans.items()
    )
    return dataclasses.replace(
        base, plans={**base.plans, url: dataclasses.replace(page, plans=table)}
    )


def _lines(table: str) -> list[list[str]]:
    """Return a rendered table's rows, each as its cells."""
    rows = [line for line in table.splitlines() if line.startswith("| ")]
    return [[cell.strip() for cell in row.strip("|").split(" | ")] for row in rows]


def test_the_readme_holds_the_rendered_table() -> None:
    page = README.read_text(encoding="utf-8")
    assert page == with_plans_table(page, CATALOG, provenance()), (
        f"README.md's plans table is out of date; run `{COMMAND}`"
    )


def test_a_row_shows_what_each_dataset_page_shows_for_the_same_plan() -> None:
    rows = _lines(plans_table(CATALOG, provenance()))
    header, body = rows[0], rows[2:]
    assert header[0] == "Data"
    assert [row[0] for row in body] == [
        f"[{r.label}](docs/datasets/{r.dataset}.md#plans)" for r in ROWS
    ]
    # Each cell appears in the dataset page's own Plans table, in the same plan's row.
    for row, cells in zip(ROWS, body, strict=True):
        dataset_page = (README.parent / "docs" / "datasets" / f"{row.dataset}.md").read_text()
        for plan, cell in zip(header[1:], cells[1:], strict=True):
            line = next(x for x in dataset_page.splitlines() if x.startswith(f"| {plan} |"))
            assert f"| {cell} |" in line, (row.label, plan, cell)


def test_a_plan_one_page_lists_gets_a_column_with_empty_cells_elsewhere() -> None:
    base = provenance()
    plans = {t.plan: t.recency or "" for t in base.plans[SNAPSHOTS].plans}
    changed = _with_page(base, SNAPSHOTS, {**plans, "Stocks Platinum": "Real-time"})
    rows = _lines(plans_table(CATALOG, changed))
    assert rows[0][-1] == "Stocks Platinum"
    assert [row[-1] for row in rows[2:]] == ["", "", "", "", "Real-time"]


def test_a_page_with_different_terms_for_one_plan_shows_both() -> None:
    base = provenance()
    changed = _with_page(base, BARS_HISTORY, {"Stocks Basic": "Real-time"})
    cell = _lines(plans_table(CATALOG, changed))[2][1]
    assert cell == "stocks: Real-time; stocks: End-of-day, 2 years"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda p: dataclasses.replace(
                p, plans={u: a for u, a in p.plans.items() if u != SNAPSHOTS}
            ),
            "has no plan tables",
        ),
        (
            lambda p: dataclasses.replace(
                p,
                urls={k: [u for u in v if u != SNAPSHOTS] for k, v in p.urls.items()},
            ),
            "is not a vendor page of stocks.snapshots",
        ),
    ],
)
def test_a_row_whose_page_is_gone_stops_the_render(
    change: Callable[[Provenance], Provenance], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        plans_table(CATALOG, change(provenance()))


def test_the_rewrite_keeps_everything_outside_the_markers_and_is_idempotent() -> None:
    page = f"before\n{PLANS_TABLE_START}\nstale\n{PLANS_TABLE_END}\nafter\n"
    rewritten = with_plans_table(page, CATALOG, provenance())
    assert rewritten.startswith(f"before\n{PLANS_TABLE_START}\n\n")
    assert rewritten.endswith(f"\n{PLANS_TABLE_END}\nafter\n")
    assert "stale" not in rewritten
    assert with_plans_table(rewritten, CATALOG, provenance()) == rewritten


@pytest.mark.parametrize(
    "page",
    ["no markers", f"{PLANS_TABLE_END}\n{PLANS_TABLE_START}\n", f"{PLANS_TABLE_START}\n"],
)
def test_a_readme_without_both_markers_is_refused(page: str) -> None:
    with pytest.raises(ValueError, match="PLANS_TABLE_START and PLANS_TABLE_END"):
        with_plans_table(page, CATALOG, provenance())
