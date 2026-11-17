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

from deephaven_massive._datasets.column_reference import COMMAND, plan_cell
from deephaven_massive._datasets.readme_plans import (
    PLANS_TABLE_END,
    PLANS_TABLE_START,
    ROWS,
    expected_plan_line,
    plan_line_problems,
    plans_table,
    with_plans_table,
)
from deephaven_massive._massive import Provenance, provenance
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Callable

README = Path(__file__).resolve().parents[2] / "README.md"
BARS_HISTORY = ROWS[0].urls[0]
SNAPSHOTS = next(row for row in ROWS if row.dataset == "stocks.snapshots").urls[0]


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


def _tables(text: str) -> list[list[list[str]]]:
    """Return each rendered table's rows, each as its cells, header and rule included."""
    tables: list[list[list[str]]] = []
    for block in text.split("\n\n"):
        rows = [line for line in block.splitlines() if line.startswith("| ")]
        if rows:
            tables.append([[cell.strip() for cell in row.strip("|").split(" | ")] for row in rows])
    return tables


def _lines(text: str) -> list[list[str]]:
    """Return the rows of the first rendered table, the one the stock examples read."""
    return _tables(text)[0]


def test_the_readme_holds_the_rendered_table() -> None:
    page = README.read_text(encoding="utf-8")
    assert page == with_plans_table(page, CATALOG, provenance()), (
        f"README.md's plans table is out of date; run `{COMMAND}`"
    )


def test_a_row_shows_what_each_dataset_page_shows_for_the_same_plan() -> None:
    labels = {f"[{r.label}](docs/datasets/{r.dataset}.md#plans)": r for r in ROWS}
    shown = []
    for table in _tables(plans_table(CATALOG, provenance())):
        header, body = table[0], table[2:]
        assert header[0] == "Data"
        # One family of plans per table: every plan's name starts with the same word.
        assert len({plan.split(maxsplit=1)[0] for plan in header[1:]}) == 1
        for cells in body:
            row = labels[cells[0]]
            shown.append(row)
            # Each cell appears in the dataset page's own Plans table, in the same plan's row.
            text = (README.parent / "docs" / "datasets" / f"{row.dataset}.md").read_text()
            for plan, cell in zip(header[1:], cells[1:], strict=True):
                line = next(x for x in text.splitlines() if x.startswith(f"| {plan} |"))
                assert f"| {cell} |" in line, (row.label, plan, cell)
    assert shown == list(ROWS)


def test_a_plan_one_page_lists_gets_a_column_with_empty_cells_elsewhere() -> None:
    base = provenance()
    plans = {t.plan: t.recency or "" for t in base.plans[SNAPSHOTS].plans}
    newest = f"{next(iter(plans)).split(maxsplit=1)[0]} Newest"
    changed = _with_page(base, SNAPSHOTS, {**plans, newest: "Real-time"})
    rows = _lines(plans_table(CATALOG, changed))
    assert rows[0][-1] == newest
    assert [row[-1] for row in rows[2:]] == ["", "", "", "", "", "Real-time"]


def test_a_page_with_different_terms_for_one_plan_shows_both() -> None:
    base = provenance()
    first = base.plans[BARS_HISTORY].plans[0].plan
    other = ROWS[0].urls[1]
    changed = _with_page(base, BARS_HISTORY, {first: "A new recency"})
    unchanged = plan_cell(first, [(other, base.plans[other])])
    cell = _lines(plans_table(CATALOG, changed))[2][1]
    assert cell == f"stocks: A new recency; stocks: {unchanged}"


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


# --- The Plan lines -----------------------------------------------------------------------------


def _example(anchor: str, plan: str | None) -> str:
    """Return one tutorial example's anchor, heading, and Plan line, when given one."""
    line = f"**Plan:** {plan}\n\n" if plan is not None else ""
    return f'<a id="{anchor}"></a>\n\n### An example\n\n{line}Some text.\n\n'


def _readme(plans: dict[str, str | None]) -> str:
    """Return a README whose tutorial holds an example per entry, then a later section."""
    body = "".join(_example(anchor, plan) for anchor, plan in plans.items())
    return f'# Title\n\n<a id="tutorial"></a>\n\n## Tutorial\n\n{body}## Next steps\n'


def _expected() -> dict[str, str | None]:
    """Return each example's Plan line, as the README holds it today."""
    return {anchor: _line(anchor) for row in ROWS for anchor in row.examples}


def _line(anchor: str) -> str:
    """Return the Plan line `expected_plan_line` gives one example, on the real provenance."""
    return expected_plan_line(
        [
            [(url, provenance().plans[url]) for url in row.urls]
            for row in ROWS
            if anchor in row.examples
        ]
    )


def test_every_plan_line_of_the_readme_is_massives() -> None:
    page = README.read_text(encoding="utf-8")
    assert plan_line_problems(page, CATALOG, provenance()) == []


def test_a_plan_line_is_any_plan_or_the_first_plan_that_includes_the_data() -> None:
    plans = list(dict.fromkeys(t.plan for t in provenance().plans[SNAPSHOTS].plans))
    snapshots = provenance().plans[SNAPSHOTS]
    every = dataclasses.replace(
        snapshots, plans=tuple(dataclasses.replace(t, access="Included") for t in snapshots.plans)
    )
    assert expected_plan_line([[(SNAPSHOTS, every)]]) == f"Any {plans[0].split()[0]} plan"
    later = dataclasses.replace(
        snapshots,
        plans=tuple(
            dataclasses.replace(t, access="Not included" if i == 0 else "Included")
            for i, t in enumerate(snapshots.plans)
        ),
    )
    assert expected_plan_line([[(SNAPSHOTS, later)]]) == f"{plans[1]} or higher"


def test_a_wrong_missing_or_unknown_plan_line_is_a_problem() -> None:
    plans = _expected()
    plans["read-daily-bars"] = "Not a plan"
    plans["stream-minute-bars"] = None
    plans["an-example-no-row-reads"] = _line("read-daily-bars")
    del plans["dashboard"]
    assert plan_line_problems(_readme(plans), CATALOG, provenance()) == [
        "an-example-no-row-reads: no row of ROWS names this example",
        "dashboard: not an example of the tutorial",
        f"read-daily-bars: **Plan:** 'Not a plan'; expected {_line('read-daily-bars')!r}",
        (
            "stream-minute-bars: no **Plan:** line under its heading; expected "
            f"{_line('stream-minute-bars')!r}"
        ),
    ]
    assert plan_line_problems(_readme(_expected()), CATALOG, provenance()) == []


def test_a_readme_without_a_tutorial_is_refused() -> None:
    with pytest.raises(ValueError, match="tutorial"):
        plan_line_problems("# Title\n", CATALOG, provenance())


def test_plans_no_plan_line_can_describe_are_refused() -> None:
    snapshots = provenance().plans[SNAPSHOTS]
    # Included only in the middle: no "X or higher" says it.
    gapped = dataclasses.replace(
        snapshots,
        plans=tuple(
            dataclasses.replace(t, access="Included" if i == 1 else "Not included")
            for i, t in enumerate(snapshots.plans)
        ),
    )
    with pytest.raises(ValueError, match="no Plan line can say"):
        expected_plan_line([[(SNAPSHOTS, gapped)]])
    # Included by every plan but one in the middle: "X or higher" would drop the first.
    holed = dataclasses.replace(
        snapshots,
        plans=tuple(
            dataclasses.replace(t, access="Not included" if i == 1 else "Included")
            for i, t in enumerate(snapshots.plans)
        ),
    )
    with pytest.raises(ValueError, match="no Plan line can say"):
        expected_plan_line([[(SNAPSHOTS, holed)]])
    # Included by none.
    none = dataclasses.replace(
        snapshots,
        plans=tuple(dataclasses.replace(t, access="Not included") for t in snapshots.plans),
    )
    with pytest.raises(ValueError, match="no Plan line can say"):
        expected_plan_line([[(SNAPSHOTS, none)]])
    # Included by every plan, of two families.
    mixed = dataclasses.replace(
        snapshots,
        plans=(
            dataclasses.replace(snapshots.plans[-1], plan="One kind"),
            dataclasses.replace(snapshots.plans[-1], plan="Another kind"),
        ),
    )
    with pytest.raises(ValueError, match="share no first word"):
        expected_plan_line([[(SNAPSHOTS, mixed)]])
