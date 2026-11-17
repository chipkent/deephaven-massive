"""The README's plans: its generated plans table, and the check of each example's Plan line.

The README's tutorial reads a few datasets, and a reader needs to know which of Massive's plans
runs each example. Two things tell them, both tied to the plan tables of Massive's own
documentation pages (`Provenance.plans`), so both follow every refresh of Massive's
documentation:

- The plans table, generated here: how recent each dataset the tutorial reads is on each plan,
  and how far back its history goes. Its cells are the ones each dataset's page shows, made by
  the same function (`column_reference.plan_cell`), so the README and the dataset pages never
  disagree. `scripts/render_reference.py` rewrites it between the README's two marker lines
  (`with_plans_table`).
- A `**Plan:**` line under each example's heading, written by hand in Massive's plan names and
  checked here (`plan_line_problems`): the line must say the plans that include every dataset
  the example reads, as "Any <family> plan" or "<plan> or higher".

The only hand input is `ROWS`: which of Massive's pages document the data each row names, and
which of the README's examples read it, by their anchors. It names datasets, pages, and
anchors, never a plan: the columns and the expected Plan lines come from whatever plans those
pages list, in the order they list them, so a plan Massive adds or renames reaches the README
at the next refresh. A row whose page the refresh no longer has, or that isn't a page of its
dataset, stops the render with `ValueError` instead of leaving the row out.

Tests fail when the committed README differs from the rendered table, or when a Plan line
disagrees with Massive's pages, so a refresh that changes a plan table lands with the README
updated. The README is this module's only reader. It is pure: it reads only its arguments and
writes nothing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.column_reference import (
    COMMAND,
    NOT_INCLUDED,
    dataset_urls,
    page_name,
    plan_cell,
    table_row,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from deephaven_massive._datasets.catalog import Catalog
    from deephaven_massive._massive import PlanAccess, Provenance

__all__ = [
    "PLANS_TABLE_END",
    "PLANS_TABLE_START",
    "ROWS",
    "PlansRow",
    "expected_plan_line",
    "plan_line_problems",
    "plans_table",
    "with_plans_table",
]


@dataclass(frozen=True)
class PlansRow:
    """One row of the README's plans table: some data the tutorial reads.

    Attributes:
        label: The row's name in the table, in the README's words, such as `"Bars, history"`.
        dataset: The dataset's dotted name, such as `"stocks.bars"`; the label links its page.
        urls: The URLs of the vendor pages, as the refresh fetched them, whose plan tables give
            the row's cells. A dataset documented on several pages, such as bars read from
            REST and streamed, names only the pages of the data the tutorial reads.
        examples: The anchors of the README's examples that read this data, such as
            `"read-daily-bars"`; their Plan lines must name the plans that include it.
    """

    label: str
    dataset: str
    urls: tuple[str, ...]
    examples: tuple[str, ...]


_REST: Final = "https://massive.com/docs/rest/stocks"
"""The prefix of Massive's REST pages for stocks."""

ROWS: Final = (
    PlansRow(
        "Bars, history",
        "stocks.bars",
        (
            f"{_REST}/aggregates/custom-bars.md",
            f"{_REST}/aggregates/daily-market-summary.md",
        ),
        (
            "read-daily-bars",
            "whole-market-day",
            "compare-stocks",
            "chart-closes",
            "backfill-then-live",
        ),
    ),
    PlansRow(
        "Minute bars, streamed",
        "stocks.bars",
        ("https://massive.com/docs/websocket/stocks/aggregates-per-minute.md",),
        (
            "stream-minute-bars",
            "change-since-close",
            "table-shapes",
            "backfill-then-live",
            "dashboard",
        ),
    ),
    PlansRow(
        "Previous day bar",
        "stocks.previous_day_bar",
        (f"{_REST}/aggregates/previous-day-bar.md",),
        (
            "drive-with-a-watchlist",
            "convert-symbols",
            "change-since-close",
            "dashboard",
            "stop-live-tables",
        ),
    ),
    PlansRow(
        "News", "reference.news", (f"{_REST}/news.md",), ("follow-the-news", "stop-live-tables")
    ),
    PlansRow(
        "Ticker details",
        "reference.ticker_details",
        (f"{_REST}/tickers/ticker-overview.md",),
        ("see-problems",),
    ),
    PlansRow(
        "Snapshots",
        "stocks.snapshots",
        (f"{_REST}/snapshots/full-market-snapshot.md",),
        ("what-your-plan-allows",),
    ),
    PlansRow(
        "Option chain",
        "options.chain",
        ("https://massive.com/docs/rest/options/snapshots/option-chain-snapshot.md",),
        ("options-chain",),
    ),
)
"""The data the README's tutorial reads, in the order its examples first read it."""

PLANS_TABLE_START: Final = (
    f"<!-- The plans table is generated by `{COMMAND}`; do not edit it by hand. -->"
)
"""The line before the README's generated plans table."""

PLANS_TABLE_END: Final = "<!-- End of the generated plans table. -->"
"""The line after the README's generated plans table."""

_GUIDE: Final = (
    "Each cell gives how recent a plan's data is and how far back its history goes, as "
    "Massive's pages state them."
)
"""The paragraph above the table."""

_TUTORIAL: Final = re.compile(r'<a id="tutorial"></a>\n\n## [^\n]*\n.*?(?=\n## |\Z)', re.DOTALL)
"""The README's tutorial section, up to the next second-level heading."""

_EXAMPLE: Final = re.compile(r'<a id="([^"]+)"></a>\n\n### [^\n]+\n\n(?:\*\*Plan:\*\* ([^\n]+)\n)?')
"""An example's anchor and heading, and its Plan line when one follows the heading."""


def _row_pages(
    row: PlansRow, catalog: Catalog, provenance: Provenance
) -> list[tuple[str, PlanAccess]]:
    """Return a row's vendor pages with their plan tables.

    Args:
        row: The row.
        catalog: The catalog, which holds the row's dataset.
        provenance: The vendor pages' plan tables, by URL, and each source's pages.

    Returns:
        Each of the row's URLs with its plan tables, in the row's order.

    Raises:
        KeyError: If the row's dataset isn't in the catalog.
        ValueError: If a URL isn't one of the dataset's vendor pages, or has no plan tables.
    """
    own = dataset_urls(catalog.by_name[row.dataset], provenance)
    for url in row.urls:
        if url not in own:
            raise ValueError(f"{url} is not a vendor page of {row.dataset}")
        if url not in provenance.plans:
            raise ValueError(f"{url} has no plan tables in the refreshed documentation")
    return [(url, provenance.plans[url]) for url in row.urls]


def _plans(pages: Sequence[Sequence[tuple[str, PlanAccess]]]) -> list[str]:
    """Return every plan the pages list, in the order they first list them."""
    return list(dict.fromkeys(t.plan for row in pages for _, page in row for t in page.plans))


def plans_table(catalog: Catalog, provenance: Provenance) -> str:
    """Render the README's plans table: one table per family of plans.

    Massive's plan names start with the subscription they belong to, so the plans the rows'
    pages list fall into families by their first word. Each family gets its own table, so a
    reader of one subscription's examples isn't given a column for each plan of another.

    Args:
        catalog: The catalog, which holds each row's dataset.
        provenance: The vendor pages' plan tables, by URL, and each source's pages.

    Returns:
        The guide, then each family's table after a blank line, ending in one newline. A table
        has a header of **Data** and the family's plans in the order the pages list them, then
        one row per entry of `ROWS` whose pages list a plan of the family: its label linked to
        its dataset's page, and each cell `plan_cell` of its pages for that plan.

    Raises:
        KeyError: If a row's dataset isn't in the catalog.
        ValueError: If a row's URL isn't one of its dataset's vendor pages, or has no plan
            tables.
    """
    pages = [_row_pages(row, catalog, provenance) for row in ROWS]
    families: dict[str, list[str]] = {}
    for plan in _plans(pages):
        families.setdefault(_family(plan), []).append(plan)
    lines = [_GUIDE]
    for plans in families.values():
        lines.extend(("", table_row(["Data", *plans]), table_row(["---"] * (len(plans) + 1))))
        for row, row_pages in zip(ROWS, pages, strict=True):
            if not set(plans) & set(_plans([row_pages])):
                continue
            name = page_name(catalog.by_name[row.dataset])
            link = f"[{row.label}](docs/datasets/{name}#plans)"
            lines.append(table_row([link, *(plan_cell(plan, row_pages) for plan in plans)]))
    return "\n".join(lines) + "\n"


def _family(plan: str) -> str:
    """Return the family of a plan: the first word of its name, its subscription."""
    return plan.split(maxsplit=1)[0]


def with_plans_table(page: str, catalog: Catalog, provenance: Provenance) -> str:
    """Return the README with its generated plans table replaced by `plans_table()`.

    Args:
        page: The README's text. Everything outside its two marker lines is kept as written.
        catalog: The catalog, for `plans_table`.
        provenance: The vendor pages' plan tables, for `plans_table`.

    Returns:
        The README as `scripts/render_reference.py` writes it.

    Raises:
        ValueError: If the page lacks `PLANS_TABLE_START`, or `PLANS_TABLE_END` after it, or
            as `plans_table` raises.
    """
    start = page.find(PLANS_TABLE_START)
    end = page.find(PLANS_TABLE_END, max(start, 0))
    if start < 0 or end < 0:
        raise ValueError(
            "the README needs the lines PLANS_TABLE_START and PLANS_TABLE_END around its "
            "generated plans table"
        )
    head = page[: start + len(PLANS_TABLE_START)]
    return f"{head}\n\n{plans_table(catalog, provenance)}\n{page[end:]}"


def expected_plan_line(pages: Sequence[Sequence[tuple[str, PlanAccess]]]) -> str:
    """Say which plans include all the data of some rows, as an example's Plan line says it.

    A plan includes a row's data when every one of the row's pages shows it something other
    than `NOT_INCLUDED`; a page that doesn't list the plan doesn't include it.

    Args:
        pages: The vendor pages of each row the example reads, with their plan tables.

    Returns:
        "Any <family> plan" when every plan the pages list includes all the data, where
        <family> is the first word every plan's name shares; or
        "<plan> or higher" when that plan and every plan the pages list after it include all
        the data, and the plan before it doesn't.

    Raises:
        ValueError: If the plans' names share no first word, or if the plans that include the
            data aren't one plan and every plan after it, since no Plan line can say that.
    """
    plans = _plans(pages)
    included = [
        all(
            plan_cell(plan, [(url, page)]) not in ("", NOT_INCLUDED)
            for row in pages
            for url, page in row
        )
        for plan in plans
    ]
    if all(included):
        families = {_family(plan) for plan in plans}
        if len(families) != 1:
            raise ValueError(f"the plans {plans} share no first word")
        return f"Any {families.pop()} plan"
    first = len(included) - next(i for i, ok in enumerate(reversed(included)) if not ok)
    if first == len(plans) or not all(included[first:]):
        raise ValueError(f"no Plan line can say which of {plans} include the data")
    return f"{plans[first]} or higher"


def plan_line_problems(page: str, catalog: Catalog, provenance: Provenance) -> list[str]:
    """Check each example's Plan line in the README against Massive's pages.

    Args:
        page: The README's text.
        catalog: The catalog, which holds each row's dataset.
        provenance: The vendor pages' plan tables.

    Returns:
        One line per problem, empty when every Plan line is right: an example in `ROWS` the
        tutorial lacks, an example of the tutorial no row names, an example without a Plan
        line, and a Plan line that says something other than `expected_plan_line` of the rows
        the example reads.

    Raises:
        ValueError: If the README has no tutorial section, or as `_row_pages` and
            `expected_plan_line` raise.
    """
    tutorial = _TUTORIAL.search(page)
    if tutorial is None:
        raise ValueError('the README needs a section that starts <a id="tutorial"></a>')
    lines: dict[str, str] = dict(_EXAMPLE.findall(tutorial.group()))
    named = {anchor for row in ROWS for anchor in row.examples}
    problems = [f"{anchor}: no row of ROWS names this example" for anchor in lines.keys() - named]
    problems.extend(f"{anchor}: not an example of the tutorial" for anchor in named - lines.keys())
    for anchor in sorted(named & lines.keys()):
        rows = [_row_pages(row, catalog, provenance) for row in ROWS if anchor in row.examples]
        expected = expected_plan_line(rows)
        if not lines[anchor]:
            problems.append(f"{anchor}: no **Plan:** line under its heading; expected {expected!r}")
        elif lines[anchor] != expected:
            problems.append(f"{anchor}: **Plan:** {lines[anchor]!r}; expected {expected!r}")
    return sorted(problems)
