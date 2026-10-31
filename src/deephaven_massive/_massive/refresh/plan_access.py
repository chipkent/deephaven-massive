"""Read the plan tables at the end of a vendor documentation page.

Every REST, WebSocket, and flat-file page Massive publishes for an endpoint, feed, or file ends
in three sections that say which of Massive's plans get the data:

```
## Plan Access

**Plan Access:** Included in all Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** End-of-day, 15-minute delayed, or real-time
...

## Plan History

**Plan History:** Records date back to September 10, 2003
...
```

Each section has its summary line and up to two tables (individual and business plans) of
`| <plan> | <value> |` rows; a WebSocket page's history section has a summary line and no
table. `plan_access(page)` reads the three sections and the page's title (its one `###`
heading) into one `PlanAccess`, with one `PlanTerms` per plan, merged across the sections by
the plan's name. The refresh stores the result in `descriptions.json`, keyed by page URL, and
the generated dataset reference renders it, so the reference shows Massive's own plan tables
and follows them on every refresh.

Every input is untrusted vendor text: it is parsed with anchored regular expressions and never
evaluated. A page without a "Plan Access" section has no plan tables (an overview, a
quickstart); a page that has one but not the shape above raises `docs.DocsError`, which stops
the refresh, since a changed layout is a change at Massive that a person must look at.

This module imports only the standard library, `refresh.docs`, and `spec.types`.
"""

from __future__ import annotations

import re
from typing import Final

from deephaven_massive._massive.refresh.docs import DocsError
from deephaven_massive._massive.spec.types import PlanAccess, PlanTerms

__all__ = ["plan_access"]

_SECTIONS: Final = ("Access", "Recency", "History")
"""The three plan sections, in page order, by the word after `## Plan `."""

_TITLE: Final = re.compile(r"^### (?P<title>\S.*?)\s*$", re.MULTILINE)
"""A page's title heading."""

_SECTION: Final = re.compile(r"^## Plan (?P<name>Access|Recency|History)\s*$")
"""The heading of one plan section."""

_SUMMARY: Final = re.compile(
    r"^\*\*Plan (?P<name>Access|Recency|History):\*\*(?: (?P<text>\S.*?))?\s*$"
)
"""A plan section's summary line, which repeats the section's name."""

_ROW: Final = re.compile(r"^\| (?P<plan>[^|]*?\S) \| (?P<value>[^|]*?\S) \|\s*$")
"""One row of a plan table: the plan's name and its value."""

_TABLE_LINES: Final = re.compile(
    r"^(?:#### (?:Individual|Business) Plans|\| Plan \| \w+ \||\|(?: ?-+ ?\|)+)\s*$"
)
"""A line a plan table brings with it: its subheading, its header row, or its separator row."""


def plan_access(page: str) -> PlanAccess | None:
    """Read what one vendor documentation page says about Massive's plans.

    Args:
        page: The page's Markdown text, as the snapshot stores it.

    Returns:
        The page's title, the three summary lines (a summary Massive left empty is `""`), and
        one `PlanTerms` per plan, in the order the access section lists the plans (a plan only
        the recency or history section names follows them, `"Not included"` in access); no
        plans for a page whose sections have only their summary lines ("Available to existing
        subscribers only"); None for a page without a "Plan Access" section.

    Raises:
        DocsError: If the page has a "Plan Access" section but no title, a section is missing,
            out of order, or repeated, a section lacks its summary line, a section holds a line
            that is not part of a plan table, or a plan appears twice in one section.
    """
    lines = page.splitlines()
    starts = [(i, found["name"]) for i, line in enumerate(lines) if (found := _SECTION.match(line))]
    if not starts:
        return None
    title = _TITLE.search(page)
    if title is None:
        raise DocsError("a page with plan tables has no ### title")
    if [name for _, name in starts] != list(_SECTIONS):
        raise DocsError(f"plan sections {[name for _, name in starts]}, expected {list(_SECTIONS)}")
    summaries: dict[str, str] = {}
    values: dict[str, dict[str, str]] = {}
    ends = [i for i, _ in starts[1:]] + [_section_end(lines, starts[-1][0])]
    for (start, name), end in zip(starts, ends, strict=True):
        summaries[name], values[name] = _section(name, lines[start + 1 : end])
    plans = list(dict.fromkeys([*values["Access"], *values["Recency"], *values["History"]]))
    return PlanAccess(
        title=title["title"],
        access=summaries["Access"],
        recency=summaries["Recency"],
        history=summaries["History"],
        plans=tuple(
            PlanTerms(
                plan=plan,
                access=values["Access"].get(plan, "Not included"),
                recency=values["Recency"].get(plan),
                history=values["History"].get(plan),
            )
            for plan in plans
        ),
    )


def _section_end(lines: list[str], start: int) -> int:
    """Return where the last plan section ends: the next `#` or `##` heading, or the page's end.

    Args:
        lines: The page's lines.
        start: The index of the section's heading.

    Returns:
        The index of the first line after the section.
    """
    for index in range(start + 1, len(lines)):
        if re.match(r"^#{1,2} ", lines[index]):
            return index
    return len(lines)


def _section(name: str, lines: list[str]) -> tuple[str, dict[str, str]]:
    """Read one plan section's summary line and its rows.

    Args:
        name: The section's name: `"Access"`, `"Recency"`, or `"History"`.
        lines: The section's lines after its heading.

    Returns:
        The summary text and each plan's value, in table order.

    Raises:
        DocsError: If the summary line is missing or names another section, a line is
            neither blank nor part of a plan table, or a plan appears twice.
    """
    content = [line for line in lines if line.strip()]
    summary = _SUMMARY.match(content[0]) if content else None
    if summary is None or summary["name"] != name:
        raise DocsError(f"the Plan {name} section has no summary line")
    rows: dict[str, str] = {}
    for line in content[1:]:
        if _TABLE_LINES.match(line):
            continue
        row = _ROW.match(line)
        if row is None:
            raise DocsError(f"the Plan {name} section has an unexpected line: {line!r:.80}")
        if row["plan"] in rows:
            raise DocsError(f"the Plan {name} section names {row['plan']!r} twice")
        rows[row["plan"]] = row["value"]
    return summary["text"] or "", rows
