"""The table of problem causes on the troubleshooting page, rendered from `CAUSES`.

`docs/troubleshooting.md` lists every `Cause` of `m.problems()` between two marker lines.
`scripts/render_reference.py` rewrites that part with `with_causes_reference`, so the page
follows `problem_events.CAUSES`, and a test checks the committed page is current. Pure text: no
Deephaven call.
"""

from __future__ import annotations

from typing import Final

from deephaven_massive._tables.problem_events import CAUSES

__all__ = [
    "CAUSES_REFERENCE_END",
    "CAUSES_REFERENCE_START",
    "causes_reference",
    "with_causes_reference",
]

CAUSES_REFERENCE_START: Final = (
    "<!-- The causes below are generated from problem_events.CAUSES by "
    "scripts/render_reference.py. Edit the causes, not this part of the page. -->"
)
"""The line before the generated table of `docs/troubleshooting.md`."""

CAUSES_REFERENCE_END: Final = "<!-- End of the generated causes. -->"
"""The line after the generated table of `docs/troubleshooting.md`."""


def causes_reference() -> str:
    """Return the Markdown table of every cause, in `CAUSES` order.

    Returns:
        One row per cause: its `Cause` value, its `Source`, its `Action`, whether it lasts
        until it clears (`EndedAt`), what happened, and what to do about it.
    """
    lines = [
        "| `Cause` | `Source` | `Action` | Lasts | What happened | What to do |",
        "|---|---|---|---|---|---|",
    ]
    for cause, traits in CAUSES.items():
        lasts = "yes" if traits.span else "no"
        lines.append(
            f"| `{cause.value}` | `{traits.source.value}` | `{traits.action.value}` | {lasts} | "
            f"{traits.summary} | {traits.fix} |"
        )
    return "\n".join(lines) + "\n"


def with_causes_reference(page: str) -> str:
    """Return `docs/troubleshooting.md` with its generated table replaced by `causes_reference()`.

    Args:
        page: The page's text. Everything outside its two markers is kept as written.

    Returns:
        The page as `scripts/render_reference.py` writes it.

    Raises:
        ValueError: If the page lacks `CAUSES_REFERENCE_START`, or `CAUSES_REFERENCE_END`
            after it.
    """
    start = page.find(CAUSES_REFERENCE_START)
    end = page.find(CAUSES_REFERENCE_END, max(start, 0))
    if start < 0 or end < 0:
        raise ValueError(
            "The troubleshooting page needs the lines CAUSES_REFERENCE_START and "
            "CAUSES_REFERENCE_END, in that order, around its generated table."
        )
    head = page[: start + len(CAUSES_REFERENCE_START)]
    return f"{head}\n\n{causes_reference()}\n{page[end:]}"
