"""Tests of the reader of a vendor page's plan tables."""

from __future__ import annotations

import pytest

from deephaven_massive._massive.refresh.docs import DocsError
from deephaven_massive._massive.refresh.plan_access import plan_access
from deephaven_massive._massive.spec.types import PlanAccess, PlanTerms

_PAGE = """# REST
## Stocks

### Custom Bars (OHLC)

**Endpoint:** `GET /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}`

## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** 15-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | 15-minute delayed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Real-time |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
"""


def test_a_page_without_plan_sections_has_none() -> None:
    assert plan_access("# REST\n\n### Overview\n\nNothing about plans.\n") is None


def test_the_three_sections_merge_by_plan() -> None:
    assert plan_access(_PAGE) == PlanAccess(
        access="Included in select Stocks plans",
        recency="15-minute delayed or real-time",
        history="Not applicable to websockets",
        plans=(
            PlanTerms(plan="Stocks Basic", access="Not included", recency="Not included"),
            PlanTerms(plan="Stocks Starter", access="Included", recency="15-minute delayed"),
            PlanTerms(plan="Stocks Business", access="Included", recency="Real-time"),
            # Named only by the recency table: listed after the others, not included.
            PlanTerms(
                plan="Stocks Business + Expansion", access="Not included", recency="Real-time"
            ),
        ),
    )


def test_a_page_with_summaries_only_has_no_plans() -> None:
    page = (
        "### Analytics\n\n## Plan Access\n\n**Plan Access:** Available to existing subscribers"
        " only\n\n## Plan Recency\n\n**Plan Recency:** Updated daily\n\n## Plan History\n\n"
        "**Plan History:**\n\n# Next part\n\nIgnored.\n"
    )
    found = plan_access(page)
    assert found is not None
    assert (found.access, found.recency, found.history, found.plans) == (
        "Available to existing subscribers only",
        "Updated daily",
        "",
        (),
    )


@pytest.mark.parametrize(
    ("page", "message"),
    [
        pytest.param(
            _PAGE.replace("## Plan History", "## Plan Other"),
            "plan sections \\['Access', 'Recency'\\]",
            id="a section missing",
        ),
        pytest.param(
            _PAGE.replace("**Plan Recency:**", "**Plan Access:**"),
            "the Plan Recency section has no summary line",
            id="a summary naming another section",
        ),
        pytest.param(
            _PAGE.replace("**Plan Recency:** 15-minute delayed or real-time\n", ""),
            "the Plan Recency section has no summary line",
            id="no summary",
        ),
        pytest.param(
            _PAGE.replace("**Plan History:** Not applicable to websockets\n", ""),
            "the Plan History section has no summary line",
            id="an empty section",
        ),
        pytest.param(
            _PAGE.replace("| Stocks Starter | Included |", "Stocks Starter: Included"),
            "the Plan Access section has an unexpected line: 'Stocks Starter: Included'",
            id="a line outside a table",
        ),
        pytest.param(
            _PAGE.replace("| Stocks Business | Included |", "| Stocks Starter | Included |"),
            "the Plan Access section names 'Stocks Starter' twice",
            id="a plan twice",
        ),
    ],
)
def test_an_unexpected_shape_stops_the_refresh(page: str, message: str) -> None:
    with pytest.raises(DocsError, match=message):
        plan_access(page)
