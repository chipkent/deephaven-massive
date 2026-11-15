"""Tests of the catalog of the `responses` group (`tests/_live/live_run/reply_examples.py`)."""

from __future__ import annotations

from datetime import date

import pytest

from tests._live.live_run import reply_examples
from tests._live.plans import TIERS, LivePlans

TODAY = date(2026, 11, 4)


def _plans(**tiers: str) -> LivePlans:
    return LivePlans({n: tiers.get(n, "basic") for n in TIERS}, s3=False, flat_files=(), source="t")


def _example(name: str) -> reply_examples.Example:
    return next(e for e in reply_examples.EXAMPLES if e.name == name)


@pytest.mark.parametrize(
    ("name", "tiers", "reason"),
    [
        ("market-status", {}, None),
        ("refusal-stocks-developer", {}, "not this tier: records the refusal on stocks starter"),
        ("stream-forex-quotes", {}, "needs Currencies Starter ($49/month)"),
        ("stream-forex-quotes", {"currencies": "starter"}, None),
        (
            "nvda-daily-2024-06-adjusted",
            {},
            "needs Stocks Starter ($29/month) (history back to 2024-06-03)",
        ),
        ("nvda-daily-2024-06-adjusted", {"stocks": "starter"}, None),
        ("nvda-daily-2024-06-adjusted", {"stocks": "advanced"}, None),
    ],
)
def test_an_example_is_skipped_with_what_it_needs(
    name: str, tiers: dict[str, str], reason: str | None
) -> None:
    assert reply_examples.skip_reason(_example(name), _plans(**tiers), TODAY) == reason


def test_history_beyond_every_window_but_the_last_names_the_tier_that_keeps_all() -> None:
    reason = reply_examples.history_need("stocks", date(2000, 1, 3), TODAY)
    assert reason == "needs Stocks Advanced ($199/month) (history back to 2000-01-03)"
