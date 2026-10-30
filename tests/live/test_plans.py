"""Tests of the key's plans: coverage, probe, cache, and history verdict (`tests/_live/plans.py`).

The probe runs against `FakeRestServer` and `FakeS3`, scripted as each plan answers.
"""

from __future__ import annotations

from datetime import date

import pytest

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the servers come from the `fake_rest`
# and `fake_s3` fixtures.
from tests._live import plans

_DAY = date(2026, 10, 29)
"""The run's day of the history checks."""


def test_a_declaration() -> None:
    declared = plans.declared_plans(" Stocks=Starter, ,s3,options=business_expansion")
    assert declared.tiers == dict.fromkeys(plans.TIERS, "basic") | {
        "stocks": "starter",
        "options": "business_expansion",
    }
    assert declared.s3
    assert declared.flat_files is None
    assert declared.source == "declared in MASSIVE_LIVE_PLANS"


@pytest.mark.parametrize("text", ["dummy", "stocks=gold", "indices=developer"])
def test_a_wrong_declaration_is_a_usage_error(text: str) -> None:
    with pytest.raises(pytest.UsageError, match="is not <class>=<plan> or s3"):
        plans.declared_plans(text)


def test_the_plans_in_a_line() -> None:
    tiers = dict.fromkeys(plans.TIERS, "basic")
    probed = plans.LivePlans(tiers=tiers, s3=False, flat_files=(), source="probed x")
    assert plans.plans_text(probed).endswith("futures=basic; flat files: none (probed x)")
    assert "flat files: a/b, c/d (" in plans.plans_text(
        plans.LivePlans(tiers=tiers, s3=True, flat_files=("a/b", "c/d"), source="x")
    )
    assert "flat files: declared (" in plans.plans_text(plans.declared_plans("s3"))
    assert "flat files: none (" in plans.plans_text(plans.declared_plans(""))


@pytest.mark.parametrize(
    ("held", "needed", "met"),
    [
        ("business", "advanced", False),
        ("business", "developer", False),
        ("business", "starter", True),
        ("business", "basic", True),
        ("business", "business", True),
        ("business", "business_expansion", False),
        ("business_expansion", "advanced", True),
        ("business_expansion", "business", True),
        ("advanced", "business", False),
        ("advanced", "business_expansion", False),
        ("advanced", "developer", True),
        ("starter", "developer", False),
    ],
)
def test_a_stocks_or_options_business_plan_is_a_branch(held: str, needed: str, met: bool) -> None:
    assert plans.meets("stocks", held, needed) is met
    assert plans.meets("options", held, needed) is met


def test_the_other_classes_are_ordered() -> None:
    assert plans.meets("indices", "business", "advanced")
    assert plans.meets("futures", "business", "developer")
    assert not plans.meets("currencies", "starter", "business")


def test_unmet_requirements() -> None:
    held = plans.declared_plans("stocks=business,s3")
    needs = ("stocks:starter", "stocks:advanced", "s3", "some:basic", "options:basic")
    assert plans.unmet(needs, held) == ["stocks:advanced"]
    paid = plans.declared_plans(",".join(f"{n}=business" for n in plans.TIERS))
    assert plans.unmet(("some:basic", "s3"), paid) == ["some:basic", "s3"]


@pytest.mark.parametrize(
    ("requirement", "text"),
    [
        ("s3", "S3 flat files (MASSIVE_S3_* credentials, on the class's Starter plan)"),
        ("some:basic", "at least one class left on Basic"),
        ("stocks:basic", "Stocks Basic (free)"),
        ("currencies:starter", "Currencies Starter ($49/month)"),
        (
            "options:business_expansion",
            "Options Business with the trades and quotes expansion (price on request)",
        ),
    ],
)
def test_the_text_of_a_requirement(requirement: str, text: str) -> None:
    assert plans.plan_text(requirement) == text


def test_the_flat_files_a_plan_reaches() -> None:
    assert plans.reaches(plans.declared_plans("s3"), "any/prefix")
    tiers = dict.fromkeys(plans.TIERS, "basic")
    probed = plans.LivePlans(tiers=tiers, s3=True, flat_files=("a/b",), source="x")
    assert plans.reaches(probed, "a/b")
    assert not plans.reaches(probed, "c/d")


def test_the_documented_history_windows() -> None:
    held = plans.declared_plans("stocks=starter,options=advanced")
    assert plans.history_start(held, "stocks", date(2026, 10, 29)) == date(2021, 10, 29)
    assert plans.history_start(held, "currencies", date(2028, 2, 29)) == date(2026, 2, 28)
    assert plans.history_start(held, "options", _DAY) is None
    assert plans.history_start(held, "indices", _DAY) is None


def _refusal(target: str, detail: str | None = None) -> Exception:
    detail = detail or (
        "Your plan doesn't include this data timeframe. Please upgrade your plan at "
        "https://massive.com/pricing"
    )
    return Exception(f"HTTP 403 from GET {target} on api.massive.com (request_id 1): {detail}")


def test_a_timeframe_refusal_is_matched_by_its_phrase() -> None:
    assert plans.is_timeframe_refusal(_refusal("/v2/aggs/ticker/AAPL/prev"))
    assert plans.is_timeframe_refusal(
        Exception("Your plan doesn\u2019t include this data timeframe")
    )
    assert not plans.is_timeframe_refusal(_refusal("/v3/x?timeframe=DELAYED", "not entitled"))
    assert not plans.is_timeframe_refusal(Exception("timeframe"))


def test_the_refused_request() -> None:
    option = "/v2/aggs/ticker/O%3ANVDA240621C00120000/range/1/day/2023-06-12/2023-06-05"
    assert plans.refused_request(_refusal(option)) == ("options", date(2023, 6, 5))
    futures = "/futures/v1/aggs/ESZ3?window_start.gte=2023-10-28"
    assert plans.refused_request(_refusal(futures)) == ("futures", date(2023, 10, 28))
    assert plans.refused_request(_refusal("/v2/aggs/ticker/C%3AEURUSD/prev")) == (
        "currencies",
        None,
    )
    assert plans.refused_request(Exception("no target")) == (None, None)


def test_the_history_verdict() -> None:
    basic = plans.declared_plans("")
    keeps = plans.declared_plans("stocks=starter,options=advanced")
    old = _refusal("/v2/aggs/ticker/AAPL/range/1/day/2023-01-02/2023-01-09?adjusted=true")
    assert plans.history_verdict(old, basic, _DAY) == (
        "outside the plan's history: 2023-01-02 is before 2024-10-29, the first day stocks keeps"
    )
    assert plans.history_verdict(old, keeps, _DAY) is None  # Starter keeps five years
    recent = _refusal("/v2/aggs/ticker/AAPL/range/1/day/2026-01-02/2026-01-09")
    assert plans.history_verdict(recent, basic, _DAY) is None
    option = _refusal("/v2/aggs/ticker/O%3ASPY230616C00400000/range/1/day/2023-06-05/2023-06-12")
    assert plans.history_verdict(option, keeps, _DAY) is None  # Advanced keeps all history
    assert plans.history_verdict(option, basic, _DAY) is not None
    assert plans.history_verdict(_refusal("/v2/aggs/ticker/AAPL/prev"), basic, _DAY) is None
    assert plans.history_verdict(_refusal("/v1/marketstatus/now?d=2020-01-01"), basic, _DAY) is None
    assert plans.history_verdict(old, None, _DAY) is None


def test_the_history_windows_are_the_documented_ones() -> None:
    # The "Plan History" tables of the vendored aggregates documentation of each class (the
    # stocks, options, indices, forex, and futures aggregates pages of Massive's REST docs): a
    # plan they list as "All history" (every Advanced and Business plan, Currencies Starter,
    # every Indices plan) has no entry.
    assert dict(plans.HISTORY_YEARS) == {
        ("stocks", "basic"): 2,
        ("stocks", "starter"): 5,
        ("stocks", "developer"): 10,
        ("options", "basic"): 2,
        ("options", "starter"): 2,
        ("options", "developer"): 4,
        ("currencies", "basic"): 2,
        ("futures", "basic"): 2,
        ("futures", "starter"): 2,
        ("futures", "developer"): 5,
    }


def test_business_plans_keep_all_history() -> None:
    held = plans.declared_plans("stocks=business,options=business")
    assert plans.history_start(held, "stocks", _DAY) is None
    assert plans.history_start(held, "options", _DAY) is None


def test_the_first_day_a_plan_keeps_is_inside_its_window() -> None:
    basic = plans.declared_plans("")
    first = _refusal("/v2/aggs/ticker/AAPL/range/1/day/2024-10-29/2024-11-05")
    assert plans.history_verdict(first, basic, _DAY) is None
    before = _refusal("/v2/aggs/ticker/AAPL/range/1/day/2024-10-28/2024-11-05")
    assert plans.history_verdict(before, basic, _DAY) is not None


def test_a_declaration_may_name_the_flat_file_prefixes_reached() -> None:
    read = plans.declared_plans("stocks=starter, s3=us_stocks_sip/day_aggs_v1, S3=Mixed/Case")
    assert read.flat_files == ("Mixed/Case", "us_stocks_sip/day_aggs_v1")
    assert read.s3
    assert plans.declared_plans("s3").flat_files is None
