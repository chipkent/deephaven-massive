"""The key's Massive plans: what each covers, and the history each keeps.

Massive sells one subscription per asset class (stocks, options, indices, currencies,
futures), each on its own plan, and the S3 flat files ride on the paid plans. A live test
declares the plans it needs (`needs("<class>:<plan>")`); the run's plans decide whether it
runs. This module holds:

- the plans per class, which plan covers which (a stocks or options Business plan is a branch
  of its own), and their prices;
- `LivePlans`, the run's plans, and the `MASSIVE_LIVE_PLANS` declaration that overrides the
  probe (`probe`, which finds them by asking Massive);
- the documented history window of each plan, and the verdict on a request refused for its
  timeframe: expected outside the window, a failure inside it.

`tests/conftest.py` holds the fixtures that call these. Every function here is pure.
"""

from __future__ import annotations

import dataclasses
import re
import urllib.parse
from datetime import date
from typing import TYPE_CHECKING, Final

import pytest

from tests._live.classes import class_of_path

if TYPE_CHECKING:
    from collections.abc import Mapping

PLANS_VARIABLE: Final = "MASSIVE_LIVE_PLANS"
"""An override of the probed plans, as `stocks=starter,currencies=starter,s3`: when it is set,
nothing is probed, and every class it leaves out is Basic."""


TIERS: Final = {
    "stocks": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "options": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "indices": ("basic", "starter", "advanced", "business"),
    "currencies": ("basic", "starter", "business"),
    "futures": ("basic", "starter", "developer", "advanced", "business"),
}
"""Each class's plans, cheapest first (https://massive.com/pricing). For stocks and options,
`business` is the base Business plan and `business_expansion` the Business plan with the trades
and quotes expansion, which only a declaration names: the probe cannot tell the two apart."""

_BRANCHES: Final[Mapping[str, Mapping[str, frozenset[str]]]] = {
    name: {
        "business": frozenset({"basic", "starter", "business"}),
        "business_expansion": frozenset(TIERS[name]),
    }
    for name in ("stocks", "options")
}
"""The plans a stocks or options Business plan covers. The base Business plan has no trades,
quotes, LULD, or per-second streams, and so is no Developer or Advanced plan; with the
expansion it covers every plan (the plan tables in `vendor/massive/websocket/`)."""

PRICES: Final = {
    ("stocks", "starter"): "$29",
    ("stocks", "developer"): "$79",
    ("stocks", "advanced"): "$199",
    ("stocks", "business"): "$2,499",
    ("stocks", "business_expansion"): "price on request",
    ("options", "starter"): "$29",
    ("options", "developer"): "$79",
    ("options", "advanced"): "$199",
    ("options", "business"): "$1,999",
    ("options", "business_expansion"): "price on request",
    ("indices", "starter"): "$49",
    ("indices", "advanced"): "$99",
    ("indices", "business"): "$2,500",
    ("currencies", "starter"): "$49",
    ("currencies", "business"): "$999",
    ("futures", "starter"): "$29",
    ("futures", "developer"): "$79",
    ("futures", "advanced"): "$199",
    ("futures", "business"): "$999 a venue",
}
"""The monthly price of each paid plan (https://massive.com/pricing, https://massive.com/business)."""

HISTORY_YEARS: Final[Mapping[tuple[str, str], int]] = {
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
"""How many years of history each plan keeps, from the "Plan History" tables of the vendored
aggregates documentation; a plan not listed, every Advanced and Business plan among them, keeps
all history."""

S3: Final = "s3"
"""The requirement of the S3 flat-file credentials."""

SOME_BASIC: Final = "some:basic"
"""A requirement met when at least one class is on Basic: the rate-limit test needs one."""


@dataclasses.dataclass(frozen=True)
class LivePlans:
    """The plans a live run takes the API key and the S3 credentials to hold.

    Created once per run by the `live_plans` fixture: probed from Massive, read back from the
    day's cache, or declared in `MASSIVE_LIVE_PLANS`. Immutable, so any thread may read it.

    Example:
        ::

            plans = declared_plans("stocks=starter,s3")
            assert unmet(("stocks:basic", "options:starter"), plans) == ["options:starter"]

    Attributes:
        tiers: Each class's plan, one of its `TIERS`, such as `{"stocks": "starter", ...}`.
        s3: Whether the S3 credentials reach at least one flat-file prefix (or, when declared,
            whether `s3` is declared).
        flat_files: The flat-file prefixes whose file of a recent published Monday the
            credentials opened, sorted; None when declared, which lets every prefix be tried.
        source: How the plans were found, for the evidence: probed, cached, or declared.
    """

    tiers: Mapping[str, str]
    s3: bool
    flat_files: tuple[str, ...] | None
    source: str


def declared_plans(text: str) -> LivePlans:
    """Read a `MASSIVE_LIVE_PLANS` declaration: each class's plan, and whether S3 is declared.

    Args:
        text: The variable's value, such as `"stocks=starter,currencies=starter,s3"`; spaces
            and case are ignored.

    Returns:
        The plans: every class Basic unless named.

    Raises:
        pytest.UsageError: If an entry names an unknown class or plan.
    """
    tiers = dict.fromkeys(TIERS, "basic")
    s3 = False
    for entry in text.lower().replace(" ", "").split(","):
        if not entry:
            continue
        if entry == S3:
            s3 = True
            continue
        name, _, tier = entry.partition("=")
        if tier not in TIERS.get(name, ()):
            raise pytest.UsageError(
                f"{PLANS_VARIABLE}: {entry!r} is not <class>=<plan> or s3; classes and plans: "
                + "; ".join(f"{c}: {', '.join(t)}" for c, t in TIERS.items())
            )
        tiers[name] = tier
    return LivePlans(tiers=tiers, s3=s3, flat_files=None, source=f"declared in {PLANS_VARIABLE}")


def plans_text(plans: LivePlans) -> str:
    """Describe the run's plans in one line, for the evidence and the summary.

    Args:
        plans: The plans.

    Returns:
        Such as `"stocks=starter options=basic ...; flat files: us_stocks_sip/... (probed ...)"`.
    """
    tiers = " ".join(f"{name}={tier}" for name, tier in plans.tiers.items())
    if plans.flat_files is None:
        files = "declared" if plans.s3 else "none"
    else:
        files = ", ".join(plans.flat_files) or "none"
    return f"{tiers}; flat files: {files} ({plans.source})"


def meets(name: str, held: str, needed: str) -> bool:
    """Tell whether a class's plan covers another plan of the class.

    Plans are ordered cheapest first, each covering the ones before it, except a stocks or
    options Business plan, which covers only the plans `_BRANCHES` names, and is covered by no
    plan but itself and the Business plan with the expansion.

    Args:
        name: The class.
        held: The plan the key holds.
        needed: The plan a test needs.

    Returns:
        True when `held` covers `needed`.
    """
    branches = _BRANCHES.get(name, {})
    if held in branches:
        return needed in branches[held]
    if needed in branches:
        return False
    tiers = TIERS[name]
    return tiers.index(held) >= tiers.index(needed)


def plan_text(requirement: str) -> str:
    """Name a requirement for a person: its plan and monthly price.

    Args:
        requirement: `"<class>:<plan>"`, `"s3"`, or `"some:basic"`.

    Returns:
        Such as `"Currencies Starter ($49/month)"`.
    """
    if requirement == S3:
        return "S3 flat files (MASSIVE_S3_* credentials, on the class's Starter plan)"
    if requirement == SOME_BASIC:
        return "at least one class left on Basic"
    name, _, tier = requirement.partition(":")
    price = "free" if tier == "basic" else PRICES[name, tier]
    shown = tier.replace("_expansion", " with the trades and quotes expansion")
    month = "" if price in {"free", "price on request"} else "/month"
    return f"{name.capitalize()} {shown.capitalize()} ({price}{month})"


def unmet(requirements: tuple[str, ...], plans: LivePlans) -> list[str]:
    """Return the requirements the run's plans do not meet.

    Args:
        requirements: `"<class>:<plan>"`, `"s3"`, or `"some:basic"` each.
        plans: The run's plans.

    Returns:
        The unmet ones, in order.
    """
    found = []
    for requirement in requirements:
        if requirement == S3:
            met = plans.s3
        elif requirement == SOME_BASIC:
            met = "basic" in plans.tiers.values()
        else:
            name, _, tier = requirement.partition(":")
            met = meets(name, plans.tiers[name], tier)
        if not met:
            found.append(requirement)
    return found


def reaches(plans: LivePlans, prefix: str) -> bool:
    """Tell whether the S3 credentials reach a flat-file prefix, by the plans probe.

    Args:
        plans: The run's plans.
        prefix: The generated prefix.

    Returns:
        True when the probe opened the prefix's file, or when the plans were declared, which
        names no prefix and so lets each be tried.
    """
    return plans.flat_files is None or prefix in plans.flat_files


def history_start(plans: LivePlans, name: str, day: date) -> date | None:
    """Return the first day a class's plan keeps, by the documented history windows.

    Args:
        plans: The run's plans.
        name: The class.
        day: The run's day.

    Returns:
        The day `HISTORY_YEARS` years before `day` (February 29 read as the 28th), or None for
        a plan that keeps all history.
    """
    years = HISTORY_YEARS.get((name, plans.tiers[name]))
    if years is None:
        return None
    if (day.month, day.day) == (2, 29):
        return date(day.year - years, 2, 28)
    return day.replace(year=day.year - years)


TIMEFRAME_PHRASE: Final = "doesn't include this data timeframe"
"""The phrase of Massive's refusal of a request beyond the plan's history, in lower case."""

_REQUEST_TARGET: Final = re.compile(r" from GET (\S+) on ")
"""Where a wire error's message names the request's target (path and query)."""

_ISO_DATE: Final = re.compile(r"\d{4}-\d{2}-\d{2}")
"""An ISO date in a request's target."""


def is_timeframe_refusal(error: BaseException) -> bool:
    """Return whether a refusal says the request reaches beyond the plan's history.

    Massive words it "Your plan doesn't include this data timeframe". The phrase is matched in
    the message, never a single word: a request's target is percent-encoded and holds no
    spaces, so the phrase cannot come from a query string. A typographic apostrophe matches
    too.

    Args:
        error: The refusal.

    Returns:
        True for a refusal of the plan's history window.
    """
    return TIMEFRAME_PHRASE in str(error).lower().replace("\u2019", "'")


def refused_request(error: BaseException) -> tuple[str | None, date | None]:
    """Read the class and the earliest day a refused REST request asked for.

    The wire error's message reads `"<what> from GET <target> on <host>...: <detail>"`; the
    class comes from the target's path (`classes.class_of_path`) and the day from the ISO dates
    in the target.

    Args:
        error: The refusal.

    Returns:
        The class (None for data of no class, or a message that names no target) and the
        earliest ISO date the target names (None when it names none).
    """
    found = _REQUEST_TARGET.search(str(error))
    if found is None:
        return None, None
    target = urllib.parse.unquote(found.group(1))
    days = [date.fromisoformat(text) for text in _ISO_DATE.findall(target)]
    return class_of_path(target.partition("?")[0]), min(days, default=None)


def history_verdict(error: BaseException, plans: LivePlans | None, day: date) -> str | None:
    """Decide whether a timeframe refusal is outside the plan's documented history.

    A refusal of a day before the first day the plan keeps (`history_start`) is expected, and
    the test skips. A refusal of a day the plan keeps, of a request that names no day, of data
    of no class, or of a plan that keeps all history contradicts the documented window: it may
    be a library bug, so the test fails.

    Args:
        error: The timeframe refusal.
        plans: The run's plans, or None when no live test has resolved them.
        day: The run's day.

    Returns:
        The skip reason when the refused day is outside the window, else None.
    """
    name, asked = refused_request(error)
    if plans is None or name is None or asked is None:
        return None
    first = history_start(plans, name, day)
    if first is None or asked >= first:
        return None
    return f"outside the plan's history: {asked} is before {first}, the first day {name} keeps"
