"""The key's Massive plans: what each covers, how to find them, and the history each keeps.

Massive sells one subscription per asset class (stocks, options, indices, currencies,
futures), each on its own plan, and the S3 flat files ride on the paid plans. A live test
declares the plans it needs (`needs("<class>:<plan>")`); the run's plans decide whether it
runs. This module holds:

- the plans per class, which plan covers which (a stocks or options Business plan is a branch
  of its own), their prices, and their documented history windows;
- the probe that asks Massive which plans a key holds, through a REST client and a flat-file
  client the caller creates, and which never guesses: an answer counts only with rows;
- the day-long cache of the probe's answer, which names the credentials only by a digest, and
  the `MASSIVE_LIVE_PLANS` declaration that overrides both;
- the verdict on a request refused for its timeframe: expected outside the plan's documented
  history, a failure inside it.

`tests/conftest.py` holds the fixtures that call these. Every function is pure but the probes,
which send requests, and the cache's reader and writer, which touch one file.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import urllib.parse
from datetime import date, time
from typing import TYPE_CHECKING, Any, Final, TypeAlias

import pytest

from deephaven_massive._massive import (
    FlatFileClient,
    RestClient,
    Transport,
    WireEntitlementError,
    object_key,
    source,
    sources,
)
from tests._live.classes import bucket_of, class_of_path

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

PLANS_VARIABLE: Final = "MASSIVE_LIVE_PLANS"
"""An override of the probed plans, as `stocks=starter,currencies=starter,s3`: when it is set,
nothing is probed, and every class it leaves out is Basic."""

PLANS_CACHE: Final = ".live-plans.json"
"""The file, in the repository root, that keeps the probed plans until the New York day ends. It
names the key and the S3 credentials only by a digest; deleting it makes the next run probe
again, as an upgrade during the day needs."""

CACHE_VERSION: Final = 2
"""The layout of the plans cache; a cache of another version is ignored and probed again."""

TIERS: Final = {
    "stocks": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "options": ("basic", "starter", "developer", "advanced", "business", "business_expansion"),
    "indices": ("basic", "starter", "advanced", "business"),
    "currencies": ("basic", "starter", "business"),
    "futures": ("basic", "starter", "developer", "advanced", "business"),
}
"""Each class's plans, cheapest first (https://massive.com/pricing). For stocks and options,
`business` is the base Business plan and `business_expansion` the Business plan with the trades
and quotes expansion."""

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
    ("stocks", "business"): 5,
    ("options", "basic"): 2,
    ("options", "starter"): 2,
    ("options", "developer"): 4,
    ("options", "business"): 2,
    ("currencies", "basic"): 2,
    ("futures", "basic"): 2,
    ("futures", "starter"): 2,
    ("futures", "developer"): 5,
}
"""How many years of history each plan keeps, from the "Plan History" tables of the vendored
aggregates documentation; a plan not listed keeps all history. A base Business plan is taken to
keep what the cheapest plan it covers keeps, the conservative reading of an undocumented
window."""

S3: Final = "s3"
"""The requirement of the S3 flat-file credentials."""

SOME_BASIC: Final = "some:basic"
"""A requirement met when at least one class is on Basic: the rate-limit test needs one."""

SNAPSHOT_RESET: Final = (time(3, 30), time(4, 30))
"""New York times between which the probe does not run: Massive clears its snapshots at 03:30
and refills them from about 04:00 (`vendor/massive/rest/stocks/snapshots/`), with a margin."""

Rows: TypeAlias = "list[dict[str, Any]]"

Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"
"""A raw REST request: the generated path template, its placeholder values, and its query."""


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


class PlanProbeError(RuntimeError):
    """A plan probe's answer that proves nothing, so the probe cannot name the plan.

    Raised by the probes for an empty answer where a liquid ticker always has rows, and for a
    probe started during Massive's snapshot reset. The `live_plans` fixture turns it into a
    usage error asking to declare the plans in `MASSIVE_LIVE_PLANS` instead.
    """


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


def digest(secret: str | None) -> str | None:
    """Return a short digest of a credential, so the plans cache never holds the credential.

    Args:
        secret: The API key or the S3 access key id, or None when unset.

    Returns:
        The first 16 hex digits of its SHA-256 digest, or None.
    """
    return None if not secret else hashlib.sha256(secret.encode()).hexdigest()[:16]


def cached_plans(path: Path, *, owner: Mapping[str, str | None], day: date) -> LivePlans | None:
    """Read the plans cache, if it was written that day, in this layout, for the same credentials.

    Args:
        path: The cache file.
        owner: The digests of the API key and the S3 access key id (`digest`).
        day: The run's New York day; a cache of another day is stale, since a plan bought or
            cancelled during the month changes what the key can do.

    Returns:
        The cached plans, or None when the file is missing, unreadable, of another layout
        version, stale, written for other credentials, or names an unknown plan.
    """
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
        if stored["version"] != CACHE_VERSION:
            return None
        if stored["owner"] != dict(owner) or stored["day"] != day.isoformat():
            return None
        tiers = {name: str(stored["tiers"][name]) for name in TIERS}
        flat_files = tuple(str(prefix) for prefix in stored["flat_files"])
        probed = str(stored["probed"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if any(tiers[name] not in TIERS[name] for name in TIERS):
        return None
    return LivePlans(
        tiers=tiers,
        s3=bool(flat_files),
        flat_files=flat_files,
        source=f"cached in {PLANS_CACHE}, probed {probed}",
    )


def store_plans(
    path: Path, plans: LivePlans, *, owner: Mapping[str, str | None], day: date
) -> None:
    """Write probed plans to the cache, for the later runs of the same New York day.

    Args:
        path: The cache file, replaced whole.
        plans: The plans just probed.
        owner: The digests of the API key and the S3 access key id (`digest`).
        day: The run's New York day.
    """
    stored = {
        "version": CACHE_VERSION,
        "owner": dict(owner),
        "day": day.isoformat(),
        "probed": plans.source.removeprefix("probed "),
        "tiers": dict(plans.tiers),
        "flat_files": list(plans.flat_files or ()),
    }
    path.write_text(json.dumps(stored, indent=2) + "\n", encoding="utf-8")


def entitled(client: RestClient, request: Request) -> Rows | None:
    """Send one probe request and tell a refusal apart from an answer that proves access.

    A refusal is an HTTP 403, a 200 whose envelope says `NOT_AUTHORIZED`, or a snapshot whose
    every row carries an `error` (Massive answers a ticker the plan excludes with such a row).
    An answer proves access only with at least one row: each probe asks for a liquid ticker,
    which always has some. Any other failure propagates: a probe that cannot tell must not
    guess a plan.

    Args:
        client: The probe's REST client.
        request: The request.

    Returns:
        The rows without error rows, or None when the request was refused.

    Raises:
        PlanProbeError: When Massive answered without a row, which proves nothing either way.
    """
    path, path_params, query = request
    try:
        page = client.get(
            source(Transport.REST, path), path_params, query, bucket=bucket_of(path, path_params)
        )
    except WireEntitlementError:
        return None
    rows = [row for row in page.rows if row.get("error") is None]
    if page.rows and not rows:
        return None
    if not rows:
        msg = f"{path} {dict(path_params)} {dict(query)} answered without a row"
        raise PlanProbeError(msg)
    return rows


def _has_fmv(rows: Rows) -> bool:
    """Tell whether a snapshot carries the fair market value only Business plans see.

    Args:
        rows: The snapshot's rows.

    Returns:
        True when a row has a non-null `fmv`.
    """
    return any(row.get("fmv") is not None for row in rows)


def _trades_and_quotes(
    client: RestClient, paths: tuple[str, str], params: Mapping[str, object]
) -> tuple[bool, bool]:
    """Ask whether the plan includes a ticker's trades and its quotes.

    Args:
        client: The probe's REST client.
        paths: The class's trades and quotes path templates.
        params: The placeholder values of both, naming one liquid ticker.

    Returns:
        Whether the trades and whether the quotes were answered.
    """
    trades, quotes = paths
    return (
        entitled(client, (trades, params, {"limit": 1})) is not None,
        entitled(client, (quotes, params, {"limit": 1})) is not None,
    )


def _equity_plan(snapshot: Rows, answers: tuple[bool, bool]) -> str:
    """Name a stocks or options plan from its snapshot and its trades and quotes answers.

    Args:
        snapshot: The answered snapshot's rows.
        answers: Whether trades and whether quotes were answered.

    Returns:
        `"business_expansion"` or `"business"` when the snapshot carries `fmv` (with or
        without trades or quotes), else `"advanced"` (quotes), `"developer"` (trades), or
        `"starter"`.
    """
    trades, quotes = answers
    if _has_fmv(snapshot):
        return "business_expansion" if trades or quotes else "business"
    return "advanced" if quotes else "developer" if trades else "starter"


def _stocks_plan(client: RestClient) -> str:
    """Probe the Stocks plan: AAPL's snapshot (Starter), its trades, and its quotes.

    Args:
        client: The probe's REST client.

    Returns:
        The plan (`_equity_plan`), or `"basic"` when the snapshot is refused.
    """
    rows = entitled(
        client, ("/v2/snapshot/locale/us/markets/stocks/tickers", {}, {"tickers": ["AAPL"]})
    )
    if rows is None:
        return "basic"
    paths = ("/v3/trades/{stockTicker}", "/v3/quotes/{stockTicker}")
    return _equity_plan(rows, _trades_and_quotes(client, paths, {"stockTicker": "AAPL"}))


def _options_plan(client: RestClient) -> str:
    """Probe the Options plan: one contract of SPY's chain (Starter), its trades, and quotes.

    Args:
        client: The probe's REST client.

    Returns:
        The plan (`_equity_plan`), or `"basic"` when the chain is refused.

    Raises:
        PlanProbeError: When the chain's row names no contract to ask for trades and quotes.
    """
    rows = entitled(
        client,
        ("/v3/snapshot/options/{underlyingAsset}", {"underlyingAsset": "SPY"}, {"limit": 1}),
    )
    if rows is None:
        return "basic"
    details = [row.get("details") for row in rows]
    contracts = [str(d["ticker"]) for d in details if isinstance(d, dict) and d.get("ticker")]
    if not contracts:
        msg = f"SPY's chain names no contract: {rows}"
        raise PlanProbeError(msg)
    paths = ("/v3/trades/{optionsTicker}", "/v3/quotes/{optionsTicker}")
    return _equity_plan(rows, _trades_and_quotes(client, paths, {"optionsTicker": contracts[0]}))


def _indices_plan(client: RestClient) -> str:
    """Probe the Indices plan: the snapshot of `I:NDX` (Starter), real-time (Advanced).

    Indices Business has no mark a request can see, so it reads as Advanced.

    Args:
        client: The probe's REST client.

    Returns:
        The plan.
    """
    rows = entitled(client, ("/v3/snapshot/indices", {}, {"ticker.any_of": ["I:NDX"]}))
    if rows is None:
        return "basic"
    return "advanced" if any(row.get("timeframe") == "REAL-TIME" for row in rows) else "starter"


def _currencies_plan(client: RestClient) -> str:
    """Probe the Currencies plan: the snapshot of `C:EURUSD` (Starter), with `fmv` (Business).

    Args:
        client: The probe's REST client.

    Returns:
        The plan.
    """
    rows = entitled(
        client,
        ("/v2/snapshot/locale/global/markets/forex/tickers", {}, {"tickers": ["C:EURUSD"]}),
    )
    if rows is None:
        return "basic"
    return "business" if _has_fmv(rows) else "starter"


def _futures_plan(client: RestClient) -> str:
    """Probe the Futures plan: one ES contract's snapshot (Starter) and its trades (Developer).

    Developer and Advanced both include trades and quotes, and Business per venue leaves no
    mark a request can see, so every plan above Starter reads as Developer: the futures tests
    need nothing above it.

    Args:
        client: The probe's REST client.

    Returns:
        `"basic"`, `"starter"`, or `"developer"`.

    Raises:
        PlanProbeError: When the snapshot's row names no contract to ask for trades.
    """
    rows = entitled(client, ("/futures/v1/snapshot", {}, {"product_code": "ES", "limit": 1}))
    if rows is None:
        return "basic"
    tickers = [row.get("ticker") or (row.get("details") or {}).get("ticker") for row in rows]
    contracts = [str(ticker) for ticker in tickers if ticker]
    if not contracts:
        msg = f"the ES snapshot names no contract: {rows}"
        raise PlanProbeError(msg)
    trades = ("/futures/v1/trades/{ticker}", {"ticker": contracts[0]}, {"limit": 1})
    return "developer" if entitled(client, trades) is not None else "starter"


PLAN_PROBES: Final[Mapping[str, Callable[[RestClient], str]]] = {
    "stocks": _stocks_plan,
    "options": _options_plan,
    "indices": _indices_plan,
    "currencies": _currencies_plan,
    "futures": _futures_plan,
}
"""Each class's plan probe: one snapshot of a liquid ticker, then its trades and quotes."""


def probe_tiers(client: RestClient, *, now: time) -> dict[str, str]:
    """Ask Massive which plan the key holds in each class, a few requests per class.

    Args:
        client: A REST client on the key, whose governor paces the requests.
        now: The New York time of day the probe runs at.

    Returns:
        Each class's plan.

    Raises:
        PlanProbeError: During Massive's snapshot reset (`SNAPSHOT_RESET`), when a snapshot can
            lack the `fmv` that marks a Business plan, and when an answer proves nothing.
    """
    if SNAPSHOT_RESET[0] <= now < SNAPSHOT_RESET[1]:
        msg = (
            f"Massive clears and refills its snapshots from {SNAPSHOT_RESET[0]:%H:%M} to "
            f"{SNAPSHOT_RESET[1]:%H:%M} New York time, when a Business plan cannot be seen"
        )
        raise PlanProbeError(msg)
    return {name: probe(client) for name, probe in PLAN_PROBES.items()}


def reached_prefixes(client: FlatFileClient, day: date) -> tuple[str, ...]:
    """Open one file of every flat-file prefix, to find the prefixes the credentials reach.

    A prefix is reached only when its file of `day` opens: a 403 is a refusal, and a missing
    file proves nothing, so neither counts. Each file is closed as soon as it opens, so only
    the start of its body is read.

    Args:
        client: A flat-file client on the run's S3 credentials.
        day: A recent published Monday, whose files exist; a futures prefix reads `cme`.

    Returns:
        The prefixes reached, sorted.
    """
    reached = []
    for prefix in sources(Transport.FLAT_FILES):
        venue = "cme" if "{venue}" in prefix else None
        try:
            file = client.open(object_key(prefix, day, venue))
        except WireEntitlementError:
            continue
        if file is not None:
            file.close()
            reached.append(prefix)
    return tuple(sorted(reached))


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
