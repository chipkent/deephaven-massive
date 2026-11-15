"""The probe that asks Massive which plans a key holds, and its hour-long cache.

A live run finds the key's plans instead of trusting a hand-written list: per asset class, a
snapshot of one liquid ticker (Starter), its trades (Developer) and quotes (Advanced), and the
snapshot's `fmv` (Business), then one file of every flat-file prefix. The probe never guesses:
an answer counts only with rows, and a probe that cannot tell raises `PlanProbeError`, which
ends the run with a request to declare the plans in `MASSIVE_LIVE_PLANS`. The answer is kept
in `.live-plans.json` for an hour (`CACHE_LIFETIME`), naming the credentials only by a digest.
The plans themselves, what each covers, and their history windows are in `plans`.

The probes send requests through the REST and flat-file clients the caller creates, so the
run's source of plans in `tests/conftest.py` (`_plans_of_key`) controls their pacing and their
credentials; the cache's reader and writer touch one file.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Final, TypeAlias

from deephaven_massive._massive import (
    FlatFileClient,
    RestClient,
    Transport,
    WireEntitlementError,
    object_key,
    source,
    sources,
)
from tests._live.classes import bucket_of
from tests._live.plans import TIERS, LivePlans

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

PLANS_CACHE: Final = ".live-plans.json"
"""The file, in the repository root, that keeps the probed plans for `CACHE_LIFETIME`. It names
the key and the S3 credentials only by a digest; deleting it makes the next run probe again."""

CACHE_VERSION: Final = 3
"""The layout of the plans cache; a cache of another version is ignored and probed again."""

CACHE_LIFETIME: Final = timedelta(hours=1)
"""How long the probed plans are reused: short, so a plan bought or cancelled during a day of
runs is seen within the hour; deleting the cache sees it at once."""

SNAPSHOT_RESET: Final = (time(3, 30), time(4, 30))
"""New York times between which the probe does not run: Massive clears its snapshots at 03:30
and refills them from about 04:00 (`vendor/massive/rest/stocks/snapshots/`), with a margin."""

Rows: TypeAlias = "list[dict[str, Any]]"

Request: TypeAlias = "tuple[str, Mapping[str, object], Mapping[str, object]]"
"""A raw REST request: the generated path template, its placeholder values, and its query."""


class PlanProbeError(RuntimeError):
    """A plan probe's answer that proves nothing, so the probe cannot name the plan.

    Raised by the probes for an empty answer where a liquid ticker always has rows, and for a
    probe started during Massive's snapshot reset. The run's source of plans in
    `tests/conftest.py` turns it into a usage error asking to declare the plans in
    `MASSIVE_LIVE_PLANS` instead.
    """


def digest(secret: str | None) -> str | None:
    """Return a short digest of a credential, so the plans cache never holds the credential.

    Args:
        secret: The API key or the S3 access key id, or None when unset.

    Returns:
        The first 16 hex digits of its SHA-256 digest, or None.
    """
    return None if not secret else hashlib.sha256(secret.encode()).hexdigest()[:16]


def cached_plans(path: Path, *, owner: Mapping[str, str | None], now: datetime) -> LivePlans | None:
    """Read the plans cache, if it is fresh, in this layout, and for the same credentials.

    Args:
        path: The cache file.
        owner: The digests of the API key and the S3 access key id (`digest`).
        now: The present, as an aware instant; a cache probed `CACHE_LIFETIME` or longer
            before it is stale, since a plan bought or cancelled changes what the key can do.

    Returns:
        The cached plans, or None when the file is missing, unreadable, of another layout
        version, stale, written for other credentials, or names an unknown plan.
    """
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
        if stored["version"] != CACHE_VERSION or stored["owner"] != dict(owner):
            return None
        probed = str(stored["probed"])
        if not timedelta(0) <= now - datetime.fromisoformat(probed) < CACHE_LIFETIME:
            return None
        tiers = {name: str(stored["tiers"][name]) for name in TIERS}
        flat_files = tuple(str(prefix) for prefix in stored["flat_files"])
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


def store_plans(path: Path, plans: LivePlans, *, owner: Mapping[str, str | None]) -> None:
    """Write probed plans to the cache, for the runs of the next `CACHE_LIFETIME`.

    Args:
        path: The cache file, replaced whole.
        plans: The plans just probed; their `source` is `probed <UTC instant>`.
        owner: The digests of the API key and the S3 access key id (`digest`).
    """
    stored = {
        "version": CACHE_VERSION,
        "owner": dict(owner),
        "probed": plans.source.removeprefix("probed "),
        "tiers": dict(plans.tiers),
        "flat_files": list(plans.flat_files or ()),
    }
    path.write_text(json.dumps(stored, indent=2) + "\n", encoding="utf-8")


def entitled(client: RestClient, request: Request, *, rows_prove: bool = True) -> Rows | None:
    """Send one probe request and tell a refusal apart from an answer that proves access.

    A refusal is an HTTP 403, a 200 whose envelope says `NOT_AUTHORIZED`, or a snapshot whose
    every row carries an `error` (Massive answers a ticker the plan excludes with such a row).
    A snapshot proves access only with at least one row: each probe asks for a liquid ticker,
    which always has some, so an empty snapshot proves nothing. A trades or quotes answer
    proves access even when empty, since the plan refuses those with a 403, and a contract can
    go a while without a trade. Any other failure propagates: a probe that cannot tell must
    not guess a plan.

    Args:
        client: The probe's REST client.
        request: The request.
        rows_prove: Whether only an answer with rows proves access (snapshots); False lets an
            empty 200 count as access (trades and quotes).

    Returns:
        The rows without error rows, or None when the request was refused.

    Raises:
        PlanProbeError: When a snapshot answered without a row, which proves nothing either
            way.
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
    if not rows and rows_prove:
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
        entitled(client, (trades, params, {"limit": 1}), rows_prove=False) is not None,
        entitled(client, (quotes, params, {"limit": 1}), rows_prove=False) is not None,
    )


def _equity_plan(snapshot: Rows, answers: tuple[bool, bool]) -> str:
    """Name a stocks or options plan from its snapshot and its trades and quotes answers.

    A snapshot with `fmv` is a Business plan. The base Business plan answers trades and
    quotes over REST too, at end-of-day recency, so their answers cannot tell it from the
    Business plan with the trades and quotes expansion: the probe reads every Business key as
    the base plan, and only `MASSIVE_LIVE_PLANS` declares the expansion.

    Args:
        snapshot: The answered snapshot's rows.
        answers: Whether trades and whether quotes were answered.

    Returns:
        `"business"` when the snapshot carries `fmv`, else `"advanced"` (quotes),
        `"developer"` (trades), or `"starter"`.
    """
    trades, quotes = answers
    if _has_fmv(snapshot):
        return "business"
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


_CHAIN_PAGE: Final = 250
"""How many contracts of SPY's chain the options probe reads to find a busy one."""


def _contract_of(row: dict[str, Any]) -> str:
    """Return the contract a snapshot row names, or `""` when it names none.

    Args:
        row: A row of SPY's option chain, or of the futures snapshot (`_futures_contract`).

    Returns:
        Its `details.ticker`, or `""`.
    """
    details = row.get("details")
    return str(details.get("ticker") or "") if isinstance(details, dict) else ""


def _day_volume(row: dict[str, Any]) -> float:
    """Return the day's volume of an option chain row, 0 when it has none.

    Args:
        row: A row of the chain snapshot.

    Returns:
        Its `day.volume`.
    """
    day = row.get("day")
    volume = day.get("volume") if isinstance(day, dict) else None
    return float(volume) if isinstance(volume, int | float) else 0.0


def _options_plan(client: RestClient) -> str:
    """Probe the Options plan: SPY's chain (Starter), then its busiest contract's trades and quotes.

    The busiest contract of a page of the chain, by the day's volume, is the one asked about,
    so its trades and quotes are as likely as any to answer with rows; an empty answer counts
    as access all the same (`entitled`).

    Args:
        client: The probe's REST client.

    Returns:
        The plan (`_equity_plan`), or `"basic"` when the chain is refused.

    Raises:
        PlanProbeError: When the chain's row names no contract to ask for trades and quotes.
    """
    rows = entitled(
        client,
        (
            "/v3/snapshot/options/{underlyingAsset}",
            {"underlyingAsset": "SPY"},
            {"limit": _CHAIN_PAGE},
        ),
    )
    if rows is None:
        return "basic"
    named = [row for row in rows if _contract_of(row)]
    if not named:
        msg = f"SPY's chain names no contract: {rows[:3]}"
        raise PlanProbeError(msg)
    contracts = [_contract_of(max(named, key=_day_volume))]
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


_FUTURES_PAGE: Final = 50
"""How many ES contracts of the futures snapshot the probe reads to find the front one."""


def _recency_labels(row: dict[str, Any]) -> set[str]:
    """Return the recency labels a futures snapshot row carries.

    Massive labels a contract's last trade, last quote, and last minute bar with the plan's
    recency (`timeframe`: `REAL-TIME` or `DELAYED`); a contract that has not traded carries
    none.

    Args:
        row: A row of the futures snapshot.

    Returns:
        Its labels, such as `{"DELAYED"}`; empty when it carries none.
    """
    parts = (row.get(key) for key in ("last_trade", "last_quote", "last_minute"))
    return {
        str(part["timeframe"]) for part in parts if isinstance(part, dict) and part.get("timeframe")
    }


def _session_volume(row: dict[str, Any]) -> float:
    """Return the session's volume of a futures snapshot row, 0 when it has none.

    Args:
        row: A row of the futures snapshot.

    Returns:
        Its `session.volume`.
    """
    session = row.get("session")
    volume = session.get("volume") if isinstance(session, dict) else None
    return float(volume) if isinstance(volume, int | float) else 0.0


def _futures_contract(row: dict[str, Any]) -> str:
    """Return the contract a futures snapshot row names, or `""` when it names none.

    The documentation's sample puts `ticker` on the row, while the OpenAPI specification puts
    it under `details`, so both are read.

    Args:
        row: A row of the futures snapshot.

    Returns:
        Its `ticker`, else its `details.ticker`, else `""`.
    """
    return str(row.get("ticker") or "") or _contract_of(row)


def _futures_plan(client: RestClient) -> str:
    """Probe the Futures plan: the ES snapshot (Starter), its front contract's trades and label.

    The front contract is the labelled ES contract (`_recency_labels`) with the most volume
    this session. Its trades answer from Developer up, and its recency label tells a delayed
    plan (Developer) from a real-time one: `REAL-TIME` reads as Advanced. Futures Business,
    sold per venue, leaves no other mark a request can see, so it reads as Advanced too.

    Args:
        client: The probe's REST client.

    Returns:
        `"basic"`, `"starter"`, `"developer"`, or `"advanced"`.

    Raises:
        PlanProbeError: When no row of the snapshot names a contract with a recency label.
    """
    rows = entitled(
        client, ("/futures/v1/snapshot", {}, {"product_code": "ES", "limit": _FUTURES_PAGE})
    )
    if rows is None:
        return "basic"
    labelled = [row for row in rows if _futures_contract(row) and _recency_labels(row)]
    if not labelled:
        msg = f"the ES snapshot names no contract with a recency label: {rows[:3]}"
        raise PlanProbeError(msg)
    front = max(labelled, key=_session_volume)
    trades = ("/futures/v1/trades/{ticker}", {"ticker": _futures_contract(front)}, {"limit": 1})
    if entitled(client, trades, rows_prove=False) is None:
        return "starter"
    return "advanced" if "REAL-TIME" in _recency_labels(front) else "developer"


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
