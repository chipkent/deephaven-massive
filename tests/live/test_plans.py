"""Tests of the key's plans: coverage, probe, cache, and history verdict (`tests/_live/plans.py`).

The probe runs against `FakeRestServer` and `FakeS3`, scripted as each plan answers.
"""

from __future__ import annotations

import json
from datetime import date, time
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive import FlatFileClient, RequestGovernor, RestClient, object_key

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the servers come from the `fake_rest`
# and `fake_s3` fixtures.
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope
from tests._live import plans

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3

_NOON = time(12)
_REFUSED = Reply(envelope(status="NOT_AUTHORIZED", message="not entitled"), status=403)
_OK = Reply(envelope([{"x": 1}]))
_HAS = {
    # plan -> (snapshot, trades, quotes, fmv)
    "basic": (False, False, False, False),
    "starter": (True, False, False, False),
    "developer": (True, True, False, False),
    "advanced": (True, True, True, False),
    "business": (True, False, False, True),
    "business_expansion": (True, True, True, True),
}


def _answers(name: str, held: str) -> tuple[bool, bool, bool, bool]:
    if name == "futures" and held in {"advanced", "business"}:
        return (True, True, True, False)
    if name in {"indices", "currencies"} and held in {"advanced", "business"}:
        return (True, False, False, held == "business")
    return _HAS[held]


def _script(server: FakeRestServer, held: dict[str, str]) -> None:
    """Answer each class's probe requests as the plan `held[class]` would."""

    def snapshot(name: str, key: str, row: dict[str, object]) -> Reply:
        found, _, _, fmv = _answers(name, held[name])
        body = {"status": "OK", "request_id": "r", key: [row | {"fmv": 1.0 if fmv else None}]}
        return Reply(body) if found else _REFUSED

    def answer(name: str, index: int) -> Reply:
        return _OK if _answers(name, held[name])[index] else _REFUSED

    option = "O%3ASPY261218C00600000"
    server.reply(
        "/v2/snapshot/locale/us/markets/stocks/tickers",
        snapshot("stocks", "tickers", {"ticker": "AAPL"}),
    )
    server.reply("/v3/trades/AAPL", answer("stocks", 1))
    server.reply("/v3/quotes/AAPL", answer("stocks", 2))
    server.reply(
        "/v3/snapshot/options/SPY",
        snapshot("options", "results", {"details": {"ticker": "O:SPY261218C00600000"}}),
    )
    server.reply(f"/v3/trades/{option}", answer("options", 1))
    server.reply(f"/v3/quotes/{option}", answer("options", 2))
    timeframe = "REAL-TIME" if held["indices"] in {"advanced", "business"} else "DELAYED"
    indices = Reply(envelope([{"ticker": "I:NDX", "timeframe": timeframe}]))
    server.reply("/v3/snapshot/indices", _REFUSED if held["indices"] == "basic" else indices)
    server.reply(
        "/v2/snapshot/locale/global/markets/forex/tickers",
        snapshot("currencies", "tickers", {"ticker": "C:EURUSD"}),
    )
    futures = Reply(envelope([{"details": {"ticker": "ESZ6"}}]))
    server.reply("/futures/v1/snapshot", _REFUSED if held["futures"] == "basic" else futures)
    server.reply("/futures/v1/trades/ESZ6", answer("futures", 1))


@pytest.fixture
def client(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor(plans.TIERS, clock=FakeClock())
    made = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield made
    made.close()


_READ_AS = {
    ("futures", "advanced"): "developer",
    ("futures", "business"): "developer",
    ("indices", "business"): "advanced",
}
"""The plans a probe cannot tell apart, and what it reads them as."""


@pytest.mark.parametrize(
    "held",
    [
        dict.fromkeys(plans.TIERS, "basic"),
        {
            "stocks": "starter",
            "options": "developer",
            "indices": "starter",
            "currencies": "starter",
            "futures": "advanced",
        },
        {
            "stocks": "advanced",
            "options": "business",
            "indices": "advanced",
            "currencies": "business",
            "futures": "starter",
        },
        {
            "stocks": "business_expansion",
            "options": "advanced",
            "indices": "business",
            "currencies": "basic",
            "futures": "developer",
        },
        {
            "stocks": "business",
            "options": "business_expansion",
            "indices": "basic",
            "currencies": "basic",
            "futures": "business",
        },
        {
            "stocks": "developer",
            "options": "starter",
            "indices": "basic",
            "currencies": "basic",
            "futures": "basic",
        },
    ],
)
def test_the_probe_finds_each_plan(
    held: dict[str, str], fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, held)
    expected = {name: _READ_AS.get((name, tier), tier) for name, tier in held.items()}
    assert plans.probe_tiers(client, now=_NOON) == expected


def test_a_snapshot_of_only_error_rows_is_a_refusal(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "starter"))
    errors = Reply(envelope(tickers=[{"ticker": "AAPL", "error": "NOT_ENTITLED"}]))
    fake_rest.reply_first("/v2/snapshot/locale/us/markets/stocks/tickers", errors)
    assert plans.probe_tiers(client, now=_NOON)["stocks"] == "basic"


@pytest.mark.parametrize(
    "empty",
    ["/v2/snapshot/locale/us/markets/stocks/tickers", "/v3/trades/AAPL", "/v3/snapshot/indices"],
)
def test_an_answer_without_a_row_stops_the_probe(
    empty: str, fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "starter") | {"stocks": "advanced"})
    body = envelope(tickers=[]) if "stocks/tickers" in empty else envelope([])
    fake_rest.reply_first(empty, Reply(body))
    with pytest.raises(plans.PlanProbeError, match="without a row"):
        plans.probe_tiers(client, now=_NOON)


@pytest.mark.parametrize(
    ("path", "row"),
    [("/v3/snapshot/options/SPY", {"day": {}}), ("/futures/v1/snapshot", {"details": {}})],
)
def test_a_snapshot_without_a_contract_stops_the_probe(
    path: str, row: dict[str, object], *, fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "starter"))
    fake_rest.reply_first(path, Reply(envelope([row])))
    with pytest.raises(plans.PlanProbeError, match="no contract"):
        plans.probe_tiers(client, now=_NOON)


@pytest.mark.parametrize("now", [time(3, 30), time(4, 29, 59)])
def test_no_probe_during_the_snapshot_reset(
    now: time, fake_rest: FakeRestServer, client: RestClient
) -> None:
    with pytest.raises(plans.PlanProbeError, match="refills its snapshots"):
        plans.probe_tiers(client, now=now)
    assert fake_rest.requests == []


def test_the_reset_ends_at_its_bound(fake_rest: FakeRestServer, client: RestClient) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "basic"))
    assert set(plans.probe_tiers(client, now=time(4, 30)).values()) == {"basic"}


def test_only_a_file_that_opens_reaches_its_prefix(fake_s3: FakeS3) -> None:
    day = date(2026, 10, 26)
    fake_s3.put(object_key("us_stocks_sip/day_aggs_v1", day), b"x")
    fake_s3.put(object_key("us_futures_{venue}/minute_aggs_v1", day, "cme"), b"x")
    fake_s3.refuse("us_stocks_sip/", status=403, code="AccessDenied", message="no")
    fake_s3.put(object_key("us_indices/day_aggs_v1", day), b"x")
    client = FlatFileClient(
        access_key_id="id",
        secret_access_key="secret",  # noqa: S106 - a fake credential the fake S3 accepts
        endpoint_url="https://files.massive.com",
        bucket="flatfiles",
        max_pool=1,
    )
    try:
        reached = plans.reached_prefixes(client, day)
    finally:
        client.close()
    assert reached == ("us_futures_{venue}/minute_aggs_v1", "us_indices/day_aggs_v1")


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


def test_a_digest_hides_the_credential() -> None:
    assert plans.digest(None) is None
    assert plans.digest("") is None
    found = plans.digest("key")
    assert found is not None
    assert len(found) == 16
    assert "key" not in found


_DAY = date(2026, 10, 29)
_OWNER = {"api_key": plans.digest("key"), "s3": None}


def _probed() -> plans.LivePlans:
    tiers = dict.fromkeys(plans.TIERS, "basic") | {"stocks": "business"}
    return plans.LivePlans(
        tiers=tiers, s3=True, flat_files=("a/b",), source="probed 2026-10-29T12:00:00Z"
    )


def test_the_cache_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "plans.json"
    plans.store_plans(path, _probed(), owner=_OWNER, day=_DAY)
    assert "key" not in json.loads(path.read_text())["owner"]["api_key"]
    back = plans.cached_plans(path, owner=_OWNER, day=_DAY)
    assert back == plans.LivePlans(
        tiers=_probed().tiers,
        s3=True,
        flat_files=("a/b",),
        source="cached in .live-plans.json, probed 2026-10-29T12:00:00Z",
    )


@pytest.mark.parametrize(
    "change",
    [
        {"version": 1},
        {"day": "2026-10-28"},
        {"owner": {"api_key": "other", "s3": None}},
        {"tiers": dict.fromkeys(plans.TIERS, "gold")},
        {"tiers": {}},
        {"flat_files": None},
    ],
)
def test_a_cache_that_does_not_fit_is_ignored(change: dict[str, object], tmp_path: Path) -> None:
    path = tmp_path / "plans.json"
    plans.store_plans(path, _probed(), owner=_OWNER, day=_DAY)
    path.write_text(json.dumps(json.loads(path.read_text()) | change))
    assert plans.cached_plans(path, owner=_OWNER, day=_DAY) is None


def test_a_missing_or_broken_cache_is_ignored(tmp_path: Path) -> None:
    path = tmp_path / "plans.json"
    assert plans.cached_plans(path, owner=_OWNER, day=_DAY) is None
    path.write_text("{")
    assert plans.cached_plans(path, owner=_OWNER, day=_DAY) is None


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
