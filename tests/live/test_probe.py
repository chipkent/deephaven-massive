"""Tests of the plan probe and its cache (`tests/_live/probe.py`).

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
from tests._live import plans, probe

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
    # The base Business plan answers trades and quotes over REST too, at end-of-day recency.
    "business": (True, True, True, True),
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
    ("stocks", "business_expansion"): "business",
    ("options", "business_expansion"): "business",
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
    assert probe.probe_tiers(client, now=_NOON) == expected


def test_a_snapshot_of_only_error_rows_is_a_refusal(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "starter"))
    errors = Reply(envelope(tickers=[{"ticker": "AAPL", "error": "NOT_ENTITLED"}]))
    fake_rest.reply_first("/v2/snapshot/locale/us/markets/stocks/tickers", errors)
    assert probe.probe_tiers(client, now=_NOON)["stocks"] == "basic"


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
    with pytest.raises(probe.PlanProbeError, match="without a row"):
        probe.probe_tiers(client, now=_NOON)


@pytest.mark.parametrize(
    ("path", "row"),
    [("/v3/snapshot/options/SPY", {"day": {}}), ("/futures/v1/snapshot", {"details": {}})],
)
def test_a_snapshot_without_a_contract_stops_the_probe(
    path: str, row: dict[str, object], *, fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "starter"))
    fake_rest.reply_first(path, Reply(envelope([row])))
    with pytest.raises(probe.PlanProbeError, match="no contract"):
        probe.probe_tiers(client, now=_NOON)


@pytest.mark.parametrize("now", [time(3, 30), time(4, 29, 59)])
def test_no_probe_during_the_snapshot_reset(
    now: time, fake_rest: FakeRestServer, client: RestClient
) -> None:
    with pytest.raises(probe.PlanProbeError, match="refills its snapshots"):
        probe.probe_tiers(client, now=now)
    assert fake_rest.requests == []


def test_the_reset_ends_at_its_bound(fake_rest: FakeRestServer, client: RestClient) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "basic"))
    assert set(probe.probe_tiers(client, now=time(4, 30)).values()) == {"basic"}


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
        reached = probe.reached_prefixes(client, day)
    finally:
        client.close()
    assert reached == ("us_futures_{venue}/minute_aggs_v1", "us_indices/day_aggs_v1")


def test_a_digest_hides_the_credential() -> None:
    assert probe.digest(None) is None
    assert probe.digest("") is None
    found = probe.digest("key")
    assert found is not None
    assert len(found) == 16
    assert "key" not in found


_DAY = date(2026, 10, 29)


_OWNER = {"api_key": probe.digest("key"), "s3": None}


def _probed() -> plans.LivePlans:
    tiers = dict.fromkeys(plans.TIERS, "basic") | {"stocks": "business"}
    return plans.LivePlans(
        tiers=tiers, s3=True, flat_files=("a/b",), source="probed 2026-10-29T12:00:00Z"
    )


def test_the_cache_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "plans.json"
    probe.store_plans(path, _probed(), owner=_OWNER, day=_DAY)
    assert "key" not in json.loads(path.read_text())["owner"]["api_key"]
    back = probe.cached_plans(path, owner=_OWNER, day=_DAY)
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
    probe.store_plans(path, _probed(), owner=_OWNER, day=_DAY)
    path.write_text(json.dumps(json.loads(path.read_text()) | change))
    assert probe.cached_plans(path, owner=_OWNER, day=_DAY) is None


def test_a_missing_or_broken_cache_is_ignored(tmp_path: Path) -> None:
    path = tmp_path / "plans.json"
    assert probe.cached_plans(path, owner=_OWNER, day=_DAY) is None
    path.write_text("{")
    assert probe.cached_plans(path, owner=_OWNER, day=_DAY) is None


@pytest.mark.parametrize("name", ["stocks", "options"])
def test_a_snapshot_with_fmv_reads_as_base_business_whatever_trades_answer(
    name: str, fake_rest: FakeRestServer, client: RestClient
) -> None:
    _script(fake_rest, dict.fromkeys(plans.TIERS, "basic") | {name: "business_expansion"})
    assert probe.probe_tiers(client, now=_NOON)[name] == "business"
