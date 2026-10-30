"""Tests of the live run's probe (`tests/_live/live_run/run_probe.py`).

The probe runs against `FakeRestServer` and `FakeS3` with a planted fake key, scripted as a
free key answers: every snapshot refused. It must record each request's path and status but
never a body or the key, adopt the tiers, and come back unknown or rejected when Massive's
answers say so.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import TYPE_CHECKING

from deephaven_massive._massive import Transport, object_key, sources
from tests._fakes.rest import Reply, envelope
from tests._live import plans
from tests._live.calendar import NEW_YORK, recent_monday
from tests._live.live_run import run_probe
from tests._live.plans import TIERS

if TYPE_CHECKING:
    from tests._fakes.rest import FakeRestServer
    from tests._fakes.s3 import FakeS3

PLANTED = "PLANTED-probe-key-6a1b"

NOON = datetime(2026, 11, 3, 12, tzinfo=NEW_YORK)

_REFUSED = Reply(envelope(status="NOT_AUTHORIZED", message="not entitled"), status=403)

_SNAPSHOTS = (
    "/v2/snapshot/locale/us/markets/stocks/tickers",
    "/v3/snapshot/options/SPY",
    "/v3/snapshot/indices",
    "/v2/snapshot/locale/global/markets/forex/tickers",
    "/futures/v1/snapshot",
)


def _free_key(
    server: FakeRestServer, *, server_time: str | None = "2026-11-03T12:00:00-05:00"
) -> None:
    body: dict[str, object] = {"market": "open", "echo": PLANTED}
    if server_time is not None:
        body["serverTime"] = server_time
    server.reply("/v1/marketstatus/now", Reply(body))
    for path in _SNAPSHOTS:
        server.reply(path, _REFUSED)


def _credentials(
    server: FakeRestServer, s3_pair: tuple[str, str] | None = None
) -> run_probe.Credentials:
    return run_probe.Credentials(api_key=PLANTED, s3_pair=s3_pair, rest_base_url=server.url)


def test_a_free_key_reads_basic_everywhere_and_records_each_request(
    fake_rest: FakeRestServer,
) -> None:
    _free_key(fake_rest)
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert result.tiers == dict.fromkeys(TIERS, "basic")
    assert (result.key_rejected, result.error, result.flat_files) == (False, "", [])
    assert result.when == "2026-11-03T17:00:00Z"
    assert result.history_start["stocks"] == "2024-11-03"
    assert result.clock_offset is not None
    assert [(r["path"], r["status"]) for r in result.requests] == [
        ("/v1/marketstatus/now", 200),
        *((path, 403) for path in _SNAPSHOTS),
    ]
    assert PLANTED not in json.dumps(result.requests)


def test_a_probe_without_server_time_has_no_clock_offset(fake_rest: FakeRestServer) -> None:
    _free_key(fake_rest, server_time=None)
    assert run_probe.probe_key(_credentials(fake_rest), now=NOON).clock_offset is None


def test_a_rejected_key_stops_the_probe(fake_rest: FakeRestServer) -> None:
    fake_rest.reply(
        "/v1/marketstatus/now",
        Reply(envelope(status="ERROR", message="Unknown API Key"), status=401),
    )
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert (result.key_rejected, result.tiers, result.history_start) == (True, None, {})
    assert result.error == "the API key was rejected"


def test_a_probe_during_the_snapshot_reset_is_unknown(fake_rest: FakeRestServer) -> None:
    _free_key(fake_rest)
    reset = datetime(2026, 11, 3, 3, 45, tzinfo=NEW_YORK)
    result = run_probe.probe_key(_credentials(fake_rest), now=reset)
    assert result.tiers is None
    assert result.error.startswith("PlanProbeError: ")
    assert [r["path"] for r in result.requests] == ["/v1/marketstatus/now"]


def test_the_s3_pair_finds_the_reached_prefixes(fake_rest: FakeRestServer, fake_s3: FakeS3) -> None:
    _free_key(fake_rest)
    day = recent_monday(NOON.date())
    fake_s3.put(object_key("us_stocks_sip/day_aggs_v1", day), b"x")
    result = run_probe.probe_key(_credentials(fake_rest, ("PLANTED-id", "PLANTED-s3")), now=NOON)
    assert result.flat_files == ["us_stocks_sip/day_aggs_v1"]


def test_an_unreachable_flat_file_endpoint_reaches_nothing(
    fake_rest: FakeRestServer, fake_s3: FakeS3
) -> None:
    _free_key(fake_rest)
    day = recent_monday(NOON.date())
    for prefix in sources(Transport.FLAT_FILES):
        venue = "cme" if "{venue}" in prefix else None
        fake_s3.fail(object_key(prefix, day, venue), "unreachable")
    result = run_probe.probe_key(_credentials(fake_rest, ("PLANTED-id", "PLANTED-s3")), now=NOON)
    assert result.flat_files == []


def test_plans_take_the_live_tests_grammar() -> None:
    tiers = dict.fromkeys(TIERS, "basic") | {"stocks": "starter", "currencies": "business"}
    text = "stocks=starter,options=basic,indices=basic,currencies=business,futures=basic"
    assert run_probe.plans_variable(tiers, []) == text
    with_files = run_probe.plans_variable(tiers, ["us_stocks_sip/day_aggs_v1", "a/b"])
    assert with_files == text + ",s3=a/b,s3=us_stocks_sip/day_aggs_v1"
    read = plans.declared_plans(with_files)  # the live tests read it back exactly
    assert (read.tiers, read.s3, read.flat_files) == (
        tiers,
        True,
        ("a/b", "us_stocks_sip/day_aggs_v1"),
    )
    assert plans.reaches(read, "a/b")
    assert not plans.reaches(read, "global_crypto/day_aggs_v1")


def test_a_tier_that_keeps_all_history_reads_all() -> None:
    tiers = dict.fromkeys(TIERS, "advanced")
    found = run_probe._history(tiers, date(2026, 11, 3))
    assert found["stocks"] == "all"
