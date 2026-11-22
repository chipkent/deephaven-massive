"""Tests of the live run's probe (`tests/_live/live_run/run_probe.py`).

The probe runs against `FakeRestServer` with a planted fake key. It must record each request's
path and status but never a body or the key, read the clock offset and the closures, ask
nothing about what the key holds, and come back with an error or rejected when Massive's
answers say so.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from tests._fakes.rest import Reply, envelope
from tests._live.calendar import NEW_YORK
from tests._live.live_run import run_probe

if TYPE_CHECKING:
    from tests._fakes.rest import FakeRestServer

PLANTED = "PLANTED-probe-key-6a1b"

NOON = datetime(2026, 11, 3, 12, tzinfo=NEW_YORK)

_UPCOMING = [
    {"date": "2026-11-26", "exchange": "NYSE", "name": "Thanksgiving", "status": "closed"},
    {"date": "2026-11-27", "exchange": "NASDAQ", "name": "Thanksgiving", "status": "early-close"},
    {"date": "2026-12-25", "exchange": "NYSE", "name": "Christmas", "status": "early-close"},
    {"date": "2026-12-25", "exchange": "NASDAQ", "name": "Christmas", "status": "closed"},
    {"date": "2026-12-25", "exchange": "OTC", "name": "Christmas", "status": "early-close"},
    {"date": "2027-01-01", "exchange": "NYSE", "name": "New Year", "status": "open"},
]
"""Massive's upcoming closures as a probe reads them: a closure on one exchange and an early
close on another make the day closed, and anything but a closure is left out."""


def _answering(
    server: FakeRestServer, *, server_time: str | None = "2026-11-03T12:00:00-05:00"
) -> None:
    body: dict[str, object] = {"market": "open", "echo": PLANTED}
    if server_time is not None:
        body["serverTime"] = server_time
    server.reply("/v1/marketstatus/now", Reply(body))
    server.reply("/v1/marketstatus/upcoming", Reply(_UPCOMING))


def _credentials(server: FakeRestServer) -> run_probe.Credentials:
    return run_probe.Credentials(api_key=PLANTED, rest_base_url=server.url)


def test_a_probe_reads_the_clock_and_the_closures_and_records_each_request(
    fake_rest: FakeRestServer,
) -> None:
    _answering(fake_rest)
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert (result.key_rejected, result.error) == (False, "")
    assert result.when == "2026-11-03T17:00:00Z"
    assert result.clock_offset is not None
    assert [(r["path"], r["status"]) for r in result.requests] == [
        ("/v1/marketstatus/now", 200),
        ("/v1/marketstatus/upcoming", 200),
    ]
    assert PLANTED not in json.dumps(result.requests)
    assert result.closures == {
        "2026-11-26": "closed",
        "2026-11-27": "early-close",
        "2026-12-25": "closed",
    }


def test_closures_that_cannot_be_read_are_unknown(fake_rest: FakeRestServer) -> None:
    _answering(fake_rest)
    fake_rest.reply_first("/v1/marketstatus/upcoming", Reply(["not an object"]))
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert (result.error, result.closures) == ("", None)
    fake_rest.reply_first("/v1/marketstatus/upcoming", Reply({"status": "NOT_FOUND"}, status=404))
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert (result.error, result.closures) == ("", None)


def test_a_probe_without_server_time_has_no_clock_offset(fake_rest: FakeRestServer) -> None:
    _answering(fake_rest, server_time=None)
    assert run_probe.probe_key(_credentials(fake_rest), now=NOON).clock_offset is None


def test_a_rejected_key_stops_the_probe(fake_rest: FakeRestServer) -> None:
    fake_rest.reply(
        "/v1/marketstatus/now",
        Reply(envelope(status="ERROR", message="Unknown API Key"), status=401),
    )
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert (result.key_rejected, result.error) == (True, "the API key was rejected")


def test_a_market_status_that_fails_comes_back_with_its_error(fake_rest: FakeRestServer) -> None:
    fake_rest.reply("/v1/marketstatus/now", Reply({"status": "NOT_FOUND"}, status=404))
    result = run_probe.probe_key(_credentials(fake_rest), now=NOON)
    assert not result.key_rejected
    assert result.error.startswith("Wire")
    assert [r["path"] for r in result.requests] == ["/v1/marketstatus/now"]


def test_credentials_come_from_the_environment() -> None:
    environ = {"MASSIVE_API_KEY": f" {PLANTED} ", "MASSIVE_S3_ACCESS_KEY_ID": "PLANTED-id"}
    assert run_probe.credentials(environ) == run_probe.Credentials(api_key=PLANTED)
