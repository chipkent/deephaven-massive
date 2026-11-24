"""Tests of the live tests' recorded requests (`tests/_live/recorded_requests.py`).

Each request runs with a real `RestClient` against `FakeRestServer`, so the recorded query is
the one the library encoded and sent.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    RequestGovernor,
    RestClient,
    Transport,
    WireEntitlementError,
    source,
)

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the server comes from `fake_rest`.
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope
from tests._live import recorded_requests

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests._fakes.rest import FakeRestServer

_CONTRACTS = "/futures/v1/contracts"
_QUERY = {"date.gte": date(2026, 11, 16), "limit": 1000, "ticker.any_of": ["0BTZ9", "0ESZ4"]}


@pytest.fixture
def client(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor((*dm.Subscription, "other"), clock=FakeClock())
    made = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield made
    made.close()


def _get(client: RestClient, lines: list[str]) -> list[dict[str, object]]:
    page = recorded_requests.get(
        client,
        source(Transport.REST, _CONTRACTS),
        path_params={},
        query=_QUERY,
        bucket="futures",
        evidence=lines.append,
    )
    return page.rows


def test_a_request_is_recorded_as_sent_with_massive_s_request_id(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply(_CONTRACTS, Reply(envelope([{"ticker": "0BTZ9"}], request_id="r-776")))
    lines: list[str] = []
    assert _get(client, lines) == [{"ticker": "0BTZ9"}]
    host = fake_rest.url.split("://")[1].split(":")[0]
    assert lines == [
        (
            "GET /futures/v1/contracts?date.gte=2026-11-16&limit=1000&ticker.any_of=0BTZ9,0ESZ4 "
            f"on {host}: HTTP 200, request_id r-776"
        )
    ]
    assert fake_rest.get_queries(one_value=True) == [
        (
            _CONTRACTS,
            {"date.gte": "2026-11-16", "limit": "1000", "ticker.any_of": "0BTZ9,0ESZ4"},
        )
    ]


def test_a_refused_request_is_recorded_before_its_error_propagates(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    refusal = {"status": "NOT_AUTHORIZED", "request_id": "r-403", "message": "upgrade"}
    fake_rest.reply(_CONTRACTS, Reply(refusal, status=403))
    lines: list[str] = []
    with pytest.raises(WireEntitlementError):
        _get(client, lines)
    assert len(lines) == 1
    assert lines[0].endswith(": HTTP 403, request_id r-403")
