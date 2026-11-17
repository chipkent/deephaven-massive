"""Tests of the options live tests' raw requests (`tests/_live/option_requests.py`), offline.

Each request runs with a real `RestClient` against `FakeRestServer`, so the client encodes and
sends it as it does in the live run: a query name the path lacks or a value of a type its
parameter refuses fails here, in CI, rather than on the owner's live run.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RequestGovernor, RestClient, Transport, source

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the server comes from `fake_rest`.
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope
from tests._live import option_requests

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests._fakes.rest import FakeRestServer


@pytest.fixture
def client(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor((*dm.Subscription, "other"), clock=FakeClock())
    made = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield made
    made.close()


@pytest.mark.parametrize(
    ("request_", "sent"),
    [
        (
            option_requests.open_interest(root="SPXW", through=date(2025, 11, 17), limit=2),
            {"root": "SPXW", "date.lte": "2025-11-17", "limit": "2"},
        ),
        (option_requests.open_interest(limit=50000), {"limit": "50000"}),
        (option_requests.unusual_trades(limit=300), {"limit": "300"}),
        (
            option_requests.index_list(active=True),
            {"market": "indices", "active": "true", "limit": "1000"},
        ),
        (
            option_requests.index_list(active=False),
            {"market": "indices", "active": "false", "limit": "1000"},
        ),
    ],
)
def test_each_request_is_sent_as_built(
    fake_rest: FakeRestServer,
    client: RestClient,
    *,
    request_: option_requests.Request,
    sent: dict[str, str],
) -> None:
    fake_rest.reply_default(Reply(envelope([])))
    path, query = request_
    client.get(source(Transport.REST, path), {}, query, bucket="other")
    assert fake_rest.get_queries(one_value=True) == [(path, sent)]
