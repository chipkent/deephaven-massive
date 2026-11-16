"""Live check of what Massive answers to a request target the library refuses to build.

`request_target` refuses a path value holding `/`, so no ticker leaves its path segment, since
Massive serves no data for one holding `%2F` (it answered HTTP 404, and on 2026-11-16 a 307
redirect). This test builds such a target by hand, past the refusal, and records Massive's
answer, so the refusal rests on evidence read rather than kept.
Each test records what Massive sent (`evidence`); one the key's plan does not cover skips on
Massive's refusal, quoting it. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final

import pytest
import urllib3

from deephaven_massive._massive import Transport, source
from deephaven_massive._massive.net.http import new_ssl_context
from deephaven_massive._massive.rest.query import request_target

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig

pytestmark = pytest.mark.integration

# Fixture type, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"

_TARGET: Final = "/v3/trades/BRK%2FB?limit=1"
"""One trade of the ticker `BRK/B`, its slash percent-encoded, as `request_target` would send it
were the refusal lifted."""


@pytest.mark.settles("115")
@pytest.mark.timeout(60)
def test_massive_answers_a_path_ticker_holding_an_encoded_slash(
    live_config: MassiveConfig, *, evidence: Evidence
) -> None:
    """What `/v3/trades/BRK%2FB` answers: the request `request_target` refuses to build.

    Sends the target once, raw, outside the run's rate governor, with the key in the header,
    without following a redirect, and records the status, the message, the redirect's
    `Location`, and the row count. The library's own target builder refuses the value first,
    which the test shows. Passes on any answer, which it records; skips on a 403.

    Reading it: a 404, a redirect, or an empty 200 means the refusal stands on evidence, since
    none of them serves Berkshire B's trades; rows mean Massive reads `%2F` as `BRK.B`, and a
    user who wrote `BRK/B` would get Berkshire B's data, which the grammar's refusal of `BRK/B`
    still covers.
    """
    trades = source(Transport.REST, "/v3/trades/{stockTicker}")
    with pytest.raises(ValueError, match=r"contain '/', which Massive cannot read"):
        request_target(trades, {"stockTicker": "BRK/B"}, {"limit": 1})
    pool = urllib3.PoolManager(ssl_context=new_ssl_context(), retries=False)
    try:
        response = pool.request(
            "GET",
            live_config.rest_base_url + _TARGET,
            headers={"Authorization": f"Bearer {live_config.api_key}"},
        )
    finally:
        pool.clear()
    try:
        body = json.loads(response.data)
    except ValueError:
        body = response.data[:200].decode("utf-8", "replace")
    rows = body.get("results") if isinstance(body, dict) else None
    message = body.get("message") if isinstance(body, dict) else body
    evidence(
        f"GET {_TARGET}: HTTP {response.status}; message {message!r}; "
        f"Location {response.headers.get('Location')!r}"
    )
    evidence(f"rows: {len(rows) if isinstance(rows, list) else rows!r}")
    verdict = "ERROR" if response.status != 200 else ("DATA" if rows else "EMPTY")
    evidence(f"verdict {verdict}")
    if response.status == 403:
        pytest.skip(f"not entitled to stock trades ({message})")
