"""Live checks of the library's rate governors against Massive: learning Basic, and forgetting it.

The sessions of one API key share a rate governor. A session that declares nothing learns
that a subscription is on Basic from Massive's refusal, whose text names the per-minute limit,
and the governor paces it from then on. What it learned lives as long as the governor: once
the key's last session has closed and the governor holds no slot or pause, it is dropped, and
a new session starts unpaced. The test here draws one refusal on purpose from a class the key
holds on Basic, so it needs at least one class left on Basic. It is marked
`integration_rate_limits`, so `-m integration` leaves it out and `-m integration_rate_limits`
runs it, with no other client of the key. How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import RATE_WINDOW

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

    from deephaven_massive import MassiveConfig
    from deephaven_massive._ingest.rate_limits import GovernorRegistry

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)

_MINUTE = 61.0
"""Seconds the test lets pass first, so earlier requests have left Massive's window."""

_CALLS = 7
"""The most one-request calls that learn the limit: five answered, the refused sixth, whose
retry after the pause is the seventh request, and one to spare."""


def _row(
    m: dm.MassiveSession, bucket: str, *, rows_when: RowsWhen, attempts: int
) -> dict[str, Any]:
    """Return a bucket's row of `m.rate_limits()` once it shows a number of attempts.

    The rows are written by the session's diagnostics writer after each change of the
    governor, so a row read at once may not yet show the last request.

    Args:
        m: The session.
        bucket: The row's `Bucket`.
        rows_when: Waits until a table's rows satisfy a predicate, failing after 15 s.
        attempts: The least `Attempts` the row must show.

    Returns:
        The row.
    """
    view = m.rate_limits()
    try:
        rows = rows_when(
            view,
            lambda rows: any(r["Bucket"] == bucket and r["Attempts"] >= attempts for r in rows),
        )
    finally:
        m.release(view)
    return next(r for r in rows if r["Bucket"] == bucket)


@pytest.mark.needs("some:basic")
@pytest.mark.integration_rate_limits
@pytest.mark.verifies("65")
@pytest.mark.requests(7)
@pytest.mark.timeout(330)
def test_a_learned_basic_limit_is_forgotten_once_the_keys_sessions_close(
    live_config: MassiveConfig,
    *,
    basic_class: str,
    one_request_call: Callable[[dm.MassiveSession], object],
    own_governors: GovernorRegistry,
    rows_when: RowsWhen,
    evidence: Evidence,
) -> None:
    """Whether a session learns Basic from Massive's real refusal, and a later session forgets it.

    Uses a rate-governor registry of its own (`own_governors`), so only the test's sessions
    hold its governor, and sessions that declare nothing Basic. After a pause of 61 s, so
    earlier requests have left Massive's window:

    1. Session A makes one-request calls of a class on Basic (Indices preferred) until its
       `m.rate_limits()` row reads `LimitSource` `LEARNED`: Massive answers five, refuses
       the sixth with HTTP 429, and the library learns the limit from the refusal's text,
       pauses a minute, and sends it again. Records A's row and the calls it took.
    2. Closes A, then waits until the governor holds no slot: 61 s after the last answer.
    3. Opens session B with the same key, and records its row of the class.

    Passes when A learned the limit within seven calls and B's row is unpaced, with no
    attempt or refusal: B has a new governor, so the learned limit went with the old one.
    Learning fails when Massive's refusal no longer carries the text the library matches
    ("maximum requests per minute"), which then never paces a Basic subscription by itself.
    The test makes up to seven calls of the class, normally seven requests in six calls,
    and takes about three minutes.
    """
    config = dataclasses.replace(live_config, rest_basic_subscriptions=frozenset())
    threading.Event().wait(_MINUTE)
    start = time.monotonic()
    row: dict[str, Any] = {}
    with dm.MassiveSession(config) as first:
        for calls in range(1, _CALLS + 1):
            one_request_call(first)
            row = _row(first, basic_class, rows_when=rows_when, attempts=calls)
            evidence(
                f"{basic_class} call {calls} answered at {time.monotonic() - start:.1f} s: "
                f"LimitSource {row['LimitSource']}, Attempts {row['Attempts']}, "
                f"Refusals {row['Refusals']}"
            )
            if row["LimitSource"] == "LEARNED":
                break
        evidence(f"session A's {basic_class} row: {row}")
    assert row["LimitSource"] == "LEARNED", f"no limit learned in {_CALLS} calls: is it on Basic?"
    threading.Event().wait(RATE_WINDOW + 1.0)
    with dm.MassiveSession(config) as second:
        fresh = _row(second, basic_class, rows_when=rows_when, attempts=0)
        evidence(f"session B's {basic_class} row: {fresh}; governors kept: {len(own_governors)}")
    learned = (fresh["RequestsPerMinute"], fresh["LimitSource"])
    counts = (fresh["Attempts"], fresh["Refusals"])
    assert learned == (None, None), f"session B still paces {basic_class}: {fresh}"
    assert counts == (0, 0), f"session B shares session A's governor: {fresh}"
