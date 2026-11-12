"""The live run's probe: what the key can do right now, and every request it took to find out.

The live-run program (`live_run`) probes at each start and at a fixed time each trading day
(`timetable`). One probe asks, in this order:

1. Massive's market status, whose `serverTime` gives the machine clock's offset; an HTTP 401
   here means the key was rejected, and the program stops.
2. Each subscription's tier, by the live suite's own probe (`probe.probe_tiers`): a snapshot
   of one liquid ticker, its trades and quotes, and the `fmv` field. Using the suite's probe
   keeps the tiers in the exact grammar the live tests read from `MASSIVE_LIVE_PLANS`.
3. Massive's upcoming market closures (`/v1/marketstatus/upcoming`), which plan a stream night
   (`timetable.next_stream_night`) around a holiday or an early close no fixed rule predicts.
   A probe that cannot read them still tells the tiers; the program keeps the closures it had.
4. With both S3 credentials set, which flat-file prefixes they reach (`probe.reached_prefixes`).

Every request is paced as Basic (5 a minute per subscription), since nothing is known yet; no
subscription gets more than three. A probe that cannot tell the tiers (an answer that proves
nothing, a 429, a 5xx, a timeout) comes back *unknown*, and the program keeps the previous
tiers and tries again later. Each request's path, status, and request id are recorded through
the wire package's reply-capture hook, never a body or a header; the history each subscription
keeps comes from the documented windows (`plans.history_start`).

The probe opens no session: it uses the wire package's clients directly. Importing them still
imports the library, and so Deephaven, which needs a running JVM, so the live run's parent runs
the probe in a child process (`probe_result.probe_in_child`).
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive import (
    FlatFileClient,
    RequestGovernor,
    RestClient,
    Transport,
    WireAuthError,
    WireError,
    set_reply_capture,
    source,
)
from tests._live.calendar import NEW_YORK, recent_monday
from tests._live.live_run.children import S3_VARIABLES
from tests._live.live_run.probe_result import ProbeResult
from tests._live.plans import TIERS, LivePlans, history_start
from tests._live.probe import PlanProbeError, probe_tiers, reached_prefixes

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven_massive._massive import CapturedReply

REST_BASE_URL: Final = "https://api.massive.com"
"""Massive's REST API."""

S3_ENDPOINT_URL: Final = "https://files.massive.com"
"""Massive's flat-file endpoint."""

S3_BUCKET: Final = "flatfiles"
"""Massive's flat-file bucket."""


@dataclass(frozen=True)
class Credentials:
    """The secrets and endpoints a probe uses. Never written anywhere.

    Attributes:
        api_key: The API key.
        s3_pair: The S3 access key id and secret access key, or None when unset.
        rest_base_url: The REST API's base URL.
        s3_endpoint_url: The flat-file endpoint.
        s3_bucket: The flat-file bucket.
    """

    api_key: str
    s3_pair: tuple[str, str] | None
    rest_base_url: str = REST_BASE_URL
    s3_endpoint_url: str = S3_ENDPOINT_URL
    s3_bucket: str = S3_BUCKET


def credentials(environ: Mapping[str, str]) -> Credentials:
    """Read the probe's credentials from the environment.

    Args:
        environ: The environment.

    Returns:
        The API key, and the S3 pair when both are set.
    """
    access, secret = (environ.get(name, "").strip() for name in S3_VARIABLES)
    return Credentials(
        api_key=environ.get("MASSIVE_API_KEY", "").strip(),
        s3_pair=(access, secret) if access and secret else None,
    )


def probe_key(credentials: Credentials, *, now: datetime) -> ProbeResult:
    """Ask Massive what the key can do now.

    Args:
        credentials: The secrets and endpoints.
        now: The current time, aware; its New York date picks the flat-file day and the
            history windows, and its time of day is checked against Massive's snapshot reset.

    Returns:
        The result; its `tiers` are None when the probe could not tell them.
    """
    result = ProbeResult(when=now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
    governor = RequestGovernor((*TIERS, "other"), learn_only=("other",))
    governor.declare(TIERS)
    client = RestClient(
        credentials.api_key,
        base_url=credentials.rest_base_url,
        max_concurrency=1,
        governor=governor,
    )
    try:
        with _recording(result):
            _probe_rest(client, result, now)
            if result.tiers is not None and credentials.s3_pair is not None:
                result.flat_files = _reached(credentials, now)
    finally:
        client.close()
    if result.tiers is not None:
        today = now.astimezone(NEW_YORK).date()
        result.history_start = _history(result.tiers, today)
    return result


def _probe_rest(client: RestClient, result: ProbeResult, now: datetime) -> None:
    """Read the clock offset and the tiers into a result.

    Args:
        client: The probe's REST client.
        result: Filled in place.
        now: The current time, aware.
    """
    try:
        page = client.get(source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other")
        server_time = page.body.get("serverTime") if isinstance(page.body, dict) else None
        if isinstance(server_time, str):
            stamp = datetime.fromisoformat(server_time)
            result.clock_offset = round((datetime.now(UTC) - stamp).total_seconds(), 3)
        result.tiers = probe_tiers(client, now=now.astimezone(NEW_YORK).time())
        result.closures = _closures(client)
    except WireAuthError:
        result.key_rejected = True
        result.error = "the API key was rejected"
    except (PlanProbeError, WireError) as error:
        result.error = f"{type(error).__name__}: {error}"


def _closures(client: RestClient) -> dict[str, str] | None:
    """Read Massive's upcoming market closures.

    Args:
        client: The probe's REST client.

    Returns:
        Each listed date's status, `closed` winning over `early-close`; None when the request
        failed or its answer is not a list of objects.
    """
    try:
        upcoming = source(Transport.REST, "/v1/marketstatus/upcoming")
        page = client.get(upcoming, {}, {}, bucket="other")
    except WireError:
        return None
    found: dict[str, str] = {}
    for entry in page.rows:
        day, status = entry.get("date"), entry.get("status")
        if isinstance(day, str) and status in {"closed", "early-close"}:
            found[day] = "closed" if "closed" in {status, found.get(day)} else status
    return found


def _reached(credentials: Credentials, now: datetime) -> list[str]:
    """Find the flat-file prefixes the S3 pair reaches.

    Args:
        credentials: The secrets, with the S3 pair set.
        now: The current time, aware.

    Returns:
        The reached prefixes; empty when the endpoint fails.
    """
    assert credentials.s3_pair is not None  # checked by the caller
    access_key_id, secret_access_key = credentials.s3_pair
    client = FlatFileClient(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        endpoint_url=credentials.s3_endpoint_url,
        bucket=credentials.s3_bucket,
        max_pool=1,
    )
    try:
        day = recent_monday(now.astimezone(NEW_YORK).date())
        return list(reached_prefixes(client, day))
    except WireError:
        return []
    finally:
        client.close()


def _history(tiers: dict[str, str], today: date) -> dict[str, str]:
    """Return each subscription's earliest readable date by the documented windows.

    Args:
        tiers: Each subscription's tier.
        today: The New York date.

    Returns:
        Each class's first readable date, ISO, or `"all"`.
    """
    plans = LivePlans(tiers=tiers, s3=False, flat_files=(), source="probed")
    found = {}
    for name in tiers:
        first = history_start(plans, name, today)
        found[name] = "all" if first is None else first.isoformat()
    return found


@contextlib.contextmanager
def _recording(result: ProbeResult) -> Iterator[None]:
    """Record every reply's request while the probe runs, never its body.

    Args:
        result: Its `requests` receives one entry per reply.

    Yields:
        Control to the probe.
    """

    def sink(reply: CapturedReply) -> None:
        result.requests.append(
            {
                "path": reply.path,
                "query": dict(reply.query),
                "status": reply.status,
                "request_id": reply.request_id,
            }
        )

    set_reply_capture(sink)
    try:
        yield
    finally:
        set_reply_capture(None)
