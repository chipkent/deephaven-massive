"""The live run's probe: whether Massive takes the key, its clock, and its market closures.

The live-run program (`live_run`) probes at each start and at a fixed time each trading day
(`timetable`). One probe asks, in this order:

1. Massive's market status, whose `serverTime` gives the machine clock's offset; an HTTP 401
   here means the key was rejected, and the program stops.
2. Massive's upcoming market closures (`/v1/marketstatus/upcoming`), which plan a stream night
   (`timetable.next_stream_night`) around a holiday or an early close no fixed rule predicts.
   A probe that cannot read them keeps the closures the program had.

Both are reference data, which belong to no subscription. The probe asks nothing about what
the key holds: the checks learn that from Massive's answers. A probe whose market status fails
(a 429, a 5xx, a timeout) comes back with its error, and the program tries again later. Each
request's path, status, and request id are recorded through the wire package's reply-capture
hook, never a body or a header.

The probe opens no session: it uses the wire package's clients directly. Importing them still
imports the library, and so Deephaven, which needs a running JVM, so the live run's parent runs
the probe in a child process (`probe_result.probe_in_child`).
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive import (
    RequestGovernor,
    RestClient,
    Transport,
    WireAuthError,
    WireError,
    set_reply_capture,
    source,
)
from tests._live.live_run.probe_result import ProbeResult

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven_massive._massive import CapturedReply

REST_BASE_URL: Final = "https://api.massive.com"
"""Massive's REST API."""


@dataclass(frozen=True)
class Credentials:
    """The key and endpoint a probe uses. Never written anywhere.

    Attributes:
        api_key: The API key.
        rest_base_url: The REST API's base URL.
    """

    api_key: str
    rest_base_url: str = REST_BASE_URL


def credentials(environ: Mapping[str, str]) -> Credentials:
    """Read the probe's credentials from the environment.

    Args:
        environ: The environment.

    Returns:
        The API key.
    """
    return Credentials(api_key=environ.get("MASSIVE_API_KEY", "").strip())


def probe_key(credentials: Credentials, *, now: datetime) -> ProbeResult:
    """Ask Massive whether it takes the key, for its clock, and for its upcoming closures.

    Args:
        credentials: The key and endpoint.
        now: The current time, aware, when the probe runs.

    Returns:
        The result; its `error` says why the market status could not be read.
    """
    result = ProbeResult(when=now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
    governor = RequestGovernor(("other",), learn_only=("other",))
    client = RestClient(
        credentials.api_key,
        base_url=credentials.rest_base_url,
        max_concurrency=1,
        governor=governor,
    )
    try:
        with _recording(result):
            _probe_rest(client, result)
    finally:
        client.close()
    return result


def _probe_rest(client: RestClient, result: ProbeResult) -> None:
    """Read the clock offset and the closures into a result.

    Args:
        client: The probe's REST client.
        result: Filled in place.
    """
    try:
        page = client.get(source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other")
        server_time = page.body.get("serverTime") if isinstance(page.body, dict) else None
        if isinstance(server_time, str):
            stamp = datetime.fromisoformat(server_time)
            result.clock_offset = round((datetime.now(UTC) - stamp).total_seconds(), 3)
        result.closures = _closures(client)
    except WireAuthError:
        result.key_rejected = True
        result.error = "the API key was rejected"
    except WireError as error:
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
