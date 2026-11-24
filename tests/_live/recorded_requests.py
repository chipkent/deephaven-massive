"""A live test's REST request recorded in its evidence as sent, with Massive's request ID.

A report to Massive cites each request it quotes by the request as sent and the `request_id`
Massive gave it, which Massive's support can look up. `get` sends one GET with the live
client and records, for each reply Massive sent it (a retry's too), one evidence line with the
path, the query as the client sent it, the host, the status, and the `request_id`. The lines
come from the client's reply-capture hook (`set_reply_capture`), so the query is the text that
went out, not a rendering of the arguments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._massive import CapturedReply, set_reply_capture

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive import Page, RestClient, RestPath


def get(
    client: RestClient,
    path: RestPath,
    *,
    path_params: Mapping[str, object],
    query: Mapping[str, object],
    bucket: str,
    evidence: Callable[[str], None],
) -> Page:
    """Send one GET, and record each reply Massive sent it in the evidence.

    A request Massive refused is recorded too, before its error propagates.

    Args:
        client: The live REST client.
        path: The path, from the library's spec.
        path_params: The path's values, by name.
        query: The query parameters.
        bucket: The rate-limit bucket the request is paced in.
        evidence: Records one line per reply (`line`).

    Returns:
        The page Massive answered.
    """
    replies: list[CapturedReply] = []
    # The live tests' process installs no other sink, so this one replaces none.
    set_reply_capture(replies.append)
    try:
        return client.get(path, path_params, query, bucket=bucket)
    finally:
        set_reply_capture(None)
        for reply in replies:
            evidence(line(reply))


def line(reply: CapturedReply) -> str:
    """Describe one reply as the evidence records it.

    Args:
        reply: The reply, as the client captured it.

    Returns:
        Such as `"GET /futures/v1/contracts?ticker.any_of=0BTZ9,0ESZ4&limit=1000 on
        api.massive.com: HTTP 200, request_id 6a1f..."`; the query is not percent-encoded, so
        it reads as sent.
    """
    query = "&".join(f"{name}={value}" for name, value in reply.query)
    return (
        f"GET {reply.path}?{query} on {reply.host}: HTTP {reply.status}, "
        f"request_id {reply.request_id}"
    )
