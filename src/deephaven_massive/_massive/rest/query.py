"""Turn a REST path and its parameter values into the request target the client sends.

The REST client calls `request_target` once per first page; later pages follow the server's
`next_url`, which `next_target` checks and cleans. Every value is encoded by the `ParamKind`
the spec gives its parameter (`spec.values.encode_value`, which documents what each kind
accepts and how it is written), never by guessing from the Python type, so a value of the
wrong type is refused here instead of reaching Massive as surprising text.

Names arrive in wire form (`"timestamp.gte"`); the library's filter spelling is translated
before the client. The functions are pure and never modify their arguments, so they are safe
from any thread. This module imports only the standard library, `net.origins`, and `spec`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote, unquote, urlencode, urlsplit

from deephaven_massive._massive.net.origins import origin_of
from deephaven_massive._massive.spec.values import encode_value

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._massive.net.origins import Origin
    from deephaven_massive._massive.spec.types import RestPath

__all__ = ["next_target", "request_target"]


def request_target(
    rest_path: RestPath, path_params: Mapping[str, object], query_params: Mapping[str, object]
) -> str:
    """Return the path and query of the first request for a REST path.

    Args:
        rest_path: The path, whose `params` say each parameter's kind and place.
        path_params: A value for every placeholder of the template, by wire name. Each is
            encoded by its kind and then percent-encoded whole (`I:SPX` is sent as
            `I%3ASPX`). No value may contain `/`: Massive answers HTTP 404 for a segment
            holding `%2F`, and no canonical symbol contains one (a forex pair is `C:EURUSD`).
        query_params: Query values by wire name, in the order to send them. A `None` value is
            left out.

    Returns:
        The request target, such as `"/v3/trades/AAPL?timestamp.gte=1704067200000000000"`,
        with no `?` when no query value is sent.

    Raises:
        ValueError: If a path parameter is missing, `None`, or unknown, its encoded value
            contains `/`, or a query name is not a query parameter of the path. Each value is
            encoded by `encode_value`, which raises `TypeError` or `ValueError` for a value
            its kind cannot send.
    """
    params = {param.name: param for param in rest_path.params}
    path_names = {name for name, param in params.items() if param.in_path}
    given = dict(path_params)
    if set(given) != path_names or any(value is None for value in given.values()):
        raise ValueError(
            f"{rest_path.template} takes path parameters {sorted(path_names)}, "
            f"given {sorted(name for name, value in given.items() if value is not None)}"
        )
    texts = {name: encode_value(params[name], value) for name, value in given.items()}
    slashed = sorted(name for name, text in texts.items() if "/" in text)
    if slashed:
        raise ValueError(
            f"{rest_path.template}: path parameters {slashed} contain '/', which Massive cannot "
            "read inside a path segment"
        )
    segments = {name: quote(text, safe="") for name, text in texts.items()}
    path = rest_path.template.format_map(segments)
    query = []
    for name, value in dict(query_params).items():
        param = params.get(name)
        if param is None or param.in_path:
            raise ValueError(f"{rest_path.template} has no query parameter {name!r}")
        if value is not None:
            query.append((name, encode_value(param, value)))
    return f"{path}?{urlencode(query, quote_via=quote)}" if query else path


def next_target(next_url: str, origin: Origin) -> str:
    """Return the request target of a `next_url`, after checking where it points.

    Massive's `next_url` is an absolute URL that carries the cursor and the original query.
    The client requests it only on the REST host it was built for: the bearer key must never
    go to another host. Any `apiKey` query parameter is removed, so the key is never put in a
    URL (and so never in a log); the rest of the query is kept byte for byte. The error for a
    refused `next_url` quotes it without `apiKey` and without user information, so no
    credential reaches an exception or a log either.

    Args:
        next_url: The `next_url` of the previous page.
        origin: The origin of the client's base URL, from `origin_of`.

    Returns:
        The target to request on `origin`: the path, and the query without `apiKey`.

    Raises:
        ValueError: If `next_url` is not an absolute `http` or `https` URL, or its scheme,
            host, or port differs from `origin`'s; the message names both, redacted.
    """
    parts = urlsplit(next_url)
    kept = [
        item
        for item in parts.query.split("&")
        if item and unquote(item.partition("=")[0]) != "apiKey"
    ]
    path = parts.path or "/"
    target = f"{path}?{'&'.join(kept)}" if kept else path
    try:
        points_to: Origin | None = origin_of(next_url)
    except ValueError:
        points_to = None
    if points_to != origin:
        shown = f"{parts.scheme}://{parts.netloc.rpartition('@')[2]}{target}"
        raise ValueError(f"next_url {shown!r} leaves {origin[0]}://{origin[1]}:{origin[2]}")
    return target
