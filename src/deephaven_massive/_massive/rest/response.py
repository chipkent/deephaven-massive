"""Decode Massive's REST responses strictly, and map failed responses to wire errors.

The REST client hands every response body here exactly once. A 200 body becomes a `Page`; any
other status becomes the `WireError` the client raises once it stops retrying. Nothing that
is malformed passes as data: a body that is not UTF-8 JSON, an envelope of the wrong shape, or
a missing result key under an error status raises `WireError`, so a broken response can never
become an empty or truncated dataset.

**Where the rows are** depends on the path's `RestPath.result_key`:

- A key such as `"results"` or `"last"`: the body is an envelope object, and the rows are the
  value under the key. A dotted key (`"results.values"`) walks into nested objects. An object
  there is one row and a list is the rows. When the key is absent (Massive omits it when a
  range has no data), the page is empty if the envelope's `status` is `OK` or `DELAYED`, and
  an error otherwise.
- `None`: the body is the rows. A JSON list is the rows, kept whole: their keys are data (a
  holiday row's `status` is `"closed"`). A JSON object is one row and also the envelope, so
  its `status` and `request_id` describe the page and the `ENVELOPE_KEYS` are removed from the
  row.

**Envelope status.** For every path, `NOT_AUTHORIZED` raises `WireEntitlementError` and
`ERROR` raises `WireError`. `OK`, `DELAYED`, and `success` (the "last" endpoints) are the
documented statuses of a good page. Any other status still yields its rows, and is reported in
the page's `DriftReport.statuses`, so a new status Massive introduces is seen, not swallowed.

**Error mapping** of a status other than 200: 401 is `WireAuthError`, 403 is
`WireEntitlementError`, and every other status is `WireError`. Each error carries the status,
the host, the request target (whose query never holds a secret), and the `request_id`, and its
message names them with the first 200 characters of the server's `message` (or of the body,
when it has none).

The functions are pure and safe from any thread. This module imports only the standard
library, `net`, and `spec`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypedDict

from deephaven_massive._massive.net import codec
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
)
from deephaven_massive._massive.spec.types import ENVELOPE_KEYS, DriftReport

if TYPE_CHECKING:
    from deephaven_massive._massive.spec.types import RestPath

__all__ = ["GOOD_STATUSES", "Page", "Request", "decode_page", "http_error"]

GOOD_STATUSES = frozenset({"OK", "DELAYED", "success"})
"""The documented envelope statuses of a good page; any other non-error status is drift."""

_EMPTY_STATUSES = frozenset({"OK", "DELAYED"})
"""The statuses under which a missing result key means an empty page, not an error."""

_EXCERPT = 200
"""How many characters of a server's message or body an error quotes."""


class ErrorContext(TypedDict):
    """The arguments of a `WireError` for one failed request: `WireError(**context)`."""

    message: str
    status: int | None
    host: str
    path: str
    request_id: str | None


@dataclass(frozen=True)
class Request:
    """Where one request went, for naming it in errors and logs.

    Attributes:
        host: The REST host name, such as `"api.massive.com"`.
        target: The path and query requested, such as `"/v3/trades/AAPL?limit=50000"`. The
            client never puts a secret in a query, so the target is safe to show.
    """

    host: str
    target: str

    def context(
        self, what: str, detail: str, *, status: int | None, request_id: str | None = None
    ) -> ErrorContext:
        """Return the arguments of a wire error for this request, as `WireError(**context)`.

        Args:
            what: What happened, such as `"HTTP 404"`, leading the message.
            detail: The server's message or the cause, ending the message.
            status: The HTTP status, or `None` when there was no response.
            request_id: Massive's `request_id`, or `None`.

        Returns:
            The arguments; the message reads
            `"<what> from GET <target> on <host> (request_id <id>): <detail>"`, without the
            parenthesis when there is no `request_id`.
        """
        rid = f" (request_id {request_id})" if request_id else ""
        return ErrorContext(
            message=f"{what} from GET {self.target} on {self.host}{rid}: {detail}",
            status=status,
            host=self.host,
            path=self.target,
            request_id=request_id,
        )


@dataclass(frozen=True)
class Page:
    """One decoded 200 response of a REST path.

    Created by the REST client and handed to the caller, who passes `rows` to `to_batch`. It
    is immutable and safe to share between threads, as long as no one mutates the decoded
    objects it holds.

    Attributes:
        status: The envelope's `status` (`"OK"`, `"DELAYED"`, `"success"`, ...), or `""` when
            the body has none (a bare list, `/v1/marketstatus/now`). A caller reads `DELAYED`
            here.
        request_id: The envelope's `request_id`, or `None`.
        rows: The rows, each a decoded JSON object, in the server's order. When paging stops
            at a row limit, the last page holds only the rows within it.
        body: The whole decoded body, for what is outside the rows (such as `next_url`, or a
            symbol beside the result key).
        drift: What the envelope had that is not documented: `statuses` holds an unknown
            `status`. The rows' own drift comes from `to_batch`.

    The `repr` leaves out `rows` and `body`: they can be large, and a body may hold a
    `next_url` that carries an `apiKey`.

    Example:
        ::

            page = client.get(trades, {"stockTicker": "AAPL"}, {"limit": 1000}, bucket="stocks")
            batch, drift = to_batch(page.rows, trades, required={"SipTimestamp"})
    """

    status: str
    request_id: str | None
    rows: list[dict[str, object]] = field(repr=False)
    body: dict[str, object] | list[object] = field(repr=False)
    drift: DriftReport = field(default_factory=DriftReport)


def _excerpt(data: bytes) -> str:
    """Return the start of a body as text, for an error message.

    Args:
        data: The body.

    Returns:
        The first 200 characters, with undecodable bytes replaced, or `"an empty body"`.
    """
    return data.decode("utf-8", "replace")[:_EXCERPT] or "an empty body"


def http_error(status: int, data: bytes, request: Request) -> WireError:
    """Return the error for a response whose HTTP status is not 200.

    Args:
        status: The HTTP status.
        data: The response body, which may be a JSON envelope or anything else (an HTML
            page from a proxy).
        request: The request that received it.

    Returns:
        `WireAuthError` for 401, `WireEntitlementError` for 403, and `WireError` otherwise,
        quoting the envelope's `message` (or `error`) when the body is one, else the body.
    """
    kind = {401: WireAuthError, 403: WireEntitlementError}.get(status, WireError)
    try:
        body = codec.loads(data)
    except ValueError:
        body = None
    detail = _excerpt(data)
    request_id = None
    if isinstance(body, dict):
        message = body.get("message") or body.get("error")
        if isinstance(message, str) and message:
            detail = message[:_EXCERPT]
        request_id = _request_id(body.get("request_id"))
    return kind(**request.context(f"HTTP {status}", detail, status=status, request_id=request_id))


def _as_rows(value: object, request: Request) -> list[dict[str, object]]:
    """Return the rows a result value holds.

    Args:
        value: An object (one row) or a list of objects (the rows).
        request: The request, for the error.

    Returns:
        The rows.

    Raises:
        WireError: If `value` is neither, or a list holds something other than an object.
    """
    if isinstance(value, dict):
        return [value]
    if not isinstance(value, list):
        raise WireError(
            **request.context(
                "HTTP 200", "the rows are neither a JSON object nor a list", status=200
            )
        )
    for index, row in enumerate(value):
        if not isinstance(row, dict):
            raise WireError(
                **request.context("HTTP 200", f"row {index} is not a JSON object", status=200)
            )
    return value


def _request_id(value: object) -> str | None:
    """Return an envelope's `request_id` as text, whatever JSON scalar Massive sent.

    The `request_id` is metadata for logs and support requests, so it never fails a call:
    Massive's documentation samples send it as text and as a number (`1`). Text is kept, an
    integer or a number becomes its `repr` text (`1`, `2.5`, `1e+20`), and a boolean `true` or
    `false`. A missing value, `null`, or an object or list (never documented) gives None.

    Args:
        value: The decoded `request_id` value, or None when the envelope has none.

    Returns:
        The text, or None.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    return None


def _envelope(body: dict[str, object], request: Request) -> tuple[str, str | None]:
    """Read and check an envelope's `status` and `request_id`.

    Args:
        body: The decoded envelope.
        request: The request, for the error.

    Returns:
        The `status` (`""` when absent) and the `request_id` as text (`_request_id`; `None`
        when absent).

    Raises:
        WireError: If `status` is not text, or `status` is `ERROR`. A `request_id` of any type
            is accepted: it is metadata and never fails a call.
        WireEntitlementError: If `status` is `NOT_AUTHORIZED`.
    """
    status = body.get("status", "")
    request_id = _request_id(body.get("request_id"))
    if not isinstance(status, str):
        raise WireError(
            **request.context("HTTP 200", "the envelope's status is not text", status=200)
        )
    if status in {"NOT_AUTHORIZED", "ERROR"}:
        message = body.get("message") or body.get("error") or "no message"
        context = request.context(
            f"HTTP 200 with status {status}",
            str(message)[:_EXCERPT],
            status=200,
            request_id=request_id,
        )
        if status == "NOT_AUTHORIZED":
            raise WireEntitlementError(**context)
        raise WireError(**context)
    return status, request_id


def _result(body: dict[str, object], result_key: str) -> object:
    """Return the value at a dotted result key, or `None` when a step is missing or null.

    Args:
        body: The decoded envelope.
        result_key: The key, such as `"results"` or `"results.values"`.

    Returns:
        The value, or `None`.
    """
    value: object = body
    for step in result_key.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(step)
    return value


def decode_page(data: bytes, rest_path: RestPath, request: Request) -> Page:
    """Decode the body of a 200 response into a `Page`.

    Besides the errors below, a body with an `ERROR` status raises `WireError` and one with a
    `NOT_AUTHORIZED` status raises `WireEntitlementError`, for every path.

    Args:
        data: The body, which must be one UTF-8 JSON document.
        rest_path: The path requested; its `result_key` says where the rows are.
        request: The request, for errors.

    Returns:
        The page. Its `drift.statuses` holds the envelope status when it is neither a good
        nor an error status.

    Raises:
        WireError: If the body is not UTF-8 JSON (quoting its first 200 bytes), has the wrong
            shape, or lacks the result key under a status other than `OK` or `DELAYED`.
    """
    try:
        body = codec.loads(data)
    except ValueError as error:
        raise WireError(
            **request.context(
                "HTTP 200", f"the body is not JSON ({error}): {data[:_EXCERPT]!r}", status=200
            )
        ) from error
    if isinstance(body, list) and rest_path.result_key is None:
        return Page(status="", request_id=None, rows=_as_rows(body, request), body=body)
    if not isinstance(body, dict):
        raise WireError(**request.context("HTTP 200", "the body is not a JSON object", status=200))
    status, request_id = _envelope(body, request)
    drift = DriftReport(statuses=frozenset({status} - GOOD_STATUSES - {""}))
    if rest_path.result_key is None:
        row = {key: value for key, value in body.items() if key not in ENVELOPE_KEYS}
        return Page(status=status, request_id=request_id, rows=[row], body=body, drift=drift)
    value = _result(body, rest_path.result_key)
    if value is not None:
        rows = _as_rows(value, request)
    elif status in _EMPTY_STATUSES:
        rows = []
    else:
        raise WireError(
            **request.context(
                f"HTTP 200 with status {status or '(none)'}",
                f"the body has no {rest_path.result_key!r}",
                status=200,
                request_id=request_id,
            )
        )
    return Page(status=status, request_id=request_id, rows=rows, body=body, drift=drift)
