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
the host, the request target (whose query never holds a secret), and the `request_id`. Its
message is one of two sentences `RequestTarget` builds: `answered`, for an answer Massive gave,
quoting the first 200 characters of the server's `message` (or of the body, when it has none)
and never judging it; and `unreadable`, for an answer the library could not read, naming what
was wrong with it.

The functions are pure and safe from any thread. This module imports only the standard
library, `net`, and `spec`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING, TypedDict

from deephaven_massive._massive.net import codec
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
    WireFailure,
)
from deephaven_massive._massive.spec.types import ENVELOPE_KEYS, DriftReport

if TYPE_CHECKING:
    from deephaven_massive._massive.spec.types import RestPath

__all__ = ["GOOD_STATUSES", "Page", "RequestTarget", "body_excerpt", "decode_page", "http_error"]

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
class RequestTarget:
    """Where one request went, for naming it in errors and logs.

    Attributes:
        host: The REST host name, such as `"api.massive.com"`.
        target: The path and query requested, such as `"/v3/trades/AAPL?limit=50000"`. The
            client never puts a secret in a query, so the target is safe to show.
    """

    host: str
    target: str

    def answered(
        self,
        status: int,
        text: str,
        *,
        request_id: str | None,
        envelope_status: str | None = None,
    ) -> ErrorContext:
        """Return the arguments of a wire error for an answer Massive gave this request.

        The sentence is the same for every status, so no site judges whether a 429 or a 503
        is a refusal: the error's class says the rest. Massive's text is quoted as it is.

        Args:
            status: The HTTP status.
            text: Massive's text: the envelope's `message`, or the body's start when it has
                none; empty for an empty body.
            request_id: Massive's `request_id`, or `None`.
            envelope_status: The envelope's `status` when it, not the HTTP status, is the
                answer (`NOT_AUTHORIZED`, `ERROR`), or `None`.

        Returns:
            The arguments, as `WireError(**context)`; the message reads `"Massive answered
            GET /v3/trades/AAPL on api.massive.com with HTTP 403 (request_id abc123): "You
            are not entitled to this data"."`, with `and status NOT_AUTHORIZED` after the
            HTTP status when `envelope_status` is given, without the parenthesis when there
            is no `request_id`, and ending `and no message.` instead of the quotation when
            `text` is empty.
        """
        answer = (
            f"HTTP {status}"
            if envelope_status is None
            else f"HTTP {status} and status {envelope_status}"
        )
        quoted = f': "{text}".' if text else " and no message."
        return self._context(
            f"{self._answered(answer, request_id)}{quoted}", status=status, request_id=request_id
        )

    def unreadable(
        self, status: int, problem: str, *, request_id: str | None = None
    ) -> ErrorContext:
        """Return the arguments of a wire error for an answer the library could not read.

        Args:
            status: The HTTP status.
            problem: What was wrong with the answer, as a clause with no final period, such as
                `"the body is not a JSON object"`.
            request_id: Massive's `request_id`, or `None`.

        Returns:
            The arguments, as `WireError(**context)`; the message reads `"Massive answered GET
            /v3/trades/AAPL on api.massive.com with HTTP 200, and the library could not read
            the answer: the body is not a JSON object."`, with the `request_id` parenthesis
            after the status when there is one.
        """
        return self._context(
            f"{self._answered(f'HTTP {status}', request_id)}, and the library could not read "
            f"the answer: {problem}.",
            status=status,
            request_id=request_id,
        )

    def _answered(self, answer: str, request_id: str | None) -> str:
        """Return the clause that names the request and Massive's answer to it.

        Args:
            answer: The answer, such as `"HTTP 403"`.
            request_id: Massive's `request_id`, or `None`.

        Returns:
            `"Massive answered GET <target> on <host> with <answer>"`, then
            `" (request_id <id>)"` when there is one.
        """
        rid = f" (request_id {request_id})" if request_id else ""
        return f"Massive answered GET {self.target} on {self.host} with {answer}{rid}"

    def _context(self, message: str, *, status: int, request_id: str | None) -> ErrorContext:
        """Return the arguments of a wire error for this request, with its message.

        Args:
            message: The message.
            status: The HTTP status.
            request_id: Massive's `request_id`, or `None`.

        Returns:
            The arguments, as `WireError(**context)`.
        """
        return ErrorContext(
            message=message,
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
            batch, drift, marks = to_batch(page.rows, trades, required={"SipTimestamp"})
    """

    status: str
    request_id: str | None
    rows: list[dict[str, object]] = field(repr=False)
    body: dict[str, object] | list[object] = field(repr=False)
    drift: DriftReport = field(default_factory=DriftReport)

    @property
    def adjusted(self) -> bool | None:
        """Whether Massive says this page's prices are adjusted for splits: its `adjusted` echo.

        The aggregates (the range, grouped daily, and `/prev`) answer every request with the
        envelope field `adjusted`, "whether or not this response was adjusted for splits", so a
        caller can check that every page of a download is on the basis it asked for. Paths
        that document no such field (the indicators, the daily open-close) give None.

        Returns:
            The envelope's `adjusted` when it is a JSON boolean; None when the body is not an
            object or has no boolean `adjusted`.
        """
        if not isinstance(self.body, dict):
            return None
        echoed = self.body.get("adjusted")
        return echoed if isinstance(echoed, bool) else None


_BODY_EXCERPT = 2000
"""How many characters of a failed response's body the client's DEBUG line quotes."""


def body_excerpt(data: bytes | bytearray) -> str:
    """Return the start of a failed response's body as text, for the client's DEBUG line.

    Longer than an error's excerpt, so the line shows Massive's whole error envelope or the
    meaningful part of a proxy's page. The client has already cleaned the body of its secrets
    (`net.redaction`).

    Args:
        data: The body.

    Returns:
        The first 2,000 characters, with undecodable bytes replaced; empty for an empty body.
    """
    # A UTF-8 character is at most 4 bytes, so this many bytes hold the excerpt: a large body
    # is never decoded whole only to be cut.
    return bytes(data[: _BODY_EXCERPT * 4]).decode("utf-8", "replace")[:_BODY_EXCERPT]


def _excerpt(data: bytes | bytearray) -> str:
    """Return the start of a body as text, for an error message.

    Args:
        data: The body.

    Returns:
        The first 200 characters, with undecodable bytes replaced; empty for an empty body.
    """
    # A character is at most four UTF-8 bytes, so only the excerpt's bytes are decoded.
    return data[: _EXCERPT * 4].decode("utf-8", "replace")[:_EXCERPT]


def http_error(status: int, data: bytes | bytearray, request: RequestTarget) -> WireError:
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
    try:
        body = codec.loads(data)
    except ValueError:
        body = None
    text = _excerpt(data)
    request_id = None
    if isinstance(body, dict):
        message = body.get("message") or body.get("error")
        if isinstance(message, str) and message:
            text = message[:_EXCERPT]
        request_id = _request_id(body.get("request_id"))
    context = request.answered(status, text, request_id=request_id)
    if status == HTTPStatus.UNAUTHORIZED:
        return WireAuthError(**context)
    if status == HTTPStatus.FORBIDDEN:
        return WireEntitlementError(**context)
    return WireError(**context, kind=WireFailure.REFUSED)


def _as_rows(value: object, request: RequestTarget) -> list[dict[str, object]]:
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
            **request.unreadable(200, "the rows are neither a JSON object nor a list"),
            kind=WireFailure.UNREADABLE,
        )
    for index, row in enumerate(value):
        if not isinstance(row, dict):
            raise WireError(
                **request.unreadable(200, f"row {index} is not a JSON object"),
                kind=WireFailure.UNREADABLE,
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


def _envelope(body: dict[str, object], request: RequestTarget) -> tuple[str, str | None]:
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
            **request.unreadable(200, "the envelope's status is not text", request_id=request_id),
            kind=WireFailure.UNREADABLE,
        )
    if status in {"NOT_AUTHORIZED", "ERROR"}:
        message = body.get("message") or body.get("error") or ""
        context = request.answered(
            200, str(message)[:_EXCERPT], request_id=request_id, envelope_status=status
        )
        if status == "NOT_AUTHORIZED":
            raise WireEntitlementError(**context)
        raise WireError(**context, kind=WireFailure.REFUSED)
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


def decode_page(data: bytes | bytearray, rest_path: RestPath, request: RequestTarget) -> Page:
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
        WireError: If the body is not UTF-8 JSON (quoting its first 200 characters), has the
            wrong shape, or lacks the result key under a status other than `OK` or `DELAYED`;
            each of these is marked `unreadable`, while a status `ERROR` is Massive's answer
            and is not.
    """
    try:
        body = codec.loads(data)
    except ValueError as error:
        excerpt = _excerpt(data)
        problem = (
            "the body is empty"
            if not excerpt
            else f'the body is not JSON and starts "{excerpt}": {error}'
        )
        raise WireError(**request.unreadable(200, problem), kind=WireFailure.UNREADABLE) from error
    if isinstance(body, list) and rest_path.result_key is None:
        return Page(status="", request_id=None, rows=_as_rows(body, request), body=body)
    if not isinstance(body, dict):
        raise WireError(
            **request.unreadable(200, "the body is not a JSON object"),
            kind=WireFailure.UNREADABLE,
        )
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
        labelled = f"its status is {status!r}" if status else "it has no status"
        raise WireError(
            **request.unreadable(
                200,
                f"the body has no {rest_path.result_key!r} and {labelled}",
                request_id=request_id,
            ),
            kind=WireFailure.UNREADABLE,
        )
    return Page(status=status, request_id=request_id, rows=rows, body=body, drift=drift)
