"""The three errors every Massive wire client raises.

The REST, WebSocket, and flat-file clients of `deephaven_massive._massive` report every failure
they cannot handle themselves as one of these errors, never as a urllib3, websockets, botocore,
or JSON exception:

- `WireError`: any failure: an HTTP status the client does not retry, retries that ran out
  (a persistent 429 is `WireError` with `status=429`), a connection or TLS failure, a body that
  is not JSON, a fatal WebSocket handshake, or a value that does not fit its declared type.
- `WireAuthError`: Massive rejected the credentials (HTTP 401, WebSocket `auth_failed`, or an
  S3 `InvalidAccessKeyId` or `SignatureDoesNotMatch`).
- `WireEntitlementError`: the credentials are valid but the plan does not cover the request
  (HTTP 403, a `NOT_AUTHORIZED` envelope, or any other S3 403).

Each carries the HTTP `status`, the `host` and `path` of the request, Massive's `request_id`,
and a `message`. The library outside `_massive` never shows these to users: `_errors.from_wire`
turns each one into the public `MassiveError`, `AuthenticationError`, or `EntitlementError`
with the same message, `status`, and `request_id`.

This module imports only the standard library. It is part of `net`, the layer every client
shares.
"""

from __future__ import annotations

__all__ = ["WireAuthError", "WireEntitlementError", "WireError"]


class WireError(Exception):
    """A failure talking to Massive, raised by a wire client.

    The base class of the wire errors, and the error for every failure that is neither an
    authentication nor an entitlement problem. A client builds one when it gives up on a
    request; the library converts it with `_errors.from_wire` before a user sees it. The
    context attributes (`message`, `status`, `host`, `path`, `request_id`) are read-only, so
    an instance can be handed between threads as a value (a WebSocket `Reject` carries one
    from the client's loop thread to the library's flush thread). Python's own exception
    attributes (`args`, `__traceback__`, `__cause__`, `__notes__`) stay writable, so one
    instance must not be raised on two threads at once.

    Example:
        A REST client giving up on a rate limit::

            from deephaven_massive._massive import WireError

            error = WireError(
                "HTTP 429 from GET /v3/trades/AAPL (request_id abc123): rate limited",
                status=429,
                host="api.massive.com",
                path="/v3/trades/AAPL",
                request_id="abc123",
            )
            assert str(error) == error.message
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        host: str | None = None,
        path: str | None = None,
        request_id: str | None = None,
    ) -> None:
        """Create a wire error.

        Args:
            message: The complete text a person reads: what failed, and the status, path, and
                request id when known. It is the error's `str()`. It never holds a secret.
            status: The HTTP status of the response, or `None` when there was no response (a
                connection failure, a decoding failure, a value of the wrong type).
            host: The host the request went to, such as `"api.massive.com"`, or `None`.
            path: The request's path, without its query's secrets: the REST path, the
                WebSocket path, or the S3 object key; `None` when there was no request.
            request_id: The `request_id` of Massive's response (`x-amz-request-id` for S3), or
                `None` when the response had none.
        """
        super().__init__(message)
        self._message = message
        self._status = status
        self._host = host
        self._path = path
        self._request_id = request_id

    @property
    def message(self) -> str:
        """The complete text of the error, as given when it was created and as `str()` shows."""
        return self._message

    @property
    def status(self) -> int | None:
        """The HTTP status of the response, or `None` when there was none."""
        return self._status

    @property
    def host(self) -> str | None:
        """The host the request went to, or `None`."""
        return self._host

    @property
    def path(self) -> str | None:
        """The request's path or S3 object key, without secrets, or `None`."""
        return self._path

    @property
    def request_id(self) -> str | None:
        """The `request_id` of Massive's response, or `None`."""
        return self._request_id


class WireAuthError(WireError):
    """Massive rejected the credentials.

    Raised for an HTTP 401, a WebSocket `auth_failed` reply, and an S3 `InvalidAccessKeyId` or
    `SignatureDoesNotMatch` error. Retrying with the same credentials cannot succeed, so the
    clients never retry it. `_errors.from_wire` turns it into `AuthenticationError`.

    Example:
        ::

            from deephaven_massive._massive import WireAuthError, WireError

            error = WireAuthError("HTTP 401 from GET /v3/trades/AAPL: unknown API key", status=401)
            assert isinstance(error, WireError)
    """


class WireEntitlementError(WireError):
    """The credentials are valid, but the plan does not cover the request.

    Raised for an HTTP 403, a 200 response whose envelope `status` is `NOT_AUTHORIZED`, and any
    S3 403 other than a credential error. `_errors.from_wire` turns it into `EntitlementError`.

    Example:
        ::

            from deephaven_massive._massive import WireEntitlementError

            error = WireEntitlementError(
                "HTTP 403 from GET /v3/quotes/AAPL: not entitled", status=403
            )
            assert error.status == 403
    """
