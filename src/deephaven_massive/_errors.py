"""The library's exception hierarchy.

Every error that Massive or the library itself reports derives from `MassiveError`, so one
`except dm.MassiveError` catches all of them. Two subclasses name the failures a user can act
on:

- `AuthenticationError`: the API key is missing or Massive rejected it, or Massive's S3
  endpoint rejected the flat-file credentials.
- `EntitlementError`: the key is valid, but the plan lacks the dataset, the WebSocket host, or
  the timeframe that was requested.

Every `MassiveError` carries two read-only attributes, set when the error came from an HTTP
response and `None` otherwise: `status`, the HTTP status, and `request_id`, the `request_id` of
Massive's response. Branch on `status` (for example 404 or 429) instead of parsing messages.

The wire package `deephaven_massive._massive` raises its own errors (`WireError`,
`WireAuthError`, `WireEntitlementError`), and `from_wire` turns each into its public
counterpart here, keeping the message, `status`, and `request_id`. Users never see a wire error.

Parameter mistakes are not `MassiveError`s: a wrong *value* raises `ValueError` and a wrong
*type* (for example a plain string where an enum member is required) raises `TypeError`, both
synchronously on the calling thread.

Static verbs (`fetch()`, `history()`) raise these exceptions directly. Live tables report them
through `TablePublisher.publish_failure`, so the table fails where Deephaven users look, and the
failure is also logged at ERROR and shown in `m.streams()`.

The module also holds `PermanentError`, the internal base of the ingest errors a retry cannot
fix, which is not exported, and `LIBRARY_BUG`, the sentence that ends every message about a
defect of the library itself. The rate-limited log helpers, `log_once` and `BoundedLog`, are in
`_log_limits`.

The public names are re-exported by `deephaven_massive/__init__.py`; this module is private.
"""

from __future__ import annotations

from typing import Final

from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError

__all__ = [
    "LIBRARY_BUG",
    "AuthenticationError",
    "EntitlementError",
    "MassiveError",
    "PermanentError",
    "from_wire",
]


LIBRARY_BUG: Final = (
    "This is a bug in deephaven-massive. Turn on dm.enable_debug_logging(), reproduce it, and "
    "report it with the log and m.streams() at "
    "https://github.com/chipkent/deephaven-massive/issues."
)
"""The last sentence of every message about a defect of the library itself, never the user's:
how to report it, with what."""


class MassiveError(Exception):
    """Base class of every error that Massive or this library reports.

    Raised directly for failures that are neither authentication nor entitlement problems: a
    transport error, an HTTP error other than 401 or 403, a rate limit that outlived the
    retries, a closed session, or a timestamp whose declared unit does not fit its magnitude.
    The message always carries the context needed to act on it (the host, the dataset, the
    server's text). `status` and `request_id` are read-only and safe to read from any thread.

    Example:
        Catch every library failure in one place, and branch on the HTTP status::

            import deephaven_massive as dm

            try:
                raise dm.MassiveError("rate limited", status=429, request_id="abc123")
            except dm.MassiveError as e:
                if e.status == 429:
                    print(f"slow down (request {e.request_id}): {e}")
    """

    def __init__(
        self, *args: object, status: int | None = None, request_id: str | None = None
    ) -> None:
        """Create the error.

        Args:
            *args: The exception's arguments, as for `Exception`; normally one message.
            status: The HTTP status of the response the error came from, or `None` when it did
                not come from an HTTP response.
            request_id: The `request_id` of Massive's response, or `None` when there was no
                response or it carried none.
        """
        super().__init__(*args)
        self._status = status
        self._request_id = request_id

    @property
    def status(self) -> int | None:
        """The HTTP status of the response the error came from (such as 404 or 429), or `None`."""
        return self._status

    @property
    def request_id(self) -> str | None:
        """The `request_id` of Massive's response, for a support request, or `None`."""
        return self._request_id


class AuthenticationError(MassiveError):
    """The API key is missing or Massive rejected it, or the S3 credentials were rejected.

    Raised by `MassiveConfig()` when `MASSIVE_API_KEY` is not set and no `api_key` was
    passed. A live table fails with this error when the WebSocket server
    answers `auth_failed`; every stream on that connection fails with it. `history()` raises it
    when Massive's S3 endpoint rejects the flat-file credentials (`InvalidAccessKeyId`,
    `SignatureDoesNotMatch`); the message names `s3_access_key_id` and `s3_secret_access_key`.

    Example:
        ::

            import deephaven_massive as dm

            try:
                cfg = dm.MassiveConfig()
            except dm.AuthenticationError:
                cfg = dm.MassiveConfig(api_key="...")
    """


class EntitlementError(MassiveError):
    """The key is valid, but the plan does not include what was requested.

    Raised when Massive answers HTTP 403 (`NOT_AUTHORIZED`) to a REST request, when a WebSocket
    subscription is refused with an explicit error for its topic, or when a plan has no
    WebSocket access for an asset class at all. The message names the nearest entitled
    alternative when one exists, for example `m.stocks.snapshots(symbols).poll(15)` for delayed
    NBBO on a plan without real-time quotes. Only the affected table fails; other tables on the
    same connection keep running.

    Example:
        ::

            import deephaven_massive as dm

            # A plan gap is a MassiveError, so one handler can catch every library failure.
            assert issubclass(dm.EntitlementError, dm.MassiveError)
            try:
                raise dm.EntitlementError("NBBO quotes are not on this plan")
            except dm.EntitlementError as e:
                print(f"not on this plan: {e}")
    """


class PermanentError(MassiveError):
    """A failure that the same request would meet again, so a retry cannot fix it.

    Internal: not exported from `dm`, and only its subclasses are raised. They are the ingest
    errors whose cause is settled for the session or the request: a row that breaks its
    dataset's schema (`RowDataError`), a parameter the chosen path does not take
    (`UnsupportedParameterError`), a symbol table's row that is not Massive's ticker
    (`SymbolRowError`), and a crypto pair missing from the session's ticker list
    (`UnlistedPairError`). A `poll()` table fails with one rather than retrying it every cycle.
    The subclasses are defined in the ingest modules (`_ingest/poll.py`, `_ingest/rest.py`),
    and the fatal set of `poll()` names only this base, so a subclass defined in a module the
    poll module cannot import (`rest.py` imports `poll.py`) still fails the table.

    Example:
        ::

            from deephaven_massive._errors import MassiveError, PermanentError

            assert issubclass(PermanentError, MassiveError)
    """


def from_wire(error: WireError) -> MassiveError:
    """Return the public error for an error raised by the wire package.

    The REST, WebSocket, and flat-file transports call it on every `WireError` they do not
    handle themselves, then raise the result `from` the wire error (static verbs) or fail a
    table with it (`poll()` and streams). It creates a new exception and has no side effects.

    Args:
        error: The wire error.

    Returns:
        `AuthenticationError` for a `WireAuthError`, `EntitlementError` for a
        `WireEntitlementError`, and `MassiveError` for any other `WireError`, with the wire
        error's message, `status`, and `request_id`.
    """
    public: type[MassiveError]
    if isinstance(error, WireAuthError):
        public = AuthenticationError
    elif isinstance(error, WireEntitlementError):
        public = EntitlementError
    else:
        public = MassiveError
    return public(error.message, status=error.status, request_id=error.request_id)
