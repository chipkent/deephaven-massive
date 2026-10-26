"""The library's exception hierarchy.

Every error that Massive or the library itself reports derives from `MassiveError`, so one
`except dm.MassiveError` catches all of them. Two subclasses name the failures a user can act
on:

- `AuthenticationError`: the API key is missing or Massive rejected it, or Massive's S3
  endpoint rejected the flat-file credentials.
- `EntitlementError`: the key is valid, but the plan lacks the dataset, the WebSocket host, or
  the timeframe that was requested.

Parameter mistakes are not `MassiveError`s: a wrong *value* raises `ValueError` and a wrong
*type* (for example a plain string where an enum member is required) raises `TypeError`, both
synchronously on the calling thread.

Static verbs (`fetch()`, `history()`) raise these exceptions directly. Live tables report them
through `TablePublisher.publish_failure`, so the table fails where Deephaven users look, and the
failure is also logged at ERROR and shown in `m.streams()`.

The module also holds `log_once`, the one helper for facts that are logged a single time per
process (a codec choice, a detected unit, rate-limit advice).

The public names are re-exported by `deephaven_massive/__init__.py`; this module is private.
"""

from __future__ import annotations

import logging
import threading

__all__ = ["AuthenticationError", "EntitlementError", "MassiveError", "log_once"]


class MassiveError(Exception):
    """Base class of every error that Massive or this library reports.

    Raised directly for failures that are neither authentication nor entitlement problems: a
    transport error, an HTTP error other than 403, a rate limit that outlived the SDK's retries,
    a closed session, or a timestamp whose declared unit does not fit its magnitude. The message
    always carries the context needed to act on it (the host, the dataset, the server's text).

    Example:
        Catch every library failure in one place::

            import deephaven_massive as dm

            try:
                m = dm.MassiveSession()
            except dm.MassiveError as e:
                print(f"cannot start Massive: {e}")
    """


class AuthenticationError(MassiveError):
    """The API key is missing or Massive rejected it, or the S3 credentials were rejected.

    Raised by `MassiveConfig()` when neither `MASSIVE_API_KEY` nor `POLYGON_API_KEY` is set and
    no `api_key` was passed. A live table fails with this error when the WebSocket server
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


_logged_lock = threading.Lock()
_logged: set[str] = set()


def log_once(
    log: logging.Logger, key: str, message: str, *args: object, level: int = logging.INFO
) -> None:
    """Log a message the first time `key` is seen in the process, and never again.

    For facts worth stating once rather than per event: the JSON codec chosen, the unit the
    FMV magnitude rule detected, the rate-limit advice. Thread-safe: concurrent first calls
    log exactly once.

    Args:
        log: The logger to write to.
        key: Identifies the fact; later calls with the same key do nothing.
        message: The `%`-style message.
        *args: The message's arguments.
        level: The logging level; INFO by default.
    """
    with _logged_lock:
        if key in _logged:
            return
        _logged.add(key)
    log.log(level, message, *args)
