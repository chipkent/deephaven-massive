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

Every `MassiveError` also says whose problem it is, through `cause`, an `ErrorCause`:

- `EXPECTED`, the default: a condition the library expects and reports, such as a refusal, a
  plan or rate limit, a network failure, or a closed session.
- `USER_DEFECT`: a defect in the user's code or input that the library caught, such as a symbol
  table that failed upstream; the message names what to fix.
- `LIBRARY_DEFECT`: a bug in the library, or data from Massive it cannot convert or read; the
  message ends with `LIBRARY_BUG`, which says how to report it.

The cause sets the level a failure is logged at: ERROR for a defect, so that a broad `except`
never hides one, and below ERROR for an expected condition (`_log_limits.log_failure`).
`library_bug` and `user_defect` build the two kinds of defect message, `cause_of` reads the
cause of any exception, and `failure_text` is the text a log record gives it.

The wire package `deephaven_massive._massive` raises its own errors (`WireError`,
`WireAuthError`, `WireEntitlementError`), and `from_wire` turns each into its public
counterpart here, keeping the message, `status`, and `request_id`. Users never see a wire error.

Parameter mistakes are not `MassiveError`s: a wrong *value* raises `ValueError` and a wrong
*type* (for example a plain string where an enum member is required) raises `TypeError`, both
synchronously on the calling thread.

Static verbs (`fetch()`, `history()`) read in the background by default: a failure after the
call has returned fails the verb's table instead of raising, while a parameter mistake or a
closed session still raises at the call. With `wait=True` they raise these exceptions
directly. A live table fails with one (its publisher's `publish_failure`), so the failure shows
where Deephaven users look; it is also logged, at the level its cause gives, and shown in
`m.streams()`.

The module also holds `PermanentError`, the internal base of the ingest errors a retry cannot
fix, which is not exported; its subclass `RowDataError`, which the ingest layer raises and
the tables layer names;
`LIBRARY_BUG`, the sentence that ends every message about a
defect of the library itself; and `raises_through`, the declaration that lets a test check
the `Raises:` section of a function whose errors come from the helpers it calls. The
rate-limited log helpers, `log_once` and `BoundedLog`, are in `_log_limits`.

The public names are re-exported by `deephaven_massive/__init__.py`; this module is private.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar, Final, TypeVar

from deephaven_massive._massive import WireAuthError, WireEntitlementError, WireError

if TYPE_CHECKING:
    from collections.abc import Callable


__all__ = [
    "LIBRARY_BUG",
    "RAISES_THROUGH",
    "AuthenticationError",
    "CutShortError",
    "EntitlementError",
    "ErrorCause",
    "MassiveError",
    "PermanentError",
    "RowDataError",
    "UnlistedPairError",
    "cause_of",
    "failure_text",
    "from_wire",
    "library_bug",
    "raises_through",
    "user_defect",
    "wire_cause",
]

RAISES_THROUGH: Final = "raises_through"
"""The attribute in which `raises_through` records a function's helpers."""

_F = TypeVar("_F", bound="Callable[..., object]")


LIBRARY_BUG: Final = (
    "This is a bug in deephaven-massive. Turn on dm.enable_debug_logging(), reproduce it, and "
    "report it with the log and m.streams() at "
    "https://github.com/chipkent/deephaven-massive/issues."
)
"""The sentence that ends every log record and error message about a defect of the library.

The wire package ends its own ERROR records with the same text, which it keeps a copy of since
it imports nothing else of the library."""


class ErrorCause(StrEnum):
    """Whose problem a `MassiveError` is, which tells you what to do about it.

    Read it from an error's `cause`. The library logs a defect at ERROR and an expected condition
    below ERROR. The values are the library's own names, not values Massive sends.

    Example:
        Report a bug in the library, and handle every other failure as your application's own::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch()
            except dm.MassiveError as e:
                if e.cause is dm.ErrorCause.LIBRARY_DEFECT:
                    print(f"please report: {e}")
                else:
                    print(f"failed: {e}")
    """

    EXPECTED = "expected"
    """A condition the library expects and reports, such as a refused request, a plan or rate
    limit, a network failure, or a closed session. Logged below ERROR."""

    USER_DEFECT = "user_defect"
    """A mistake in your code or input that the library caught, such as a table of symbols
    of yours that failed. Logged at ERROR, with a message that names the fix."""

    LIBRARY_DEFECT = "library_defect"
    """A bug in the library, or data from Massive it can't read. Logged at ERROR, with a
    message that says how to report it."""


class MassiveError(Exception):
    """The base class of every error Massive or the library reports.

    One `except dm.MassiveError` catches them all. The library raises `MassiveError` itself for any
    other failure: a network error, an HTTP error other than 401 or 403, a rate limit that outlasted
    the retries, or a closed session. The message names the host, the dataset, and Massive's own
    text. To branch on the failure, read `status`, such as 404 or 429, instead of the message.

    A `fetch()` or `history()` with `wait=True` raises the error. A live table, including the
    table `fetch()` and `history()` return by default, fails with it instead: the table shows the
    error, the log records it, and `m.streams()` lists it.

    Example:
        Catch every failure of the library, and branch on the HTTP status::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch()
            except dm.MassiveError as e:
                if e.status == 429:
                    print(f"slow down (request {e.request_id}): {e}")
    """

    default_cause: ClassVar[ErrorCause] = ErrorCause.EXPECTED
    """The `cause` of an error of this class created without one; a subclass whose every
    instance is a defect sets its own."""

    def __init__(
        self,
        *args: object,
        status: int | None = None,
        request_id: str | None = None,
        cause: ErrorCause | None = None,
    ) -> None:
        """Create the error.

        Args:
            *args: The exception's arguments, as for `Exception`; normally one message.
            status: The HTTP status of the response the error came from, or `None` when it did
                not come from an HTTP response.
            request_id: The `request_id` of Massive's response, or `None` when there was no
                response or it carried none.
            cause: Whose problem the error is, or `None` (absent) for the class's
                `default_cause`, which is `ErrorCause.EXPECTED` unless a subclass sets another.
        """
        super().__init__(*args)
        self._status = status
        self._request_id = request_id
        self._cause = self.default_cause if cause is None else cause

    @property
    def status(self) -> int | None:
        """The HTTP status of the response behind the error, such as 404 or 429, or `None`."""
        return self._status

    @property
    def request_id(self) -> str | None:
        """The `request_id` of Massive's response, for a support request to Massive, or `None`."""
        return self._request_id

    @property
    def cause(self) -> ErrorCause:
        """Whose problem the error is: expected, a mistake in your code, or a library bug."""
        return self._cause


class AuthenticationError(MassiveError):
    """Massive refused your keys: the API key is missing or rejected, or the S3 keys are.

    `dm.MassiveConfig()` raises it when `MASSIVE_API_KEY` isn't set and you pass no `api_key`. A
    live table fails with it when Massive's streaming server rejects the key. Every other stream on
    that connection fails too. Massive gives the same answer to a plan with no streams of the asset
    class, and the message says so. `history()` raises it when Massive's flat-file server rejects
    the S3 keys. The message then names `s3_access_key_id` and `s3_secret_access_key`.

    Example:
        ::

            import deephaven_massive as dm

            try:
                config = dm.MassiveConfig()
            except dm.AuthenticationError:
                config = dm.MassiveConfig(api_key="...")
    """


class EntitlementError(MassiveError):
    """Your key is valid, but your plan doesn't include what you asked for.

    Massive refused the request: a REST request with HTTP 403, a stream subscription, or a flat
    file. The library raises it too when Massive marks current data as delayed and its subscription
    isn't in `MassiveConfig.delayed_subscriptions`. The message then names the setting to change.
    Only the table that asked fails. Other tables on the same connection keep running.

    The message says what Massive refused, in Massive's own words. A refused flat-file `history()`
    also names `source=dm.HistorySource.REST` when REST can serve the read.

    Example:
        Read a day of trades, and report it when your plan lacks them::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                day = m.stocks.trades("AAPL").history("2025-06-06", "2025-06-06")
            except dm.EntitlementError as e:
                print(f"not on this plan: {e}")
    """


class PermanentError(MassiveError):
    """A failure that the same request would meet again, so a retry cannot fix it.

    Internal: not exported from `dm`, and only its subclasses are raised. They are the ingest
    errors whose cause is settled for the session or the request: a row that breaks its
    dataset's schema (`RowDataError`), a symbol table's row no request can carry
    (`SymbolRowError`), a crypto pair missing from the session's ticker list
    (`UnlistedPairError`), a symbol Massive has no data for under
    `unavailable_symbols=OnError.RAISE` (`UnavailableSymbolError`, `_ingest/rest.py`), and, in
    a read by symbol, a parameter a symbol's path does not take (`UnsupportedParameterError`,
    `_ingest/symbol_reads.py`).

    A `poll()` table fails with any of them except `UnsupportedParameterError` rather than
    retrying it every cycle. `UnsupportedParameterError` never reaches a `poll()` cycle: the
    read by symbol records it as that symbol's failure, so it fails only its own symbol and the
    read goes on with the others. `RowDataError` and `UnlistedPairError` are defined here,
    since the REST transport raises them and the tables layer names them; `SymbolRowError` is
    the poll module's (`_tables/poll.py`). The fatal set of `poll()` names only this base, so a
    subclass defined in a module the poll module cannot import (the ingest layer, such as
    `UnavailableSymbolError`) still fails the table.

    Example:
        ::

            from deephaven_massive._errors import MassiveError, PermanentError

            assert issubclass(PermanentError, MassiveError)
    """


def from_wire(error: WireError, *, cause: ErrorCause | None = None) -> MassiveError:
    """Return the public error for an error raised by the wire package.

    The REST, WebSocket, and flat-file transports call it on every `WireError` they do not
    handle themselves, then raise the result `from` the wire error (static verbs) or fail a
    table with it (`poll()` and streams). It creates a new exception and has no side effects.

    Args:
        error: The wire error.
        cause: Whose problem it is, or `None` (absent) to take it from the wire error:
            `ErrorCause.LIBRARY_DEFECT` for data the client could not read (`unreadable`), and
            `ErrorCause.EXPECTED` otherwise.

    Returns:
        `AuthenticationError` for a `WireAuthError`, `EntitlementError` for a
        `WireEntitlementError`, and `MassiveError` for any other `WireError`, with the wire
        error's message, `status`, and `request_id`, and `cause`.
    """
    public: type[MassiveError]
    if isinstance(error, WireAuthError):
        public = AuthenticationError
    elif isinstance(error, WireEntitlementError):
        public = EntitlementError
    else:
        public = MassiveError
    if cause is None:
        cause = wire_cause(error)
    return public(error.message, status=error.status, request_id=error.request_id, cause=cause)


def wire_cause(error: WireError) -> ErrorCause:
    """Return whose problem a wire error is, from what the wire client knows.

    Args:
        error: The wire error.

    Returns:
        `ErrorCause.LIBRARY_DEFECT` for data Massive delivered that the client could not read
        (`unreadable`) or a failure of the client itself (`client_defect`), and
        `ErrorCause.EXPECTED` for every failure to reach or hear from Massive.
    """
    if error.unreadable or error.client_defect:
        return ErrorCause.LIBRARY_DEFECT
    return ErrorCause.EXPECTED


def cause_of(error: BaseException) -> ErrorCause:
    """Return whose problem an exception the library caught is.

    Args:
        error: What was caught.

    Returns:
        A `MassiveError`'s `cause`; `ErrorCause.LIBRARY_DEFECT` for any other `Exception`,
        which the library does not expect from its own code; `ErrorCause.EXPECTED` for a
        `BaseException` that is not an `Exception`, such as `KeyboardInterrupt`, which is no
        defect.
    """
    if isinstance(error, MassiveError):
        return error.cause
    if isinstance(error, Exception):
        return ErrorCause.LIBRARY_DEFECT
    return ErrorCause.EXPECTED


def library_bug(message: str) -> str:
    """Return a message about a defect of the library, ended with how to report it.

    Args:
        message: What went wrong, such as `"the flush thread died"`; a trailing period is
            added when it ends without one.

    Returns:
        `message` followed by `LIBRARY_BUG`.
    """
    stop = "" if message.endswith((".", "!", "?")) else "."
    return f"{message}{stop} {LIBRARY_BUG}"


def user_defect(problem: str, fix: str) -> str:
    """Return a message about a defect in the user's code or input, naming what to fix.

    It never asks for a bug report: the fix is the user's.

    Args:
        problem: What failed and where, naming the user's table or code, such as `"the symbol
            table failed upstream: <cause>"`.
        fix: What the user does about it, such as `"fix that table and call the method
            again"`.

    Returns:
        `"<problem>; <fix>"`.
    """
    return f"{problem}; {fix}"


def failure_text(error: BaseException) -> str:
    """Return the text a log record gives a failure, with how to report a library defect.

    Args:
        error: The failure.

    Returns:
        `str(error)` for an expected failure or a user defect (whose message names the fix);
        for a library defect, the message ended with `LIBRARY_BUG` unless it already holds it,
        and an exception that is not a `MassiveError` named by its type.
    """
    text = str(error) if isinstance(error, MassiveError) else f"{type(error).__name__}: {error}"
    if cause_of(error) is not ErrorCause.LIBRARY_DEFECT or LIBRARY_BUG in text:
        return text
    return library_bug(text)


def raises_through(*helpers: Callable[..., object]) -> Callable[[_F], _F]:
    """Declare the helpers whose errors a function lets through, so its `Raises:` can be checked.

    Most public functions of the library check nothing themselves: a dataset method passes its
    arguments to `build_query`, and a verb runs through the session's guard and a transport.
    Their `Raises:` sections document the errors of those helpers, which a linter that reads
    only the function's own `raise` statements cannot check. This decorator names the helpers,
    and `tests/test_errors.py` requires each declared function's `Raises:` section to list
    exactly the exceptions it raises itself and those its helpers' `Raises:` sections list: a
    helper's error the function leaves out fails the test, and so does a listed error that
    nothing raises.

    The decorator stores the helpers in the function's `raises_through` attribute
    (`RAISES_THROUGH`) and returns the same function, so it costs nothing at call time.
    `functools.wraps` copies the attribute to a wrapper, so a decorated verb keeps it. Apply it
    beneath any wrapping decorator. It runs once, at import, and is thread-safe.

    Args:
        *helpers: The functions or methods the decorated function calls whose exceptions it
            lets through, such as `build_query` or `SessionCore.run_verb`. A helper's own
            `Raises:` section is what counts.

    Returns:
        A decorator that records `helpers` on a function and returns it unchanged.

    Example:
        A dataset method lets through the errors of the factory that checks its arguments::

            from deephaven_massive._errors import RAISES_THROUGH, raises_through
            from deephaven_massive.namespaces._query_building import build_query
            from deephaven_massive.queries import StreamHistoryQuery


            @raises_through(build_query)
            def trades(self, symbols):
                return build_query(
                    self._core, TRADES, query_type=StreamHistoryQuery, symbols=symbols
                )


            assert getattr(trades, RAISES_THROUGH) == (build_query,)
    """

    def declare(function: _F) -> _F:
        vars(function)[RAISES_THROUGH] = helpers
        return function

    return declare


class CutShortError(MassiveError):
    """A symbol's REST data cut short: Massive answered HTTP 404 for a later page of its pages.

    Internal: not exported from `dm`; `fetch()` and `history()` raise it as the `MassiveError`
    it is, with status 404. The REST transport raises it, and a read by symbol
    (`_ingest/symbol_reads.py`) names it: unlike any other 404, which the same request would
    meet again, a page that vanished mid-read may be there when the symbol is read again.
    """


class RowDataError(PermanentError):
    """Rows from Massive that break their dataset's schema.

    A REST page whose every row was dropped because a key could not be read (one odd row is
    dropped and counted, never raised), a symbol spelling the library cannot read under
    `unreadable_symbols=RAISE`, a flat-file line that is not a readable row under
    `malformed_rows=RAISE`, or, in a `poll()` cycle, a null or NaN value in one of the
    dataset's poll keys. A `fetch()` or `history()` table fails with it like any `MassiveError`,
    and a call with `wait=True` raises it; a `poll()` table fails with it, since the next cycle
    would fail alike. Data from Massive that the library cannot read is the library's to fix, so
    its cause is `ErrorCause.LIBRARY_DEFECT`.
    """

    default_cause = ErrorCause.LIBRARY_DEFECT


class UnlistedPairError(PermanentError):
    """A crypto ticker whose two currencies Massive's reference data does not give.

    A path that takes the pair as two currencies (`/v1/last/crypto/{from}/{to}`,
    `/v1/open-close/crypto/{from}/{to}/{date}`) needs the split, which only Massive's reference
    data gives; the session's crypto ticker list, read again on a miss at most once an hour,
    does not list the ticker, or gives currencies that do not join to it
    (`RestReader.crypto_pair` in `_ingest/rest.py`). Such a pair is one Massive has
    no data for, and follows `MassiveConfig.unlisted_symbols`, whether passed in code or in a
    table: under `LOG` and `IGNORE`, `fetch()` and each `poll()` cycle leave it out, and a
    followed read marks it `FAILED`; under `RAISE`, a `fetch()`, `poll()`, or followed table
    fails with it, and `fetch(wait=True)` raises it.
    """
