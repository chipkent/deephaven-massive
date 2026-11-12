"""The library's exception hierarchy.

Every error that Massive or the library itself reports derives from `MassiveError`, so one
`except dm.MassiveError` catches all of them. Three subclasses name the failures a user can act
on:

- `AuthenticationError`: the API key is missing or Massive rejected it, or Massive's S3
  endpoint rejected the flat-file credentials.
- `EntitlementError`: the key is valid, but the plan lacks the dataset, the WebSocket host, or
  the timeframe that was requested.
- `SessionClosedError`: the session is closed, so a call on it, or a live table of it, can do
  nothing more.

Every `MassiveError` carries two read-only attributes, set when the error came from an HTTP
response and `None` otherwise: `status`, the HTTP status, and `request_id`, the `request_id` of
Massive's response. Branch on `status` (for example 404 or 429) instead of parsing messages.

Three more attributes tell code what to do about an error:

- `source`, an `ErrorSource`: what stopped the call. `MASSIVE`, the default, when Massive
  answered with a refusal or an error; `NETWORK` when no answer came through the library's
  retries; `USER` when the user's code or settings stopped it; and `LIBRARY` for a bug in the
  library, data from Massive it cannot read included.
- `is_retryable`: whether the same call may succeed later. True for a status in
  `RETRYABLE_STATUSES` (429, 500, 502, 503, 504) and for a `NETWORK` error; false for any
  other unless the raising code says otherwise, and always false for a bug.
- `is_bug`: whether a defect caused the error. Always true for `LIBRARY`; true for a user's
  bug the library caught, such as a symbol table that failed upstream, whose class or raise
  site says so.

`is_bug` sets the level a failure is logged at: ERROR for a bug, so that a broad `except` never
hides one, and below ERROR otherwise (`_log_limits.log_failure`). `library_bug` and
`user_defect` build the two kinds of bug message, `source_of` and `is_bug_of` read the source
and the bug flag of any exception, and `failure_text` is the text a log record gives it.

The wire package `deephaven_massive._massive` raises its own errors (`WireError`,
`WireAuthError`, `WireEntitlementError`), and `from_wire` turns each into its public
counterpart here, keeping the message, `status`, and `request_id`, and taking `source` from
what the wire error says (`wire_source`). Users never see a wire error.

Parameter mistakes are not `MassiveError`s: a wrong *value* raises `ValueError` and a wrong
*type* (for example a plain string where an enum member is required) raises `TypeError`, both
synchronously on the calling thread.

Static verbs (`fetch()`, `history()`) read in the background by default: a failure after the
call has returned fails the verb's table instead of raising, while a parameter mistake or a
closed session still raises at the call. With `wait=True` they raise these exceptions
directly. A live table fails with one (its publisher's `publish_failure`), so the failure shows
where Deephaven users look; it is also logged, at the level its `is_bug` gives, and shown in
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
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar, Final, TypeVar

from deephaven_massive._massive import (
    RETRYABLE_STATUSES,
    WireAuthError,
    WireEntitlementError,
    WireError,
    WireFailure,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


__all__ = [
    "LIBRARY_BUG",
    "RAISES_THROUGH",
    "AuthenticationError",
    "CutShortError",
    "EntitlementError",
    "ErrorSource",
    "MassiveError",
    "PermanentError",
    "RowDataError",
    "SessionClosedError",
    "UnlistedPairError",
    "copy_error",
    "every_symbol_failed",
    "failure_text",
    "from_wire",
    "is_bug_of",
    "library_bug",
    "raises_through",
    "source_of",
    "user_defect",
    "wire_source",
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


class ErrorSource(StrEnum):
    """What stopped a call that raised a `MassiveError`: Massive, the network, you, or a bug.

    Read it from an error's `source` to decide what to do. Massive's refusals and errors
    are `MASSIVE`, an answer that never came is `NETWORK`, a problem in your code or settings is
    `USER`, and a bug in the library is `LIBRARY`. Later versions may add members, so end a
    `match` on it with `case _:`. The values are the library's
    own names, not values Massive sends.

    Example:
        Tell a library bug from a problem in your code or settings::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch(wait=True)
            except dm.MassiveError as e:
                if e.source is dm.ErrorSource.LIBRARY:
                    print(f"please report: {e}")
                elif e.source is dm.ErrorSource.USER:
                    print(f"fix this: {e}")
                else:
                    print(f"failed: {e}")
    """

    MASSIVE = "massive"
    """Massive refused or failed the request: an HTTP error such as 403 or 503, a rejected key,
    a plan without the data, a WebSocket status message, or Massive's reference data lacking
    what you asked for. The message quotes Massive's answer."""

    NETWORK = "network"
    """No answer came: a connection failed through every retry the library made, or a live
    table got nothing for longer than `MassiveConfig.max_outage` (its stream's connection stayed
    down, or its `poll()` cycle received no page). A `poll()` whose cycles kept failing keeps its
    latest cycle's source instead."""

    USER = "user"
    """Your code or settings stopped the call, such as a missing API key, delayed data your
    `MassiveConfig` doesn't allow, a certificate or proxy setting that fails, a closed session,
    or a watchlist table of yours that failed. The message names the fix."""

    LIBRARY = "library"
    """A bug in the library, or data from Massive it can't read. The message says how to report
    it."""


class MassiveError(Exception):
    """The base class of every error Massive or the library reports.

    One `except dm.MassiveError` catches them all. The library raises `MassiveError` itself for
    other failures, such as a network error, an HTTP error other than 401 or 403, a rate limit
    that lasted longer than the read waits, or a bug in the library. The
    message names what failed, and quotes Massive's text when Massive answered. To branch on
    the failure, read `is_retryable`, `is_bug`, `source`, or `status` instead of the message.

    A `fetch()` or `history()` with `wait=True` raises the error. A live table, including the
    table `fetch()` and `history()` return by default, fails with it instead: the table shows the
    error, the log records it, and `m.streams()` lists it.

    Example:
        Catch any `MassiveError`, and branch on the HTTP status::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch(wait=True)
            except dm.MassiveError as e:
                if e.status == 429:
                    print(f"slow down (request {e.request_id}): {e}")
    """

    default_source: ClassVar[ErrorSource] = ErrorSource.MASSIVE
    """The `source` of an error of this class created without one; a subclass whose every
    instance has another source sets its own."""

    default_is_bug: ClassVar[bool] = False
    """Whether an error of this class created without `is_bug` is a bug; a subclass whose every
    instance is a user's bug sets it. A `LIBRARY` error is a bug whatever this says."""

    def __init__(
        self,
        *args: object,
        status: int | None = None,
        request_id: str | None = None,
        source: ErrorSource | None = None,
        is_retryable: bool | None = None,
        is_bug: bool | None = None,
    ) -> None:
        """Create the error.

        Args:
            *args: The exception's arguments, as for `Exception`; normally one message.
            status: The HTTP status of the response the error came from, or `None` when it did
                not come from an HTTP response.
            request_id: The `request_id` of Massive's response, or `None` when there was no
                response or it carried none.
            source: What stopped the call, or `None` (absent) for the class's
                `default_source`, which is `ErrorSource.MASSIVE` unless a subclass sets another.
            is_retryable: Whether the same call may succeed later, or `None` (absent) to
                decide from `status` and `source`: true for a status in `RETRYABLE_STATUSES` or
                a `NETWORK` source. A bug is never retryable, whatever this says.
            is_bug: Whether a defect caused the error, or `None` (absent) for the class's
                `default_is_bug`. A `LIBRARY` error is a bug whatever this says.
        """
        super().__init__(*args)
        self._status = status
        self._request_id = request_id
        self._source = self.default_source if source is None else source
        if is_bug is None:
            is_bug = self.default_is_bug
        self._is_bug = is_bug or self._source is ErrorSource.LIBRARY
        if is_retryable is None:
            is_retryable = status in RETRYABLE_STATUSES or self._source is ErrorSource.NETWORK
        self._is_retryable = is_retryable and not self._is_bug

    @property
    def status(self) -> int | None:
        """The HTTP status of the response behind the error, or `None` when there was none.

        Usually Massive's, such as 404 or 429. A flat file's comes from S3, and a proxy that
        refused its credentials gives its own 407.
        """
        return self._status

    @property
    def request_id(self) -> str | None:
        """The `request_id` of Massive's response, for a support request to Massive, or `None`."""
        return self._request_id

    @property
    def source(self) -> ErrorSource:
        """What stopped the call: Massive, the network, your code or settings, or a bug."""
        return self._source

    @property
    def is_retryable(self) -> bool:
        """Whether the same call may succeed if you make it again later.

        True for exactly these: a rate limit (HTTP 429); a failure on Massive's side (HTTP 500,
        502, 503, or 504); a `NETWORK` error; a flat file Massive has not published yet; pages
        of one read that disagreed on `adjusted`; a `poll()` stopped by cycles that kept
        failing, when its latest cycle's error is retryable; and a watchlist read that failed
        because every symbol failed, when every symbol's error is retryable. False for every
        other error, such as any other HTTP status, an `AuthenticationError`, an
        `EntitlementError`, a `SessionClosedError`, or a bug.

        The library has already retried a server failure or a lost connection a few times, and
        you see a rate limit only when its pause lasted longer than the read waits, so wait
        before you call again.
        """
        return self._is_retryable

    @property
    def is_bug(self) -> bool:
        """Whether a defect caused the error: a bug in the library, or one in your code.

        True for every `ErrorSource.LIBRARY` error; its message says how to report it. True too
        for a bug in your code that the library caught, such as a watchlist table that failed
        or a parameter one symbol's endpoint doesn't take; its message names the fix. The
        library logs a bug at ERROR, and any other error below ERROR.
        """
        return self._is_bug


class AuthenticationError(MassiveError):
    """Your keys are missing, or Massive refused them: the API key, or the S3 keys of flat files.

    `dm.MassiveConfig()` raises it when `MASSIVE_API_KEY` isn't set and you pass no `api_key`. A
    `fetch()` or `history()` with `wait=True` raises it when Massive rejects the API key (HTTP
    401), and so does `history(..., wait=True)` when Massive's flat-file server rejects the S3
    keys, whose message then names `s3_access_key_id` and `s3_secret_access_key`; without
    `wait=True`, the table fails with it. A live table fails with it when Massive's streaming
    server rejects the key, and so does every other stream on that connection. Massive gives
    the same answer to a plan with no streams of the asset class, and the message says so.
    Its `source` is `ErrorSource.USER` when the key is missing, and `ErrorSource.MASSIVE` when
    Massive refused it.

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

    Massive refused the request: a REST request with HTTP 403, a flat file, or a stream
    subscription, which fails only the table that asked while other tables on the same
    connection keep running. The library raises it too when Massive marks current data as
    delayed and its subscription isn't in `MassiveConfig.delayed_subscriptions`. The message
    then names the setting to change. Its `source` is `ErrorSource.MASSIVE` when Massive
    refused, and `ErrorSource.USER` when `delayed_subscriptions` refused delayed data.

    The message says what Massive refused, in Massive's own words. A refused flat-file `history()`
    also names `source=dm.HistorySource.REST` when REST can serve the read.

    Example:
        Read a day of trades, and report it when your plan lacks them::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            try:
                day = m.stocks.trades("AAPL").history("2025-06-06", "2025-06-06", wait=True)
            except dm.EntitlementError as e:
                print(f"not on this plan: {e}")
    """


class PermanentError(MassiveError):
    """A failure that the same request would meet again, so a retry cannot fix it.

    Internal: not exported from `dm`, and only its subclasses are raised. They are the ingest
    errors whose cause is settled for the session or the request: a row that breaks its
    dataset's schema (`RowDataError`), a symbol table's row that is not Massive's ticker
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


def from_wire(error: WireError, *, source: ErrorSource | None = None) -> MassiveError:
    """Return the public error for an error raised by the wire package.

    The REST, WebSocket, and flat-file transports call it on every `WireError` they do not
    handle themselves, then raise the result `from` the wire error (static verbs) or fail a
    table with it (`poll()` and streams). It creates a new exception and has no side effects.

    Args:
        error: The wire error.
        source: What stopped the call, or `None` (absent) to take it from the wire error
            (`wire_source`).

    Returns:
        `AuthenticationError` for a `WireAuthError`, `EntitlementError` for a
        `WireEntitlementError`, and `MassiveError` for any other `WireError`, with the wire
        error's message, `status`, `request_id`, and `source`, and retryable as
        `MassiveError` decides from them.
    """
    public: type[MassiveError]
    if isinstance(error, WireAuthError):
        public = AuthenticationError
    elif isinstance(error, WireEntitlementError):
        public = EntitlementError
    else:
        public = MassiveError
    return public(
        error.message,
        status=error.status,
        request_id=error.request_id,
        source=wire_source(error) if source is None else source,
    )


def wire_source(error: WireError) -> ErrorSource:
    """Return what stopped a call, from the kind of failure its wire error is.

    Args:
        error: The wire error.

    Returns:
        `ErrorSource.MASSIVE` for a refusal or an error Massive answered with
        (`WireFailure.REFUSED`); `ErrorSource.NETWORK` for a request that got no answer
        (`NO_ANSWER`); `ErrorSource.USER` for this machine's settings (`LOCAL`) or a request
        the caller stopped (`STOPPED`); and `ErrorSource.LIBRARY` for data the client could not
        read (`UNREADABLE`) or a failure of the client itself (`CLIENT_DEFECT`).
    """
    return _SOURCES[error.kind]


_SOURCES: Final = MappingProxyType(
    {
        WireFailure.REFUSED: ErrorSource.MASSIVE,
        WireFailure.NO_ANSWER: ErrorSource.NETWORK,
        WireFailure.LOCAL: ErrorSource.USER,
        WireFailure.STOPPED: ErrorSource.USER,
        WireFailure.UNREADABLE: ErrorSource.LIBRARY,
        WireFailure.CLIENT_DEFECT: ErrorSource.LIBRARY,
    }
)
"""The source of the public error of each kind of wire failure (`wire_source`)."""


def source_of(error: BaseException) -> ErrorSource:
    """Return what stopped a call, for any exception the library caught.

    Args:
        error: What was caught.

    Returns:
        A `MassiveError`'s `source`; `ErrorSource.LIBRARY` for any other `Exception`, which
        the library does not expect from its own code; `ErrorSource.USER` for a
        `BaseException` that is not an `Exception`, such as `KeyboardInterrupt`.
    """
    if isinstance(error, MassiveError):
        return error.source
    if isinstance(error, Exception):
        return ErrorSource.LIBRARY
    return ErrorSource.USER


def is_bug_of(error: BaseException) -> bool:
    """Tell whether a defect caused an exception the library caught, which logs it at ERROR.

    Args:
        error: What was caught.

    Returns:
        A `MassiveError`'s `is_bug`; True for any other `Exception`, which the library does not
        expect from its own code; False for a `BaseException` that is not an `Exception`, such
        as `KeyboardInterrupt`.
    """
    if isinstance(error, MassiveError):
        return error.is_bug
    return isinstance(error, Exception)


def copy_error(
    error: MassiveError, message: str | None = None, *, is_retryable: bool | None = None
) -> MassiveError:
    """Return a new error of the same class and attributes, for a thread of its own to raise.

    Args:
        error: The error to copy.
        message: The copy's message, or `None` (absent) for `error`'s own.
        is_retryable: The copy's `is_retryable`, or `None` (absent) for `error`'s own.

    Returns:
        A new instance of `type(error)` with the same `status`, `request_id`, `source`, and
        `is_bug`, and the given message and `is_retryable`.
    """
    return type(error)(
        str(error) if message is None else message,
        status=error.status,
        request_id=error.request_id,
        source=error.source,
        is_retryable=error.is_retryable if is_retryable is None else is_retryable,
        is_bug=error.is_bug,
    )


def every_symbol_failed(errors: Sequence[MassiveError]) -> MassiveError:
    """Return the error a read raises when every one of its symbols failed.

    The first symbol's error stands for them all, as a copy that is retryable only when every
    symbol's error is: calling again helps only if each symbol may then succeed.

    Args:
        errors: Each symbol's error, the first symbol's first; at least one.

    Returns:
        A copy of the first error (`copy_error`).
    """
    return copy_error(errors[0], is_retryable=all(e.is_retryable for e in errors))


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
    if source_of(error) is not ErrorSource.LIBRARY or LIBRARY_BUG in text:
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
    meet again, a page that vanished mid-read may be there when the symbol is read again, so
    a followed read reads the symbol again (`symbol_reads.failure_kind`). The error itself
    follows the rule for a 404 and is not `is_retryable`.
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
    its source is `ErrorSource.LIBRARY`, a bug.
    """

    default_source = ErrorSource.LIBRARY


class UnlistedPairError(PermanentError):
    """A crypto ticker whose two currencies Massive's reference data does not give.

    A path that takes the pair as two currencies (`/v1/last/crypto/{from}/{to}`,
    `/v1/open-close/crypto/{from}/{to}/{date}`) needs the split, which only Massive's reference
    data gives; the session's crypto ticker list, read again on a miss at most once an hour,
    does not list the ticker, or gives currencies that do not join to it
    (`RestReader.crypto_pair` in `_ingest/rest.py`). A pair of a symbol table follows
    `MassiveConfig.unlisted_symbols`: under `LOG` and `IGNORE`, `fetch()` and each `poll()`
    cycle leave it out, and a followed read marks it `FAILED`; under `RAISE`, a `fetch()`,
    `poll()`, or followed table fails with it, and `fetch(wait=True)` raises it. A pair passed in
    code always raises it at the call.
    """


class SessionClosedError(MassiveError):
    """The session is closed, so it reads nothing more.

    A call on a session after `m.close()` raises it, and so does a call that was running when
    the session closed. A live table that was running when the session closed fails with it.
    The message names the session, such as `"MassiveSession #1 is closed"`. Create a new
    `dm.MassiveSession()` to read again. Its `source` is `ErrorSource.USER`, and
    `is_retryable` is False.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            m.close()
            try:
                m.stocks.dividends("AAPL").fetch()
            except dm.SessionClosedError:
                m = dm.MassiveSession()
    """

    default_source = ErrorSource.USER
