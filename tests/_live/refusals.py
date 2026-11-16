"""Massive's refusals as a live test reports them: one line with the status, text, and request.

A live test sends its requests whatever the key holds, and when Massive refuses one, the test
skips and quotes Massive. `refusal_line` words the quote, from the error's own message (the
wire errors name the status, the request, and Massive's text) and its status. A refusal of a
request reaching beyond the history the key keeps (`is_timeframe_refusal`) also names the
earliest day the request asked for (`refused_request`), so a reader can judge it. The library's
own refusal of a stream Massive serves only in real time, on a key whose data Massive labels
delayed, carries no status (`is_real_time_only_refusal`), so it is not Massive's refusal
(`is_massive_refusal`).

`tests/conftest.py` calls these from its hook and fixtures, and the live run's and tester's
example runners from their verdicts. Every function here is pure.
"""

from __future__ import annotations

import re
import urllib.parse
from datetime import date
from typing import TYPE_CHECKING, Final

from deephaven_massive import EntitlementError, ErrorSource
from deephaven_massive._massive import WireEntitlementError
from tests._live.classes import class_of_path

if TYPE_CHECKING:
    from deephaven_massive._errors import MassiveError
    from deephaven_massive._massive import WireError

TIMEFRAME_PHRASE: Final = "doesn't include this data timeframe"
"""The phrase of Massive's refusal of a request beyond the key's history, in lower case."""

REAL_TIME_ONLY_PHRASE: Final = "streams only in real time, and Massive labels this key's"
"""The library's words when it refuses a stream Massive serves only in real time (fair market
value, the options admin messages) because Massive labels the key's data of its subscription
delayed, before the stream opens (`host_decision.require_real_time`)."""

_REQUEST_TARGET: Final = re.compile(r" from GET (\S+) on ")
"""Where a wire error's message names the request's target (path and query)."""

_ISO_DATE: Final = re.compile(r"\d{4}-\d{2}-\d{2}")
"""An ISO date in a request's target."""


def is_timeframe_refusal(error: BaseException) -> bool:
    """Return whether a refusal says the request reaches beyond the key's history.

    Massive words it "Your plan doesn't include this data timeframe". The phrase is matched in
    the message, never a single word: a request's target is percent-encoded and holds no
    spaces, so the phrase cannot come from a query string. A typographic apostrophe matches
    too.

    Args:
        error: The refusal.

    Returns:
        True for a refusal of the request's timeframe.
    """
    return TIMEFRAME_PHRASE in str(error).lower().replace("\u2019", "'")


def is_massive_refusal(error: BaseException) -> bool:
    """Return whether an error is Massive's refusal of a request, not one the library made.

    Args:
        error: The error.

    Returns:
        True for the wire's `WireEntitlementError` (an HTTP 403, a `NOT_AUTHORIZED` envelope,
        an S3 403), or an `EntitlementError` whose `source` is `ErrorSource.MASSIVE` and that
        carries Massive's `status`; False for any other error, the library's own refusals
        included, such as a real-time-only stream's, which carries no status.
    """
    if isinstance(error, WireEntitlementError):
        return True
    return (
        isinstance(error, EntitlementError)
        and error.source is ErrorSource.MASSIVE
        and error.status is not None
    )


def is_real_time_only_refusal(error: BaseException) -> bool:
    """Return whether an error is the library's refusal of a real-time-only stream.

    The library raises it as the stream opens, with no status, when Massive labels the key's
    data of the stream's subscription delayed: Massive has no delayed feed of the dataset, so
    the plan does not include it.

    Args:
        error: The error the stream's verb raised.

    Returns:
        True for an `EntitlementError` with no status whose message holds
        `REAL_TIME_ONLY_PHRASE`.
    """
    return (
        isinstance(error, EntitlementError)
        and error.status is None
        and REAL_TIME_ONLY_PHRASE in str(error)
    )


def refused_request(error: BaseException) -> tuple[str | None, date | None]:
    """Read the class and the earliest day a refused REST request asked for.

    The wire error's message reads `"<what> from GET <target> on <host>...: <detail>"`; the
    class comes from the target's path (`classes.class_of_path`) and the day from the ISO dates
    in the target.

    Args:
        error: The refusal.

    Returns:
        The class (None for data of no class, or a message that names no target) and the
        earliest ISO date the target names (None when it names none).
    """
    found = _REQUEST_TARGET.search(str(error))
    if found is None:
        return None, None
    target = urllib.parse.unquote(found.group(1))
    days = [date.fromisoformat(text) for text in _ISO_DATE.findall(target)]
    return class_of_path(target.partition("?")[0]), min(days, default=None)


def refusal_line(error: WireError | MassiveError) -> str:
    """Word Massive's refusal on one line: its status, its text, and the request.

    The message of a wire error, and of the library's error made from one, already names the
    status, the request, and Massive's text, as `"HTTP 403 from GET <target> on <host>
    (request_id ...): <text>"`; the line keeps it whole, its whitespace folded into single
    spaces. A message that does not name the status (a WebSocket reply, an S3 answer) gets it
    in front. A timeframe refusal ends with the earliest day its request asked for.

    Args:
        error: The refusal: a `WireEntitlementError`, or an `EntitlementError` whose source is
            Massive.

    Returns:
        Such as `"HTTP 403 from GET /v3/quotes/AAPL on api.massive.com (request_id 1): not
        entitled"`, or for a timeframe refusal `"...: Your plan doesn't include this data
        timeframe; earliest day asked: 2023-06-05"`.
    """
    line = " ".join(str(error).split())
    if error.status is not None and f"HTTP {error.status}" not in line:
        line = f"HTTP {error.status}: {line}"
    if is_timeframe_refusal(error):
        _, asked = refused_request(error)
        if asked is not None:
            line = f"{line}; earliest day asked: {asked}"
    return line
