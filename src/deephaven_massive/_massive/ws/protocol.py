"""Massive's WebSocket protocol as values: frames to send, frames received, failures to classify.

Everything here is a pure function or a loop-confined object with no I/O, so the protocol is
tested without a socket; `ws.client` owns the socket and calls in here:

- **Control frames.** `auth_frame` and `subscription_frames` build the text frames the client
  sends: `{"action":"auth","params":"<key>"}` and `{"action":"subscribe","params":"T.AAPL,Q.AAPL"}`
  with at most `MAX_TOPICS_PER_FRAME` topics each. They are `str`, so `websockets` sends text
  frames.
- **The handshake.** `handshake_status` reads the `connected` greeting and the reply to the
  authentication frame.
- **Received frames.** `FrameDecoder` decodes each frame once, with the `net.codec` JSON codec,
  into `Status` events and per-channel `Data`: rows keyed by long-form column, `Reject`s for the
  rows that fail validation, and the `DriftReport` of the channel's objects in the frame.
- **Failures.** `classify_failure` decides whether an exception from opening the socket is
  fatal or retryable, from websockets' own rule and two overrides, and how long a retryable
  one must wait at least.

Topics are exact strings throughout: nothing here matches one topic as a prefix of another.
Symbols are passed through as the wire spells them (`EUR/USD`, `BTC-USD`); mapping them to
Massive's canonical forms is the library's business.

This module imports the standard library, `websockets`, `net`, and `spec`.
"""

from __future__ import annotations

import email.utils
import json
import logging
import re
import ssl
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from http import HTTPStatus
from typing import TYPE_CHECKING, Literal

from websockets.client import process_exception
from websockets.exceptions import InvalidStatus

from deephaven_massive._massive.net import codec
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import DriftReport, Source, Transport, WireType

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

__all__ = [
    "MAX_TOPICS_PER_FRAME",
    "Data",
    "FrameDecoder",
    "Reject",
    "Status",
    "StatusKind",
    "auth_frame",
    "classify_failure",
    "handshake_status",
    "subscription_frames",
]

logger = logging.getLogger(__name__)

MAX_TOPICS_PER_FRAME = 1000
"""The most topics one subscribe or unsubscribe frame names.

Massive does not document a frame-size limit; 1,000 topics of options contracts make a frame
of about 25 KB, which is conservative.
"""

_INT64 = (-(2**63), 2**63 - 1)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# An ISO-8601 instant: whole seconds, an optional fraction of up to 9 digits, a required offset.
_ISO_INSTANT = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})"
)
_INT32 = (-(2**31), 2**31 - 1)
# How much of an offending value an error message quotes.
_QUOTE_LIMIT = 50
# How much of an undecodable frame a log message quotes.
_LOG_LIMIT = 200


class StatusKind(StrEnum):
    """What a status object received after authentication says."""

    ACK = "ACK"
    """The server acknowledged subscriptions ("subscribed to: T.AAPL")."""
    REFUSED = "REFUSED"
    """The server refused a request (`"status": "error"`), usually for lack of entitlement."""
    MAX_CONNECTIONS = "MAX_CONNECTIONS"
    """The account holds too many connections; the server is about to close this one."""
    OTHER = "OTHER"
    """Any other status, such as the acknowledgement of an unsubscription."""


@dataclass(frozen=True, kw_only=True)
class Status:
    """One status object the server sent after authentication.

    The client decodes each `{"ev": "status", ...}` object into one of these and hands it to
    its `on_status` callback; the library keeps the per-topic acknowledgement state from them.
    Immutable, so it may be passed between threads.

    Attributes:
        kind: What the status says.
        topics: For `ACK`, the topics listed after "subscribed to:", split at commas, in order;
            empty when the message cannot be parsed. For `REFUSED`, the topics of a known
            channel the message names (usually one); empty when it names none, which means
            the refusal applies to the whole request it answers. Empty for the other kinds.
        message: The server's `message` text, verbatim (empty when it sent none).

    Example:
        ::

            from deephaven_massive._massive import Status, StatusKind

            status = Status(kind=StatusKind.REFUSED, message="Real-time data requires a plan")
            assert status.realtime_verdict
    """

    kind: StatusKind
    topics: tuple[str, ...] = ()
    message: str = ""

    @property
    def realtime_verdict(self) -> bool:
        """Whether this is a refusal whose message mentions "real-time" (in any case).

        The library reads it as the server's verdict that the account lacks real-time access
        on this host, and may move the stream to the delayed host.
        """
        return self.kind is StatusKind.REFUSED and "real-time" in self.message.lower()


@dataclass(frozen=True, kw_only=True)
class Reject:
    """One received row that failed validation and is not delivered.

    The library fails the streams that would have received the row; the frame's other rows
    are delivered. Immutable, so it may be passed between threads.

    Attributes:
        channel: The channel the row arrived on, such as `"T"`.
        symbol: The row's symbol exactly as the wire spells it (`"EUR/USD"`, not `"C:EURUSD"`),
            read from the column `set_required` declared as the channel's symbol; `None` when
            no symbol column is declared or the row's value is missing or not text.
        error: Why the row was rejected: a missing required key or an unconvertible value,
            naming the field, the expected type, and the value.
    """

    channel: str
    symbol: str | None
    error: WireError


@dataclass(frozen=True, kw_only=True)
class Data:
    """One channel's data objects from one frame, decoded.

    Attributes:
        channel: The channel, such as `"T"`.
        rows: The valid rows in arrival order, each mapping every column of the channel's
            source to its value (`None` when the wire omitted it or sent null).
        rejects: The rows that failed validation, in arrival order.
        drift: The wire keys of these objects that the source does not declare, and the
            declared keys some object lacked.
    """

    channel: str
    rows: list[dict[str, object]]
    rejects: list[Reject]
    drift: DriftReport


def auth_frame(api_key: str) -> str:
    """Return the authentication frame. Never log it: it holds the key.

    Args:
        api_key: The Massive API key.

    Returns:
        `{"action":"auth","params":"<key>"}` as text.
    """
    return json.dumps({"action": "auth", "params": api_key}, separators=(",", ":"))


def subscription_frames(
    action: Literal["subscribe", "unsubscribe"], topics: Sequence[str]
) -> list[str]:
    """Return the frames that subscribe or unsubscribe `topics`, in order.

    Args:
        action: `"subscribe"` or `"unsubscribe"`.
        topics: Exact topics such as `"T.AAPL"` or `"A.*"`, in the order to send them.

    Returns:
        One text frame per `MAX_TOPICS_PER_FRAME` topics; empty when `topics` is empty.
    """
    return [
        json.dumps(
            {"action": action, "params": ",".join(topics[i : i + MAX_TOPICS_PER_FRAME])},
            separators=(",", ":"),
        )
        for i in range(0, len(topics), MAX_TOPICS_PER_FRAME)
    ]


def handshake_status(data: str | bytes) -> tuple[str, str]:
    """Read the status of a handshake frame: the greeting or the reply to authentication.

    Args:
        data: The frame, such as
            `[{"ev":"status","status":"connected","message":"Connected Successfully"}]`.

    Returns:
        The first status object's `status` and `message`, such as
        `("auth_success", "authenticated")`; the message is empty when it is absent.

    Raises:
        ValueError: If the frame is not JSON, or holds no status object with a text `status`.
    """
    value = codec.loads(data)
    for item in value if isinstance(value, list) else [value]:
        if isinstance(item, dict) and item.get("ev") == "status":
            status, message = item.get("status"), item.get("message", "")
            if isinstance(status, str):
                return status, message if isinstance(message, str) else ""
    raise ValueError(f"expected a status object, got {_quote(data, _LOG_LIMIT)}")


def classify_failure(error: Exception, *, host: str, path: str, now: float) -> WireError | float:
    """Decide whether a failure to open the socket is fatal, and how long a retry must wait.

    websockets' public rule, `websockets.client.process_exception`, decides: it retries an
    `OSError`, a timeout, an EOF during the upgrade, and HTTP 500, 502, 503, and 504, and
    treats everything else (HTTP 400 or 404, an invalid URI, ...) as fatal. Two cases override
    it: an HTTP 429 is always retried, after its `Retry-After`, and a TLS certificate failure
    is always fatal, although it is an `OSError`.

    Args:
        error: What opening the socket (TCP, TLS, proxy, HTTP upgrade) raised.
        host: The host connected to, for the error message.
        path: The URL path, such as `"/stocks"`, for the error message.
        now: The current time in seconds since the Unix epoch, to read an HTTP-date
            `Retry-After`.

    Returns:
        For a fatal failure, the `WireError` to report, naming the host and path: for a TLS
        certificate failure it names `SSL_CERT_FILE`, and for an HTTP status it carries
        `status`. For a retryable failure, the least wait in seconds before the next attempt:
        the `Retry-After` of an HTTP 429 (0.0 when absent or unreadable), and 0.0 otherwise,
        so the backoff alone decides.
    """
    if isinstance(error, ssl.SSLCertVerificationError):
        return WireError(
            f"TLS certificate verification failed for {host}: {error.verify_message}; "
            "set SSL_CERT_FILE to a bundle that trusts the server's certificate",
            host=host,
            path=path,
        )
    status = error.response.status_code if isinstance(error, InvalidStatus) else None
    if isinstance(error, InvalidStatus) and status == HTTPStatus.TOO_MANY_REQUESTS:
        return _retry_after(error.response.headers.get("Retry-After"), now)
    if process_exception(error) is None:
        return 0.0
    reason = f"HTTP {status}" if status is not None else f"{type(error).__name__}: {error}"
    return WireError(
        f"WebSocket connection to {host}{path} failed: {reason}",
        status=status,
        host=host,
        path=path,
    )


def _retry_after(value: str | None, now: float) -> float:
    """Return the seconds a `Retry-After` header asks to wait.

    Args:
        value: The header's value: delta-seconds (`"120"`) or an HTTP-date; `None` when absent.
        now: The current time in seconds since the Unix epoch.

    Returns:
        The wait in seconds, never negative; 0.0 when the header is absent, and 0.0 with a
        WARNING quoting it when it is unreadable.
    """
    if value is None:
        return 0.0
    value = value.strip()
    if value.isascii() and value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except ValueError:
        logger.warning("ignoring an unreadable Retry-After header: %r", value[:_LOG_LIMIT])
        return 0.0
    if when.tzinfo is None:  # an HTTP-date is always GMT; "-0000" parses as naive
        when = when.replace(tzinfo=UTC)
    return max(0.0, when.timestamp() - now)


def _quote(value: object, limit: int = _QUOTE_LIMIT) -> str:
    """Return the start of a value's text for a message.

    Args:
        value: Any decoded or raw value.
        limit: How many characters to keep.

    Returns:
        `repr(value)`, cut to `limit` characters.
    """
    return repr(value)[:limit]


class _UnconvertibleError(ValueError):
    """A wire value that does not fit its field's `WireType`; the message names the type."""


def _integer(value: object, bounds: tuple[int, int], expected: str) -> int:
    """Return an integer that is not a `bool` and lies within `bounds`.

    Args:
        value: The decoded JSON value.
        bounds: The inclusive range.
        expected: The type's name, for the error.

    Returns:
        The value.

    Raises:
        _UnconvertibleError: If the value is not such an integer.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise _UnconvertibleError(expected)
    if not bounds[0] <= value <= bounds[1]:
        raise _UnconvertibleError(f"{expected} in [{bounds[0]}, {bounds[1]}]")
    return value


def _long(value: object) -> int:
    """Convert a `LONG` or `EPOCH` value. An epoch keeps its count; the library applies its unit.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The value, an `int` within int64 and never a `bool`.
    """
    return _integer(value, _INT64, "an int64")


def _double(value: object) -> float:
    """Convert a `DOUBLE` value.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The number as a `float`.

    Raises:
        _UnconvertibleError: If the value is a `bool` or not a number, or an integer too large
            for a double.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _UnconvertibleError("a number")
    try:
        return float(value)
    except OverflowError:  # an integer of more than about 308 digits
        raise _UnconvertibleError("a number within the range of a double") from None


def _bool(value: object) -> bool:
    """Convert a `BOOL` value.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The value.

    Raises:
        _UnconvertibleError: If the value is not `true` or `false` (a number never is).
    """
    if not isinstance(value, bool):
        raise _UnconvertibleError("true or false")
    return value


def _string(value: object) -> str:
    """Convert a `STRING` value.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The value.

    Raises:
        _UnconvertibleError: If the value is not text (a number never is).
    """
    if not isinstance(value, str):
        raise _UnconvertibleError("text")
    return value


def _date(value: object) -> date:
    """Convert a `DATE` value.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The calendar date.

    Raises:
        _UnconvertibleError: If the value is not `YYYY-MM-DD` text (week dates and the basic
            format are refused too).
    """
    expected = "a date as YYYY-MM-DD"
    if not isinstance(value, str) or len(value) != len("YYYY-MM-DD"):
        raise _UnconvertibleError(expected)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise _UnconvertibleError(expected) from None


def _instant(value: object) -> int:
    """Convert an `ISO_INSTANT` value to exact integer nanoseconds since the Unix epoch.

    The text must be `YYYY-MM-DDTHH:MM:SS`, an optional fraction of 1 to 9 digits, and an
    offset (`Z` or `+HH:MM`/`-HH:MM`). The fraction is read digit by digit, never through a
    float or a `datetime` (which keeps only microseconds), so nanoseconds survive exactly.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The instant as nanoseconds since 1970-01-01T00:00:00Z.

    Raises:
        _UnconvertibleError: If the value is not such text: no offset (its zone would be a
            guess), more than 9 fractional digits, or an impossible date or time.
    """
    expected = "an ISO-8601 date-time with an offset"
    match = _ISO_INSTANT.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise _UnconvertibleError(expected)
    seconds, fraction, offset = match.groups()
    try:
        whole = datetime.fromisoformat(seconds + ("+00:00" if offset == "Z" else offset))
    except ValueError:
        raise _UnconvertibleError(expected) from None
    elapsed = whole - _EPOCH
    total_seconds = elapsed.days * 86_400 + elapsed.seconds
    return total_seconds * 1_000_000_000 + int((fraction or "").ljust(9, "0"))


def _int_array(value: object) -> list[int]:
    """Convert an `INT_ARRAY` value.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The list of integers.

    Raises:
        _UnconvertibleError: If the value is not a list, or an element is a `bool`, not an
            integer, or outside int32.
    """
    if not isinstance(value, list):
        raise _UnconvertibleError("a list of int32")
    return [_integer(element, _INT32, "a list of int32") for element in value]


def _json_text(value: object) -> str:
    """Convert a `JSON_TEXT` value: any JSON value, kept as its compact JSON text.

    Args:
        value: The decoded JSON value, not `None`.

    Returns:
        The text, such as `'{"a":1}'`; a string value keeps its quotes.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


# Each wire type's converter. Every converter takes a non-null decoded JSON value and returns
# its Python form, or raises `_UnconvertibleError`; nothing is coerced.
_CONVERTERS: dict[WireType, Callable[[object], object]] = {
    WireType.LONG: _long,
    WireType.EPOCH: _long,
    WireType.DOUBLE: _double,
    WireType.BOOL: _bool,
    WireType.STRING: _string,
    WireType.DATE: _date,
    WireType.ISO_INSTANT: _instant,
    WireType.INT_ARRAY: _int_array,
    WireType.JSON_TEXT: _json_text,
}

# A word of a refusal message that may name a topic: a run of characters with no space,
# comma, semicolon, quote, bracket, or parenthesis.
_WORD = re.compile(r"""[^\s,;'"()\[\]]+""")


@dataclass(frozen=True, kw_only=True)
class _Step:
    """How one field of a channel is read from each object: computed once, used per row.

    Attributes:
        wire_key: The key on the wire.
        column: The long-form column it fills.
        convert: The converter of the field's `WireType`.
        required: Whether a missing value (or null) rejects the row.
    """

    wire_key: str
    column: str
    convert: Callable[[object], object]
    required: bool = False


@dataclass(frozen=True, kw_only=True)
class _Channel:
    """What the decoder knows about one channel: its source and the library's key choice.

    Attributes:
        source: The channel's generated source.
        declared: The source's wire keys.
        shape: The keys of an object that carries exactly the declared fields and `ev`; an
            object of that shape has no drift, which one set comparison decides.
        steps: One `_Step` per field, in the source's order.
        symbol: The wire key holding the symbol, or `None` when none is declared.
    """

    source: Source
    declared: frozenset[str]
    shape: frozenset[str]
    steps: tuple[_Step, ...]
    symbol: str | None = None

    @classmethod
    def plan(
        cls, source: Source, required: frozenset[str] = frozenset(), symbol: str | None = None
    ) -> _Channel:
        """Compute a channel's reading plan.

        Args:
            source: The channel's generated source.
            required: The wire keys whose absence rejects a row.
            symbol: The wire key holding the symbol, or `None`.

        Returns:
            The plan.
        """
        declared = frozenset(f.wire_key for f in source.fields)
        steps = tuple(
            _Step(
                wire_key=f.wire_key,
                column=f.column,
                convert=_CONVERTERS[f.wire_type],
                required=f.wire_key in required,
            )
            for f in source.fields
        )
        return cls(
            source=source,
            declared=declared,
            shape=declared | {"ev"},
            steps=steps,
            symbol=symbol,
        )


@dataclass(kw_only=True)
class _Accumulator:
    """One channel's decoded objects of the frame being decoded."""

    rows: list[dict[str, object]] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)
    extra: set[str] = field(default_factory=set)
    missing: set[str] = field(default_factory=set)


class FrameDecoder:
    """Decodes the frames of one connection against its channels' generated sources.

    The client creates one at construction from the sources it was given, and keeps it across
    reconnections, so the library's `set_required` choices survive them. Not thread-safe: only
    the client's loop thread uses it.

    Example:
        ::

            from deephaven_massive._massive.spec.types import Field, Source, Transport, WireType
            from deephaven_massive._massive.ws.protocol import FrameDecoder

            trades = Source(
                key="stocks/T",
                transport=Transport.WS,
                fields=(
                    Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
                    Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE),
                ),
            )
            decoder = FrameDecoder([trades], host="socket.massive.com", path="/stocks")
            decoder.set_required("T", frozenset({"Ticker"}), symbol="Ticker")
            [data] = decoder.decode('[{"ev":"T","sym":"AAPL","p":1}]')
            assert data.rows == [{"Ticker": "AAPL", "Price": 1.0}]
    """

    def __init__(self, sources: Iterable[Source], *, host: str, path: str) -> None:
        """Index the sources by channel.

        Args:
            sources: The WebSocket sources of the connection's cluster. Each key is
                `"<cluster>/<channel>"`, and the channel is the `ev` value its objects carry.
            host: The host of the connection, named in reject errors.
            path: The URL path of the connection, such as `"/stocks"`, named in reject errors.

        Raises:
            ValueError: If a source is not a WebSocket source, its key names no channel, or
                two sources name one channel.
        """
        self._host = host
        self._path = path
        self._channels: dict[str, _Channel] = {}
        self._unknown: set[str] = set()
        for source in sources:
            channel = source.key.partition("/")[2]
            if source.transport is not Transport.WS or not channel:
                raise ValueError(f"{source.key!r} is not a WebSocket source '<cluster>/<channel>'")
            if channel in self._channels:
                raise ValueError(f"two sources for channel {channel!r}")
            self._channels[channel] = _Channel.plan(source)

    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        """Declare a channel's required key columns and its symbol column, replacing any earlier.

        Args:
            channel: The channel, such as `"T"`.
            keys: Long-form columns of the channel's source; a row missing one (or holding null)
                is rejected. Empty means no column is required.
            symbol: The long-form column holding the row's symbol, read raw into each
                `Reject.symbol`; `None` when the channel's rows carry no symbol.

        Raises:
            ValueError: If the channel has no source, or `keys` or `symbol` names a column the
                source lacks.
        """
        current = self._channels.get(channel)
        if current is None:
            raise ValueError(f"no source for channel {channel!r}")
        by_column = {f.column: f.wire_key for f in current.source.fields}
        unknown = sorted((keys | {symbol} if symbol is not None else keys) - by_column.keys())
        if unknown:
            raise ValueError(f"source {current.source.key!r} has no columns {unknown}")
        self._channels[channel] = _Channel.plan(
            current.source,
            frozenset(by_column[k] for k in keys),
            None if symbol is None else by_column[symbol],
        )

    def decode(self, data: str | bytes) -> list[Status | Data]:
        """Decode one frame into its events, in order.

        Each status object becomes a `Status` where it appears. A channel's data objects become
        one `Data`, placed where the channel's first object appears. A frame that is not JSON,
        an element that is not an object with a text `ev`, and the objects of a channel with no
        source are dropped and logged (ERROR, ERROR, and WARNING once per channel); none of
        them stops the other events.

        Args:
            data: The frame's payload.

        Returns:
            The events; empty for a frame that carried none.
        """
        try:
            value = codec.loads(data)
        except ValueError:
            logger.exception(
                "dropped a frame from %s%s that is not JSON: %s",
                self._host,
                self._path,
                _quote(data, _LOG_LIMIT),
            )
            return []
        order: list[Status | str] = []
        frame: dict[str, _Accumulator] = {}
        for item in value if isinstance(value, list) else [value]:
            if not isinstance(item, dict) or not isinstance(ev := item.get("ev"), str):
                logger.error(
                    "dropped an element of a frame from %s%s with no text 'ev': %s",
                    self._host,
                    self._path,
                    _quote(item, _LOG_LIMIT),
                )
            elif ev == "status":
                order.append(self._status(item))
            elif ev not in self._channels:
                if ev not in self._unknown:
                    self._unknown.add(ev)
                    logger.warning(
                        "dropping objects of channel %r from %s%s, which has no source",
                        ev,
                        self._host,
                        self._path,
                    )
            else:
                if ev not in frame:
                    frame[ev] = _Accumulator()
                    order.append(ev)
                self._row(ev, item, frame[ev])
        return [event if isinstance(event, Status) else self._data(event, frame) for event in order]

    def _status(self, item: dict[str, object]) -> Status:
        """Decode one status object.

        Args:
            item: The object, whose `ev` is `"status"`.

        Returns:
            The status: `ACK` for a `success` whose message starts "subscribed to", `REFUSED`
            for an `error`, `MAX_CONNECTIONS` for `max_connections`, `OTHER` otherwise.
        """
        status, message = item.get("status"), item.get("message")
        text = message if isinstance(message, str) else ""
        if status == "success" and text.lower().startswith("subscribed to"):
            _, colon, listed = text.partition(":")
            topics = tuple(t.strip() for t in listed.split(",") if t.strip()) if colon else ()
            return Status(kind=StatusKind.ACK, topics=topics, message=text)
        if status == "error":
            return Status(kind=StatusKind.REFUSED, topics=self._named_topics(text), message=text)
        if status == "max_connections":
            return Status(kind=StatusKind.MAX_CONNECTIONS, message=text)
        return Status(kind=StatusKind.OTHER, message=text)

    def _named_topics(self, message: str) -> tuple[str, ...]:
        """Return the topics of known channels a refusal names, in order, without repeats.

        A word counts as a topic when, without trailing sentence punctuation, it is
        `<channel>.<rest>` for a channel with a source, so prose such as "U.S." never does.

        Args:
            message: The refusal's text.

        Returns:
            The topics, such as `("T.AAPL",)`.
        """
        topics: dict[str, None] = {}
        for word in _WORD.findall(message):
            topic = word.rstrip(".:!?")
            channel, dot, rest = topic.partition(".")
            if dot and rest and channel in self._channels:
                topics[topic] = None
        return tuple(topics)

    def _row(self, channel: str, item: dict[str, object], into: _Accumulator) -> None:
        """Decode one data object into a row or a reject, and record its drift.

        Args:
            channel: The object's channel, which has a source.
            item: The object.
            into: The channel's accumulator for this frame.
        """
        spec = self._channels[channel]
        keys = item.keys()
        if keys != spec.shape:  # the common case, an exact shape, costs one comparison
            present = keys - {"ev"}
            into.extra |= present - spec.declared
            into.missing |= spec.declared - present
        row: dict[str, object] = {}
        for step in spec.steps:
            value = item.get(step.wire_key)
            if value is None:
                if step.required:
                    problem = f"missing required key {step.wire_key!r} ({step.column})"
                    into.rejects.append(self._reject(channel, item, problem))
                    return
                row[step.column] = None
                continue
            try:
                row[step.column] = step.convert(value)
            except _UnconvertibleError as error:
                problem = (
                    f"field {step.wire_key!r} ({step.column}) expects {error}, got {_quote(value)}"
                )
                into.rejects.append(self._reject(channel, item, problem))
                return
        into.rows.append(row)

    def _reject(self, channel: str, item: dict[str, object], problem: str) -> Reject:
        """Build the reject of one row.

        Args:
            channel: The row's channel.
            item: The row's object, from which the raw symbol is read.
            problem: What is wrong with the row.

        Returns:
            The reject, whose error names the connection, the channel, the symbol, and
            `problem`.
        """
        key = self._channels[channel].symbol
        symbol = item.get(key) if key is not None else None
        text = symbol if isinstance(symbol, str) else None
        about = f" for {text}" if text is not None else ""
        error = WireError(
            f"rejected a {channel} row{about} from {self._host}{self._path}: {problem}",
            host=self._host,
            path=self._path,
        )
        return Reject(channel=channel, symbol=text, error=error)

    @staticmethod
    def _data(channel: str, frame: dict[str, _Accumulator]) -> Data:
        """Build the `Data` of one channel from its accumulator.

        Args:
            channel: The channel.
            frame: The frame's accumulators by channel.

        Returns:
            The channel's rows, rejects, and drift.
        """
        acc = frame[channel]
        drift = DriftReport(extra=frozenset(acc.extra), missing=frozenset(acc.missing))
        return Data(channel=channel, rows=acc.rows, rejects=acc.rejects, drift=drift)
