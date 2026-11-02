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
  into `Status` events and per-channel `Data`: the rows (tuples of values in the order of the
  source's fields), `Reject`s for the rows dropped because a required key cannot be read, and
  the `DriftReport` of the channel's objects in the frame. Each value goes through its field's
  converter, the converter the REST client uses too (a field with a named `conversion` through
  that conversion), so a value means the same on both transports. A channel's objects are read
  a column at a time, as a REST page is (`spec.convert.column_converter`), and read again one by
  one only when a column holds a value that reading cannot vouch for; either way the rows,
  rejects, and drift are the same. Drift is reported as REST reports it: `extra` holds the
  keys a channel does not declare, which are dropped (a key the source lists as ignored,
  `Source.ignored_keys`, is dropped without being reported), `missing` only the fields the vendor
  marks required (`Field.required`) that some object lacked or held as null, and
  `unconvertible` the fields that held a value that does not fit, which reads as null. A row
  whose required key (`FrameDecoder.set_required`) is absent, null, or does not fit cannot be
  routed, so it is dropped and becomes a `Reject`. Each drifted key carries a sample
  (`DriftReport.samples`): the symbol of the first object that showed it and, for an extra key
  or a value that does not fit, its value. A dropped row carries its whole raw text
  (`Reject.raw`); it, and a row holding a value that does not fit, is logged whole at DEBUG on
  this module's logger, once, where it is decoded.
- **Failures.** `classify_failure` decides whether an exception from opening the socket is
  fatal or retryable, from websockets' own rule and two overrides, and how long a retryable
  one must wait at least. The overrides use the shared `net` helpers: `is_tls_failure`
  and the `Retry-After` reader.

Topics are exact strings throughout: nothing here matches one topic as a prefix of another.
Symbols are passed through as the wire spells them (`EUR/USD`, `BTC-USD`); mapping them to
Massive's canonical forms is the library's business.

This module imports the standard library, `websockets`, `net`, and `spec`.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from enum import StrEnum
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, Literal, NamedTuple

from websockets.client import process_exception
from websockets.exceptions import InvalidStatus

from deephaven_massive._massive.net import codec
from deephaven_massive._massive.net.errors import LIBRARY_BUG_REPORT, WireError, is_tls_failure
from deephaven_massive._massive.net.retry import retry_after_seconds
from deephaven_massive._massive.spec.convert import (
    UnconvertibleError,
    column_converter,
    excerpt,
    field_converter,
    missing_key_message,
    row_excerpt,
    unconvertible_message,
)
from deephaven_massive._massive.spec.types import DriftReport, DriftSample, Source, Transport

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from deephaven_massive._massive.spec.types import Field

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

DROPPED_ERROR_LIMIT: Final = 20
"""How many dropped frames or elements one connection logs at ERROR; later ones log at DEBUG."""

MAX_TOPICS_PER_FRAME = 1000
"""The most topics one subscribe or unsubscribe frame names.

Massive does not document a frame-size limit; 1,000 topics of options contracts make a frame
of about 25 KB, which is conservative.
"""

COLUMN_MIN_OBJECTS: Final = 4
"""The fewest objects of one channel in one frame that are read a column at a time.

Reading by column costs a few calls per column whatever the number of objects, so it is
cheaper than reading the objects one by one only from about four objects of a trade or quote
on (measured with the generated `stocks/T` source: a lone object takes about 1.7 times as long
by column, and a frame of 100 objects half as long). Fewer are read one by one, which gives the
same rows, rejects, and drift."""

# How much of an undecodable frame or element a log message or handshake error quotes.
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

            status = Status(kind=StatusKind.REFUSED, message="not authorized")
            assert not status.topics  # refuses the whole request it answers
    """

    kind: StatusKind
    topics: tuple[str, ...] = ()
    message: str = ""


@dataclass(frozen=True, kw_only=True)
class Reject:
    """One received row dropped because a required key is absent, null, or does not fit.

    The row cannot be routed, so it is not delivered; the library counts it as drift and fails
    no stream for it. The frame's other rows are delivered. Immutable, so it may be passed
    between threads.

    Attributes:
        channel: The channel the row arrived on, such as `"T"`.
        symbol: The row's symbol exactly as the wire spells it (`"EUR/USD"`, not `"C:EURUSD"`),
            read from the column `set_required` declared as the channel's symbol; `None` when
            no symbol column is declared or the row's value is missing or not text.
        error: Why the row was dropped: a missing required key or a value that does not fit,
            naming the connection, the field, the expected type, and the value.
        raw: The whole row as it arrived, its compact JSON text cut to 2,000 characters
            (`spec.convert.row_excerpt`), so a log line can show every field it held.
    """

    channel: str
    symbol: str | None
    error: WireError
    raw: str


@dataclass(frozen=True, kw_only=True)
class Data:
    """One channel's data objects from one frame, decoded.

    Attributes:
        channel: The channel, such as `"T"`.
        rows: The rows in arrival order, without the rows dropped for a required key: each a
            tuple of the values of the channel's source's fields, in the order of
            `Source.fields`, each in the form the field's converter gives it (`spec.convert`),
            or `None` when the wire omitted it, sent null, or sent a value that does not fit.
        rejects: The rows dropped for a required key, in arrival order.
        drift: The wire keys of these objects that the source does not declare (`extra`),
            the vendor-required fields some object lacked or held as null (`missing`), and the
            fields that held a value that does not fit (`unconvertible`).
    """

    channel: str
    rows: list[tuple[object, ...]]
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
    raise ValueError(f"expected a status object, got {_quote(data)}")


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
    if is_tls_failure(error):
        return WireError(
            f"TLS certificate verification failed for {host}: {error}; "
            "set SSL_CERT_FILE to a bundle that trusts the server's certificate",
            host=host,
            path=path,
        )
    status = error.response.status_code if isinstance(error, InvalidStatus) else None
    if isinstance(error, InvalidStatus) and status == HTTPStatus.TOO_MANY_REQUESTS:
        wait = retry_after_seconds(error.response.headers.get("Retry-After"), now)
        return 0.0 if wait is None else wait
    if process_exception(error) is None:
        return 0.0
    reason = f"HTTP {status}" if status is not None else f"{type(error).__name__}: {error}"
    return WireError(
        f"WebSocket connection to {host}{path} failed: {reason}",
        status=status,
        host=host,
        path=path,
    )


def _quote(value: object) -> str:
    """Return the start of a raw frame, or of an element of one, for a log line or an error.

    A row's bad value is quoted by `spec.convert` instead, as every decoder quotes it; this is
    for input that may not be JSON at all, such as undecodable bytes.

    Args:
        value: A raw frame (`str` or `bytes`) or a decoded element.

    Returns:
        `repr(value)`, cut to 200 characters.
    """
    return repr(value)[:_LOG_LIMIT]


# A word of a refusal message that may name a topic: a run of characters with no space,
# comma, semicolon, quote, bracket, or parenthesis.
_WORD = re.compile(r"""[^\s,;'"()\[\]]+""")


class _Step(NamedTuple):
    """How one field of a channel is read from each object: computed once, used per row.

    A tuple, so the decoder unpacks each step's parts at once for every value it reads.

    Attributes:
        wire_key: The key of the field's value in an object.
        column: The long-form column the value is stored under in a row.
        required: Whether a missing value (or null), or one that does not fit, drops the row:
            the library's choice through `FrameDecoder.set_required`, not the vendor's
            `Field.required`.
        convert: The field's converter (`spec.convert.field_converter`), which checks and
            converts each non-null value.
        field: The generated field, named in the message of a value that does not fit.
        read_column: The field's column reader (`spec.convert.column_converter`), which checks
            and converts the field's values of many objects at once.
    """

    wire_key: str
    column: str
    required: bool
    convert: Callable[[object], object]
    field: Field
    read_column: Callable[[list[object]], list[object] | None]


@dataclass(frozen=True, kw_only=True)
class _Channel:
    """What the decoder knows about one channel: its source and the library's key choice.

    Attributes:
        source: The channel's generated source.
        shape: The keys of an object that carries exactly the source's fields, its ignored
            keys, and `ev`; an object of that shape has no extra key, which one set comparison
            decides, and any other key of an object is an extra key.
        vendor_required: The wire keys of the fields the vendor marks required, whose absence
            or null is reported as drift.
        steps: One `_Step` per field, in the source's order.
        symbol: The wire key holding the symbol, or `None` when none is declared.
    """

    source: Source
    shape: frozenset[str]
    vendor_required: frozenset[str]
    steps: tuple[_Step, ...]
    symbol: str | None = None

    @classmethod
    def plan(
        cls, source: Source, required: frozenset[str] = frozenset(), symbol: str | None = None
    ) -> _Channel:
        """Compute a channel's reading plan.

        Args:
            source: The channel's generated source.
            required: The wire keys whose absence, null, or value that does not fit drops a row.
            symbol: The wire key holding the symbol, or `None`.

        Returns:
            The plan.
        """
        steps = tuple(
            _Step(
                f.wire_key,
                f.column,
                f.wire_key in required,
                field_converter(f),
                f,
                column_converter(f),
            )
            for f in source.fields
        )
        return cls(
            source=source,
            shape=frozenset(f.wire_key for f in source.fields) | source.ignored_keys | {"ev"},
            vendor_required=frozenset(f.wire_key for f in source.fields if f.required),
            steps=steps,
            symbol=symbol,
        )


@dataclass(kw_only=True)
class _Accumulator:
    """One channel's decoded objects of the frame being decoded."""

    # Each kept row's values, in the order of the channel's source's fields.
    rows: list[tuple[object, ...]] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)
    extra: set[str] = field(default_factory=set)
    missing: set[str] = field(default_factory=set)
    unconvertible: set[str] = field(default_factory=set)
    # The first object's symbol (and value, for an extra key) for each drifted key.
    samples: dict[str, DriftSample] = field(default_factory=dict)


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
            [data] = decoder.decode('[{"ev":"T","sym":"AAPL","p":1.5}]')
            assert data.rows == [("AAPL", 1.5)]
    """

    def __init__(self, sources: Iterable[Source], *, host: str, path: str) -> None:
        """Index the sources by channel.

        Args:
            sources: The WebSocket sources of the connection's cluster. Each key is
                `"<cluster>/<channel>"`, and the channel is the `ev` value its objects carry.
            host: The host of the connection, named in reject errors.
            path: The URL path of the connection, such as `"/stocks"`, named in reject errors.

        Raises:
            ValueError: If a source is not a WebSocket source, its key names no channel, a
                field's wire key is dotted (a WebSocket object is flat, so such a field could
                never be read), or two sources name one channel.
        """
        self._host = host
        self._path = path
        self._channels: dict[str, _Channel] = {}
        self._unknown: set[str] = set()
        self._dropped = 0  # frames and elements dropped, for `_drop_level`
        for source in sources:
            channel = source.key.partition("/")[2]
            if source.transport is not Transport.WS or not channel:
                raise ValueError(f"{source.key!r} is not a WebSocket source '<cluster>/<channel>'")
            dotted = sorted(f.wire_key for f in source.fields if "." in f.wire_key)
            if dotted:
                raise ValueError(
                    f"source {source.key!r} has dotted wire keys {dotted}; "
                    "a WebSocket object is flat"
                )
            if channel in self._channels:
                raise ValueError(f"two sources for channel {channel!r}")
            self._channels[channel] = _Channel.plan(source)

    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        """Declare a channel's required key columns and its symbol column, replacing any earlier.

        Args:
            channel: The channel, such as `"T"`.
            keys: Long-form columns of the channel's source; a row missing one, holding it as
                null, or holding a value that does not fit is dropped. Empty means no column is
                required.
            symbol: The long-form column holding the row's symbol, read raw into each
                `Reject.symbol` and drift sample; `None` when the channel's rows carry no
                symbol.

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
        source are dropped and logged (the first two at ERROR, as data the library cannot read,
        up to `DROPPED_ERROR_LIMIT` times per connection and at DEBUG after that; the third at
        WARNING once per channel); none of them stops the other events.

        Args:
            data: The frame's payload.

        Returns:
            The events; empty for a frame that carried none.
        """
        try:
            value = codec.loads(data)
        except ValueError:
            logger.log(
                self._drop_level(),
                "dropped a frame from %s%s that is not JSON: %s. %s",
                self._host,
                self._path,
                _quote(data),
                LIBRARY_BUG_REPORT,
                exc_info=True,
            )
            return []
        order: list[Status | str] = []
        frame: dict[str, list[dict[str, object]]] = {}  # each channel's objects, in order
        for item in value if isinstance(value, list) else [value]:
            if not isinstance(item, dict) or not isinstance(ev := item.get("ev"), str):
                logger.log(
                    self._drop_level(),
                    "dropped an element of a frame from %s%s with no text 'ev': %s. %s",
                    self._host,
                    self._path,
                    _quote(item),
                    LIBRARY_BUG_REPORT,
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
            elif ev in frame:
                frame[ev].append(item)
            else:
                frame[ev] = [item]
                order.append(ev)
        return [
            event if isinstance(event, Status) else self._data(event, frame[event])
            for event in order
        ]

    def _drop_level(self) -> int:
        """Count one dropped frame or element, and return the level to log it at.

        Returns:
            ERROR for the first `DROPPED_ERROR_LIMIT` of the connection, so a vendor change
            that breaks every frame logs a bounded number of ERRORs, and DEBUG after that.
        """
        self._dropped += 1
        if self._dropped <= DROPPED_ERROR_LIMIT:
            return logging.ERROR
        if self._dropped == DROPPED_ERROR_LIMIT + 1:
            logger.error(
                "%s%s: %d dropped frames or elements were logged; further ones are logged at DEBUG",
                self._host,
                self._path,
                DROPPED_ERROR_LIMIT,
            )
        return logging.DEBUG

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
        """Decode one data object into a row, or drop it, and record its drift.

        Args:
            channel: The object's channel, which has a source.
            item: The object.
            into: The channel's accumulator for this frame.
        """
        spec = self._channels[channel]
        self._scan(channel, item, into=into)
        row: list[object] = []
        odd: list[tuple[str, object, str]] = []
        for key, _, required, convert, f, _ in spec.steps:
            value = item.get(key)
            problem = None
            if value is not None:
                try:
                    row.append(convert(value))
                    continue
                except UnconvertibleError:
                    problem = unconvertible_message(f, value)
            if required:
                self._drop(channel, item, problem or missing_key_message(f), into=into)
                return
            if problem is not None:
                odd.append((key, value, problem))
            row.append(None)
        if odd:
            self._odd(channel, item, odd, into=into)
        into.rows.append(tuple(row))

    def _scan(self, channel: str, item: dict[str, object], *, into: _Accumulator) -> None:
        """Record an object's extra keys (`_extra`) and its missing vendor-required fields.

        The vendor-required scan runs only on a channel that has such fields, and covers every
        object, a dropped one included. Samples are taken only for a key not seen before in
        this frame, so a drifting frame costs one lookup per key.

        Args:
            channel: The object's channel, which has a source.
            item: The object.
            into: The channel's accumulator for this frame.
        """
        spec = self._channels[channel]
        self._extra(channel, item, into=into)
        if spec.vendor_required:
            for key in spec.vendor_required:
                if item.get(key) is None and key not in into.missing:
                    into.missing.add(key)
                    into.samples[key] = DriftSample(self._symbol(channel, item), None)

    def _extra(self, channel: str, item: dict[str, object], *, into: _Accumulator) -> None:
        """Record an object's extra keys: the keys its channel's source does not declare.

        An object of exactly the declared shape, the common case, has none, which one set
        comparison decides. A sample is taken only for a key not seen before in this frame.

        Args:
            channel: The object's channel, which has a source.
            item: The object.
            into: The channel's accumulator for this frame.
        """
        shape = self._channels[channel].shape
        keys = item.keys()
        if keys != shape:
            for key in keys - shape:
                if key not in into.extra:
                    into.extra.add(key)
                    into.samples[key] = DriftSample(self._symbol(channel, item), excerpt(item[key]))

    def _drop(
        self, channel: str, item: dict[str, object], problem: str, *, into: _Accumulator
    ) -> None:
        """Drop a row for a required key: record its `Reject` and log it whole at DEBUG.

        Args:
            channel: The row's channel.
            item: The row's object, from which the raw symbol is read.
            problem: Why: the clause `spec.convert` shares with every decoder, for a missing key
                or a value that does not fit.
            into: The channel's accumulator for this frame.
        """
        error = WireError(
            f"dropped {self._about(channel, item, problem)}", host=self._host, path=self._path
        )
        raw = row_excerpt(item)
        logger.debug("%s; the row: %s", error, raw)
        symbol = self._symbol(channel, item)
        into.rejects.append(Reject(channel=channel, symbol=symbol, error=error, raw=raw))

    def _odd(
        self,
        channel: str,
        item: dict[str, object],
        odd: list[tuple[str, object, str]],
        *,
        into: _Accumulator,
    ) -> None:
        """Record the values of a kept row that do not fit, and log the row whole at DEBUG.

        Args:
            channel: The row's channel.
            item: The row's object.
            odd: Each value that does not fit: its wire key, the value, and the clause naming
                the field, the expected type, and the value.
            into: The channel's accumulator for this frame.
        """
        for key, value, _ in odd:
            if key not in into.unconvertible:
                into.unconvertible.add(key)
                into.samples[key] = DriftSample(self._symbol(channel, item), excerpt(value))
        if logger.isEnabledFor(logging.DEBUG):
            problems = "; ".join(problem for _, _, problem in odd)
            reason = self._about(channel, item, problems)
            logger.debug("read as null in %s; the row: %s", reason, row_excerpt(item))

    def _about(self, channel: str, item: dict[str, object], problem: str) -> str:
        """Name a row and what is wrong with it, for a dropped row's reason and the DEBUG log.

        Args:
            channel: The row's channel.
            item: The row's object, from which the raw symbol is read.
            problem: What is wrong with the row.

        Returns:
            `"a <channel> row[ for <symbol>] from <host><path>: <problem>"`.
        """
        text = self._symbol(channel, item)
        about = f" for {text}" if text is not None else ""
        return f"a {channel} row{about} from {self._host}{self._path}: {problem}"

    def _symbol(self, channel: str, item: dict[str, object]) -> str | None:
        """Return an object's symbol as the wire spells it, for a drift sample or a reason.

        Args:
            channel: The object's channel.
            item: The object.

        Returns:
            The value of the key `set_required` declared as the channel's symbol, or None when
            none is declared or the value is missing or not text.
        """
        key = self._channels[channel].symbol
        symbol = item.get(key) if key is not None else None
        return symbol if isinstance(symbol, str) else None

    def _data(self, channel: str, items: list[dict[str, object]]) -> Data:
        """Decode one channel's objects of a frame, column by column when that is certain.

        From `COLUMN_MIN_OBJECTS` objects on, the objects are read a column at a time
        (`_by_column`); when that reading cannot vouch for a column, they are read again one by
        one (`_row`), which judges each value on its own, and why is logged at DEBUG. Fewer
        objects are read one by one at once. Either way the rows, the rejects, and the drift
        are the same, as for a REST page (`rest.batch`).

        Args:
            channel: The channel, which has a source.
            items: Its objects in the frame, in arrival order.

        Returns:
            The channel's rows, rejects, and drift.
        """
        acc = _Accumulator()
        if len(items) < COLUMN_MIN_OBJECTS or not self._by_column(channel, items, into=acc):
            for item in items:
                self._row(channel, item, acc)
        drift = DriftReport(
            extra=frozenset(acc.extra),
            missing=frozenset(acc.missing),
            unconvertible=frozenset(acc.unconvertible),
            samples=acc.samples,
        )
        return Data(channel=channel, rows=acc.rows, rejects=acc.rejects, drift=drift)

    def _by_column(
        self, channel: str, items: list[dict[str, object]], *, into: _Accumulator
    ) -> bool:
        """Read a channel's objects a column at a time, if that reading can vouch for them.

        The fast path, taken by nearly every frame of `COLUMN_MIN_OBJECTS` objects or more:
        each field's values are gathered into one list and checked and converted at once
        (`spec.convert.column_converter`), the reading REST pages share. It never judges an
        odd row: a required key that is absent or null, or a value the column reading cannot
        vouch for, makes it log why at DEBUG and return False, having recorded nothing.
        Otherwise it records the rows, the objects' extra keys, and their missing
        vendor-required fields in `into`, with the samples `_scan` would take.

        Args:
            channel: The channel, which has a source.
            items: Its objects in the frame, in arrival order.
            into: The channel's accumulator for this frame; changed only when it returns True.

        Returns:
            True when the objects were read, none dropped and no value found unconvertible;
            False when they must be read one by one (`_row`).
        """
        spec = self._channels[channel]
        columns: list[list[object]] = []
        absent: list[tuple[str, int]] = []  # each vendor-required key some object lacks
        reason = ""
        for key, column, required, _, f, read_column in spec.steps:
            values = [item.get(key) for item in items]
            if None in values:
                if required:
                    reason = f"the required column {column} is absent or null in some object"
                    break
                if f.required:
                    absent.append((key, values.index(None)))
            converted = read_column(values)
            if converted is None:
                reason = f"the column {column} holds a value that needs its own check"
                break
            columns.append(converted)
        if reason:
            logger.debug(
                "%s%s: reading %d %s rows one by one: %s",
                self._host,
                self._path,
                len(items),
                channel,
                reason,
            )
            return False
        if not all(map(spec.shape.issuperset, items)):
            for item in items:
                self._extra(channel, item, into=into)
        for key, index in absent:
            into.missing.add(key)
            into.samples[key] = DriftSample(self._symbol(channel, items[index]), None)
        # A source with no field still has a row, empty, per object.
        into.rows = list(zip(*columns, strict=True)) if columns else [()] * len(items)
        return True
