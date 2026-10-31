"""The ports between the session core and its transports, and the values that cross them.

The tables layer creates and tracks live results but does no I/O; the ingest layer owns every
wire client and connection. This module is the contract between the two, so the session core
never imports the code that does the I/O:

- `IngestPorts`: the session's ingest side as the core sees it, one factory per transport.
  `SessionClients` (`_ingest/clients.py`) implements it, and `MassiveSession` attaches it to the
  core once (`SessionCore.attach`).
- `RestTransport` (`fetch`, `poll`, REST `history`, and the WebSocket host decision),
  `StreamTransport` (opening a WebSocket stream), and `FlatFileTransport` (flat-file
  `history`): the transports the core's hooks return and the verbs call.
- `StreamHandle`: a live stream as `StreamTransport.open_stream` hands it to the core.
- `HostDecision`: which WebSocket host serves an (asset class, channel family), and why, as
  `RestTransport.resolve_host` decides it.

Everything here is a protocol or an immutable value: no I/O, no threads, and no Deephaven call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Sequence
    from datetime import date, timedelta

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._enums import AssetClass, ChannelFamily, PollMode, Recency
    from deephaven_massive._errors import MassiveError

__all__ = [
    "FlatFileRead",
    "FlatFileTransport",
    "HostDecision",
    "IngestPorts",
    "RestTransport",
    "StreamHandle",
    "StreamTransport",
]


@dataclass(frozen=True, slots=True)
class HostDecision:
    """Which WebSocket host serves a (asset class, channel family), and why.

    Returned by `RestTransport.resolve_host`; immutable, so it can be cached and shared between
    threads.

    Attributes:
        host: The hostname, such as `"delayed.massive.com"`.
        recency: Whether the host serves real-time or delayed data (`host_decision.recency_of`),
            or `UNKNOWN` for a pinned host the library does not know.
        evidence: Why: a human-readable account of the decision, shown in `m.connections()`.
    """

    host: str
    recency: Recency
    evidence: str


@dataclass(frozen=True, slots=True)
class StreamHandle:
    """A live stream as the WebSocket transport hands it to the session.

    The transport creates the stream's publisher (`BlinkPublisher`) and blink table inside `scope`
    (the stream scope) and registers the blink table's `on_shutdown_callback`, where the stream's
    registry record is dropped (`SessionCore.stream_shut_down`) and topic reference counts are
    released. The session's registry record holds the handle until the record is deleted (its holds
    released and its blink table shut down), and releases `scope` when the last result is released.
    Holding the handle holds `stop_watch`, and through it a reactive stream's watchlist listener,
    which Deephaven itself keeps only weakly.

    Attributes:
        stream_id: The stream's identifier, minted by the session before the open and unique
            within the process (the `StreamId` column of `m.streams()`).
        blink: The stream's own blink table. It is never handed to users: `blink()`
            returns a described copy of it.
        scope: The stream scope that manages `blink` and the publisher.
        host: The WebSocket host serving the stream.
        path: The cluster path, such as `/stocks`.
        evidence: Why the stream's host was chosen (`HostDecision.evidence`), for the log line
            of the open.
        recency: Whether the host serves real-time or delayed data (`HostDecision.recency`),
            for the stream's `m.streams()` row and its tables' descriptions.
        fail: Fails the stream's table with the error and records the failure, once; returns
            False when the stream had already failed. Takes the stream's publish lock and
            makes a Deephaven call, so never call it under `registry_lock` or on an
            update-graph thread (`StreamSink.fail`).
        stop_watch: Stops a reactive stream's watchlist listener (a Deephaven call, run on the
            reaper once the stream has shut down); None for a stream of fixed symbols.
    """

    stream_id: int
    blink: Table
    scope: LivenessScope
    host: str
    path: str
    evidence: str
    fail: Callable[[MassiveError], bool]
    stop_watch: Callable[[], None] | None = None
    recency: Recency = field(kw_only=True)


class RestTransport(Protocol):
    """The REST transport, from `SessionCore.rest_transport()` (`IngestPorts.rest`).

    Every method runs synchronously on the calling thread and raises `EntitlementError` on
    HTTP 403 and `MassiveError` on other failures; `poll()` instead fails its live table.

    The raw-row read the futures calendar uses (`RestEndpoints.fetch_rows`) is not part of this
    port: it takes the rate governor's bucket and the wire layer's `Cancel` token, which no
    module of `_tables` may name. The ingest layer, which wires the calendar to the REST
    transport, reaches it on the concrete `RestEndpoints` (`SessionClients.rest`).
    """

    def resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family), once per session.

        Called by the WebSocket transport on the caller thread at the first stream verb for the
        pair. The first call may make one REST request; the decision is then cached for the
        life of the session, so later calls return it without a request. Each new decision
        writes the pair's `Transport=REST` row of `m.connections()` (host, recency, evidence).

        Args:
            asset_class: The asset class of the stream.
            family: The channel family, `TICK` or `FMV`.

        Returns:
            The pair's decision: its host, its recency, and the evidence it rests on.

        Raises:
            AuthenticationError: Massive rejected the API key.
            MassiveError: The request failed. A refused request is not an error: the class
                then streams from the real-time host with `REALTIME` recency, and a refusal
                from the WebSocket server fails the refused streams.
        """
        ...

    def crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Return a crypto ticker's base and quote, looking them up in Massive if needed.

        Called by the WebSocket transport for each crypto ticker of a stream, before it starts
        or on its lookup worker. The first such call of the session blocks while it reads
        Massive's crypto ticker list (`/v3/reference/tickers?market=crypto`, every page, listed
        and delisted pairs), which the session then keeps; a failed read is read again by the
        next call.

        Args:
            symbol: A canonical crypto ticker.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")` for `X:DOTUSD`.

        A failed read, or a ticker the list does not hold, raises a `MassiveError` naming the
        ticker.
        """
        ...

    def fetch(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        call: int,
        stop: threading.Event | None = None,
    ) -> Table:
        """Return every row the dataset's `fetch` endpoint returns for the request, static.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            call: The verb call's id (`next_stream_id`), which its log lines and thread names
                carry.
            stop: A background read's stop token (`IngestPorts.read_stop`): once set, the
                read's requests stop waiting for the rate governor and no new one starts, so
                the call ends soon with an error the caller ignores. None for a call only the
                session's close stops.

        Returns:
            A static table with the dataset's columns, in its schema's order.

        Raises:
            ValueError: Before any request, if a path the call reads does not take a parameter
                or filter the request sets.
            AuthenticationError: If Massive rejects the API key (HTTP 401).
            EntitlementError: If the plan does not include the endpoint (HTTP 403).
            MassiveError: If a rate limit outlives the retries, a row breaks the dataset's
                schema, a symbol's later page answers HTTP 404, or a request fails in any other
                way.
        """
        ...

    def check_fetch(self, spec: DatasetSpec, request: Request) -> None:
        """Check, with no request, that `fetch` can plan its requests for the request.

        A background `fetch(wait=False)` calls it on the caller thread, so a parameter a path
        does not take raises there, at the call, as `fetch` raises it before its first request.
        The plan splits no crypto pair (that may read Massive's ticker list): the read does.

        Args:
            spec: The dataset.
            request: The normalized arguments, with static symbols.

        Raises:
            ValueError: The request sets a parameter a path of the call does not take.
        """
        ...

    def poll(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        interval: timedelta,
        mode: PollMode,
        query: str,
    ) -> Table:
        """Return a live table that re-runs `fetch` every `interval`, tracked by the session.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            interval: The time between cycles.
            mode: How cycles combine.
            query: The `repr` of the query, for the `Query` column of `m.streams()`.

        Returns:
            The live table, with the dataset's columns, tracked by the session. A failure
            after it exists fails the table rather than raising.

        Raises:
            ValueError: Before the table exists, if a path the call reads does not take a
                parameter or filter the request sets, even when another of its paths does.
                Over a reactive symbol table, whose symbols choose the paths at each cycle,
                only a parameter that no fetch path takes raises here; one the cycle's path
                lacks fails the table then.
            AuthenticationError: If Massive rejects the API key while the call reads its
                crypto ticker list to split a crypto ticker.
            EntitlementError: If the plan does not include that list (HTTP 403).
            MassiveError: If the session is closed, or a crypto ticker cannot be split into
                its currencies (Massive's crypto ticker list cannot be read or does not hold
                it). A cycle's failure later fails the table instead of raising.
        """
        ...

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        stop: threading.Event | None = None,
        days: Sequence[date] | None = None,
    ) -> Table:
        """Return the dataset's REST history over `window`, paged internally, static.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the parsed `start` and `end` bounds.
            window: What the call reads and keeps (`history_window` in
                `_datasets/history_source.py`).
            call: The verb call's id, as `fetch` takes it.
            stop: A background read's stop token, as `fetch` takes it.
            days: For `dm.ALL`, read one request per day, the only days of the window to
                request (the days the flat files lack); None for every day of the window.
                Ignored for explicit symbols, whose requests cover the window.

        Returns:
            A static table with the dataset's columns, holding the rows inside `window`.

        Raises:
            AuthenticationError: If Massive rejects the API key (HTTP 401).
            EntitlementError: If the plan does not include the endpoint (HTTP 403).
            MassiveError: If a rate limit outlives the retries, a row breaks the dataset's
                schema, a page of adjusted bars echoes another basis than the request, a
                symbol's later page answers HTTP 404, or a request fails in any other way. A
                bar of a day or longer stamped at a time Massive is not known to use is
                returned and logs a WARNING; it raises nothing.
        """
        ...

    def check_history(self, spec: DatasetSpec, request: Request, window: HistoryWindow) -> None:
        """Check, with no request, that `history` can plan its requests over `window`.

        The `check_fetch` of a background `history(..., source=REST or AUTO, wait=False)` that
        chose REST.

        Args:
            spec: The dataset; `history()` has checked that REST can serve the request.
            request: The normalized arguments, with static symbols and the parsed bounds.
            window: The call's window.

        Raises:
            ValueError: The request sets a parameter a path of the call does not take.
        """
        ...


class StreamTransport(Protocol):
    """The WebSocket transport, from `SessionCore.stream_transport()` (`IngestPorts.streams`)."""

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        """Create a new stream: its publisher and blink table in a new stream scope.

        Runs on the caller thread; may resolve the host with one REST call. Must not hold the
        session's registry lock while calling Deephaven. The session has already registered the
        stream under `stream_id`, so `SessionCore.fail_stream(stream_id, ...)` and
        `SessionCore.stream_shut_down(stream_id)` may be called from the moment the publisher
        exists, even before this method returns. The publisher is named
        `massive.{dataset}#{stream_id}`, and the blink table's `on_shutdown_callback` must first
        call `SessionCore.stream_shut_down(stream_id)` and only then release the stream's topics.
        The returned handle carries `stream_id`. If the open raises anything, even
        `KeyboardInterrupt`, the transport has released the stream scope and the stream's
        references (its connection lease and topics) before the exception propagates.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            stream_id: The id the session minted for the stream.

        Returns:
            The stream's handle, carrying `stream_id`, its blink table, and its scope.

        Raises:
            ValueError: If explicit options contracts would put more than 1,000 topics on the
                session's `/options` connection, or the stream is a fair-market-value stream
                of a subscription in `MassiveConfig.delayed_subscriptions`.
            AuthenticationError: If Massive rejects the API key on the request that decides the
                WebSocket host.
            EntitlementError: If the subscription is not in
                `MassiveConfig.delayed_subscriptions` and Massive labels its snapshot delayed.
            MassiveError: If the session is closed, the host cannot be decided, or Massive's
                crypto ticker list cannot be read or does not hold a crypto pair.
        """
        ...


@dataclass(frozen=True, slots=True)
class FlatFileRead:
    """What a flat-file `history` read returns: its rows by day, and the days it could not read.

    Returned by `FlatFileTransport.history`; immutable. The query merges `empty` and the parts,
    with any rows it reads elsewhere for the unread days, in date order.

    Attributes:
        parts: Each kept file's static tables with the file's day (for futures, its session),
            in date order and, within a day, venue and file order.
        empty: A static table with every column of the dataset and no row, which the merge
            starts from so a read without rows keeps every column.
        unread: The trading days whose rows the parts leave out, oldest first: every day
            whose file is missing, or, when the read inferred holidays, the first day whose
            file is missing with no later file, the parts then holding no later day either.
        uncertain: For futures named by dates, the requested sessions whose rows may sit in
            a file the read did not read, oldest first: a file held rows of a session other
            than its own day's, so the range's first or last session may be incomplete.
            Empty when no file did.
        absent: When the read inferred holidays, the trading days at the range's end whose
            file is missing with no later file, and that lie `ROLL_LIMIT` (7 days) or more
            before the current day: Massive publishes a file the day after its day, so these
            have no file at all, rather than one not published yet. Oldest first; empty for
            none.
    """

    parts: tuple[tuple[date, Table], ...]
    empty: Table
    unread: tuple[date, ...]
    uncertain: tuple[date, ...] = ()
    absent: tuple[date, ...] = ()


class FlatFileTransport(Protocol):
    """The flat-file transport, from `SessionCore.flat_file_transport()` (`IngestPorts.flat_files`).

    It only reads files: `HistoryQuery.history()` has already chosen flat files as the source
    (`choose_source` in `_datasets/history_source.py`).
    """

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        call: int,
        infer_holidays: bool,
        stop: threading.Event | None = None,
    ) -> FlatFileRead:
        """Return the rows of `window` from the dataset's flat files, static.

        Args:
            spec: The dataset, with a flat-file prefix for the request.
            request: The normalized arguments, with the raw `start` and `end` bounds and static
                symbols (never a reactive table).
            window: What the call reads and keeps (`history_window` in
                `_datasets/history_source.py`).
            dates: The partition dates to read (`partition_dates` in `_datasets/history_source.py`).
            call: The verb call's id (`next_stream_id`), which its log lines and thread names
                carry.
            infer_holidays: True when no other source can serve the call: a missing trading
                day's file with a later day's file is a day without trading. False to return
                every missing trading day as unread.
            stop: A background read's stop token (`IngestPorts.read_stop`): once set, every
                file read stops at its next block and no new one starts. None for a call only
                the session's close stops.

        Returns:
            The read (`FlatFileRead`): the files' tables by day, an empty table with every
            column, and the days the files could not serve.

        Raises:
            AuthenticationError: If S3 rejects the flat-file credentials.
            EntitlementError: If the plan does not include the files (S3 answers 403); for
                futures files the message names the venue and the `venue=` parameter.
            MassiveError: If the bucket or endpoint is wrong, or a read fails in any other
                way. A bar of a day or longer stamped at a time Massive is not known to use is
                returned and logs a WARNING; it raises nothing.
        """
        ...


class IngestPorts(Protocol):
    """The session's ingest side as the core sees it: one factory per transport.

    Implemented by `SessionClients` in `_ingest/clients.py`, which owns the session's wire
    clients and connections, and attached to the core once by `MassiveSession` (`attach`). The
    core's hooks (`rest_transport`, `stream_transport`, `flat_file_transport`) call these
    methods; verbs call the hooks. Each method runs on the calling thread, makes no network
    request, and may be called from any thread.
    """

    def rest(self) -> RestTransport:
        """Return the session's REST transport, over one REST client for the session's life.

        The first call creates the session's REST client and registers its close with the core
        (`SessionCore.on_close`); later calls share it.
        """
        ...

    def streams(self) -> StreamTransport:
        """Return a WebSocket transport over the session's connections.

        Each call may return a new, lightweight transport; every one opens its streams on the
        connections of the session's connection group, which sessions of the same API key and
        connection settings share. The first call joins the group and registers the session's
        leaving it with the session's closers. No connection is opened.
        """
        ...

    def flat_files(self) -> FlatFileTransport:
        """Return a new flat-file transport for one `history()` call.

        The first call creates the session's S3 client and registers its close with the core
        (`SessionCore.on_close`); later calls share it.

        Raises:
            ValueError: If the configuration has no S3 credentials.
        """
        ...

    def read_stop(self) -> threading.Event:
        """Return a new stop token for one background read, unset.

        The token is the wire layer's cancellation token, which the transports' `fetch` and
        `history` accept as `stop`: setting it ends the read's waits for the rate governor at
        once and stops its file reads at their next block. The tables layer only sets it and
        reads it, as the `threading.Event` it is.
        """
        ...
